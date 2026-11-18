// Archive external blog links and retain their Wayback URLs in outlinksr/db.json.
// Run from the blog root: ./outlinksr/run.sh
// Failed links are only retried with --retry-failed. Successful links never
// trigger another request; pending jobs resume without resubmission.
// Set ARCHIVE_ORG_ACCESS_KEY and ARCHIVE_ORG_SECRET_KEY before saving new pages.
// Rate-limit cooldowns are saved in the ledger and honored by later runs.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/net/html"
)

var archivePattern = regexp.MustCompile(`^https?://web\.archive\.org/web/[0-9]{14}/https?://.+$`)
var errIncomplete = errors.New("some links are not archived; see outlinksr/db.json")

type Entry struct {
	Status     string `json:"status"`
	ArchiveURL string `json:"archive_url,omitempty"`
	JobID      string `json:"job_id,omitempty"`
	Error      string `json:"error,omitempty"`
	RetryAfter string `json:"retry_after,omitempty"`
}

type Ledger struct {
	mu   sync.Mutex
	path string
	data map[string]Entry
}

func splitFragment(raw string) (string, string) {
	base, fragment, _ := strings.Cut(raw, "#")
	return base, fragment
}

func extractLinks(content string) []string {
	seen := make(map[string]bool)
	tokenizer := html.NewTokenizer(strings.NewReader(content))
	for {
		kind := tokenizer.Next()
		if kind == html.ErrorToken {
			break
		}
		if kind != html.StartTagToken && kind != html.SelfClosingTagToken {
			continue
		}
		token := tokenizer.Token()
		if token.Data != "a" {
			continue
		}
		for _, attr := range token.Attr {
			if attr.Key != "href" {
				continue
			}
			raw := strings.TrimSpace(attr.Val)
			parsed, err := url.Parse(raw)
			if err == nil && (parsed.Scheme == "http" || parsed.Scheme == "https") && parsed.Host != "" && !excludedLink(raw) {
				seen[raw] = true
			}
			break
		}
	}
	return sortedKeys(seen)
}

func sortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func collectLinks(root string) ([]string, error) {
	command := exec.Command("git", "ls-files", "-z", "--cached", "--others", "--exclude-standard", "--", "*.html")
	command.Dir = root
	files, err := command.Output()
	if err != nil {
		return nil, fmt.Errorf("list blog HTML files: %w", err)
	}
	seen := make(map[string]bool)
	for _, name := range strings.Split(string(files), "\x00") {
		if name == "" {
			continue
		}
		content, err := os.ReadFile(filepath.Join(root, name))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		for _, link := range extractLinks(string(content)) {
			seen[link] = true
		}
	}
	return sortedKeys(seen), nil
}

func lockRepo(root string) (func(), error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if resolved, err := filepath.EvalSymlinks(root); err == nil {
		root = resolved
	}
	sum := sha256.Sum256([]byte(root))
	name := filepath.Join(os.TempDir(), fmt.Sprintf("blog-archive-links-%x.lock", sum[:10]))
	file, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		file.Close()
		return nil, fmt.Errorf("another archive-links process is already running: %w", err)
	}
	return func() { file.Close() }, nil
}

func loadLedger(path string) (*Ledger, error) {
	l := &Ledger{path: path, data: make(map[string]Entry)}
	content, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return l, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(content, &l.data); err != nil || l.data == nil {
		return nil, fmt.Errorf("invalid archive ledger %s", path)
	}
	for raw, entry := range l.data {
		if entry.Status == "" || (entry.Status == "archived" && !archivePattern.MatchString(entry.ArchiveURL)) {
			return nil, fmt.Errorf("invalid archive ledger entry: %s", raw)
		}
	}
	return l, nil
}

func (l *Ledger) record(base string) (Entry, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	var result Entry
	found := false
	for _, raw := range sortedKeys(l.data) {
		if candidate, _ := splitFragment(raw); candidate != base {
			continue
		}
		entry := l.data[raw]
		if !found || entry.Status == "archived" {
			result, found = entry, true
		}
		if result.Status == "archived" {
			break
		}
	}
	result.ArchiveURL, _ = splitFragment(result.ArchiveURL)
	return result, found
}

func (l *Ledger) put(base string, aliases []string, entry Entry) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	updated := make(map[string]Entry, len(l.data)+len(aliases))
	for raw, previous := range l.data {
		updated[raw] = previous
		if candidate, _ := splitFragment(raw); candidate == base {
			aliases = append(aliases, raw)
		}
	}
	changed := false
	for _, raw := range aliases {
		value := entry
		if _, fragment := splitFragment(raw); fragment != "" && value.ArchiveURL != "" {
			value.ArchiveURL += "#" + fragment
		}
		if previous, ok := updated[raw]; !ok || previous != value {
			updated[raw], changed = value, true
		}
	}
	if !changed {
		return nil
	}
	return l.write(updated)
}

// write commits a replacement while the caller holds l.mu.
func (l *Ledger) write(updated map[string]Entry) error {
	var content bytes.Buffer
	encoder := json.NewEncoder(&content)
	encoder.SetIndent("", "  ")
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(updated); err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(l.path), ".archive-links-*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	if _, err = file.Write(content.Bytes()); err != nil {
		return err
	}
	if err = file.Sync(); err != nil {
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	if err = os.Rename(file.Name(), l.path); err != nil {
		return err
	}
	l.data = updated
	return nil
}

func (l *Ledger) retryAfter() time.Time {
	l.mu.Lock()
	defer l.mu.Unlock()
	return latestRetryAfter(l.data)
}

func (l *Ledger) removeExcluded() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	updated := make(map[string]Entry, len(l.data))
	for raw, entry := range l.data {
		if !excludedLink(raw) {
			updated[raw] = entry
		}
	}
	if len(updated) == len(l.data) {
		return nil
	}
	if cooldown := latestRetryAfter(l.data); cooldown.After(latestRetryAfter(updated)) {
		for _, raw := range sortedKeys(updated) {
			if entry := updated[raw]; entry.Status != "archived" {
				entry.RetryAfter = cooldown.UTC().Format(time.RFC3339Nano)
				updated[raw] = entry
				break
			}
		}
	}
	return l.write(updated)
}

func latestRetryAfter(entries map[string]Entry) time.Time {
	var latest time.Time
	for _, entry := range entries {
		if parsed, err := time.Parse(time.RFC3339Nano, entry.RetryAfter); err == nil && parsed.After(latest) {
			latest = parsed
		}
	}
	return latest
}

func (l *Ledger) setCooldown(until time.Time, urls []string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !until.After(latestRetryAfter(l.data)) {
		return nil
	}
	var target string
	for _, raw := range sortedKeys(l.data) {
		if l.data[raw].Status != "archived" {
			target = raw
			break
		}
	}
	if target == "" {
		// A first-ever availability request can be rate-limited before any result
		// exists. Keep the URL unprocessed so the next eligible run checks it.
		candidates := append([]string(nil), urls...)
		sort.Strings(candidates)
		for _, raw := range candidates {
			if entry, exists := l.data[raw]; !exists || entry.Status != "archived" {
				target = raw
				break
			}
		}
	}
	if target == "" {
		return nil
	}
	updated := make(map[string]Entry, len(l.data)+1)
	for raw, entry := range l.data {
		updated[raw] = entry
	}
	entry := updated[target]
	if entry.Status == "" {
		entry.Status = "unchecked"
	}
	entry.RetryAfter = until.UTC().Format(time.RFC3339Nano)
	updated[target] = entry
	return l.write(updated)
}

type capture struct {
	Status        string `json:"status"`
	JobID         string `json:"job_id"`
	OriginalJobID string `json:"original_job_id"`
	OriginalURL   string `json:"original_url"`
	Timestamp     string `json:"timestamp"`
	StatusExt     string `json:"status_ext"`
	Message       string `json:"message"`
}

func (c capture) reason() string {
	if c.StatusExt != "" {
		return c.StatusExt
	}
	if c.Message != "" {
		return c.Message
	}
	return "Unexpected Wayback response."
}

func backpressure(c capture) bool {
	switch c.StatusExt {
	case "error:user-session-limit", "error:blocked-client-ip", "error:no-browsers-available", "error:internal-server-error", "error:celery":
		return true
	}
	message := strings.ToLower(c.Message)
	return strings.Contains(c.StatusExt, "daily-bandwidth") || strings.Contains(message, "limit of active sessions") || strings.Contains(message, "rate limit")
}

type serviceUnavailable struct {
	message string
	retryAt time.Time
}

func (e *serviceUnavailable) Error() string { return e.message }

func parseRetryAfter(value string, now time.Time) time.Time {
	value = strings.TrimSpace(value)
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil && seconds >= 0 && seconds <= int64((1<<63-1)/time.Second) {
		return now.Add(time.Duration(seconds) * time.Second)
	}
	if date, err := http.ParseTime(value); err == nil {
		return date
	}
	return now.Add(time.Minute)
}

type authenticationError struct{ message string }

func (e *authenticationError) Error() string { return e.message }

func fatalServiceError(err error) bool {
	var unavailable *serviceUnavailable
	return errors.As(err, &unavailable) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

type archiveService interface {
	available(context.Context, string) (string, error)
	save(context.Context, string) (capture, error)
	status(context.Context, string) (capture, error)
}

type paceGate struct {
	mu   sync.Mutex
	next time.Time
}

func wait(ctx context.Context, duration time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if duration <= 0 {
		return nil
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (g *paceGate) wait(ctx context.Context, interval time.Duration) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := wait(ctx, time.Until(g.next)); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	g.next = time.Now().Add(interval)
	return nil
}

type Client struct {
	httpClient      *http.Client
	availabilityURL string
	saveURL         string
	authorization   string
	requestInterval time.Duration
	saveInterval    time.Duration
	requestGate     paceGate
	saveGate        paceGate
}

func (c *Client) pace(ctx context.Context, save bool) error {
	if !save {
		return c.requestGate.wait(ctx, c.requestInterval)
	}
	// Keep save starts spaced even when status requests occupy the request gate.
	c.saveGate.mu.Lock()
	defer c.saveGate.mu.Unlock()
	if err := wait(ctx, time.Until(c.saveGate.next)); err != nil {
		return err
	}
	if err := c.requestGate.wait(ctx, c.requestInterval); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c.saveGate.next = time.Now().Add(c.saveInterval)
	return nil
}

func newClient(credentials string) (*Client, error) {
	access, secret := os.Getenv("ARCHIVE_ORG_ACCESS_KEY"), os.Getenv("ARCHIVE_ORG_SECRET_KEY")
	if access == "" && secret == "" {
		access, secret = os.Getenv("IA_ACCESS_KEY"), os.Getenv("IA_SECRET_KEY")
	}
	if credentials != "" {
		content, err := os.ReadFile(credentials)
		if err != nil {
			return nil, err
		}
		var keys struct {
			Access string `json:"access_key"`
			Secret string `json:"secret_key"`
		}
		if err := json.Unmarshal(content, &keys); err != nil || keys.Access == "" || keys.Secret == "" {
			return nil, errors.New("credentials file needs nonempty access_key and secret_key strings")
		}
		access, secret = keys.Access, keys.Secret
	}
	if (access == "") != (secret == "") {
		return nil, errors.New("set both ARCHIVE_ORG_ACCESS_KEY and ARCHIVE_ORG_SECRET_KEY, or neither")
	}
	c := &Client{
		httpClient:      &http.Client{Timeout: 45 * time.Second},
		availabilityURL: "https://archive.org/wayback/available",
		saveURL:         "https://web.archive.org/save",
		requestInterval: time.Second,
		saveInterval:    21 * time.Second,
	}
	if access != "" {
		c.authorization = "LOW " + access + ":" + secret
		c.saveInterval = 9 * time.Second
	}
	return c, nil
}

func (c *Client) request(ctx context.Context, endpoint string, data url.Values, output any) error {
	attempts := 3
	if data != nil {
		attempts = 1 // A timed-out POST may already have started a capture.
	}
	var last error
	for attempt := 0; attempt < attempts; attempt++ {
		if err := c.pace(ctx, data != nil); err != nil {
			return err
		}
		method := http.MethodGet
		var body io.Reader
		if data != nil {
			method, body = http.MethodPost, strings.NewReader(data.Encode())
		}
		request, err := http.NewRequestWithContext(ctx, method, endpoint, body)
		if err != nil {
			return err
		}
		request.Header.Set("Accept", "application/json")
		request.Header.Set("User-Agent", "blog-archive-links/1.0")
		if data != nil {
			request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		}
		if c.authorization != "" {
			request.Header.Set("Authorization", c.authorization)
		}
		response, err := c.httpClient.Do(request)
		if err == nil {
			if response.StatusCode == http.StatusTooManyRequests {
				response.Body.Close()
				retryAt := parseRetryAfter(response.Header.Get("Retry-After"), time.Now())
				return &serviceUnavailable{
					message: "Wayback rate limit; retry after " + retryAt.UTC().Format(time.RFC3339) + ".",
					retryAt: retryAt,
				}
			}
			if data != nil && (response.StatusCode == 401 || response.StatusCode == 403) {
				response.Body.Close()
				return &authenticationError{fmt.Sprintf("Save API returned HTTP %d; check Internet Archive credentials.", response.StatusCode)}
			}
			if response.StatusCode != http.StatusOK {
				response.Body.Close()
				last = fmt.Errorf("Wayback returned HTTP %d", response.StatusCode)
				if response.StatusCode < 500 {
					return last
				}
				if data != nil {
					return &serviceUnavailable{message: last.Error() + "; captures paused. Rerun later to recover the submission."}
				}
			} else {
				err = json.NewDecoder(io.LimitReader(response.Body, 8<<20)).Decode(output)
				response.Body.Close()
				if err == nil {
					return nil
				}
				last = fmt.Errorf("decode Wayback response: %w", err)
			}
		} else {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			last = fmt.Errorf("Wayback connection failed: %w", err)
		}
		if data != nil {
			return last
		}
		if attempt+1 < attempts {
			if err := wait(ctx, time.Duration(1<<(attempt+1))*time.Second); err != nil {
				return err
			}
		}
	}
	return &serviceUnavailable{message: last.Error()}
}

func (c *Client) available(ctx context.Context, raw string) (string, error) {
	var result struct {
		Snapshots struct {
			Closest struct {
				Available bool            `json:"available"`
				URL       string          `json:"url"`
				Status    json.RawMessage `json:"status"`
			} `json:"closest"`
		} `json:"archived_snapshots"`
	}
	err := c.request(ctx, c.availabilityURL+"?"+url.Values{"url": {raw}}.Encode(), nil, &result)
	if err != nil {
		return "", err
	}
	capture := result.Snapshots.Closest
	if capture.Available && strings.Trim(string(capture.Status), `"`) == "200" && archivePattern.MatchString(capture.URL) {
		return "https://" + strings.SplitN(capture.URL, "://", 2)[1], nil
	}
	return "", nil
}

func (c *Client) save(ctx context.Context, raw string) (capture, error) {
	var result capture
	if c.authorization == "" {
		return result, &authenticationError{"Set ARCHIVE_ORG_ACCESS_KEY and ARCHIVE_ORG_SECRET_KEY to save new pages."}
	}
	err := c.request(ctx, c.saveURL, url.Values{"url": {raw}, "skip_first_archive": {"1"}, "if_not_archived_within": {"3600"}}, &result)
	return result, err
}

func (c *Client) status(ctx context.Context, job string) (capture, error) {
	var result capture
	err := c.request(ctx, c.saveURL+"/status/"+url.PathEscape(job), nil, &result)
	return result, err
}

type work struct {
	base    string
	aliases []string
	entry   Entry
}

type runner struct {
	ledger       *Ledger
	client       archiveService
	concurrency  int
	retryFailed  bool
	pollInterval time.Duration
	pollLimit    int
	output       io.Writer
	outputMu     sync.Mutex
}

func (r *runner) printf(format string, args ...any) {
	r.outputMu.Lock()
	defer r.outputMu.Unlock()
	if r.output != nil {
		fmt.Fprintf(r.output, format, args...)
	}
}

func parallel(ctx context.Context, items []work, count int, fn func(context.Context, work) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan work)
	failures := make(chan error, count)
	var workers sync.WaitGroup
	for i := 0; i < count; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case item, ok := <-jobs:
					if !ok {
						return
					}
					if ctx.Err() != nil {
						return
					}
					if err := fn(ctx, item); err != nil {
						select {
						case failures <- err:
						default:
						}
						cancel()
						return
					}
				}
			}
		}()
	}
feed:
	for _, item := range items {
		select {
		case <-ctx.Done():
			break feed
		case jobs <- item:
		}
	}
	close(jobs)
	workers.Wait()
	close(failures)
	var first error
	var longest *serviceUnavailable
	for err := range failures {
		if first == nil {
			first = err
		}
		var unavailable *serviceUnavailable
		if errors.As(err, &unavailable) && !unavailable.retryAt.IsZero() && (longest == nil || unavailable.retryAt.After(longest.retryAt)) {
			longest = unavailable
		}
	}
	if longest != nil {
		return longest
	}
	if first != nil {
		return first
	}
	return ctx.Err()
}

func (r *runner) recover(ctx context.Context, item work, fallback Entry) error {
	archive, err := r.client.available(ctx, item.base)
	if err != nil {
		return err
	}
	if archive != "" {
		fallback = Entry{Status: "archived", ArchiveURL: archive}
	}
	return r.ledger.put(item.base, item.aliases, fallback)
}

func (r *runner) finishJob(ctx context.Context, base string, aliases []string, entry Entry) error {
	limit := r.pollLimit
	if limit <= 0 {
		limit = 36
	}
	for i := 0; i < limit; i++ {
		result, err := r.client.status(ctx, entry.JobID)
		if err != nil {
			if fatalServiceError(err) {
				return err
			}
			entry.Status, entry.Error = "uncertain", "Job status unavailable; no capture found."
			return r.recover(ctx, work{base, aliases, entry}, entry)
		}
		if result.OriginalJobID != "" && result.OriginalJobID != entry.JobID {
			entry = Entry{Status: "pending", JobID: result.OriginalJobID}
			if err := r.ledger.put(base, aliases, entry); err != nil {
				return err
			}
			continue
		}
		if backpressure(result) {
			return &serviceUnavailable{message: "Wayback paused captures: " + result.reason() + ". Rerun later."}
		}
		switch result.Status {
		case "success":
			archive := "https://web.archive.org/web/" + result.Timestamp + "/" + result.OriginalURL
			if archivePattern.MatchString(archive) {
				return r.ledger.put(base, aliases, Entry{Status: "archived", ArchiveURL: archive})
			}
		case "error":
			return r.recover(ctx, work{base, aliases, entry}, Entry{Status: "failed", Error: result.reason()})
		case "pending":
			if err := wait(ctx, r.pollInterval); err != nil {
				return err
			}
			continue
		}
		entry.Status, entry.Error = "uncertain", "Unexpected capture response."
		return r.ledger.put(base, aliases, entry)
	}
	r.printf("Still pending: %s\n", base)
	return &serviceUnavailable{message: "A capture is still pending; rerun to resume its job before submitting more captures."}
}

func (r *runner) run(ctx context.Context, urls []string) (runError error) {
	if err := r.ledger.removeExcluded(); err != nil {
		return err
	}
	filtered := make([]string, 0, len(urls))
	for _, raw := range urls {
		if !excludedLink(raw) {
			filtered = append(filtered, raw)
		}
	}
	urls = filtered
	defer func() {
		var unavailable *serviceUnavailable
		if errors.As(runError, &unavailable) && !unavailable.retryAt.IsZero() {
			if err := r.ledger.setCooldown(unavailable.retryAt, urls); err != nil {
				runError = errors.Join(runError, fmt.Errorf("persist rate-limit cooldown: %w", err))
			}
		}
	}()
	if retryAt := r.ledger.retryAfter(); retryAt.After(time.Now()) {
		return &serviceUnavailable{
			message: "Wayback requests are paused until " + retryAt.UTC().Format(time.RFC3339) + ".",
			retryAt: retryAt,
		}
	}
	groups := make(map[string][]string)
	for _, raw := range urls {
		base, _ := splitFragment(raw)
		groups[base] = append(groups[base], raw)
	}
	var lookups, pending []work
	for _, base := range sortedKeys(groups) {
		aliases := groups[base]
		entry, found := r.ledger.record(base)
		if archivePattern.MatchString(base) {
			entry, found = Entry{Status: "archived", ArchiveURL: base}, true
		}
		if found && entry.Status == "archived" {
			if err := r.ledger.put(base, aliases, entry); err != nil {
				return err
			}
			continue
		}
		if found && (entry.Status == "pending" || entry.Status == "ready" || (entry.Status == "needs_auth" && r.retryFailed)) {
			if entry.Status == "needs_auth" {
				entry = Entry{Status: "ready"}
			}
			if err := r.ledger.put(base, aliases, entry); err != nil {
				return err
			}
			pending = append(pending, work{base, aliases, entry})
			continue
		}
		if found && !r.retryFailed && entry.Status != "submitting" && entry.Status != "unchecked" {
			if err := r.ledger.put(base, aliases, entry); err != nil {
				return err
			}
			continue
		}
		lookups = append(lookups, work{base, aliases, entry})
	}
	var pendingMu sync.Mutex
	err := parallel(ctx, lookups, 4, func(ctx context.Context, item work) error {
		r.printf("Checking: %s\n", item.base)
		archive, err := r.client.available(ctx, item.base)
		if err != nil {
			if fatalServiceError(err) {
				return err
			}
			return r.ledger.put(item.base, item.aliases, Entry{Status: "failed", Error: err.Error()})
		}
		if archive != "" {
			r.printf("Found: %s\n", archive)
			return r.ledger.put(item.base, item.aliases, Entry{Status: "archived", ArchiveURL: archive})
		}
		if item.entry.Status == "submitting" && !r.retryFailed {
			return r.ledger.put(item.base, item.aliases, Entry{Status: "uncertain", Error: "Interrupted submission; no capture found. Use --retry-failed to retry explicitly."})
		}
		item.entry = Entry{Status: "ready"}
		if err := r.ledger.put(item.base, item.aliases, item.entry); err != nil {
			return err
		}
		pendingMu.Lock()
		pending = append(pending, item)
		pendingMu.Unlock()
		return nil
	})
	if err != nil {
		return err
	}
	sort.SliceStable(pending, func(i, j int) bool {
		if (pending[i].entry.Status == "pending") != (pending[j].entry.Status == "pending") {
			return pending[i].entry.Status == "pending"
		}
		return pending[i].base < pending[j].base
	})
	count := r.concurrency
	if count <= 0 {
		count = 3
	}
	err = parallel(ctx, pending, count, func(ctx context.Context, item work) error {
		if item.entry.Status != "pending" {
			if err := r.ledger.put(item.base, item.aliases, Entry{Status: "submitting"}); err != nil {
				return err
			}
			r.printf("Saving: %s\n", item.base)
			result, err := r.client.save(ctx, item.base)
			if err != nil {
				if fatalServiceError(err) {
					return err
				}
				entry := Entry{Status: "uncertain", Error: err.Error()}
				var auth *authenticationError
				if errors.As(err, &auth) {
					entry.Status = "needs_auth"
				}
				if err := r.ledger.put(item.base, item.aliases, entry); err != nil {
					return err
				}
				if auth != nil {
					return auth
				}
				return nil
			}
			if backpressure(result) {
				return &serviceUnavailable{message: "Wayback paused captures: " + result.reason() + ". Rerun later."}
			}
			if result.JobID == "" {
				return r.recover(ctx, item, Entry{Status: "failed", Error: result.reason()})
			}
			item.entry = Entry{Status: "pending", JobID: result.JobID}
			if err := r.ledger.put(item.base, item.aliases, item.entry); err != nil {
				return err
			}
		}
		if err := r.finishJob(ctx, item.base, item.aliases, item.entry); err != nil {
			return err
		}
		entry, _ := r.ledger.record(item.base)
		r.printf("%s: %s\n", entry.Status, item.base)
		return nil
	})
	if err != nil {
		return err
	}
	counts := make(map[string]int)
	for _, raw := range urls {
		base, _ := splitFragment(raw)
		entry, _ := r.ledger.record(base)
		counts[entry.Status]++
	}
	var summary []string
	for _, status := range sortedKeys(counts) {
		summary = append(summary, fmt.Sprintf("%d %s", counts[status], status))
	}
	r.printf("%d links, %d distinct pages: %s\n", len(urls), len(groups), strings.Join(summary, ", "))
	if counts["archived"] != len(urls) {
		return errIncomplete
	}
	return nil
}

func main() {
	defaultRoot := "."
	if output, err := exec.Command("git", "rev-parse", "--show-toplevel").Output(); err == nil {
		defaultRoot = strings.TrimSpace(string(output))
	}
	root := flag.String("root", defaultRoot, "Blog repository directory")
	retry := flag.Bool("retry-failed", false, "Explicitly retry unsuccessful links")
	concurrency := flag.Int("concurrency", 3, "Maximum simultaneous capture jobs")
	credentials := flag.String("credentials", "", "Private JSON file containing access_key and secret_key")
	explain := flag.String("explain", "", "Explain whether a URL is excluded without making archive requests")
	flag.Parse()
	if *explain != "" {
		if reason := exclusionReason(*explain); reason != "" {
			fmt.Println("Excluded: " + reason)
		} else {
			fmt.Println("Eligible: no exclusion rule matched")
		}
		return
	}
	if *concurrency < 1 {
		fmt.Fprintln(os.Stderr, "--concurrency must be positive")
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	err := func() error {
		unlock, err := lockRepo(*root)
		if err != nil {
			return err
		}
		defer unlock()
		urls, err := collectLinks(*root)
		if err != nil {
			return err
		}
		ledger, err := loadLedger(filepath.Join(*root, "outlinksr", "db.json"))
		if err != nil {
			return err
		}
		client, err := newClient(*credentials)
		if err != nil {
			return err
		}
		r := runner{ledger: ledger, client: client, concurrency: *concurrency, retryFailed: *retry, pollInterval: 10 * time.Second, output: os.Stdout}
		return r.run(ctx, urls)
	}()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		if errors.Is(err, context.Canceled) {
			os.Exit(130)
		}
		os.Exit(1)
	}
}
