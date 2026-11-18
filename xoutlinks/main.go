// Archive external blog links and retain their Wayback URLs in xoutlinks/db.json.
// Run from the blog root: ./xoutlinks/run.sh
// Failed links are retried by default. Archives expire after six
// months; fresh entries are skipped and pending jobs resume without resubmission.
// Set ARCHIVE_ORG_ACCESS_KEY and ARCHIVE_ORG_SECRET_KEY before saving new pages.
// Entries waiting for credentials resume automatically when credentials exist.
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
	"maps"
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
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/net/html"
)

var archivePattern = regexp.MustCompile(`^https?://web\.archive\.org/web/([0-9]{14})/https?://.+$`)
var errIncomplete = errors.New("some links are not archived; see xoutlinks/db.json")

const databaseVersion = 1
const blockedURLError = "error:blocked-url"

type database struct {
	Version int              `json:"version"`
	RetryAt string           `json:"retry_at,omitempty"`
	Entries map[string]Entry `json:"entries"`
}

type Entry struct {
	Status           string `json:"status"`
	ArchiveURL       string `json:"archive_url,omitempty"`
	ExpiresAt        string `json:"expires_at,omitempty"`
	RefreshStartedAt string `json:"refresh_started_at,omitempty"`
	JobID            string `json:"job_id,omitempty"`
	Error            string `json:"error,omitempty"`
	ErrorMsg         string `json:"error_msg,omitempty"`
	RetryAt          string `json:"retry_at,omitempty"`
}

func (e Entry) errorText() string {
	if e.Error == "" {
		return e.ErrorMsg
	}
	if e.ErrorMsg == "" || e.ErrorMsg == e.Error {
		return e.Error
	}
	return e.Error + ": " + e.ErrorMsg
}

type Ledger struct {
	mu       sync.Mutex
	path     string
	data     map[string]Entry
	cooldown time.Time
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
			if err == nil && (parsed.Scheme == "http" || parsed.Scheme == "https") && parsed.Host != "" {
				seen[raw] = true
			}
			break
		}
	}
	return sortedKeys(seen)
}

func filterLinks(links []string) (included, excluded []string) {
	included, excluded = []string{}, []string{}
	for _, link := range links {
		if excludedLink(link) {
			excluded = append(excluded, link)
		} else {
			included = append(included, link)
		}
	}
	sort.Strings(included)
	sort.Strings(excluded)
	return included, excluded
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
	return loadLedgerAt(path, time.Now())
}

func loadLedgerAt(path string, now time.Time) (*Ledger, error) {
	l := &Ledger{path: path, data: make(map[string]Entry)}
	content, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return l, nil
	}
	if err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(content, &fields); err != nil || fields == nil {
		return nil, fmt.Errorf("invalid archive ledger %s", path)
	}
	_, versioned := fields["version"]
	if versioned {
		var stored database
		if err := json.Unmarshal(content, &stored); err != nil || stored.Entries == nil {
			return nil, fmt.Errorf("invalid archive ledger %s", path)
		}
		if stored.Version != databaseVersion {
			return nil, fmt.Errorf("unsupported archive database version %d", stored.Version)
		}
		l.data = stored.Entries
		if stored.RetryAt != "" {
			l.cooldown, err = time.Parse(time.RFC3339Nano, stored.RetryAt)
			if err != nil {
				return nil, fmt.Errorf("invalid global retry_at in %s", path)
			}
		}
	} else if err := json.Unmarshal(content, &l.data); err != nil || l.data == nil {
		return nil, fmt.Errorf("invalid legacy archive ledger %s", path)
	}
	rewrite := !versioned
	for raw, entry := range l.data {
		if normalized := normalizeFailure(raw, entry); normalized != entry {
			entry, rewrite = normalized, true
		}
		if entry.Status == "" || (entry.Status == "archived" && !archivePattern.MatchString(entry.ArchiveURL)) {
			return nil, fmt.Errorf("invalid archive ledger entry: %s", raw)
		}
		for _, timestamp := range []string{entry.ExpiresAt, entry.RefreshStartedAt, entry.RetryAt} {
			if timestamp != "" {
				if _, err := time.Parse(time.RFC3339, timestamp); err != nil {
					return nil, fmt.Errorf("invalid archive timestamp for %s", raw)
				}
			}
		}
		// Only the legacy, unversioned database gets this one-time grace period.
		// In version 1 and later, a missing expiry must remain expired.
		if !versioned && entry.ArchiveURL != "" && entry.ExpiresAt == "" {
			entry.ExpiresAt = expiryAfterSixMonths(now).Format(time.RFC3339)
		}
		if entry.Status == "archived" && (entry.Error != "" || entry.ErrorMsg != "" || entry.RetryAt != "") {
			entry.Error, entry.ErrorMsg, entry.RetryAt = "", "", ""
			rewrite = true
		} else if entry.permanentFailure() && (entry.Status != "blocked" || entry.JobID != "" || entry.RetryAt != "") {
			entry.Status, entry.JobID, entry.RetryAt = "blocked", "", ""
			rewrite = true
		} else if entry.Status == "blocked" && entry.notFound() && !entry.permanentFailure() {
			entry.Status = "failed"
			rewrite = true
		}
		l.data[raw] = entry
	}
	if !l.cooldown.IsZero() && !l.cooldown.After(now) {
		l.cooldown = time.Time{}
		rewrite = true
	}
	if rewrite {
		if err := l.write(l.data, l.cooldown); err != nil {
			return nil, fmt.Errorf("update archive database: %w", err)
		}
	}
	return l, nil
}

func expiryAfterSixMonths(now time.Time) time.Time {
	now = now.UTC()
	month := time.Date(now.Year(), now.Month()+6, 1, now.Hour(), now.Minute(), now.Second(), 0, time.UTC)
	lastDay := month.AddDate(0, 1, -1).Day()
	return time.Date(month.Year(), month.Month(), min(now.Day(), lastDay), now.Hour(), now.Minute(), now.Second(), 0, time.UTC)
}

func entryExpired(entry Entry, now time.Time) bool {
	expiry, err := time.Parse(time.RFC3339, entry.ExpiresAt)
	return err != nil || !now.Before(expiry)
}

func transition(entry Entry, status string) Entry {
	entry.Status, entry.JobID, entry.Error, entry.ErrorMsg = status, "", "", ""
	entry.RetryAt = ""
	return entry
}

func (l *Ledger) record(base string) (Entry, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if entry, ok := l.data[base]; ok && entry.Status == "archived" {
		entry.ArchiveURL, _ = splitFragment(entry.ArchiveURL)
		return entry, true
	}
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
	entry = normalizeFailure(base, entry)
	if entry.Status == "archived" {
		entry.Error, entry.ErrorMsg, entry.RetryAt = "", "", ""
	} else if entry.permanentFailure() {
		entry.Status, entry.JobID, entry.RetryAt = "blocked", "", ""
	}
	var updated map[string]Entry
	for raw := range l.data {
		if candidate, _ := splitFragment(raw); candidate == base {
			aliases = append(aliases, raw)
		}
	}
	for _, raw := range aliases {
		value := entry
		if _, fragment := splitFragment(raw); fragment != "" && value.ArchiveURL != "" {
			value.ArchiveURL += "#" + fragment
		}
		if previous, ok := l.data[raw]; !ok || previous != value {
			if updated == nil {
				updated = maps.Clone(l.data)
			}
			updated[raw] = value
		}
	}
	if updated == nil {
		return nil
	}
	return l.write(updated, l.cooldown)
}

// write commits a replacement while the caller holds l.mu.
func (l *Ledger) write(updated map[string]Entry, cooldown time.Time) error {
	var retryAt string
	if !cooldown.IsZero() {
		retryAt = cooldown.UTC().Format(time.RFC3339Nano)
	}
	if err := writeJSON(l.path, database{Version: databaseVersion, RetryAt: retryAt, Entries: updated}); err != nil {
		return err
	}
	l.data, l.cooldown = updated, cooldown
	return nil
}

func writeJSON(path string, value any) error {
	var content bytes.Buffer
	encoder := json.NewEncoder(&content)
	encoder.SetIndent("", "  ")
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return err
	}
	if previous, err := os.ReadFile(path); err == nil && bytes.Equal(previous, content.Bytes()) {
		return nil
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".archive-links-*.tmp")
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
	if err = os.Rename(file.Name(), path); err != nil {
		return err
	}
	return nil
}

func (l *Ledger) retryAt() time.Time {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.cooldown
}

func (l *Ledger) removeExcluded() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	updated := maps.Clone(l.data)
	for raw := range updated {
		if excludedLink(raw) {
			delete(updated, raw)
		}
	}
	if len(updated) == len(l.data) {
		return nil
	}
	return l.write(updated, l.cooldown)
}

func (l *Ledger) setCooldown(until time.Time) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !until.After(l.cooldown) {
		return nil
	}
	return l.write(l.data, until)
}

func (l *Ledger) clearExpiredCooldown(now time.Time) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.cooldown.IsZero() || l.cooldown.After(now) {
		return nil
	}
	return l.write(l.data, time.Time{})
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

func (c capture) failure(entry Entry) Entry {
	entry.Error, entry.ErrorMsg = c.StatusExt, c.Message
	if entry.Error == "" && entry.ErrorMsg == "" {
		entry.ErrorMsg = "unexpected Wayback response."
	}
	return entry
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
	authenticated() bool
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
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		g.mu.Lock()
		delay := time.Until(g.next)
		if delay <= 0 {
			g.next = time.Now().Add(interval)
			g.mu.Unlock()
			return ctx.Err()
		}
		g.mu.Unlock()
		if err := wait(ctx, delay); err != nil {
			return err
		}
	}
}

func (g *paceGate) postpone(until time.Time) time.Time {
	g.mu.Lock()
	defer g.mu.Unlock()
	if until.After(g.next) {
		g.next = until
	}
	return g.next
}

type Client struct {
	httpClient      *http.Client
	availabilityURL string
	saveURL         string
	authorization   string
	authRejected    atomic.Bool
	requestInterval time.Duration
	saveInterval    time.Duration
	requestGate     paceGate
	saveGate        paceGate
	onRateLimit     func(time.Time) error
}

func (c *Client) authenticated() bool {
	return c.authorization != "" && !c.authRejected.Load()
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
	c := &Client{
		httpClient:      &http.Client{Timeout: 45 * time.Second},
		availabilityURL: "https://archive.org/wayback/available",
		saveURL:         "https://web.archive.org/save",
		requestInterval: time.Second,
		saveInterval:    21 * time.Second,
	}
	if access != "" && secret != "" {
		c.authorization = "LOW " + access + ":" + secret
		c.saveInterval = 9 * time.Second
	}
	return c, nil
}

func (c *Client) request(ctx context.Context, endpoint string, data url.Values, output any) error {
	attempts := 3
	if data != nil {
		attempts = 1 // A timed-out POST may already have started a capture.
		if !c.authenticated() {
			return &authenticationError{"set valid ARCHIVE_ORG_ACCESS_KEY and ARCHIVE_ORG_SECRET_KEY to save pages."}
		}
	}
	var last error
	for attempt := 0; attempt < attempts; {
		retry, err := c.requestOnce(ctx, endpoint, data, output)
		var limited *serviceUnavailable
		if errors.As(err, &limited) && !limited.retryAt.IsZero() {
			until := c.requestGate.postpone(limited.retryAt)
			if c.onRateLimit != nil {
				if err := c.onRateLimit(until); err != nil {
					return err
				}
			}
			// HTTP 429 rejected the request, so retrying it after the shared
			// cooldown is safe, including POSTs. Ambiguous failures still stop.
			continue
		}
		if err == nil || !retry {
			return err
		}
		last = err
		attempt++
		if attempt < attempts {
			if err := wait(ctx, time.Duration(1<<attempt)*time.Second); err != nil {
				return err
			}
		}
	}
	if data != nil {
		return last
	}
	return &serviceUnavailable{message: last.Error()}
}

// requestOnce reports whether a failed GET can safely be retried. Only an
// explicit HTTP 429 permits a POST retry; ambiguous failures are never retried.
func (c *Client) requestOnce(ctx context.Context, endpoint string, data url.Values, output any) (bool, error) {
	if err := c.pace(ctx, data != nil); err != nil {
		return false, err
	}
	method := http.MethodGet
	var body io.Reader
	if data != nil {
		method, body = http.MethodPost, strings.NewReader(data.Encode())
	}
	request, err := http.NewRequestWithContext(ctx, method, endpoint, body)
	if err != nil {
		return false, err
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("User-Agent", "blog-archive-links/1.0")
	if data != nil {
		request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		// Another worker can discover rejected credentials while this one waits.
		if !c.authenticated() {
			return false, &authenticationError{"internet archive credentials need to be updated."}
		}
	}
	if c.authorization != "" {
		request.Header.Set("Authorization", c.authorization)
	}
	response, err := c.httpClient.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		return true, fmt.Errorf("wayback connection failed: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusTooManyRequests {
		retryAt := parseRetryAfter(response.Header.Get("Retry-After"), time.Now())
		return false, &serviceUnavailable{
			message: "wayback rate limit; retry after " + retryAt.Local().Format("2006-01-02 15:04:05 MST") + ".",
			retryAt: retryAt,
		}
	}
	if (data != nil || strings.HasPrefix(endpoint, c.saveURL+"/status/")) && (response.StatusCode == 401 || response.StatusCode == 403) {
		c.authRejected.Store(true)
		return false, &authenticationError{fmt.Sprintf("wayback API returned HTTP %d; check Internet Archive credentials.", response.StatusCode)}
	}
	if response.StatusCode != http.StatusOK {
		err := fmt.Errorf("wayback returned HTTP %d", response.StatusCode)
		if response.StatusCode >= 500 && data != nil {
			return false, &serviceUnavailable{message: err.Error() + "; captures paused. Rerun later to recover the submission."}
		}
		return response.StatusCode >= 500, err
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 8<<20)).Decode(output); err != nil {
		return true, fmt.Errorf("decode Wayback response: %w", err)
	}
	return false, nil
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
	// Outlink capture is opt-in; leave it disabled for these per-page requests.
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
	ledger        *Ledger
	client        archiveService
	concurrency   int
	pollInterval  time.Duration
	retryInterval time.Duration
	output        io.Writer
	outputMu      sync.Mutex
	now           func() time.Time
}

func (r *runner) currentTime() time.Time {
	if r.now != nil {
		return r.now().UTC()
	}
	return time.Now().UTC()
}

func (r *runner) archivedEntry(archive string) Entry {
	return Entry{
		Status: "archived", ArchiveURL: archive,
		ExpiresAt: expiryAfterSixMonths(r.currentTime()).Format(time.RFC3339),
	}
}

func (r *runner) acceptsArchive(previous Entry, archive string) bool {
	if previous.RefreshStartedAt == "" {
		return true
	}
	started, err := time.Parse(time.RFC3339, previous.RefreshStartedAt)
	match := archivePattern.FindStringSubmatch(archive)
	if err != nil || len(match) != 2 {
		return false
	}
	captured, err := time.Parse("20060102150405", match[1])
	// SPN may reuse a snapshot made within the last hour. Older snapshots must
	// not silently renew an expired record.
	return err == nil && !captured.Before(started.Add(-time.Hour))
}

func (r *runner) printf(format string, args ...any) {
	r.outputMu.Lock()
	defer r.outputMu.Unlock()
	if r.output != nil {
		fmt.Fprintf(r.output, format, args...)
	}
}

func (r *runner) printRateLimit(until time.Time) {
	r.printf("wayback rate limit; waiting until %s.\n", until.Local().Format("2006-01-02 15:04:05 MST"))
}

type retryLater struct {
	entry Entry
	delay time.Duration
}

func (e *retryLater) Error() string { return e.entry.errorText() }

func (r *runner) attempt(ctx context.Context, item work) error {
	if r.retryInterval > 0 && item.entry.Status != "needs_auth" && item.entry.Status != "blocked" {
		if until, err := time.Parse(time.RFC3339Nano, item.entry.RetryAt); err == nil && until.After(r.currentTime()) {
			r.printf("%s: retry at %s\n", item.base, until.Local().Format("2006-01-02 15:04:05 MST"))
			return &retryLater{entry: item.entry, delay: until.Sub(r.currentTime())}
		}
	}
	item.entry.RetryAt = ""
	err := r.process(ctx, item)
	entry, found := r.ledger.record(item.base)
	if r.retryInterval <= 0 || ctx.Err() != nil || !found || (entry.Error == "" && entry.ErrorMsg == "") || entry.Status == "blocked" || entry.Status == "needs_auth" {
		return err
	}
	until := r.currentTime().Add(r.retryInterval)
	entry.RetryAt = until.Format(time.RFC3339Nano)
	if saveErr := r.store(item, entry); saveErr != nil {
		return errors.Join(err, saveErr)
	}
	r.printf("%s: retry at %s\n", item.base, until.Local().Format("2006-01-02 15:04:05 MST"))
	return &retryLater{entry: entry, delay: r.retryInterval}
}

func parallel(ctx context.Context, items []work, count int, fn func(context.Context, work) error) error {
	failures := make(chan error, len(items))
	slots := make(chan struct{}, count)
	var workers sync.WaitGroup
launch:
	for _, item := range items {
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			break launch
		}
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				if ctx.Err() != nil {
					<-slots
					return
				}
				err := fn(ctx, item)
				<-slots
				var retry *retryLater
				if !errors.As(err, &retry) {
					if err != nil {
						failures <- err
					}
					return
				}
				if err := wait(ctx, retry.delay); err != nil {
					failures <- err
					return
				}
				item.entry = retry.entry
				select {
				case slots <- struct{}{}:
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	workers.Wait()
	close(failures)
	var collected []error
	var longest *serviceUnavailable
	longestIndex := 0
	for err := range failures {
		collected = append(collected, err)
		var unavailable *serviceUnavailable
		if errors.As(err, &unavailable) && !unavailable.retryAt.IsZero() && (longest == nil || unavailable.retryAt.After(longest.retryAt)) {
			longest = unavailable
			longestIndex = len(collected) - 1
		}
	}
	if longest != nil {
		// The runner persists the first rate limit it finds in the joined error.
		collected[0], collected[longestIndex] = collected[longestIndex], collected[0]
	}
	return errors.Join(append(collected, ctx.Err())...)
}

func (r *runner) recover(ctx context.Context, item work, fallback Entry) error {
	fallback = normalizeFailure(item.base, fallback)
	if fallback.permanentFailure() {
		return r.store(item, fallback)
	}
	archive, err := r.client.available(ctx, item.base)
	if err != nil {
		if ctx.Err() != nil {
			return err
		}
		if fallback.ErrorMsg != "" {
			fallback.ErrorMsg += "; "
		}
		fallback.ErrorMsg += "archive lookup failed: " + err.Error()
		return r.store(item, fallback)
	}
	if archive != "" && r.acceptsArchive(item.entry, archive) {
		fallback = r.archivedEntry(archive)
	}
	return r.ledger.put(item.base, item.aliases, fallback)
}

func (r *runner) finishJob(ctx context.Context, base string, aliases []string, entry Entry) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		result, err := r.client.status(ctx, entry.JobID)
		if err != nil {
			return r.fail(work{base, aliases, entry}, "pending", err)
		}
		if result.OriginalJobID != "" && result.OriginalJobID != entry.JobID {
			entry = transition(entry, "pending")
			entry.JobID = result.OriginalJobID
			if err := r.ledger.put(base, aliases, entry); err != nil {
				return err
			}
			continue
		}
		if backpressure(result) {
			return r.store(work{base, aliases, entry}, result.failure(entry))
		}
		switch result.Status {
		case "success":
			archive := "https://web.archive.org/web/" + result.Timestamp + "/" + result.OriginalURL
			if archivePattern.MatchString(archive) && r.acceptsArchive(entry, archive) {
				return r.ledger.put(base, aliases, r.archivedEntry(archive))
			}
			entry = transition(entry, "failed")
			entry.ErrorMsg = "wayback returned an old or invalid snapshot instead of refreshing it."
			return r.ledger.put(base, aliases, entry)
		case "error":
			fallback := result.failure(transition(entry, "failed"))
			return r.recover(ctx, work{base, aliases, entry}, fallback)
		case "pending":
			if err := wait(ctx, r.pollInterval); err != nil {
				return err
			}
			continue
		}
		entry = result.failure(entry)
		entry.Status = "uncertain"
		return r.ledger.put(base, aliases, entry)
	}
}

func (r *runner) store(item work, entry Entry) error {
	return r.ledger.put(item.base, item.aliases, entry)
}

func (r *runner) fail(item work, status string, err error) error {
	if fatalServiceError(err) {
		return err
	}
	var auth *authenticationError
	if errors.As(err, &auth) {
		status = "needs_auth"
	}
	entry := item.entry
	entry.Status, entry.Error, entry.ErrorMsg = status, "", err.Error()
	return r.store(item, entry)
}

func (r *runner) process(ctx context.Context, item work) (err error) {
	defer func() {
		if err != nil {
			if ctx.Err() == nil {
				entry, found := r.ledger.record(item.base)
				if !found {
					entry.Status = "failed"
				}
				entry.Error, entry.ErrorMsg = "", err.Error()
				err = errors.Join(err, r.store(item, entry))
			}
			err = fmt.Errorf("%s: %w", item.base, err)
			return
		}
		if entry, _ := r.ledger.record(item.base); entry.errorText() != "" {
			r.printf("%s: %s: %s\n", item.base, entry.Status, entry.errorText())
		}
	}()
	entry := item.entry
	if entry.Status == "needs_auth" && !r.client.authenticated() {
		return r.store(item, entry)
	}
	lookup := false
	switch entry.Status {
	case "archived":
		if !entryExpired(entry, r.currentTime()) {
			return r.store(item, entry)
		}
		entry = transition(entry, "ready")
		entry.RefreshStartedAt = r.currentTime().Format(time.RFC3339)
	case "needs_auth":
		if entry.JobID != "" {
			entry.Status, entry.Error, entry.ErrorMsg = "pending", "", ""
		} else {
			entry = transition(entry, "ready")
			if entry.ArchiveURL != "" {
				entry.RefreshStartedAt = r.currentTime().Format(time.RFC3339)
			}
		}
	case "ready", "pending":
	default:
		lookup = true
	}
	if entry.ArchiveURL != "" && entryExpired(entry, r.currentTime()) &&
		(entry.RefreshStartedAt == "" || entry.Status == "failed") {
		entry.RefreshStartedAt = r.currentTime().Format(time.RFC3339)
	}
	item.entry = entry
	if lookup {
		r.printf("%s: checking\n", item.base)
		archive, err := r.client.available(ctx, item.base)
		if err != nil {
			return r.fail(item, "failed", err)
		}
		if archive != "" && r.acceptsArchive(entry, archive) {
			r.printf("%s: found: %s\n", item.base, archive)
			return r.store(item, r.archivedEntry(archive))
		}
		entry = transition(entry, "ready")
		if entry.ArchiveURL != "" {
			entry.RefreshStartedAt = r.currentTime().Format(time.RFC3339)
		}
		item.entry = entry
	}
	if entry.Status != "pending" {
		if !r.client.authenticated() {
			return r.fail(item, "needs_auth", &authenticationError{"internet archive credentials are required to save this page."})
		}
		item.entry = transition(entry, "submitting")
		if err := r.store(item, item.entry); err != nil {
			return err
		}
		r.printf("%s: saving\n", item.base)
		result, err := r.client.save(ctx, item.base)
		if err != nil {
			return r.fail(item, "uncertain", err)
		}
		if backpressure(result) {
			return r.store(item, result.failure(transition(item.entry, "failed")))
		}
		if result.JobID == "" {
			entry = result.failure(transition(item.entry, "failed"))
			return r.recover(ctx, item, entry)
		}
		entry = transition(item.entry, "pending")
		entry.JobID = result.JobID
	} else {
		r.printf("%s: saving\n", item.base)
	}
	if err := r.store(item, entry); err != nil {
		return err
	}
	if err := r.finishJob(ctx, item.base, item.aliases, entry); err != nil {
		return err
	}
	entry, _ = r.ledger.record(item.base)
	if entry.errorText() == "" {
		r.printf("%s: %s\n", item.base, entry.Status)
	}
	return nil
}

func (r *runner) run(ctx context.Context, urls []string) (runError error) {
	if err := r.ledger.removeExcluded(); err != nil {
		return err
	}
	groups := make(map[string][]string)
	for _, raw := range urls {
		if !excludedLink(raw) {
			base, _ := splitFragment(raw)
			groups[base] = append(groups[base], raw)
		}
	}
	urls = nil
	var items []work
	for _, base := range sortedKeys(groups) {
		entry, _ := r.ledger.record(base)
		if entry.Status == "blocked" {
			delete(groups, base)
			continue
		}
		items = append(items, work{base, groups[base], entry})
		urls = append(urls, groups[base]...)
	}
	defer func() {
		var unavailable *serviceUnavailable
		if errors.As(runError, &unavailable) && !unavailable.retryAt.IsZero() {
			if err := r.ledger.setCooldown(unavailable.retryAt); err != nil {
				runError = errors.Join(runError, fmt.Errorf("persist rate-limit cooldown: %w", err))
			}
		}
		runError = errors.Join(runError, r.ledger.clearExpiredCooldown(r.currentTime()))
	}()
	if client, ok := r.client.(*Client); ok {
		previous := client.onRateLimit
		client.onRateLimit = func(until time.Time) error {
			if err := r.ledger.setCooldown(until); err != nil {
				return err
			}
			r.printRateLimit(until)
			if previous != nil {
				return previous(until)
			}
			return nil
		}
		defer func() { client.onRateLimit = previous }()
	}
	if retryAt := r.ledger.retryAt(); retryAt.After(r.currentTime()) {
		r.printRateLimit(retryAt)
		if err := wait(ctx, retryAt.Sub(r.currentTime())); err != nil {
			return err
		}
	}
	if err := r.ledger.clearExpiredCooldown(r.currentTime()); err != nil {
		return err
	}
	// Resume already-submitted jobs before starting new work.
	sort.SliceStable(items, func(i, j int) bool {
		return items[i].entry.JobID != "" && items[j].entry.JobID == ""
	})
	count := r.concurrency
	if count <= 0 {
		count = 3
	}
	runError = parallel(ctx, items, count, r.attempt)
	if ctx.Err() != nil {
		return runError
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
		if counts["needs_auth"] > 0 {
			return errors.Join(runError, fmt.Errorf("%w; valid Internet Archive credentials are required", errIncomplete))
		}
		return errors.Join(runError, errIncomplete)
	}
	return runError
}

func main() {
	flag.Usage = func() {
		fmt.Fprintln(flag.CommandLine.Output(), "usage: xoutlinks [options]")
		flag.PrintDefaults()
	}
	defaultRoot := "."
	if output, err := exec.Command("git", "rev-parse", "--show-toplevel").Output(); err == nil {
		defaultRoot = strings.TrimSpace(string(output))
	}
	root := flag.String("root", defaultRoot, "blog repository directory")
	concurrency := flag.Int("concurrency", 3, "maximum simultaneous capture jobs")
	retryInterval := flag.Duration("retry-interval", 4*time.Minute, "delay before retrying failed captures")
	credentials := flag.String("credentials", "", "private JSON file containing access_key and secret_key")
	explain := flag.String("explain", "", "explain whether a URL is excluded without making archive requests")
	flag.Parse()
	if *explain != "" {
		if reason := exclusionReason(*explain); reason != "" {
			fmt.Printf("%s: excluded: %s\n", *explain, reason)
		} else {
			fmt.Printf("%s: eligible: no exclusion rule matched\n", *explain)
		}
		return
	}
	if *concurrency < 1 {
		fmt.Fprintln(os.Stderr, "--concurrency must be positive")
		os.Exit(1)
	}
	if *retryInterval <= 0 {
		fmt.Fprintln(os.Stderr, "--retry-interval must be positive")
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
		urls, excluded := filterLinks(urls)
		if err := writeJSON(filepath.Join(*root, "xoutlinks", "dbx.json"), excluded); err != nil {
			return fmt.Errorf("write excluded links: %w", err)
		}
		ledger, err := loadLedger(filepath.Join(*root, "xoutlinks", "db.json"))
		if err != nil {
			return err
		}
		client, err := newClient(*credentials)
		if err != nil {
			return err
		}
		r := runner{ledger: ledger, client: client, concurrency: *concurrency, pollInterval: 10 * time.Second, retryInterval: *retryInterval, output: os.Stdout}
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
