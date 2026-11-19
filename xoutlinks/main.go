// Save external blog links as self-contained HTML using SingleFile and Chrome.
// Run from the blog root: ./xoutlinks/run.sh
// The first argument selects the output directory; xoutlinks/db.json records progress.
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
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/net/html"
)

const databaseVersion = 2
const singleFileVersion = "2.27.3"

var errIncomplete = errors.New("some links are not saved; see xoutlinks/db.json")

type database struct {
	Version int              `json:"version"`
	Entries map[string]Entry `json:"entries"`
}

type Entry struct {
	Status         string `json:"status"`
	File           string `json:"file,omitempty"`
	SavedAt        string `json:"saved_at,omitempty"`
	PendingFile    string `json:"pending_file,omitempty"`
	ArchiveURL     string `json:"archive_url,omitempty"` // Retain previously collected Wayback URLs.
	Error          string `json:"error,omitempty"`
	ErrorMsg       string `json:"error_msg,omitempty"`
	RetryAt        string `json:"retry_at,omitempty"`
	ForceRearchive bool   `json:"force_rearchive,omitempty"`
}

func (e Entry) errorText() string {
	if e.Error == "" {
		return e.ErrorMsg
	}
	if e.ErrorMsg == "" || e.Error == e.ErrorMsg {
		return e.Error
	}
	return e.Error + ": " + e.ErrorMsg
}

type Ledger struct {
	mu       sync.Mutex
	path     string
	data     map[string]Entry
	excluded []string
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

func collectLinks(root, backups string) ([]string, error) {
	command := exec.Command("git", "ls-files", "-z", "--cached", "--others", "--exclude-standard", "--", "*.html")
	command.Dir = root
	files, err := command.Output()
	if err != nil {
		return nil, fmt.Errorf("list blog HTML files: %w", err)
	}
	seen := make(map[string]bool)
	for _, name := range strings.Split(string(files), "\x00") {
		// Never crawl downloaded pages, even if one was accidentally added to Git.
		if name == "" || strings.HasPrefix(filepath.ToSlash(name), backups+"/") {
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
		return nil, fmt.Errorf("another xoutlinks process is already running: %w", err)
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
	var stored database
	if err := json.Unmarshal(content, &stored); err != nil || stored.Entries == nil {
		return nil, fmt.Errorf("invalid backup database %s", path)
	}
	if stored.Version != 1 && stored.Version != databaseVersion {
		return nil, fmt.Errorf("unsupported backup database version %d", stored.Version)
	}
	rewrite := stored.Version != databaseVersion
	for raw, entry := range stored.Entries {
		if stored.Version == 1 {
			// Wayback success, blocking, jobs and cooldowns say nothing about a local capture.
			entry = Entry{Status: "ready", ArchiveURL: entry.ArchiveURL, ForceRearchive: entry.ForceRearchive}
		}
		switch entry.Status {
		case "ready", "saving", "saved", "failed", "blocked":
		default:
			return nil, fmt.Errorf("invalid backup status for %s: %s", raw, entry.Status)
		}
		for _, timestamp := range []string{entry.RetryAt, entry.SavedAt} {
			if timestamp != "" {
				if _, err := time.Parse(time.RFC3339Nano, timestamp); err != nil {
					return nil, fmt.Errorf("invalid backup timestamp for %s", raw)
				}
			}
		}
		for _, name := range []string{entry.File, entry.PendingFile} {
			if name != "" && (!filepath.IsLocal(name) || !strings.HasSuffix(name, ".html")) {
				return nil, fmt.Errorf("invalid backup filename for %s", raw)
			}
		}
		previous := entry
		if entry.Status == "saved" {
			entry.Error, entry.ErrorMsg, entry.RetryAt = "", "", ""
		} else if !entry.ForceRearchive {
			entry = normalizeFailure(raw, entry)
			if entry.permanentFailure() {
				entry.Status, entry.PendingFile, entry.RetryAt = "blocked", "", ""
			}
		}
		if until, err := time.Parse(time.RFC3339Nano, entry.RetryAt); err == nil && !until.After(time.Now()) {
			entry.RetryAt = ""
		}
		rewrite = rewrite || entry != previous
		l.data[raw] = entry
	}
	if rewrite {
		if err := l.write(l.data); err != nil {
			return nil, err
		}
	}
	return l, nil
}

func (l *Ledger) record(base string) Entry {
	l.mu.Lock()
	defer l.mu.Unlock()
	var result Entry
	best := -1
	for _, raw := range sortedKeys(l.data) {
		if candidate, _ := splitFragment(raw); candidate != base {
			continue
		}
		entry := l.data[raw]
		score := 0
		if raw == base {
			score++
		}
		if entry.Status == "saved" {
			score += 2
		}
		if entry.PendingFile != "" {
			score += 4
		}
		if entry.ForceRearchive {
			score += 8
		}
		if score > best {
			result, best = entry, score
		}
	}
	result.ArchiveURL, _ = splitFragment(result.ArchiveURL)
	return result
}

func (l *Ledger) put(base string, aliases []string, entry Entry) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	updated := maps.Clone(l.data)
	for raw := range l.data {
		if candidate, _ := splitFragment(raw); candidate == base {
			aliases = append(aliases, raw)
		}
	}
	for _, raw := range aliases {
		value := entry
		// Preserve each original Wayback URL independently of the local backup.
		if previous := l.data[raw]; previous.ArchiveURL != "" {
			value.ArchiveURL = previous.ArchiveURL
		}
		updated[raw] = value
	}
	if maps.Equal(l.data, updated) {
		return l.writeExcluded()
	}
	return l.write(updated)
}

// write commits a replacement while the caller holds l.mu.
func (l *Ledger) write(updated map[string]Entry) error {
	if err := writeJSON(l.path, database{Version: databaseVersion, Entries: updated}); err != nil {
		return err
	}
	l.data = updated
	return l.writeExcluded()
}

func (l *Ledger) setExcluded(links []string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.excluded = append([]string{}, links...)
	return l.writeExcluded()
}

// writeExcluded is called with l.mu held after the primary database is saved.
func (l *Ledger) writeExcluded() error {
	if l.excluded == nil {
		return nil
	}
	links := make(map[string]bool)
	for _, raw := range l.excluded {
		links[raw] = true
	}
	for raw, entry := range l.data {
		if entry.Status == "blocked" {
			links[raw] = true
		}
	}
	if err := writeJSON(filepath.Join(filepath.Dir(l.path), "dbx.json"), sortedKeys(links)); err != nil {
		return fmt.Errorf("write excluded links: %w", err)
	}
	return nil
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
	return l.write(updated)
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
	file, err := os.OpenFile(path+".tmp", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
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
	return os.Rename(file.Name(), path)
}

type captureError struct {
	code    string
	message string
	retryAt time.Time
}

func (e *captureError) Error() string { return e.message }

type pageSaver interface {
	prepare(context.Context) error
	save(context.Context, string, string) error
}

type singleFile struct {
	binary string
	chrome string
	cache  string
	output io.Writer
}

func (s *singleFile) prepare(ctx context.Context) error {
	if s.chrome == "" {
		home, _ := os.UserHomeDir()
		for _, name := range []string{
			"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
			filepath.Join(home, "Applications/Google Chrome.app/Contents/MacOS/Google Chrome"),
			"google-chrome", "chromium", "chromium-browser",
		} {
			if path, err := exec.LookPath(name); err == nil {
				s.chrome = path
				break
			}
		}
	}
	if path, err := exec.LookPath(s.chrome); err == nil {
		s.chrome = path
	} else {
		return errors.New("chrome was not found; set --chrome to its executable path")
	}
	if s.binary == "" {
		s.binary, _ = exec.LookPath("single-file")
	}
	if s.binary != "" {
		path, err := exec.LookPath(s.binary)
		if err != nil {
			return fmt.Errorf("find singlefile executable: %w", err)
		}
		s.binary = path
		return nil
	}
	return s.install(ctx)
}

func (s *singleFile) install(ctx context.Context) error {
	// Pin and verify the standalone executable; no Node, Deno or browser download.
	releases := map[string]struct{ name, sha256 string }{
		"darwin/arm64": {"single-file-aarch64-apple-darwin", "bca0eccc5551f4358bc8c7e584bbdb13d073c6d335412d3a6bdb244a94c7027b"},
		"darwin/amd64": {"single-file-x86_64-apple-darwin", "19f4b794554ab36ba5bbc91747f33e000a79e309c3ad9b245dcd3d186395f041"},
		"linux/arm64":  {"single-file-aarch64-linux", "ce5f493ee42102b41134fa76f8c3bcf0c2f6214f99654b754d7427a5e4da4fb9"},
		"linux/amd64":  {"single-file-x86_64-linux", "3e956a47097af7c3da2ffc063a064462651c4486241488250e3296ed6e381ab2"},
	}
	release, ok := releases[runtime.GOOS+"/"+runtime.GOARCH]
	if !ok {
		return errors.New("no bundled singlefile release for this platform; set --single-file")
	}
	name := filepath.Join(s.cache, "single-file-"+singleFileVersion+"-"+runtime.GOOS+"-"+runtime.GOARCH)
	if _, err := exec.LookPath(name); err == nil {
		s.binary = name
		return nil
	}
	if err := os.MkdirAll(s.cache, 0755); err != nil {
		return err
	}
	if s.output != nil {
		fmt.Fprintf(s.output, "installing singlefile %s\n", singleFileVersion)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"https://github.com/gildas-lormeau/single-file-cli/releases/download/v"+singleFileVersion+"/"+release.name, nil)
	if err != nil {
		return err
	}
	response, err := (&http.Client{Timeout: 5 * time.Minute}).Do(request)
	if err != nil {
		return fmt.Errorf("download singlefile: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("download singlefile: HTTP %d", response.StatusCode)
	}
	file, err := os.CreateTemp(s.cache, ".download-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(io.MultiWriter(file, hash), response.Body); err != nil {
		return err
	}
	if fmt.Sprintf("%x", hash.Sum(nil)) != release.sha256 {
		return errors.New("singlefile download checksum mismatch")
	}
	if err := file.Chmod(0755); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(file.Name(), name); err != nil {
		return err
	}
	s.binary = name
	return nil
}

func (s *singleFile) save(ctx context.Context, raw, destination string) error {
	partial := destination + ".part.html"
	defer os.Remove(partial)
	command := exec.CommandContext(ctx, s.binary, raw, partial,
		"--browser-executable-path="+s.chrome,
		"--crawl-links=false", "--compress-content=false", "--block-scripts=true",
		"--browser-wait-until-fallback=false", "--filename-conflict-action=overwrite", "--dump-json=true")
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.Cancel = func() error {
		err := syscall.Kill(-command.Process.Pid, syscall.SIGTERM)
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
	command.WaitDelay = 5 * time.Second
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	err := command.Run()
	if command.Process != nil {
		// Also clean up Chrome descendants after cancellation or a CLI crash.
		_ = syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err != nil {
		message := strings.TrimSpace(stderr.String())
		if message == "" {
			message = err.Error()
		}
		return &captureError{code: "error:capture", message: message}
	}
	var result struct {
		Response struct {
			Status     int    `json:"status"`
			StatusText string `json:"statusText"`
			Headers    []struct {
				Name  string `json:"name"`
				Value string `json:"value"`
			} `json:"headers"`
		} `json:"response"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		return &captureError{code: "error:capture", message: "invalid singlefile response: " + err.Error()}
	}
	status := result.Response.Status
	if status < 200 || status >= 300 {
		failure := &captureError{code: fmt.Sprintf("error:http-%d", status),
			message: fmt.Sprintf("HTTP status=%d %s", status, result.Response.StatusText)}
		if status == http.StatusTooManyRequests {
			for _, header := range result.Response.Headers {
				if strings.EqualFold(header.Name, "Retry-After") {
					failure.retryAt = parseRetryAfter(header.Value, time.Now())
				}
			}
		}
		return failure
	}
	file, err := os.OpenFile(partial, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("open captured page: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if info.Size() == 0 {
		return &captureError{code: "error:capture", message: "singlefile produced an empty page"}
	}
	if err := file.Chmod(0644); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(partial, destination)
}

func parseRetryAfter(value string, now time.Time) time.Time {
	if seconds, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64); err == nil && seconds >= 0 && seconds <= int64((1<<63-1)/time.Second) {
		return now.Add(time.Duration(seconds) * time.Second)
	}
	date, _ := http.ParseTime(value)
	return date
}

type work struct {
	base    string
	aliases []string
	entry   Entry
}

type runner struct {
	ledger        *Ledger
	saver         pageSaver
	root          string
	backups       string
	concurrency   int
	retryInterval time.Duration
	output        io.Writer
	outputMu      sync.Mutex
}

func (r *runner) printf(format string, args ...any) {
	r.outputMu.Lock()
	defer r.outputMu.Unlock()
	if r.output != nil {
		fmt.Fprintf(r.output, format, args...)
	}
}

func errorLine(base string, entry Entry) string {
	if entry.errorText() == "" {
		return ""
	}
	prefix := fmt.Sprintf("%s: %s: ", base, entry.Status)
	message := strings.ReplaceAll(entry.errorText(), "\r\n", "\n")
	return prefix + strings.ReplaceAll(message, "\n", "\n"+prefix) + "\n"
}

func (r *runner) printRetry(base string, entry Entry, until time.Time) {
	r.printf("%s%s: retry at %s\n", errorLine(base, entry), base, until.Local().Format("2006-01-02 15:04:05 MST"))
}

func (r *runner) complete(item work, entry Entry, name string) error {
	info, err := os.Stat(filepath.Join(r.root, name))
	if err != nil {
		return err
	}
	if entry.File != name || entry.SavedAt == "" {
		entry.SavedAt = info.ModTime().UTC().Format(time.RFC3339Nano)
	}
	entry.Status, entry.File = "saved", name
	entry.PendingFile, entry.Error, entry.ErrorMsg, entry.RetryAt = "", "", "", ""
	entry.ForceRearchive = false
	return r.ledger.put(item.base, item.aliases, entry)
}

func (r *runner) exists(name string) (bool, error) {
	if name == "" {
		return false, nil
	}
	info, err := os.Stat(filepath.Join(r.root, name))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return info.Mode().IsRegular() && info.Size() > 0, nil
}

type retryLater struct {
	entry Entry
	delay time.Duration
}

func (e *retryLater) Error() string { return e.entry.errorText() }

func (r *runner) attempt(ctx context.Context, item work) error {
	entry := item.entry
	if until, err := time.Parse(time.RFC3339Nano, entry.RetryAt); err == nil && until.After(time.Now()) &&
		(!entry.ForceRearchive || entry.PendingFile != "") {
		r.printRetry(item.base, entry, until)
		return &retryLater{entry, time.Until(until)}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if entry.PendingFile == "" {
		sum := sha256.Sum256([]byte(item.base))
		parsed, _ := url.Parse(item.base)
		host := strings.ReplaceAll(parsed.Hostname(), ":", "-")
		if len(host) > 80 {
			host = host[:80]
		}
		name := fmt.Sprintf("%s-%x-%s.html", host, sum[:8], time.Now().UTC().Format("20060102T150405.000000000Z"))
		entry.PendingFile = filepath.ToSlash(filepath.Join(r.backups, name))
	}
	entry.Status, entry.Error, entry.ErrorMsg, entry.RetryAt = "saving", "", "", ""
	if err := r.ledger.put(item.base, item.aliases, entry); err != nil {
		return err
	}
	r.printf("%s: saving\n", item.base)
	err := r.saver.save(ctx, item.base, filepath.Join(r.root, entry.PendingFile))
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err == nil {
		if err := r.complete(item, entry, entry.PendingFile); err != nil {
			return err
		}
		r.printf("%s: saved\n", item.base)
		return nil
	}
	entry.Status, entry.Error, entry.ErrorMsg = "failed", "error:capture", err.Error()
	var failure *captureError
	if errors.As(err, &failure) {
		entry.Error = failure.code
	}
	entry = normalizeFailure(item.base, entry)
	if entry.permanentFailure() {
		entry.Status, entry.PendingFile, entry.ForceRearchive = "blocked", "", false
	} else if r.retryInterval > 0 {
		until := time.Now().Add(r.retryInterval)
		if failure != nil && failure.retryAt.After(until) {
			until = failure.retryAt
		}
		entry.RetryAt = until.UTC().Format(time.RFC3339Nano)
	}
	if err := r.ledger.put(item.base, item.aliases, entry); err != nil {
		return err
	}
	if entry.RetryAt != "" {
		until, _ := time.Parse(time.RFC3339Nano, entry.RetryAt)
		r.printRetry(item.base, entry, until)
		return &retryLater{entry, time.Until(until)}
	}
	r.printf("%s", errorLine(item.base, entry))
	return nil
}

func wait(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(max(delay, 0))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ctx.Err()
	}
}

func parallel(ctx context.Context, items []work, count int, fn func(context.Context, work) error) error {
	failures := make(chan error, len(items))
	report := func(item work, err error) {
		if err != nil {
			failures <- fmt.Errorf("%s: %w", item.base, err)
		}
	}
	slots := make(chan struct{}, count)
	var workers sync.WaitGroup
launch:
	for i, item := range items {
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			for _, queued := range items[i:] {
				report(queued, ctx.Err())
			}
			break launch
		}
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				if ctx.Err() != nil {
					<-slots
					report(item, ctx.Err())
					return
				}
				err := fn(ctx, item)
				<-slots
				var retry *retryLater
				if !errors.As(err, &retry) {
					report(item, err)
					return
				}
				if err := wait(ctx, retry.delay); err != nil {
					report(item, err)
					return
				}
				item.entry = retry.entry
				select {
				case slots <- struct{}{}:
				case <-ctx.Done():
					report(item, ctx.Err())
					return
				}
			}
		}()
	}
	workers.Wait()
	close(failures)
	var collected []error
	for err := range failures {
		collected = append(collected, err)
	}
	return errors.Join(collected...)
}

func (r *runner) run(ctx context.Context, urls []string) error {
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
	var items []work
	for _, base := range sortedKeys(groups) {
		item := work{base, groups[base], r.ledger.record(base)}
		entry := item.entry
		if entry.Status == "blocked" && !entry.ForceRearchive {
			continue
		}
		name := entry.PendingFile
		if name == "" && !entry.ForceRearchive {
			name = entry.File
		}
		exists, err := r.exists(name)
		if err != nil {
			return err
		}
		if exists {
			if err := r.complete(item, entry, name); err != nil {
				return err
			}
			continue
		}
		items = append(items, item)
	}
	if len(items) > 0 {
		if err := r.saver.prepare(ctx); err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Join(r.root, r.backups), 0755); err != nil {
			return err
		}
	}
	runError := parallel(ctx, items, max(r.concurrency, 1), r.attempt)
	if ctx.Err() != nil {
		return runError
	}
	counts := make(map[string]int)
	for base := range groups {
		counts[r.ledger.record(base).Status]++
	}
	var summary []string
	for _, status := range sortedKeys(counts) {
		summary = append(summary, fmt.Sprintf("%d %s", counts[status], status))
	}
	r.printf("%d distinct pages: %s\n", len(groups), strings.Join(summary, ", "))
	if counts["saved"]+counts["blocked"] != len(groups) {
		return errors.Join(runError, errIncomplete)
	}
	return runError
}

func backupDirectory(root, directory string) (string, error) {
	absolute, err := filepath.Abs(directory)
	if err != nil {
		return "", err
	}
	relative, err := filepath.Rel(root, absolute)
	if err != nil || relative == "." || !filepath.IsLocal(relative) {
		return "", errors.New("backup directory must be inside the blog repository")
	}
	return filepath.ToSlash(relative), nil
}

func main() {
	flag.Usage = func() {
		fmt.Fprintln(flag.CommandLine.Output(), "usage: xoutlinks <backup-directory> [options]")
		flag.PrintDefaults()
	}
	defaultRoot := "."
	if output, err := exec.Command("git", "rev-parse", "--show-toplevel").Output(); err == nil {
		defaultRoot = strings.TrimSpace(string(output))
	}
	root := flag.String("root", defaultRoot, "blog repository directory")
	concurrency := flag.Int("concurrency", 3, "maximum simultaneous page saves")
	retryInterval := flag.Duration("retry-interval", 4*time.Minute, "delay before retrying failed saves")
	binary := flag.String("single-file", os.Getenv("SINGLE_FILE_BIN"), "SingleFile executable (default: PATH or a cached standalone release)")
	chrome := flag.String("chrome", os.Getenv("CHROME_BIN"), "installed Chrome executable (default: autodetect)")
	args := os.Args[1:]
	var out string
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		out, args = args[0], args[1:]
	}
	flag.CommandLine.Parse(args)
	if out == "" {
		fmt.Fprintln(os.Stderr, "backup directory is required as the first argument")
		flag.Usage()
		os.Exit(1)
	}
	if flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "unexpected positional arguments")
		os.Exit(1)
	}
	if *concurrency < 1 || *retryInterval <= 0 {
		fmt.Fprintln(os.Stderr, "--concurrency and --retry-interval must be positive")
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	err := func() error {
		absolute, err := filepath.Abs(*root)
		if err != nil {
			return err
		}
		directory, err := backupDirectory(absolute, out)
		if err != nil {
			return err
		}
		unlock, err := lockRepo(absolute)
		if err != nil {
			return err
		}
		defer unlock()
		urls, err := collectLinks(absolute, directory)
		if err != nil {
			return err
		}
		urls, excluded := filterLinks(urls)
		ledger, err := loadLedger(filepath.Join(absolute, "xoutlinks", "db.json"))
		if err != nil {
			return err
		}
		if err := ledger.setExcluded(excluded); err != nil {
			return err
		}
		saver := &singleFile{binary: *binary, chrome: *chrome, cache: filepath.Join(absolute, "xoutlinks", ".cache"), output: os.Stdout}
		r := runner{ledger: ledger, saver: saver, root: absolute, backups: directory, concurrency: *concurrency, retryInterval: *retryInterval, output: os.Stdout}
		return r.run(ctx, urls)
	}()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		if errors.Is(err, context.Canceled) || ctx.Err() != nil {
			os.Exit(130)
		}
		os.Exit(1)
	}
}
