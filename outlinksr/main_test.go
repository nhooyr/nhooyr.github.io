package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const testTimestamp = "20261118123456"

func testSnapshot(url string) string {
	return "https://web.archive.org/web/" + testTimestamp + "/" + url
}

type fakeArchiveService struct {
	mu             sync.Mutex
	availableCalls []string
	saveCalls      []string
	statusCalls    []string
	availableFn    func(context.Context, string) (string, error)
	saveFn         func(context.Context, string) (capture, error)
	statusFn       func(context.Context, string) (capture, error)
}

func (f *fakeArchiveService) available(ctx context.Context, url string) (string, error) {
	f.mu.Lock()
	f.availableCalls = append(f.availableCalls, url)
	f.mu.Unlock()
	if f.availableFn != nil {
		return f.availableFn(ctx, url)
	}
	return "", nil
}

func (f *fakeArchiveService) save(ctx context.Context, url string) (capture, error) {
	f.mu.Lock()
	f.saveCalls = append(f.saveCalls, url)
	f.mu.Unlock()
	if f.saveFn != nil {
		return f.saveFn(ctx, url)
	}
	return capture{JobID: "job:" + url}, nil
}

func (f *fakeArchiveService) status(ctx context.Context, job string) (capture, error) {
	f.mu.Lock()
	f.statusCalls = append(f.statusCalls, job)
	f.mu.Unlock()
	if f.statusFn != nil {
		return f.statusFn(ctx, job)
	}
	return capture{
		Status: "success", Timestamp: testTimestamp,
		OriginalURL: strings.TrimPrefix(job, "job:"),
	}, nil
}

func (f *fakeArchiveService) counts() [3]int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return [3]int{len(f.availableCalls), len(f.saveCalls), len(f.statusCalls)}
}

func newTestLedger(t *testing.T) (*Ledger, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "db.json")
	ledger, err := loadLedger(path)
	if err != nil {
		t.Fatal(err)
	}
	return ledger, path
}

func testRun(t *testing.T, ledger *Ledger, client archiveService, urls []string) error {
	t.Helper()
	r := runner{ledger: ledger, client: client, concurrency: 3}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return r.run(ctx, urls)
}

func testReadEntries(t *testing.T, path string) map[string]Entry {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var entries map[string]Entry
	if err := json.Unmarshal(data, &entries); err != nil {
		t.Fatal(err)
	}
	return entries
}

func TestRepeatRunMakesNoRequestsOrFileChanges(t *testing.T) {
	ledger, path := newTestLedger(t)
	url := "https://example.com/page"
	client := &fakeArchiveService{}
	if err := testRun(t, ledger, client, []string{url}); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	counts := client.counts()
	if err := testRun(t, ledger, client, []string{url}); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if counts != client.counts() || string(before) != string(after) {
		t.Fatalf("Repeat run did work: calls %v -> %v, changed bytes: %v", counts, client.counts(), string(before) != string(after))
	}
}

func TestOnlyNewLinksDoWork(t *testing.T) {
	ledger, _ := newTestLedger(t)
	old, newURL := "https://example.com/old", "https://example.com/new?meaningful=value"
	client := &fakeArchiveService{}
	if err := testRun(t, ledger, client, []string{old}); err != nil {
		t.Fatal(err)
	}
	if err := testRun(t, ledger, client, []string{old, newURL}); err != nil {
		t.Fatal(err)
	}
	if got := client.counts(); got != [3]int{2, 2, 2} {
		t.Fatalf("Old link was processed again: %v", got)
	}
	if !reflect.DeepEqual(client.saveCalls, []string{old, newURL}) {
		t.Fatalf("Unexpected saves: %v", client.saveCalls)
	}
}

func TestFragmentsShareCaptureAndNewFragmentReusesIt(t *testing.T) {
	ledger, path := newTestLedger(t)
	base := "https://example.com/page?q=a%2Bb&x=kept"
	urls := []string{base + "#one", base + "#two"}
	client := &fakeArchiveService{}
	if err := testRun(t, ledger, client, urls); err != nil {
		t.Fatal(err)
	}
	if got := client.counts(); got != [3]int{1, 1, 1} {
		t.Fatalf("Fragment aliases did not share a capture: %v", got)
	}
	urls = append(urls, base+"#three")
	if err := testRun(t, ledger, client, urls); err != nil {
		t.Fatal(err)
	}
	if got := client.counts(); got != [3]int{1, 1, 1} {
		t.Fatalf("New fragment performed unnecessary network work: %v", got)
	}
	entries := testReadEntries(t, path)
	for _, url := range urls {
		if got := entries[url]; got.Status != "archived" || got.ArchiveURL != testSnapshot(url) {
			t.Errorf("Incorrect fragment result for %s: %+v", url, got)
		}
	}
}

func TestHTMLExtractionPreservesQueryAndDecodesEntities(t *testing.T) {
	content := `<a href="https://example.com/search?q=a%2Bb&amp;next=https%3A%2F%2Fexample.net#results">one</a>
<a href='https://example.com/search?q=a%2Bb&amp;next=https%3A%2F%2Fexample.net#results'>duplicate</a>
<a href=" https://example.org/other ">other</a>
<a href="/local">local</a><a href="mailto:test@example.com">mail</a>
<link href="https://example.com/style.css" rel="stylesheet">`
	got := extractLinks(content)
	sort.Strings(got)
	want := []string{
		"https://example.com/search?q=a%2Bb&next=https%3A%2F%2Fexample.net#results",
		"https://example.org/other",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Unexpected extracted URLs: %v", got)
	}
}

func TestExistingSnapshotSkipsSaveAndStatus(t *testing.T) {
	ledger, path := newTestLedger(t)
	url := "https://example.com/page"
	client := &fakeArchiveService{availableFn: func(_ context.Context, url string) (string, error) {
		return testSnapshot(url), nil
	}}
	if err := testRun(t, ledger, client, []string{url}); err != nil {
		t.Fatal(err)
	}
	if got := client.counts(); got != [3]int{1, 0, 0} {
		t.Fatalf("Existing snapshot caused unnecessary work: %v", got)
	}
	if got := testReadEntries(t, path)[url]; got.ArchiveURL != testSnapshot(url) {
		t.Fatalf("Snapshot was not persisted: %+v", got)
	}
}

func TestWaybackLinksNeedNoNetwork(t *testing.T) {
	ledger, path := newTestLedger(t)
	url := testSnapshot("https://example.com/page#section")
	client := &fakeArchiveService{}
	if err := testRun(t, ledger, client, []string{url}); err != nil {
		t.Fatal(err)
	}
	if got := client.counts(); got != [3]int{} {
		t.Fatalf("Tried archiving an archive URL: %v", got)
	}
	if got := testReadEntries(t, path)[url]; got.ArchiveURL != url {
		t.Fatalf("Incorrect archive result: %+v", got)
	}
}

func TestPendingJobResumesWithoutAnotherPost(t *testing.T) {
	ledger, _ := newTestLedger(t)
	url := "https://example.com/page"
	if err := ledger.put(url, []string{url}, Entry{Status: "pending", JobID: "job:" + url}); err != nil {
		t.Fatal(err)
	}
	client := &fakeArchiveService{}
	if err := testRun(t, ledger, client, []string{url}); err != nil {
		t.Fatal(err)
	}
	if got := client.counts(); got != [3]int{0, 0, 1} {
		t.Fatalf("Pending capture was submitted again: %v", got)
	}
}

func TestReadyCheckpointSkipsAvailability(t *testing.T) {
	ledger, _ := newTestLedger(t)
	url := "https://example.com/page"
	if err := ledger.put(url, []string{url}, Entry{Status: "ready"}); err != nil {
		t.Fatal(err)
	}
	client := &fakeArchiveService{}
	if err := testRun(t, ledger, client, []string{url}); err != nil {
		t.Fatal(err)
	}
	if got := client.counts(); got != [3]int{0, 1, 1} {
		t.Fatalf("Ready capture performed an unnecessary lookup: %v", got)
	}
}

func TestCanceledSubmissionIsNotPostedAgain(t *testing.T) {
	ledger, path := newTestLedger(t)
	url := "https://example.com/page"
	ctx, cancel := context.WithCancel(context.Background())
	client := &fakeArchiveService{saveFn: func(_ context.Context, _ string) (capture, error) {
		entry := testReadEntries(t, path)[url]
		if entry.Status != "submitting" {
			t.Errorf("Save started before its checkpoint was persisted: %+v", entry)
		}
		cancel()
		return capture{}, context.Canceled
	}}
	r := runner{ledger: ledger, client: client, concurrency: 3}
	if err := r.run(ctx, []string{url}); err == nil {
		t.Fatal("Expected canceled submission error")
	}
	resumed := &fakeArchiveService{}
	if err := testRun(t, ledger, resumed, []string{url}); !errors.Is(err, errIncomplete) {
		t.Fatalf("Expected an incomplete archive after interrupted submission: %v", err)
	}
	if got := resumed.counts(); got[1] != 0 || got[2] != 0 {
		t.Fatalf("Interrupted capture was resubmitted: %v", got)
	}
}

func TestFailedEntriesRequireExplicitRetry(t *testing.T) {
	ledger, path := newTestLedger(t)
	url := "https://example.com/page"
	if err := ledger.put(url, []string{url}, Entry{Status: "failed", Error: "Unavailable"}); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(path)
	client := &fakeArchiveService{}
	if err := testRun(t, ledger, client, []string{url}); !errors.Is(err, errIncomplete) {
		t.Fatalf("Expected incomplete status: %v", err)
	}
	after, _ := os.ReadFile(path)
	if got := client.counts(); got != [3]int{} || string(before) != string(after) {
		t.Fatalf("Cached failure was retried or changed: %v", got)
	}
	r := runner{ledger: ledger, client: client, concurrency: 3, retryFailed: true}
	if err := r.run(context.Background(), []string{url}); err != nil {
		t.Fatal(err)
	}
	if got := client.counts(); got != [3]int{1, 1, 1} {
		t.Fatalf("Explicit retry did not run once: %v", got)
	}
}

func TestCaptureConcurrencyIsBounded(t *testing.T) {
	ledger, _ := newTestLedger(t)
	urls := []string{"https://example.com/1", "https://example.com/2", "https://example.com/3", "https://example.com/4", "https://example.com/5"}
	started := make(chan string, len(urls))
	release := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	var active, maximum atomic.Int32
	client := &fakeArchiveService{saveFn: func(ctx context.Context, url string) (capture, error) {
		n := active.Add(1)
		defer active.Add(-1)
		for previous := maximum.Load(); n > previous; previous = maximum.Load() {
			if maximum.CompareAndSwap(previous, n) {
				break
			}
		}
		started <- url
		select {
		case <-release:
			return capture{JobID: "job:" + url}, nil
		case <-ctx.Done():
			return capture{}, ctx.Err()
		}
	}}
	r := runner{ledger: ledger, client: client, concurrency: 3}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.run(ctx, urls) }()
	for range 3 {
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("Saves did not run concurrently")
		}
	}
	select {
	case <-started:
		t.Fatal("More saves started than the configured concurrency")
	case <-time.After(20 * time.Millisecond):
	}
	once.Do(func() { close(release) })
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := maximum.Load(); got != 3 {
		t.Fatalf("Expected three simultaneous saves; got %d", got)
	}
}

func TestLookupServiceFailureCancelsQueuedWork(t *testing.T) {
	ledger, _ := newTestLedger(t)
	urls := []string{"https://example.com/1", "https://example.com/2", "https://example.com/3", "https://example.com/4", "https://example.com/5", "https://example.com/6"}
	client := &fakeArchiveService{availableFn: func(ctx context.Context, url string) (string, error) {
		if url == urls[0] {
			return "", &serviceUnavailable{message: "Rate limited"}
		}
		<-ctx.Done()
		return "", ctx.Err()
	}}
	r := runner{ledger: ledger, client: client, concurrency: 3}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := r.run(ctx, urls)
	var unavailable *serviceUnavailable
	if !errors.As(err, &unavailable) {
		t.Fatalf("Expected service failure: %v", err)
	}
	if got := client.counts(); got[0] > 4 || got[1] != 0 {
		t.Fatalf("Queued lookups continued after service failure: %v", got)
	}
}

func TestSimultaneousRunLockIsRejected(t *testing.T) {
	root := t.TempDir()
	unlock, err := lockRepo(root)
	if err != nil {
		t.Fatal(err)
	}
	held := true
	defer func() {
		if held {
			unlock()
		}
	}()
	if secondUnlock, err := lockRepo(root); err == nil {
		secondUnlock()
		t.Fatal("Second simultaneous run acquired the repository lock")
	}
	unlock()
	held = false
	unlockedAgain, err := lockRepo(root)
	if err != nil {
		t.Fatalf("Lock was not released: %v", err)
	}
	unlockedAgain()
}

func TestFailedAtomicWriteRetainsLedgerState(t *testing.T) {
	ledger, path := newTestLedger(t)
	old, newURL := "https://example.com/old", "https://example.com/new"
	if err := ledger.put(old, []string{old}, Entry{Status: "archived", ArchiveURL: testSnapshot(old)}); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Force the final rename to fail after the replacement JSON has been written.
	blocked := filepath.Join(filepath.Dir(path), "cannot-replace-directory")
	if err := os.Mkdir(blocked, 0700); err != nil {
		t.Fatal(err)
	}
	ledger.path = blocked
	err = ledger.put(newURL, []string{newURL}, Entry{Status: "archived", ArchiveURL: testSnapshot(newURL)})
	ledger.path = path
	if err == nil {
		t.Fatal("Expected atomic replacement failure")
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("Failed write changed the valid ledger")
	}
	if _, found := ledger.record(newURL); found {
		t.Fatal("Failed write changed in-memory state")
	}
	files, err := filepath.Glob(filepath.Join(filepath.Dir(path), ".archive-links-*.tmp"))
	if err != nil || len(files) != 0 {
		t.Fatalf("Temporary write files were left behind: %v, %v", files, err)
	}
}

func TestConcurrentLedgerWritesLoseNoResults(t *testing.T) {
	ledger, path := newTestLedger(t)
	const count = 20
	var workers sync.WaitGroup
	for n := range count {
		workers.Add(1)
		go func() {
			defer workers.Done()
			url := fmt.Sprintf("https://example.com/%d", n)
			if err := ledger.put(url, []string{url}, Entry{Status: "archived", ArchiveURL: testSnapshot(url)}); err != nil {
				t.Errorf("Write failed: %v", err)
			}
		}()
	}
	workers.Wait()
	entries := testReadEntries(t, path)
	if len(entries) != count {
		t.Fatalf("Concurrent writes lost results: got %d, want %d", len(entries), count)
	}
	for n := range count {
		url := fmt.Sprintf("https://example.com/%d", n)
		if got := entries[url]; got.Status != "archived" || got.ArchiveURL != testSnapshot(url) {
			t.Errorf("Incorrect concurrent result for %s: %+v", url, got)
		}
	}
}

func clearTestCredentials(t *testing.T) {
	t.Helper()
	for _, name := range []string{"ARCHIVE_ORG_ACCESS_KEY", "ARCHIVE_ORG_SECRET_KEY", "IA_ACCESS_KEY", "IA_SECRET_KEY"} {
		t.Setenv(name, "")
	}
}

func TestCredentialsFileOverridesEnvironment(t *testing.T) {
	clearTestCredentials(t)
	t.Setenv("ARCHIVE_ORG_ACCESS_KEY", "environment-access")
	t.Setenv("ARCHIVE_ORG_SECRET_KEY", "environment-secret")
	path := filepath.Join(t.TempDir(), "private-credentials.json")
	if err := os.WriteFile(path, []byte(`{"access_key":"sample-access","secret_key":"sample-secret"}`), 0600); err != nil {
		t.Fatal(err)
	}
	client, err := newClient(path)
	if err != nil {
		t.Fatal(err)
	}
	if client.authorization != "LOW sample-access:sample-secret" {
		t.Fatal("Credentials file did not override the environment")
	}
}

func TestInvalidCredentialsNeverEchoValues(t *testing.T) {
	clearTestCredentials(t)
	invalid := []string{
		`{}`, `[]`, `null`, `"sample-secret"`, `17`,
		`{"access_key":"sample-access"}`,
		`{"access_key":"","secret_key":"sample-secret"}`,
		`{"access_key":17,"secret_key":"sample-secret"}`,
		`{"access_key":"sample-access","secret_key":false}`,
		`{"secret_key":"sample-secret", malformed}`,
	}
	for n, content := range invalid {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "private-credentials.json")
			if err := os.WriteFile(path, []byte(content), 0600); err != nil {
				t.Fatal(err)
			}
			_, err := newClient(path)
			if err == nil {
				t.Fatal("Invalid credentials were accepted")
			}
			if strings.Contains(err.Error(), "sample-access") || strings.Contains(err.Error(), "sample-secret") {
				t.Fatal("Credential values appeared in the validation error")
			}
		})
	}
}

func TestKnownAuthFailureDoesNotCauseUnauthorizedHTTPPost(t *testing.T) {
	clearTestCredentials(t)
	ledger, path := newTestLedger(t)
	old, newURL := "https://example.com/old", "https://example.com/new"
	if err := ledger.put(old, []string{old}, Entry{Status: "needs_auth", Error: "Credentials required"}); err != nil {
		t.Fatal(err)
	}
	var gets, posts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			posts.Add(1)
		} else {
			gets.Add(1)
		}
		fmt.Fprint(w, `{"archived_snapshots":{}}`)
	}))
	defer server.Close()
	client, err := newClient("")
	if err != nil {
		t.Fatal(err)
	}
	client.httpClient, client.availabilityURL, client.saveURL = server.Client(), server.URL, server.URL
	client.requestInterval, client.saveInterval = 0, 0
	if err := testRun(t, ledger, client, []string{old, newURL}); err == nil {
		t.Fatal("Expected missing-credentials result")
	}
	if gets.Load() != 1 || posts.Load() != 0 {
		t.Fatalf("Unexpected anonymous requests: %d GETs, %d POSTs", gets.Load(), posts.Load())
	}
	if got := testReadEntries(t, path)[newURL].Status; got != "needs_auth" {
		t.Fatalf("New link did not retain auth requirement: %s", got)
	}
	if err := testRun(t, ledger, client, []string{old, newURL}); !errors.Is(err, errIncomplete) {
		t.Fatalf("Unexpected repeat result: %v", err)
	}
	if gets.Load() != 1 || posts.Load() != 0 {
		t.Fatal("Repeat run made unnecessary HTTP requests")
	}
}

func TestSaveFormPreservesURLAndAvoidsRecapture(t *testing.T) {
	type requestDetails struct {
		method, authorization, contentType string
		form                               url.Values
	}
	details := make(chan requestDetails, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Error(err)
		}
		details <- requestDetails{r.Method, r.Header.Get("Authorization"), r.Header.Get("Content-Type"), r.PostForm}
		fmt.Fprint(w, `{"job_id":"form-job"}`)
	}))
	defer server.Close()
	client := &Client{httpClient: server.Client(), saveURL: server.URL, authorization: "LOW sample-access:sample-secret"}
	raw := "https://example.com/search?q=a%2Bb&name=caf%C3%A9&x=one%20two"
	result, err := client.save(context.Background(), raw)
	if err != nil || result.JobID != "form-job" {
		t.Fatalf("Unexpected save result: %+v, %v", result, err)
	}
	got := <-details
	want := url.Values{"url": {raw}, "skip_first_archive": {"1"}, "if_not_archived_within": {"3600"}}
	if !reflect.DeepEqual(got.form, want) || got.method != http.MethodPost || got.contentType != "application/x-www-form-urlencoded" {
		t.Fatalf("Unexpected save request: %+v", got)
	}
	if got.authorization != "LOW sample-access:sample-secret" {
		t.Fatal("Save request omitted authorization")
	}
}

func TestAvailabilityQueryPreservesOriginalURL(t *testing.T) {
	original := "https://example.com/search?q=a%2Bb&next=https%3A%2F%2Fexample.org"
	requests := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- r.URL.Query().Get("url")
		json.NewEncoder(w).Encode(map[string]any{
			"archived_snapshots": map[string]any{"closest": map[string]any{
				"available": true, "status": "200", "url": testSnapshot(original),
			}},
		})
	}))
	defer server.Close()
	client := &Client{httpClient: server.Client(), availabilityURL: server.URL}
	archive, err := client.available(context.Background(), original)
	if err != nil || archive != testSnapshot(original) {
		t.Fatalf("Unexpected availability result: %q, %v", archive, err)
	}
	if got := <-requests; got != original {
		t.Fatalf("Availability request altered the URL: %q", got)
	}
}

type testRoundTripper func(*http.Request) (*http.Response, error)

func (f testRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestSaveTimeoutIsNeverRetried(t *testing.T) {
	var requests atomic.Int32
	client := &Client{
		saveURL: "https://example.test/save", authorization: "LOW sample-access:sample-secret",
		httpClient: &http.Client{Transport: testRoundTripper(func(_ *http.Request) (*http.Response, error) {
			requests.Add(1)
			return nil, context.DeadlineExceeded
		})},
	}
	_, err := client.save(context.Background(), "https://example.com/page")
	if err == nil {
		t.Fatal("Expected timeout")
	}
	if requests.Load() != 1 {
		t.Fatalf("Ambiguous save was retried %d times", requests.Load())
	}
}

func TestHTTPRateLimitIsNotRetried(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Retry-After", "60")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()
	client := &Client{httpClient: server.Client(), availabilityURL: server.URL}
	_, err := client.available(context.Background(), "https://example.com/page")
	var unavailable *serviceUnavailable
	if !errors.As(err, &unavailable) || requests.Load() != 1 {
		t.Fatalf("Rate limit was retried or misclassified: %d requests, %v", requests.Load(), err)
	}
}

func TestPollingExhaustionPreservesPendingJobAndStopsNewSaves(t *testing.T) {
	ledger, path := newTestLedger(t)
	pendingURL, newURL := "https://example.com/pending", "https://example.com/new"
	entry := Entry{Status: "pending", JobID: "still-running"}
	if err := ledger.put(pendingURL, []string{pendingURL}, entry); err != nil {
		t.Fatal(err)
	}
	if err := ledger.put(newURL, []string{newURL}, Entry{Status: "ready"}); err != nil {
		t.Fatal(err)
	}
	client := &fakeArchiveService{statusFn: func(context.Context, string) (capture, error) {
		return capture{Status: "pending"}, nil
	}}
	r := runner{ledger: ledger, client: client, concurrency: 1, pollLimit: 1}
	err := r.run(context.Background(), []string{pendingURL, newURL})
	var unavailable *serviceUnavailable
	if !errors.As(err, &unavailable) {
		t.Fatalf("Polling exhaustion did not stop the batch: %v", err)
	}
	if got := client.counts(); got != [3]int{0, 0, 1} {
		t.Fatalf("Polling exhaustion released capacity for a new save: %v", got)
	}
	stored := testReadEntries(t, path)[pendingURL]
	if stored.Status != "pending" || stored.JobID != entry.JobID {
		t.Fatalf("Lost pending capture: %+v", stored)
	}
}

func TestHTTPServerErrorStopsSaveWithoutRetry(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer server.Close()
	client := &Client{httpClient: server.Client(), saveURL: server.URL, authorization: "LOW sample-access:sample-secret"}
	_, err := client.save(context.Background(), "https://example.com/page")
	var unavailable *serviceUnavailable
	if !errors.As(err, &unavailable) || requests.Load() != 1 {
		t.Fatalf("Server error did not halt the save without retry: %d requests, %v", requests.Load(), err)
	}
}

func TestRetryAfterParsesSecondsAndHTTPDates(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 34, 56, 0, time.UTC)
	for _, test := range []struct {
		name, value string
		want        time.Time
	}{
		{"seconds", "300", now.Add(5 * time.Minute)},
		{"zero", "0", now},
		{"HTTP date", now.Add(10 * time.Minute).Format(http.TimeFormat), now.Add(10 * time.Minute)},
		{"missing", "", now.Add(time.Minute)},
		{"invalid", "invalid", now.Add(time.Minute)},
		{"negative", "-1", now.Add(time.Minute)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := parseRetryAfter(test.value, now); !got.Equal(test.want) {
				t.Fatalf("Retry-After parsed to %s, want %s", got, test.want)
			}
		})
	}
}

func TestFutureCooldownMakesNoRequestsAndDoesNotChangeLedger(t *testing.T) {
	ledger, path := newTestLedger(t)
	url := "https://example.com/pending"
	until := time.Now().UTC().Truncate(time.Second).Add(5 * time.Minute)
	entry := Entry{Status: "pending", JobID: "keep-this-job", RetryAfter: until.Format(time.RFC3339)}
	if err := ledger.put(url, []string{url}, entry); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	client := &fakeArchiveService{}
	err = testRun(t, ledger, client, []string{url})
	var unavailable *serviceUnavailable
	if !errors.As(err, &unavailable) {
		t.Fatalf("Future cooldown did not stop the run: %v", err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := client.counts(); got != [3]int{} || string(before) != string(after) {
		t.Fatalf("Cooldown run performed work: %v, changed bytes: %v", got, string(before) != string(after))
	}
	if got := testReadEntries(t, path)[url]; got.Status != "pending" || got.JobID != entry.JobID {
		t.Fatalf("Cooldown lost the pending job: %+v", got)
	}
}

func TestRateLimitPersistsCooldownWithoutLosingPendingJob(t *testing.T) {
	ledger, path := newTestLedger(t)
	url := "https://example.com/pending"
	until := time.Now().UTC().Truncate(time.Second).Add(5 * time.Minute)
	entry := Entry{Status: "pending", JobID: "keep-this-job"}
	if err := ledger.put(url, []string{url}, entry); err != nil {
		t.Fatal(err)
	}
	client := &fakeArchiveService{statusFn: func(context.Context, string) (capture, error) {
		return capture{}, &serviceUnavailable{message: "Rate limited", retryAt: until}
	}}
	err := testRun(t, ledger, client, []string{url})
	var unavailable *serviceUnavailable
	if !errors.As(err, &unavailable) {
		t.Fatalf("Expected rate limit: %v", err)
	}
	stored := testReadEntries(t, path)[url]
	if stored.Status != "pending" || stored.JobID != entry.JobID || stored.RetryAfter != until.Format(time.RFC3339) {
		t.Fatalf("Cooldown did not preserve pending state: %+v", stored)
	}
	if !ledger.retryAfter().Equal(until) {
		t.Fatal("Persisted cooldown was not loaded")
	}
	before, _ := os.ReadFile(path)
	counts := client.counts()
	if err := testRun(t, ledger, client, []string{url}); !errors.As(err, &unavailable) {
		t.Fatalf("Expected repeated run to respect cooldown: %v", err)
	}
	after, _ := os.ReadFile(path)
	if client.counts() != counts || string(before) != string(after) {
		t.Fatal("Repeat during cooldown made requests or changed state")
	}
}

func TestCooldownCanNeverBeShortened(t *testing.T) {
	ledger, path := newTestLedger(t)
	url := "https://example.com/pending"
	until := time.Now().UTC().Truncate(time.Second).Add(10 * time.Minute)
	if err := ledger.put(url, []string{url}, Entry{Status: "pending", JobID: "keep-this-job"}); err != nil {
		t.Fatal(err)
	}
	if err := ledger.setCooldown(until, []string{url}); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(path)
	if err := ledger.setCooldown(until.Add(-5*time.Minute), []string{url}); err != nil {
		t.Fatal(err)
	}
	after, _ := os.ReadFile(path)
	if !ledger.retryAfter().Equal(until) || string(before) != string(after) {
		t.Fatal("Shorter retry interval changed the existing cooldown")
	}
}

func TestRateLimitBeforeFirstResultRetainsRetryableLink(t *testing.T) {
	ledger, path := newTestLedger(t)
	url := "https://example.com/new"
	until := time.Now().UTC().Truncate(time.Second).Add(5 * time.Minute)
	client := &fakeArchiveService{availableFn: func(context.Context, string) (string, error) {
		return "", &serviceUnavailable{message: "Rate limited", retryAt: until}
	}}
	if err := testRun(t, ledger, client, []string{url}); err == nil {
		t.Fatal("Expected rate limit")
	}
	entry := testReadEntries(t, path)[url]
	if entry.Status != "unchecked" || entry.RetryAfter != until.Format(time.RFC3339) {
		t.Fatalf("First rate limit lost its retryable link: %+v", entry)
	}
	entry.RetryAfter = time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
	if err := ledger.put(url, []string{url}, entry); err != nil {
		t.Fatal(err)
	}
	resumed := &fakeArchiveService{availableFn: func(context.Context, string) (string, error) {
		return testSnapshot(url), nil
	}}
	if err := testRun(t, ledger, resumed, []string{url}); err != nil {
		t.Fatalf("Expired cooldown did not allow the unprocessed link to run: %v", err)
	}
	if got := resumed.counts(); got != [3]int{1, 0, 0} {
		t.Fatalf("Unexpected work after cooldown: %v", got)
	}
}

func TestOwnGitHubLinksAreExcluded(t *testing.T) {
	excluded := []string{
		"https://github.com/nhooyr",
		"https://github.com/nhooyr/",
		"http://github.com/nhooyr?tab=repositories",
		"https://github.com/nhooyr/dotfiles",
		"https://github.com/nhooyr/nhooyr.github.io/blob/main/index.html",
		"https://www.github.com/nhooyr/flappybird?tab=readme-ov-file#readme",
		"https://GitHub.Com/NHoOyR/dotfiles",
		"https://WWW.GITHUB.COM/NHOOYR?tab=repositories",
	}
	retained := []string{
		"https://github.com/nhooyr-other",
		"https://github.com/nhooyr2/dotfiles",
		"https://github.com/other-user/nhooyr",
		"https://github.com/other-user/project?owner=nhooyr",
		"https://example.com/nhooyr",
		"https://gist.github.com/nhooyr/example",
	}
	var content strings.Builder
	for _, raw := range excluded {
		if !excludedLink(raw) {
			t.Errorf("Own GitHub link was not excluded: %s", raw)
		}
		fmt.Fprintf(&content, "<a href=%q>excluded</a>", raw)
	}
	for _, raw := range retained {
		if excludedLink(raw) {
			t.Errorf("Unrelated link was excluded: %s", raw)
		}
		fmt.Fprintf(&content, "<a href=%q>retained</a>", raw)
	}
	got := extractLinks(content.String())
	sort.Strings(got)
	sort.Strings(retained)
	if !reflect.DeepEqual(got, retained) {
		t.Fatalf("HTML extraction did not apply the exclusion accurately: %v", got)
	}
}

func TestWikipediaDomainsAreExcludedPrecisely(t *testing.T) {
	excluded := []string{
		"https://wikipedia.org",
		"https://www.wikipedia.org/",
		"https://en.wikipedia.org/wiki/Kaslo",
		"https://fr.wikipedia.org/wiki/Paris",
		"https://en.m.wikipedia.org/wiki/Kaslo?oldid=1#History",
		"https://EN.WIKIPEDIA.ORG/wiki/Kaslo",
		"http://m.wikipedia.org/",
	}
	retained := []string{
		"https://notwikipedia.org/wiki/Kaslo",
		"https://wikipedia.org.example.com/",
		"https://en.wikipedia.org.evil.test/wiki/Kaslo",
		"https://wikipedia.com/",
		"https://commons.wikimedia.org/wiki/Kaslo",
	}
	var content strings.Builder
	for _, raw := range excluded {
		if !excludedLink(raw) {
			t.Errorf("Wikipedia link was not excluded: %s", raw)
		}
		fmt.Fprintf(&content, "<a href=%q>excluded</a>", raw)
	}
	for _, raw := range retained {
		if excludedLink(raw) {
			t.Errorf("Unrelated domain was excluded: %s", raw)
		}
		fmt.Fprintf(&content, "<a href=%q>retained</a>", raw)
	}
	got := extractLinks(content.String())
	sort.Strings(retained)
	if !reflect.DeepEqual(got, retained) {
		t.Fatalf("HTML extraction did not apply Wikipedia exclusions precisely: %v", got)
	}
}

func TestExcludedLinkBoundaries(t *testing.T) {
	excluded := []string{
		"https://sample-user@example.com/private",
		"https://sample-user:sample-password@example.com/private",
		"https://discord.gg/sample-invite",
		"https://DISCORD.GG/sample-invite",
		"https://m.me/sample-user",
		"https://www.m.me/sample-user",
		"https://wa.me/15555550100",
		"https://www.wa.me/15555550100",
		"https://signal.me/#sample-contact",
		"https://www.signal.me/#sample-contact",
		"https://t.me/sample-user",
		"https://www.t.me/sample-user",
		"https://t.me/+sample-invite",
		"https://telegram.me/sample-user",
		"https://www.telegram.me/sample-user",
		"https://discord.com/invite/sample-invite",
		"https://www.discord.com/users/12345",
		"https://discordapp.com/%69nvite/sample-invite",
		"https://snapchat.com/t/sample-invite",
		"https://www.snapchat.com/add/sample-user",
		"https://api.whatsapp.com/send?phone=15555550100",
		"https://www.api.whatsapp.com/send?phone=15555550100",
		"https://web.whatsapp.com/",
		"https://www.web.whatsapp.com/",
		"https://zoom.us/j/12345",
		"https://us05web.zoom.us/j/12345",
		"https://WWW.ZOOM.US/meeting/12345",
		"https://example.com/page?%74oken=sample-value",
		"https://example.com/page?authori%7Aation=sample-value",
		"https://example.com/page?%61ccess%5Ftoken=sample-value",
		"https://open.spotify.com/user/example?si=sample-share-id",
		"https://www.youtube.com/watch?v=sample-video-id",
		"https://www.facebook.com/photo/?fbid=12345&set=sample-album",
		"https://t.me/s/public_channel",
		"https://t.me/s/public_channel/123?before=124",
		"https://www.t.me/s/public_channel",
		"https://telegram.me/s/public_channel",
		"https://www.telegram.me/s/public_channel",
		"https://discord.com/blog/public-article",
	}
	for _, key := range []string{"auth", "access_token", "authorization", "password", "pwd", "secret", "token"} {
		excluded = append(excluded,
			"https://example.com/page?"+key+"=sample-value",
			"https://example.com/page?"+strings.ToUpper(key)+"=sample-value",
		)
	}
	retained := []string{
		"https://discord.gg.example.com/sample-invite",
		"https://m.me.example.com/sample-user",
		"https://wa.me.example.com/sample-user",
		"https://t.me.example.com/sample-user",
		"https://zoom.us.example.com/j/12345",
		"https://examplezoom.us/j/12345",
		"https://api.whatsapp.com.example.com/send",
	}
	var content strings.Builder
	for _, raw := range excluded {
		if !excludedLink(raw) {
			t.Errorf("Excluded link was retained: %s", raw)
		}
		fmt.Fprintf(&content, "<a href=%q>excluded</a>", raw)
	}
	for _, raw := range retained {
		if excludedLink(raw) {
			t.Errorf("Eligible link or lookalike domain was excluded: %s", raw)
		}
		fmt.Fprintf(&content, "<a href=%q>retained</a>", raw)
	}
	got := extractLinks(content.String())
	sort.Strings(retained)
	if !reflect.DeepEqual(got, retained) {
		t.Fatalf("HTML extraction did not retain exactly the eligible links: %v", got)
	}
}

func TestSocialAndMessagingPlatformsAreExcludedIncludingSubdomains(t *testing.T) {
	domains := []string{
		"facebook.com", "fb.com", "fb.me", "instagram.com", "threads.com", "threads.net",
		"twitter.com", "x.com", "t.co", "linkedin.com", "lnkd.in", "tiktok.com",
		"youtube.com", "youtu.be", "reddit.com", "redd.it", "pinterest.com", "pin.it",
		"flickr.com", "spotify.com", "snapchat.com", "discord.com", "discordapp.com", "discord.gg",
		"telegram.org", "t.me", "telegram.me", "whatsapp.com", "wa.me", "signal.org", "signal.me",
		"messenger.com", "m.me", "zoom.us",
	}
	var content strings.Builder
	for _, domain := range domains {
		t.Run(domain, func(t *testing.T) {
			for _, host := range []string{domain, "www." + domain, "public." + strings.ToUpper(domain)} {
				raw := "https://" + host + "/public-page"
				if !excludedLink(raw) {
					t.Errorf("Social or messaging platform was not excluded: %s", raw)
				}
			}
			for _, host := range []string{domain + ".example.org", "not" + domain} {
				if excludedLink("https://" + host + "/article") {
					t.Errorf("Lookalike domain was incorrectly excluded: %s", host)
				}
			}
		})
		fmt.Fprintf(&content, "<a href=%q>social</a>", "https://"+domain+"/public-page")
	}
	eligible := "https://example.org/public-article"
	fmt.Fprintf(&content, "<a href=%q>article</a>", eligible)
	if got := extractLinks(content.String()); !reflect.DeepEqual(got, []string{eligible}) {
		t.Fatalf("Social links remained in HTML extraction: %v", got)
	}
}

func TestCommunityProfilesExcludedWhileTechnicalArticlesRemain(t *testing.T) {
	excluded := []string{
		"https://chatgpt.com/u/example",
		"https://www.chatgpt.com/u/example",
		"https://cplusplus.com/user/example/",
		"https://www.cplusplus.com/user/example/",
		"https://stackexchange.com/users/12345/example?tab=accounts",
		"https://math.stackexchange.com/users/12345/example",
		"https://stackoverflow.com/users/12345/example",
		"https://www.stackoverflow.com/users/12345/example",
		"https://powdertoy.co.uk/User.html?Name=example",
		"https://www.powdertoy.co.uk/user.html?Name=example",
	}
	retained := []string{
		"https://cplusplus.com/doc/tutorial/",
		"https://www.cplusplus.com/reference/vector/vector/",
		"https://cplusplus.com/articles/example/",
		"https://stackoverflow.com/questions/12345/technical-question",
		"https://www.stackoverflow.com/questions/12345/technical-question",
		"https://math.stackexchange.com/questions/12345/technical-question",
		"https://stackexchange.com/sites",
		"https://powdertoy.co.uk/Discussions/Thread/View.html?Thread=12345",
		"https://chatgpt.com/share/public-example",
	}
	var content strings.Builder
	for _, raw := range excluded {
		if !excludedLink(raw) {
			t.Errorf("Community profile was not excluded: %s", raw)
		}
		fmt.Fprintf(&content, "<a href=%q>profile</a>", raw)
	}
	for _, raw := range retained {
		if excludedLink(raw) {
			t.Errorf("Public article was incorrectly excluded: %s", raw)
		}
		fmt.Fprintf(&content, "<a href=%q>article</a>", raw)
	}
	sort.Strings(retained)
	if got := extractLinks(content.String()); !reflect.DeepEqual(got, retained) {
		t.Fatalf("Community profile exclusions affected article extraction: %v", got)
	}
}

func TestOwnGitHubActivitySearchIsExcluded(t *testing.T) {
	for _, qualifier := range []string{"involves", "author", "user", "org"} {
		for _, owner := range []string{"nhooyr", "NHoOyR"} {
			raw := "https://github.com/search?" + url.Values{"q": {"is:issue " + qualifier + ":" + owner}, "type": {"issues"}}.Encode()
			if !excludedLink(raw) {
				t.Errorf("Own GitHub activity search was not excluded: %s", raw)
			}
		}
	}
	for _, query := range []string{"involves:other-user", "user:nhooyr-other", "repo:other-user/nhooyr", "nhooyr parser"} {
		raw := "https://github.com/search?" + url.Values{"q": {query}, "type": {"issues"}}.Encode()
		if excludedLink(raw) {
			t.Errorf("Unrelated GitHub search was excluded: %s", raw)
		}
	}
}

func TestLocalNetworkAddressesAreExcludedWithoutBlockingPublicAddresses(t *testing.T) {
	excluded := []string{
		"http://localhost:8080/",
		"http://preview.LOCALHOST/",
		"http://printer.local/",
		"http://127.0.0.1/",
		"http://127.42.0.1/",
		"http://10.0.0.1/",
		"http://172.16.0.1/",
		"http://172.31.255.254/",
		"http://192.168.1.5/",
		"http://169.254.5.5/",
		"http://0.0.0.0/",
		"http://[::1]/",
		"http://[fc00::1]/",
		"http://[fd12::1]/",
		"http://[fe80::1]/",
		"http://[::]/",
		"http://[::ffff:127.0.0.1]/",
	}
	retained := []string{
		"http://8.8.8.8/",
		"http://1.1.1.1/",
		"http://172.15.0.1/",
		"http://172.32.0.1/",
		"http://192.169.1.1/",
		"http://[2001:4860:4860::8888]/",
		"https://localhost.example.com/",
		"https://public.local.example.com/",
	}
	for _, raw := range excluded {
		if !excludedLink(raw) {
			t.Errorf("Local network URL was not excluded: %s", raw)
		}
	}
	for _, raw := range retained {
		if excludedLink(raw) {
			t.Errorf("Public address was excluded: %s", raw)
		}
	}
}

func TestExcludedLinksArePrunedAndNeverRequested(t *testing.T) {
	ledger, path := newTestLedger(t)
	private := []string{
		"https://discord.gg/sample-invite",
		"https://example.com/private?ToKeN=sample-value",
		"https://t.me/sample-user",
		"https://us05web.zoom.us/j/12345",
		"https://open.spotify.com/user/example?si=sample-share-id",
		"https://www.youtube.com/watch?v=sample-video-id",
		"https://www.facebook.com/photo/?fbid=12345",
	}
	for _, raw := range private {
		if err := ledger.put(raw, []string{raw}, Entry{Status: "pending", JobID: "must-not-poll"}); err != nil {
			t.Fatal(err)
		}
	}
	public := "https://example.org/article?id=123&lang=en"
	client := &fakeArchiveService{availableFn: func(_ context.Context, raw string) (string, error) {
		return testSnapshot(raw), nil
	}}
	urls := append(append([]string(nil), private...), public)
	if err := testRun(t, ledger, client, urls); err != nil {
		t.Fatal(err)
	}
	if got := client.counts(); got != [3]int{1, 0, 0} || !reflect.DeepEqual(client.availableCalls, []string{public}) {
		t.Fatalf("Non-public links caused requests: %v, %v", got, client.availableCalls)
	}
	entries := testReadEntries(t, path)
	if len(entries) != 1 || entries[public].ArchiveURL != testSnapshot(public) {
		t.Fatalf("Non-public records were retained or public record lost: %+v", entries)
	}
}

func TestExcludedLedgerEntriesArePrunedWithoutRepeatWrites(t *testing.T) {
	ledger, path := newTestLedger(t)
	excluded := []string{
		"https://github.com/nhooyr",
		"https://GitHub.Com/NHOOYR/dotfiles?tab=readme",
		"https://www.github.com/nhooyr/nhooyr.github.io/blob/main/index.html",
		"https://en.wikipedia.org/wiki/Kaslo",
		"https://en.m.wikipedia.org/wiki/Kaslo?oldid=1#History",
	}
	for _, raw := range excluded {
		if err := ledger.put(raw, []string{raw}, Entry{Status: "archived", ArchiveURL: testSnapshot(raw)}); err != nil {
			t.Fatal(err)
		}
	}
	retained := "https://github.com/other-user/project"
	if err := ledger.put(retained, []string{retained}, Entry{Status: "archived", ArchiveURL: testSnapshot(retained)}); err != nil {
		t.Fatal(err)
	}
	if err := ledger.removeExcluded(); err != nil {
		t.Fatal(err)
	}
	entries := testReadEntries(t, path)
	if len(entries) != 1 || entries[retained].ArchiveURL != testSnapshot(retained) {
		t.Fatalf("Pruning removed the wrong entries: %+v", entries)
	}
	for _, raw := range excluded {
		if _, found := ledger.record(raw); found {
			t.Errorf("Excluded entry remained in memory: %s", raw)
		}
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	marker := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := os.Chtimes(path, marker, marker); err != nil {
		t.Fatal(err)
	}
	if err := ledger.removeExcluded(); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) || !info.ModTime().Equal(marker) {
		t.Fatal("Repeated pruning rewrote an unchanged ledger")
	}
}

func TestRunnerSkipsExcludedLinksAndProcessesOtherLinks(t *testing.T) {
	ledger, path := newTestLedger(t)
	old := "https://github.com/nhooyr/dotfiles"
	if err := ledger.put(old, []string{old}, Entry{Status: "pending", JobID: "must-not-poll"}); err != nil {
		t.Fatal(err)
	}
	oldWikipedia := "https://en.wikipedia.org/wiki/Kaslo"
	if err := ledger.put(oldWikipedia, []string{oldWikipedia}, Entry{Status: "pending", JobID: "must-not-poll-wikipedia"}); err != nil {
		t.Fatal(err)
	}
	eligible := "https://github.com/other-user/project"
	client := &fakeArchiveService{availableFn: func(_ context.Context, raw string) (string, error) {
		return testSnapshot(raw), nil
	}}
	urls := []string{
		old, "https://github.com/nhooyr?tab=repositories", "https://www.github.com/NHOOYR/blog",
		oldWikipedia, "https://wikipedia.org", "https://en.m.wikipedia.org/wiki/Kaslo", eligible,
	}
	if err := testRun(t, ledger, client, urls); err != nil {
		t.Fatal(err)
	}
	if got := client.counts(); got != [3]int{1, 0, 0} || !reflect.DeepEqual(client.availableCalls, []string{eligible}) {
		t.Fatalf("Excluded links caused requests: calls %v, lookups %v", got, client.availableCalls)
	}
	entries := testReadEntries(t, path)
	if len(entries) != 1 || entries[eligible].ArchiveURL != testSnapshot(eligible) {
		t.Fatalf("Excluded records were retained or eligible record lost: %+v", entries)
	}
}

func TestPruningOwnGitHubRecordPreservesGlobalCooldown(t *testing.T) {
	ledger, path := newTestLedger(t)
	excluded, retained := "https://github.com/nhooyr/dotfiles", "https://example.com/pending"
	until := time.Now().UTC().Truncate(time.Second).Add(5 * time.Minute)
	if err := ledger.put(excluded, []string{excluded}, Entry{Status: "pending", JobID: "excluded-job", RetryAfter: until.Format(time.RFC3339)}); err != nil {
		t.Fatal(err)
	}
	if err := ledger.put(retained, []string{retained}, Entry{Status: "pending", JobID: "retained-job"}); err != nil {
		t.Fatal(err)
	}
	if err := ledger.removeExcluded(); err != nil {
		t.Fatal(err)
	}
	entries := testReadEntries(t, path)
	if len(entries) != 1 {
		t.Fatalf("Excluded record remained after pruning: %+v", entries)
	}
	entry := entries[retained]
	if entry.Status != "pending" || entry.JobID != "retained-job" || !ledger.retryAfter().Equal(until) {
		t.Fatalf("Pruning lost cooldown or altered the retained job: %+v", entry)
	}
	client := &fakeArchiveService{}
	if err := testRun(t, ledger, client, []string{retained}); err == nil {
		t.Fatal("Pruning bypassed the active global cooldown")
	}
	if got := client.counts(); got != [3]int{} {
		t.Fatalf("Pruning led to requests during cooldown: %v", got)
	}
}

func TestConcurrentRateLimitsPreserveLongestCooldown(t *testing.T) {
	ledger, path := newTestLedger(t)
	urls := []string{"https://example.com/first", "https://example.com/second"}
	for _, raw := range urls {
		if err := ledger.put(raw, []string{raw}, Entry{Status: "pending", JobID: "job:" + raw}); err != nil {
			t.Fatal(err)
		}
	}
	first := time.Now().UTC().Truncate(time.Second).Add(5 * time.Minute)
	longest := first.Add(5 * time.Minute)
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	client := &fakeArchiveService{statusFn: func(ctx context.Context, job string) (capture, error) {
		started <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
			return capture{}, ctx.Err()
		}
		if job == "job:"+urls[0] {
			return capture{}, &serviceUnavailable{message: "First limit", retryAt: first}
		}
		// Model a longer Retry-After response arriving after another worker
		// has already canceled the batch.
		<-ctx.Done()
		return capture{}, &serviceUnavailable{message: "Longer limit", retryAt: longest}
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	r := runner{ledger: ledger, client: client, concurrency: 2}
	done := make(chan error, 1)
	go func() { done <- r.run(ctx, urls) }()
	for range 2 {
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("Pending jobs did not start concurrently")
		}
	}
	releaseOnce.Do(func() { close(release) })
	err := <-done
	var unavailable *serviceUnavailable
	if !errors.As(err, &unavailable) || !unavailable.retryAt.Equal(longest) {
		t.Fatalf("Longest Retry-After was lost: %v", err)
	}
	if !ledger.retryAfter().Equal(longest) {
		t.Fatal("Longest Retry-After was not persisted")
	}
	for _, raw := range urls {
		entry := testReadEntries(t, path)[raw]
		if entry.Status != "pending" || entry.JobID != "job:"+raw {
			t.Errorf("Rate limit lost pending job %s: %+v", raw, entry)
		}
	}
}

func TestLeqraIsExcludedWithoutMatchingLookalikes(t *testing.T) {
	for _, raw := range []string{"https://leqra.com", "https://www.leqra.com/game", "https://PLAY.LEQRA.COM/"} {
		if !excludedLink(raw) {
			t.Errorf("Leqra URL was not excluded: %s", raw)
		}
	}
	for _, raw := range []string{"https://notleqra.com/", "https://leqra.com.example.org/"} {
		if excludedLink(raw) {
			t.Errorf("Leqra lookalike was excluded: %s", raw)
		}
	}
	ledger, path := newTestLedger(t)
	excluded, eligible := "https://leqra.com", "https://example.org/article"
	if err := ledger.put(excluded, []string{excluded}, Entry{Status: "pending", JobID: "must-not-poll"}); err != nil {
		t.Fatal(err)
	}
	client := &fakeArchiveService{availableFn: func(_ context.Context, raw string) (string, error) {
		return testSnapshot(raw), nil
	}}
	if err := testRun(t, ledger, client, []string{excluded, eligible}); err != nil {
		t.Fatal(err)
	}
	if got := client.counts(); got != [3]int{1, 0, 0} || !reflect.DeepEqual(client.availableCalls, []string{eligible}) {
		t.Fatalf("Leqra caused unnecessary requests: %v, %v", got, client.availableCalls)
	}
	if entries := testReadEntries(t, path); len(entries) != 1 || entries[eligible].ArchiveURL != testSnapshot(eligible) {
		t.Fatalf("Leqra record was not pruned correctly: %+v", entries)
	}
}

func TestMutableRulesControlMatching(t *testing.T) {
	useRules := func(t *testing.T, config rulesConfig) {
		t.Helper()
		original := exclusionSettings
		exclusionSettings = config
		t.Cleanup(func() { exclusionSettings = original })
	}
	t.Run("domains", func(t *testing.T) {
		useRules(t, rulesConfig{})
		raw := "https://example.com/article"
		if excludedLink(raw) {
			t.Fatal("Empty configuration excluded a public URL")
		}
		exclusionSettings.Domains = []string{"example.com"}
		if !excludedLink(raw) || !excludedLink("https://sub.example.com/article") || excludedLink("https://example.com.other.org/article") {
			t.Fatal("Domain configuration did not control domain matching")
		}
		exclusionSettings.Domains = nil
		if excludedLink(raw) {
			t.Fatal("Removing a domain did not remove its exclusion")
		}
	})
	t.Run("credentials toggle", func(t *testing.T) {
		useRules(t, rulesConfig{})
		raw := "https://sample-user@example.com/article"
		if excludedLink(raw) {
			t.Fatal("Credentials were blocked while the setting was disabled")
		}
		exclusionSettings.BlockCredentials = true
		if !excludedLink(raw) {
			t.Fatal("Enabling the credentials setting had no effect")
		}
	})
	t.Run("private IP toggle", func(t *testing.T) {
		useRules(t, rulesConfig{})
		raw := "http://10.0.0.1/article"
		if excludedLink(raw) {
			t.Fatal("Private IP was blocked while the setting was disabled")
		}
		exclusionSettings.BlockPrivateIPs = true
		if !excludedLink(raw) {
			t.Fatal("Enabling the private IP setting had no effect")
		}
	})
	t.Run("sensitive query keys", func(t *testing.T) {
		useRules(t, rulesConfig{SensitiveQueryKeys: []string{"private_key"}})
		if !excludedLink("https://example.com/?PRIVATE_KEY=sample") || excludedLink("https://example.com/?session=sample") {
			t.Fatal("Configured sensitive query key was not applied precisely")
		}
		exclusionSettings.SensitiveQueryKeys = []string{"session"}
		if excludedLink("https://example.com/?private_key=sample") || !excludedLink("https://example.com/?session=sample") {
			t.Fatal("Changing the sensitive query list had no effect")
		}
	})
	t.Run("path rules", func(t *testing.T) {
		useRules(t, rulesConfig{Paths: []pathRule{{Host: "example.com", Path: "/private"}}})
		if !excludedLink("https://example.com/private/child") || excludedLink("https://example.com/private-extra") || excludedLink("https://other.org/private") {
			t.Fatal("Path rule did not respect segment and host boundaries")
		}
		if !excludedLink("https://www.example.com/private") || excludedLink("https://sub.example.com/private") {
			t.Fatal("Path host matching did not normalize www or restrict other subdomains")
		}
		exclusionSettings.Paths[0].Subdomains = true
		if !excludedLink("https://sub.example.com/private") || excludedLink("https://example.com.other.org/private") {
			t.Fatal("Subdomains path setting had no effect or crossed a domain boundary")
		}
		if excludedLink("https://example.com/PRIVATE") {
			t.Fatal("Case-sensitive path rule matched a different case")
		}
		exclusionSettings.Paths[0].IgnoreCase = true
		if !excludedLink("https://example.com/PRIVATE") {
			t.Fatal("IgnoreCase path setting had no effect")
		}
		exclusionSettings.Paths[0].Exact = true
		if excludedLink("https://example.com/private/child") || !excludedLink("https://example.com/private") {
			t.Fatal("Exact path setting had no effect")
		}
	})
	t.Run("query token rules", func(t *testing.T) {
		useRules(t, rulesConfig{QueryTokens: []queryTokenRule{{Host: "example.com", Path: "/search", Key: "q", Tokens: []string{"owner:sample"}}}})
		if !excludedLink("https://example.com/search?q=is%3Aissue+owner%3Asample") || excludedLink("https://example.com/article?q=owner%3Asample") || excludedLink("https://example.com/search?q=owner%3Asample-other") {
			t.Fatal("Query-token rule did not respect path and token boundaries")
		}
		if !excludedLink("https://www.example.com/search?Q=owner%3Asample") || excludedLink("https://sub.example.com/search?q=owner%3Asample") {
			t.Fatal("Query host/key matching did not normalize www and key case or restrict subdomains")
		}
		exclusionSettings.QueryTokens[0].Subdomains = true
		if !excludedLink("https://sub.example.com/search?q=owner%3Asample") || excludedLink("https://example.com.other.org/search?q=owner%3Asample") {
			t.Fatal("Subdomains token setting had no effect or crossed a domain boundary")
		}
		if excludedLink("https://example.com/search?q=OWNER%3ASAMPLE") {
			t.Fatal("Case-sensitive query token matched a different case")
		}
		exclusionSettings.QueryTokens[0].IgnoreCase = true
		if !excludedLink("https://example.com/search?q=OWNER%3ASAMPLE") {
			t.Fatal("IgnoreCase token setting had no effect")
		}
		exclusionSettings.QueryTokens[0].Tokens = []string{"owner:other"}
		if excludedLink("https://example.com/search?q=owner%3Asample") || !excludedLink("https://example.com/search?q=owner%3Aother") {
			t.Fatal("Changing the token list had no effect")
		}
		exclusionSettings.QueryTokens[0].Key = "query"
		if excludedLink("https://example.com/search?q=owner%3Aother") || !excludedLink("https://example.com/search?query=owner%3Aother") {
			t.Fatal("Changing the query key had no effect")
		}
	})
}

func TestExclusionReasonAgreesWithMatching(t *testing.T) {
	for _, raw := range []string{
		"https://leqra.com/", "https://en.wikipedia.org/wiki/Example", "https://github.com/nhooyr",
		"https://instagram.com/example", "http://127.0.0.1/", "https://example.org/?token=sample-value",
	} {
		if reason := exclusionReason(raw); reason == "" || !excludedLink(raw) {
			t.Errorf("Excluded URL has no explanation: %s", raw)
		}
	}
	for _, raw := range []string{"https://example.org/article", "https://cplusplus.com/doc/tutorial/", "https://leqra.com.example.org/"} {
		if reason := exclusionReason(raw); reason != "" || excludedLink(raw) {
			t.Errorf("Eligible URL has an exclusion reason: %s: %s", raw, reason)
		}
	}
}
