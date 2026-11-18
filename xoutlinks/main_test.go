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
	"os/exec"
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

var testNow = time.Date(2026, 11, 18, 12, 34, 56, 0, time.UTC)

func fixedTestTime() time.Time { return testNow }

func testSnapshot(url string) string {
	return "https://web.archive.org/web/" + testTimestamp + "/" + url
}

type fakeArchiveService struct {
	mu              sync.Mutex
	unauthenticated bool
	availableCalls  []string
	saveCalls       []string
	statusCalls     []string
	availableFn     func(context.Context, string) (string, error)
	saveFn          func(context.Context, string) (capture, error)
	statusFn        func(context.Context, string) (capture, error)
}

func (f *fakeArchiveService) authenticated() bool { return !f.unauthenticated }

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
	ledger, err := loadLedgerAt(path, testNow)
	if err != nil {
		t.Fatal(err)
	}
	return ledger, path
}

func testRun(t *testing.T, ledger *Ledger, client archiveService, urls []string) error {
	t.Helper()
	r := runner{ledger: ledger, client: client, now: fixedTestTime, concurrency: 3}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return r.run(ctx, urls)
}

func testReadDatabase(t *testing.T, path string) database {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var stored database
	if err := json.Unmarshal(data, &stored); err != nil {
		t.Fatal(err)
	}
	if stored.Version != databaseVersion || stored.Entries == nil {
		t.Fatalf("Expected a version %d database", databaseVersion)
	}
	return stored
}

func testReadEntries(t *testing.T, path string) map[string]Entry {
	t.Helper()
	return testReadDatabase(t, path).Entries
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

func TestExcludedLinksDatabaseIsSortedUniqueAndStable(t *testing.T) {
	root := t.TempDir()
	command := exec.Command("git", "init", "-q", root)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("Initialize test repository: %v: %s", err, output)
	}
	if err := os.MkdirAll(filepath.Join(root, "xoutlinks"), 0700); err != nil {
		t.Fatal(err)
	}
	pages := map[string]string{
		"index.html": `<a href="https://en.wikipedia.org/wiki/Example">wiki</a>
<a href="https://example.org/article">article</a>
<a href="https://github.com/nhooyr/nhooyr.github.io">source</a><a href="/local">local</a>`,
		"more.html": `<a href="https://en.wikipedia.org/wiki/Example">duplicate</a>
<a href="https://github.com/nhooyr/nhooyr.github.io?tab=readme&amp;x=1">source</a>`,
	}
	for name, content := range pages {
		if err := os.WriteFile(filepath.Join(root, name), []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
	}
	links, err := collectLinks(root)
	if err != nil {
		t.Fatal(err)
	}
	included, excluded := filterLinks(links)
	want := []string{
		"https://en.wikipedia.org/wiki/Example",
		"https://github.com/nhooyr/nhooyr.github.io",
		"https://github.com/nhooyr/nhooyr.github.io?tab=readme&x=1",
	}
	if !reflect.DeepEqual(included, []string{"https://example.org/article"}) || !reflect.DeepEqual(excluded, want) {
		t.Fatalf("Incorrect link partition: included=%v, excluded=%v", included, excluded)
	}
	path := filepath.Join(root, "xoutlinks", "dbx.json")
	if err := writeJSON(path, excluded); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var decoded []string
	if err := json.Unmarshal(before, &decoded); err != nil || !reflect.DeepEqual(decoded, want) {
		t.Fatalf("Excluded database is not the expected URL array: %s, %v", before, err)
	}
	// A deliberately old mtime proves an unchanged inventory is not rewritten.
	oldTime := time.Unix(1000000000, 0)
	if err := os.Chtimes(path, oldTime, oldTime); err != nil {
		t.Fatal(err)
	}
	if err := writeJSON(path, excluded); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil || !info.ModTime().Equal(oldTime) {
		t.Fatalf("Unchanged inventory was rewritten: %v", err)
	}
	if err := os.Remove(filepath.Join(root, "more.html")); err != nil {
		t.Fatal(err)
	}
	links, err = collectLinks(root)
	if err != nil {
		t.Fatal(err)
	}
	_, excluded = filterLinks(links)
	if err := writeJSON(path, excluded); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(after, &decoded); err != nil || !reflect.DeepEqual(decoded, want[:2]) {
		t.Fatalf("Inventory did not remove the deleted link: %s, %v", after, err)
	}
}

func TestExcludedLinksDatabaseUsesEmptyArray(t *testing.T) {
	_, excluded := filterLinks([]string{"https://example.org/article"})
	path := filepath.Join(t.TempDir(), "dbx.json")
	if err := writeJSON(path, excluded); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(path)
	if err != nil || string(content) != "[]\n" {
		t.Fatalf("Expected empty JSON array, got %q: %v", content, err)
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

func TestWaybackLinksAreExcludedWithoutDiscardingArchiveResults(t *testing.T) {
	ledger, path := newTestLedger(t)
	original := "https://example.com/page"
	archived := testSnapshot(original)
	if err := ledger.put(archived, []string{archived}, Entry{Status: "pending", JobID: "must-not-poll"}); err != nil {
		t.Fatal(err)
	}
	retained := Entry{Status: "archived", ArchiveURL: archived, ExpiresAt: expiryAfterSixMonths(testNow).Format(time.RFC3339)}
	if err := ledger.put(original, []string{original}, retained); err != nil {
		t.Fatal(err)
	}
	client := &fakeArchiveService{}
	if err := testRun(t, ledger, client, []string{original, archived, "https://web.archive.org/web/20200101000000/https://other.example/"}); err != nil {
		t.Fatal(err)
	}
	if got := client.counts(); got != [3]int{} {
		t.Fatalf("Archive URLs caused requests: %v", got)
	}
	entries := testReadEntries(t, path)
	if len(entries) != 1 || entries[original] != retained {
		t.Fatalf("Excluded archive inputs affected a saved result: %+v", entries)
	}
}

func TestRecordPrefersArchivedEntriesAndStripsFragments(t *testing.T) {
	base := "https://example.com/page"
	archived := Entry{Status: "archived", ArchiveURL: testSnapshot(base)}
	fragment := archived
	fragment.ArchiveURL += "#section"
	pending := Entry{Status: "pending", JobID: "job"}
	for _, test := range []struct {
		name    string
		entries map[string]Entry
	}{
		{"base archived", map[string]Entry{base: archived, base + "#section": pending}},
		{"base archived with fragment", map[string]Entry{base: fragment}},
		{"alias archived", map[string]Entry{base: pending, base + "#section": fragment}},
		{"alias only", map[string]Entry{base + "#section": fragment}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ledger := &Ledger{data: test.entries}
			if got, found := ledger.record(base); !found || got != archived {
				t.Fatalf("Expected archived base entry, got %+v, found=%v", got, found)
			}
		})
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

func TestCanceledSubmissionReusesAvailableCapture(t *testing.T) {
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
	r := runner{ledger: ledger, client: client, now: fixedTestTime, concurrency: 3}
	if err := r.run(ctx, []string{url}); err == nil {
		t.Fatal("Expected canceled submission error")
	}
	resumed := &fakeArchiveService{availableFn: func(context.Context, string) (string, error) { return testSnapshot(url), nil }}
	if err := testRun(t, ledger, resumed, []string{url}); err != nil {
		t.Fatalf("Interrupted submission did not recover its capture: %v", err)
	}
	if got := resumed.counts(); got != [3]int{1, 0, 0} {
		t.Fatalf("Interrupted capture was resubmitted: %v", got)
	}
}

func TestFailedEntriesRetryAutomatically(t *testing.T) {
	ledger, path := newTestLedger(t)
	url := "https://example.com/page"
	if err := ledger.put(url, []string{url}, Entry{Status: "failed", Error: "Unavailable"}); err != nil {
		t.Fatal(err)
	}
	client := &fakeArchiveService{}
	if err := testRun(t, ledger, client, []string{url}); err != nil {
		t.Fatal(err)
	}
	if got := client.counts(); got != [3]int{1, 1, 1} {
		t.Fatalf("Automatic retry did not run once: %v", got)
	}
	if got := testReadEntries(t, path)[url]; got.Status != "archived" || got.Error != "" {
		t.Fatalf("Automatic retry did not clear the failure: %+v", got)
	}
	if err := testRun(t, ledger, client, []string{url}); err != nil || client.counts() != [3]int{1, 1, 1} {
		t.Fatalf("Successful retry was repeated: %v, %v", client.counts(), err)
	}
}

func TestBlockedURLsAreSavedAndIgnoredOnLaterRuns(t *testing.T) {
	for _, stage := range []string{"save", "status"} {
		t.Run(stage, func(t *testing.T) {
			ledger, path := newTestLedger(t)
			blocked, good := "https://example.com/blocked", "https://example.com/good"
			client := &fakeArchiveService{
				saveFn: func(_ context.Context, raw string) (capture, error) {
					if stage == "save" && raw == blocked {
						return capture{Status: "error", StatusExt: blockedURLError}, nil
					}
					return capture{JobID: "job:" + raw}, nil
				},
				statusFn: func(_ context.Context, job string) (capture, error) {
					raw := strings.TrimPrefix(job, "job:")
					if raw == blocked {
						return capture{Status: "error", StatusExt: blockedURLError}, nil
					}
					return capture{Status: "success", Timestamp: testTimestamp, OriginalURL: raw}, nil
				},
			}
			r := runner{ledger: ledger, client: client, now: fixedTestTime, concurrency: 1}
			if err := r.run(context.Background(), []string{blocked, good}); !errors.Is(err, errIncomplete) {
				t.Fatalf("First blocked result should be reported after the batch: %v", err)
			}
			entries := testReadEntries(t, path)
			if got := entries[blocked]; got.Status != "blocked" || got.Error != blockedURLError || got.JobID != "" {
				t.Fatalf("Blocked result was not saved permanently: %+v", got)
			}
			if entries[good].Status != "archived" || len(client.availableCalls) != 2 {
				t.Fatal("Blocked result prevented other work or caused an extra lookup")
			}
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			resumed := &fakeArchiveService{}
			r.client = resumed
			if err := r.run(context.Background(), []string{blocked, good}); err != nil {
				t.Fatalf("Cached blocked URL was not ignored: %v", err)
			}
			after, err := os.ReadFile(path)
			if err != nil || resumed.counts() != [3]int{} || string(before) != string(after) {
				t.Fatalf("Blocked URL caused repeat work: %v, %v", resumed.counts(), err)
			}
		})
	}
}

func TestLoadingMigratesPreviouslyBlockedFailures(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.json")
	raw := "https://example.com/blocked"
	if err := writeJSON(path, database{Version: databaseVersion, Entries: map[string]Entry{raw: {Status: "failed", Error: blockedURLError}}}); err != nil {
		t.Fatal(err)
	}
	ledger, err := loadLedgerAt(path, testNow)
	if err != nil {
		t.Fatal(err)
	}
	client := &fakeArchiveService{}
	r := runner{ledger: ledger, client: client, now: fixedTestTime}
	if err := r.run(context.Background(), []string{raw}); err != nil || client.counts() != [3]int{} {
		t.Fatalf("Previously blocked URL was retried: %v, %v", client.counts(), err)
	}
	if got := testReadEntries(t, path)[raw]; got.Status != "blocked" || got.Error != blockedURLError {
		t.Fatalf("Blocked failure was not migrated: %+v", got)
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
	r := runner{ledger: ledger, client: client, now: fixedTestTime, concurrency: 3}
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

func TestLookupServiceFailureDoesNotCancelQueuedWork(t *testing.T) {
	ledger, path := newTestLedger(t)
	urls := []string{"https://example.com/1", "https://example.com/2", "https://example.com/3", "https://example.com/4", "https://example.com/5", "https://example.com/6"}
	client := &fakeArchiveService{availableFn: func(_ context.Context, url string) (string, error) {
		if url == urls[0] {
			return "", &serviceUnavailable{message: "service unavailable"}
		}
		return testSnapshot(url), nil
	}}
	r := runner{ledger: ledger, client: client, now: fixedTestTime, concurrency: 3}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := r.run(ctx, urls)
	var unavailable *serviceUnavailable
	if !errors.As(err, &unavailable) {
		t.Fatalf("Expected service failure: %v", err)
	}
	if got := client.counts(); got != [3]int{len(urls), 0, 0} {
		t.Fatalf("Service failure prevented remaining lookups: %v", got)
	}
	entries := testReadEntries(t, path)
	if entries[urls[0]].Status != "failed" || entries[urls[0]].Error == "" {
		t.Fatalf("Service failure was not saved: %+v", entries[urls[0]])
	}
	for _, raw := range urls[1:] {
		if entries[raw].Status != "archived" {
			t.Errorf("Remaining URL was not processed: %s", raw)
		}
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
	if cooldownErr := ledger.setCooldown(testNow.Add(time.Hour)); cooldownErr == nil || !ledger.retryAfter().IsZero() {
		t.Fatal("Failed cooldown write changed in-memory state")
	}
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
	if err := testRun(t, ledger, client, []string{old, newURL}); !errors.Is(err, errIncomplete) {
		t.Fatalf("Missing credentials should report incomplete work: %v", err)
	}
	if gets.Load() != 1 || posts.Load() != 0 {
		t.Fatalf("Unexpected anonymous requests: %d GETs, %d POSTs", gets.Load(), posts.Load())
	}
	if got := testReadEntries(t, path)[newURL].Status; got != "needs_auth" {
		t.Fatalf("New link did not retain auth requirement: %s", got)
	}
	if err := testRun(t, ledger, client, []string{old, newURL}); !errors.Is(err, errIncomplete) {
		t.Fatalf("Missing credentials should still fail on repeat runs: %v", err)
	}
	if gets.Load() != 1 || posts.Load() != 0 {
		t.Fatal("Repeat run made unnecessary HTTP requests")
	}
}

func TestSaveFormPreservesURLWithoutCapturingOutlinks(t *testing.T) {
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
	if _, enabled := got.form["capture_outlinks"]; enabled {
		t.Fatal("Save request must not enable outlink capture")
	}
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

func TestHTTPRateLimitWaitsThenRetries(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodPost} {
		t.Run(method, func(t *testing.T) {
			var requests atomic.Int32
			var retryAt time.Time
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if requests.Add(1) == 1 {
					w.Header().Set("Retry-After", "1")
					w.WriteHeader(http.StatusTooManyRequests)
					return
				}
				fmt.Fprint(w, `{}`)
			}))
			defer server.Close()
			client := &Client{httpClient: server.Client(), authorization: "LOW sample:secret"}
			client.onRateLimit = func(until time.Time) error { retryAt = until; return nil }
			var data url.Values
			if method == http.MethodPost {
				data = url.Values{"url": {"https://example.com/page"}}
			}
			var result map[string]any
			if err := client.request(context.Background(), server.URL, data, &result); err != nil {
				t.Fatal(err)
			}
			if requests.Load() != 2 || retryAt.IsZero() || time.Now().Before(retryAt) {
				t.Fatalf("Rate-limited request did not wait and retry: %d requests, retryAt=%v", requests.Load(), retryAt)
			}
		})
	}
}

func TestPendingJobContinuesPollingUntilFinished(t *testing.T) {
	ledger, path := newTestLedger(t)
	pendingURL, newURL := "https://example.com/pending", "https://example.com/new"
	entry := Entry{Status: "pending", JobID: "still-running"}
	if err := ledger.put(pendingURL, []string{pendingURL}, entry); err != nil {
		t.Fatal(err)
	}
	if err := ledger.put(newURL, []string{newURL}, Entry{Status: "ready"}); err != nil {
		t.Fatal(err)
	}
	polls := 0
	client := &fakeArchiveService{statusFn: func(_ context.Context, job string) (capture, error) {
		if job == entry.JobID {
			polls++
			if polls <= 100 {
				return capture{Status: "pending"}, nil
			}
			return capture{Status: "success", Timestamp: testTimestamp, OriginalURL: pendingURL}, nil
		}
		return capture{Status: "success", Timestamp: testTimestamp, OriginalURL: newURL}, nil
	}}
	r := runner{ledger: ledger, client: client, now: fixedTestTime, concurrency: 1}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := r.run(ctx, []string{pendingURL, newURL}); err != nil {
		t.Fatal(err)
	}
	if got := client.counts(); got != [3]int{0, 1, 102} || polls != 101 {
		t.Fatalf("Polling stopped early or resubmitted the pending job: %v, %d polls", got, polls)
	}
	for _, raw := range []string{pendingURL, newURL} {
		if stored := testReadEntries(t, path)[raw]; stored.Status != "archived" || stored.ArchiveURL != testSnapshot(raw) {
			t.Fatalf("Capture did not finish: %+v", stored)
		}
	}
}

func TestPendingJobWaitCanBeCanceledAndResumed(t *testing.T) {
	ledger, path := newTestLedger(t)
	pendingURL, newURL := "https://example.com/pending", "https://example.com/new"
	entry := Entry{Status: "pending", JobID: "job:" + pendingURL}
	if err := ledger.put(pendingURL, []string{pendingURL}, entry); err != nil {
		t.Fatal(err)
	}
	if err := ledger.put(newURL, []string{newURL}, Entry{Status: "ready"}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	polls := 0
	client := &fakeArchiveService{statusFn: func(context.Context, string) (capture, error) {
		polls++
		if polls == 100 {
			cancel()
		}
		return capture{Status: "pending"}, nil
	}}
	r := runner{ledger: ledger, client: client, now: fixedTestTime, concurrency: 1}
	if err := r.run(ctx, []string{pendingURL, newURL}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Cancellation did not stop polling: %v", err)
	}
	if got := client.counts(); got != [3]int{0, 0, 100} {
		t.Fatalf("Cancellation submitted new work or stopped polling early: %v", got)
	}
	stored := testReadEntries(t, path)[pendingURL]
	if stored != entry {
		t.Fatalf("Lost pending capture: %+v", stored)
	}
	resumed := &fakeArchiveService{}
	if err := testRun(t, ledger, resumed, []string{pendingURL, newURL}); err != nil {
		t.Fatal(err)
	}
	if got := resumed.counts(); got != [3]int{0, 1, 2} {
		t.Fatalf("Resuming duplicated a pending submission: %v", got)
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

func TestFutureCooldownCanBeCanceledWithoutRequestsOrChanges(t *testing.T) {
	ledger, path := newTestLedger(t)
	raw := "https://example.com/pending"
	until := testNow.Add(time.Hour)
	entry := Entry{Status: "pending", JobID: "keep-this-job"}
	if err := ledger.put(raw, []string{raw}, entry); err != nil {
		t.Fatal(err)
	}
	if err := ledger.setCooldown(until); err != nil {
		t.Fatal(err)
	}
	ledger, err := loadLedgerAt(path, testNow)
	if err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(path)
	client := &fakeArchiveService{}
	var output strings.Builder
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	r := runner{ledger: ledger, client: client, now: fixedTestTime, output: &output}
	if err := r.run(ctx, []string{raw}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Cooldown did not wait for cancellation: %v", err)
	}
	after, _ := os.ReadFile(path)
	if client.counts() != [3]int{} || string(before) != string(after) {
		t.Fatal("Canceled cooldown performed network work or changed the database")
	}
	want := "wayback rate limit; waiting until " + until.Local().Format("2006-01-02 15:04:05 MST") + ".\n"
	if output.String() != want {
		t.Fatalf("Startup cooldown message = %q, want %q", output.String(), want)
	}
}

func TestLoadClearsExpiredCooldownWithoutChangingEntries(t *testing.T) {
	for _, until := range []time.Time{testNow.Add(-time.Minute), testNow} {
		t.Run(until.Format(time.RFC3339), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "db.json")
			entries := map[string]Entry{"https://example.com/pending": {Status: "pending", JobID: "keep-this-job", Error: "keep this error"}}
			if err := writeJSON(path, database{Version: databaseVersion, RetryAfter: until.Format(time.RFC3339), Entries: entries}); err != nil {
				t.Fatal(err)
			}
			ledger, err := loadLedgerAt(path, testNow)
			if err != nil {
				t.Fatal(err)
			}
			stored := testReadDatabase(t, path)
			if !ledger.retryAfter().IsZero() || stored.RetryAfter != "" || !reflect.DeepEqual(stored.Entries, entries) {
				t.Fatalf("Expired cooldown was not cleared cleanly: %+v", stored)
			}
			marker := time.Unix(1000000000, 0)
			if err := os.Chtimes(path, marker, marker); err != nil {
				t.Fatal(err)
			}
			if _, err := loadLedgerAt(path, testNow); err != nil {
				t.Fatal(err)
			}
			info, err := os.Stat(path)
			if err != nil || !info.ModTime().Equal(marker) {
				t.Fatalf("Loading a cleared database rewrote it: %v", err)
			}
		})
	}
}

func TestCooldownCleanupPreservesNewerDeadline(t *testing.T) {
	ledger, path := newTestLedger(t)
	until := testNow.Add(time.Hour)
	if err := ledger.setCooldown(until); err != nil {
		t.Fatal(err)
	}
	if err := ledger.clearExpiredCooldown(testNow); err != nil {
		t.Fatal(err)
	}
	if !ledger.retryAfter().Equal(until) || testReadDatabase(t, path).RetryAfter != until.Format(time.RFC3339) {
		t.Fatal("Cleanup removed a deadline that is still in the future")
	}
}

func TestRateLimitPersistsCooldownWithoutLosingPendingJob(t *testing.T) {
	ledger, path := newTestLedger(t)
	url := "https://example.com/pending"
	until := testNow.Add(5 * time.Minute)
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
	if stored.Status != "pending" || stored.JobID != entry.JobID || stored.RetryAfter != "" || testReadDatabase(t, path).RetryAfter != until.Format(time.RFC3339) {
		t.Fatalf("Cooldown did not preserve pending state: %+v", stored)
	}
	if !ledger.retryAfter().Equal(until) {
		t.Fatal("Persisted cooldown was not loaded")
	}
	before, _ := os.ReadFile(path)
	counts := client.counts()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	r := runner{ledger: ledger, client: client, now: fixedTestTime}
	if err := r.run(ctx, []string{url}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Expected repeated run to wait during cooldown: %v", err)
	}
	after, _ := os.ReadFile(path)
	if client.counts() != counts || string(before) != string(after) {
		t.Fatal("Repeat during cooldown made requests or changed state")
	}
}

func TestCooldownCanNeverBeShortened(t *testing.T) {
	ledger, path := newTestLedger(t)
	url := "https://example.com/pending"
	until := testNow.Add(10 * time.Minute)
	if err := ledger.put(url, []string{url}, Entry{Status: "pending", JobID: "keep-this-job"}); err != nil {
		t.Fatal(err)
	}
	if err := ledger.setCooldown(until); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(path)
	if err := ledger.setCooldown(until.Add(-5 * time.Minute)); err != nil {
		t.Fatal(err)
	}
	after, _ := os.ReadFile(path)
	if !ledger.retryAfter().Equal(until) || string(before) != string(after) {
		t.Fatal("Shorter retry interval changed the existing cooldown")
	}
}

func TestRateLimitBeforeFirstResultRetainsGlobalCooldown(t *testing.T) {
	ledger, path := newTestLedger(t)
	url := "https://example.com/new"
	until := testNow.Add(5 * time.Minute)
	client := &fakeArchiveService{availableFn: func(context.Context, string) (string, error) {
		return "", &serviceUnavailable{message: "Rate limited", retryAt: until}
	}}
	if err := testRun(t, ledger, client, []string{url}); err == nil {
		t.Fatal("Expected rate limit")
	}
	stored := testReadDatabase(t, path)
	if stored.Entries[url].Status != "failed" || stored.Entries[url].Error == "" || stored.RetryAfter != until.Format(time.RFC3339) {
		t.Fatalf("First rate limit lost its global cooldown: %+v", stored)
	}
	resumed := &fakeArchiveService{availableFn: func(context.Context, string) (string, error) {
		return testSnapshot(url), nil
	}}
	r := runner{ledger: ledger, client: resumed, now: func() time.Time { return until.Add(time.Second) }}
	if err := r.run(context.Background(), []string{url}); err != nil {
		t.Fatalf("Expired cooldown did not allow the unprocessed link to run: %v", err)
	}
	if got := resumed.counts(); got != [3]int{1, 0, 0} {
		t.Fatalf("Unexpected work after cooldown: %v", got)
	}
}

func TestBlogSourceRepositoryIsExcludedPrecisely(t *testing.T) {
	excluded := []string{
		"https://github.com/nhooyr/nhooyr.github.io",
		"https://github.com/nhooyr/nhooyr.github.io/",
		"http://github.com/nhooyr/nhooyr.github.io?tab=readme#top",
		"https://github.com/nhooyr/nhooyr.github.io/blob/main/index.html",
		"https://WWW.GITHUB.COM/NHoOyR/NHoOyR.GITHUB.IO/tree/main",
	}
	retained := []string{
		"https://github.com/nhooyr",
		"https://github.com/nhooyr/dotfiles",
		"https://github.com/nhooyr/nhooyr.github.io-other",
		"https://github.com/other-user/nhooyr.github.io",
		"https://github.com.evil.test/nhooyr/nhooyr.github.io",
		"https://gist.github.com/nhooyr/nhooyr.github.io",
	}
	for _, raw := range excluded {
		if !excludedLink(raw) {
			t.Errorf("Blog source link was not excluded: %s", raw)
		}
	}
	for _, raw := range retained {
		if excludedLink(raw) {
			t.Errorf("Unrelated GitHub link was excluded: %s", raw)
		}
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
	got, _ := filterLinks(extractLinks(content.String()))
	sort.Strings(retained)
	if !reflect.DeepEqual(got, retained) {
		t.Fatalf("HTML extraction did not apply Wikipedia exclusions precisely: %v", got)
	}
}

func TestAllOtherLinksRemainEligible(t *testing.T) {
	for _, raw := range []string{
		"https://leqra.com", "https://www.facebook.com/example", "https://instagram.com/example",
		"https://discord.gg/invite", "http://m.me/example", "https://wa.me/example",
		"https://signal.me/#example", "https://t.me/example", "https://snapchat.com/t/example",
		"https://us05web.zoom.us/j/123?pwd=example", "https://open.spotify.com/user/example",
		"https://www.youtube.com/watch?v=example", "https://www.reddit.com/user/example",
		"https://chatgpt.com/u/example", "https://cplusplus.com/user/example",
		"https://stackexchange.com/users/123/example", "https://powdertoy.co.uk/User.html?Name=example",
		"https://github.com/search?q=involves%3Anhooyr&type=issues",
		"https://www.linkedin.com/in/example", "https://www.threads.com/example",
		"https://www.tiktok.com/@example", "https://www.pinterest.com/example", "https://x.com/example",
		"https://www.flickr.com/people/example",
	} {
		if excludedLink(raw) {
			t.Errorf("Unexpected exclusion: %s", raw)
		}
	}
}

func TestExcludedLinksArePrunedAndNeverRequested(t *testing.T) {
	ledger, path := newTestLedger(t)
	excluded := []string{
		"https://github.com/nhooyr/nhooyr.github.io/blob/main/index.html",
		"https://en.wikipedia.org/wiki/Example",
	}
	for _, raw := range excluded {
		if err := ledger.put(raw, []string{raw}, Entry{Status: "pending", JobID: "must-not-poll"}); err != nil {
			t.Fatal(err)
		}
	}
	eligible := "https://leqra.com"
	client := &fakeArchiveService{availableFn: func(_ context.Context, raw string) (string, error) {
		return testSnapshot(raw), nil
	}}
	urls := append(append([]string(nil), excluded...), eligible)
	if err := testRun(t, ledger, client, urls); err != nil {
		t.Fatal(err)
	}
	if got := client.counts(); got != [3]int{1, 0, 0} || !reflect.DeepEqual(client.availableCalls, []string{eligible}) {
		t.Fatalf("Excluded links caused requests: %v, %v", got, client.availableCalls)
	}
	entries := testReadEntries(t, path)
	if len(entries) != 1 || entries[eligible].ArchiveURL != testSnapshot(eligible) {
		t.Fatalf("Excluded records were retained or eligible record lost: %+v", entries)
	}
}

func TestExcludedLedgerEntriesArePrunedWithoutRepeatWrites(t *testing.T) {
	ledger, path := newTestLedger(t)
	excluded := []string{
		"https://github.com/nhooyr/nhooyr.github.io",
		"https://GitHub.Com/nhooyr/nhooyr.github.io?tab=readme",
		"https://github.com/nhooyr/nhooyr.github.io/blob/main/index.html",
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
	old := "https://github.com/nhooyr/nhooyr.github.io"
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
		old, "https://github.com/nhooyr/nhooyr.github.io?tab=repositories", "https://github.com/nhooyr/nhooyr.github.io/tree/main",
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
	excluded, retained := "https://github.com/nhooyr/nhooyr.github.io", "https://example.com/pending"
	until := testNow.Add(5 * time.Minute)
	if err := ledger.put(excluded, []string{excluded}, Entry{Status: "pending", JobID: "excluded-job"}); err != nil {
		t.Fatal(err)
	}
	if err := ledger.setCooldown(until); err != nil {
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
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	r := runner{ledger: ledger, client: client, now: fixedTestTime}
	if err := r.run(ctx, []string{retained}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Pruning bypassed the active global cooldown: %v", err)
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
	first := testNow.Add(5 * time.Minute)
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
		// Concurrent requests can return different retry deadlines.
		return capture{}, &serviceUnavailable{message: "Longer limit", retryAt: longest}
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	r := runner{ledger: ledger, client: client, now: fixedTestTime, concurrency: 2}
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

func TestExclusionReasonAgreesWithMatching(t *testing.T) {
	for _, raw := range []string{"https://en.wikipedia.org/wiki/Example", "https://github.com/nhooyr/nhooyr.github.io"} {
		if exclusionReason(raw) == "" || !excludedLink(raw) {
			t.Errorf("Excluded URL has no explanation: %s", raw)
		}
	}
	for _, raw := range []string{"https://example.org/article", "https://leqra.com", "https://github.com/nhooyr", "https://instagram.com/example"} {
		if reason := exclusionReason(raw); reason != "" || excludedLink(raw) {
			t.Errorf("Eligible URL has an exclusion reason: %s: %s", raw, reason)
		}
	}
}

func TestLegacyMigrationBackfillsFromTodayOnlyOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.json")
	oldArchive := "https://web.archive.org/web/20100101000000/https://example.com/old"
	existingExpiry := testNow.AddDate(0, 1, 0).Format(time.RFC3339)
	legacy := map[string]Entry{
		"https://example.com/old":      {Status: "archived", ArchiveURL: oldArchive},
		"https://example.com/pending":  {Status: "pending", ArchiveURL: oldArchive, JobID: "existing-job"},
		"https://example.com/failed":   {Status: "failed", ArchiveURL: oldArchive, Error: "old error"},
		"https://example.com/existing": {Status: "archived", ArchiveURL: oldArchive, ExpiresAt: existingExpiry},
		"https://example.com/unsaved":  {Status: "ready"},
	}
	content, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, content, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadLedgerAt(path, testNow); err != nil {
		t.Fatal(err)
	}
	entries := testReadEntries(t, path)
	for raw, original := range legacy {
		want := original
		if want.ArchiveURL != "" && want.ExpiresAt == "" {
			want.ExpiresAt = expiryAfterSixMonths(testNow).Format(time.RFC3339)
		}
		if got := entries[raw]; got != want {
			t.Errorf("Migration changed the wrong fields for %s: got %+v, want %+v", raw, got, want)
		}
	}
	before, _ := os.ReadFile(path)
	if _, err := loadLedgerAt(path, testNow.AddDate(0, 1, 0)); err != nil {
		t.Fatal(err)
	}
	after, _ := os.ReadFile(path)
	if string(before) != string(after) {
		t.Fatal("Reloading a migrated database extended expiry or rewrote state")
	}
}

func TestVersionedMissingExpiryStaysExpiredAndRefreshes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.json")
	raw := "https://example.com/old"
	oldArchive := "https://web.archive.org/web/20100101000000/" + raw
	stored := database{Version: databaseVersion, Entries: map[string]Entry{raw: {Status: "archived", ArchiveURL: oldArchive}}}
	content, _ := json.Marshal(stored)
	if err := os.WriteFile(path, content, 0600); err != nil {
		t.Fatal(err)
	}
	ledger, err := loadLedgerAt(path, testNow)
	if err != nil {
		t.Fatal(err)
	}
	afterLoad, _ := os.ReadFile(path)
	entry, _ := ledger.record(raw)
	if entry.ExpiresAt != "" || string(content) != string(afterLoad) {
		t.Fatal("Versioned entry was incorrectly given a migration grace period")
	}
	client := &fakeArchiveService{}
	if err := testRun(t, ledger, client, []string{raw}); err != nil {
		t.Fatal(err)
	}
	if got := client.counts(); got != [3]int{0, 1, 1} {
		t.Fatalf("Missing expiry did not directly refresh: %v", got)
	}
	if got := testReadEntries(t, path)[raw]; got.ArchiveURL != testSnapshot(raw) || got.ExpiresAt != expiryAfterSixMonths(testNow).Format(time.RFC3339) {
		t.Fatalf("Missing-expiry refresh did not save its new expiry: %+v", got)
	}
}

func TestFreshExpirySkipsOldSnapshotWithoutRewriting(t *testing.T) {
	ledger, path := newTestLedger(t)
	raw := "https://example.com/old"
	entry := Entry{
		Status: "archived", ArchiveURL: "https://web.archive.org/web/20100101000000/" + raw,
		ExpiresAt: testNow.Add(time.Second).Format(time.RFC3339),
	}
	if err := ledger.put(raw, []string{raw}, entry); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(path)
	client := &fakeArchiveService{}
	if err := testRun(t, ledger, client, []string{raw}); err != nil {
		t.Fatal(err)
	}
	after, _ := os.ReadFile(path)
	if client.counts() != [3]int{} || string(before) != string(after) {
		t.Fatal("Fresh expiry caused a request or file rewrite based on old snapshot age")
	}
}

func TestSixMonthExpiryClampsCalendarMonthEnd(t *testing.T) {
	for _, test := range []struct{ from, want string }{
		{"2026-08-31T12:34:56Z", "2027-02-28T12:34:56Z"},
		{"2023-08-31T12:34:56Z", "2024-02-29T12:34:56Z"},
		{"2026-10-31T12:34:56Z", "2027-04-30T12:34:56Z"},
		{"2024-02-29T12:34:56Z", "2024-08-29T12:34:56Z"},
		{"2026-11-18T12:34:56Z", "2027-05-18T12:34:56Z"},
	} {
		t.Run(test.from, func(t *testing.T) {
			from, _ := time.Parse(time.RFC3339, test.from)
			if got := expiryAfterSixMonths(from).Format(time.RFC3339); got != test.want {
				t.Fatalf("Expiry is %s, want %s", got, test.want)
			}
		})
	}
}

func TestExpiredRefreshRetainsBackupUntilSuccessAndUpdatesAliases(t *testing.T) {
	ledger, path := newTestLedger(t)
	base := "https://example.com/page"
	aliases := []string{base + "#one", base + "#two"}
	oldArchive := "https://web.archive.org/web/20100101000000/" + base
	expired := testNow.Format(time.RFC3339)
	if err := ledger.put(base, aliases, Entry{Status: "archived", ArchiveURL: oldArchive, ExpiresAt: expired}); err != nil {
		t.Fatal(err)
	}
	checkBackup := func(status string) {
		entries := testReadEntries(t, path)
		for _, alias := range aliases {
			_, fragment := splitFragment(alias)
			got := entries[alias]
			if got.Status != status || got.ArchiveURL != oldArchive+"#"+fragment || got.ExpiresAt != expired || got.RefreshStartedAt != testNow.Format(time.RFC3339) {
				t.Errorf("Refresh lost backup during %s: %+v", status, got)
			}
		}
	}
	client := &fakeArchiveService{
		saveFn: func(_ context.Context, raw string) (capture, error) {
			checkBackup("submitting")
			return capture{JobID: "job:" + raw}, nil
		},
		statusFn: func(_ context.Context, _ string) (capture, error) {
			checkBackup("pending")
			return capture{Status: "success", Timestamp: testTimestamp, OriginalURL: base}, nil
		},
	}
	if err := testRun(t, ledger, client, aliases); err != nil {
		t.Fatal(err)
	}
	if got := client.counts(); got != [3]int{0, 1, 1} {
		t.Fatalf("Expired aliases did not share one direct refresh: %v", got)
	}
	for alias, got := range testReadEntries(t, path) {
		if got.Status != "archived" || got.ArchiveURL != testSnapshot(alias) || got.ExpiresAt != expiryAfterSixMonths(testNow).Format(time.RFC3339) || got.RefreshStartedAt != "" {
			t.Errorf("Successful refresh did not replace backup: %+v", got)
		}
	}
	if err := testRun(t, ledger, client, aliases); err != nil || client.counts() != [3]int{0, 1, 1} {
		t.Fatalf("Freshly refreshed aliases triggered repeat work: %v, %v", client.counts(), err)
	}
}

func TestFailedRefreshRetainsBackupAndRetriesOncePerRun(t *testing.T) {
	ledger, path := newTestLedger(t)
	raw := "https://example.com/page"
	oldArchive := "https://web.archive.org/web/20100101000000/" + raw
	expired := testNow.Add(-time.Second).Format(time.RFC3339)
	if err := ledger.put(raw, []string{raw}, Entry{Status: "archived", ArchiveURL: oldArchive, ExpiresAt: expired}); err != nil {
		t.Fatal(err)
	}
	client := &fakeArchiveService{
		statusFn: func(context.Context, string) (capture, error) {
			return capture{Status: "error", StatusExt: "error:target-not-found"}, nil
		},
		availableFn: func(context.Context, string) (string, error) { return oldArchive, nil },
	}
	if err := testRun(t, ledger, client, []string{raw}); !errors.Is(err, errIncomplete) {
		t.Fatalf("Expected failed refresh: %v", err)
	}
	entry := testReadEntries(t, path)[raw]
	if entry.Status != "failed" || entry.ArchiveURL != oldArchive || entry.ExpiresAt != expired {
		t.Fatalf("Failed refresh discarded or renewed old backup: %+v", entry)
	}
	before, _ := os.ReadFile(path)
	counts := client.counts()
	if err := testRun(t, ledger, client, []string{raw}); !errors.Is(err, errIncomplete) {
		t.Fatalf("Expected retry of failed refresh: %v", err)
	}
	after, _ := os.ReadFile(path)
	want := [3]int{counts[0] + 2, counts[1] + 1, counts[2] + 1}
	if client.counts() != want || string(before) != string(after) {
		t.Fatal("Failed refresh lost its backup or retried more than once")
	}
}

func TestRefreshSnapshotMustBeWithinServerDedupWindow(t *testing.T) {
	for _, test := range []struct {
		name string
		age  time.Duration
		good bool
	}{
		{"exactly one hour", time.Hour, true},
		{"older than one hour", time.Hour + time.Second, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			ledger, path := newTestLedger(t)
			raw := "https://example.com/page"
			oldArchive := "https://web.archive.org/web/20100101000000/" + raw
			expired := testNow.Format(time.RFC3339)
			if err := ledger.put(raw, []string{raw}, Entry{Status: "archived", ArchiveURL: oldArchive, ExpiresAt: expired}); err != nil {
				t.Fatal(err)
			}
			timestamp := testNow.Add(-test.age).Format("20060102150405")
			client := &fakeArchiveService{statusFn: func(context.Context, string) (capture, error) {
				return capture{Status: "success", Timestamp: timestamp, OriginalURL: raw}, nil
			}}
			err := testRun(t, ledger, client, []string{raw})
			entry := testReadEntries(t, path)[raw]
			if test.good {
				if err != nil || entry.Status != "archived" || entry.ExpiresAt != expiryAfterSixMonths(testNow).Format(time.RFC3339) {
					t.Fatalf("Recent deduplicated snapshot was rejected: %+v, %v", entry, err)
				}
			} else {
				if !errors.Is(err, errIncomplete) || entry.Status != "failed" || entry.ArchiveURL != oldArchive || entry.ExpiresAt != expired {
					t.Fatalf("Old snapshot incorrectly renewed backup: %+v, %v", entry, err)
				}
				counts := client.counts()
				want := [3]int{counts[0] + 1, counts[1] + 1, counts[2] + 1}
				if err := testRun(t, ledger, client, []string{raw}); !errors.Is(err, errIncomplete) || client.counts() != want {
					t.Fatal("Rejected old snapshot did not receive exactly one refresh attempt")
				}
			}
		})
	}
}

func TestPendingRefreshResumesWithoutAnotherSave(t *testing.T) {
	ledger, path := newTestLedger(t)
	raw := "https://example.com/page"
	old := Entry{
		Status: "pending", JobID: "job:" + raw, ArchiveURL: "https://web.archive.org/web/20100101000000/" + raw,
		ExpiresAt: testNow.Format(time.RFC3339), RefreshStartedAt: testNow.Format(time.RFC3339),
	}
	if err := ledger.put(raw, []string{raw}, old); err != nil {
		t.Fatal(err)
	}
	client := &fakeArchiveService{statusFn: func(context.Context, string) (capture, error) {
		if got := testReadEntries(t, path)[raw]; got != old {
			t.Errorf("Resuming pending refresh changed backup: %+v", got)
		}
		return capture{Status: "success", Timestamp: testTimestamp, OriginalURL: raw}, nil
	}}
	if err := testRun(t, ledger, client, []string{raw}); err != nil || client.counts() != [3]int{0, 0, 1} {
		t.Fatalf("Pending refresh was not resumed directly: %v, %v", client.counts(), err)
	}
}

func TestCanceledRefreshRetriesWithoutLosingBackup(t *testing.T) {
	ledger, path := newTestLedger(t)
	raw := "https://example.com/page"
	oldArchive := "https://web.archive.org/web/20100101000000/" + raw
	expired := testNow.Format(time.RFC3339)
	if err := ledger.put(raw, []string{raw}, Entry{Status: "archived", ArchiveURL: oldArchive, ExpiresAt: expired}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	client := &fakeArchiveService{saveFn: func(context.Context, string) (capture, error) {
		cancel()
		return capture{}, context.Canceled
	}}
	r := runner{ledger: ledger, client: client, now: fixedTestTime, concurrency: 1}
	if err := r.run(ctx, []string{raw}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Expected canceled refresh: %v", err)
	}
	entry := testReadEntries(t, path)[raw]
	if entry.Status != "submitting" || entry.ArchiveURL != oldArchive || entry.ExpiresAt != expired {
		t.Fatalf("Canceled refresh lost its backup/checkpoint: %+v", entry)
	}
	resumed := &fakeArchiveService{
		availableFn: func(context.Context, string) (string, error) { return oldArchive, nil },
		saveFn: func(context.Context, string) (capture, error) {
			if got := testReadEntries(t, path)[raw]; got.ArchiveURL != oldArchive || got.ExpiresAt != expired {
				t.Errorf("Retry discarded the previous backup: %+v", got)
			}
			return capture{JobID: "job:" + raw}, nil
		},
	}
	if err := testRun(t, ledger, resumed, []string{raw}); err != nil {
		t.Fatalf("Interrupted refresh did not retry: %v", err)
	}
	entry = testReadEntries(t, path)[raw]
	if resumed.counts() != [3]int{1, 1, 1} || entry.Status != "archived" || entry.ArchiveURL != testSnapshot(raw) {
		t.Fatalf("Interrupted refresh did not save the fresh capture: %+v, %v", entry, resumed.counts())
	}
}

func TestUnsupportedDatabaseVersionDoesNotRewriteFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.json")
	content, err := json.Marshal(database{Version: databaseVersion + 1, Entries: map[string]Entry{}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, content, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadLedgerAt(path, testNow); err == nil {
		t.Fatal("Newer database version was accepted")
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != string(after) {
		t.Fatal("Unsupported database version was rewritten")
	}
}

func TestPendingBackupWithoutRefreshStartCannotRenewFromOldSnapshot(t *testing.T) {
	for _, expiry := range []string{"", testNow.Add(-time.Hour).Format(time.RFC3339)} {
		t.Run("expiry="+expiry, func(t *testing.T) {
			ledger, path := newTestLedger(t)
			raw := "https://example.com/page"
			oldArchive := "https://web.archive.org/web/20100101000000/" + raw
			entry := Entry{Status: "pending", JobID: "old-job", ArchiveURL: oldArchive, ExpiresAt: expiry}
			if err := ledger.put(raw, []string{raw}, entry); err != nil {
				t.Fatal(err)
			}
			client := &fakeArchiveService{statusFn: func(context.Context, string) (capture, error) {
				return capture{Status: "success", Timestamp: "20100101000000", OriginalURL: raw}, nil
			}}
			if err := testRun(t, ledger, client, []string{raw}); !errors.Is(err, errIncomplete) {
				t.Fatalf("Old pending snapshot was accepted: %v", err)
			}
			stored := testReadEntries(t, path)[raw]
			if stored.Status != "failed" || stored.ArchiveURL != oldArchive || stored.ExpiresAt != expiry || stored.RefreshStartedAt != testNow.Format(time.RFC3339) {
				t.Fatalf("Missing refresh boundary incorrectly renewed or discarded the backup: %+v", stored)
			}
			if got := client.counts(); got != [3]int{0, 0, 1} {
				t.Fatalf("Pending backup caused unexpected work: %v", got)
			}
		})
	}
}

func TestAutomaticRefreshRetryResetsOldFreshnessBoundary(t *testing.T) {
	ledger, path := newTestLedger(t)
	raw := "https://example.com/page"
	oldTimestamp := testNow.Add(-24 * time.Hour).Format("20060102150405")
	oldArchive := "https://web.archive.org/web/" + oldTimestamp + "/" + raw
	entry := Entry{
		Status: "failed", Error: "previous refresh failed", ArchiveURL: oldArchive,
		ExpiresAt:        testNow.Add(-time.Hour).Format(time.RFC3339),
		RefreshStartedAt: testNow.Add(-48 * time.Hour).Format(time.RFC3339),
	}
	if err := ledger.put(raw, []string{raw}, entry); err != nil {
		t.Fatal(err)
	}
	client := &fakeArchiveService{
		availableFn: func(context.Context, string) (string, error) { return oldArchive, nil },
		saveFn: func(context.Context, string) (capture, error) {
			if stored := testReadEntries(t, path)[raw]; stored.RefreshStartedAt != testNow.Format(time.RFC3339) {
				t.Errorf("Automatic retry retained an obsolete freshness boundary: %+v", stored)
			}
			return capture{JobID: "job:" + raw}, nil
		},
	}
	r := runner{ledger: ledger, client: client, now: fixedTestTime, concurrency: 1}
	if err := r.run(context.Background(), []string{raw}); err != nil {
		t.Fatal(err)
	}
	if got := client.counts(); got != [3]int{1, 1, 1} {
		t.Fatalf("Retry accepted the old snapshot instead of refreshing: %v", got)
	}
	stored := testReadEntries(t, path)[raw]
	if stored.Status != "archived" || stored.ArchiveURL != testSnapshot(raw) || stored.ExpiresAt != expiryAfterSixMonths(testNow).Format(time.RFC3339) {
		t.Fatalf("Automatic retry did not renew from the fresh capture: %+v", stored)
	}
}

func TestKnownNeedsAuthWithoutCredentialsErrorsWithoutChanges(t *testing.T) {
	ledger, path := newTestLedger(t)
	raw := "https://example.com/page"
	entry := Entry{Status: "needs_auth", Error: "Credentials required"}
	if err := ledger.put(raw, []string{raw}, entry); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(path)
	client := &fakeArchiveService{unauthenticated: true}
	if err := testRun(t, ledger, client, []string{raw}); !errors.Is(err, errIncomplete) {
		t.Fatalf("Missing credentials should fail the run: %v", err)
	}
	after, _ := os.ReadFile(path)
	if client.counts() != [3]int{} || string(before) != string(after) {
		t.Fatal("Known authentication requirement caused requests or a file rewrite")
	}
}

func TestCredentialsAutomaticallyResumeNeedsAuthWithoutLookup(t *testing.T) {
	ledger, path := newTestLedger(t)
	raw := "https://example.com/page"
	if err := ledger.put(raw, []string{raw}, Entry{Status: "needs_auth", Error: "Credentials required"}); err != nil {
		t.Fatal(err)
	}
	client := &fakeArchiveService{}
	if err := testRun(t, ledger, client, []string{raw}); err != nil {
		t.Fatal(err)
	}
	if got := client.counts(); got != [3]int{0, 1, 1} {
		t.Fatalf("Adding credentials repeated the lookup or failed to resume: %v", got)
	}
	if got := testReadEntries(t, path)[raw]; got.Status != "archived" || got.ArchiveURL != testSnapshot(raw) {
		t.Fatalf("Authentication-resumed capture was not saved: %+v", got)
	}
}

func TestMissingCredentialsPreserveExpiredBackupUntilAutomaticResume(t *testing.T) {
	ledger, path := newTestLedger(t)
	raw := "https://example.com/page"
	oldArchive := "https://web.archive.org/web/20100101000000/" + raw
	expiry := testNow.Add(-time.Hour).Format(time.RFC3339)
	if err := ledger.put(raw, []string{raw}, Entry{Status: "archived", ArchiveURL: oldArchive, ExpiresAt: expiry}); err != nil {
		t.Fatal(err)
	}
	client := &fakeArchiveService{unauthenticated: true}
	if err := testRun(t, ledger, client, []string{raw}); !errors.Is(err, errIncomplete) {
		t.Fatalf("Expired backup requiring credentials should report incomplete work: %v", err)
	}
	entry := testReadEntries(t, path)[raw]
	if client.counts() != [3]int{} || entry.Status != "needs_auth" || entry.ArchiveURL != oldArchive || entry.ExpiresAt != expiry {
		t.Fatalf("Authentication deferral lost the backup or performed work: %+v, %v", entry, client.counts())
	}
	before, _ := os.ReadFile(path)
	if err := testRun(t, ledger, client, []string{raw}); !errors.Is(err, errIncomplete) {
		t.Fatalf("Missing credentials should still fail on repeat runs: %v", err)
	}
	after, _ := os.ReadFile(path)
	if client.counts() != [3]int{} || string(before) != string(after) {
		t.Fatal("Repeated missing credentials caused work")
	}
	authenticated := &fakeArchiveService{}
	if err := testRun(t, ledger, authenticated, []string{raw}); err != nil {
		t.Fatal(err)
	}
	entry = testReadEntries(t, path)[raw]
	if authenticated.counts() != [3]int{0, 1, 1} || entry.Status != "archived" || entry.ArchiveURL != testSnapshot(raw) {
		t.Fatalf("Providing credentials did not resume the backup refresh: %+v, %v", entry, authenticated.counts())
	}
}

func TestPendingJobsStillPollWithoutCredentials(t *testing.T) {
	ledger, _ := newTestLedger(t)
	raw := "https://example.com/page"
	if err := ledger.put(raw, []string{raw}, Entry{Status: "pending", JobID: "job:" + raw}); err != nil {
		t.Fatal(err)
	}
	client := &fakeArchiveService{unauthenticated: true}
	if err := testRun(t, ledger, client, []string{raw}); err != nil || client.counts() != [3]int{0, 0, 1} {
		t.Fatalf("Credentials prevented a submitted job from finishing: %v, %v", client.counts(), err)
	}
}

func TestDeferredAuthenticationDoesNotHideOtherFailures(t *testing.T) {
	ledger, _ := newTestLedger(t)
	blocked, failed := "https://example.com/blocked", "https://example.com/failed"
	if err := ledger.put(blocked, []string{blocked}, Entry{Status: "needs_auth", Error: "Credentials required"}); err != nil {
		t.Fatal(err)
	}
	if err := ledger.put(failed, []string{failed}, Entry{Status: "failed", Error: "Capture failed"}); err != nil {
		t.Fatal(err)
	}
	client := &fakeArchiveService{unauthenticated: true}
	if err := testRun(t, ledger, client, []string{blocked, failed}); !errors.Is(err, errIncomplete) {
		t.Fatalf("Authentication deferral hid an actual failed capture: %v", err)
	}
	if client.counts() != [3]int{1, 0, 0} || !reflect.DeepEqual(client.availableCalls, []string{failed}) {
		t.Fatal("Failed capture was not rechecked, or was submitted without credentials")
	}
}

func TestAuthenticationRejectionReportsErrorAndDefersRemainingPosts(t *testing.T) {
	ledger, path := newTestLedger(t)
	var posts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			posts.Add(1)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		fmt.Fprint(w, `{"archived_snapshots":{}}`)
	}))
	defer server.Close()
	client := &Client{httpClient: server.Client(), availabilityURL: server.URL, saveURL: server.URL, authorization: "LOW sample-access:sample-secret"}
	urls := []string{"https://example.com/first", "https://example.com/second"}
	r := runner{ledger: ledger, client: client, now: fixedTestTime, concurrency: 1}
	if err := r.run(context.Background(), urls); !errors.Is(err, errIncomplete) {
		t.Fatalf("Authentication rejection should report incomplete work: %v", err)
	}
	if posts.Load() != 1 || client.authenticated() {
		t.Fatalf("Rejected credentials caused additional posts: %d", posts.Load())
	}
	for _, raw := range urls {
		if got := testReadEntries(t, path)[raw].Status; got != "needs_auth" {
			t.Errorf("Remaining capture was not deferred: %s: %s", raw, got)
		}
	}
	_, err := client.save(context.Background(), "https://example.com/third")
	var auth *authenticationError
	if !errors.As(err, &auth) || posts.Load() != 1 {
		t.Fatalf("Rejected client posted again: %d, %v", posts.Load(), err)
	}
}

func TestStatusAuthenticationFailurePreservesJobForResume(t *testing.T) {
	ledger, path := newTestLedger(t)
	raw := "https://example.com/page"
	entry := Entry{
		Status: "pending", JobID: "job:" + raw,
		ArchiveURL: "https://web.archive.org/web/20100101000000/" + raw,
		ExpiresAt:  testNow.Add(-time.Hour).Format(time.RFC3339), RefreshStartedAt: testNow.Format(time.RFC3339),
	}
	if err := ledger.put(raw, []string{raw}, entry); err != nil {
		t.Fatal(err)
	}
	client := &fakeArchiveService{unauthenticated: true, statusFn: func(context.Context, string) (capture, error) {
		return capture{}, &authenticationError{message: "Credentials required for status"}
	}}
	if err := testRun(t, ledger, client, []string{raw}); !errors.Is(err, errIncomplete) {
		t.Fatalf("Status authentication should report incomplete work: %v", err)
	}
	stored := testReadEntries(t, path)[raw]
	if stored.Status != "needs_auth" || stored.JobID != entry.JobID || stored.ArchiveURL != entry.ArchiveURL || stored.ExpiresAt != entry.ExpiresAt {
		t.Fatalf("Status authentication failure lost the pending job/backup: %+v", stored)
	}
	authenticated := &fakeArchiveService{}
	if err := testRun(t, ledger, authenticated, []string{raw}); err != nil || authenticated.counts() != [3]int{0, 0, 1} {
		t.Fatalf("Restored credentials resubmitted instead of polling: %v, %v", authenticated.counts(), err)
	}
}

func TestHTTPStatusAuthenticationFailuresAreNotRetried(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				w.WriteHeader(status)
			}))
			defer server.Close()
			client := &Client{httpClient: server.Client(), saveURL: server.URL, authorization: "LOW sample-access:sample-secret"}
			_, err := client.status(context.Background(), "existing-job")
			var auth *authenticationError
			if !errors.As(err, &auth) || requests.Load() != 1 || client.authenticated() {
				t.Fatalf("Status authentication failure was misclassified or retried: %v, %d requests", err, requests.Load())
			}
		})
	}
}

func TestSavedCooldownWaitsThenResumesPendingJob(t *testing.T) {
	ledger, path := newTestLedger(t)
	raw := "https://example.com/pending"
	until := time.Now().Add(40 * time.Millisecond)
	entry := Entry{Status: "pending", JobID: "existing-job"}
	if err := ledger.put(raw, []string{raw}, entry); err != nil {
		t.Fatal(err)
	}
	if err := ledger.setCooldown(until); err != nil {
		t.Fatal(err)
	}
	client := &fakeArchiveService{statusFn: func(context.Context, string) (capture, error) {
		if time.Now().Before(until) {
			t.Error("Polled before the saved cooldown ended")
		}
		if !ledger.retryAfter().IsZero() || testReadDatabase(t, path).RetryAfter != "" {
			t.Error("Expired saved cooldown was not cleared before resuming")
		}
		return capture{Status: "success", Timestamp: testTimestamp, OriginalURL: raw}, nil
	}}
	r := runner{ledger: ledger, client: client, concurrency: 1}
	if err := r.run(context.Background(), []string{raw}); err != nil {
		t.Fatal(err)
	}
	if client.counts() != [3]int{0, 0, 1} {
		t.Fatalf("Pending job was resubmitted: %v", client.counts())
	}
}

func TestRateLimitPausesOtherRequests(t *testing.T) {
	var firstRequests atomic.Int32
	otherStarted := make(chan time.Time, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/first" && firstRequests.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		if r.URL.Path == "/other" {
			otherStarted <- time.Now()
		}
		fmt.Fprint(w, `{}`)
	}))
	defer server.Close()
	limited := make(chan time.Time, 1)
	client := &Client{httpClient: server.Client(), onRateLimit: func(until time.Time) error {
		limited <- until
		return nil
	}}
	done := make(chan error, 1)
	go func() {
		var result map[string]any
		done <- client.request(context.Background(), server.URL+"/first", nil, &result)
	}()
	until := <-limited
	var result map[string]any
	if err := client.request(context.Background(), server.URL+"/other", nil, &result); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if started := <-otherStarted; started.Before(until) {
		t.Fatalf("Another request started during the global cooldown: %v < %v", started, until)
	}
}

func TestRateLimitedPendingJobWaitsAndKeepsItsID(t *testing.T) {
	ledger, path := newTestLedger(t)
	raw := "https://example.com/pending"
	if err := ledger.put(raw, []string{raw}, Entry{Status: "pending", JobID: "existing-job"}); err != nil {
		t.Fatal(err)
	}
	var gets, posts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			posts.Add(1)
		}
		if gets.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		if r.URL.Path != "/status/existing-job" {
			t.Errorf("Unexpected job request: %s", r.URL.Path)
		}
		fmt.Fprintf(w, `{"status":"success","timestamp":%q,"original_url":%q}`, testTimestamp, raw)
	}))
	defer server.Close()
	client := &Client{httpClient: server.Client(), saveURL: server.URL}
	client.onRateLimit = func(until time.Time) error {
		stored := testReadEntries(t, path)[raw]
		if stored.Status != "pending" || stored.JobID != "existing-job" || stored.RetryAfter != "" || testReadDatabase(t, path).RetryAfter == "" {
			t.Errorf("Rate limit lost its job or cooldown: %+v", stored)
		}
		return nil
	}
	r := runner{ledger: ledger, client: client, concurrency: 1}
	if err := r.run(context.Background(), []string{raw}); err != nil {
		t.Fatal(err)
	}
	if gets.Load() != 2 || posts.Load() != 0 {
		t.Fatalf("Pending capture was resubmitted: %d GETs, %d POSTs", gets.Load(), posts.Load())
	}
	stored := testReadEntries(t, path)[raw]
	if stored.Status != "archived" || stored.RetryAfter != "" || stored.Error != "" {
		t.Fatalf("Completion retained retry or error metadata: %+v", stored)
	}
	if testReadDatabase(t, path).RetryAfter != "" {
		t.Fatal("Completed rate-limit wait left an expired global cooldown")
	}
}

func TestSuccessfulEntriesClearRetryAndError(t *testing.T) {
	ledger, path := newTestLedger(t)
	base := "https://example.com/page"
	aliases := []string{base, base + "#section"}
	until := testNow.Add(time.Hour).Format(time.RFC3339)
	if err := ledger.put(base, aliases, Entry{Status: "pending", JobID: "existing-job", Error: "old error"}); err != nil {
		t.Fatal(err)
	}
	if err := ledger.setCooldown(testNow.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	archived := Entry{Status: "archived", ArchiveURL: testSnapshot(base), ExpiresAt: expiryAfterSixMonths(testNow).Format(time.RFC3339)}
	dirty := archived
	dirty.Error, dirty.RetryAfter = "old error", until
	if err := ledger.put(base, aliases, dirty); err != nil {
		t.Fatal(err)
	}
	for raw, entry := range testReadEntries(t, path) {
		want := archived
		if _, fragment := splitFragment(raw); fragment != "" {
			want.ArchiveURL += "#" + fragment
		}
		if entry != want {
			t.Errorf("Successful entry retained obsolete metadata: %+v", entry)
		}
	}
	if !ledger.retryAfter().Equal(testNow.Add(time.Hour)) || testReadDatabase(t, path).RetryAfter != until {
		t.Fatal("Completing work lost the global cooldown")
	}
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(content), `"retry_after"`) != 1 || strings.Contains(string(content), `"error"`) {
		t.Fatal("Retry metadata was not exclusively global, or completed errors remained")
	}
}

func TestSuccessfulEntryPreservesGlobalCooldown(t *testing.T) {
	ledger, path := newTestLedger(t)
	completed, pending := "https://example.com/a", "https://example.com/b"
	until := testNow.Add(time.Hour)
	for _, raw := range []string{completed, pending} {
		if err := ledger.put(raw, []string{raw}, Entry{Status: "pending", JobID: "job:" + raw}); err != nil {
			t.Fatal(err)
		}
	}
	if err := ledger.setCooldown(until); err != nil {
		t.Fatal(err)
	}
	if err := ledger.put(completed, []string{completed}, Entry{Status: "archived", ArchiveURL: testSnapshot(completed)}); err != nil {
		t.Fatal(err)
	}
	entries := testReadEntries(t, path)
	if entry := entries[completed]; entry.RetryAfter != "" || entry.Error != "" {
		t.Fatalf("Successful entry retained obsolete metadata: %+v", entry)
	}
	if entry := entries[pending]; entry.Status != "pending" || entry.JobID != "job:"+pending || entry.RetryAfter != "" {
		t.Fatalf("Completing a URL altered pending work: %+v", entry)
	}
	if testReadDatabase(t, path).RetryAfter != until.Format(time.RFC3339) {
		t.Fatal("Completing a URL lost the global cooldown")
	}
}

func TestLoadingMigratesLegacyRetryMetadataToGlobalCooldown(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.json")
	completed, pending := "https://example.com/a", "https://example.com/b"
	until := testNow.Add(2 * time.Hour).Format(time.RFC3339)
	archived := Entry{Status: "archived", ArchiveURL: testSnapshot(completed), ExpiresAt: expiryAfterSixMonths(testNow).Format(time.RFC3339)}
	dirty := archived
	dirty.Error, dirty.RetryAfter = "old error", testNow.Add(time.Hour).Format(time.RFC3339)
	unfinished := Entry{Status: "pending", JobID: "existing-job", Error: "keep this error", RetryAfter: until}
	if err := writeJSON(path, database{Version: databaseVersion, RetryAfter: testNow.Add(30 * time.Minute).Format(time.RFC3339), Entries: map[string]Entry{completed: dirty, pending: unfinished}}); err != nil {
		t.Fatal(err)
	}
	if _, err := loadLedgerAt(path, testNow); err != nil {
		t.Fatal(err)
	}
	entries := testReadEntries(t, path)
	if entries[completed] != archived {
		t.Fatalf("Existing successful entry was not cleaned: %+v", entries[completed])
	}
	unfinished.RetryAfter = ""
	if entries[pending] != unfinished {
		t.Fatalf("Cleanup altered unfinished work: %+v", entries[pending])
	}
	if testReadDatabase(t, path).RetryAfter != until {
		t.Fatal("Legacy cooldown was not migrated to the global field")
	}
	marker := time.Unix(1000000000, 0)
	if err := os.Chtimes(path, marker, marker); err != nil {
		t.Fatal(err)
	}
	if _, err := loadLedgerAt(path, testNow); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil || !info.ModTime().Equal(marker) {
		t.Fatalf("Reloading a clean database rewrote it: %v", err)
	}
}

func TestRateLimitWaitCanBeCanceledAndCooldownSurvives(t *testing.T) {
	ledger, path := newTestLedger(t)
	raw := "https://example.com/pending"
	entry := Entry{Status: "pending", JobID: "existing-job", ArchiveURL: testSnapshot(raw), ExpiresAt: testNow.Format(time.RFC3339)}
	if err := ledger.put(raw, []string{raw}, entry); err != nil {
		t.Fatal(err)
	}
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Retry-After", "3600")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &Client{httpClient: server.Client(), saveURL: server.URL, onRateLimit: func(time.Time) error {
		cancel()
		return nil
	}}
	r := runner{ledger: ledger, client: client, concurrency: 1}
	if err := r.run(ctx, []string{raw}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Expected cancellation during rate-limit wait: %v", err)
	}
	stored := testReadEntries(t, path)[raw]
	if requests.Load() != 1 || stored.Status != "pending" || stored.JobID != entry.JobID || stored.ArchiveURL != entry.ArchiveURL || stored.ExpiresAt != entry.ExpiresAt || stored.RetryAfter != "" || testReadDatabase(t, path).RetryAfter == "" {
		t.Fatalf("Cancellation lost pending state or retried: %+v, %d requests", stored, requests.Load())
	}
}

func TestLongerCooldownExtendsAnExistingWait(t *testing.T) {
	var gate paceGate
	gate.postpone(time.Now().Add(30 * time.Millisecond))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- gate.wait(ctx, 0) }()
	later := time.Now().Add(70 * time.Millisecond)
	gate.postpone(later)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if time.Now().Before(later) {
		t.Fatal("Wait ignored the extended cooldown")
	}
}

func TestArchiveDomainsAreExcludedPrecisely(t *testing.T) {
	for _, raw := range []string{
		"https://archive.org",
		"https://archive.org/details/example",
		"https://ARCHIVE.ORG./details/example",
		"https://web.archive.org",
		"http://web.archive.org/",
		"https://WEB.ARCHIVE.ORG./web/20261118000000/https://example.com/",
		"https://web.archive.org/save/https://example.com/",
		"https://notweb.archive.org/",
		"https://foo.bar.archive.org/anything",
	} {
		if !excludedLink(raw) {
			t.Errorf("Internet Archive URL was not excluded: %s", raw)
		}
	}
	for _, raw := range []string{
		"https://notarchive.org/",
		"https://archive.org.example.com/",
		"https://web.archive.org.example.com/",
		"https://example.com/archive.org",
	} {
		if excludedLink(raw) {
			t.Errorf("Unrelated URL was excluded: %s", raw)
		}
	}
}
