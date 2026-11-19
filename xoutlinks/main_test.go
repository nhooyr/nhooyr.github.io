package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeSaver struct {
	mu           sync.Mutex
	preparations int
	calls        []string
	saveFn       func(context.Context, string, string) error
}

func (s *fakeSaver) prepare(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.preparations++
	return nil
}

func (s *fakeSaver) save(ctx context.Context, raw, path string) error {
	s.mu.Lock()
	s.calls = append(s.calls, raw)
	s.mu.Unlock()
	if s.saveFn != nil {
		return s.saveFn(ctx, raw, path)
	}
	return os.WriteFile(path, []byte("<!doctype html><p>saved page</p>"), 0644)
}

func (s *fakeSaver) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.calls)
}

func newTestRunner(t *testing.T) (*runner, *fakeSaver) {
	t.Helper()
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "xoutlinks"), 0755); err != nil {
		t.Fatal(err)
	}
	ledger, err := loadLedger(filepath.Join(root, "xoutlinks", "db.json"))
	if err != nil {
		t.Fatal(err)
	}
	saver := &fakeSaver{}
	return &runner{root: root, backups: "backups", ledger: ledger, saver: saver, concurrency: 3}, saver
}

func putEntry(t *testing.T, r *runner, raw string, entry Entry) {
	t.Helper()
	base, _ := splitFragment(raw)
	if err := r.ledger.put(base, []string{raw}, entry); err != nil {
		t.Fatal(err)
	}
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestIncrementalBackups(t *testing.T) {
	r, saver := newTestRunner(t)
	urls := []string{"https://example.com/a#one", "https://example.com/a#two", "https://example.com/b?q=1"}
	if err := r.run(context.Background(), urls); err != nil {
		t.Fatal(err)
	}
	if saver.count() != 2 {
		t.Fatalf("saved %d pages instead of two distinct URLs", saver.count())
	}
	entry := r.ledger.record("https://example.com/a")
	if entry.Status != "saved" || entry.File == "" || entry.SavedAt == "" || entry.PendingFile != "" {
		t.Fatalf("incomplete saved entry: %+v", entry)
	}
	before := readFile(t, r.ledger.path)
	info, _ := os.Stat(r.ledger.path)
	r.saver = nil // A completed run must not even prepare Chrome or SingleFile.
	if err := r.run(context.Background(), append(urls, "https://example.com/a#three")); err != nil {
		t.Fatal(err)
	}
	afterAlias := readFile(t, r.ledger.path)
	if bytes.Equal(before, afterAlias) {
		t.Fatal("new alias was not recorded")
	}
	if got := r.ledger.data["https://example.com/a#three"].File; got != entry.File {
		t.Fatalf("new fragment did not reuse the backup: %q", got)
	}
	info, _ = os.Stat(r.ledger.path)
	if err := r.run(context.Background(), append(urls, "https://example.com/a#three")); err != nil {
		t.Fatal(err)
	}
	afterInfo, _ := os.Stat(r.ledger.path)
	if !bytes.Equal(afterAlias, readFile(t, r.ledger.path)) || !info.ModTime().Equal(afterInfo.ModTime()) {
		t.Fatal("repeat run changed the database")
	}
	r.saver = saver
	if err := r.run(context.Background(), append(urls, "https://example.com/new")); err != nil {
		t.Fatal(err)
	}
	if saver.count() != 3 {
		t.Fatalf("new run did unnecessary captures: %v", saver.calls)
	}
}

func TestMissingBackupIsSavedAgain(t *testing.T) {
	r, saver := newTestRunner(t)
	raw := "https://example.com/"
	if err := r.run(context.Background(), []string{raw}); err != nil {
		t.Fatal(err)
	}
	entry := r.ledger.record(raw)
	if err := os.Remove(filepath.Join(r.root, entry.File)); err != nil {
		t.Fatal(err)
	}
	if err := r.run(context.Background(), []string{raw}); err != nil {
		t.Fatal(err)
	}
	if saver.count() != 2 {
		t.Fatal("missing local backup was skipped")
	}
}

func TestConfiguredBackupDirectoryPreservesResume(t *testing.T) {
	r, _ := newTestRunner(t)
	r.backups = "xoutlinks/backups"
	raw := "https://example.com/"
	if err := r.run(context.Background(), []string{raw}); err != nil {
		t.Fatal(err)
	}
	entry := r.ledger.record(raw)
	if !strings.HasPrefix(entry.File, "xoutlinks/backups/") {
		t.Fatalf("saved outside the configured directory: %s", entry.File)
	}
	before := readFile(t, r.ledger.path)
	ledger, err := loadLedger(r.ledger.path)
	if err != nil {
		t.Fatal(err)
	}
	r.ledger, r.saver = ledger, nil
	if err := r.run(context.Background(), []string{raw}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, readFile(t, r.ledger.path)) {
		t.Fatal("resuming a configured directory changed the database")
	}
}

func TestBackupDirectoryRelativeToWorkingDirectory(t *testing.T) {
	root := t.TempDir()
	directory := filepath.Join(root, "xoutlinks")
	if err := os.Mkdir(directory, 0755); err != nil {
		t.Fatal(err)
	}
	t.Chdir(directory)
	got, err := backupDirectory(root, "./backups")
	if err != nil || got != "xoutlinks/backups" {
		t.Fatalf("relative directory resolved to %q: %v", got, err)
	}
	for _, invalid := range []string{root, filepath.Dir(root)} {
		if _, err := backupDirectory(root, invalid); err == nil {
			t.Fatalf("accepted invalid backup directory: %s", invalid)
		}
	}
}

func TestMigrateWaybackWithoutLosingArchiveURLs(t *testing.T) {
	r, _ := newTestRunner(t)
	raw := "https://example.com/"
	archive := "https://web.archive.org/web/20261118000000/https://example.com/"
	old := fmt.Sprintf(`{"version":1,"retry_at":"2099-01-01T00:00:00Z","entries":{
		%q:{"status":"archived","archive_url":%q},
		"https://example.net/":{"status":"blocked","error":"error:blocked","error_msg":"wayback blocked"},
		"https://example.org/":{"status":"replay_pending","job_id":"old-job","retry_at":"2099-01-01T00:00:00Z","force_rearchive":true}}}`, raw, archive)
	if err := os.WriteFile(r.ledger.path, []byte(old), 0600); err != nil {
		t.Fatal(err)
	}
	ledger, err := loadLedger(r.ledger.path)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range ledger.data {
		if entry.Status != "ready" || entry.Error != "" || entry.RetryAt != "" {
			t.Fatalf("Wayback state leaked into local capture: %+v", entry)
		}
	}
	if ledger.record(raw).ArchiveURL != archive || !ledger.record("https://example.org/").ForceRearchive {
		t.Fatal("migration lost archive URL or explicit force request")
	}
	r.ledger = ledger
	if err := r.run(context.Background(), []string{raw, "https://example.net/"}); err != nil {
		t.Fatal(err)
	}
	if ledger.record(raw).ArchiveURL != archive {
		t.Fatal("local save discarded the old Wayback URL")
	}
	if !bytes.Contains(readFile(t, ledger.path), []byte(`"version": 2`)) {
		t.Fatal("database version was not upgraded")
	}
}

func TestInvalidDatabaseDoesNotGetRewritten(t *testing.T) {
	for _, data := range []string{
		`{"version":999,"entries":{}}`,
		`{"version":2,"entries":{"https://example.com":{"status":"saved","file":"../outside.html"}}}`,
		`{"version":2,"entries":{"https://example.com":{"status":"failed","retry_at":"tomorrow"}}}`,
	} {
		t.Run(data, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "db.json")
			if err := os.WriteFile(path, []byte(data), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := loadLedger(path); err == nil {
				t.Fatal("invalid database was accepted")
			}
			if string(readFile(t, path)) != data {
				t.Fatal("invalid database was modified")
			}
		})
	}
}

func TestLoadingClearsExpiredRetriesAndKeepsBlockedFailuresPermanent(t *testing.T) {
	r, _ := newTestRunner(t)
	raw := "https://example.com/"
	putEntry(t, r, raw, Entry{Status: "failed", Error: "error:blocked",
		ErrorMsg: "site blocked access", PendingFile: "backups/pending.html",
		RetryAt: time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano)})
	putEntry(t, r, raw+"expired", Entry{Status: "failed", Error: "error:capture",
		RetryAt: time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano)})
	ledger, err := loadLedger(r.ledger.path)
	if err != nil {
		t.Fatal(err)
	}
	blocked := ledger.record(raw)
	if blocked.Status != "blocked" || blocked.PendingFile != "" || blocked.RetryAt != "" {
		t.Fatalf("blocked failure remained retryable: %+v", blocked)
	}
	if ledger.record(raw+"expired").RetryAt != "" {
		t.Fatal("expired retry_at was retained")
	}
}

func TestInterruptedSaveResumesPendingFile(t *testing.T) {
	r, saver := newTestRunner(t)
	raw := "https://example.com/"
	ctx, cancel := context.WithCancel(context.Background())
	saver.saveFn = func(context.Context, string, string) error {
		cancel()
		return ctx.Err()
	}
	if err := r.run(ctx, []string{raw}); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation: %v", err)
	}
	before := r.ledger.record(raw)
	if before.Status != "saving" || before.PendingFile == "" || before.Error != "" {
		t.Fatalf("pending save was not preserved: %+v", before)
	}
	ledger, err := loadLedger(r.ledger.path)
	if err != nil {
		t.Fatal(err)
	}
	r.ledger = ledger
	saver.saveFn = nil
	if err := r.run(context.Background(), []string{raw}); err != nil {
		t.Fatal(err)
	}
	after := ledger.record(raw)
	if after.File != before.PendingFile || after.Status != "saved" {
		t.Fatalf("resume did not use the pending destination: %+v", after)
	}
}

func TestCompletedFileBeforeDBUpdateIsRecovered(t *testing.T) {
	r, _ := newTestRunner(t)
	raw := "https://example.com/"
	if err := os.Mkdir(filepath.Join(r.root, "backups"), 0755); err != nil {
		t.Fatal(err)
	}
	name := "backups/completed.html"
	if err := os.WriteFile(filepath.Join(r.root, name), []byte("<p>complete</p>"), 0644); err != nil {
		t.Fatal(err)
	}
	putEntry(t, r, raw, Entry{Status: "saving", PendingFile: name, ForceRearchive: true})
	r.saver = nil
	if err := r.run(context.Background(), []string{raw}); err != nil {
		t.Fatal(err)
	}
	entry := r.ledger.record(raw)
	if entry.Status != "saved" || entry.File != name || entry.ForceRearchive || entry.PendingFile != "" {
		t.Fatalf("completed file was not recovered: %+v", entry)
	}
}

func TestForcedRearchiveKeepsPreviousBackupUntilSuccess(t *testing.T) {
	r, saver := newTestRunner(t)
	raw := "https://example.com/"
	if err := r.run(context.Background(), []string{raw}); err != nil {
		t.Fatal(err)
	}
	old := r.ledger.record(raw)
	forced := old
	forced.ForceRearchive = true
	putEntry(t, r, raw+"#force", forced)
	saver.saveFn = func(context.Context, string, string) error { return errors.New("temporary network failure") }
	if err := r.run(context.Background(), []string{raw}); !errors.Is(err, errIncomplete) {
		t.Fatalf("expected incomplete backup: %v", err)
	}
	failed := r.ledger.record(raw)
	if failed.File != old.File || !failed.ForceRearchive || failed.PendingFile == "" {
		t.Fatalf("failed replacement lost the old backup or resume state: %+v", failed)
	}
	if len(readFile(t, filepath.Join(r.root, old.File))) == 0 {
		t.Fatal("old backup was damaged")
	}
	saver.saveFn = nil
	if err := r.run(context.Background(), []string{raw}); err != nil {
		t.Fatal(err)
	}
	saved := r.ledger.record(raw)
	if saved.File != failed.PendingFile || saved.File == old.File || saved.ForceRearchive {
		t.Fatalf("forced capture did not finish correctly: %+v", saved)
	}
	if saver.count() != 3 || saved.Error != "" || saved.ErrorMsg != "" || saved.RetryAt != "" {
		t.Fatalf("incorrect capture count or stale error: %+v", saved)
	}
}

func TestAutomaticRetryDoesNotDelayOtherURLs(t *testing.T) {
	r, saver := newTestRunner(t)
	r.concurrency, r.retryInterval = 1, 20*time.Millisecond
	var calls atomic.Int32
	otherSaved := make(chan struct{})
	saver.saveFn = func(ctx context.Context, raw, path string) error {
		if strings.Contains(raw, "retry") {
			if calls.Add(1) == 1 {
				return errors.New("temporary failure")
			}
			select {
			case <-otherSaved:
			default:
				return errors.New("retry blocked the other URL")
			}
		} else {
			close(otherSaved)
		}
		return os.WriteFile(path, []byte("<p>saved</p>"), 0644)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := r.run(ctx, []string{"https://a.test/retry", "https://b.test/other"}); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 || saver.count() != 3 {
		t.Fatalf("unexpected retries: %v", saver.calls)
	}
}

func TestRetryDeadlineSurvivesCancellation(t *testing.T) {
	r, saver := newTestRunner(t)
	r.retryInterval = time.Hour
	raw := "https://example.com/"
	entry := Entry{Status: "failed", Error: "error:capture", ErrorMsg: "temporary failure",
		RetryAt: time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano), PendingFile: "backups/pending.html"}
	putEntry(t, r, raw, entry)
	before := readFile(t, r.ledger.path)
	var output bytes.Buffer
	r.output = &output
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
	defer cancel()
	if err := r.run(ctx, []string{raw}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected canceled wait: %v", err)
	}
	if saver.count() != 0 || !bytes.Equal(before, readFile(t, r.ledger.path)) {
		t.Fatal("canceled wait changed progress or submitted a save")
	}
	if !strings.Contains(output.String(), raw+": failed: error:capture: temporary failure\n"+raw+": retry at ") {
		t.Fatalf("saved error was not printed before deadline: %s", output.String())
	}
	entry.RetryAt = time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano)
	putEntry(t, r, raw, entry)
	if err := r.run(context.Background(), []string{raw}); err != nil {
		t.Fatal(err)
	}
	if saver.count() != 1 || r.ledger.record(raw).RetryAt != "" {
		t.Fatal("expired retry deadline did not clear")
	}
}

func TestPermanentFailuresAreIgnoredBut404Retries(t *testing.T) {
	for _, tc := range []struct {
		url     string
		status  int
		blocked bool
	}{
		{"https://example.com/403", 403, true},
		{"https://example.com/999", 999, true},
		{"https://www.linkedin.com/in/test", 404, true},
		{"https://example.com/404", 404, false},
	} {
		t.Run(tc.url, func(t *testing.T) {
			r, saver := newTestRunner(t)
			saver.saveFn = func(context.Context, string, string) error {
				return &captureError{code: fmt.Sprintf("error:http-%d", tc.status), message: fmt.Sprintf("HTTP status=%d", tc.status)}
			}
			err := r.run(context.Background(), []string{tc.url})
			if tc.blocked {
				if err != nil || r.ledger.record(tc.url).Status != "blocked" {
					t.Fatalf("permanent failure was not recorded: %v, %+v", err, r.ledger.record(tc.url))
				}
				r.saver = nil
				if err := r.run(context.Background(), []string{tc.url}); err != nil {
					t.Fatal(err)
				}
			} else {
				if !errors.Is(err, errIncomplete) {
					t.Fatalf("nonpermanent failure did not produce nonzero result: %v", err)
				}
				saver.saveFn = nil
				if err := r.run(context.Background(), []string{tc.url}); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestPinterestTimeoutRule(t *testing.T) {
	for _, tc := range []struct {
		url     string
		message string
		blocked bool
	}{
		{"https://www.pinterest.com/aqsnow", "Load timeout", true},
		{"https://pinterest.com/aqsnow", "Capture timeout", true},
		{"https://WWW.PINTEREST.COM/aqsnow", "Setup TIMEOUT", true},
		{"https://uk.pinterest.com/aqsnow", "connection timed out", true},
		{"https://pinterest.com/aqsnow", "context deadline exceeded", true},
		{"https://pinterest.com/aqsnow", "net::ERR_TIMED_OUT", true},
		{"https://pinterest.com/aqsnow", "HTTP status=404", false},
		{"https://pinterest.com/aqsnow", "HTTP status=429", false},
		{"https://pinterest.com/aqsnow", "browser exited unexpectedly", false},
		{"https://pinterest.com.example.com/aqsnow", "Load timeout", false},
		{"https://notpinterest.com/aqsnow", "Load timeout", false},
		{"https://example.com/", "Load timeout", false},
	} {
		t.Run(tc.url+"/"+tc.message, func(t *testing.T) {
			entry := normalizeFailure(tc.url, Entry{Status: "failed", Error: "error:capture", ErrorMsg: tc.message})
			if entry.permanentFailure() != tc.blocked {
				t.Fatalf("wrong timeout classification: %+v", entry)
			}
			if entry.ErrorMsg != tc.message {
				t.Fatalf("original error message was changed: %+v", entry)
			}
			if excludedLink(tc.url) {
				t.Fatal("timeout rule excluded the URL before capture")
			}
		})
	}
	saved := Entry{Status: "saved", ErrorMsg: "Load timeout"}
	if got := normalizeFailure("https://www.pinterest.com/aqsnow", saved); got != saved {
		t.Fatal("timeout rule changed a saved entry")
	}
}

func TestPinterestTimeoutIsSavedAndSkipped(t *testing.T) {
	r, saver := newTestRunner(t)
	r.retryInterval = time.Hour
	raw := "https://www.pinterest.com/aqsnow"
	message := "Load timeout [2026-11-19T11:55:42.505Z] URL: " + raw + " Error: Load timeout"
	saver.saveFn = func(context.Context, string, string) error {
		return &captureError{code: "error:capture", message: message}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := r.run(ctx, []string{raw}); err != nil {
		t.Fatal(err)
	}
	entry := r.ledger.record(raw)
	if entry.Status != "blocked" || entry.Error != "error:blocked" || entry.ErrorMsg != message ||
		entry.RetryAt != "" || entry.PendingFile != "" {
		t.Fatalf("Pinterest timeout was not permanently blocked: %+v", entry)
	}
	ledger, err := loadLedger(r.ledger.path)
	if err != nil {
		t.Fatal(err)
	}
	r.ledger, r.saver = ledger, nil
	if err := r.run(context.Background(), []string{raw}); err != nil {
		t.Fatal(err)
	}
	if saver.count() != 1 {
		t.Fatal("blocked Pinterest capture was retried")
	}
}

func TestLoadingBlocksPendingPinterestTimeout(t *testing.T) {
	r, _ := newTestRunner(t)
	raw := "https://www.pinterest.com/aqsnow"
	putEntry(t, r, raw, Entry{Status: "failed", Error: "error:capture", ErrorMsg: "Load timeout",
		PendingFile: "backups/pending.html", RetryAt: time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano)})
	ledger, err := loadLedger(r.ledger.path)
	if err != nil {
		t.Fatal(err)
	}
	entry := ledger.record(raw)
	if entry.Status != "blocked" || entry.Error != "error:blocked" || entry.RetryAt != "" || entry.PendingFile != "" {
		t.Fatalf("stored Pinterest timeout stayed retryable: %+v", entry)
	}
}

func TestFailureDoesNotStopRemainingURLs(t *testing.T) {
	r, saver := newTestRunner(t)
	r.concurrency = 1
	saver.saveFn = func(_ context.Context, raw, path string) error {
		if strings.Contains(raw, "fail") {
			return errors.New("failure")
		}
		return os.WriteFile(path, []byte("saved"), 0644)
	}
	if err := r.run(context.Background(), []string{"https://a.test/fail", "https://b.test/ok", "https://c.test/ok"}); !errors.Is(err, errIncomplete) {
		t.Fatalf("expected nonzero result after processing all URLs: %v", err)
	}
	if saver.count() != 3 || r.ledger.record("https://c.test/ok").Status != "saved" {
		t.Fatal("failure stopped later URLs")
	}
}

func TestExcludedDatabaseIncludesBlockedLinks(t *testing.T) {
	r, _ := newTestRunner(t)
	linkedin := "https://www.linkedin.com/in/aqsnow/"
	pinterest := "https://www.pinterest.com/aqsnow"
	for _, raw := range []string{linkedin, pinterest} {
		putEntry(t, r, raw, Entry{Status: "blocked", Error: "error:blocked"})
	}
	putEntry(t, r, "https://example.com/saved", Entry{Status: "saved"})
	explicit := []string{"https://excluded.example/", linkedin, "https://excluded.example/"}
	if err := r.ledger.setExcluded(explicit); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(filepath.Dir(r.ledger.path), "dbx.json")
	var got []string
	if err := json.Unmarshal(readFile(t, path), &got); err != nil {
		t.Fatal(err)
	}
	want := []string{"https://excluded.example/", linkedin, pinterest}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("wrong excluded links: %v", got)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.ledger.setExcluded(explicit); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !before.ModTime().Equal(after.ModTime()) {
		t.Fatal("unchanged excluded links were rewritten")
	}
}

func TestExcludedDatabaseTracksBlockingAndSuccessfulRetry(t *testing.T) {
	r, saver := newTestRunner(t)
	if err := r.ledger.setExcluded(nil); err != nil {
		t.Fatal(err)
	}
	raw := "https://www.pinterest.com/aqsnow"
	saver.saveFn = func(context.Context, string, string) error {
		return &captureError{code: "error:capture", message: "Load timeout"}
	}
	if err := r.run(context.Background(), []string{raw}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(filepath.Dir(r.ledger.path), "dbx.json")
	var got []string
	if err := json.Unmarshal(readFile(t, path), &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []string{raw}) {
		t.Fatalf("newly blocked URL missing: %v", got)
	}
	entry := r.ledger.record(raw)
	entry.ForceRearchive = true
	putEntry(t, r, raw, entry)
	saver.saveFn = nil
	if err := r.run(context.Background(), []string{raw}); err != nil {
		t.Fatal(err)
	}
	if got := string(readFile(t, path)); got != "[]\n" {
		t.Fatalf("successful retry remains in excluded links: %s", got)
	}
}

func TestExcludedDatabaseRecoversAfterWriteFailure(t *testing.T) {
	r, _ := newTestRunner(t)
	if err := r.ledger.setExcluded(nil); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(filepath.Dir(r.ledger.path), "dbx.json")
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0755); err != nil {
		t.Fatal(err)
	}
	raw := "https://www.pinterest.com/aqsnow"
	entry := Entry{Status: "blocked", Error: "error:blocked"}
	if err := r.ledger.put(raw, []string{raw}, entry); err == nil {
		t.Fatal("expected excluded-index write failure")
	}
	if r.ledger.record(raw).Status != "blocked" {
		t.Fatal("secondary write failure lost the primary record")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := r.ledger.put(raw, []string{raw}, entry); err != nil {
		t.Fatal(err)
	}
	var got []string
	if err := json.Unmarshal(readFile(t, path), &got); err != nil || !reflect.DeepEqual(got, []string{raw}) {
		t.Fatalf("excluded index was not repaired: %v, %v", got, err)
	}
}

func TestConcurrentCapturesAndAtomicWrites(t *testing.T) {
	r, saver := newTestRunner(t)
	var active, peak atomic.Int32
	arrived := make(chan struct{}, 20)
	release := make(chan struct{})
	go func() {
		for range 3 {
			<-arrived
		}
		close(release)
	}()
	saver.saveFn = func(ctx context.Context, _ string, path string) error {
		n := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); n > old; old = peak.Load() {
			if peak.CompareAndSwap(old, n) {
				break
			}
		}
		arrived <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
		return os.WriteFile(path, []byte("saved"), 0644)
	}
	var urls []string
	for i := range 20 {
		urls = append(urls, fmt.Sprintf("https://example.com/%d", i))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := r.run(ctx, urls); err != nil {
		t.Fatal(err)
	}
	if peak.Load() < 2 || peak.Load() > 3 {
		t.Fatalf("unexpected capture concurrency: %d", peak.Load())
	}
	reloaded, err := loadLedger(r.ledger.path)
	if err != nil || len(reloaded.data) != len(urls) {
		t.Fatalf("concurrent writes lost results: %v", err)
	}
}

func TestErrorsAndRetryMessagesStayTogether(t *testing.T) {
	r, _ := newTestRunner(t)
	var output bytes.Buffer
	r.output = &output
	var group sync.WaitGroup
	for i := range 20 {
		group.Add(1)
		go func() {
			defer group.Done()
			r.printRetry(fmt.Sprintf("https://example.com/%d", i),
				Entry{Status: "failed", ErrorMsg: "error"}, time.Now())
		}()
	}
	group.Wait()
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 40 {
		t.Fatalf("unexpected output: %s", output.String())
	}
	for i := 0; i < len(lines); i += 2 {
		url := strings.SplitN(lines[i], ": failed:", 2)[0]
		if !strings.HasPrefix(lines[i+1], url+": retry at ") {
			t.Fatalf("interleaved messages: %q / %q", lines[i], lines[i+1])
		}
	}
}

func TestMultilineErrorPrefixesEveryLine(t *testing.T) {
	line := errorLine("https://example.com/", Entry{Status: "failed", Error: "error:capture", ErrorMsg: "first line\nsecond line"})
	if line != "https://example.com/: failed: error:capture: first line\nhttps://example.com/: failed: second line\n" {
		t.Fatalf("unprefixed error line: %q", line)
	}
}

func TestCancellationErrorsHaveURLPrefixes(t *testing.T) {
	r, saver := newTestRunner(t)
	ctx, cancel := context.WithCancel(context.Background())
	saver.saveFn = func(ctx context.Context, _ string, _ string) error {
		cancel()
		<-ctx.Done()
		return ctx.Err()
	}
	urls := []string{"https://a.test/", "https://b.test/", "https://c.test/", "https://d.test/"}
	err := r.run(ctx, urls)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation: %v", err)
	}
	for _, raw := range urls {
		if strings.Count(err.Error(), raw+": context canceled") != 1 {
			t.Fatalf("missing or duplicate URL cancellation: %v", err)
		}
	}
}

func TestLockRejectsSimultaneousWriters(t *testing.T) {
	root := t.TempDir()
	unlock, err := lockRepo(root)
	if err != nil {
		t.Fatal(err)
	}
	if second, err := lockRepo(root); err == nil {
		second()
		t.Fatal("concurrent writer was allowed")
	}
	unlock()
	again, err := lockRepo(root)
	if err != nil {
		t.Fatal(err)
	}
	again()
}

func TestFailedDBWriteDoesNotChangeInMemoryState(t *testing.T) {
	r, _ := newTestRunner(t)
	r.ledger.path = t.TempDir() // A directory cannot be replaced with a file.
	if err := r.ledger.put("https://example.com", []string{"https://example.com"}, Entry{Status: "saved"}); err == nil {
		t.Fatal("expected write failure")
	}
	if len(r.ledger.data) != 0 {
		t.Fatal("failed write changed in-memory progress")
	}
}

func TestCollectLinksNeverScansBackups(t *testing.T) {
	r, _ := newTestRunner(t)
	r.backups = "xoutlinks/backups"
	for name, content := range map[string]string{
		"index.html": `<a href="https://example.com/?a=1&amp;b=2#section">one</a><a href="/local">local</a><a href="mailto:test@example.com">mail</a>`,
		"xoutlinks/backups/accidentally-added.html": `<a href="https://should-not-be-crawled.test/">backup</a>`,
	} {
		path := filepath.Join(r.root, name)
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
	}
	for _, args := range [][]string{{"init", "-q"}, {"add", "-f", "xoutlinks/backups/accidentally-added.html"}} {
		cmd := exec.Command("git", args...)
		cmd.Dir = r.root
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git: %v: %s", err, out)
		}
	}
	links, err := collectLinks(r.root, r.backups)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(links, []string{"https://example.com/?a=1&b=2#section"}) {
		t.Fatalf("wrong links: %v", links)
	}
}

func TestExclusionsRemainPrecise(t *testing.T) {
	// Test explicit exclusions independently of the user's configured defaults.
	previous := exclusions
	exclusions = append(append([]linkRule{}, previous...),
		linkRule{domain: "github.com", path: "/nhooyr/nhooyr.github.io"},
		linkRule{domain: "wikipedia.org"}, linkRule{domain: "archive.org"})
	t.Cleanup(func() { exclusions = previous })
	for _, raw := range []string{
		"https://github.com/nhooyr/nhooyr.github.io",
		"https://WWW.GITHUB.COM/NHoOyR/NHoOyR.GITHUB.IO/blob/main/index.html",
		"https://wikipedia.org", "https://en.m.wikipedia.org/wiki/Kaslo#History",
		"https://archive.org", "https://WEB.ARCHIVE.ORG./web/test", "https://a.b.archive.org",
	} {
		if !excludedLink(raw) {
			t.Errorf("expected exclusion: %s", raw)
		}
	}
	for _, raw := range []string{
		"https://github.com/nhooyr", "https://github.com/nhooyr/nhooyr.github.io-other",
		"https://gist.github.com/nhooyr/nhooyr.github.io", "https://wikipedia.org.example.com",
		"https://notarchive.org", "https://commons.wikimedia.org", "https://leqra.com",
		"https://discord.gg/invite", "https://www.facebook.com/example", "https://www.linkedin.com/in/test",
		"https://www.threads.com/example", "https://us05web.zoom.us/j/123?pwd=example",
	} {
		if excludedLink(raw) {
			t.Errorf("unexpected exclusion: %s", raw)
		}
	}
}

func shellSaver(t *testing.T, body string) *singleFile {
	t.Helper()
	path := filepath.Join(t.TempDir(), "single-file")
	if err := os.WriteFile(path, []byte("#!/bin/sh\nset -eu\n"+body), 0755); err != nil {
		t.Fatal(err)
	}
	return &singleFile{binary: path, chrome: "/installed/chrome"}
}

func TestSingleFileReusesCachedExecutable(t *testing.T) {
	dir := t.TempDir()
	s := &singleFile{chrome: "/bin/sh", cache: dir}
	name := filepath.Join(dir, "single-file-"+singleFileVersion+"-"+runtime.GOOS+"-"+runtime.GOARCH)
	if err := os.WriteFile(name, []byte("#!/bin/sh\nexit 0\n"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := s.prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	if s.binary != name {
		t.Fatalf("did not reuse cached executable: %s", s.binary)
	}
}

func TestSingleFileArgumentsAndAtomicOutput(t *testing.T) {
	s := shellSaver(t, `printf '<html>saved</html>' > "$2"
printf '%s\n' "$@" > "$2.args"
printf '{"response":{"status":200}}'
`)
	path := filepath.Join(t.TempDir(), "page.html")
	raw := "https://example.com/?literal=$(false)&value=two"
	if err := s.save(context.Background(), raw, path); err != nil {
		t.Fatal(err)
	}
	args := string(readFile(t, path+".part.html.args"))
	for _, arg := range []string{raw, "--browser-executable-path=/installed/chrome", "--crawl-links=false", "--compress-content=false", "--block-scripts=true"} {
		if !strings.Contains(args, arg+"\n") {
			t.Errorf("missing capture argument %q: %s", arg, args)
		}
	}
	if string(readFile(t, path)) != "<html>saved</html>" {
		t.Fatal("completed capture was not published")
	}
	if _, err := os.Stat(path + ".part.html"); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("partial file was left behind")
	}
}

func TestSingleFileRejectsHTTPErrorPages(t *testing.T) {
	for _, status := range []int{401, 403, 404, 429, 500, 999} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			s := shellSaver(t, fmt.Sprintf(`printf '<html>error page</html>' > "$2"
printf '{"response":{"status":%d,"statusText":"failed","headers":[{"name":"retry-after","value":"120"}]}}'
`, status))
			path := filepath.Join(t.TempDir(), "page.html")
			err := s.save(context.Background(), "https://example.com", path)
			var failure *captureError
			if !errors.As(err, &failure) || !strings.Contains(failure.message, fmt.Sprintf("HTTP status=%d", status)) {
				t.Fatalf("wrong error: %v", err)
			}
			if status == 429 && !failure.retryAt.After(time.Now().Add(time.Minute)) {
				t.Fatal("retry-after was not retained")
			}
			for _, name := range []string{path, path + ".part.html"} {
				if _, err := os.Stat(name); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("error page was left as a backup: %s", name)
				}
			}
		})
	}
}

func TestSingleFileFailurePreservesExistingBackup(t *testing.T) {
	for _, body := range []string{
		`printf 'partial' > "$2"; printf 'full error details\n' >&2; exit 1`,
		`printf '{"response":{"status":200}}'; : > "$2"`,
		`printf '<html>partial</html>' > "$2"; printf 'not JSON'`,
	} {
		s := shellSaver(t, body)
		path := filepath.Join(t.TempDir(), "page.html")
		if err := os.WriteFile(path, []byte("old backup"), 0644); err != nil {
			t.Fatal(err)
		}
		if err := s.save(context.Background(), "https://example.com", path); err == nil {
			t.Fatal("invalid capture was accepted")
		}
		if string(readFile(t, path)) != "old backup" {
			t.Fatal("failed save replaced the existing backup")
		}
	}
}

func TestSingleFileCancellationCleansPartialOutput(t *testing.T) {
	s := shellSaver(t, `printf 'partial' > "$2"; sleep 30`)
	path := filepath.Join(t.TempDir(), "page.html")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := s.save(ctx, "https://example.com", path); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected cancellation: %v", err)
	}
	if _, err := os.Stat(path + ".part.html"); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("cancellation left a partial backup")
	}
}

func TestChromeCaptureIntegration(t *testing.T) {
	binary := os.Getenv("SINGLE_FILE_TEST_BIN")
	if binary == "" {
		t.Skip("set SINGLE_FILE_TEST_BIN to test the installed Chrome and SingleFile")
	}
	var crawled atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/":
			w.Header().Set("Content-Type", "text/html")
			fmt.Fprint(w, `<!doctype html><html><head><link rel="stylesheet" href="/style.css"></head><body>
<h1>local capture test</h1><img src="/image.png"><a href="/do-not-crawl">external page</a>
<script>document.body.insertAdjacentHTML("beforeend","<p>rendered-by-javascript</p>")</script></body></html>`)
		case "/style.css":
			w.Header().Set("Content-Type", "text/css")
			fmt.Fprint(w, "h1 {color: rgb(10, 20, 30)}")
		case "/image.png":
			w.Header().Set("Content-Type", "image/png")
			data, _ := base64.StdEncoding.DecodeString("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aR1sAAAAASUVORK5CYII=")
			w.Write(data)
		case "/do-not-crawl":
			crawled.Add(1)
			fmt.Fprint(w, "should not be requested")
		default:
			http.Error(w, "not found", http.StatusNotFound)
		}
	}))
	defer server.Close()
	s := &singleFile{binary: binary}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := s.prepare(ctx); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "capture.html")
	if err := s.save(ctx, server.URL, path); err != nil {
		t.Fatal(err)
	}
	content := string(readFile(t, path))
	for _, want := range []string{"local capture test", "rendered-by-javascript", "data:image/png"} {
		if !strings.Contains(content, want) {
			t.Fatalf("saved page lacks %q", want)
		}
	}
	if strings.Contains(content, "document.body.insertAdjacentHTML") {
		t.Fatal("the original executable script was retained")
	}
	if crawled.Load() != 0 {
		t.Fatalf("crawled outlinks %d times", crawled.Load())
	}
	err := s.save(ctx, server.URL+"/missing", filepath.Join(t.TempDir(), "404.html"))
	var failure *captureError
	if !errors.As(err, &failure) || !strings.Contains(failure.message, "HTTP status=404") {
		t.Fatalf("real Chrome HTTP failure was not detected: %v", err)
	}
}
