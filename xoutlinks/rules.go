package main

import (
	"net/url"
	"regexp"
	"strings"
)

var forbiddenHTTPStatus = regexp.MustCompile(`(?i)\bHTTP\s+status\s*=\s*403\b`)
var missingHTTPStatus = regexp.MustCompile(`(?i)(\bHTTP\s+status\s*=\s*)404\b`)

func (e Entry) permanentFailure() bool {
	if e.Status == "needs_auth" {
		return false
	}
	return e.Error == blockedURLError || e.Error == "error:no-access" || forbiddenHTTPStatus.MatchString(e.ErrorMsg)
}

func (e Entry) notFound() bool {
	return e.Error == "error:not-found" || e.Error == "error:target-not-found" || missingHTTPStatus.MatchString(e.ErrorMsg)
}

func normalizeFailure(raw string, entry Entry) Entry {
	if entry.Status == "archived" || entry.Status == "needs_auth" || !entry.notFound() {
		return entry
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return entry
	}
	host := strings.TrimSuffix(strings.ToLower(parsed.Hostname()), ".")
	if host != "linkedin.com" && !strings.HasSuffix(host, ".linkedin.com") {
		return entry
	}
	entry.Error = "error:no-request"
	if missingHTTPStatus.MatchString(entry.ErrorMsg) {
		entry.ErrorMsg = missingHTTPStatus.ReplaceAllString(entry.ErrorMsg, "${1}403")
	} else if entry.ErrorMsg == "" {
		entry.ErrorMsg = "HTTP status=403"
	} else if !forbiddenHTTPStatus.MatchString(entry.ErrorMsg) {
		entry.ErrorMsg += " (HTTP status=403)"
	}
	return entry
}

func excludedLink(raw string) bool {
	return exclusionReason(raw) != ""
}

func exclusionReason(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "invalid URL"
	}
	host := strings.TrimSuffix(strings.ToLower(parsed.Hostname()), ".")
	if host == "archive.org" || strings.HasSuffix(host, ".archive.org") {
		return "Internet Archive"
	}
	if host == "wikipedia.org" || strings.HasSuffix(host, ".wikipedia.org") {
		return "Wikipedia"
	}
	if strings.TrimPrefix(host, "www.") == "github.com" {
		const repository = "/nhooyr/nhooyr.github.io"
		path := strings.ToLower(parsed.Path)
		if path == repository || strings.HasPrefix(path, repository+"/") {
			return "blog source repository"
		}
	}
	return ""
}
