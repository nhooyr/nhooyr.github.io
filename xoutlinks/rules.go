package main

import (
	"net/url"
	"regexp"
	"strings"
)

// Domains include subdomains. Path rules match the domain (or www) and subpaths.
// Rules exclude matching links unless block404 is set, which only blocks 404s.
var rules = []linkRule{
	{domain: "github.com", path: "/nhooyr/nhooyr.github.io"}, // This blog's source repository.
	{domain: "wikipedia.org"},                                // Skip Wikipedia links.
	{domain: "archive.org"},                                  // Skip Internet Archive links.
	{domain: "linkedin.com", block404: true},                 // Treat LinkedIn 404s as blocked access.
	{domain: "threads.com", block404: true},                  // Treat Threads 404s as blocked access.
}

type linkRule struct {
	domain   string
	path     string
	block404 bool
}

func (r linkRule) matches(parsed *url.URL) bool {
	host := strings.TrimSuffix(strings.ToLower(parsed.Hostname()), ".")
	if r.path != "" {
		if strings.TrimPrefix(host, "www.") != r.domain {
			return false
		}
		path := strings.ToLower(parsed.Path)
		return path == r.path || strings.HasPrefix(path, r.path+"/")
	}
	return host == r.domain || strings.HasSuffix(host, "."+r.domain)
}

var forbiddenHTTPStatus = regexp.MustCompile(`(?i)\bHTTP\s+status\s*=\s*403\b`)
var missingHTTPStatus = regexp.MustCompile(`(?i)(\bHTTP\s+(?:status\s*=\s*)?)404\b`)

func (e Entry) permanentFailure() bool {
	if e.Status == "needs_auth" || e.Status == "replay_pending" {
		return false
	}
	return e.Status == "blocked" || e.Error == blockedURLError || e.Error == "error:no-access" || forbiddenHTTPStatus.MatchString(e.ErrorMsg)
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
	for _, rule := range rules {
		if !rule.block404 || !rule.matches(parsed) {
			continue
		}
		entry.Error = "error:no-request"
		if missingHTTPStatus.MatchString(entry.ErrorMsg) {
			entry.ErrorMsg = missingHTTPStatus.ReplaceAllString(entry.ErrorMsg, "${1}403")
		} else if entry.ErrorMsg == "" {
			entry.ErrorMsg = "HTTP status=403"
		} else if !forbiddenHTTPStatus.MatchString(entry.ErrorMsg) {
			entry.ErrorMsg += " (HTTP status=403)"
		}
		if entry.Status == "replay_pending" {
			entry.Status = "blocked"
		}
		break
	}
	return entry
}

func excludedLink(raw string) bool {
	parsed, err := url.Parse(raw)
	if err != nil {
		return true
	}
	for _, rule := range rules {
		if !rule.block404 && rule.matches(parsed) {
			return true
		}
	}
	return false
}
