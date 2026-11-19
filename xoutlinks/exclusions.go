package main

import (
	"net/url"
	"regexp"
	"strings"
)

var exclusions = []linkRule{
	{domain: "linkedin.com", block404: true},                 // Treat LinkedIn 404s as blocked access.
	{domain: "pinterest.com", blockTimeout: true},            // Treat Pinterest timeouts as blocked access.
}

type linkRule struct {
	domain       string
	path         string
	block404     bool
	blockTimeout bool
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
var blockedHTTPStatus = regexp.MustCompile(`(?i)\bHTTP\s+(?:status\s*=\s*)?999\b`)
var missingHTTPStatus = regexp.MustCompile(`(?i)(\bHTTP\s+(?:status\s*=\s*)?)404\b`)
var timeoutError = regexp.MustCompile(`(?i)\b(?:timeout|timed\s+out|deadline\s+exceeded|ERR_TIMED_OUT)\b`)

func (e Entry) permanentFailure() bool {
	if e.Status == "blocked" || e.Error == "error:blocked-url" || e.Error == "error:blocked" || blockedHTTPStatus.MatchString(e.ErrorMsg) {
		return true
	}
	return e.Error == "error:no-access" || forbiddenHTTPStatus.MatchString(e.ErrorMsg)
}

func (e Entry) notFound() bool {
	return e.Error == "error:not-found" || e.Error == "error:target-not-found" || missingHTTPStatus.MatchString(e.ErrorMsg)
}

func normalizeFailure(raw string, entry Entry) Entry {
	if entry.Status == "saved" {
		return entry
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return entry
	}
	for _, rule := range exclusions {
		if !rule.matches(parsed) {
			continue
		}
		if rule.blockTimeout && timeoutError.MatchString(entry.errorText()) {
			if entry.ErrorMsg == "" {
				entry.ErrorMsg = entry.Error
			}
			entry.Error = "error:blocked"
			return entry
		}
		if !rule.block404 || !entry.notFound() {
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
		break
	}
	return entry
}

func excludedLink(raw string) bool {
	parsed, err := url.Parse(raw)
	if err != nil {
		return true
	}
	for _, rule := range exclusions {
		if !rule.block404 && !rule.blockTimeout && rule.matches(parsed) {
			return true
		}
	}
	return false
}
