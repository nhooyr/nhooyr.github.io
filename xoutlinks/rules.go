package main

import (
	"net/url"
	"strings"
)

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
