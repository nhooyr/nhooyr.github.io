package main

import (
	"net"
	"net/url"
	"strings"
)

// Edit this configuration to change the exclusions. Domain names include every
// subdomain and are case-insensitive. Paths match a whole path segment and its
// descendants unless Exact is set. Path/query host rules also match www; set
// Subdomains to include other subdomains. Query keys are case-insensitive;
// query token rules match complete, whitespace-separated tokens.
// Inspect a URL without making any archive requests:
//
//	./run.sh --explain https://example.com/page
var exclusionSettings = rulesConfig{
	BlockCredentials: true,
	BlockPrivateIPs:  true,
	SensitiveQueryKeys: []string{
		"access_token", "auth", "authorization", "password", "pwd", "secret", "token",
	},
	Domains: []string{
		// Sites we control or do not need to preserve.
		"leqra.com", "wikipedia.org",

		// Local hostnames.
		"localhost", "local",

		// Social, messaging, media-sharing, and meeting services.
		"facebook.com", "fb.com", "fb.me", "instagram.com", "threads.com", "threads.net",
		"twitter.com", "x.com", "t.co", "linkedin.com", "lnkd.in", "tiktok.com",
		"youtube.com", "youtu.be", "reddit.com", "redd.it", "pinterest.com", "pin.it",
		"flickr.com", "spotify.com", "snapchat.com", "discord.com", "discordapp.com", "discord.gg",
		"telegram.org", "t.me", "telegram.me", "whatsapp.com", "wa.me", "signal.org", "signal.me",
		"messenger.com", "m.me", "zoom.us",
	},
	Paths: []pathRule{
		{Host: "github.com", Path: "/nhooyr", IgnoreCase: true},
		{Host: "chatgpt.com", Path: "/u"},
		{Host: "stackexchange.com", Path: "/users", Subdomains: true},
		{Host: "stackoverflow.com", Path: "/users"},
	},
}

type rulesConfig struct {
	BlockCredentials   bool
	BlockPrivateIPs    bool
	SensitiveQueryKeys []string
	Domains            []string
	Paths              []pathRule
	QueryTokens        []queryTokenRule
}

type pathRule struct {
	Host       string
	Path       string
	Exact      bool
	IgnoreCase bool
	Subdomains bool
}

type queryTokenRule struct {
	Host       string
	Path       string
	Key        string
	Tokens     []string
	IgnoreCase bool
	Subdomains bool
}

func matchesHost(host, domain string) bool {
	domain = strings.TrimSuffix(strings.ToLower(domain), ".")
	return domain != "" && (host == domain || strings.HasSuffix(host, "."+domain))
}

func matchesText(value, pattern string, ignoreCase bool) bool {
	return value == pattern || (ignoreCase && strings.EqualFold(value, pattern))
}

func matchesRuleHost(host, domain string, subdomains bool) bool {
	if subdomains {
		return matchesHost(host, domain)
	}
	return strings.TrimPrefix(host, "www.") == strings.TrimPrefix(strings.ToLower(domain), "www.")
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
	if exclusionSettings.BlockPrivateIPs {
		if ip := net.ParseIP(host); ip != nil && (ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsUnspecified()) {
			return "non-public IP address"
		}
	}
	if exclusionSettings.BlockCredentials && parsed.User != nil {
		return "URL contains credentials"
	}
	for key := range parsed.Query() {
		for _, sensitive := range exclusionSettings.SensitiveQueryKeys {
			if strings.EqualFold(key, sensitive) {
				return "sensitive query parameter: " + sensitive
			}
		}
	}
	for _, domain := range exclusionSettings.Domains {
		if matchesHost(host, domain) {
			return "excluded domain: " + domain
		}
	}
	for _, rule := range exclusionSettings.Paths {
		if !matchesRuleHost(host, rule.Host, rule.Subdomains) {
			continue
		}
		path, pattern := parsed.Path, rule.Path
		if rule.IgnoreCase {
			path, pattern = strings.ToLower(path), strings.ToLower(pattern)
		}
		if path == pattern || (!rule.Exact && strings.HasPrefix(path, strings.TrimSuffix(pattern, "/")+"/")) {
			return "excluded path: " + rule.Host + rule.Path
		}
	}
	for _, rule := range exclusionSettings.QueryTokens {
		if !matchesRuleHost(host, rule.Host, rule.Subdomains) || !matchesText(parsed.Path, rule.Path, rule.IgnoreCase) {
			continue
		}
		for key, values := range parsed.Query() {
			if !strings.EqualFold(key, rule.Key) {
				continue
			}
			for _, value := range values {
				for _, token := range strings.Fields(value) {
					for _, excluded := range rule.Tokens {
						if matchesText(token, excluded, rule.IgnoreCase) {
							return "excluded query token: " + excluded
						}
					}
				}
			}
		}
	}
	return ""
}
