package autopsy

import (
	"regexp"
)

// secretPatterns matches common secret formats in strings.
// These are applied to error messages and payloads before storage
// to prevent credential leakage into autopsy reports.
var secretPatterns = []*regexp.Regexp{
	// Bearer tokens
	regexp.MustCompile(`(?i)(Bearer\s+)\S+`),
	// Key=value patterns for passwords, secrets, tokens, API keys
	regexp.MustCompile(`(?i)(password|passwd|secret|token|api[_-]?key|access[_-]?key|auth)\s*[=:]\s*\S+`),
	// Authorization headers
	regexp.MustCompile(`(?i)(authorization)\s*[=:]\s*\S+`),
	// AWS-style keys (AKIA...)
	regexp.MustCompile(`AKIA[0-9A-Z]{16}`),
	// Long base64-like strings (40+ chars, likely tokens/keys)
	regexp.MustCompile(`[A-Za-z0-9+/]{40,}={0,2}`),
	// Passwords in URLs (scheme://user:pass@host)
	regexp.MustCompile(`://[^@\s]+:[^@\s]+@`),
}

// ScrubSecrets replaces detected secrets in the input string with [REDACTED].
func ScrubSecrets(input string) string {
	result := input
	for _, pattern := range secretPatterns {
		result = pattern.ReplaceAllStringFunc(result, func(match string) string {
			// For key=value patterns, keep the key and redact the value
			kv := regexp.MustCompile(`(?i)^((?:password|passwd|secret|token|api[_-]?key|access[_-]?key|auth|authorization|Bearer)\s*[=:\s])\s*`)
			if loc := kv.FindStringIndex(match); loc != nil {
				return match[:loc[1]] + "[REDACTED]"
			}
			return "[REDACTED]"
		})
	}
	return result
}
