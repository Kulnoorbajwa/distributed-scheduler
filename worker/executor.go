package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os/exec"
	"strings"
	"time"

	"go.uber.org/zap"
)

// allowedCommands is the set of binaries workers are permitted to execute.
// Shell interpreters (bash, sh, python) are intentionally excluded — they
// allow arbitrary code execution, defeating the allowlist entirely.
var allowedCommands = map[string]bool{
	"echo":     true,
	"date":     true,
	"hostname": true,
	"ls":       true,
	"cat":      true,
	"wc":       true,
	"sort":     true,
	"head":     true,
	"tail":     true,
	"grep":     true,
	"sleep":    true,
}

// blockedArgs prevents dangerous flags on allowed commands.
// Note: exec.Command does NOT invoke a shell, so metacharacters like |, ;, &&
// are not interpreted. These are blocked defensively in case of future changes.
var blockedArgs = []string{
	"--exec",
	"-o",
	"--output",
	"--upload-file", // curl write
	"-T",            // curl upload shorthand
	"-K",            // curl read config from file
	"-F",            // curl form upload (can read local files)
	">/",            // redirect (no-op without shell, defensive)
	"|",             // pipe (no-op without shell, defensive)
	";",             // chaining (no-op without shell, defensive)
	"&&",            // chaining
	"||",            // chaining
	"`",             // backtick subshell
	"$(",            // subshell
	"-c",            // interpreter flag (bash -c, sh -c, python -c)
}

// JobPayload is the JSON structure workers receive in job.Payload
//
//	{"type": "shell",  "command": "python3 process_data.py --input data.csv"}
//	{"type": "http",   "method": "GET", "url": "https://example.com/api/health"}
//	{"type": "sleep",  "duration_ms": 5000}
//	{"type": "fail",   "message": "simulated error"}
type JobPayload struct {
	Type string `json:"type"`

	// shell jobs
	Command string `json:"command,omitempty"`

	// http jobs
	Method  string            `json:"method,omitempty"`
	URL     string            `json:"url,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
	Body    string            `json:"body,omitempty"`

	// sleep jobs (testing)
	DurationMs int `json:"duration_ms,omitempty"`

	// fail jobs (testing retries)
	Message string `json:"message,omitempty"`
}

// JobResult captures the output of a job execution
type JobResult struct {
	ExitCode int    `json:"exit_code"`
	Stdout   string `json:"stdout"`
	Stderr   string `json:"stderr"`
	Duration string `json:"duration"`
}

// executePayload parses the JSON payload and runs the appropriate job type
func (w *Worker) executePayload(ctx context.Context, payload string) error {
	var p JobPayload
	if err := json.Unmarshal([]byte(payload), &p); err != nil {
		return fmt.Errorf("invalid JSON payload: %w", err)
	}

	switch p.Type {
	case "shell":
		return w.executeShell(ctx, &p)
	case "http":
		return w.executeHTTP(ctx, &p)
	case "sleep":
		return w.executeSleep(ctx, &p)
	case "fail":
		return fmt.Errorf("deliberate failure: %s", p.Message)
	default:
		return fmt.Errorf("unknown job type: %q", p.Type)
	}
}

// validateCommand checks the command against the allowlist and blocked args
func validateCommand(command string) error {
	if command == "" {
		return fmt.Errorf("shell job requires a 'command' field")
	}

	parts := strings.Fields(command)
	binary := parts[0]

	// Strip path — only check the base binary name
	if idx := strings.LastIndex(binary, "/"); idx >= 0 {
		binary = binary[idx+1:]
	}

	if !allowedCommands[binary] {
		return fmt.Errorf("command %q is not in the allowlist", binary)
	}

	// Check for dangerous arguments / shell injection patterns
	for _, arg := range parts[1:] {
		for _, blocked := range blockedArgs {
			if strings.Contains(arg, blocked) {
				return fmt.Errorf("argument %q contains blocked pattern %q", arg, blocked)
			}
		}
	}

	// Block path traversal in arguments
	for _, arg := range parts[1:] {
		if strings.Contains(arg, "../") {
			return fmt.Errorf("argument %q contains path traversal", arg)
		}
	}

	return nil
}

// executeShell runs an allowlisted command and captures output
func (w *Worker) executeShell(ctx context.Context, p *JobPayload) error {
	if err := validateCommand(p.Command); err != nil {
		return fmt.Errorf("command rejected: %w", err)
	}

	start := time.Now()

	parts := strings.Fields(p.Command)
	cmd := exec.CommandContext(ctx, parts[0], parts[1:]...)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()

	result := JobResult{
		Stdout:   truncate(stdout.String(), 4096),
		Stderr:   truncate(stderr.String(), 4096),
		Duration: time.Since(start).String(),
	}

	if cmd.ProcessState != nil {
		result.ExitCode = cmd.ProcessState.ExitCode()
	}

	resultJSON, _ := json.Marshal(result)
	w.log.Info("shell job completed",
		zap.String("command", p.Command),
		zap.Int("exit_code", result.ExitCode),
		zap.String("duration", result.Duration),
		zap.String("result", string(resultJSON)),
	)

	if err != nil {
		return fmt.Errorf("command failed (exit %d): %s", result.ExitCode, truncate(stderr.String(), 512))
	}
	return nil
}

// ssrfSafeClient returns an HTTP client whose dialer validates every resolved
// IP against private/internal ranges, preventing DNS rebinding attacks.
func ssrfSafeClient(timeout time.Duration) *http.Client {
	dialer := &net.Dialer{Timeout: timeout}
	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(addr)
			if err != nil {
				return nil, fmt.Errorf("invalid address %q: %w", addr, err)
			}

			ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
			if err != nil {
				return nil, fmt.Errorf("DNS lookup failed for %q: %w", host, err)
			}

			for _, ipAddr := range ips {
				ip := ipAddr.IP
				if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
					return nil, fmt.Errorf("blocked connection to private IP %s (SSRF protection)", ip)
				}
				if ip4 := ip.To4(); ip4 != nil && ip4[0] == 169 && ip4[1] == 254 {
					return nil, fmt.Errorf("blocked connection to metadata IP %s (SSRF protection)", ip)
				}
			}

			// Connect using the resolved IPs directly — no second DNS lookup
			return dialer.DialContext(ctx, network, net.JoinHostPort(ips[0].IP.String(), port))
		},
	}
	return &http.Client{
		Transport: transport,
		Timeout:   timeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 5 {
				return fmt.Errorf("too many redirects")
			}
			return nil
		},
	}
}

// blockedHeaders are HTTP headers that user payloads must not override.
var blockedHeaders = map[string]bool{
	"host":              true,
	"transfer-encoding": true,
	"te":                true,
	"trailer":           true,
	"connection":        true,
	"upgrade":           true,
	"proxy-authorization": true,
}

// executeHTTP makes an HTTP request with SSRF-safe dialing
func (w *Worker) executeHTTP(ctx context.Context, p *JobPayload) error {
	if p.URL == "" {
		return fmt.Errorf("http job requires a 'url' field")
	}
	if p.Method == "" {
		p.Method = "GET"
	}

	// Only allow http and https schemes
	parsed, err := url.Parse(p.URL)
	if err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("URL scheme %q not allowed — only http and https", parsed.Scheme)
	}
	if parsed.Host == "" {
		return fmt.Errorf("URL must include a host")
	}

	start := time.Now()

	var bodyReader io.Reader
	if p.Body != "" {
		bodyReader = strings.NewReader(p.Body)
	}

	req, err := http.NewRequestWithContext(ctx, p.Method, p.URL, bodyReader)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	for k, v := range p.Headers {
		if blockedHeaders[strings.ToLower(k)] {
			return fmt.Errorf("header %q is not allowed", k)
		}
		req.Header.Set(k, v)
	}

	// Use SSRF-safe client that validates IPs at dial time (no TOCTOU)
	client := ssrfSafeClient(30 * time.Second)
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))

	w.log.Info("http job completed",
		zap.String("method", p.Method),
		zap.String("url", p.URL),
		zap.Int("status_code", resp.StatusCode),
		zap.String("duration", time.Since(start).String()),
		zap.String("body_preview", truncate(string(body), 256)),
	)

	if resp.StatusCode >= 400 {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, truncate(string(body), 256))
	}
	return nil
}

// executeSleep waits for the specified duration (useful for testing)
func (w *Worker) executeSleep(ctx context.Context, p *JobPayload) error {
	if p.DurationMs <= 0 {
		p.DurationMs = 2000
	}

	w.log.Info("sleep job starting", zap.Int("duration_ms", p.DurationMs))

	select {
	case <-ctx.Done():
		return fmt.Errorf("job cancelled or timed out during sleep")
	case <-time.After(time.Duration(p.DurationMs) * time.Millisecond):
		return nil
	}
}

// truncate cuts a string to max length with an ellipsis
func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}
