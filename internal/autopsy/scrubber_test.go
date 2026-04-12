package autopsy

import (
	"strings"
	"testing"
)

func TestScrubSecrets_BearerToken(t *testing.T) {
	input := "Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.abc.def"
	result := ScrubSecrets(input)
	if strings.Contains(result, "eyJhbGci") {
		t.Errorf("Bearer token not scrubbed: %s", result)
	}
	if !strings.Contains(result, "[REDACTED]") {
		t.Errorf("expected [REDACTED] in output: %s", result)
	}
}

func TestScrubSecrets_PasswordKeyValue(t *testing.T) {
	tests := []struct {
		input  string
		secret string
	}{
		{"password=hunter2", "hunter2"},
		{"secret: mysecretvalue", "mysecretvalue"},
		{"token=abc123def456", "abc123def456"},
		{"api_key=sk_live_xxxxxxxxxxxxxxxx", "sk_live_xxxxxxxxxxxxxxxx"},
		{"API-KEY: some-api-key-value", "some-api-key-value"},
	}

	for _, tt := range tests {
		result := ScrubSecrets(tt.input)
		if strings.Contains(result, tt.secret) {
			t.Errorf("secret not scrubbed from %q: got %q", tt.input, result)
		}
	}
}

func TestScrubSecrets_PasswordInURL(t *testing.T) {
	input := "connecting to https://admin:secretpass@db.example.com:5432/mydb"
	result := ScrubSecrets(input)
	if strings.Contains(result, "secretpass") {
		t.Errorf("password in URL not scrubbed: %s", result)
	}
}

func TestScrubSecrets_NoFalsePositive(t *testing.T) {
	input := "job failed with exit code 1: file not found"
	result := ScrubSecrets(input)
	if result != input {
		t.Errorf("innocent string was modified: %q → %q", input, result)
	}
}

func TestScrubSecrets_AWSKey(t *testing.T) {
	input := "found credential AKIAIOSFODNN7EXAMPLE in config"
	result := ScrubSecrets(input)
	if strings.Contains(result, "AKIAIOSFODNN7EXAMPLE") {
		t.Errorf("AWS key not scrubbed: %s", result)
	}
}

func TestClassifyCause_Timeout(t *testing.T) {
	attempts := []AttemptDetail{
		{Error: "context deadline exceeded"},
		{Error: "timeout waiting for response"},
	}
	cause, recs := classifyCause(attempts, nil)
	if !strings.Contains(cause, "timeout") {
		t.Errorf("expected timeout cause, got: %s", cause)
	}
	if len(recs) == 0 {
		t.Error("expected recommendations")
	}
}

func TestClassifyCause_PersistentFailure(t *testing.T) {
	attempts := []AttemptDetail{
		{Error: "file not found: /data/input.csv"},
		{Error: "file not found: /data/input.csv"},
	}
	cause, _ := classifyCause(attempts, nil)
	if !strings.Contains(cause, "persistent failure") {
		t.Errorf("expected persistent failure, got: %s", cause)
	}
}

func TestClassifyCause_Intermittent(t *testing.T) {
	attempts := []AttemptDetail{
		{Error: "connection refused", WorkerID: "w1"},
		{Error: "out of memory", WorkerID: "w2"},
	}
	cause, _ := classifyCause(attempts, nil)
	if !strings.Contains(cause, "intermittent") {
		t.Errorf("expected intermittent failure, got: %s", cause)
	}
}
