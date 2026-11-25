package main

import "testing"

func TestRunWithoutCommand(t *testing.T) {
	if code := run(nil); code != 1 {
		t.Fatalf("no command: exit %d, want 1", code)
	}
}

func TestRunVersion(t *testing.T) {
	if code := run([]string{"version"}); code != 0 {
		t.Fatalf("version: exit %d, want 0", code)
	}
}

func TestRunCompletion(t *testing.T) {
	if code := run([]string{"completion", "bash"}); code != 0 {
		t.Fatalf("completion bash: exit %d, want 0", code)
	}
	if code := run([]string{"completion", "fish"}); code != 1 {
		t.Fatalf("completion fish: exit %d, want 1", code)
	}
}
