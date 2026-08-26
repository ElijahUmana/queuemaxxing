package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestOperationJournalClassifiesCompletedAndAmbiguous(t *testing.T) {
	journal := NewOperationJournal()
	now := time.Date(2026, 8, 26, 20, 0, 0, 0, time.UTC)
	if err := journal.Invoke("one", "enqueue", json.RawMessage(`{"id":1}`), now); err != nil {
		t.Fatal(err)
	}
	if err := journal.Invoke("two", "ack", json.RawMessage(`{"id":2}`), now); err != nil {
		t.Fatal(err)
	}
	if err := journal.Complete("one", 201, json.RawMessage(`{"ok":true}`), nil, now.Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if got := journal.Completed(); len(got) != 1 || got[0].ID != "one" {
		t.Fatalf("completed = %+v", got)
	}
	if got := journal.Ambiguous(); len(got) != 1 || got[0].ID != "two" {
		t.Fatalf("ambiguous = %+v", got)
	}
	path := filepath.Join(t.TempDir(), "operations.json")
	if err := journal.Write(path); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(contents, []byte(`"completed_at"`)) {
		t.Fatalf("operation artifact lacks completion: %s", contents)
	}
}

func TestCorruptionHelpersPreserveSource(t *testing.T) {
	directory := t.TempDir()
	source := filepath.Join(directory, "source.wal")
	truncated := filepath.Join(directory, "truncated.wal")
	flipped := filepath.Join(directory, "flipped.wal")
	original := []byte{0x00, 0x01, 0x02, 0x03}
	if err := os.WriteFile(source, original, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := CopyAndTruncate(source, truncated, 2); err != nil {
		t.Fatal(err)
	}
	if err := CopyAndFlipBit(source, flipped, 1, 2); err != nil {
		t.Fatal(err)
	}
	assertFileBytes(t, source, original)
	assertFileBytes(t, truncated, original[:2])
	assertFileBytes(t, flipped, []byte{0x00, 0x05, 0x02, 0x03})
}

func TestStartProcessCapturesArtifacts(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell fixture is Unix-specific")
	}
	artifacts := filepath.Join(t.TempDir(), "artifacts")
	process, err := StartProcess(context.Background(), "/bin/sh", []string{"-c", "printf stdout; printf stderr >&2"}, nil, artifacts)
	if err != nil {
		t.Fatal(err)
	}
	if err := process.Wait(); err != nil {
		t.Fatal(err)
	}
	assertFileBytes(t, filepath.Join(artifacts, "stdout.log"), []byte("stdout"))
	assertFileBytes(t, filepath.Join(artifacts, "stderr.log"), []byte("stderr"))
}

func TestWaitForOutputIncludesDiagnostics(t *testing.T) {
	process := &Process{Stdout: &lockedBuffer{}, Stderr: &lockedBuffer{}}
	_, _ = process.Stdout.Write([]byte("ready"))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := process.WaitForOutput(ctx, "stdout", "ready"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err := process.WaitForOutput(ctx, "stdout", "missing")
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v", err)
	}
}

func TestFreeTCPAddress(t *testing.T) {
	address, err := FreeTCPAddress()
	if err != nil {
		t.Fatal(err)
	}
	if address == "" {
		t.Fatal("empty address")
	}
}

func assertFileBytes(t *testing.T, path string, want []byte) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("%s = %x, want %x", path, got, want)
	}
}
