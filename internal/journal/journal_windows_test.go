//go:build windows

package journal

import (
	"context"
	"path/filepath"
	"testing"
)

func TestWindowsJournalLifecycleDoesNotSyncDirectories(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "journal")
	store, err := Open(Config{Dir: dir})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.Append(context.Background(), TransactionID{1}, []byte("first")); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if err := store.Checkpoint(context.Background(), 1, []byte("snapshot")); err != nil {
		t.Fatalf("Checkpoint() error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reopened, err := Open(Config{Dir: dir})
	if err != nil {
		t.Fatalf("second Open() error = %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if _, err := reopened.Append(context.Background(), TransactionID{2}, []byte("second")); err != nil {
		t.Fatalf("second Append() error = %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
}
