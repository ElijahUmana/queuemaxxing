package journal

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestOpenRejectsFilesystemShapeConflicts(t *testing.T) {
	parent := t.TempDir()
	filePath := filepath.Join(parent, "file")
	if err := os.WriteFile(filePath, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(Config{Dir: filePath}); err == nil {
		t.Fatal("file data root accepted")
	}

	for _, child := range []string{"wal", "snapshots"} {
		t.Run(child, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, child), []byte("x"), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := Open(Config{Dir: dir}); err == nil {
				t.Fatalf("%s file accepted", child)
			}
		})
	}
}

func TestOpenRejectsHeadWithoutWALAndNonPristineWithoutHead(t *testing.T) {
	storeID := [16]byte{1}
	t.Run("head-without-wal", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.MkdirAll(filepath.Join(dir, "wal"), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Join(dir, "snapshots"), 0o700); err != nil {
			t.Fatal(err)
		}
		head := encodeHead(headState{StoreID: storeID, WALFloor: 1, WALHead: 1})
		if err := os.WriteFile(filepath.Join(dir, "HEAD"), head, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := Open(Config{Dir: dir}); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("non-pristine-without-head", func(t *testing.T) {
		dir := t.TempDir()
		writeRecoverySegments(t, dir, []recoverySegment{{header: segmentHeader{StoreID: storeID, ID: 1, FirstLSN: 1}, records: []Record{{LSN: 1}}}})
		if err := os.MkdirAll(filepath.Join(dir, "snapshots"), 0o700); err != nil {
			t.Fatal(err)
		}
		if _, err := Open(Config{Dir: dir}); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("error = %v", err)
		}
	})
}

func TestRecoverInterruptedInitializationAndSnapshotConflict(t *testing.T) {
	storeID := [16]byte{1}
	t.Run("pristine", func(t *testing.T) {
		dir := t.TempDir()
		writeRecoverySegments(t, dir, []recoverySegment{{header: segmentHeader{StoreID: storeID, ID: 1, FirstLSN: 1}}})
		if err := os.MkdirAll(filepath.Join(dir, "snapshots"), 0o700); err != nil {
			t.Fatal(err)
		}
		store, err := Open(Config{Dir: dir})
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()
		if store.head.WALFloor != 1 || store.active == nil {
			t.Fatalf("recovered = %+v", store.head)
		}
	})
	t.Run("snapshot-conflict", func(t *testing.T) {
		dir := t.TempDir()
		store := openTestJournal(t, Config{Dir: dir})
		if _, err := store.Append(context.Background(), TransactionID{1}, []byte("one")); err != nil {
			t.Fatal(err)
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
		snapshot := encodeSnapshot(store.storeID, Snapshot{Generation: 2, ThroughLSN: 1, Payload: []byte("future")})
		path := filepath.Join(dir, "snapshots", snapshotName(2, 1))
		if err := os.WriteFile(path, snapshot, 0o600); err != nil {
			t.Fatal(err)
		}
		quarantine := filepath.Join(dir, "quarantine")
		if err := os.MkdirAll(quarantine, 0o700); err != nil {
			t.Fatal(err)
		}
		destination := filepath.Join(quarantine, filepath.Base(path)+".uncommitted")
		if err := os.WriteFile(destination, []byte("existing"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := Open(Config{Dir: dir}); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("error = %v", err)
		}
	})
}
