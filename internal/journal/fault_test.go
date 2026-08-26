package journal

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var errInjected = errors.New("injected fault")

func TestAppendFaultPhasesFailStopWithoutPublishing(t *testing.T) {
	for _, phase := range []string{"before-write", "after-write", "wal-sync", "head-rename"} {
		t.Run(phase, func(t *testing.T) {
			armed := false
			store := openTestJournal(t, Config{Dir: t.TempDir(), Faults: FaultHooks{
				BeforeWrite: func(path string, _ []byte) error {
					if armed && phase == "before-write" && strings.HasSuffix(path, ".wal") {
						return errInjected
					}
					return nil
				},
				AfterWrite: func(path string, _ []byte) error {
					if armed && phase == "after-write" && strings.HasSuffix(path, ".wal") {
						return errInjected
					}
					return nil
				},
				BeforeSync: func(path string) error {
					if armed && phase == "wal-sync" && strings.HasSuffix(path, ".wal") {
						return errInjected
					}
					return nil
				},
				BeforeRename: func(_, destination string) error {
					if armed && phase == "head-rename" && filepath.Base(destination) == "HEAD" {
						return errInjected
					}
					return nil
				},
			}})
			armed = true
			if _, err := store.Append(context.Background(), TransactionID{1}, []byte("payload")); !errors.Is(err, ErrReadOnly) {
				t.Fatalf("append error = %v", err)
			}
			if len(store.Records()) != 0 || store.Stats().DurableLSN != 0 || !store.Stats().ReadOnly {
				t.Fatalf("published state = %#v records=%#v", store.Stats(), store.Records())
			}
		})
	}
}

func TestCheckpointFaultPhasesFailStopWithoutPublishing(t *testing.T) {
	for _, phase := range []string{"snapshot-write", "snapshot-sync", "snapshot-rename", "snapshot-dir-sync"} {
		t.Run(phase, func(t *testing.T) {
			armed := false
			store := openTestJournal(t, Config{Dir: t.TempDir(), Faults: FaultHooks{
				BeforeWrite: func(path string, _ []byte) error {
					if armed && phase == "snapshot-write" && strings.HasSuffix(path, ".snap.tmp") {
						return errInjected
					}
					return nil
				},
				BeforeSync: func(path string) error {
					if !armed {
						return nil
					}
					if phase == "snapshot-sync" && strings.HasSuffix(path, ".snap.tmp") {
						return errInjected
					}
					if phase == "snapshot-dir-sync" && filepath.Base(path) == "snapshots" {
						return errInjected
					}
					return nil
				},
				BeforeRename: func(_, destination string) error {
					if armed && phase == "snapshot-rename" && strings.HasSuffix(destination, ".snap") {
						return errInjected
					}
					return nil
				},
			}})
			if _, err := store.Append(context.Background(), TransactionID{1}, []byte("one")); err != nil {
				t.Fatal(err)
			}
			armed = true
			if err := store.Checkpoint(context.Background(), 1, []byte("state")); !errors.Is(err, ErrReadOnly) {
				t.Fatalf("checkpoint error = %v", err)
			}
			if snapshot := store.Snapshot(); snapshot.Generation != 0 || len(snapshot.Payload) != 0 {
				t.Fatalf("published snapshot = %+v", snapshot)
			}
		})
	}
}

func TestRotationAndCompactionFaults(t *testing.T) {
	t.Run("rotation", func(t *testing.T) {
		armed := false
		store := openTestJournal(t, Config{Dir: t.TempDir(), SegmentSize: 190, Faults: FaultHooks{BeforeSync: func(path string) error {
			if armed && strings.HasSuffix(path, ".wal") {
				return errInjected
			}
			return nil
		}}})
		if _, err := store.Append(context.Background(), TransactionID{1}, []byte("first")); err != nil {
			t.Fatal(err)
		}
		armed = true
		if _, err := store.Append(context.Background(), TransactionID{2}, []byte("second")); !errors.Is(err, ErrReadOnly) {
			t.Fatalf("rotation error = %v", err)
		}
	})

	t.Run("compaction-remove", func(t *testing.T) {
		armed := false
		store := openTestJournal(t, Config{Dir: t.TempDir(), SegmentSize: 190, Faults: FaultHooks{BeforeRemove: func(path string) error {
			if armed && strings.HasSuffix(path, ".wal") {
				return errInjected
			}
			return nil
		}}})
		for index := byte(1); index <= 4; index++ {
			if _, err := store.Append(context.Background(), TransactionID{index}, []byte("record")); err != nil {
				t.Fatal(err)
			}
			if err := store.Checkpoint(context.Background(), uint64(index), []byte{index}); err != nil {
				t.Fatal(err)
			}
		}
		armed = true
		if err := store.Checkpoint(context.Background(), 4, []byte("latest")); !errors.Is(err, ErrReadOnly) && !errors.Is(err, ErrInvalidLSN) {
			t.Fatalf("compaction error = %v", err)
		}
	})
}

func TestInterruptedPublicationCleanupAndQuarantineConflict(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "wal"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "snapshots"), 0o700); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{filepath.Join(dir, "HEAD.tmp"), filepath.Join(dir, "wal", "segment.tmp"), filepath.Join(dir, "snapshots", "snapshot.tmp")} {
		if err := os.WriteFile(path, []byte("partial"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	store := openTestJournal(t, Config{Dir: dir})
	for _, path := range []string{filepath.Join(dir, "HEAD.tmp"), filepath.Join(dir, "wal", "segment.tmp"), filepath.Join(dir, "snapshots", "snapshot.tmp")} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("interrupted file remains: %s (%v)", path, err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}
