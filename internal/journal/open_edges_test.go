package journal

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestOpenRestrictsExistingStorageModes(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows does not expose Unix permission bits")
	}
	dir := t.TempDir()
	store := openTestJournal(t, Config{Dir: dir})
	if _, err := store.Append(context.Background(), TransactionID{1}, []byte("record")); err != nil {
		t.Fatal(err)
	}
	if err := store.Checkpoint(context.Background(), 1, []byte("snapshot")); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	for _, path := range []string{dir, filepath.Join(dir, "wal"), filepath.Join(dir, "snapshots")} {
		if err := os.Chmod(path, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	files := []string{filepath.Join(dir, "HEAD"), filepath.Join(dir, "LOCK")}
	segments, _ := filepath.Glob(filepath.Join(dir, "wal", "*.wal"))
	snapshots, _ := filepath.Glob(filepath.Join(dir, "snapshots", "*.snap"))
	files = append(files, segments...)
	files = append(files, snapshots...)
	for _, path := range files {
		if err := os.Chmod(path, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	reopened := openTestJournal(t, Config{Dir: dir})
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{dir, filepath.Join(dir, "wal"), filepath.Join(dir, "snapshots")} {
		info, err := os.Stat(path)
		if err != nil || info.Mode().Perm() != 0o700 {
			t.Fatalf("directory mode %s = %v, error=%v", path, info.Mode().Perm(), err)
		}
	}
	for _, path := range files {
		info, err := os.Stat(path)
		if err != nil || info.Mode().Perm() != 0o600 {
			t.Fatalf("file mode %s = %v, error=%v", path, info.Mode().Perm(), err)
		}
	}
}

func TestOpenRejectsSymlinkedJournalPathsWithoutTouchingTargets(t *testing.T) {
	for _, test := range []struct {
		name     string
		linkPath func(string) string
		target   func(string) string
	}{
		{name: "wal-directory", linkPath: func(dir string) string { return filepath.Join(dir, "wal") }, target: func(external string) string { return external }},
		{name: "snapshot-directory", linkPath: func(dir string) string { return filepath.Join(dir, "snapshots") }, target: func(external string) string { return external }},
		{name: "head-temporary", linkPath: func(dir string) string { return filepath.Join(dir, "HEAD.tmp") }, target: func(external string) string { return filepath.Join(external, "outside.tmp") }},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			external := t.TempDir()
			target := test.target(external)
			if target != external {
				if err := os.WriteFile(target, []byte("outside"), 0o600); err != nil {
					t.Fatal(err)
				}
			} else if err := os.WriteFile(filepath.Join(external, "outside.tmp"), []byte("outside"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, test.linkPath(dir)); err != nil {
				t.Skipf("symbolic links unavailable: %v", err)
			}
			if _, err := Open(Config{Dir: dir}); err == nil {
				t.Fatal("Open() accepted symlinked journal path")
			}
			contents, err := os.ReadFile(filepath.Join(external, "outside.tmp"))
			if err != nil || string(contents) != "outside" {
				t.Fatalf("external target changed: contents=%q error=%v", contents, err)
			}
		})
	}
}

func TestOpenRejectsSymlinkedInterruptedPublicationWithoutTouchingTarget(t *testing.T) {
	for _, directory := range []string{"wal", "snapshots"} {
		t.Run(directory, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.MkdirAll(filepath.Join(dir, "wal"), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(filepath.Join(dir, "snapshots"), 0o700); err != nil {
				t.Fatal(err)
			}
			external := filepath.Join(t.TempDir(), "outside.tmp")
			if err := os.WriteFile(external, []byte("outside"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(external, filepath.Join(dir, directory, "publication.tmp")); err != nil {
				t.Skipf("symbolic links unavailable: %v", err)
			}
			if _, err := Open(Config{Dir: dir}); err == nil {
				t.Fatal("Open() accepted symlinked interrupted publication")
			}
			contents, err := os.ReadFile(external)
			if err != nil || string(contents) != "outside" {
				t.Fatalf("external target changed: contents=%q error=%v", contents, err)
			}
		})
	}
}

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
