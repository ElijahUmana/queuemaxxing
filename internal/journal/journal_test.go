package journal

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
)

func TestAppendBatchPersistsBeforeVisibilityAndRecovers(t *testing.T) {
	dir := t.TempDir()
	journal := openTestJournal(t, Config{Dir: dir})
	firstID := TransactionID{1}
	secondID := TransactionID{2}
	lsns, err := journal.AppendBatch(context.Background(), []Record{
		{LSN: 99, TransactionID: firstID, Payload: []byte("first")},
		{TransactionID: secondID, Payload: []byte("second")},
	})
	if err != nil {
		t.Fatalf("AppendBatch() error = %v", err)
	}
	if !equalLSNs(lsns, []uint64{1, 2}) {
		t.Fatalf("AppendBatch() LSNs = %v, want [1 2]", lsns)
	}
	assertRecords(t, journal.Records(), []Record{
		{LSN: 1, TransactionID: firstID, Payload: []byte("first")},
		{LSN: 2, TransactionID: secondID, Payload: []byte("second")},
	})
	if stats := journal.Stats(); stats.DurableLSN != 2 || stats.LastSyncAt == nil || stats.ReadOnly {
		t.Fatalf("Stats() = %+v", stats)
	}
	if err := journal.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reopened := openTestJournal(t, Config{Dir: dir})
	assertRecords(t, reopened.Records(), []Record{
		{LSN: 1, TransactionID: firstID, Payload: []byte("first")},
		{LSN: 2, TransactionID: secondID, Payload: []byte("second")},
	})
	lsn, err := reopened.Append(context.Background(), TransactionID{3}, []byte("third"))
	if err != nil || lsn != 3 {
		t.Fatalf("Append() = (%d, %v), want (3, nil)", lsn, err)
	}
}

func TestOpenExclusivelyLocksDirectory(t *testing.T) {
	dir := t.TempDir()
	first := openTestJournal(t, Config{Dir: dir})
	if _, err := Open(Config{Dir: dir}); !errors.Is(err, ErrLocked) {
		t.Fatalf("second Open() error = %v, want ErrLocked", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	second := openTestJournal(t, Config{Dir: dir})
	if err := second.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
}

func TestSyncFailureDoesNotPublishAndMakesJournalReadOnly(t *testing.T) {
	dir := t.TempDir()
	var mu sync.Mutex
	walSyncs := 0
	injected := errors.New("injected sync failure")
	journal := openTestJournal(t, Config{
		Dir: dir,
		Faults: FaultHooks{BeforeSync: func(path string) error {
			if strings.HasSuffix(path, ".wal") {
				mu.Lock()
				defer mu.Unlock()
				walSyncs++
				if walSyncs == 1 {
					return injected
				}
			}
			return nil
		}},
	})
	if _, err := journal.Append(context.Background(), TransactionID{1}, []byte("not-visible")); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("Append() error = %v, want ErrReadOnly", err)
	}
	if records := journal.Records(); len(records) != 0 {
		t.Fatalf("Records() = %v, want empty", records)
	}
	stats := journal.Stats()
	if !stats.ReadOnly || !strings.Contains(stats.ReadOnlyReason, injected.Error()) || stats.DurableLSN != 0 {
		t.Fatalf("Stats() = %+v", stats)
	}
	if _, err := journal.Append(context.Background(), TransactionID{2}, []byte("blocked")); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("second Append() error = %v, want ErrReadOnly", err)
	}
}

func TestRecoveryRejectsMissingWALFloorOrHead(t *testing.T) {
	for _, removeIndex := range []int{0, -1} {
		t.Run(map[bool]string{true: "floor", false: "head"}[removeIndex == 0], func(t *testing.T) {
			dir := t.TempDir()
			journal := openTestJournal(t, Config{Dir: dir, SegmentSize: 190})
			for index := 0; index < 4; index++ {
				if _, err := journal.Append(context.Background(), TransactionID{byte(index + 1)}, bytes.Repeat([]byte{byte(index + 1)}, 32)); err != nil {
					t.Fatal(err)
				}
			}
			if err := journal.Close(); err != nil {
				t.Fatal(err)
			}
			paths, err := filepath.Glob(filepath.Join(dir, "wal", "*.wal"))
			if err != nil || len(paths) < 2 {
				t.Fatalf("segments = %v, error = %v", paths, err)
			}
			sort.Strings(paths)
			index := removeIndex
			if index < 0 {
				index = len(paths) - 1
			}
			if err := os.Remove(paths[index]); err != nil {
				t.Fatal(err)
			}
			if _, err := Open(Config{Dir: dir, SegmentSize: 190}); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("Open() error = %v, want ErrCorrupt", err)
			}
		})
	}
}

func TestRecoveryQuarantinesSegmentBeyondDurableHead(t *testing.T) {
	dir := t.TempDir()
	journal := openTestJournal(t, Config{Dir: dir})
	if _, err := journal.Append(context.Background(), TransactionID{1}, []byte("committed")); err != nil {
		t.Fatal(err)
	}
	journal.mu.Lock()
	contents, err := os.ReadFile(journal.activePath)
	if err != nil {
		journal.mu.Unlock()
		t.Fatal(err)
	}
	digest := sha256.Sum256(contents)
	if err := journal.createSegment(journal.activeID+1, journal.nextLSN, digest); err != nil {
		journal.mu.Unlock()
		t.Fatal(err)
	}
	journal.mu.Unlock()
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := openTestJournal(t, Config{Dir: dir})
	defer reopened.Close()
	assertRecords(t, reopened.Records(), []Record{{LSN: 1, TransactionID: TransactionID{1}, Payload: []byte("committed")}})
	quarantined, err := filepath.Glob(filepath.Join(dir, "quarantine", "*.uncommitted"))
	if err != nil || len(quarantined) != 1 {
		t.Fatalf("uncommitted quarantine = %v, error = %v", quarantined, err)
	}
}

func TestRecoveryDiscardsFullyWrittenSuffixBeyondDurableHead(t *testing.T) {
	dir := t.TempDir()
	journal := openTestJournal(t, Config{Dir: dir})
	if _, err := journal.Append(context.Background(), TransactionID{1}, []byte("committed")); err != nil {
		t.Fatal(err)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	path := onlySegment(t, dir)
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	uncommitted := encodeRecord(Record{LSN: 2, TransactionID: TransactionID{2}, Payload: []byte("uncommitted")})
	if _, err := file.Write(uncommitted); err != nil {
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := openTestJournal(t, Config{Dir: dir})
	assertRecords(t, reopened.Records(), []Record{{LSN: 1, TransactionID: TransactionID{1}, Payload: []byte("committed")}})
	lsn, err := reopened.Append(context.Background(), TransactionID{3}, []byte("next"))
	if err != nil || lsn != 2 {
		t.Fatalf("Append() = (%d, %v), want (2, nil)", lsn, err)
	}
}

func TestRecoveryRepairsOnlyNewestTornTail(t *testing.T) {
	dir := t.TempDir()
	journal := openTestJournal(t, Config{Dir: dir})
	if _, err := journal.Append(context.Background(), TransactionID{1}, []byte("durable")); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if err := journal.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	path := onlySegment(t, dir)
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	partial := encodeRecord(Record{LSN: 2, TransactionID: TransactionID{2}, Payload: []byte("torn")})
	if _, err := file.Write(partial[:len(partial)/2]); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := openTestJournal(t, Config{Dir: dir})
	assertRecords(t, reopened.Records(), []Record{{LSN: 1, TransactionID: TransactionID{1}, Payload: []byte("durable")}})
	quarantined, err := filepath.Glob(filepath.Join(dir, "quarantine", "*.bad"))
	if err != nil || len(quarantined) != 1 {
		t.Fatalf("quarantine files = %v, error = %v", quarantined, err)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := decodeRecord(path, contents[segmentHeaderSize:], segmentHeaderSize); err != nil {
		t.Fatalf("retained record is invalid: %v", err)
	}
	if len(contents) != segmentHeaderSize+len(encodeRecord(Record{LSN: 1, TransactionID: TransactionID{1}, Payload: []byte("durable")})) {
		t.Fatalf("repaired segment size = %d", len(contents))
	}
}

func TestRecoveryRejectsChecksumFailureInCompleteFinalRecord(t *testing.T) {
	dir := t.TempDir()
	journal := openTestJournal(t, Config{Dir: dir})
	if _, err := journal.Append(context.Background(), TransactionID{1}, []byte("complete")); err != nil {
		t.Fatal(err)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	path := onlySegment(t, dir)
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	contents[segmentHeaderSize+recordHeaderSize] ^= 0xff
	if err := os.WriteFile(path, contents, 0o640); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(Config{Dir: dir}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open() error = %v, want ErrCorrupt", err)
	}
}

func TestRecoveryRejectsCorruptionBeforeValidRecord(t *testing.T) {
	dir := t.TempDir()
	journal := openTestJournal(t, Config{Dir: dir})
	for index, payload := range []string{"first", "second", "third"} {
		if _, err := journal.Append(context.Background(), TransactionID{byte(index + 1)}, []byte(payload)); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	path := onlySegment(t, dir)
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	contents[segmentHeaderSize+recordHeaderSize] ^= 0xff
	if err := os.WriteFile(path, contents, 0o640); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(Config{Dir: dir}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open() error = %v, want ErrCorrupt", err)
	}
}

func TestOpenRecoversInterruptedInitialHeadPublication(t *testing.T) {
	dir := t.TempDir()
	failed := false
	_, err := Open(Config{
		Dir: dir,
		Faults: FaultHooks{BeforeRename: func(_, newPath string) error {
			if filepath.Base(newPath) == "HEAD" {
				failed = true
				return errors.New("initial head failure")
			}
			return nil
		}},
	})
	if err == nil || !failed {
		t.Fatalf("Open() error = %v, failed = %v", err, failed)
	}
	recovered := openTestJournal(t, Config{Dir: dir})
	defer recovered.Close()
	if lsn, err := recovered.Append(context.Background(), TransactionID{1}, []byte("first")); err != nil || lsn != 1 {
		t.Fatalf("Append() = (%d, %v), want (1, nil)", lsn, err)
	}
}

func TestCheckpointHeadFailureDoesNotPublishSnapshotInMemory(t *testing.T) {
	dir := t.TempDir()
	failHead := false
	injected := errors.New("head publish failure")
	journal := openTestJournal(t, Config{
		Dir: dir,
		Faults: FaultHooks{BeforeRename: func(_, newPath string) error {
			if failHead && filepath.Base(newPath) == "HEAD" {
				return injected
			}
			return nil
		}},
	})
	if _, err := journal.Append(context.Background(), TransactionID{1}, []byte("one")); err != nil {
		t.Fatal(err)
	}
	failHead = true
	if err := journal.Checkpoint(context.Background(), 1, []byte("not-committed")); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("Checkpoint() error = %v, want ErrReadOnly", err)
	}
	if snapshot := journal.Snapshot(); snapshot.Generation != 0 || snapshot.ThroughLSN != 0 || len(snapshot.Payload) != 0 {
		t.Fatalf("Snapshot() = %+v, want zero snapshot", snapshot)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	recovered := openTestJournal(t, Config{Dir: dir})
	if err := recovered.Checkpoint(context.Background(), 1, []byte("committed-on-retry")); err != nil {
		t.Fatalf("checkpoint retry error = %v", err)
	}
	if snapshot := recovered.Snapshot(); snapshot.Generation != 1 || string(snapshot.Payload) != "committed-on-retry" {
		t.Fatalf("retried Snapshot() = %+v", snapshot)
	}
}

func TestCheckpointRecoversLatestAndFallsBackFromCorruptSnapshot(t *testing.T) {
	dir := t.TempDir()
	journal := openTestJournal(t, Config{Dir: dir, SegmentSize: 256})
	if _, err := journal.Append(context.Background(), TransactionID{1}, []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := journal.Checkpoint(context.Background(), 1, []byte("state-one")); err != nil {
		t.Fatalf("first Checkpoint() error = %v", err)
	}
	if _, err := journal.Append(context.Background(), TransactionID{2}, []byte("two")); err != nil {
		t.Fatal(err)
	}
	if err := journal.Checkpoint(context.Background(), 2, []byte("state-two")); err != nil {
		t.Fatalf("second Checkpoint() error = %v", err)
	}
	if _, err := journal.Append(context.Background(), TransactionID{3}, []byte("three")); err != nil {
		t.Fatal(err)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := openTestJournal(t, Config{Dir: dir, SegmentSize: 256})
	if snapshot := reopened.Snapshot(); snapshot.Generation != 2 || snapshot.ThroughLSN != 2 || string(snapshot.Payload) != "state-two" {
		t.Fatalf("Snapshot() = %+v", snapshot)
	}
	assertRecords(t, reopened.Records(), []Record{{LSN: 3, TransactionID: TransactionID{3}, Payload: []byte("three")}})
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}

	snapshots, err := filepath.Glob(filepath.Join(dir, "snapshots", "*.snap"))
	if err != nil || len(snapshots) != 2 {
		t.Fatalf("snapshots = %v, error = %v", snapshots, err)
	}
	sort.Strings(snapshots)
	latest := snapshots[len(snapshots)-1]
	contents, err := os.ReadFile(latest)
	if err != nil {
		t.Fatal(err)
	}
	contents[snapshotHeaderSize] ^= 0xff
	if err := os.WriteFile(latest, contents, 0o640); err != nil {
		t.Fatal(err)
	}

	fallback := openTestJournal(t, Config{Dir: dir, SegmentSize: 256})
	if snapshot := fallback.Snapshot(); snapshot.Generation != 1 || snapshot.ThroughLSN != 1 || string(snapshot.Payload) != "state-one" {
		t.Fatalf("fallback Snapshot() = %+v", snapshot)
	}
	assertRecords(t, fallback.Records(), []Record{
		{LSN: 2, TransactionID: TransactionID{2}, Payload: []byte("two")},
		{LSN: 3, TransactionID: TransactionID{3}, Payload: []byte("three")},
	})
}

func TestConcurrentAppendProducesContiguousDurableLSNs(t *testing.T) {
	journal := openTestJournal(t, Config{Dir: t.TempDir(), SegmentSize: 512})
	const count = 64
	lsns := make(chan uint64, count)
	errorsFound := make(chan error, count)
	var wait sync.WaitGroup
	for index := 0; index < count; index++ {
		wait.Add(1)
		go func(value byte) {
			defer wait.Done()
			lsn, err := journal.Append(context.Background(), TransactionID{value}, bytes.Repeat([]byte{value}, 16))
			if err != nil {
				errorsFound <- err
				return
			}
			lsns <- lsn
		}(byte(index + 1))
	}
	wait.Wait()
	close(lsns)
	close(errorsFound)
	for err := range errorsFound {
		t.Fatalf("concurrent Append() error = %v", err)
	}
	got := make([]int, 0, count)
	for lsn := range lsns {
		got = append(got, int(lsn))
	}
	sort.Ints(got)
	for index, lsn := range got {
		if lsn != index+1 {
			t.Fatalf("sorted LSNs[%d] = %d", index, lsn)
		}
	}
	if records := journal.Records(); len(records) != count {
		t.Fatalf("len(Records()) = %d", len(records))
	}
}

func TestContextCancellationBeforeMutation(t *testing.T) {
	journal := openTestJournal(t, Config{Dir: t.TempDir()})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := journal.Append(ctx, TransactionID{1}, []byte("cancelled")); !errors.Is(err, context.Canceled) {
		t.Fatalf("Append() error = %v, want context.Canceled", err)
	}
	if err := journal.Checkpoint(ctx, 0, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("Checkpoint() error = %v, want context.Canceled", err)
	}
	if len(journal.Records()) != 0 {
		t.Fatal("cancelled append mutated records")
	}
}

func openTestJournal(t *testing.T, config Config) *FileJournal {
	t.Helper()
	journal, err := Open(config)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = journal.Close() })
	return journal
}

func onlySegment(t *testing.T, dir string) string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(dir, "wal", "*.wal"))
	if err != nil || len(paths) != 1 {
		t.Fatalf("WAL segments = %v, error = %v", paths, err)
	}
	return paths[0]
}

func assertRecords(t *testing.T, got, want []Record) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("len(records) = %d, want %d: %#v", len(got), len(want), got)
	}
	for index := range want {
		if got[index].LSN != want[index].LSN || got[index].TransactionID != want[index].TransactionID || !bytes.Equal(got[index].Payload, want[index].Payload) {
			t.Fatalf("records[%d] = %#v, want %#v", index, got[index], want[index])
		}
	}
}

func equalLSNs(left, right []uint64) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
