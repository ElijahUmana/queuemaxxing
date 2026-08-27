package journal

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
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
	if err := journal.active.Close(); err != nil {
		journal.mu.Unlock()
		t.Fatal(err)
	}
	journal.active = nil
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
	if lsn, err := fallback.Append(context.Background(), TransactionID{4}, []byte("four")); err != nil || lsn != 4 {
		t.Fatalf("fallback Append() = (%d, %v), want (4, nil)", lsn, err)
	}
	wantReplacement := filepath.Join(dir, "snapshots", snapshotName(2, 2))
	if latest != wantReplacement {
		t.Fatalf("corrupt snapshot path = %s, replacement path = %s", latest, wantReplacement)
	}
	if _, err := os.Stat(wantReplacement); !os.IsNotExist(err) {
		t.Fatalf("corrupt snapshot remains published: %v", err)
	}
	quarantined := filepath.Join(dir, "quarantine", filepath.Base(latest)+".corrupt")
	quarantinedContents, err := os.ReadFile(quarantined)
	if err != nil {
		t.Fatalf("read quarantined corrupt snapshot: %v", err)
	}
	if !bytes.Equal(quarantinedContents, contents) {
		t.Fatal("quarantined corrupt snapshot bytes changed")
	}
	if err := fallback.Checkpoint(context.Background(), 2, []byte("state-two-rebuilt")); err != nil {
		t.Fatalf("fallback Checkpoint() error = %v", err)
	}
	if err := fallback.Close(); err != nil {
		t.Fatal(err)
	}

	recovered := openTestJournal(t, Config{Dir: dir, SegmentSize: 256})
	if snapshot := recovered.Snapshot(); snapshot.Generation != 2 || snapshot.ThroughLSN != 2 || string(snapshot.Payload) != "state-two-rebuilt" {
		t.Fatalf("recovered Snapshot() = %+v", snapshot)
	}
	assertRecords(t, recovered.Records(), []Record{
		{LSN: 3, TransactionID: TransactionID{3}, Payload: []byte("three")},
		{LSN: 4, TransactionID: TransactionID{4}, Payload: []byte("four")},
	})
}

func TestOversizedBatchUsesSegmentSizeAsRotationTarget(t *testing.T) {
	dir := t.TempDir()
	const segmentTarget = int64(190)
	store := openTestJournal(t, Config{Dir: dir, SegmentSize: segmentTarget})
	batch := []Record{
		{TransactionID: TransactionID{1}, Payload: bytes.Repeat([]byte{1}, 64)},
		{TransactionID: TransactionID{2}, Payload: bytes.Repeat([]byte{2}, 64)},
		{TransactionID: TransactionID{3}, Payload: bytes.Repeat([]byte{3}, 64)},
	}
	lsns, err := store.AppendBatch(context.Background(), batch)
	if err != nil || !equalLSNs(lsns, []uint64{1, 2, 3}) {
		t.Fatalf("AppendBatch() = (%v, %v)", lsns, err)
	}
	if store.activeSize <= segmentTarget {
		t.Fatalf("oversized batch segment size = %d, want > %d", store.activeSize, segmentTarget)
	}
	if lsn, err := store.Append(context.Background(), TransactionID{4}, []byte("next")); err != nil || lsn != 4 {
		t.Fatalf("Append() = (%d, %v), want (4, nil)", lsn, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	paths, err := filepath.Glob(filepath.Join(dir, "wal", "*.wal"))
	if err != nil || len(paths) != 2 {
		t.Fatalf("segments = %v, error = %v", paths, err)
	}
	sort.Strings(paths)
	for _, path := range paths {
		contents, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if len(contents) <= segmentHeaderSize {
			t.Fatalf("empty WAL segment %s has size %d", path, len(contents))
		}
	}

	reopened := openTestJournal(t, Config{Dir: dir, SegmentSize: segmentTarget})
	want := []Record{
		{LSN: 1, TransactionID: TransactionID{1}, Payload: batch[0].Payload},
		{LSN: 2, TransactionID: TransactionID{2}, Payload: batch[1].Payload},
		{LSN: 3, TransactionID: TransactionID{3}, Payload: batch[2].Payload},
		{LSN: 4, TransactionID: TransactionID{4}, Payload: []byte("next")},
	}
	assertRecords(t, reopened.Records(), want)
}

func TestFutureCorruptSnapshotDoesNotBlockGenerationProgress(t *testing.T) {
	dir := t.TempDir()
	store := openTestJournal(t, Config{Dir: dir, SegmentSize: 256})
	for generation := uint64(1); generation <= 2; generation++ {
		if _, err := store.Append(context.Background(), TransactionID{byte(generation)}, []byte{byte(generation)}); err != nil {
			t.Fatal(err)
		}
		if err := store.Checkpoint(context.Background(), generation, []byte{byte(generation)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.Append(context.Background(), TransactionID{3}, []byte{3}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	corruptPath := filepath.Join(dir, "snapshots", snapshotName(5, 3))
	corruptBytes := []byte("future corrupt snapshot evidence")
	if err := os.WriteFile(corruptPath, corruptBytes, 0o600); err != nil {
		t.Fatal(err)
	}

	reopened := openTestJournal(t, Config{Dir: dir, SegmentSize: 256})
	if snapshot := reopened.Snapshot(); snapshot.Generation != 2 || snapshot.ThroughLSN != 2 {
		t.Fatalf("fallback Snapshot() = %+v", snapshot)
	}
	quarantined := filepath.Join(dir, "quarantine", filepath.Base(corruptPath)+".corrupt")
	contents, err := os.ReadFile(quarantined)
	if err != nil || !bytes.Equal(contents, corruptBytes) {
		t.Fatalf("quarantined evidence = %q, error = %v", contents, err)
	}
	if _, err := os.Stat(corruptPath); !os.IsNotExist(err) {
		t.Fatalf("future corrupt snapshot remains published: %v", err)
	}

	if err := reopened.Checkpoint(context.Background(), 3, []byte("generation-three")); err != nil {
		t.Fatalf("generation 3 checkpoint: %v", err)
	}
	for generation := uint64(4); generation <= 5; generation++ {
		if _, err := reopened.Append(context.Background(), TransactionID{byte(generation)}, []byte{byte(generation)}); err != nil {
			t.Fatal(err)
		}
		if err := reopened.Checkpoint(context.Background(), generation, []byte{byte(generation)}); err != nil {
			t.Fatalf("generation %d checkpoint: %v", generation, err)
		}
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}

	latest := openTestJournal(t, Config{Dir: dir, SegmentSize: 256})
	if snapshot := latest.Snapshot(); snapshot.Generation != 5 || snapshot.ThroughLSN != 5 || !bytes.Equal(snapshot.Payload, []byte{5}) {
		t.Fatalf("latest Snapshot() = %+v", snapshot)
	}
	contents, err = os.ReadFile(quarantined)
	if err != nil || !bytes.Equal(contents, corruptBytes) {
		t.Fatalf("preserved evidence = %q, error = %v", contents, err)
	}
}

func TestGroupCommitSharesDurabilityBoundary(t *testing.T) {
	var mu sync.Mutex
	walSyncs := 0
	headPublishes := 0
	armed := false
	journal := openTestJournal(t, Config{Dir: t.TempDir(), Faults: FaultHooks{
		BeforeSync: func(path string) error {
			if armed && strings.HasSuffix(path, ".wal") {
				mu.Lock()
				walSyncs++
				mu.Unlock()
			}
			return nil
		},
		BeforeRename: func(_, destination string) error {
			if armed && filepath.Base(destination) == "HEAD" {
				mu.Lock()
				headPublishes++
				mu.Unlock()
			}
			return nil
		},
	}})
	armed = true

	const count = 64
	start := make(chan struct{})
	results := make(chan uint64, count)
	errorsFound := make(chan error, count)
	var wait sync.WaitGroup
	for index := 0; index < count; index++ {
		wait.Add(1)
		go func(value byte) {
			defer wait.Done()
			<-start
			lsn, err := journal.Append(context.Background(), TransactionID{value}, []byte{value})
			if err != nil {
				errorsFound <- err
				return
			}
			results <- lsn
		}(byte(index + 1))
	}
	close(start)
	wait.Wait()
	close(results)
	close(errorsFound)
	for err := range errorsFound {
		t.Fatal(err)
	}
	if len(results) != count {
		t.Fatalf("successful appends = %d, want %d", len(results), count)
	}
	mu.Lock()
	defer mu.Unlock()
	if walSyncs >= count || headPublishes >= count {
		t.Fatalf("group commit did not coalesce: WAL syncs=%d HEAD publishes=%d appends=%d", walSyncs, headPublishes, count)
	}
	if walSyncs != headPublishes {
		t.Fatalf("durability boundaries differ: WAL syncs=%d HEAD publishes=%d", walSyncs, headPublishes)
	}
}

func TestGroupCommitSyncFailureRejectsWholeBatch(t *testing.T) {
	armed := false
	journal := openTestJournal(t, Config{Dir: t.TempDir(), Faults: FaultHooks{BeforeSync: func(path string) error {
		if armed && strings.HasSuffix(path, ".wal") {
			return errInjected
		}
		return nil
	}}})
	armed = true

	const count = 32
	start := make(chan struct{})
	errorsFound := make(chan error, count)
	var wait sync.WaitGroup
	for index := 0; index < count; index++ {
		wait.Add(1)
		go func(value byte) {
			defer wait.Done()
			<-start
			_, err := journal.Append(context.Background(), TransactionID{value}, []byte{value})
			errorsFound <- err
		}(byte(index + 1))
	}
	close(start)
	wait.Wait()
	close(errorsFound)
	for err := range errorsFound {
		if !errors.Is(err, ErrReadOnly) {
			t.Fatalf("Append() error = %v, want ErrReadOnly", err)
		}
	}
	if records := journal.Records(); len(records) != 0 {
		t.Fatalf("failed batch published %d records", len(records))
	}
	if stats := journal.Stats(); stats.DurableLSN != 0 || !stats.ReadOnly {
		t.Fatalf("Stats() = %+v", stats)
	}
}

func BenchmarkDurableAppend(b *testing.B) {
	b.Run("serial", func(b *testing.B) {
		store, err := Open(Config{Dir: b.TempDir()})
		if err != nil {
			b.Fatal(err)
		}
		defer store.Close()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			if _, err := store.Append(context.Background(), TransactionID{1}, []byte("payload")); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("concurrent", func(b *testing.B) {
		store, err := Open(Config{Dir: b.TempDir()})
		if err != nil {
			b.Fatal(err)
		}
		defer store.Close()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, err := store.Append(context.Background(), TransactionID{1}, []byte("payload")); err != nil {
					b.Fatal(err)
				}
			}
		})
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

func TestConcurrentCloseReturnsSameErrorAndReleasesStorage(t *testing.T) {
	dir := t.TempDir()
	closeFailure := errors.New("injected close failure")
	var mu sync.Mutex
	closeHooks := 0
	store := openTestJournal(t, Config{Dir: dir, Faults: FaultHooks{BeforeClose: func(resource string) error {
		mu.Lock()
		defer mu.Unlock()
		closeHooks++
		if resource == "active WAL" {
			return closeFailure
		}
		return nil
	}}})
	if _, err := store.Append(context.Background(), TransactionID{1}, []byte("durable")); err != nil {
		t.Fatal(err)
	}

	const callers = 32
	start := make(chan struct{})
	results := make(chan error, callers)
	var wait sync.WaitGroup
	for index := 0; index < callers; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			results <- store.Close()
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	for err := range results {
		if !errors.Is(err, closeFailure) {
			t.Fatalf("Close() error = %v, want injected failure", err)
		}
	}
	if err := store.Close(); !errors.Is(err, closeFailure) {
		t.Fatalf("repeated Close() error = %v, want injected failure", err)
	}
	mu.Lock()
	if closeHooks != 3 {
		t.Fatalf("close hooks = %d, want 3 resources closed once", closeHooks)
	}
	mu.Unlock()

	reopened := openTestJournal(t, Config{Dir: dir})
	assertRecords(t, reopened.Records(), []Record{{LSN: 1, TransactionID: TransactionID{1}, Payload: []byte("durable")}})
}

func TestCloseDrainsAcceptedAppendsAndRejectsNewOnes(t *testing.T) {
	dir := t.TempDir()
	enteredSync := make(chan struct{})
	releaseSync := make(chan struct{})
	armed := false
	var blockOnce sync.Once
	journal := openTestJournal(t, Config{Dir: dir, Faults: FaultHooks{BeforeSync: func(path string) error {
		if armed && strings.HasSuffix(path, ".wal") {
			blockOnce.Do(func() {
				close(enteredSync)
				<-releaseSync
			})
		}
		return nil
	}}})
	armed = true

	const count = commitQueueCapacity + 1
	errorsFound := make(chan error, count)
	go func() {
		_, err := journal.Append(context.Background(), TransactionID{1}, []byte{1})
		errorsFound <- err
	}()
	<-enteredSync
	for index := 1; index < count; index++ {
		go func(value int) {
			_, err := journal.Append(context.Background(), TransactionID{byte(value)}, []byte{byte(value)})
			errorsFound <- err
		}(index)
	}
	deadline := time.After(5 * time.Second)
	for len(journal.commitCh) != commitQueueCapacity {
		select {
		case <-deadline:
			t.Fatalf("accepted queue length = %d, want %d", len(journal.commitCh), commitQueueCapacity)
		default:
			runtime.Gosched()
		}
	}

	closed := make(chan error, 1)
	go func() { closed <- journal.Close() }()
	close(releaseSync)
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	for index := 0; index < count; index++ {
		if err := <-errorsFound; err != nil {
			t.Fatalf("accepted Append() error = %v", err)
		}
	}
	if _, err := journal.Append(context.Background(), TransactionID{}, nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("Append() after Close error = %v, want ErrClosed", err)
	}

	reopened := openTestJournal(t, Config{Dir: dir})
	if records := reopened.Records(); len(records) != count {
		t.Fatalf("recovered records = %d, want %d", len(records), count)
	}
	if stats := reopened.Stats(); stats.DurableLSN != count {
		t.Fatalf("recovered durable LSN = %d, want %d", stats.DurableLSN, count)
	}
}

func TestAppendBatchResourceBounds(t *testing.T) {
	t.Run("exact-engine-group", func(t *testing.T) {
		store := openTestJournal(t, Config{Dir: t.TempDir()})
		const records = 64
		const payloadBytes = (8 << 20) / records
		input := make([]Record, records)
		for index := range input {
			input[index] = Record{TransactionID: TransactionID{byte(index)}, Payload: bytes.Repeat([]byte{byte(index)}, payloadBytes)}
		}
		lsns, err := store.AppendBatch(context.Background(), input)
		if err != nil || len(lsns) != records || lsns[0] != 1 || lsns[records-1] != records {
			t.Fatalf("AppendBatch() = (%d LSNs, %v)", len(lsns), err)
		}
		if stats := store.Stats(); stats.DurableLSN != records {
			t.Fatalf("DurableLSN = %d, want %d", stats.DurableLSN, records)
		}
	})

	t.Run("exact-record-count", func(t *testing.T) {
		store := openTestJournal(t, Config{Dir: t.TempDir()})
		input := make([]Record, maxAppendRecords)
		lsns, err := store.AppendBatch(context.Background(), input)
		if err != nil || len(lsns) != maxAppendRecords || lsns[0] != 1 || lsns[len(lsns)-1] != maxAppendRecords {
			t.Fatalf("AppendBatch() = (%d LSNs, %v)", len(lsns), err)
		}
	})

	t.Run("record-count-plus-one", func(t *testing.T) {
		store := openTestJournal(t, Config{Dir: t.TempDir()})
		if _, err := store.Append(context.Background(), TransactionID{1}, []byte("before")); err != nil {
			t.Fatal(err)
		}
		if _, err := store.AppendBatch(context.Background(), make([]Record, maxAppendRecords+1)); !errors.Is(err, ErrBatchLarge) {
			t.Fatalf("AppendBatch() error = %v, want ErrBatchLarge", err)
		}
		if stats := store.Stats(); stats.DurableLSN != 1 {
			t.Fatalf("DurableLSN = %d, want 1", stats.DurableLSN)
		}
		if lsn, err := store.Append(context.Background(), TransactionID{2}, []byte("after")); err != nil || lsn != 2 {
			t.Fatalf("Append() = (%d, %v), want (2, nil)", lsn, err)
		}
	})

	t.Run("exact-encoded-bytes", func(t *testing.T) {
		payloadBytes := []int{int(maxPayloadSize) - recordHeaderSize - recordTrailerSize, 0}
		total, err := validateAppendBatchSize(len(payloadBytes), func(index int) int { return payloadBytes[index] })
		if err != nil || total != maxAppendBytes {
			t.Fatalf("validateAppendBatchSize() = (%d, %v), want (%d, nil)", total, err, maxAppendBytes)
		}
	})

	t.Run("encoded-bytes-plus-one", func(t *testing.T) {
		payloadBytes := []int{int(maxPayloadSize) - recordHeaderSize - recordTrailerSize + 1, 0}
		if _, err := validateAppendBatchSize(len(payloadBytes), func(index int) int { return payloadBytes[index] }); !errors.Is(err, ErrBatchLarge) {
			t.Fatalf("validateAppendBatchSize() error = %v, want ErrBatchLarge", err)
		}
		store := openTestJournal(t, Config{Dir: t.TempDir()})
		input := []Record{{Payload: bytes.Repeat([]byte{1}, maxAppendBytes/2)}, {Payload: bytes.Repeat([]byte{2}, maxAppendBytes/2+1)}}
		if _, err := store.AppendBatch(context.Background(), input); !errors.Is(err, ErrBatchLarge) {
			t.Fatalf("AppendBatch() error = %v, want ErrBatchLarge", err)
		}
		if stats := store.Stats(); stats.DurableLSN != 0 {
			t.Fatalf("DurableLSN = %d, want 0", stats.DurableLSN)
		}
		if records := store.Records(); len(records) != 0 {
			t.Fatalf("Records() = %d, want 0", len(records))
		}
	})

	t.Run("canceled-precedes-bound", func(t *testing.T) {
		store := openTestJournal(t, Config{Dir: t.TempDir()})
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := store.AppendBatch(ctx, make([]Record, maxAppendRecords+1)); !errors.Is(err, context.Canceled) {
			t.Fatalf("AppendBatch() error = %v, want context.Canceled", err)
		}
	})
}

func TestConcurrentOversizedBatchesAreRejectedWithoutMutation(t *testing.T) {
	store := openTestJournal(t, Config{Dir: t.TempDir()})
	input := make([]Record, maxAppendRecords+1)
	const callers = 64
	errorsFound := make(chan error, callers)
	var wait sync.WaitGroup
	for index := 0; index < callers; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, err := store.AppendBatch(context.Background(), input)
			errorsFound <- err
		}()
	}
	wait.Wait()
	close(errorsFound)
	for err := range errorsFound {
		if !errors.Is(err, ErrBatchLarge) {
			t.Fatalf("AppendBatch() error = %v, want ErrBatchLarge", err)
		}
	}
	if stats := store.Stats(); stats.DurableLSN != 0 {
		t.Fatalf("DurableLSN = %d, want 0", stats.DurableLSN)
	}
	if lsn, err := store.Append(context.Background(), TransactionID{1}, []byte("first")); err != nil || lsn != 1 {
		t.Fatalf("Append() = (%d, %v), want (1, nil)", lsn, err)
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
