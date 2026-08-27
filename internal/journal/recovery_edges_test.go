package journal

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func newRootedTestJournal(t *testing.T, dir string) *FileJournal {
	t.Helper()
	instance := &FileJournal{dir: dir, walDir: filepath.Join(dir, "wal"), snapshotDir: filepath.Join(dir, "snapshots")}
	if err := instance.openRoot(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.root.Close() })
	return instance
}

func TestRecoverSegmentsRejectsInvalidHistories(t *testing.T) {
	storeID := [16]byte{1}
	otherID := [16]byte{2}
	baseHead := headState{StoreID: storeID, WALFloor: 1, WALHead: 1, DurableLSN: 1}

	tests := []struct {
		name      string
		head      headState
		snapshots []snapshotCandidate
		segments  []recoverySegment
	}{
		{name: "empty-store-id", head: headState{}, segments: []recoverySegment{{header: segmentHeader{StoreID: storeID, ID: 1, FirstLSN: 1}}}},
		{name: "range-mismatch", head: headState{StoreID: storeID, WALFloor: 2, WALHead: 2}, segments: []recoverySegment{{header: segmentHeader{StoreID: storeID, ID: 1, FirstLSN: 1}}}},
		{name: "store-mismatch", head: baseHead, segments: []recoverySegment{{header: segmentHeader{StoreID: otherID, ID: 1, FirstLSN: 1}, records: []Record{{LSN: 1}}}}},
		{name: "missing-snapshot", head: headState{StoreID: storeID, WALFloor: 1, WALHead: 1, DurableLSN: 2, SnapshotGeneration: 1, SnapshotThroughLSN: 1}, segments: []recoverySegment{{header: segmentHeader{StoreID: storeID, ID: 1, FirstLSN: 1}, records: []Record{{LSN: 2}}}}},
		{name: "snapshot-identity", head: headState{StoreID: storeID, WALFloor: 1, WALHead: 1, DurableLSN: 2, SnapshotGeneration: 1, SnapshotThroughLSN: 1}, snapshots: []snapshotCandidate{{storeID: otherID, snapshot: Snapshot{Generation: 1, ThroughLSN: 1}}}, segments: []recoverySegment{{header: segmentHeader{StoreID: storeID, ID: 1, FirstLSN: 1}, records: []Record{{LSN: 2}}}}},
		{name: "first-lsn-gap", head: baseHead, segments: []recoverySegment{{header: segmentHeader{StoreID: storeID, ID: 1, FirstLSN: 2}, records: []Record{{LSN: 2}}}}},
		{name: "record-lsn-gap", head: headState{StoreID: storeID, WALFloor: 1, WALHead: 1, DurableLSN: 2}, segments: []recoverySegment{{header: segmentHeader{StoreID: storeID, ID: 1, FirstLSN: 1}, records: []Record{{LSN: 1}, {LSN: 3}}}}},
		{name: "durable-head-mismatch", head: headState{StoreID: storeID, WALFloor: 1, WALHead: 1, DurableLSN: 2}, segments: []recoverySegment{{header: segmentHeader{StoreID: storeID, ID: 1, FirstLSN: 1}, records: []Record{{LSN: 1}}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			paths := writeRecoverySegments(t, dir, test.segments)
			instance := newRootedTestJournal(t, dir)
			if err := os.MkdirAll(instance.snapshotDir, 0o700); err != nil {
				t.Fatal(err)
			}
			if _, _, _, err := instance.recoverSegments(paths, test.snapshots, test.head); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestRecoverSegmentsRejectsBrokenSecondSegment(t *testing.T) {
	storeID := [16]byte{1}
	firstRecord := encodeRecord(Record{LSN: 1})
	firstBytes := append(encodeSegmentHeader(segmentHeader{StoreID: storeID, ID: 1, FirstLSN: 1}), firstRecord...)
	firstDigest := sha256.Sum256(firstBytes)
	cases := []struct {
		name   string
		second segmentHeader
	}{
		{name: "store", second: segmentHeader{StoreID: [16]byte{2}, ID: 2, FirstLSN: 2, Previous: firstDigest}},
		{name: "id", second: segmentHeader{StoreID: storeID, ID: 3, FirstLSN: 2, Previous: firstDigest}},
		{name: "hash", second: segmentHeader{StoreID: storeID, ID: 2, FirstLSN: 2}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			paths := writeRecoverySegments(t, dir, []recoverySegment{
				{header: segmentHeader{StoreID: storeID, ID: 1, FirstLSN: 1}, records: []Record{{LSN: 1}}},
				{header: test.second, records: []Record{{LSN: 2}}},
			})
			instance := newRootedTestJournal(t, dir)
			if err := os.MkdirAll(instance.snapshotDir, 0o700); err != nil {
				t.Fatal(err)
			}
			head := headState{StoreID: storeID, WALFloor: 1, WALHead: 2, DurableLSN: 2}
			if _, _, _, err := instance.recoverSegments(paths, nil, head); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestRecoverSegmentsUsesExactAndFallbackSnapshots(t *testing.T) {
	storeID := [16]byte{1}
	for _, test := range []struct {
		name        string
		head        headState
		snapshots   []snapshotCandidate
		wantGen     uint64
		firstRecord uint64
	}{
		{name: "exact", head: headState{StoreID: storeID, WALFloor: 1, WALHead: 1, DurableLSN: 3, SnapshotGeneration: 2, SnapshotThroughLSN: 2}, snapshots: []snapshotCandidate{{storeID: storeID, snapshot: Snapshot{Generation: 2, ThroughLSN: 2}}}, wantGen: 2, firstRecord: 3},
		{name: "fallback", head: headState{StoreID: storeID, WALFloor: 1, WALHead: 1, DurableLSN: 3, SnapshotGeneration: 3, SnapshotThroughLSN: 2}, snapshots: []snapshotCandidate{{storeID: storeID, snapshot: Snapshot{Generation: 2, ThroughLSN: 1}}}, wantGen: 2, firstRecord: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			records := []Record{{LSN: test.firstRecord}}
			for lsn := test.firstRecord + 1; lsn <= test.head.DurableLSN; lsn++ {
				records = append(records, Record{LSN: lsn})
			}
			paths := writeRecoverySegments(t, dir, []recoverySegment{{header: segmentHeader{StoreID: storeID, ID: 1, FirstLSN: 1}, records: records}})
			instance := newRootedTestJournal(t, dir)
			if err := os.MkdirAll(instance.snapshotDir, 0o700); err != nil {
				t.Fatal(err)
			}
			_, recovered, _, err := instance.recoverSegments(paths, test.snapshots, test.head)
			if err != nil || instance.snapshot.Generation != test.wantGen || len(recovered) == 0 {
				t.Fatalf("snapshot=%+v records=%+v error=%v", instance.snapshot, recovered, err)
			}
		})
	}
}

func TestInterruptedSegmentReconciliationConflicts(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	if err := os.MkdirAll(walDir, 0o700); err != nil {
		t.Fatal(err)
	}
	paths := writeRecoverySegments(t, dir, []recoverySegment{{header: segmentHeader{ID: 1}}, {header: segmentHeader{ID: 2}}})
	instance := newRootedTestJournal(t, dir)
	kept, err := instance.reconcileInterruptedSegmentPublication(paths, 1)
	if err != nil || len(kept) != 1 {
		t.Fatalf("kept=%v error=%v", kept, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "quarantine", filepath.Base(paths[1])+".uncommitted")); err != nil {
		t.Fatal(err)
	}

	bad := filepath.Join(walDir, "not-a-number.wal")
	if err := os.WriteFile(bad, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := instance.reconcileInterruptedSegmentPublication([]string{bad}, 1); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("error=%v", err)
	}
}

func TestTailRepairEvidenceConflictAndTruncateFailure(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	if err := os.MkdirAll(walDir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(walDir, segmentName(1))
	if err := os.WriteFile(path, []byte("original"), 0o600); err != nil {
		t.Fatal(err)
	}
	instance := newRootedTestJournal(t, dir)
	instance.faults = FaultHooks{BeforeTruncate: func(string, int64) error { return errInjected }}
	if err := instance.repairTail(path, 0, []byte("suffix")); !errors.Is(err, errInjected) {
		t.Fatalf("truncate error = %v", err)
	}
	instance.faults = FaultHooks{}
	quarantinePath := filepath.Join(dir, "quarantine", filepath.Base(path)+"-0.bad")
	if err := os.WriteFile(quarantinePath, []byte("different"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := instance.repairTail(path, 0, []byte("suffix")); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("evidence conflict = %v", err)
	}
}

func TestRepairableTailAndEmbeddedRecordDetection(t *testing.T) {
	if !isRepairableTornTail([]byte{1, 2, 3}) {
		t.Fatal("short tail not repairable")
	}
	valid := encodeRecord(Record{LSN: 1, Payload: []byte("x")})
	if isRepairableTornTail(valid) {
		t.Fatal("complete record marked repairable")
	}
	corrupt := bytes.Clone(valid)
	corrupt[0] ^= 0xff
	if isRepairableTornTail(corrupt) {
		t.Fatal("bad magic marked repairable")
	}
	for _, modify := range []func([]byte){
		func(data []byte) { binary.LittleEndian.PutUint16(data[4:6], 99) },
		func(data []byte) { binary.LittleEndian.PutUint32(data[16:20], 0) },
		func(data []byte) { data[44] ^= 0xff },
	} {
		candidate := bytes.Clone(valid)
		modify(candidate)
		if isRepairableTornTail(candidate) {
			t.Fatal("invalid header marked repairable")
		}
	}
	if !containsValidRecord(append([]byte("noise"), valid...), 0) {
		t.Fatal("embedded record not found")
	}
	if !containsValidRecord(append([]byte{byte(recordMagic), 0, 0, 0}, valid...), 0) {
		t.Fatal("valid record after false magic not found")
	}
	if containsValidRecord([]byte("noise"), 0) {
		t.Fatal("false embedded record")
	}
}

func TestRepairTailSyncFailures(t *testing.T) {
	for _, failBase := range []string{"store", "quarantine"} {
		t.Run(failBase, func(t *testing.T) {
			dir := t.TempDir()
			walDir := filepath.Join(dir, "wal")
			if err := os.MkdirAll(walDir, 0o700); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(walDir, segmentName(1))
			if err := os.WriteFile(path, []byte("data"), 0o600); err != nil {
				t.Fatal(err)
			}
			instance := newRootedTestJournal(t, dir)
			instance.faults = FaultHooks{BeforeSync: func(path string) error {
				if (failBase == "store" && path == dir) || (failBase == "quarantine" && filepath.Base(path) == "quarantine") {
					return errInjected
				}
				return nil
			}}
			if err := instance.repairTail(path, 2, []byte("ta")); !errors.Is(err, errInjected) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestRepairTailEmptyAndMatchingEvidence(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	if err := os.MkdirAll(walDir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(walDir, segmentName(1))
	if err := os.WriteFile(path, []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	instance := newRootedTestJournal(t, dir)
	if err := instance.repairTail(path, 2, nil); err != nil {
		t.Fatal(err)
	}
	contents, _ := os.ReadFile(path)
	if string(contents) != "da" {
		t.Fatalf("contents = %q", contents)
	}
	if err := os.WriteFile(path, []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := instance.repairTail(path, 2, []byte("ta")); err != nil {
		t.Fatal(err)
	}
	if err := instance.repairTail(path, 2, []byte("ta")); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotPruningAndNoopCompaction(t *testing.T) {
	dir := t.TempDir()
	instance := newRootedTestJournal(t, dir)
	instance.head = headState{WALFloor: 1}
	if err := os.MkdirAll(instance.walDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(instance.snapshotDir, 0o700); err != nil {
		t.Fatal(err)
	}
	instance.storeID = [16]byte{1}
	for generation := uint64(1); generation <= 3; generation++ {
		snapshot := Snapshot{Generation: generation, ThroughLSN: generation, Payload: []byte{byte(generation)}}
		path := filepath.Join(instance.snapshotDir, snapshotName(generation, generation))
		if err := os.WriteFile(path, encodeSnapshot(instance.storeID, snapshot), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := instance.pruneSnapshotsLocked(); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(instance.snapshotDir)
	if err != nil || len(entries) != 2 {
		t.Fatalf("snapshots=%v err=%v", entries, err)
	}
	if err := instance.compactLocked(); err != nil {
		t.Fatal(err)
	}
}

type recoverySegment struct {
	header  segmentHeader
	records []Record
	suffix  []byte
}

func writeRecoverySegments(t *testing.T, dir string, segments []recoverySegment) []string {
	t.Helper()
	walDir := filepath.Join(dir, "wal")
	if err := os.MkdirAll(walDir, 0o700); err != nil {
		t.Fatal(err)
	}
	paths := make([]string, 0, len(segments))
	for _, segment := range segments {
		path := filepath.Join(walDir, segmentName(segment.header.ID))
		contents := encodeSegmentHeader(segment.header)
		for _, record := range segment.records {
			contents = append(contents, encodeRecord(record)...)
		}
		contents = append(contents, segment.suffix...)
		if err := os.WriteFile(path, contents, 0o600); err != nil {
			t.Fatal(err)
		}
		paths = append(paths, path)
	}
	return paths
}
