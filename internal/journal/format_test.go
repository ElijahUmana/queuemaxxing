package journal

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCorruptionErrorAndOpenValidation(t *testing.T) {
	cause := &CorruptionError{Path: "wal", Offset: 12, LSN: 7, Reason: "damaged"}
	if !errors.Is(cause, ErrCorrupt) || !strings.Contains(cause.Error(), "offset=12") {
		t.Fatalf("error = %v", cause)
	}
	if ErrClosed.Error() != "journal is closed" {
		t.Fatalf("error string = %q", ErrClosed.Error())
	}
	if _, err := Open(Config{}); err == nil {
		t.Fatal("empty directory accepted")
	}
	if _, err := Open(Config{Dir: t.TempDir(), SegmentSize: 1}); err == nil {
		t.Fatal("undersized segment accepted")
	}
}

func TestSegmentHeaderRejectsEveryFieldClass(t *testing.T) {
	valid := encodeSegmentHeader(segmentHeader{StoreID: [16]byte{1}, ID: 2, FirstLSN: 3})
	tests := map[string][]byte{
		"incomplete": valid[:10],
		"magic":      mutate(valid, func(data []byte) { data[0] ^= 0xff }),
		"version":    mutate(valid, func(data []byte) { binary.LittleEndian.PutUint16(data[8:10], 99); fixCRC(data, 76, 76) }),
		"checksum":   mutate(valid, func(data []byte) { data[76] ^= 0xff }),
	}
	for name, encoded := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeSegmentHeader(name, encoded); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("error = %v", err)
			}
		})
	}
	decoded, err := decodeSegmentHeader("valid", valid)
	if err != nil || decoded.ID != 2 || decoded.FirstLSN != 3 || decoded.StoreID[0] != 1 {
		t.Fatalf("decoded = %+v, %v", decoded, err)
	}
}

func TestRecordDecoderRejectsEveryFieldClass(t *testing.T) {
	record := Record{LSN: 7, TransactionID: TransactionID{1}, Payload: []byte("payload")}
	valid := encodeRecord(record)
	payloadLength := len(record.Payload)
	trailer := recordHeaderSize + payloadLength
	tests := map[string][]byte{
		"incomplete-header": valid[:10],
		"magic":             mutate(valid, func(data []byte) { data[0] ^= 0xff }),
		"format":            mutate(valid, func(data []byte) { binary.LittleEndian.PutUint16(data[4:6], 99); fixCRC(data, 44, 44) }),
		"length":            mutate(valid, func(data []byte) { binary.LittleEndian.PutUint32(data[16:20], 0); fixCRC(data, 44, 44) }),
		"header-checksum":   mutate(valid, func(data []byte) { data[44] ^= 0xff }),
		"incomplete-body":   valid[:len(valid)-1],
		"trailer-length":    mutate(valid, func(data []byte) { binary.LittleEndian.PutUint32(data[trailer+4:trailer+8], 1) }),
		"trailer-magic":     mutate(valid, func(data []byte) { data[trailer+8] ^= 0xff }),
		"payload-checksum":  mutate(valid, func(data []byte) { data[recordHeaderSize] ^= 0xff }),
	}
	for name, encoded := range tests {
		t.Run(name, func(t *testing.T) {
			if _, _, err := decodeRecord(name, encoded, 12); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("error = %v", err)
			}
		})
	}
	decoded, consumed, err := decodeRecord("valid", valid, 0)
	if err != nil || consumed != len(valid) || decoded.LSN != record.LSN || !bytes.Equal(decoded.Payload, record.Payload) {
		t.Fatalf("decoded = %+v/%d, %v", decoded, consumed, err)
	}
	assertPanics(t, func() { encodeRecord(Record{Payload: make([]byte, int(maxPayloadSize)+1)}) })
}

func TestSnapshotDecoderRejectsEveryFieldClass(t *testing.T) {
	snapshot := Snapshot{Generation: 2, ThroughLSN: 7, Payload: []byte("state")}
	storeID := [16]byte{1}
	valid := encodeSnapshot(storeID, snapshot)
	trailer := snapshotHeaderSize + len(snapshot.Payload)
	tests := map[string][]byte{
		"incomplete":      valid[:10],
		"magic":           mutate(valid, func(data []byte) { data[0] ^= 0xff }),
		"format":          mutate(valid, func(data []byte) { binary.LittleEndian.PutUint16(data[8:10], 99); fixCRC(data, 52, 52) }),
		"length":          mutate(valid, func(data []byte) { binary.LittleEndian.PutUint32(data[32:36], 0); fixCRC(data, 52, 52) }),
		"header-checksum": mutate(valid, func(data []byte) { data[52] ^= 0xff }),
		"size":            valid[:len(valid)-1],
		"digest":          mutate(valid, func(data []byte) { data[trailer] ^= 0xff }),
		"lsn":             mutate(valid, func(data []byte) { binary.LittleEndian.PutUint64(data[trailer+32:trailer+40], 8) }),
		"total":           mutate(valid, func(data []byte) { binary.LittleEndian.PutUint32(data[trailer+40:trailer+44], 1) }),
		"magic-end":       mutate(valid, func(data []byte) { data[trailer+44] ^= 0xff }),
	}
	for name, encoded := range tests {
		t.Run(name, func(t *testing.T) {
			if _, _, err := decodeSnapshot(name, encoded); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("error = %v", err)
			}
		})
	}
	decodedID, decoded, err := decodeSnapshot("valid", valid)
	if err != nil || decodedID != storeID || decoded.Generation != 2 || !bytes.Equal(decoded.Payload, snapshot.Payload) {
		t.Fatalf("decoded = %x/%+v, %v", decodedID, decoded, err)
	}
	assertPanics(t, func() { encodeSnapshot(storeID, Snapshot{Payload: make([]byte, int(maxPayloadSize)+1)}) })
}

func TestHeadDecoderRejectsEveryFieldClass(t *testing.T) {
	validHead := headState{StoreID: [16]byte{1}, WALFloor: 1, WALHead: 2, DurableLSN: 3, SnapshotThroughLSN: 1}
	valid := encodeHead(validHead)
	tests := map[string][]byte{
		"size":                   valid[:10],
		"magic":                  mutate(valid, func(data []byte) { data[0] ^= 0xff }),
		"format":                 mutate(valid, func(data []byte) { binary.LittleEndian.PutUint16(data[8:10], 99); fixCRC(data, 68, 68) }),
		"checksum":               mutate(valid, func(data []byte) { data[68] ^= 0xff }),
		"zero-floor":             mutate(valid, func(data []byte) { binary.LittleEndian.PutUint64(data[28:36], 0); fixCRC(data, 68, 68) }),
		"head-before-floor":      mutate(valid, func(data []byte) { binary.LittleEndian.PutUint64(data[36:44], 0); fixCRC(data, 68, 68) }),
		"snapshot-after-durable": mutate(valid, func(data []byte) { binary.LittleEndian.PutUint64(data[60:68], 4); fixCRC(data, 68, 68) }),
	}
	for name, encoded := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeHead(name, encoded); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("error = %v", err)
			}
		})
	}
	decoded, err := decodeHead("valid", valid)
	if err != nil || decoded != validHead {
		t.Fatalf("decoded = %+v, %v", decoded, err)
	}
}

func TestJournalLifecycleStatsAndInvalidDiskEntries(t *testing.T) {
	dir := t.TempDir()
	store := openTestJournal(t, Config{Dir: dir, Now: func() time.Time { return time.Unix(10, 0).UTC() }})
	if empty, err := store.AppendBatch(context.Background(), nil); err != nil || len(empty) != 0 {
		t.Fatalf("empty batch = %v, %v", empty, err)
	}
	if _, err := store.Append(context.Background(), TransactionID{9}, make([]byte, int(maxPayloadSize)+1)); !errors.Is(err, ErrPayloadLarge) {
		t.Fatalf("oversized append error = %v", err)
	}
	if _, err := store.Append(context.Background(), TransactionID{1}, []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := store.Checkpoint(context.Background(), 2, nil); !errors.Is(err, ErrInvalidLSN) {
		t.Fatalf("invalid checkpoint error = %v", err)
	}
	stats := store.Stats()
	if stats.DurableLSN != 1 || stats.SegmentCount != 1 || stats.LastSyncAt == nil {
		t.Fatalf("stats = %+v", stats)
	}
	if snapshot := store.Snapshot(); snapshot.Generation != 0 || len(snapshot.Payload) != 0 {
		t.Fatalf("snapshot = %+v", snapshot)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(context.Background(), TransactionID{2}, nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed append error = %v", err)
	}
	if err := store.Checkpoint(context.Background(), 1, nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed checkpoint error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	invalidDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(invalidDir, "wal"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(invalidDir, "snapshots"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(invalidDir, "wal", "bad.wal"), []byte("bad"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(Config{Dir: invalidDir}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("invalid filename error = %v", err)
	}
}

func TestHeadAndSnapshotDiskFailures(t *testing.T) {
	dir := t.TempDir()
	instance := &FileJournal{dir: dir, snapshotDir: filepath.Join(dir, "snapshots")}
	if _, _, err := instance.loadHead(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "HEAD"), []byte("bad"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := instance.loadHead(); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("head error = %v", err)
	}

	if err := os.MkdirAll(instance.snapshotDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(instance.snapshotDir, "snapshot-00000000000000000001-00000000000000000001.snap"), []byte("bad"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := instance.loadSnapshots(); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("snapshot error = %v", err)
	}
}

func mutate(input []byte, change func([]byte)) []byte {
	output := bytes.Clone(input)
	change(output)
	return output
}

func fixCRC(data []byte, covered, offset int) {
	binary.LittleEndian.PutUint32(data[offset:offset+4], crc32.Checksum(data[:covered], crcTable))
}

func assertPanics(t *testing.T, operation func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	operation()
}
