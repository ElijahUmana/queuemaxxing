package integration

import (
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/ElijahUmana/queuemaxxing/internal/journal"
)

func TestNewestWALTornTailRecoveryAtEveryOffset(t *testing.T) {
	_, sourceWAL, original := createWALFixture(t)
	const recordHeaderSize = 48
	lastFrameOffset := lastRecordOffset(original)
	for cutWithinFrame := 0; cutWithinFrame < len(original)-lastFrameOffset; cutWithinFrame++ {
		cutWithinFrame := cutWithinFrame
		t.Run(stringOffset(cutWithinFrame), func(t *testing.T) {
			directory := t.TempDir()
			copyJournalTree(t, filepath.Dir(filepath.Dir(sourceWAL)), directory)
			wal := singleWALPath(t, directory)
			headPath := filepath.Join(directory, "HEAD")
			head, err := os.ReadFile(headPath)
			if err != nil {
				t.Fatal(err)
			}
			binary.LittleEndian.PutUint64(head[44:52], 1)
			binary.LittleEndian.PutUint32(head[68:72], crc32c(head[:68]))
			if err := os.WriteFile(headPath, head, 0o640); err != nil {
				t.Fatal(err)
			}
			offset := lastFrameOffset + cutWithinFrame
			if err := os.Truncate(wal, int64(offset)); err != nil {
				t.Fatal(err)
			}
			recovered, err := journal.Open(journal.Config{Dir: directory})
			if cutWithinFrame >= recordHeaderSize && err != nil {
				t.Fatalf("cut %d: recover uncommitted newest tail: %v", cutWithinFrame, err)
			}
			if cutWithinFrame < recordHeaderSize && err != nil && !errors.Is(err, journal.ErrCorrupt) {
				t.Fatalf("cut %d: unexpected error: %v", cutWithinFrame, err)
			}
			if recovered != nil {
				defer recovered.Close()
				if got := len(recovered.Records()); got != 1 {
					t.Fatalf("cut %d: recovered %d records, want committed prefix of 1", cutWithinFrame, got)
				}
			}
		})
	}
}

func TestTruncatingBelowDurableHeadRefusesStartup(t *testing.T) {
	_, sourceWAL, _ := createWALFixture(t)
	directory := t.TempDir()
	copyJournalTree(t, filepath.Dir(filepath.Dir(sourceWAL)), directory)
	wal := singleWALPath(t, directory)
	if err := os.Truncate(wal, 80); err != nil {
		t.Fatal(err)
	}
	recovered, err := journal.Open(journal.Config{Dir: directory})
	if recovered != nil {
		_ = recovered.Close()
	}
	if err == nil || !errors.Is(err, journal.ErrCorrupt) {
		t.Fatalf("error = %v, want durable-head corruption refusal", err)
	}
}

func TestWALCorruptionRefusesStartup(t *testing.T) {
	_, sourceWAL, original := createWALFixture(t)
	offsets := []int64{0, 7, 44, 76, 80, 83, int64(len(original) / 2), int64(len(original) - 1)}
	for _, offset := range offsets {
		for bit := uint8(0); bit < 8; bit++ {
			offset, bit := offset, bit
			t.Run(stringOffset(int(offset))+"/bit="+stringOffset(int(bit)), func(t *testing.T) {
				assertBitFlipRefuses(t, filepath.Dir(filepath.Dir(sourceWAL)), offset, bit)
			})
		}
	}
}

func TestWALCorruptionRefusesStartupEveryByteAndBit(t *testing.T) {
	if os.Getenv("QMAX_EXHAUSTIVE_CORRUPTION") != "1" {
		t.Skip("set QMAX_EXHAUSTIVE_CORRUPTION=1 for the scheduled exhaustive matrix")
	}
	_, sourceWAL, original := createWALFixture(t)
	root := filepath.Dir(filepath.Dir(sourceWAL))
	for offset := range original {
		for bit := uint8(0); bit < 8; bit++ {
			assertBitFlipRefuses(t, root, int64(offset), bit)
		}
	}
}

func assertBitFlipRefuses(t *testing.T, sourceRoot string, offset int64, bit uint8) {
	t.Helper()
	directory := t.TempDir()
	copyJournalTree(t, sourceRoot, directory)
	wal := singleWALPath(t, directory)
	flipped := filepath.Join(t.TempDir(), "flipped.wal")
	if err := CopyAndFlipBit(wal, flipped, offset, bit); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(flipped)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(wal, contents, 0o640); err != nil {
		t.Fatal(err)
	}
	recovered, err := journal.Open(journal.Config{Dir: directory})
	if recovered != nil {
		_ = recovered.Close()
	}
	if err == nil || !errors.Is(err, journal.ErrCorrupt) {
		t.Fatalf("offset %d bit %d: error = %v, want corruption", offset, bit, err)
	}
}

func createWALFixture(t *testing.T) (string, string, []byte) {
	t.Helper()
	directory := t.TempDir()
	store, err := journal.Open(journal.Config{Dir: directory})
	if err != nil {
		t.Fatal(err)
	}
	for index, payload := range [][]byte{[]byte(`{"operation":"one"}`), []byte(`{"operation":"two","padding":"xxxxxxxxxxxxxxxx"}`)} {
		var transactionID journal.TransactionID
		transactionID[0] = byte(index + 1)
		if _, err := store.Append(context.Background(), transactionID, payload); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	wal := singleWALPath(t, directory)
	contents, err := os.ReadFile(wal)
	if err != nil {
		t.Fatal(err)
	}
	return directory, wal, contents
}

func singleWALPath(t *testing.T, directory string) string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(directory, "wal", "*.wal"))
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 1 {
		t.Fatalf("WAL paths = %v, want one", paths)
	}
	return paths[0]
}

func copyJournalTree(t *testing.T, source, destination string) {
	t.Helper()
	if err := filepath.WalkDir(source, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		target := filepath.Join(destination, relative)
		if entry.IsDir() {
			return os.MkdirAll(target, 0o750)
		}
		contents, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, contents, 0o640)
	}); err != nil {
		t.Fatal(err)
	}
}

func lastRecordOffset(encoded []byte) int {
	const (
		segmentHeaderSize = 80
		recordHeaderSize  = 48
		recordTrailerSize = 12
	)
	offset := segmentHeaderSize
	last := offset
	for offset+recordHeaderSize <= len(encoded) {
		last = offset
		bodyLength := int(binary.LittleEndian.Uint32(encoded[offset+12 : offset+16]))
		offset += recordHeaderSize + bodyLength + recordTrailerSize
	}
	return last
}

func crc32c(encoded []byte) uint32 {
	return crc32.Checksum(encoded, crc32.MakeTable(crc32.Castagnoli))
}

func stringOffset(value int) string {
	if value == 0 {
		return "0"
	}
	buffer := [20]byte{}
	position := len(buffer)
	for value > 0 {
		position--
		buffer[position] = byte('0' + value%10)
		value /= 10
	}
	return string(buffer[position:])
}
