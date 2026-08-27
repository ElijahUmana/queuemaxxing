package journal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
)

const headSize = 72

var headMagic = [8]byte{'Q', 'M', 'X', 'H', 'E', 'A', 'D', '1'}

type headState struct {
	StoreID            [16]byte
	WALFloor           uint64
	WALHead            uint64
	DurableLSN         uint64
	SnapshotGeneration uint64
	SnapshotThroughLSN uint64
}

func encodeHead(head headState) []byte {
	encoded := make([]byte, headSize)
	copy(encoded[:8], headMagic[:])
	binary.LittleEndian.PutUint16(encoded[8:10], formatVersion)
	binary.LittleEndian.PutUint16(encoded[10:12], headSize)
	copy(encoded[12:28], head.StoreID[:])
	binary.LittleEndian.PutUint64(encoded[28:36], head.WALFloor)
	binary.LittleEndian.PutUint64(encoded[36:44], head.WALHead)
	binary.LittleEndian.PutUint64(encoded[44:52], head.DurableLSN)
	binary.LittleEndian.PutUint64(encoded[52:60], head.SnapshotGeneration)
	binary.LittleEndian.PutUint64(encoded[60:68], head.SnapshotThroughLSN)
	binary.LittleEndian.PutUint32(encoded[68:72], crc32.Checksum(encoded[:68], crcTable))
	return encoded
}

func decodeHead(path string, encoded []byte) (headState, error) {
	if len(encoded) != headSize {
		return headState{}, &CorruptionError{Path: path, Reason: "invalid head size"}
	}
	if !bytes.Equal(encoded[:8], headMagic[:]) {
		return headState{}, &CorruptionError{Path: path, Reason: "invalid head magic"}
	}
	if binary.LittleEndian.Uint16(encoded[8:10]) != formatVersion || binary.LittleEndian.Uint16(encoded[10:12]) != headSize {
		return headState{}, &CorruptionError{Path: path, Reason: "unsupported head format"}
	}
	if binary.LittleEndian.Uint32(encoded[68:72]) != crc32.Checksum(encoded[:68], crcTable) {
		return headState{}, &CorruptionError{Path: path, Reason: "head checksum mismatch"}
	}
	var result headState
	copy(result.StoreID[:], encoded[12:28])
	result.WALFloor = binary.LittleEndian.Uint64(encoded[28:36])
	result.WALHead = binary.LittleEndian.Uint64(encoded[36:44])
	result.DurableLSN = binary.LittleEndian.Uint64(encoded[44:52])
	result.SnapshotGeneration = binary.LittleEndian.Uint64(encoded[52:60])
	result.SnapshotThroughLSN = binary.LittleEndian.Uint64(encoded[60:68])
	if result.WALFloor == 0 || result.WALHead < result.WALFloor || result.SnapshotThroughLSN > result.DurableLSN {
		return headState{}, &CorruptionError{Path: path, LSN: result.DurableLSN, Reason: "invalid head invariants"}
	}
	return result, nil
}

func (journal *FileJournal) loadHead() (headState, bool, error) {
	path := filepath.Join(journal.dir, "HEAD")
	encoded, err := journal.readFile(path)
	if os.IsNotExist(err) {
		return headState{}, false, nil
	}
	if err != nil {
		return headState{}, false, fmt.Errorf("read journal head: %w", err)
	}
	head, err := decodeHead(path, encoded)
	if err != nil {
		return headState{}, false, err
	}
	return head, true, nil
}

func (journal *FileJournal) persistHeadLocked(head headState) error {
	path := filepath.Join(journal.dir, "HEAD")
	temporary := path + ".tmp"
	file, err := journal.openFile(temporary, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	cleanup := true
	defer func() {
		_ = file.Close()
		if cleanup {
			_ = journal.remove(temporary)
		}
	}()
	if err := journal.writeLocked(file, temporary, encodeHead(head)); err != nil {
		return err
	}
	if err := journal.syncLocked(file, temporary); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if journal.faults.BeforeRename != nil {
		if err := journal.faults.BeforeRename(temporary, path); err != nil {
			return err
		}
	}
	if err := journal.rename(temporary, path); err != nil {
		return err
	}
	cleanup = false
	if err := journal.syncDirectoryLocked(journal.dir); err != nil {
		return err
	}
	journal.head = head
	return nil
}
