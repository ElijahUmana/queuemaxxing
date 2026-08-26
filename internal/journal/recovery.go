package journal

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type snapshotCandidate struct {
	path     string
	storeID  [16]byte
	snapshot Snapshot
}

func trimStaleSegmentPrefix(paths []string, floor uint64) []string {
	first := 0
	for first < len(paths) {
		name := strings.TrimSuffix(filepath.Base(paths[first]), ".wal")
		id, err := strconv.ParseUint(name, 10, 64)
		if err != nil || id >= floor {
			break
		}
		first++
	}
	return paths[first:]
}

func (journal *FileJournal) reconcileInterruptedSegmentPublication(paths []string, head uint64) ([]string, error) {
	firstExtra := len(paths)
	for index, path := range paths {
		name := strings.TrimSuffix(filepath.Base(path), ".wal")
		id, err := strconv.ParseUint(name, 10, 64)
		if err != nil {
			return nil, &CorruptionError{Path: path, Reason: "invalid segment filename"}
		}
		if id > head {
			firstExtra = index
			break
		}
	}
	if firstExtra == len(paths) {
		return paths, nil
	}
	quarantineDir := filepath.Join(journal.dir, "quarantine")
	if err := os.MkdirAll(quarantineDir, 0o700); err != nil {
		return nil, err
	}
	if err := journal.syncDirectoryLocked(journal.dir); err != nil {
		return nil, err
	}
	for _, path := range paths[firstExtra:] {
		destination := filepath.Join(quarantineDir, filepath.Base(path)+".uncommitted")
		if _, err := os.Stat(destination); err == nil {
			return nil, &CorruptionError{Path: destination, Reason: "uncommitted segment quarantine destination already exists"}
		} else if !os.IsNotExist(err) {
			return nil, err
		}
		if err := os.Rename(path, destination); err != nil {
			return nil, err
		}
	}
	if err := journal.syncDirectoryLocked(quarantineDir); err != nil {
		return nil, err
	}
	if err := journal.syncDirectoryLocked(journal.walDir); err != nil {
		return nil, err
	}
	return paths[:firstExtra], nil
}

func (journal *FileJournal) recoverSegments(paths []string, snapshots []snapshotCandidate, head headState) ([]segmentMeta, []Record, [16]byte, error) {
	metas := make([]segmentMeta, 0, len(paths))
	records := make([]Record, 0)
	var storeID [16]byte
	var previousDigest [32]byte
	if head.StoreID == ([16]byte{}) {
		return nil, nil, storeID, &CorruptionError{Path: filepath.Join(journal.dir, "HEAD"), Reason: "empty store identity"}
	}
	paths, err := journal.reconcileInterruptedSegmentPublication(paths, head.WALHead)
	if err != nil {
		return nil, nil, storeID, err
	}
	paths = trimStaleSegmentPrefix(paths, head.WALFloor)
	if len(paths) == 0 {
		return nil, nil, storeID, &CorruptionError{Path: journal.walDir, LSN: head.DurableLSN, Reason: "all head-referenced WAL segments are missing"}
	}
	firstName := segmentName(head.WALFloor)
	lastName := segmentName(head.WALHead)
	if filepath.Base(paths[0]) != firstName || filepath.Base(paths[len(paths)-1]) != lastName {
		return nil, nil, storeID, &CorruptionError{Path: journal.walDir, LSN: head.DurableLSN, Reason: fmt.Sprintf("WAL segment range mismatch: expected %s through %s", firstName, lastName)}
	}
	expectedSegmentID := head.WALFloor
	expectedLSN := head.SnapshotThroughLSN + 1
	snapshotThroughLSN := head.SnapshotThroughLSN
	for index, path := range paths {
		// #nosec G304 -- path comes from segmentPaths after strict 20-digit WAL filename validation.
		contents, err := os.ReadFile(path)
		if err != nil {
			return nil, nil, storeID, fmt.Errorf("read WAL segment %s: %w", path, err)
		}
		header, err := decodeSegmentHeader(path, contents)
		if err != nil {
			return nil, nil, storeID, err
		}
		if index == 0 {
			storeID = header.StoreID
			if storeID != head.StoreID {
				return nil, nil, storeID, &CorruptionError{Path: path, Reason: "head and WAL store identities do not match"}
			}
			if len(snapshots) == 0 {
				if head.SnapshotGeneration != 0 || head.SnapshotThroughLSN != 0 {
					return nil, nil, storeID, &CorruptionError{Path: journal.snapshotDir, Reason: "head references missing snapshot"}
				}
			} else {
				matched := false
				for _, candidate := range snapshots {
					if candidate.storeID == storeID && candidate.snapshot.Generation == head.SnapshotGeneration && candidate.snapshot.ThroughLSN == head.SnapshotThroughLSN {
						journal.snapshot = candidate.snapshot
						snapshotThroughLSN = candidate.snapshot.ThroughLSN
						expectedLSN = snapshotThroughLSN + 1
						matched = true
						break
					}
				}
				if !matched {
					for _, candidate := range snapshots {
						if candidate.storeID == storeID && candidate.snapshot.Generation < head.SnapshotGeneration {
							journal.snapshot = candidate.snapshot
							snapshotThroughLSN = candidate.snapshot.ThroughLSN
							expectedLSN = snapshotThroughLSN + 1
							matched = true
							break
						}
					}
				}
				if !matched {
					return nil, nil, storeID, &CorruptionError{Path: journal.snapshotDir, Reason: "snapshot and WAL store identities do not match"}
				}
			}
		} else {
			if header.StoreID != storeID {
				return nil, nil, storeID, &CorruptionError{Path: path, Reason: "store identity mismatch"}
			}
			if header.ID != expectedSegmentID {
				return nil, nil, storeID, &CorruptionError{Path: path, Reason: fmt.Sprintf("segment ID gap: expected %d got %d", expectedSegmentID, header.ID)}
			}
			if header.Previous != previousDigest {
				return nil, nil, storeID, &CorruptionError{Path: path, Reason: "segment hash chain mismatch"}
			}
		}
		if header.ID != expectedSegmentID {
			return nil, nil, storeID, &CorruptionError{Path: path, Reason: fmt.Sprintf("unexpected first segment ID %d", header.ID)}
		}
		if header.FirstLSN > expectedLSN {
			return nil, nil, storeID, &CorruptionError{Path: path, LSN: header.FirstLSN, Reason: fmt.Sprintf("LSN gap: expected at most %d", expectedLSN)}
		}

		offset := segmentHeaderSize
		lastLSN := uint64(0)
		for offset < len(contents) {
			record, consumed, decodeErr := decodeRecord(path, contents[offset:], int64(offset))
			if decodeErr != nil {
				if index != len(paths)-1 || expectedLSN <= head.DurableLSN || !isRepairableTornTail(contents[offset:]) || containsValidRecord(contents[offset+1:], int64(offset+1)) {
					return nil, nil, storeID, decodeErr
				}
				if err := journal.repairTail(path, int64(offset), contents[offset:]); err != nil {
					return nil, nil, storeID, fmt.Errorf("repair torn WAL tail: %w", err)
				}
				contents = contents[:offset]
				break
			}
			if record.LSN > head.DurableLSN {
				if index != len(paths)-1 {
					return nil, nil, storeID, &CorruptionError{Path: path, Offset: int64(offset), LSN: record.LSN, Reason: "uncommitted record appears before WAL head segment"}
				}
				if err := journal.repairTail(path, int64(offset), contents[offset:]); err != nil {
					return nil, nil, storeID, fmt.Errorf("discard uncommitted WAL suffix: %w", err)
				}
				contents = contents[:offset]
				break
			}
			if record.LSN < expectedLSN {
				if record.LSN > snapshotThroughLSN {
					return nil, nil, storeID, &CorruptionError{Path: path, Offset: int64(offset), LSN: record.LSN, Reason: "duplicate LSN"}
				}
			} else if record.LSN != expectedLSN {
				return nil, nil, storeID, &CorruptionError{Path: path, Offset: int64(offset), LSN: record.LSN, Reason: fmt.Sprintf("LSN gap: expected %d", expectedLSN)}
			} else {
				records = append(records, record)
				expectedLSN++
			}
			lastLSN = record.LSN
			offset += consumed
		}
		digest := sha256.Sum256(contents)
		metas = append(metas, segmentMeta{
			path: path, id: header.ID, firstLSN: header.FirstLSN, lastLSN: lastLSN,
			size: int64(len(contents)), digest: digest, header: header,
		})
		previousDigest = digest
		expectedSegmentID++
	}
	if len(snapshots) > 0 && journal.snapshot.Generation == 0 {
		return nil, nil, storeID, &CorruptionError{Path: journal.snapshotDir, Reason: "snapshot and WAL store identities do not match"}
	}
	if expectedLSN-1 != head.DurableLSN {
		return nil, nil, storeID, &CorruptionError{Path: journal.walDir, LSN: expectedLSN - 1, Reason: fmt.Sprintf("durable LSN mismatch: head=%d recovered=%d", head.DurableLSN, expectedLSN-1)}
	}
	return metas, records, storeID, nil
}

func isRepairableTornTail(encoded []byte) bool {
	if len(encoded) < recordHeaderSize {
		return true
	}
	if binary.LittleEndian.Uint32(encoded[0:4]) != recordMagic || binary.LittleEndian.Uint16(encoded[4:6]) != formatVersion || binary.LittleEndian.Uint16(encoded[6:8]) != recordKindData || binary.LittleEndian.Uint16(encoded[10:12]) != recordHeaderSize {
		return false
	}
	bodyLength := binary.LittleEndian.Uint32(encoded[12:16])
	if bodyLength > maxPayloadSize || binary.LittleEndian.Uint32(encoded[16:20]) != ^bodyLength || binary.LittleEndian.Uint32(encoded[44:48]) != crc32.Checksum(encoded[:44], crcTable) {
		return false
	}
	total := recordHeaderSize + int(bodyLength) + recordTrailerSize
	return len(encoded) < total
}

func containsValidRecord(encoded []byte, baseOffset int64) bool {
	magic := make([]byte, 4)
	binary.LittleEndian.PutUint32(magic, recordMagic)
	for {
		index := bytes.Index(encoded, magic)
		if index < 0 {
			return false
		}
		if _, _, err := decodeRecord("", encoded[index:], baseOffset+int64(index)); err == nil {
			return true
		}
		encoded = encoded[index+1:]
		baseOffset += int64(index + 1)
	}
}

func (journal *FileJournal) repairTail(path string, offset int64, suffix []byte) error {
	if len(suffix) > 0 {
		quarantineDir := filepath.Join(journal.dir, "quarantine")
		if err := os.MkdirAll(quarantineDir, 0o700); err != nil {
			return err
		}
		if err := journal.syncDirectoryLocked(journal.dir); err != nil {
			return err
		}
		quarantinePath := filepath.Join(quarantineDir, fmt.Sprintf("%s-%d.bad", filepath.Base(path), offset))
		// #nosec G304,G703 -- quarantinePath is beneath the locked store and contains only filepath.Base of a validated WAL path.
		file, err := os.OpenFile(quarantinePath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if os.IsExist(err) {
			// #nosec G304,G703 -- same fixed quarantine path checked above; read verifies existing crash evidence.
			existing, readErr := os.ReadFile(quarantinePath)
			if readErr != nil {
				return readErr
			}
			if !bytes.Equal(existing, suffix) {
				return &CorruptionError{Path: quarantinePath, Offset: offset, Reason: "existing quarantine evidence does not match WAL suffix"}
			}
			file = nil
			err = nil
		}
		if err != nil {
			return err
		}
		if file != nil {
			if err := journal.writeLocked(file, quarantinePath, suffix); err != nil {
				_ = file.Close()
				return err
			}
			if err := journal.syncLocked(file, quarantinePath); err != nil {
				_ = file.Close()
				return err
			}
			if err := file.Close(); err != nil {
				return err
			}
		}
		if err := journal.syncDirectoryLocked(quarantineDir); err != nil {
			return err
		}
	}
	if journal.faults.BeforeTruncate != nil {
		if err := journal.faults.BeforeTruncate(path, offset); err != nil {
			return err
		}
	}
	// #nosec G304 -- path is an active WAL path produced by validated segment enumeration.
	file, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := file.Truncate(offset); err != nil {
		return err
	}
	return journal.syncLocked(file, path)
}

func encodeSnapshot(storeID [16]byte, snapshot Snapshot) []byte {
	if len(snapshot.Payload) > int(maxPayloadSize) {
		panic("snapshot payload exceeds maximum size")
	}
	encoded := make([]byte, snapshotHeaderSize+len(snapshot.Payload)+snapshotTrailerSize)
	payloadLength := uint32(len(snapshot.Payload)) // #nosec G115 -- bounded by maxPayloadSize above.
	totalLength := uint32(len(encoded))            // #nosec G115 -- fixed overhead plus bounded payload.
	copy(encoded[:8], snapshotMagic[:])
	binary.LittleEndian.PutUint16(encoded[8:10], formatVersion)
	binary.LittleEndian.PutUint16(encoded[10:12], snapshotHeaderSize)
	binary.LittleEndian.PutUint64(encoded[12:20], snapshot.Generation)
	binary.LittleEndian.PutUint64(encoded[20:28], snapshot.ThroughLSN)
	binary.LittleEndian.PutUint32(encoded[28:32], payloadLength)
	binary.LittleEndian.PutUint32(encoded[32:36], ^payloadLength)
	copy(encoded[36:52], storeID[:])
	binary.LittleEndian.PutUint32(encoded[52:56], crc32.Checksum(encoded[:52], crcTable))
	copy(encoded[snapshotHeaderSize:], snapshot.Payload)
	trailer := snapshotHeaderSize + len(snapshot.Payload)
	digest := sha256.Sum256(encoded[:trailer])
	copy(encoded[trailer:trailer+32], digest[:])
	binary.LittleEndian.PutUint64(encoded[trailer+32:trailer+40], snapshot.ThroughLSN)
	binary.LittleEndian.PutUint32(encoded[trailer+40:trailer+44], totalLength)
	binary.LittleEndian.PutUint32(encoded[trailer+44:trailer+48], snapshotEndMagic)
	return encoded
}

func decodeSnapshot(path string, encoded []byte) ([16]byte, Snapshot, error) {
	var storeID [16]byte
	if len(encoded) < snapshotHeaderSize+snapshotTrailerSize {
		return storeID, Snapshot{}, &CorruptionError{Path: path, Reason: "incomplete snapshot"}
	}
	if !bytes.Equal(encoded[:8], snapshotMagic[:]) {
		return storeID, Snapshot{}, &CorruptionError{Path: path, Reason: "invalid snapshot magic"}
	}
	if binary.LittleEndian.Uint16(encoded[8:10]) != formatVersion || binary.LittleEndian.Uint16(encoded[10:12]) != snapshotHeaderSize {
		return storeID, Snapshot{}, &CorruptionError{Path: path, Reason: "unsupported snapshot format"}
	}
	generation := binary.LittleEndian.Uint64(encoded[12:20])
	throughLSN := binary.LittleEndian.Uint64(encoded[20:28])
	payloadLength := binary.LittleEndian.Uint32(encoded[28:32])
	if payloadLength > maxPayloadSize || binary.LittleEndian.Uint32(encoded[32:36]) != ^payloadLength {
		return storeID, Snapshot{}, &CorruptionError{Path: path, LSN: throughLSN, Reason: "invalid snapshot length"}
	}
	if binary.LittleEndian.Uint32(encoded[52:56]) != crc32.Checksum(encoded[:52], crcTable) {
		return storeID, Snapshot{}, &CorruptionError{Path: path, LSN: throughLSN, Reason: "snapshot header checksum mismatch"}
	}
	total := snapshotHeaderSize + int(payloadLength) + snapshotTrailerSize
	if len(encoded) != total {
		return storeID, Snapshot{}, &CorruptionError{Path: path, LSN: throughLSN, Reason: "snapshot size mismatch"}
	}
	trailer := snapshotHeaderSize + int(payloadLength)
	digest := sha256.Sum256(encoded[:trailer])
	if !bytes.Equal(encoded[trailer:trailer+32], digest[:]) || binary.LittleEndian.Uint64(encoded[trailer+32:trailer+40]) != throughLSN || binary.LittleEndian.Uint32(encoded[trailer+40:trailer+44]) != uint32(total) || binary.LittleEndian.Uint32(encoded[trailer+44:trailer+48]) != snapshotEndMagic {
		return storeID, Snapshot{}, &CorruptionError{Path: path, LSN: throughLSN, Reason: "snapshot checksum or trailer mismatch"}
	}
	copy(storeID[:], encoded[36:52])
	return storeID, Snapshot{Generation: generation, ThroughLSN: throughLSN, Payload: bytes.Clone(encoded[snapshotHeaderSize:trailer])}, nil
}

func (journal *FileJournal) loadSnapshots() ([]snapshotCandidate, error) {
	entries, err := os.ReadDir(journal.snapshotDir)
	if err != nil {
		return nil, fmt.Errorf("read snapshot directory: %w", err)
	}
	candidates := make([]snapshotCandidate, 0)
	invalid := make([]error, 0)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".snap") {
			continue
		}
		path := filepath.Join(journal.snapshotDir, entry.Name())
		// #nosec G304 -- path is formed from a snapshotDir entry constrained to the .snap format.
		contents, readErr := os.ReadFile(path)
		if readErr != nil {
			invalid = append(invalid, readErr)
			continue
		}
		storeID, snapshot, decodeErr := decodeSnapshot(path, contents)
		if decodeErr != nil {
			invalid = append(invalid, decodeErr)
			continue
		}
		candidates = append(candidates, snapshotCandidate{path: path, storeID: storeID, snapshot: snapshot})
	}
	sort.Slice(candidates, func(left, right int) bool {
		return candidates[left].snapshot.Generation > candidates[right].snapshot.Generation
	})
	if len(candidates) == 0 && len(invalid) > 0 {
		return nil, errors.Join(invalid...)
	}
	return candidates, nil
}

func (journal *FileJournal) compactLocked() error {
	paths, err := journal.segmentPaths()
	if err != nil {
		return err
	}
	snapshots, err := journal.loadSnapshots()
	if err != nil {
		return err
	}
	cutoff := uint64(0)
	if len(snapshots) >= 2 {
		cutoff = snapshots[1].snapshot.ThroughLSN
	}
	removable := make([]string, 0)
	newFloor := journal.head.WALFloor
	for _, path := range paths {
		if path == journal.activePath {
			continue
		}
		// #nosec G304 -- path comes from segmentPaths after strict 20-digit WAL filename validation.
		contents, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		header, err := decodeSegmentHeader(path, contents)
		if err != nil {
			return err
		}
		lastLSN := uint64(0)
		for offset := segmentHeaderSize; offset < len(contents); {
			record, consumed, err := decodeRecord(path, contents[offset:], int64(offset))
			if err != nil {
				return err
			}
			lastLSN = record.LSN
			offset += consumed
		}
		if cutoff != 0 && lastLSN != 0 && lastLSN <= cutoff {
			removable = append(removable, path)
			if header.ID >= newFloor {
				newFloor = header.ID + 1
			}
		}
	}
	if len(removable) > 0 {
		head := journal.head
		head.WALFloor = newFloor
		if err := journal.persistHeadLocked(head); err != nil {
			return err
		}
	}
	for _, path := range removable {
		info, err := os.Stat(path)
		if err != nil {
			return err
		}
		if journal.faults.BeforeRemove != nil {
			if err := journal.faults.BeforeRemove(path); err != nil {
				return err
			}
		}
		if err := os.Remove(path); err != nil {
			return err
		}
		journal.walBytes -= info.Size()
		journal.segments--
	}
	if err := journal.syncDirectoryLocked(journal.walDir); err != nil {
		return err
	}
	return journal.pruneSnapshotsLocked()
}

func (journal *FileJournal) pruneSnapshotsLocked() error {
	valid, err := journal.loadSnapshots()
	if err != nil {
		return err
	}
	if len(valid) <= 2 {
		return journal.syncDirectoryLocked(journal.snapshotDir)
	}
	for _, snapshot := range valid[2:] {
		if journal.faults.BeforeRemove != nil {
			if err := journal.faults.BeforeRemove(snapshot.path); err != nil {
				return err
			}
		}
		if err := os.Remove(snapshot.path); err != nil {
			return err
		}
	}
	return journal.syncDirectoryLocked(journal.snapshotDir)
}
