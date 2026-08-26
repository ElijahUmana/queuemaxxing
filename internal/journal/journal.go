package journal

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	formatVersion       = uint16(1)
	segmentHeaderSize   = 80
	recordHeaderSize    = 48
	recordTrailerSize   = 12
	snapshotHeaderSize  = 56
	snapshotTrailerSize = 48
	defaultSegmentSize  = int64(64 << 20)
	maxPayloadSize      = uint32(64 << 20)
	recordKindData      = uint16(1)
)

var (
	segmentMagic       = [8]byte{'Q', 'M', 'X', 'W', 'A', 'L', '0', '1'}
	recordMagic        = uint32(0x514d5852)
	recordTrailerMagic = uint32(0x524d5851)
	snapshotMagic      = [8]byte{'Q', 'M', 'X', 'S', 'N', 'A', 'P', '1'}
	snapshotEndMagic   = uint32(0x50414e53)
	crcTable           = crc32.MakeTable(crc32.Castagnoli)
)

var (
	ErrLocked       = errors.New("journal storage directory is locked")
	ErrClosed       = errors.New("journal is closed")
	ErrReadOnly     = errors.New("journal is read-only")
	ErrCorrupt      = errors.New("journal is corrupt")
	ErrInvalidLSN   = errors.New("invalid checkpoint LSN")
	ErrPayloadLarge = errors.New("journal payload exceeds maximum size")
)

type CorruptionError struct {
	Path   string
	Offset int64
	LSN    uint64
	Reason string
}

func (err *CorruptionError) Error() string {
	return fmt.Sprintf("%v: path=%s offset=%d lsn=%d: %s", ErrCorrupt, err.Path, err.Offset, err.LSN, err.Reason)
}

func (err *CorruptionError) Unwrap() error { return ErrCorrupt }

type FaultHooks struct {
	BeforeWrite    func(path string, data []byte) error
	AfterWrite     func(path string, data []byte) error
	BeforeSync     func(path string) error
	BeforeRename   func(oldPath, newPath string) error
	BeforeRemove   func(path string) error
	BeforeTruncate func(path string, size int64) error
}

type Config struct {
	Dir         string
	SegmentSize int64
	Now         func() time.Time
	Faults      FaultHooks
}

type FileJournal struct {
	mu sync.RWMutex

	dir         string
	walDir      string
	snapshotDir string
	segmentSize int64
	now         func() time.Time
	faults      FaultHooks
	lock        *directoryLock

	storeID    [16]byte
	active     *os.File
	activePath string
	activeID   uint64
	activeSize int64
	previous   [32]byte
	head       headState

	nextLSN    uint64
	records    []Record
	snapshot   Snapshot
	walBytes   int64
	segments   int
	lastSync   *time.Time
	readOnly   bool
	readReason string
	closed     bool
}

type segmentMeta struct {
	path     string
	id       uint64
	firstLSN uint64
	lastLSN  uint64
	size     int64
	digest   [32]byte
	header   segmentHeader
}

type segmentHeader struct {
	StoreID  [16]byte
	ID       uint64
	FirstLSN uint64
	Previous [32]byte
}

func Open(config Config) (*FileJournal, error) {
	if config.Dir == "" {
		return nil, errors.New("journal directory is required")
	}
	if config.SegmentSize == 0 {
		config.SegmentSize = defaultSegmentSize
	}
	if config.SegmentSize < segmentHeaderSize+recordHeaderSize+recordTrailerSize+1 {
		return nil, fmt.Errorf("journal segment size %d is too small", config.SegmentSize)
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if err := os.MkdirAll(config.Dir, 0o700); err != nil {
		return nil, fmt.Errorf("create journal directory: %w", err)
	}
	if err := syncDirectory(filepath.Dir(config.Dir)); err != nil {
		return nil, fmt.Errorf("sync journal parent directory: %w", err)
	}
	lock, err := acquireDirectoryLock(filepath.Join(config.Dir, "LOCK"))
	if err != nil {
		return nil, err
	}
	journal := &FileJournal{
		dir:         config.Dir,
		walDir:      filepath.Join(config.Dir, "wal"),
		snapshotDir: filepath.Join(config.Dir, "snapshots"),
		segmentSize: config.SegmentSize,
		now:         config.Now,
		faults:      config.Faults,
		lock:        lock,
		nextLSN:     1,
	}
	if err := journal.open(); err != nil {
		_ = lock.Close()
		return nil, err
	}
	return journal, nil
}

func (journal *FileJournal) open() error {
	if err := os.MkdirAll(journal.walDir, 0o700); err != nil {
		return fmt.Errorf("create WAL directory: %w", err)
	}
	if err := os.MkdirAll(journal.snapshotDir, 0o700); err != nil {
		return fmt.Errorf("create snapshot directory: %w", err)
	}
	if err := journal.removeInterruptedPublications(); err != nil {
		return err
	}
	if err := syncDirectory(journal.dir); err != nil {
		return fmt.Errorf("sync journal directory: %w", err)
	}
	head, hasHead, err := journal.loadHead()
	if err != nil {
		return err
	}
	snapshots, err := journal.loadSnapshots()
	if err != nil {
		return err
	}
	segments, err := journal.segmentPaths()
	if err != nil {
		return err
	}
	if len(segments) == 0 {
		if hasHead {
			return &CorruptionError{Path: filepath.Join(journal.dir, "HEAD"), LSN: head.DurableLSN, Reason: "head references missing WAL segments"}
		}
		if len(snapshots) > 0 {
			journal.storeID = snapshots[0].storeID
			journal.snapshot = snapshots[0].snapshot
			journal.nextLSN = journal.snapshot.ThroughLSN + 1
		} else if _, err := rand.Read(journal.storeID[:]); err != nil {
			return fmt.Errorf("generate journal identity: %w", err)
		}
		if err := journal.createSegment(1, journal.nextLSN, [32]byte{}); err != nil {
			return err
		}
		return journal.persistHeadLocked(headState{StoreID: journal.storeID, WALFloor: 1, WALHead: 1, DurableLSN: journal.nextLSN - 1, SnapshotGeneration: journal.snapshot.Generation, SnapshotThroughLSN: journal.snapshot.ThroughLSN})
	}

	if !hasHead {
		return &CorruptionError{Path: filepath.Join(journal.dir, "HEAD"), Reason: "WAL segments exist without durable head metadata"}
	}
	metas, recovered, storeID, err := journal.recoverSegments(segments, snapshots, head)
	if err != nil {
		return err
	}
	journal.storeID = storeID
	journal.head = head
	journal.records = recovered
	if len(metas) > 0 {
		last := metas[len(metas)-1]
		journal.activeID = last.id
		journal.activePath = last.path
		journal.activeSize = last.size
		journal.previous = last.header.Previous
		journal.walBytes = 0
		for _, meta := range metas {
			journal.walBytes += meta.size
		}
		journal.segments = len(metas)
		journal.nextLSN = last.lastLSN + 1
		if last.lastLSN == 0 {
			journal.nextLSN = last.firstLSN
		}
		journal.active, err = os.OpenFile(last.path, os.O_RDWR|os.O_APPEND, 0o600)
		if err != nil {
			return fmt.Errorf("open active WAL segment: %w", err)
		}
	}
	return nil
}

func (journal *FileJournal) removeInterruptedPublications() error {
	for _, path := range []string{filepath.Join(journal.dir, "HEAD.tmp"), journal.walDir, journal.snapshotDir} {
		info, err := os.Stat(path)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		if !info.IsDir() {
			if err := os.Remove(path); err != nil {
				return fmt.Errorf("remove interrupted publication %s: %w", path, err)
			}
			continue
		}
		entries, err := os.ReadDir(path)
		if err != nil {
			return err
		}
		removed := false
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".tmp") {
				continue
			}
			if err := os.Remove(filepath.Join(path, entry.Name())); err != nil {
				return fmt.Errorf("remove interrupted publication %s: %w", entry.Name(), err)
			}
			removed = true
		}
		if removed {
			if err := syncDirectory(path); err != nil {
				return err
			}
		}
	}
	return nil
}

func (journal *FileJournal) Append(ctx context.Context, transactionID TransactionID, payload []byte) (uint64, error) {
	records, err := journal.AppendBatch(ctx, []Record{{TransactionID: transactionID, Payload: payload}})
	if err != nil {
		return 0, err
	}
	return records[0], nil
}

func (journal *FileJournal) AppendBatch(ctx context.Context, input []Record) ([]uint64, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(input) == 0 {
		return []uint64{}, nil
	}
	for index := range input {
		if len(input[index].Payload) > int(maxPayloadSize) {
			return nil, ErrPayloadLarge
		}
	}

	journal.mu.Lock()
	defer journal.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := journal.writable(); err != nil {
		return nil, err
	}

	encoded := make([][]byte, len(input))
	committed := make([]Record, len(input))
	lsns := make([]uint64, len(input))
	total := 0
	for index := range input {
		lsn := journal.nextLSN + uint64(index)
		committed[index] = Record{LSN: lsn, TransactionID: input[index].TransactionID, Payload: bytes.Clone(input[index].Payload)}
		encoded[index] = encodeRecord(committed[index])
		total += len(encoded[index])
		lsns[index] = lsn
	}
	if journal.activeSize > segmentHeaderSize && journal.activeSize+int64(total) > journal.segmentSize {
		if err := journal.rotateLocked(); err != nil {
			return nil, journal.failLocked("rotate WAL before append", err)
		}
	}
	buffer := make([]byte, 0, total)
	for _, frame := range encoded {
		buffer = append(buffer, frame...)
	}
	if err := journal.writeLocked(journal.active, journal.activePath, buffer); err != nil {
		return nil, journal.failLocked("append WAL", err)
	}
	journal.activeSize += int64(len(buffer))
	journal.walBytes += int64(len(buffer))
	if err := journal.syncLocked(journal.active, journal.activePath); err != nil {
		return nil, journal.failLocked("sync WAL", err)
	}
	committedLSN := lsns[len(lsns)-1]
	head := journal.head
	head.StoreID = journal.storeID
	head.WALHead = journal.activeID
	head.DurableLSN = committedLSN
	if err := journal.persistHeadLocked(head); err != nil {
		return nil, journal.failLocked("publish durable WAL head", err)
	}
	now := journal.now().UTC()
	journal.lastSync = &now
	journal.records = append(journal.records, committed...)
	journal.nextLSN += uint64(len(committed))
	return lsns, nil
}

func (journal *FileJournal) Checkpoint(ctx context.Context, throughLSN uint64, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(payload) > int(maxPayloadSize) {
		return ErrPayloadLarge
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := journal.writable(); err != nil {
		return err
	}
	if throughLSN < journal.snapshot.ThroughLSN || throughLSN >= journal.nextLSN {
		return fmt.Errorf("%w: through=%d durable=%d snapshot=%d", ErrInvalidLSN, throughLSN, journal.nextLSN-1, journal.snapshot.ThroughLSN)
	}
	generation := journal.snapshot.Generation + 1
	snapshot := Snapshot{Generation: generation, ThroughLSN: throughLSN, Payload: bytes.Clone(payload)}
	path := filepath.Join(journal.snapshotDir, snapshotName(generation, throughLSN))
	temporary := path + ".tmp"
	encoded := encodeSnapshot(journal.storeID, snapshot)
	file, err := os.OpenFile(temporary, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return journal.failLocked("create snapshot", err)
	}
	cleanup := true
	defer func() {
		_ = file.Close()
		if cleanup {
			_ = os.Remove(temporary)
		}
	}()
	if err := journal.writeLocked(file, temporary, encoded); err != nil {
		return journal.failLocked("write snapshot", err)
	}
	if err := journal.syncLocked(file, temporary); err != nil {
		return journal.failLocked("sync snapshot", err)
	}
	if err := file.Close(); err != nil {
		return journal.failLocked("close snapshot", err)
	}
	contents, err := os.ReadFile(temporary)
	if err != nil {
		return journal.failLocked("verify snapshot", err)
	}
	verifiedID, verified, err := decodeSnapshot(temporary, contents)
	if err != nil || verifiedID != journal.storeID || verified.Generation != generation || verified.ThroughLSN != throughLSN || !bytes.Equal(verified.Payload, payload) {
		if err == nil {
			err = errors.New("snapshot verification mismatch")
		}
		return journal.failLocked("verify snapshot", err)
	}
	if journal.faults.BeforeRename != nil {
		if err := journal.faults.BeforeRename(temporary, path); err != nil {
			return journal.failLocked("publish snapshot", err)
		}
	}
	if err := os.Rename(temporary, path); err != nil {
		return journal.failLocked("publish snapshot", err)
	}
	cleanup = false
	if err := journal.syncDirectoryLocked(journal.snapshotDir); err != nil {
		return journal.failLocked("sync snapshot directory", err)
	}
	if journal.activeSize > segmentHeaderSize {
		if err := journal.rotateLocked(); err != nil {
			return journal.failLocked("rotate WAL after checkpoint", err)
		}
	}
	head := journal.head
	head.StoreID = journal.storeID
	head.WALHead = journal.activeID
	head.SnapshotGeneration = snapshot.Generation
	head.SnapshotThroughLSN = snapshot.ThroughLSN
	if err := journal.persistHeadLocked(head); err != nil {
		return journal.failLocked("publish checkpoint head", err)
	}
	journal.snapshot = snapshot
	firstRetained := 0
	for firstRetained < len(journal.records) && journal.records[firstRetained].LSN <= throughLSN {
		firstRetained++
	}
	journal.records = append([]Record(nil), journal.records[firstRetained:]...)
	if err := journal.compactLocked(); err != nil {
		return journal.failLocked("compact journal", err)
	}
	return nil
}

func (journal *FileJournal) Records() []Record {
	journal.mu.RLock()
	defer journal.mu.RUnlock()
	result := make([]Record, len(journal.records))
	for index := range journal.records {
		result[index] = Record{LSN: journal.records[index].LSN, TransactionID: journal.records[index].TransactionID, Payload: bytes.Clone(journal.records[index].Payload)}
	}
	return result
}

func (journal *FileJournal) Snapshot() Snapshot {
	journal.mu.RLock()
	defer journal.mu.RUnlock()
	return Snapshot{Generation: journal.snapshot.Generation, ThroughLSN: journal.snapshot.ThroughLSN, Payload: bytes.Clone(journal.snapshot.Payload)}
}

func (journal *FileJournal) Stats() Stats {
	journal.mu.RLock()
	defer journal.mu.RUnlock()
	stats := Stats{
		DurableLSN:         journal.nextLSN - 1,
		WALBytes:           journal.walBytes,
		SegmentCount:       journal.segments,
		SnapshotGeneration: journal.snapshot.Generation,
		ReadOnly:           journal.readOnly,
		ReadOnlyReason:     journal.readReason,
	}
	if journal.lastSync != nil {
		value := *journal.lastSync
		stats.LastSyncAt = &value
	}
	return stats
}

func (journal *FileJournal) Close() error {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if journal.closed {
		return nil
	}
	journal.closed = true
	var result error
	if journal.active != nil {
		result = journal.active.Close()
		journal.active = nil
	}
	if journal.lock != nil {
		result = errors.Join(result, journal.lock.Close())
		journal.lock = nil
	}
	return result
}

func (journal *FileJournal) writable() error {
	if journal.closed {
		return ErrClosed
	}
	if journal.readOnly {
		return fmt.Errorf("%w: %s", ErrReadOnly, journal.readReason)
	}
	return nil
}

func (journal *FileJournal) failLocked(operation string, err error) error {
	journal.readOnly = true
	journal.readReason = operation + ": " + err.Error()
	return fmt.Errorf("%w: %s", ErrReadOnly, journal.readReason)
}

func (journal *FileJournal) writeLocked(file *os.File, path string, data []byte) error {
	if journal.faults.BeforeWrite != nil {
		if err := journal.faults.BeforeWrite(path, data); err != nil {
			return err
		}
	}
	written := 0
	for written < len(data) {
		count, err := file.Write(data[written:])
		written += count
		if err != nil {
			return err
		}
		if count == 0 {
			return io.ErrShortWrite
		}
	}
	if journal.faults.AfterWrite != nil {
		if err := journal.faults.AfterWrite(path, data); err != nil {
			return err
		}
	}
	return nil
}

func (journal *FileJournal) syncLocked(file *os.File, path string) error {
	if journal.faults.BeforeSync != nil {
		if err := journal.faults.BeforeSync(path); err != nil {
			return err
		}
	}
	return file.Sync()
}

func (journal *FileJournal) syncDirectoryLocked(path string) error {
	if journal.faults.BeforeSync != nil {
		if err := journal.faults.BeforeSync(path); err != nil {
			return err
		}
	}
	return syncDirectory(path)
}

func (journal *FileJournal) rotateLocked() error {
	if journal.active == nil {
		return errors.New("active WAL is unavailable")
	}
	if err := journal.syncLocked(journal.active, journal.activePath); err != nil {
		return err
	}
	if err := journal.active.Close(); err != nil {
		return err
	}
	contents, err := os.ReadFile(journal.activePath)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(contents)
	if err := journal.createSegment(journal.activeID+1, journal.nextLSN, digest); err != nil {
		return err
	}
	head := journal.head
	head.StoreID = journal.storeID
	head.WALHead = journal.activeID
	return journal.persistHeadLocked(head)
}

func (journal *FileJournal) createSegment(id, firstLSN uint64, previous [32]byte) error {
	path := filepath.Join(journal.walDir, segmentName(id))
	temporary := path + ".tmp"
	file, err := os.OpenFile(temporary, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("create WAL segment: %w", err)
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = file.Close()
			_ = os.Remove(temporary)
		}
	}()
	header := encodeSegmentHeader(segmentHeader{StoreID: journal.storeID, ID: id, FirstLSN: firstLSN, Previous: previous})
	if err := journal.writeLocked(file, temporary, header); err != nil {
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
	if err := os.Rename(temporary, path); err != nil {
		return err
	}
	cleanup = false
	if err := journal.syncDirectoryLocked(journal.walDir); err != nil {
		return err
	}
	file, err = os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	journal.active = file
	journal.activePath = path
	journal.activeID = id
	journal.activeSize = int64(len(header))
	journal.previous = previous
	journal.walBytes += int64(len(header))
	journal.segments++
	return nil
}

func encodeSegmentHeader(header segmentHeader) []byte {
	encoded := make([]byte, segmentHeaderSize)
	copy(encoded[:8], segmentMagic[:])
	binary.LittleEndian.PutUint16(encoded[8:10], formatVersion)
	binary.LittleEndian.PutUint16(encoded[10:12], segmentHeaderSize)
	binary.LittleEndian.PutUint64(encoded[12:20], header.ID)
	binary.LittleEndian.PutUint64(encoded[20:28], header.FirstLSN)
	copy(encoded[28:44], header.StoreID[:])
	copy(encoded[44:76], header.Previous[:])
	binary.LittleEndian.PutUint32(encoded[76:80], crc32.Checksum(encoded[:76], crcTable))
	return encoded
}

func decodeSegmentHeader(path string, encoded []byte) (segmentHeader, error) {
	if len(encoded) < segmentHeaderSize {
		return segmentHeader{}, &CorruptionError{Path: path, Reason: "incomplete segment header"}
	}
	if !bytes.Equal(encoded[:8], segmentMagic[:]) {
		return segmentHeader{}, &CorruptionError{Path: path, Reason: "invalid segment magic"}
	}
	if binary.LittleEndian.Uint16(encoded[8:10]) != formatVersion || binary.LittleEndian.Uint16(encoded[10:12]) != segmentHeaderSize {
		return segmentHeader{}, &CorruptionError{Path: path, Reason: "unsupported segment format"}
	}
	if binary.LittleEndian.Uint32(encoded[76:80]) != crc32.Checksum(encoded[:76], crcTable) {
		return segmentHeader{}, &CorruptionError{Path: path, Reason: "segment header checksum mismatch"}
	}
	var result segmentHeader
	result.ID = binary.LittleEndian.Uint64(encoded[12:20])
	result.FirstLSN = binary.LittleEndian.Uint64(encoded[20:28])
	copy(result.StoreID[:], encoded[28:44])
	copy(result.Previous[:], encoded[44:76])
	return result, nil
}

func encodeRecord(record Record) []byte {
	encoded := make([]byte, recordHeaderSize+len(record.Payload)+recordTrailerSize)
	binary.LittleEndian.PutUint32(encoded[0:4], recordMagic)
	binary.LittleEndian.PutUint16(encoded[4:6], formatVersion)
	binary.LittleEndian.PutUint16(encoded[6:8], recordKindData)
	binary.LittleEndian.PutUint16(encoded[8:10], 0)
	binary.LittleEndian.PutUint16(encoded[10:12], recordHeaderSize)
	binary.LittleEndian.PutUint32(encoded[12:16], uint32(len(record.Payload)))
	binary.LittleEndian.PutUint32(encoded[16:20], ^uint32(len(record.Payload)))
	binary.LittleEndian.PutUint64(encoded[20:28], record.LSN)
	copy(encoded[28:44], record.TransactionID[:])
	binary.LittleEndian.PutUint32(encoded[44:48], crc32.Checksum(encoded[:44], crcTable))
	copy(encoded[recordHeaderSize:], record.Payload)
	trailer := recordHeaderSize + len(record.Payload)
	checksum := crc32.New(crcTable)
	_, _ = checksum.Write(encoded[:44])
	_, _ = checksum.Write(record.Payload)
	binary.LittleEndian.PutUint32(encoded[trailer:trailer+4], checksum.Sum32())
	binary.LittleEndian.PutUint32(encoded[trailer+4:trailer+8], uint32(len(encoded)))
	binary.LittleEndian.PutUint32(encoded[trailer+8:trailer+12], recordTrailerMagic)
	return encoded
}

func decodeRecord(path string, encoded []byte, offset int64) (Record, int, error) {
	if len(encoded) < recordHeaderSize {
		return Record{}, 0, &CorruptionError{Path: path, Offset: offset, Reason: "incomplete record header"}
	}
	if binary.LittleEndian.Uint32(encoded[0:4]) != recordMagic {
		return Record{}, 0, &CorruptionError{Path: path, Offset: offset, Reason: "invalid record magic"}
	}
	lsn := binary.LittleEndian.Uint64(encoded[20:28])
	if binary.LittleEndian.Uint16(encoded[4:6]) != formatVersion || binary.LittleEndian.Uint16(encoded[6:8]) != recordKindData || binary.LittleEndian.Uint16(encoded[10:12]) != recordHeaderSize {
		return Record{}, 0, &CorruptionError{Path: path, Offset: offset, LSN: lsn, Reason: "unsupported record format"}
	}
	bodyLength := binary.LittleEndian.Uint32(encoded[12:16])
	if bodyLength > maxPayloadSize || binary.LittleEndian.Uint32(encoded[16:20]) != ^bodyLength {
		return Record{}, 0, &CorruptionError{Path: path, Offset: offset, LSN: lsn, Reason: "invalid record length"}
	}
	if binary.LittleEndian.Uint32(encoded[44:48]) != crc32.Checksum(encoded[:44], crcTable) {
		return Record{}, 0, &CorruptionError{Path: path, Offset: offset, LSN: lsn, Reason: "record header checksum mismatch"}
	}
	total := recordHeaderSize + int(bodyLength) + recordTrailerSize
	if len(encoded) < total {
		return Record{}, 0, &CorruptionError{Path: path, Offset: offset, LSN: lsn, Reason: "incomplete record body"}
	}
	trailer := recordHeaderSize + int(bodyLength)
	if binary.LittleEndian.Uint32(encoded[trailer+4:trailer+8]) != uint32(total) || binary.LittleEndian.Uint32(encoded[trailer+8:trailer+12]) != recordTrailerMagic {
		return Record{}, 0, &CorruptionError{Path: path, Offset: offset, LSN: lsn, Reason: "invalid record trailer"}
	}
	checksum := crc32.New(crcTable)
	_, _ = checksum.Write(encoded[:44])
	_, _ = checksum.Write(encoded[recordHeaderSize:trailer])
	if binary.LittleEndian.Uint32(encoded[trailer:trailer+4]) != checksum.Sum32() {
		return Record{}, 0, &CorruptionError{Path: path, Offset: offset, LSN: lsn, Reason: "record checksum mismatch"}
	}
	var transactionID TransactionID
	copy(transactionID[:], encoded[28:44])
	return Record{LSN: lsn, TransactionID: transactionID, Payload: bytes.Clone(encoded[recordHeaderSize:trailer])}, total, nil
}

func segmentName(id uint64) string { return fmt.Sprintf("%020d.wal", id) }

func snapshotName(generation, throughLSN uint64) string {
	return fmt.Sprintf("snapshot-%020d-%020d.snap", generation, throughLSN)
}

func (journal *FileJournal) segmentPaths() ([]string, error) {
	entries, err := os.ReadDir(journal.walDir)
	if err != nil {
		return nil, fmt.Errorf("read WAL directory: %w", err)
	}
	paths := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".wal") {
			continue
		}
		name := strings.TrimSuffix(entry.Name(), ".wal")
		if len(name) != 20 {
			return nil, &CorruptionError{Path: filepath.Join(journal.walDir, entry.Name()), Reason: "invalid segment filename"}
		}
		if _, err := strconv.ParseUint(name, 10, 64); err != nil {
			return nil, &CorruptionError{Path: filepath.Join(journal.walDir, entry.Name()), Reason: "invalid segment filename"}
		}
		paths = append(paths, filepath.Join(journal.walDir, entry.Name()))
	}
	sort.Strings(paths)
	return paths, nil
}
