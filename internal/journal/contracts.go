package journal

import (
	"context"
	"time"
)

type TransactionID [16]byte

type Record struct {
	LSN           uint64
	TransactionID TransactionID
	Payload       []byte
}

type Snapshot struct {
	Generation uint64
	ThroughLSN uint64
	Payload    []byte
}

type Stats struct {
	DurableLSN         uint64
	WALBytes           int64
	SegmentCount       int
	SnapshotGeneration uint64
	LastSyncAt         *time.Time
	ReadOnly           bool
	ReadOnlyReason     string
}

type Journal interface {
	Append(context.Context, TransactionID, []byte) (uint64, error)
	AppendBatch(context.Context, []Record) ([]uint64, error)
	Checkpoint(context.Context, uint64, []byte) error
	Records() []Record
	Snapshot() Snapshot
	Stats() Stats
	Close() error
}
