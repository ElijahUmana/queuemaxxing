package model

import (
	"encoding/json"
	"time"
)

type Ordering string

const (
	FIFO Ordering = "fifo"
	LIFO Ordering = "lifo"
)

type MessageState string

const (
	StateDelayed MessageState = "delayed"
	StateReady   MessageState = "ready"
	StateLeased  MessageState = "leased"
	StateAcked   MessageState = "acked"
	StateDead    MessageState = "dead"
)

type QueueConfig struct {
	Name                     string        `json:"name"`
	Ordering                 Ordering      `json:"ordering"`
	PriorityEnabled          bool          `json:"priority_enabled"`
	DefaultDelay             time.Duration `json:"default_delay"`
	DefaultVisibilityTimeout time.Duration `json:"default_visibility_timeout"`
	MaxDeliveries            uint32        `json:"max_deliveries"`
	CreatedAt                time.Time     `json:"created_at"`
}

type Message struct {
	ID                string          `json:"id"`
	Queue             string          `json:"queue"`
	Payload           json.RawMessage `json:"payload"`
	Priority          int32           `json:"priority"`
	Sequence          uint64          `json:"sequence,string"`
	EnqueuedAt        time.Time       `json:"enqueued_at"`
	AvailableAt       time.Time       `json:"available_at"`
	State             MessageState    `json:"state"`
	DeliveryCount     uint32          `json:"delivery_count"`
	LeaseEpoch        uint64          `json:"lease_epoch,string"`
	LeaseToken        string          `json:"-"`
	LeasedAt          *time.Time      `json:"leased_at,omitempty"`
	LeaseUntil        *time.Time      `json:"lease_until,omitempty"`
	LastFailureReason string          `json:"last_failure_reason,omitempty"`
	DeadAt            *time.Time      `json:"dead_at,omitempty"`
	ReplayOf          string          `json:"replay_of,omitempty"`
	LastLSN           uint64          `json:"last_lsn,string"`
}

type Delivery struct {
	Message       Message   `json:"message"`
	Receipt       string    `json:"receipt"`
	LeaseUntil    time.Time `json:"lease_until"`
	DeliveryCount uint32    `json:"delivery_count"`
}

type QueueCounts struct {
	Ready    uint64 `json:"ready"`
	Delayed  uint64 `json:"delayed"`
	InFlight uint64 `json:"in_flight"`
	Dead     uint64 `json:"dead"`
	Acked    uint64 `json:"acked"`
	Total    uint64 `json:"total"`
}

type QueueInfo struct {
	Config QueueConfig `json:"config"`
	Counts QueueCounts `json:"counts"`
}

type EnqueueRequest struct {
	Payload        json.RawMessage `json:"payload"`
	Priority       *int32          `json:"priority,omitempty"`
	Delay          *time.Duration  `json:"delay,omitempty"`
	AvailableAt    *time.Time      `json:"available_at,omitempty"`
	IdempotencyKey string          `json:"-"`
}

type ReceiveRequest struct {
	VisibilityTimeout time.Duration `json:"visibility_timeout"`
	WaitTimeout       time.Duration `json:"wait_timeout"`
	IdempotencyKey    string        `json:"-"`
}

type AckRequest struct {
	MessageID      string `json:"message_id"`
	Receipt        string `json:"receipt"`
	IdempotencyKey string `json:"-"`
}

type NackRequest struct {
	MessageID      string        `json:"message_id"`
	Receipt        string        `json:"receipt"`
	Delay          time.Duration `json:"delay"`
	Reason         string        `json:"reason,omitempty"`
	IdempotencyKey string        `json:"-"`
}

type ExtendRequest struct {
	MessageID         string        `json:"message_id"`
	Receipt           string        `json:"receipt"`
	VisibilityTimeout time.Duration `json:"visibility_timeout"`
	IdempotencyKey    string        `json:"-"`
}

type RedriveRequest struct {
	MessageID      string        `json:"message_id"`
	Priority       *int32        `json:"priority,omitempty"`
	Delay          time.Duration `json:"delay"`
	AvailableAt    *time.Time    `json:"available_at,omitempty"`
	IdempotencyKey string        `json:"-"`
}

type RedriveResult struct {
	Source Message `json:"source"`
	Child  Message `json:"child"`
}

type ListFilter struct {
	State  MessageState
	Limit  int
	Cursor string
}

type MessagePage struct {
	Messages    []Message `json:"messages"`
	NextCursor  string    `json:"next_cursor,omitempty"`
	SnapshotLSN uint64    `json:"snapshot_lsn,string"`
}

type ServiceStats struct {
	Queues             uint64      `json:"queues"`
	Messages           QueueCounts `json:"messages"`
	DurableLSN         uint64      `json:"durable_lsn,string"`
	WALBytes           int64       `json:"wal_bytes"`
	SnapshotGeneration uint64      `json:"snapshot_generation,string"`
	LastSyncAt         *time.Time  `json:"last_sync_at,omitempty"`
	ReadOnly           bool        `json:"read_only"`
	ReadOnlyReason     string      `json:"read_only_reason,omitempty"`
}
