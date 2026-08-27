package api

import (
	"encoding/json"
	"time"

	"github.com/ElijahUmana/queuemaxxing/internal/model"
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

type Problem struct {
	Type      string         `json:"type"`
	Title     string         `json:"title"`
	Status    int            `json:"status"`
	Code      string         `json:"code"`
	Detail    string         `json:"detail"`
	RequestID string         `json:"request_id"`
	Errors    []FieldProblem `json:"errors,omitempty"`
}

type FieldProblem struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

type CreateQueueRequest struct {
	Name                       string   `json:"name"`
	Ordering                   Ordering `json:"ordering"`
	PriorityEnabled            bool     `json:"priority_enabled"`
	DefaultDelayMS             int64    `json:"default_delay_ms"`
	DefaultVisibilityTimeoutMS int64    `json:"default_visibility_timeout_ms"`
	MaxDeliveries              uint32   `json:"max_deliveries"`
}

type QueueConfig struct {
	Name                       string    `json:"name"`
	Ordering                   Ordering  `json:"ordering"`
	PriorityEnabled            bool      `json:"priority_enabled"`
	DefaultDelayMS             int64     `json:"default_delay_ms"`
	DefaultVisibilityTimeoutMS int64     `json:"default_visibility_timeout_ms"`
	MaxDeliveries              uint32    `json:"max_deliveries"`
	CreatedAt                  time.Time `json:"created_at"`
}

type Counts struct {
	Ready    uint64 `json:"ready"`
	Delayed  uint64 `json:"delayed"`
	InFlight uint64 `json:"in_flight"`
	Dead     uint64 `json:"dead"`
	Acked    uint64 `json:"acked"`
	Total    uint64 `json:"total"`
}

type Queue struct {
	Config QueueConfig `json:"config"`
	Counts Counts      `json:"counts"`
}

type QueueList struct {
	Queues []Queue `json:"queues"`
}

type EnqueueRequest struct {
	Payload     json.RawMessage `json:"payload"`
	Priority    *int32          `json:"priority,omitempty"`
	DelayMS     *int64          `json:"delay_ms,omitempty"`
	AvailableAt *time.Time      `json:"available_at,omitempty"`
}

type ReceiveRequest struct {
	VisibilityTimeoutMS *int64 `json:"visibility_timeout_ms,omitempty"`
	WaitTimeoutMS       *int64 `json:"wait_timeout_ms,omitempty"`
}

type ReceiptRequest struct {
	ReceiptHandle string `json:"receipt_handle"`
}

type NackRequest struct {
	ReceiptHandle string `json:"receipt_handle"`
	RetryDelayMS  int64  `json:"retry_delay_ms"`
	Reason        string `json:"reason,omitempty"`
}

type ExtendRequest struct {
	ReceiptHandle       string `json:"receipt_handle"`
	VisibilityTimeoutMS int64  `json:"visibility_timeout_ms"`
}

type RedriveRequest struct {
	Priority    *int32     `json:"priority,omitempty"`
	DelayMS     *int64     `json:"delay_ms,omitempty"`
	AvailableAt *time.Time `json:"available_at,omitempty"`
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
	LeasedAt          *time.Time      `json:"leased_at,omitempty"`
	LeaseExpiresAt    *time.Time      `json:"lease_expires_at,omitempty"`
	LastFailureReason string          `json:"last_failure_reason,omitempty"`
	DeadAt            *time.Time      `json:"dead_at,omitempty"`
	ReplayOf          string          `json:"replay_of,omitempty"`
}

type Delivery struct {
	Message        Message   `json:"message"`
	ReceiptHandle  string    `json:"receipt_handle"`
	LeaseExpiresAt time.Time `json:"lease_expires_at"`
	DeliveryCount  uint32    `json:"delivery_count"`
}

type MutationResponse[T any] struct {
	Data     T    `json:"data"`
	Replayed bool `json:"replayed"`
}

type ReceiveResponse struct {
	Messages []Delivery `json:"messages"`
	PolledAt time.Time  `json:"polled_at"`
	Replayed bool       `json:"replayed"`
}

type AckResponse struct {
	MessageID string       `json:"message_id"`
	State     MessageState `json:"state"`
	Replayed  bool         `json:"replayed"`
}

type MessagePage struct {
	Messages    []Message `json:"messages"`
	NextCursor  string    `json:"next_cursor,omitempty"`
	SnapshotLSN uint64    `json:"snapshot_lsn,string"`
}

type RedriveResult struct {
	Source Message `json:"source"`
	Child  Message `json:"child"`
}

type ServiceStats struct {
	Queues             uint64     `json:"queues"`
	Messages           Counts     `json:"messages"`
	DurableLSN         uint64     `json:"durable_lsn,string"`
	WALBytes           int64      `json:"wal_bytes"`
	SnapshotGeneration uint64     `json:"snapshot_generation,string"`
	LastSyncAt         *time.Time `json:"last_sync_at,omitempty"`
	ReadOnly           bool       `json:"read_only"`
	ReadOnlyReason     string     `json:"read_only_reason,omitempty"`
}

type StatusResponse struct {
	Status string `json:"status"`
}

func queueFromModel(info model.QueueInfo) Queue {
	return Queue{
		Config: QueueConfig{
			Name: info.Config.Name, Ordering: Ordering(info.Config.Ordering), PriorityEnabled: info.Config.PriorityEnabled,
			DefaultDelayMS:             durationMS(info.Config.DefaultDelay),
			DefaultVisibilityTimeoutMS: durationMS(info.Config.DefaultVisibilityTimeout),
			MaxDeliveries:              info.Config.MaxDeliveries, CreatedAt: info.Config.CreatedAt,
		},
		Counts: countsFromModel(info.Counts),
	}
}

func countsFromModel(counts model.QueueCounts) Counts {
	return Counts{
		Ready: counts.Ready, Delayed: counts.Delayed, InFlight: counts.InFlight,
		Dead: counts.Dead, Acked: counts.Acked, Total: counts.Total,
	}
}

func messageFromModel(message model.Message) Message {
	return Message{
		ID: message.ID, Queue: message.Queue, Payload: message.Payload, Priority: message.Priority,
		Sequence: message.Sequence, EnqueuedAt: message.EnqueuedAt, AvailableAt: message.AvailableAt,
		State: MessageState(message.State), DeliveryCount: message.DeliveryCount, LeasedAt: message.LeasedAt,
		LeaseExpiresAt: message.LeaseUntil, LastFailureReason: message.LastFailureReason,
		DeadAt: message.DeadAt, ReplayOf: message.ReplayOf,
	}
}

func deliveryFromModel(delivery model.Delivery) Delivery {
	return Delivery{
		Message: messageFromModel(delivery.Message), ReceiptHandle: delivery.Receipt,
		LeaseExpiresAt: delivery.LeaseUntil, DeliveryCount: delivery.DeliveryCount,
	}
}

func statsFromModel(stats model.ServiceStats) ServiceStats {
	readOnlyReason := ""
	if stats.ReadOnly {
		readOnlyReason = "storage operation failed"
	}
	return ServiceStats{
		Queues: stats.Queues, Messages: countsFromModel(stats.Messages), DurableLSN: stats.DurableLSN,
		WALBytes: stats.WALBytes, SnapshotGeneration: stats.SnapshotGeneration,
		LastSyncAt: stats.LastSyncAt, ReadOnly: stats.ReadOnly, ReadOnlyReason: readOnlyReason,
	}
}

func durationMS(duration time.Duration) int64 {
	return duration.Milliseconds()
}
