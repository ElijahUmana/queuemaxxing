package engine

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	queueclock "github.com/ElijahUmana/queuemaxxing/internal/clock"
	"github.com/ElijahUmana/queuemaxxing/internal/journal"
	"github.com/ElijahUmana/queuemaxxing/internal/model"
)

const stateVersion = 1

const (
	mutationQueueCapacity = 256
	maxMutationBatch      = 64
	maxMutationBatchBytes = 8 << 20
	mutationBatchDelay    = 250 * time.Microsecond
)

const (
	operationCreateQueue = "create_queue"
	operationEnqueue     = "enqueue"
	operationReceive     = "receive"
	operationAck         = "ack"
	operationNack        = "nack"
	operationExtend      = "extend"
	operationRedrive     = "redrive"
)

type Limits struct {
	MaxQueues              int
	MaxMessages            int
	MaxMessagesPerQueue    int
	MaxPayloadBytes        int
	MaxIdempotencyRecords  int
	MaxIdempotencyKeyBytes int
	MaxWaitTimeout         time.Duration
	MaxVisibilityTimeout   time.Duration
	MaxDelay               time.Duration
	MaxWaiters             int
	MaxWaitersPerQueue     int
	MaxInFlight            int
	MaxInFlightPerQueue    int
	IdempotencyRetention   time.Duration
	AckTombstoneRetention  time.Duration
	MaxListLimit           int
}

func DefaultLimits() Limits {
	return Limits{
		MaxQueues:              1_000,
		MaxMessages:            1_000_000,
		MaxMessagesPerQueue:    100_000,
		MaxPayloadBytes:        1 << 20,
		MaxIdempotencyRecords:  1_000_000,
		MaxIdempotencyKeyBytes: 256,
		MaxWaitTimeout:         30 * time.Second,
		MaxVisibilityTimeout:   12 * time.Hour,
		MaxDelay:               30 * 24 * time.Hour,
		MaxWaiters:             10_000,
		MaxWaitersPerQueue:     1_000,
		MaxInFlight:            100_000,
		MaxInFlightPerQueue:    10_000,
		IdempotencyRetention:   24 * time.Hour,
		AckTombstoneRetention:  24 * time.Hour,
		MaxListLimit:           1_000,
	}
}

type Options struct {
	Limits Limits
}

type persistedState struct {
	Version      int                          `json:"version"`
	NextSequence uint64                       `json:"next_sequence"`
	Queues       map[string]*queueState       `json:"queues"`
	Idempotency  map[string]idempotencyRecord `json:"idempotency"`
}

type queueState struct {
	Config        model.QueueConfig         `json:"config"`
	Messages      map[string]*model.Message `json:"messages"`
	Receipts      map[string]string         `json:"receipts"`
	AckedAt       map[string]time.Time      `json:"acked_at"`
	AckedReceipts map[string]ackReceipt     `json:"acked_receipts"`
}

type ackReceipt struct {
	MessageID string    `json:"message_id"`
	ExpiresAt time.Time `json:"expires_at"`
}

type idempotencyRecord struct {
	Operation   string          `json:"operation"`
	Queue       string          `json:"queue"`
	Key         string          `json:"key"`
	Fingerprint string          `json:"fingerprint"`
	Result      json.RawMessage `json:"result"`
	CreatedAt   time.Time       `json:"created_at"`
	ExpiresAt   time.Time       `json:"expires_at"`
	LastLSN     uint64          `json:"last_lsn,omitempty"`
}

type persistedEnvelope struct {
	Kind    string          `json:"kind"`
	Version int             `json:"version"`
	State   json.RawMessage `json:"state,omitempty"`
	Delta   *stateDelta     `json:"delta,omitempty"`
}

type stateDelta struct {
	NextSequence      uint64                               `json:"next_sequence"`
	UpsertQueues      map[string]*queueState               `json:"upsert_queues,omitempty"`
	DeleteQueues      []string                             `json:"delete_queues,omitempty"`
	UpsertMessages    map[string]map[string]*model.Message `json:"upsert_messages,omitempty"`
	DeleteMessages    map[string][]string                  `json:"delete_messages,omitempty"`
	Receipts          map[string]map[string]*string        `json:"receipts,omitempty"`
	AckedAt           map[string]map[string]*time.Time     `json:"acked_at,omitempty"`
	AckedReceipts     map[string]map[string]*ackReceipt    `json:"acked_receipts,omitempty"`
	UpsertIdempotency map[string]idempotencyRecord         `json:"upsert_idempotency,omitempty"`
	DeleteIdempotency []string                             `json:"delete_idempotency,omitempty"`
}

type mutationResult struct {
	lsn uint64
	err error
}

type mutationRequest struct {
	ctx       context.Context
	queueName string
	operation string
	key       string
	reset     func()
	mutation  func() error
	result    chan mutationResult
}

type service struct {
	mu              sync.Mutex
	submitMu        sync.RWMutex
	journal         journal.Journal
	clock           queueclock.Clock
	limits          Limits
	state           persistedState
	wake            chan struct{}
	mutationCh      chan mutationRequest
	mutationStop    chan struct{}
	mutationDone    chan struct{}
	closeDone       chan struct{}
	closeOnce       sync.Once
	closeErr        error
	stopping        bool
	closing         bool
	closed          bool
	totalMessages   int
	totalWaiters    int
	waitersByQueue  map[string]int
	totalInFlight   int
	inFlightByQueue map[string]int
}

func New(store journal.Journal, serviceClock queueclock.Clock, options Options) (Service, error) {
	if store == nil {
		return nil, invalid("journal is required")
	}
	if serviceClock == nil {
		serviceClock = queueclock.Real{}
	}
	limits := options.Limits
	defaults := DefaultLimits()
	mergeLimits(&limits, defaults)
	if err := validateLimits(limits); err != nil {
		return nil, err
	}

	instance := &service{
		journal:         store,
		clock:           serviceClock,
		limits:          limits,
		wake:            make(chan struct{}),
		mutationCh:      make(chan mutationRequest, mutationQueueCapacity),
		mutationStop:    make(chan struct{}),
		mutationDone:    make(chan struct{}),
		closeDone:       make(chan struct{}),
		waitersByQueue:  make(map[string]int),
		inFlightByQueue: make(map[string]int),
		state: persistedState{
			Version:      stateVersion,
			NextSequence: 1,
			Queues:       make(map[string]*queueState),
			Idempotency:  make(map[string]idempotencyRecord),
		},
	}
	if err := instance.recover(); err != nil {
		return nil, &Error{Code: CodeStorageUnavailable, Message: "recover queue state", Cause: err}
	}
	go instance.runMutationCoordinator()
	return instance, nil
}

func mergeLimits(limits *Limits, defaults Limits) {
	if limits.MaxQueues == 0 {
		limits.MaxQueues = defaults.MaxQueues
	}
	if limits.MaxMessages == 0 {
		limits.MaxMessages = defaults.MaxMessages
	}
	if limits.MaxMessagesPerQueue == 0 {
		limits.MaxMessagesPerQueue = defaults.MaxMessagesPerQueue
	}
	if limits.MaxPayloadBytes == 0 {
		limits.MaxPayloadBytes = defaults.MaxPayloadBytes
	}
	if limits.MaxIdempotencyRecords == 0 {
		limits.MaxIdempotencyRecords = defaults.MaxIdempotencyRecords
	}
	if limits.MaxIdempotencyKeyBytes == 0 {
		limits.MaxIdempotencyKeyBytes = defaults.MaxIdempotencyKeyBytes
	}
	if limits.MaxWaitTimeout == 0 {
		limits.MaxWaitTimeout = defaults.MaxWaitTimeout
	}
	if limits.MaxVisibilityTimeout == 0 {
		limits.MaxVisibilityTimeout = defaults.MaxVisibilityTimeout
	}
	if limits.MaxDelay == 0 {
		limits.MaxDelay = defaults.MaxDelay
	}
	if limits.MaxWaiters == 0 {
		limits.MaxWaiters = defaults.MaxWaiters
	}
	if limits.MaxWaitersPerQueue == 0 {
		limits.MaxWaitersPerQueue = defaults.MaxWaitersPerQueue
	}
	if limits.MaxInFlight == 0 {
		limits.MaxInFlight = defaults.MaxInFlight
	}
	if limits.MaxInFlightPerQueue == 0 {
		limits.MaxInFlightPerQueue = defaults.MaxInFlightPerQueue
	}
	if limits.IdempotencyRetention == 0 {
		limits.IdempotencyRetention = defaults.IdempotencyRetention
	}
	if limits.AckTombstoneRetention == 0 {
		limits.AckTombstoneRetention = defaults.AckTombstoneRetention
	}
	if limits.MaxListLimit == 0 {
		limits.MaxListLimit = defaults.MaxListLimit
	}
}

func validateLimits(limits Limits) error {
	if limits.MaxQueues < 1 || limits.MaxMessages < 1 || limits.MaxMessagesPerQueue < 1 ||
		limits.MaxPayloadBytes < 1 || limits.MaxIdempotencyRecords < 1 || limits.MaxIdempotencyKeyBytes < 1 ||
		limits.MaxWaitTimeout < 0 || limits.MaxVisibilityTimeout <= 0 || limits.MaxDelay < 0 ||
		limits.MaxWaiters < 1 || limits.MaxWaitersPerQueue < 1 || limits.MaxInFlight < 1 || limits.MaxInFlightPerQueue < 1 ||
		limits.IdempotencyRetention <= 0 || limits.AckTombstoneRetention <= 0 || limits.MaxListLimit < 1 {
		return invalid("all engine limits must be positive except wait and delay limits, which may be zero")
	}
	return nil
}

func (s *service) recover() error {
	if snapshot := s.journal.Snapshot(); len(snapshot.Payload) > 0 {
		if err := s.applyEnvelope(snapshot.Payload, snapshot.ThroughLSN); err != nil {
			return fmt.Errorf("apply snapshot through LSN %d: %w", snapshot.ThroughLSN, err)
		}
	}
	for _, record := range s.journal.Records() {
		if len(record.Payload) > 0 {
			if err := s.applyEnvelope(record.Payload, record.LSN); err != nil {
				return fmt.Errorf("apply WAL record %d: %w", record.LSN, err)
			}
		}
	}
	return s.validateRecoveredState()
}

func (s *service) applyEnvelope(payload []byte, lsn uint64) error {
	var envelope persistedEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return fmt.Errorf("decode persisted envelope: %w", err)
	}
	if envelope.Version != stateVersion {
		return fmt.Errorf("unsupported persisted version %d", envelope.Version)
	}
	switch envelope.Kind {
	case "state":
		if envelope.Delta != nil || len(envelope.State) == 0 {
			return fmt.Errorf("state envelope has invalid structure")
		}
		var recovered persistedState
		if err := json.Unmarshal(envelope.State, &recovered); err != nil {
			return fmt.Errorf("decode persisted state: %w", err)
		}
		if recovered.Version != stateVersion {
			return fmt.Errorf("unsupported state version %d", recovered.Version)
		}
		if err := validatePersistedState(recovered, lsn); err != nil {
			return err
		}
		s.state = recovered
	case "delta":
		if envelope.Delta == nil || len(envelope.State) != 0 {
			return fmt.Errorf("delta envelope has invalid structure")
		}
		if err := validateStateDelta(s.state, *envelope.Delta, lsn); err != nil {
			return err
		}
		applyDelta(&s.state, *envelope.Delta)
		stampDeltaLSN(&s.state, *envelope.Delta, lsn)
	default:
		return fmt.Errorf("unsupported persisted kind %q", envelope.Kind)
	}
	return nil
}

func (s *service) validateRecoveredState() error {
	if s.state.Queues == nil {
		s.state.Queues = make(map[string]*queueState)
	}
	if s.state.Idempotency == nil {
		s.state.Idempotency = make(map[string]idempotencyRecord)
	}
	if s.state.NextSequence == 0 {
		s.state.NextSequence = 1
	}
	if s.inFlightByQueue == nil {
		s.inFlightByQueue = make(map[string]int)
	} else {
		clear(s.inFlightByQueue)
	}
	if s.waitersByQueue == nil {
		s.waitersByQueue = make(map[string]int)
	}
	throughLSN := uint64(0)
	if s.journal != nil {
		throughLSN = s.journal.Stats().DurableLSN
	}
	if err := validatePersistedState(s.state, throughLSN); err != nil {
		return err
	}
	if len(s.state.Queues) > s.limits.MaxQueues || len(s.state.Idempotency) > s.limits.MaxIdempotencyRecords {
		return fmt.Errorf("recovered state exceeds configured capacity")
	}
	for id, record := range s.state.Idempotency {
		if len(record.Key) > s.limits.MaxIdempotencyKeyBytes {
			return fmt.Errorf("idempotency record %q key exceeds configured capacity", id)
		}
	}
	s.totalMessages = 0
	s.totalInFlight = 0
	for name, queue := range s.state.Queues {
		if queue.Config.DefaultDelay > s.limits.MaxDelay || queue.Config.DefaultVisibilityTimeout > s.limits.MaxVisibilityTimeout {
			return fmt.Errorf("queue %q configuration exceeds current limits", name)
		}
		if queue.Messages == nil {
			queue.Messages = make(map[string]*model.Message)
		}
		if queue.Receipts == nil {
			queue.Receipts = make(map[string]string)
		}
		if queue.AckedAt == nil {
			queue.AckedAt = make(map[string]time.Time)
		}
		if queue.AckedReceipts == nil {
			queue.AckedReceipts = make(map[string]ackReceipt)
		}
		if len(queue.Messages) > s.limits.MaxMessagesPerQueue {
			return fmt.Errorf("queue %q exceeds configured message capacity", name)
		}
		s.totalMessages += len(queue.Messages)
		for _, message := range queue.Messages {
			if len(message.Payload) > s.limits.MaxPayloadBytes {
				return fmt.Errorf("queue %q message %q exceeds configured payload capacity", name, message.ID)
			}
			if message.State == model.StateLeased {
				s.totalInFlight++
				s.inFlightByQueue[name]++
			}
		}
	}
	if s.clock != nil {
		now := s.clock.Now()
		for _, queue := range s.state.Queues {
			s.materializeLocked(queue, now)
		}
	}
	for name := range s.state.Queues {
		if s.inFlightByQueue[name] > s.limits.MaxInFlightPerQueue {
			return fmt.Errorf("queue %q exceeds configured in-flight capacity", name)
		}
	}
	if s.totalMessages > s.limits.MaxMessages || s.totalInFlight > s.limits.MaxInFlight {
		return fmt.Errorf("recovered state exceeds configured capacity")
	}
	return nil
}

func validIdempotencyOperation(operation string) bool {
	switch operation {
	case operationCreateQueue, operationEnqueue, operationReceive, operationAck, operationNack, operationExtend, operationRedrive:
		return true
	default:
		return false
	}
}

func validFingerprint(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func validReceipt(receipt, messageID string, epoch uint64) bool {
	parts := strings.Split(receipt, ".")
	if len(parts) != 3 || parts[0] != messageID || parts[1] != strconv.FormatUint(epoch, 10) || len(parts[2]) != 32 {
		return false
	}
	_, err := hex.DecodeString(parts[2])
	return err == nil
}

func validIdempotencyResult(operation string, result json.RawMessage) bool {
	switch operation {
	case operationCreateQueue:
		var value queueMutationResult
		return json.Unmarshal(result, &value) == nil && value.Info.Config.Name != ""
	case operationEnqueue:
		var value enqueueMutationResult
		return json.Unmarshal(result, &value) == nil && value.Message.ID != ""
	case operationReceive:
		var value receiveMutationResult
		return json.Unmarshal(result, &value) == nil
	case operationAck:
		var value struct {
			Acked bool `json:"acked"`
		}
		return json.Unmarshal(result, &value) == nil && value.Acked
	case operationNack:
		var value nackMutationResult
		return json.Unmarshal(result, &value) == nil && value.Message.ID != ""
	case operationExtend:
		var value extendMutationResult
		return json.Unmarshal(result, &value) == nil && value.Delivery.Message.ID != ""
	case operationRedrive:
		var value redriveMutationResult
		return json.Unmarshal(result, &value) == nil && value.Result.Source.ID != "" && value.Result.Child.ID != ""
	default:
		return false
	}
}

func validatePersistedState(state persistedState, throughLSN uint64) error {
	if state.Version != stateVersion {
		return fmt.Errorf("unsupported state version %d", state.Version)
	}
	if state.NextSequence == 0 {
		return fmt.Errorf("next sequence must be positive")
	}
	sequences := make(map[uint64]string)
	receipts := make(map[string]string)
	for name, queue := range state.Queues {
		if queue == nil {
			return fmt.Errorf("queue %q has nil state", name)
		}
		if name == "" || queue.Config.Name != name {
			return fmt.Errorf("queue map key %q does not match configured name %q", name, queue.Config.Name)
		}
		if err := validateQueueName(name); err != nil {
			return fmt.Errorf("queue %q has invalid name: %w", name, err)
		}
		if queue.Config.Ordering != model.FIFO && queue.Config.Ordering != model.LIFO {
			return fmt.Errorf("queue %q has invalid ordering %q", name, queue.Config.Ordering)
		}
		if queue.Config.DefaultDelay < 0 || queue.Config.DefaultVisibilityTimeout <= 0 || queue.Config.MaxDeliveries == 0 || queue.Config.CreatedAt.IsZero() {
			return fmt.Errorf("queue %q has invalid configuration", name)
		}
		for id, message := range queue.Messages {
			if message == nil {
				return fmt.Errorf("queue %q message %q has nil state", name, id)
			}
			if id == "" || message.ID != id || message.Queue != name {
				return fmt.Errorf("queue %q message map key %q has inconsistent identity", name, id)
			}
			if !json.Valid(message.Payload) || message.Sequence == 0 || message.Sequence >= state.NextSequence || message.EnqueuedAt.IsZero() || message.AvailableAt.IsZero() {
				return fmt.Errorf("queue %q message %q has invalid immutable fields", name, id)
			}
			if other, exists := sequences[message.Sequence]; exists {
				return fmt.Errorf("messages %q and %q share sequence %d", other, id, message.Sequence)
			}
			sequences[message.Sequence] = id
			if message.LastLSN == 0 || message.LastLSN > throughLSN {
				return fmt.Errorf("queue %q message %q has invalid last LSN %d through %d", name, id, message.LastLSN, throughLSN)
			}
			receipt, hasReceipt := queue.Receipts[id]
			switch message.State {
			case model.StateDelayed, model.StateReady:
				if message.DeliveryCount >= queue.Config.MaxDeliveries || message.LeaseEpoch != uint64(message.DeliveryCount) || message.DeadAt != nil {
					return fmt.Errorf("queue %q message %q has invalid deliverable state", name, id)
				}
			case model.StateLeased:
				if !hasReceipt || !validReceipt(receipt, id, message.LeaseEpoch) || message.DeliveryCount == 0 || message.DeliveryCount > queue.Config.MaxDeliveries || message.LeaseEpoch != uint64(message.DeliveryCount) || message.LeasedAt == nil || message.LeaseUntil == nil || message.LeasedAt.Before(message.EnqueuedAt) || !message.LeasedAt.Before(*message.LeaseUntil) || message.DeadAt != nil {
					return fmt.Errorf("queue %q has incomplete lease for message %q", name, id)
				}
				if other, exists := receipts[receipt]; exists {
					return fmt.Errorf("messages %q and %q share receipt", other, id)
				}
				receipts[receipt] = id
			case model.StateAcked:
				ackedAt, exists := queue.AckedAt[id]
				if !exists || ackedAt.IsZero() || ackedAt.Before(message.EnqueuedAt) || message.DeliveryCount == 0 || message.DeliveryCount > queue.Config.MaxDeliveries || message.LeaseEpoch != uint64(message.DeliveryCount) || message.DeadAt != nil {
					return fmt.Errorf("queue %q message %q has invalid acknowledged state", name, id)
				}
			case model.StateDead:
				if message.DeliveryCount != queue.Config.MaxDeliveries || message.LeaseEpoch != uint64(message.DeliveryCount) || message.DeadAt == nil || message.DeadAt.IsZero() || message.DeadAt.Before(message.EnqueuedAt) {
					return fmt.Errorf("queue %q message %q has invalid dead-letter state", name, id)
				}
			default:
				return fmt.Errorf("queue %q message %q has invalid state %q", name, id, message.State)
			}
			if message.State != model.StateLeased && (hasReceipt || message.LeasedAt != nil || message.LeaseUntil != nil || message.LeaseToken != "") {
				return fmt.Errorf("queue %q has lease data on non-leased message %q", name, id)
			}
		}
		for id, receipt := range queue.Receipts {
			message := queue.Messages[id]
			if message == nil || message.State != model.StateLeased || receipt == "" {
				return fmt.Errorf("queue %q has invalid receipt for message %q", name, id)
			}
		}
		for id, ackedAt := range queue.AckedAt {
			message := queue.Messages[id]
			if message == nil || message.State != model.StateAcked || ackedAt.IsZero() {
				return fmt.Errorf("queue %q has invalid acknowledged timestamp for message %q", name, id)
			}
		}
		ackedReceiptCounts := make(map[string]int)
		for receipt, record := range queue.AckedReceipts {
			message := queue.Messages[record.MessageID]
			ackedAt, acked := queue.AckedAt[record.MessageID]
			if receipt == "" || record.MessageID == "" || record.ExpiresAt.IsZero() || message == nil || message.State != model.StateAcked || !acked || !validReceipt(receipt, record.MessageID, message.LeaseEpoch) || !record.ExpiresAt.After(ackedAt) {
				return fmt.Errorf("queue %q has invalid acknowledged receipt", name)
			}
			if other, exists := receipts[receipt]; exists {
				return fmt.Errorf("messages %q and %q share receipt", other, record.MessageID)
			}
			receipts[receipt] = record.MessageID
			ackedReceiptCounts[record.MessageID]++
		}
		for id, message := range queue.Messages {
			if message.State == model.StateAcked && ackedReceiptCounts[id] != 1 {
				return fmt.Errorf("queue %q message %q has invalid acknowledged receipt count", name, id)
			}
		}
	}
	for id, record := range state.Idempotency {
		if id == "" || id != idempotencyID(record.Operation, record.Queue, record.Key) || !validIdempotencyOperation(record.Operation) || record.Queue == "" || record.Key == "" || !validFingerprint(record.Fingerprint) || !json.Valid(record.Result) || !validIdempotencyResult(record.Operation, record.Result) || record.CreatedAt.IsZero() || !record.ExpiresAt.After(record.CreatedAt) || record.LastLSN == 0 || record.LastLSN > throughLSN {
			return fmt.Errorf("invalid idempotency record %q", id)
		}
		if _, exists := state.Queues[record.Queue]; !exists {
			return fmt.Errorf("idempotency record %q references unknown queue %q", id, record.Queue)
		}
	}
	return nil
}

func validateStateDelta(state persistedState, delta stateDelta, lsn uint64) error {
	if delta.NextSequence != 0 && delta.NextSequence < state.NextSequence {
		return fmt.Errorf("delta next sequence regresses from %d to %d", state.NextSequence, delta.NextSequence)
	}
	deletedQueues := make(map[string]struct{}, len(delta.DeleteQueues))
	for _, name := range delta.DeleteQueues {
		if _, exists := state.Queues[name]; !exists {
			return fmt.Errorf("delta deletes unknown queue %q", name)
		}
		if _, exists := deletedQueues[name]; exists {
			return fmt.Errorf("delta deletes queue %q more than once", name)
		}
		deletedQueues[name] = struct{}{}
	}
	for name, queue := range delta.UpsertQueues {
		if queue == nil {
			return fmt.Errorf("delta upserts nil queue %q", name)
		}
		if _, deleted := deletedQueues[name]; deleted {
			return fmt.Errorf("delta both deletes and upserts queue %q", name)
		}
	}
	for name, messages := range delta.UpsertMessages {
		if state.Queues[name] == nil && delta.UpsertQueues[name] == nil {
			return fmt.Errorf("delta messages reference unknown queue %q", name)
		}
		for id, message := range messages {
			if message == nil || id == "" {
				return fmt.Errorf("delta upserts invalid message %q in queue %q", id, name)
			}
		}
	}
	for name := range delta.DeleteMessages {
		if state.Queues[name] == nil {
			return fmt.Errorf("delta deletes messages from unknown queue %q", name)
		}
	}
	for _, changes := range []any{delta.Receipts, delta.AckedAt, delta.AckedReceipts} {
		switch entries := changes.(type) {
		case map[string]map[string]*string:
			for name := range entries {
				if state.Queues[name] == nil && delta.UpsertQueues[name] == nil {
					return fmt.Errorf("delta receipt references unknown queue %q", name)
				}
			}
		case map[string]map[string]*time.Time:
			for name := range entries {
				if state.Queues[name] == nil && delta.UpsertQueues[name] == nil {
					return fmt.Errorf("delta timestamp references unknown queue %q", name)
				}
			}
		case map[string]map[string]*ackReceipt:
			for name := range entries {
				if state.Queues[name] == nil && delta.UpsertQueues[name] == nil {
					return fmt.Errorf("delta acknowledged receipt references unknown queue %q", name)
				}
			}
		}
	}
	clone, err := cloneStateForCheckpoint(state)
	if err != nil {
		return fmt.Errorf("clone state for delta validation: %w", err)
	}
	applyDelta(&clone, delta)
	stampDeltaLSN(&clone, delta, lsn)
	if err := validatePersistedState(clone, lsn); err != nil {
		return fmt.Errorf("invalid recovered delta: %w", err)
	}
	return nil
}

func stampDeltaLSN(state *persistedState, delta stateDelta, lsn uint64) {
	for queueName, messages := range delta.UpsertMessages {
		queue := state.Queues[queueName]
		for id := range messages {
			queue.Messages[id].LastLSN = lsn
		}
	}
	for id := range delta.UpsertIdempotency {
		record := state.Idempotency[id]
		record.LastLSN = lsn
		state.Idempotency[id] = record
	}
}

func encodeDeltaRecord(delta stateDelta) (journal.Record, int, error) {
	for queueName, messages := range delta.UpsertMessages {
		copies := make(map[string]*model.Message, len(messages))
		for id, message := range messages {
			copy := cloneMessage(message)
			copy.LastLSN = 0
			copies[id] = &copy
		}
		delta.UpsertMessages[queueName] = copies
	}
	for id, record := range delta.UpsertIdempotency {
		record.LastLSN = 0
		delta.UpsertIdempotency[id] = record
	}
	envelopeBytes, err := json.Marshal(persistedEnvelope{Kind: "delta", Version: stateVersion, Delta: &delta})
	if err != nil {
		return journal.Record{}, 0, fmt.Errorf("encode state delta: %w", err)
	}
	var transactionID journal.TransactionID
	if _, err := rand.Read(transactionID[:]); err != nil {
		return journal.Record{}, 0, fmt.Errorf("generate transaction id: %w", err)
	}
	return journal.Record{TransactionID: transactionID, Payload: envelopeBytes}, len(envelopeBytes), nil
}

func diffState(before, after persistedState) stateDelta {
	delta := stateDelta{NextSequence: after.NextSequence}
	for name, queue := range after.Queues {
		oldQueue, existed := before.Queues[name]
		if !existed {
			if delta.UpsertQueues == nil {
				delta.UpsertQueues = make(map[string]*queueState)
			}
			delta.UpsertQueues[name] = queue
			continue
		}
		if !reflect.DeepEqual(oldQueue.Config, queue.Config) {
			if delta.UpsertQueues == nil {
				delta.UpsertQueues = make(map[string]*queueState)
			}
			delta.UpsertQueues[name] = &queueState{Config: queue.Config}
		}
		for id, message := range queue.Messages {
			if old, ok := oldQueue.Messages[id]; !ok || !reflect.DeepEqual(old, message) {
				if delta.UpsertMessages == nil {
					delta.UpsertMessages = make(map[string]map[string]*model.Message)
				}
				if delta.UpsertMessages[name] == nil {
					delta.UpsertMessages[name] = make(map[string]*model.Message)
				}
				delta.UpsertMessages[name][id] = message
			}
		}
		for id := range oldQueue.Messages {
			if _, ok := queue.Messages[id]; !ok {
				delta.DeleteMessages = appendMapSlice(delta.DeleteMessages, name, id)
			}
		}
		delta.Receipts = diffStringMap(delta.Receipts, name, oldQueue.Receipts, queue.Receipts)
		delta.AckedAt = diffTimeMap(delta.AckedAt, name, oldQueue.AckedAt, queue.AckedAt)
		delta.AckedReceipts = diffAckMap(delta.AckedReceipts, name, oldQueue.AckedReceipts, queue.AckedReceipts)
	}
	for name := range before.Queues {
		if _, ok := after.Queues[name]; !ok {
			delta.DeleteQueues = append(delta.DeleteQueues, name)
		}
	}
	for id, record := range after.Idempotency {
		if old, ok := before.Idempotency[id]; !ok || !reflect.DeepEqual(old, record) {
			if delta.UpsertIdempotency == nil {
				delta.UpsertIdempotency = make(map[string]idempotencyRecord)
			}
			delta.UpsertIdempotency[id] = record
		}
	}
	for id := range before.Idempotency {
		if _, ok := after.Idempotency[id]; !ok {
			delta.DeleteIdempotency = append(delta.DeleteIdempotency, id)
		}
	}
	return delta
}

func applyDelta(state *persistedState, delta stateDelta) {
	if delta.NextSequence != 0 {
		state.NextSequence = delta.NextSequence
	}
	if state.Queues == nil {
		state.Queues = make(map[string]*queueState)
	}
	if state.Idempotency == nil {
		state.Idempotency = make(map[string]idempotencyRecord)
	}
	for _, name := range delta.DeleteQueues {
		delete(state.Queues, name)
	}
	for name, incoming := range delta.UpsertQueues {
		queue := state.Queues[name]
		if queue == nil || incoming.Messages != nil {
			state.Queues[name] = incoming
		} else {
			queue.Config = incoming.Config
		}
	}
	for queueName, messages := range delta.UpsertMessages {
		queue := state.Queues[queueName]
		if queue.Messages == nil {
			queue.Messages = make(map[string]*model.Message)
		}
		for id, message := range messages {
			queue.Messages[id] = message
		}
	}
	for queueName, ids := range delta.DeleteMessages {
		for _, id := range ids {
			delete(state.Queues[queueName].Messages, id)
		}
	}
	applyStringPointers(state, delta.Receipts)
	applyTimePointers(state, delta.AckedAt)
	applyAckPointers(state, delta.AckedReceipts)
	for _, id := range delta.DeleteIdempotency {
		delete(state.Idempotency, id)
	}
	for id, record := range delta.UpsertIdempotency {
		state.Idempotency[id] = record
	}
}

func appendMapSlice(target map[string][]string, key, value string) map[string][]string {
	if target == nil {
		target = make(map[string][]string)
	}
	target[key] = append(target[key], value)
	return target
}

func diffStringMap(target map[string]map[string]*string, queue string, before, after map[string]string) map[string]map[string]*string {
	for key, value := range after {
		if old, ok := before[key]; !ok || old != value {
			if target == nil {
				target = make(map[string]map[string]*string)
			}
			if target[queue] == nil {
				target[queue] = make(map[string]*string)
			}
			copy := value
			target[queue][key] = &copy
		}
	}
	for key := range before {
		if _, ok := after[key]; !ok {
			if target == nil {
				target = make(map[string]map[string]*string)
			}
			if target[queue] == nil {
				target[queue] = make(map[string]*string)
			}
			target[queue][key] = nil
		}
	}
	return target
}

func diffTimeMap(target map[string]map[string]*time.Time, queue string, before, after map[string]time.Time) map[string]map[string]*time.Time {
	for key, value := range after {
		if old, ok := before[key]; !ok || !old.Equal(value) {
			if target == nil {
				target = make(map[string]map[string]*time.Time)
			}
			if target[queue] == nil {
				target[queue] = make(map[string]*time.Time)
			}
			copy := value
			target[queue][key] = &copy
		}
	}
	for key := range before {
		if _, ok := after[key]; !ok {
			if target == nil {
				target = make(map[string]map[string]*time.Time)
			}
			if target[queue] == nil {
				target[queue] = make(map[string]*time.Time)
			}
			target[queue][key] = nil
		}
	}
	return target
}

func diffAckMap(target map[string]map[string]*ackReceipt, queue string, before, after map[string]ackReceipt) map[string]map[string]*ackReceipt {
	for key, value := range after {
		if old, ok := before[key]; !ok || old != value {
			if target == nil {
				target = make(map[string]map[string]*ackReceipt)
			}
			if target[queue] == nil {
				target[queue] = make(map[string]*ackReceipt)
			}
			copy := value
			target[queue][key] = &copy
		}
	}
	for key := range before {
		if _, ok := after[key]; !ok {
			if target == nil {
				target = make(map[string]map[string]*ackReceipt)
			}
			if target[queue] == nil {
				target[queue] = make(map[string]*ackReceipt)
			}
			target[queue][key] = nil
		}
	}
	return target
}

func applyStringPointers(state *persistedState, changes map[string]map[string]*string) {
	for queueName, entries := range changes {
		queue := state.Queues[queueName]
		if queue.Receipts == nil {
			queue.Receipts = make(map[string]string)
		}
		for key, value := range entries {
			if value == nil {
				delete(queue.Receipts, key)
			} else {
				queue.Receipts[key] = *value
			}
		}
	}
}

func applyTimePointers(state *persistedState, changes map[string]map[string]*time.Time) {
	for queueName, entries := range changes {
		queue := state.Queues[queueName]
		if queue.AckedAt == nil {
			queue.AckedAt = make(map[string]time.Time)
		}
		for key, value := range entries {
			if value == nil {
				delete(queue.AckedAt, key)
			} else {
				queue.AckedAt[key] = *value
			}
		}
	}
}

func applyAckPointers(state *persistedState, changes map[string]map[string]*ackReceipt) {
	for queueName, entries := range changes {
		queue := state.Queues[queueName]
		if queue.AckedReceipts == nil {
			queue.AckedReceipts = make(map[string]ackReceipt)
		}
		for key, value := range entries {
			if value == nil {
				delete(queue.AckedReceipts, key)
			} else {
				queue.AckedReceipts[key] = *value
			}
		}
	}
}

func cloneStateForCheckpoint(state persistedState) (persistedState, error) {
	encoded, err := json.Marshal(state)
	if err != nil {
		return persistedState{}, err
	}
	var clone persistedState
	if err := json.Unmarshal(encoded, &clone); err != nil {
		return persistedState{}, err
	}
	return clone, nil
}

func cloneQueueState(queue *queueState) (*queueState, error) {
	if queue == nil {
		return nil, nil
	}
	clone := &queueState{
		Config:        queue.Config,
		Messages:      make(map[string]*model.Message, len(queue.Messages)),
		Receipts:      make(map[string]string, len(queue.Receipts)),
		AckedAt:       make(map[string]time.Time, len(queue.AckedAt)),
		AckedReceipts: make(map[string]ackReceipt, len(queue.AckedReceipts)),
	}
	for id, message := range queue.Messages {
		copy := cloneMessage(message)
		clone.Messages[id] = &copy
	}
	for id, receipt := range queue.Receipts {
		clone.Receipts[id] = receipt
	}
	for id, ackedAt := range queue.AckedAt {
		clone.AckedAt[id] = ackedAt
	}
	for receipt, record := range queue.AckedReceipts {
		clone.AckedReceipts[receipt] = record
	}
	return clone, nil
}

type mutationBackup struct {
	queueName          string
	queue              *queueState
	queueExisted       bool
	nextSequence       uint64
	totalMessages      int
	totalInFlight      int
	queueInFlight      int
	queueInFlightFound bool
	idempotencyID      string
	idempotency        idempotencyRecord
	idempotencyFound   bool
	prunedIdempotency  map[string]idempotencyRecord
}

func (s *service) backupMutationLocked(queueName, operation, key string) (mutationBackup, error) {
	backup := mutationBackup{
		queueName: queueName, nextSequence: s.state.NextSequence, totalMessages: s.totalMessages,
		totalInFlight: s.totalInFlight,
	}
	backup.queueInFlight, backup.queueInFlightFound = s.inFlightByQueue[queueName]
	if queue, exists := s.state.Queues[queueName]; exists {
		clone, err := cloneQueueState(queue)
		if err != nil {
			return mutationBackup{}, err
		}
		backup.queue, backup.queueExisted = clone, true
	}
	if key != "" {
		backup.idempotencyID = idempotencyID(operation, queueName, key)
		backup.idempotency, backup.idempotencyFound = s.state.Idempotency[backup.idempotencyID]
		if !backup.idempotencyFound && len(s.state.Idempotency) >= s.limits.MaxIdempotencyRecords {
			now := s.clock.Now()
			for id, record := range s.state.Idempotency {
				if !record.ExpiresAt.After(now) {
					if backup.prunedIdempotency == nil {
						backup.prunedIdempotency = make(map[string]idempotencyRecord)
					}
					backup.prunedIdempotency[id] = record
					delete(s.state.Idempotency, id)
				}
			}
		}
	}
	return backup, nil
}

func (s *service) restoreMutationLocked(backup mutationBackup) {
	s.state.NextSequence = backup.nextSequence
	s.totalMessages = backup.totalMessages
	s.totalInFlight = backup.totalInFlight
	if backup.queueInFlightFound {
		s.inFlightByQueue[backup.queueName] = backup.queueInFlight
	} else {
		delete(s.inFlightByQueue, backup.queueName)
	}
	if backup.queueExisted {
		s.state.Queues[backup.queueName] = backup.queue
	} else {
		delete(s.state.Queues, backup.queueName)
	}
	if backup.idempotencyID != "" {
		if backup.idempotencyFound {
			s.state.Idempotency[backup.idempotencyID] = backup.idempotency
		} else {
			delete(s.state.Idempotency, backup.idempotencyID)
		}
	}
	for id, record := range backup.prunedIdempotency {
		s.state.Idempotency[id] = record
	}
}

func (s *service) mutationDeltaLocked(backup mutationBackup) stateDelta {
	delta := stateDelta{NextSequence: s.state.NextSequence}
	queue := s.state.Queues[backup.queueName]
	if !backup.queueExisted {
		if queue != nil {
			delta.UpsertQueues = map[string]*queueState{backup.queueName: queue}
		}
	} else if queue == nil {
		delta.DeleteQueues = []string{backup.queueName}
	} else {
		before := persistedState{Queues: map[string]*queueState{backup.queueName: backup.queue}, Idempotency: map[string]idempotencyRecord{}}
		after := persistedState{Queues: map[string]*queueState{backup.queueName: queue}, Idempotency: map[string]idempotencyRecord{}}
		queueDelta := diffState(before, after)
		delta.UpsertQueues, delta.DeleteQueues = queueDelta.UpsertQueues, queueDelta.DeleteQueues
		delta.UpsertMessages, delta.DeleteMessages = queueDelta.UpsertMessages, queueDelta.DeleteMessages
		delta.Receipts, delta.AckedAt, delta.AckedReceipts = queueDelta.Receipts, queueDelta.AckedAt, queueDelta.AckedReceipts
	}
	if backup.idempotencyID != "" {
		record, exists := s.state.Idempotency[backup.idempotencyID]
		if exists && (!backup.idempotencyFound || !reflect.DeepEqual(record, backup.idempotency)) {
			delta.UpsertIdempotency = map[string]idempotencyRecord{backup.idempotencyID: record}
		} else if !exists && backup.idempotencyFound {
			delta.DeleteIdempotency = []string{backup.idempotencyID}
		}
	}
	for id := range backup.prunedIdempotency {
		if id == backup.idempotencyID {
			continue
		}
		if _, exists := s.state.Idempotency[id]; !exists {
			delta.DeleteIdempotency = append(delta.DeleteIdempotency, id)
		}
	}
	return delta
}

func deltaHasChanges(delta stateDelta, previousSequence uint64) bool {
	return delta.NextSequence != previousSequence || len(delta.UpsertQueues)+len(delta.DeleteQueues)+len(delta.UpsertMessages)+len(delta.DeleteMessages)+len(delta.Receipts)+len(delta.AckedAt)+len(delta.AckedReceipts)+len(delta.UpsertIdempotency)+len(delta.DeleteIdempotency) > 0
}

func (s *service) mutate(ctx context.Context, queueName, operation, key string, reset func(), mutation func() error) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	request := mutationRequest{
		ctx: ctx, queueName: queueName, operation: operation, key: key, reset: reset, mutation: mutation,
		result: make(chan mutationResult, 1),
	}
	s.submitMu.RLock()
	if s.stopping {
		s.submitMu.RUnlock()
		return 0, &Error{Code: CodeClosed, Message: "queue service is closed"}
	}
	select {
	case s.mutationCh <- request:
		s.submitMu.RUnlock()
	default:
		s.submitMu.RUnlock()
		return 0, capacity("mutation coordinator capacity exceeded")
	}
	result := <-request.result
	return result.lsn, result.err
}

func (s *service) runMutationCoordinator() {
	defer close(s.mutationDone)
	for {
		select {
		case first := <-s.mutationCh:
			batch, stopping := s.collectMutationBatch(first)
			s.processMutationRequests(batch)
			if stopping {
				s.drainMutationRequests()
				return
			}
		case <-s.mutationStop:
			s.drainMutationRequests()
			return
		}
	}
}

func (s *service) collectMutationBatch(first mutationRequest) ([]mutationRequest, bool) {
	batch := []mutationRequest{first}
	timer := time.NewTimer(mutationBatchDelay)
	defer timer.Stop()
	for len(batch) < maxMutationBatch {
		select {
		case request := <-s.mutationCh:
			batch = append(batch, request)
		case <-timer.C:
			return batch, false
		case <-s.mutationStop:
			return batch, true
		}
	}
	return batch, false
}

func (s *service) drainMutationRequests() {
	batch := make([]mutationRequest, 0, maxMutationBatch)
	for {
		select {
		case request := <-s.mutationCh:
			batch = append(batch, request)
			if len(batch) == maxMutationBatch {
				s.processMutationRequests(batch)
				batch = batch[:0]
			}
		default:
			s.processMutationRequests(batch)
			return
		}
	}
}

type preparedMutation struct {
	request mutationRequest
	backup  mutationBackup
	delta   stateDelta
	record  journal.Record
}

func (s *service) processMutationRequests(requests []mutationRequest) {
	for len(requests) > 0 {
		consumed := s.processMutationGroup(requests)
		requests = requests[consumed:]
	}
}

func (s *service) processMutationGroup(requests []mutationRequest) int {
	if len(requests) == 0 {
		return 0
	}
	s.mu.Lock()
	prepared := make([]preparedMutation, 0, min(len(requests), maxMutationBatch))
	completed := make([]mutationRequest, 0, len(requests))
	bytes := 0
	consumed := 0
	for consumed < len(requests) && len(prepared) < maxMutationBatch {
		request := requests[consumed]
		if err := request.ctx.Err(); err != nil {
			request.result <- mutationResult{err: err}
			consumed++
			continue
		}
		if request.reset != nil {
			request.reset()
		}
		backup, err := s.backupMutationLocked(request.queueName, request.operation, request.key)
		if err != nil {
			request.result <- mutationResult{err: &Error{Code: CodeStorageUnavailable, Message: "snapshot queue mutation", Cause: err}}
			consumed++
			continue
		}
		if err := request.mutation(); err != nil {
			s.restoreMutationLocked(backup)
			if len(prepared) > 0 {
				break
			}
			request.result <- mutationResult{err: err}
			consumed++
			continue
		}
		delta := s.mutationDeltaLocked(backup)
		if !deltaHasChanges(delta, backup.nextSequence) {
			if len(prepared) > 0 {
				s.restoreMutationLocked(backup)
				break
			}
			completed = append(completed, request)
			consumed++
			continue
		}
		record, recordBytes, err := encodeDeltaRecord(delta)
		if err != nil {
			s.restoreMutationLocked(backup)
			request.result <- mutationResult{err: &Error{Code: CodeStorageUnavailable, Message: "encode queue mutation", Cause: err}}
			consumed++
			continue
		}
		if recordBytes > maxMutationBatchBytes {
			s.restoreMutationLocked(backup)
			request.result <- mutationResult{err: capacity("mutation exceeds batch byte capacity")}
			consumed++
			continue
		}
		if len(prepared) > 0 && bytes+recordBytes > maxMutationBatchBytes {
			s.restoreMutationLocked(backup)
			break
		}
		bytes += recordBytes
		prepared = append(prepared, preparedMutation{request: request, backup: backup, delta: delta, record: record})
		consumed++
	}
	if len(prepared) == 0 {
		s.mu.Unlock()
		for _, request := range completed {
			request.result <- mutationResult{}
		}
		return max(consumed, 1)
	}
	records := make([]journal.Record, len(prepared))
	for index := range prepared {
		records[index] = prepared[index].record
	}
	lsns, err := s.journal.AppendBatch(context.Background(), records)
	if err == nil {
		if len(lsns) != len(prepared) {
			err = fmt.Errorf("journal returned %d LSNs for %d records", len(lsns), len(prepared))
		} else {
			for index, lsn := range lsns {
				if lsn == 0 || index > 0 && lsn <= lsns[index-1] {
					err = fmt.Errorf("journal returned invalid LSN sequence %v", lsns)
					break
				}
			}
		}
	}
	if err != nil {
		for index := len(prepared) - 1; index >= 0; index-- {
			s.restoreMutationLocked(prepared[index].backup)
		}
		s.mu.Unlock()
		storageErr := &Error{Code: CodeStorageUnavailable, Message: "persist queue state", Cause: err}
		for _, mutation := range prepared {
			mutation.request.result <- mutationResult{err: storageErr}
		}
		for _, request := range completed {
			request.result <- mutationResult{}
		}
		return consumed
	}
	for index, mutation := range prepared {
		stampDeltaLSN(&s.state, mutation.delta, lsns[index])
	}
	s.notifyLocked()
	s.mu.Unlock()
	for index, mutation := range prepared {
		mutation.request.result <- mutationResult{lsn: lsns[index]}
	}
	for _, request := range completed {
		request.result <- mutationResult{}
	}
	return consumed
}

func (s *service) notifyLocked() {
	close(s.wake)
	s.wake = make(chan struct{})
}

func (s *service) checkOpenLocked() error {
	if s.closing || s.closed {
		return &Error{Code: CodeClosed, Message: "queue service is closed"}
	}
	return nil
}

func idempotencyID(operation, queue, key string) string {
	return operation + "\x00" + queue + "\x00" + key
}

func fingerprint(value any) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func (s *service) loadIdempotencyLocked(operation, queue, key, requestFingerprint string, result any) (bool, error) {
	if key == "" {
		return false, nil
	}
	if len(key) > s.limits.MaxIdempotencyKeyBytes {
		return false, invalid("idempotency key exceeds configured limit")
	}
	record, ok := s.state.Idempotency[idempotencyID(operation, queue, key)]
	if !ok || !record.ExpiresAt.After(s.clock.Now()) {
		return false, nil
	}
	if record.Fingerprint != requestFingerprint {
		return false, &Error{Code: CodeIdempotencyConflict, Message: "idempotency key was used with a different request"}
	}
	if err := json.Unmarshal(record.Result, result); err != nil {
		return false, &Error{Code: CodeStorageUnavailable, Message: "decode idempotency result", Cause: err}
	}
	stampMutationResult(result, record.LastLSN)
	return true, nil
}

func stampMutationResult(result any, lsn uint64) {
	if lsn == 0 {
		return
	}
	switch value := result.(type) {
	case *enqueueMutationResult:
		value.Message.LastLSN = lsn
	case *receiveMutationResult:
		if value.Delivery != nil {
			value.Delivery.Message.LastLSN = lsn
		}
	case *nackMutationResult:
		value.Message.LastLSN = lsn
	case *extendMutationResult:
		value.Delivery.Message.LastLSN = lsn
	case *redriveMutationResult:
		if value.SourceChanged {
			value.Result.Source.LastLSN = lsn
		}
		value.Result.Child.LastLSN = lsn
	}
}

func (s *service) saveIdempotencyLocked(operation, queue, key, requestFingerprint string, result any) error {
	if key == "" {
		return nil
	}
	if len(key) > s.limits.MaxIdempotencyKeyBytes {
		return invalid("idempotency key exceeds configured limit")
	}
	now := s.clock.Now()
	id := idempotencyID(operation, queue, key)
	if _, exists := s.state.Idempotency[id]; !exists && len(s.state.Idempotency) >= s.limits.MaxIdempotencyRecords {
		return &Error{Code: CodeCapacityExceeded, Message: "idempotency record capacity exceeded"}
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("encode idempotency result: %w", err)
	}
	s.state.Idempotency[id] = idempotencyRecord{
		Operation: operation, Queue: queue, Key: key, Fingerprint: requestFingerprint,
		Result: encoded, CreatedAt: now, ExpiresAt: now.Add(s.limits.IdempotencyRetention),
	}
	return nil
}

func (s *service) pruneIdempotencyLocked(now time.Time) {
	for id, record := range s.state.Idempotency {
		if !record.ExpiresAt.After(now) {
			delete(s.state.Idempotency, id)
		}
	}
}

func validateQueueName(name string) error {
	if name == "" || len(name) > 128 {
		return invalid("queue name must contain 1 to 128 bytes")
	}
	for _, character := range name {
		if !(character == '-' || character == '_' || character == '.' || character >= '0' && character <= '9' || character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z') {
			return invalid("queue name contains unsupported characters")
		}
	}
	return nil
}

func invalid(message string) error  { return &Error{Code: CodeInvalid, Message: message} }
func notFound(message string) error { return &Error{Code: CodeNotFound, Message: message} }
func conflict(message string) error { return &Error{Code: CodeConflict, Message: message} }
func capacity(message string) error { return &Error{Code: CodeCapacityExceeded, Message: message} }

func cloneMessage(message *model.Message) model.Message {
	clone := *message
	clone.Payload = append(json.RawMessage(nil), message.Payload...)
	if message.LeasedAt != nil {
		leasedAt := *message.LeasedAt
		clone.LeasedAt = &leasedAt
	}
	if message.LeaseUntil != nil {
		leaseUntil := *message.LeaseUntil
		clone.LeaseUntil = &leaseUntil
	}
	if message.DeadAt != nil {
		deadAt := *message.DeadAt
		clone.DeadAt = &deadAt
	}
	return clone
}

func cloneQueueInfo(queue *queueState, now time.Time) model.QueueInfo {
	return model.QueueInfo{Config: queue.Config, Counts: counts(queue, now)}
}

func counts(queue *queueState, now time.Time) model.QueueCounts {
	var result model.QueueCounts
	for _, message := range queue.Messages {
		result.Total++
		switch logicalState(message, now, queue.Config.MaxDeliveries) {
		case model.StateReady:
			result.Ready++
		case model.StateDelayed:
			result.Delayed++
		case model.StateLeased:
			result.InFlight++
		case model.StateDead:
			result.Dead++
		case model.StateAcked:
			result.Acked++
		}
	}
	return result
}

func logicalState(message *model.Message, now time.Time, maxDeliveries uint32) model.MessageState {
	if message.State == model.StateLeased && (message.LeaseUntil == nil || !now.Before(*message.LeaseUntil)) {
		if message.DeliveryCount >= maxDeliveries {
			return model.StateDead
		}
		return model.StateReady
	}
	if message.State == model.StateReady || message.State == model.StateDelayed {
		if now.Before(message.AvailableAt) {
			return model.StateDelayed
		}
		return model.StateReady
	}
	return message.State
}

func queueMessageCount(queue *queueState) int   { return len(queue.Messages) }
func (s *service) totalMessageCountLocked() int { return s.totalMessages }

func newID() (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes[:]), nil
}

func newReceipt(messageID string, epoch uint64) (string, error) {
	randomID, err := newID()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s.%d.%s", messageID, epoch, randomID), nil
}

func checkedAdd(now time.Time, duration time.Duration) (time.Time, error) {
	if duration < 0 {
		return time.Time{}, invalid("duration must not be negative")
	}
	result := now.Add(duration)
	if result.Before(now) || result.Year() < 0 || result.Year() > 9999 {
		return time.Time{}, invalid("scheduled time is outside supported clock bounds")
	}
	return result, nil
}

func boundedAvailableAt(now time.Time, delay, maxDelay time.Duration, requested *time.Time) (time.Time, error) {
	if delay < 0 || delay > maxDelay {
		return time.Time{}, invalid("delay is outside configured bounds")
	}
	latest, err := checkedAdd(now, maxDelay)
	if err != nil {
		return time.Time{}, err
	}
	result, err := checkedAdd(now, delay)
	if err != nil {
		return time.Time{}, err
	}
	if requested != nil {
		if requested.After(latest) {
			return time.Time{}, invalid("available at is outside configured delay bounds")
		}
		if requested.After(result) {
			result = *requested
		}
	}
	return result, nil
}

func compareMessages(left, right *model.Message, config model.QueueConfig) bool {
	if config.PriorityEnabled && left.Priority != right.Priority {
		return left.Priority > right.Priority
	}
	if config.Ordering == model.LIFO {
		return left.Sequence > right.Sequence
	}
	return left.Sequence < right.Sequence
}

type listCursor struct {
	Scope              string
	SnapshotLSN        uint64
	HighWater          uint64
	Sequence           uint64
	SnapshotGeneration uint64
	SnapshotSecond     int64
	SnapshotNanosecond int32
}

const cursorPrefix = "v3."

func cursorScope(queue string, state model.MessageState, deadOnly bool) string {
	digest := sha256.New()
	_, _ = digest.Write([]byte(queue))
	_, _ = digest.Write([]byte{0})
	_, _ = digest.Write([]byte(state))
	scopeKind := byte(0)
	if deadOnly {
		scopeKind = 1
	}
	_, _ = digest.Write([]byte{0, scopeKind})
	return hex.EncodeToString(digest.Sum(nil)[:16])
}

func encodeCursor(cursor listCursor) string {
	encodedSecond := uint64(cursor.SnapshotSecond) ^ (uint64(1) << 63)
	return fmt.Sprintf("%s%s.%020d.%020d.%020d.%020d.%020d.%09d", cursorPrefix, cursor.Scope, cursor.SnapshotLSN, cursor.HighWater, cursor.Sequence, cursor.SnapshotGeneration, encodedSecond, cursor.SnapshotNanosecond)
}

func decodeCursor(encoded string) (listCursor, error) {
	if encoded == "" {
		return listCursor{}, nil
	}
	if len(encoded) != len(cursorPrefix)+32+1+20+1+20+1+20+1+20+1+20+1+9 || !strings.HasPrefix(encoded, cursorPrefix) || encoded[35] != '.' || encoded[56] != '.' || encoded[77] != '.' || encoded[98] != '.' || encoded[119] != '.' || encoded[140] != '.' {
		return listCursor{}, invalid("invalid cursor")
	}
	scope := encoded[3:35]
	if _, err := hex.DecodeString(scope); err != nil {
		return listCursor{}, invalid("invalid cursor")
	}
	parts := []string{encoded[36:56], encoded[57:77], encoded[78:98], encoded[99:119], encoded[120:140], encoded[141:150]}
	values := make([]uint64, len(parts))
	for index, part := range parts {
		if strings.Trim(part, "0123456789") != "" {
			return listCursor{}, invalid("invalid cursor")
		}
		value, err := strconv.ParseUint(part, 10, 64)
		if err != nil {
			return listCursor{}, invalid("invalid cursor")
		}
		values[index] = value
	}
	if values[5] >= uint64(time.Second) {
		return listCursor{}, invalid("invalid cursor")
	}
	cursor := listCursor{
		Scope: scope, SnapshotLSN: values[0], HighWater: values[1], Sequence: values[2], SnapshotGeneration: values[3],
		SnapshotSecond: int64(values[4] ^ (uint64(1) << 63)), SnapshotNanosecond: int32(values[5]),
	}
	if cursor.Sequence > cursor.HighWater {
		return listCursor{}, invalid("invalid cursor")
	}
	return cursor, nil
}

func normalizeReason(reason string) string {
	reason = strings.TrimSpace(reason)
	if len(reason) > 512 {
		return reason[:512]
	}
	return reason
}
