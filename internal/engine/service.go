package engine

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	queueclock "github.com/ElijahUmana/queuemaxxing/internal/clock"
	"github.com/ElijahUmana/queuemaxxing/internal/journal"
	"github.com/ElijahUmana/queuemaxxing/internal/model"
)

const stateVersion = 1

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

type service struct {
	mu      sync.Mutex
	journal journal.Journal
	clock   queueclock.Clock
	limits  Limits
	state   persistedState
	wake    chan struct{}
	closed  bool
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
		journal: store,
		clock:   serviceClock,
		limits:  limits,
		wake:    make(chan struct{}),
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
		limits.IdempotencyRetention <= 0 || limits.AckTombstoneRetention <= 0 || limits.MaxListLimit < 1 {
		return invalid("all engine limits must be positive except wait and delay limits, which may be zero")
	}
	return nil
}

func (s *service) recover() error {
	if snapshot := s.journal.Snapshot(); len(snapshot.Payload) > 0 {
		if err := s.applyEnvelope(snapshot.Payload); err != nil {
			return fmt.Errorf("apply snapshot through LSN %d: %w", snapshot.ThroughLSN, err)
		}
	}
	for _, record := range s.journal.Records() {
		if len(record.Payload) > 0 {
			if err := s.applyEnvelope(record.Payload); err != nil {
				return fmt.Errorf("apply WAL record %d: %w", record.LSN, err)
			}
		}
	}
	return s.validateRecoveredState()
}

func (s *service) applyEnvelope(payload []byte) error {
	var envelope persistedEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return fmt.Errorf("decode persisted envelope: %w", err)
	}
	if envelope.Version != stateVersion {
		return fmt.Errorf("unsupported persisted version %d", envelope.Version)
	}
	switch envelope.Kind {
	case "state":
		var recovered persistedState
		if err := json.Unmarshal(envelope.State, &recovered); err != nil {
			return fmt.Errorf("decode persisted state: %w", err)
		}
		if recovered.Version != stateVersion {
			return fmt.Errorf("unsupported state version %d", recovered.Version)
		}
		s.state = recovered
	case "delta":
		if envelope.Delta == nil {
			return fmt.Errorf("delta envelope is missing delta")
		}
		applyDelta(&s.state, *envelope.Delta)
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
	for name, queue := range s.state.Queues {
		if queue == nil {
			return fmt.Errorf("queue %q has nil state", name)
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
		for id, receipt := range queue.Receipts {
			message := queue.Messages[id]
			if message == nil || message.State != model.StateLeased || receipt == "" {
				return fmt.Errorf("queue %q has invalid receipt for message %q", name, id)
			}
		}
	}
	return nil
}

func (s *service) persistLocked(ctx context.Context, before persistedState) (uint64, error) {
	delta := diffState(before, s.state)
	expectedLSN := s.journal.Stats().DurableLSN + 1
	for queueName, messages := range delta.UpsertMessages {
		queue := s.state.Queues[queueName]
		for messageID := range messages {
			queue.Messages[messageID].LastLSN = expectedLSN
		}
	}
	delta = diffState(before, s.state)
	envelopeBytes, err := json.Marshal(persistedEnvelope{Kind: "delta", Version: stateVersion, Delta: &delta})
	if err != nil {
		return 0, fmt.Errorf("encode state delta: %w", err)
	}
	var transactionID journal.TransactionID
	if _, err := rand.Read(transactionID[:]); err != nil {
		return 0, fmt.Errorf("generate transaction id: %w", err)
	}
	lsn, err := s.journal.Append(ctx, transactionID, envelopeBytes)
	if err != nil {
		return 0, err
	}
	if lsn != expectedLSN {
		return 0, fmt.Errorf("journal assigned LSN %d, expected %d", lsn, expectedLSN)
	}
	return lsn, nil
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

func cloneState(state persistedState) (persistedState, error) {
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

func (s *service) mutate(ctx context.Context, operation func() error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return &Error{Code: CodeClosed, Message: "queue service is closed"}
	}
	before, err := cloneState(s.state)
	if err != nil {
		return &Error{Code: CodeStorageUnavailable, Message: "snapshot queue state", Cause: err}
	}
	if err := operation(); err != nil {
		s.state = before
		return err
	}
	if _, err := s.persistLocked(ctx, before); err != nil {
		s.state = before
		return &Error{Code: CodeStorageUnavailable, Message: "persist queue state", Cause: err}
	}
	s.notifyLocked()
	return nil
}

func (s *service) notifyLocked() {
	close(s.wake)
	s.wake = make(chan struct{})
}

func (s *service) checkOpenLocked() error {
	if s.closed {
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
	return true, nil
}

func (s *service) saveIdempotencyLocked(operation, queue, key, requestFingerprint string, result any) error {
	if key == "" {
		return nil
	}
	if len(key) > s.limits.MaxIdempotencyKeyBytes {
		return invalid("idempotency key exceeds configured limit")
	}
	now := s.clock.Now()
	s.pruneIdempotencyLocked(now)
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

func queueMessageCount(queue *queueState) int { return len(queue.Messages) }
func (s *service) totalMessageCountLocked() int {
	total := 0
	for _, queue := range s.state.Queues {
		total += len(queue.Messages)
	}
	return total
}

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

func availableAt(now time.Time, delay time.Duration, requested *time.Time) time.Time {
	result := now.Add(delay)
	if requested != nil && requested.After(result) {
		result = *requested
	}
	if result.Before(now) {
		return now
	}
	return result
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

func sortMessages(messages []model.Message, config model.QueueConfig) {
	sort.Slice(messages, func(i, j int) bool { return compareMessages(&messages[i], &messages[j], config) })
}

func encodeCursor(sequence uint64) string { return fmt.Sprintf("%020d", sequence) }
func decodeCursor(cursor string) (uint64, error) {
	if cursor == "" {
		return 0, nil
	}
	var sequence uint64
	if _, err := fmt.Sscanf(cursor, "%d", &sequence); err != nil {
		return 0, invalid("invalid cursor")
	}
	return sequence, nil
}

func normalizeReason(reason string) string {
	reason = strings.TrimSpace(reason)
	if len(reason) > 512 {
		return reason[:512]
	}
	return reason
}
