package engine

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	queueclock "github.com/ElijahUmana/queuemaxxing/internal/clock"
	"github.com/ElijahUmana/queuemaxxing/internal/journal"
	"github.com/ElijahUmana/queuemaxxing/internal/model"
)

type memoryJournal struct {
	mu              sync.Mutex
	records         []journal.Record
	snapshot        journal.Snapshot
	closed          bool
	appendError     error
	batchCalls      int
	batchSizes      []int
	checkpointError error
	closeError      error
	readOnly        bool
	readReason      string
}

func (store *memoryJournal) Append(ctx context.Context, transaction journal.TransactionID, payload []byte) (uint64, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.appendError != nil {
		return 0, store.appendError
	}
	lsn := uint64(len(store.records)) + store.snapshot.ThroughLSN + 1
	store.records = append(store.records, journal.Record{LSN: lsn, TransactionID: transaction, Payload: append([]byte(nil), payload...)})
	return lsn, nil
}
func (store *memoryJournal) AppendBatch(ctx context.Context, records []journal.Record) ([]uint64, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	store.batchCalls++
	store.batchSizes = append(store.batchSizes, len(records))
	if store.appendError != nil {
		return nil, store.appendError
	}
	lsns := make([]uint64, len(records))
	for index := range records {
		lsn := uint64(len(store.records)) + store.snapshot.ThroughLSN + 1
		store.records = append(store.records, journal.Record{LSN: lsn, TransactionID: records[index].TransactionID, Payload: append([]byte(nil), records[index].Payload...)})
		lsns[index] = lsn
	}
	return lsns, nil
}
func (store *memoryJournal) Checkpoint(ctx context.Context, through uint64, payload []byte) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.checkpointError != nil {
		return store.checkpointError
	}
	store.snapshot = journal.Snapshot{Generation: store.snapshot.Generation + 1, ThroughLSN: through, Payload: append([]byte(nil), payload...)}
	kept := store.records[:0]
	for _, record := range store.records {
		if record.LSN > through {
			kept = append(kept, record)
		}
	}
	store.records = kept
	return nil
}
func (store *memoryJournal) Records() []journal.Record {
	store.mu.Lock()
	defer store.mu.Unlock()
	result := make([]journal.Record, len(store.records))
	copy(result, store.records)
	return result
}
func (store *memoryJournal) Snapshot() journal.Snapshot {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.snapshot
}
func (store *memoryJournal) Stats() journal.Stats {
	store.mu.Lock()
	defer store.mu.Unlock()
	durable := store.snapshot.ThroughLSN
	if len(store.records) > 0 {
		durable = store.records[len(store.records)-1].LSN
	}
	return journal.Stats{DurableLSN: durable, SnapshotGeneration: store.snapshot.Generation, ReadOnly: store.readOnly, ReadOnlyReason: store.readReason}
}
func (store *memoryJournal) Close() error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closeError != nil {
		return store.closeError
	}
	store.closed = true
	return nil
}

type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
}
type fakeTimer struct {
	clock    *fakeClock
	channel  chan time.Time
	deadline time.Time
	active   bool
}

func newFakeClock(now time.Time) *fakeClock { return &fakeClock{now: now} }
func (clock *fakeClock) Now() time.Time     { clock.mu.Lock(); defer clock.mu.Unlock(); return clock.now }
func (clock *fakeClock) NewTimer(duration time.Duration) queueclock.Timer {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	timer := &fakeTimer{clock: clock, channel: make(chan time.Time, 1), deadline: clock.now.Add(duration), active: true}
	clock.timers = append(clock.timers, timer)
	return timer
}
func (clock *fakeClock) Advance(duration time.Duration) {
	clock.mu.Lock()
	clock.now = clock.now.Add(duration)
	now := clock.now
	for _, timer := range clock.timers {
		if timer.active && !timer.deadline.After(now) {
			timer.active = false
			timer.channel <- now
		}
	}
	clock.mu.Unlock()
}
func (timer *fakeTimer) C() <-chan time.Time { return timer.channel }
func (timer *fakeTimer) Reset(duration time.Duration) bool {
	timer.clock.mu.Lock()
	defer timer.clock.mu.Unlock()
	previous := timer.active
	timer.deadline = timer.clock.now.Add(duration)
	timer.active = true
	return previous
}
func (timer *fakeTimer) Stop() bool {
	timer.clock.mu.Lock()
	defer timer.clock.mu.Unlock()
	previous := timer.active
	timer.active = false
	return previous
}

func newTestService(t *testing.T, ordering model.Ordering, priority bool, maxDeliveries uint32) (*service, *memoryJournal, *fakeClock) {
	t.Helper()
	store := &memoryJournal{}
	clock := newFakeClock(time.Date(2026, 8, 26, 20, 0, 0, 0, time.UTC))
	created, err := New(store, clock, Options{})
	if err != nil {
		t.Fatal(err)
	}
	engine := created.(*service)
	_, _, err = engine.CreateQueue(context.Background(), model.QueueConfig{Name: "jobs", Ordering: ordering, PriorityEnabled: priority, DefaultVisibilityTimeout: time.Minute, MaxDeliveries: maxDeliveries}, "create")
	if err != nil {
		t.Fatal(err)
	}
	return engine, store, clock
}

func enqueueTest(t *testing.T, engine *service, payload string, priority int32, delay time.Duration, key string) model.Message {
	t.Helper()
	var requestedPriority *int32
	if engine.state.Queues["jobs"].Config.PriorityEnabled {
		requestedPriority = &priority
	}
	var requestedDelay *time.Duration
	if delay != 0 {
		requestedDelay = &delay
	}
	message, _, err := engine.Enqueue(context.Background(), "jobs", model.EnqueueRequest{Payload: json.RawMessage(payload), Priority: requestedPriority, Delay: requestedDelay, IdempotencyKey: key})
	if err != nil {
		t.Fatal(err)
	}
	return message
}

func receiveTest(t *testing.T, engine *service, key string) *model.Delivery {
	t.Helper()
	delivery, _, err := engine.Receive(context.Background(), "jobs", model.ReceiveRequest{VisibilityTimeout: time.Minute, IdempotencyKey: key})
	if err != nil {
		t.Fatal(err)
	}
	return delivery
}

func TestOrderingCombinations(t *testing.T) {
	for _, test := range []struct {
		name     string
		ordering model.Ordering
		priority bool
		want     []string
	}{
		{"fifo", model.FIFO, false, []string{`{"id":"a"}`, `{"id":"b"}`, `{"id":"c"}`}},
		{"lifo", model.LIFO, false, []string{`{"id":"c"}`, `{"id":"b"}`, `{"id":"a"}`}},
		{"priority-fifo", model.FIFO, true, []string{`{"id":"b"}`, `{"id":"c"}`, `{"id":"a"}`}},
		{"priority-lifo", model.LIFO, true, []string{`{"id":"c"}`, `{"id":"b"}`, `{"id":"a"}`}},
	} {
		t.Run(test.name, func(t *testing.T) {
			engine, _, _ := newTestService(t, test.ordering, test.priority, 3)
			priorityB := int32(0)
			if test.priority {
				priorityB = 2
			}
			enqueueTest(t, engine, `{"id":"a"}`, 0, 0, "a")
			enqueueTest(t, engine, `{"id":"b"}`, priorityB, 0, "b")
			enqueueTest(t, engine, `{"id":"c"}`, priorityB, 0, "c")
			for index, expected := range test.want {
				delivery := receiveTest(t, engine, "r"+string(rune('a'+index)))
				if string(delivery.Message.Payload) != expected {
					t.Fatalf("delivery %d = %s, want %s", index, delivery.Message.Payload, expected)
				}
			}
		})
	}
}

func TestDelayEligibilityAndPriority(t *testing.T) {
	engine, _, clock := newTestService(t, model.FIFO, true, 3)
	enqueueTest(t, engine, `{"id":"high"}`, 10, time.Minute, "high")
	enqueueTest(t, engine, `{"id":"low"}`, 1, 0, "low")
	if got := string(receiveTest(t, engine, "first").Message.Payload); got != `{"id":"low"}` {
		t.Fatalf("got %s", got)
	}
	clock.Advance(time.Minute)
	if got := string(receiveTest(t, engine, "second").Message.Payload); got != `{"id":"high"}` {
		t.Fatalf("got %s", got)
	}
}

func TestConcurrentReceiveHasSingleLeasePerMessage(t *testing.T) {
	engine, _, _ := newTestService(t, model.FIFO, false, 3)
	enqueueTest(t, engine, `{}`, 0, 0, "message")
	var wait sync.WaitGroup
	successes := make(chan *model.Delivery, 32)
	for index := 0; index < 32; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			delivery, _, err := engine.Receive(context.Background(), "jobs", model.ReceiveRequest{IdempotencyKey: string(rune('a' + index))})
			if err != nil {
				t.Error(err)
				return
			}
			if delivery != nil {
				successes <- delivery
			}
		}(index)
	}
	wait.Wait()
	close(successes)
	if count := len(successes); count != 1 {
		t.Fatalf("successful leases = %d, want 1", count)
	}
}

func TestLeaseExpiryFencingAndDeadLetterThreshold(t *testing.T) {
	engine, _, clock := newTestService(t, model.FIFO, false, 2)
	message := enqueueTest(t, engine, `{}`, 0, 0, "message")
	first := receiveTest(t, engine, "first")
	clock.Advance(time.Minute)
	second := receiveTest(t, engine, "second")
	if second.DeliveryCount != 2 {
		t.Fatalf("delivery count = %d", second.DeliveryCount)
	}
	if _, err := engine.Ack(context.Background(), "jobs", model.AckRequest{MessageID: message.ID, Receipt: first.Receipt}); !IsCode(err, CodeStaleReceipt) {
		t.Fatalf("old ack error = %v", err)
	}
	clock.Advance(time.Minute)
	delivery, _, err := engine.Receive(context.Background(), "jobs", model.ReceiveRequest{})
	if err != nil || delivery != nil {
		t.Fatalf("third receive = %#v, %v", delivery, err)
	}
	dead, err := engine.ListDeadLetters(context.Background(), "jobs", model.ListFilter{})
	if err != nil || len(dead.Messages) != 1 || dead.Messages[0].State != model.StateDead {
		t.Fatalf("dead letters = %#v, %v", dead, err)
	}
}

func TestNackPreservesSequenceAndRedriveCreatesIdentity(t *testing.T) {
	engine, _, _ := newTestService(t, model.LIFO, false, 1)
	original := enqueueTest(t, engine, `{"id":"original"}`, 0, 0, "original")
	delivery := receiveTest(t, engine, "receive")
	dead, _, err := engine.Nack(context.Background(), "jobs", model.NackRequest{MessageID: original.ID, Receipt: delivery.Receipt, Reason: "failed", IdempotencyKey: "nack"})
	if err != nil || dead.Sequence != original.Sequence || dead.State != model.StateDead {
		t.Fatalf("nack = %#v, %v", dead, err)
	}
	redriven, replayed, err := engine.Redrive(context.Background(), "jobs", model.RedriveRequest{MessageID: original.ID, IdempotencyKey: "redrive"})
	if err != nil || replayed || redriven.Child.ID == original.ID || redriven.Child.Sequence == original.Sequence || redriven.Child.ReplayOf != original.ID || redriven.Child.DeliveryCount != 0 {
		t.Fatalf("redrive = %#v, %t, %v", redriven, replayed, err)
	}
	again, replayed, err := engine.Redrive(context.Background(), "jobs", model.RedriveRequest{MessageID: original.ID, IdempotencyKey: "redrive"})
	if err != nil || !replayed || again.Child.ID != redriven.Child.ID {
		t.Fatalf("replayed redrive = %#v, %t, %v", again, replayed, err)
	}
}

func TestIdempotencyConflictAndReceiveReplay(t *testing.T) {
	engine, _, _ := newTestService(t, model.FIFO, false, 3)
	first := enqueueTest(t, engine, `{"value":1}`, 0, 0, "same")
	second, replayed, err := engine.Enqueue(context.Background(), "jobs", model.EnqueueRequest{Payload: json.RawMessage(`{"value":1}`), IdempotencyKey: "same"})
	if err != nil || !replayed || second.ID != first.ID {
		t.Fatalf("enqueue replay = %#v, %t, %v", second, replayed, err)
	}
	_, _, err = engine.Enqueue(context.Background(), "jobs", model.EnqueueRequest{Payload: json.RawMessage(`{"value":2}`), IdempotencyKey: "same"})
	if !IsCode(err, CodeIdempotencyConflict) {
		t.Fatalf("conflict error = %v", err)
	}
	delivery := receiveTest(t, engine, "receive")
	again, replayed, err := engine.Receive(context.Background(), "jobs", model.ReceiveRequest{VisibilityTimeout: time.Minute, IdempotencyKey: "receive"})
	if err != nil || !replayed || again.Receipt != delivery.Receipt || !again.LeaseUntil.Equal(delivery.LeaseUntil) {
		t.Fatalf("receive replay = %#v, %t, %v", again, replayed, err)
	}
}

func TestRecoveryPreservesActiveReceiptAndIdempotency(t *testing.T) {
	engine, store, clock := newTestService(t, model.FIFO, false, 3)
	message := enqueueTest(t, engine, `{}`, 0, 0, "enqueue")
	delivery := receiveTest(t, engine, "receive")
	recoveredService, err := New(store, clock, Options{})
	if err != nil {
		t.Fatal(err)
	}
	recovered := recoveredService.(*service)
	if _, err := recovered.Ack(context.Background(), "jobs", model.AckRequest{MessageID: message.ID, Receipt: delivery.Receipt, IdempotencyKey: "ack"}); err != nil {
		t.Fatalf("ack after recovery: %v", err)
	}
	replayed, isReplay, err := recovered.Enqueue(context.Background(), "jobs", model.EnqueueRequest{Payload: json.RawMessage(`{}`), IdempotencyKey: "enqueue"})
	if err != nil || !isReplay || replayed.ID != message.ID {
		t.Fatalf("idempotency recovery = %#v, %t, %v", replayed, isReplay, err)
	}
}

func TestLongPollWakesOnEnqueue(t *testing.T) {
	engine, _, _ := newTestService(t, model.FIFO, false, 3)
	result := make(chan *model.Delivery, 1)
	errors := make(chan error, 1)
	go func() {
		delivery, _, err := engine.Receive(context.Background(), "jobs", model.ReceiveRequest{WaitTimeout: 10 * time.Second})
		if err != nil {
			errors <- err
			return
		}
		result <- delivery
	}()
	time.Sleep(10 * time.Millisecond)
	enqueueTest(t, engine, `{}`, 0, 0, "message")
	select {
	case err := <-errors:
		t.Fatal(err)
	case delivery := <-result:
		if delivery == nil {
			t.Fatal("long poll returned empty")
		}
	case <-time.After(time.Second):
		t.Fatal("long poll did not wake")
	}
}

func TestNackErrorRollsBackMaterialization(t *testing.T) {
	engine, _, clock := newTestService(t, model.FIFO, false, 3)
	delayed := enqueueTest(t, engine, `{}`, 0, time.Minute, "delayed")
	clock.Advance(time.Minute)
	_, _, err := engine.Nack(context.Background(), "jobs", model.NackRequest{MessageID: "missing", Receipt: "missing"})
	if !IsCode(err, CodeNotFound) {
		t.Fatalf("nack error = %v", err)
	}
	engine.mu.Lock()
	state := engine.state.Queues["jobs"].Messages[delayed.ID].State
	engine.mu.Unlock()
	if state != model.StateDelayed {
		t.Fatalf("failed mutation leaked state %q", state)
	}
}

func TestAckIsIdempotentWithoutRequestKey(t *testing.T) {
	engine, _, _ := newTestService(t, model.FIFO, false, 3)
	message := enqueueTest(t, engine, `{}`, 0, 0, "message")
	delivery := receiveTest(t, engine, "receive")
	if replayed, err := engine.Ack(context.Background(), "jobs", model.AckRequest{MessageID: message.ID, Receipt: delivery.Receipt}); err != nil || replayed {
		t.Fatalf("first ack = %t, %v", replayed, err)
	}
	if replayed, err := engine.Ack(context.Background(), "jobs", model.AckRequest{MessageID: message.ID, Receipt: delivery.Receipt}); err != nil || !replayed {
		t.Fatalf("second ack = %t, %v", replayed, err)
	}
}

func TestCompactionSnapshotRecovery(t *testing.T) {
	engine, store, clock := newTestService(t, model.FIFO, false, 3)
	first := enqueueTest(t, engine, `{"id":1}`, 0, 0, "first")
	if err := engine.Compact(context.Background()); err != nil {
		t.Fatal(err)
	}
	second := enqueueTest(t, engine, `{"id":2}`, 0, 0, "second")
	recoveredService, err := New(store, clock, Options{})
	if err != nil {
		t.Fatal(err)
	}
	page, err := recoveredService.ListMessages(context.Background(), "jobs", model.ListFilter{})
	if err != nil || len(page.Messages) != 2 || page.Messages[0].ID != first.ID || page.Messages[1].ID != second.ID {
		t.Fatalf("recovered page = %#v, %v", page, err)
	}
}

func TestConcurrentEnqueueSequencesAreUnique(t *testing.T) {
	engine, _, _ := newTestService(t, model.FIFO, false, 3)
	const count = 100
	var wait sync.WaitGroup
	for index := 0; index < count; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			if _, _, err := engine.Enqueue(context.Background(), "jobs", model.EnqueueRequest{Payload: json.RawMessage(`{}`), IdempotencyKey: fmt.Sprintf("e-%d", index)}); err != nil {
				t.Error(err)
			}
		}(index)
	}
	wait.Wait()
	page, err := engine.ListMessages(context.Background(), "jobs", model.ListFilter{Limit: count})
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[uint64]bool, count)
	for _, message := range page.Messages {
		if seen[message.Sequence] {
			t.Fatalf("duplicate sequence %d", message.Sequence)
		}
		seen[message.Sequence] = true
	}
	if len(seen) != count {
		t.Fatalf("unique sequences = %d", len(seen))
	}
}

func TestPersistenceFailureRollsBackMutation(t *testing.T) {
	engine, store, _ := newTestService(t, model.FIFO, false, 3)
	store.appendError = errors.New("disk full")
	_, _, err := engine.Enqueue(context.Background(), "jobs", model.EnqueueRequest{Payload: json.RawMessage(`{}`), IdempotencyKey: "failed"})
	if !IsCode(err, CodeStorageUnavailable) {
		t.Fatalf("enqueue error = %v", err)
	}
	page, pageErr := engine.ListMessages(context.Background(), "jobs", model.ListFilter{})
	if pageErr != nil || len(page.Messages) != 0 || engine.totalMessages != 0 {
		t.Fatalf("rolled back state = %#v, total=%d, err=%v", page, engine.totalMessages, pageErr)
	}
}

func TestCheckpointFailureLeavesLiveStateUnchanged(t *testing.T) {
	engine, store, clock := newTestService(t, model.FIFO, false, 1)
	message := enqueueTest(t, engine, `{}`, 0, 0, "message")
	delivery := receiveTest(t, engine, "receive")
	clock.Advance(time.Minute)
	store.checkpointError = errors.New("disk full")
	if err := engine.Compact(context.Background()); !IsCode(err, CodeStorageUnavailable) {
		t.Fatalf("compact error = %v", err)
	}
	engine.mu.Lock()
	stored := engine.state.Queues["jobs"].Messages[message.ID]
	receipt := engine.state.Queues["jobs"].Receipts[message.ID]
	engine.mu.Unlock()
	if stored.State != model.StateLeased || receipt != delivery.Receipt {
		t.Fatalf("checkpoint failure mutated live state: %#v receipt=%q", stored, receipt)
	}
}

func TestExpiredFinalLeaseCanBeRedriven(t *testing.T) {
	engine, _, clock := newTestService(t, model.FIFO, false, 1)
	message := enqueueTest(t, engine, `{}`, 0, 0, "message")
	receiveTest(t, engine, "receive")
	clock.Advance(time.Minute)
	result, _, err := engine.Redrive(context.Background(), "jobs", model.RedriveRequest{MessageID: message.ID, IdempotencyKey: "redrive"})
	if err != nil || result.Source.State != model.StateDead || result.Child.ReplayOf != message.ID {
		t.Fatalf("redrive = %#v, %v", result, err)
	}
}

type nonPredictiveJournal struct {
	memoryJournal
	next uint64
}

func (store *nonPredictiveJournal) Append(_ context.Context, transaction journal.TransactionID, payload []byte) (uint64, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.appendError != nil {
		return 0, store.appendError
	}
	store.next += 7
	store.records = append(store.records, journal.Record{LSN: store.next, TransactionID: transaction, Payload: append([]byte(nil), payload...)})
	return store.next, nil
}

func (store *nonPredictiveJournal) AppendBatch(ctx context.Context, records []journal.Record) ([]uint64, error) {
	lsns := make([]uint64, len(records))
	for index := range records {
		lsn, err := store.Append(ctx, records[index].TransactionID, records[index].Payload)
		if err != nil {
			return nil, err
		}
		lsns[index] = lsn
	}
	return lsns, nil
}

func TestLastLSNUsesJournalAssignmentAndRecoveredRecord(t *testing.T) {
	store := &nonPredictiveJournal{}
	clock := newFakeClock(time.Date(2026, 8, 26, 20, 0, 0, 0, time.UTC))
	created, err := New(store, clock, Options{})
	if err != nil {
		t.Fatal(err)
	}
	engine := created.(*service)
	if _, _, err := engine.CreateQueue(context.Background(), model.QueueConfig{Name: "jobs", Ordering: model.FIFO, DefaultVisibilityTimeout: time.Minute, MaxDeliveries: 3}, "create"); err != nil {
		t.Fatal(err)
	}
	message, _, err := engine.Enqueue(context.Background(), "jobs", model.EnqueueRequest{Payload: json.RawMessage(`{}`), IdempotencyKey: "enqueue"})
	if err != nil {
		t.Fatal(err)
	}
	if message.LastLSN != 14 {
		t.Fatalf("enqueue LSN = %d, want journal-assigned 14", message.LastLSN)
	}

	recoveredService, err := New(store, clock, Options{})
	if err != nil {
		t.Fatal(err)
	}
	recovered := recoveredService.(*service)
	page, err := recovered.ListMessages(context.Background(), "jobs", model.ListFilter{})
	if err != nil || len(page.Messages) != 1 || page.Messages[0].LastLSN != 14 {
		t.Fatalf("recovered page = %+v, %v", page, err)
	}
	replayed, wasReplay, err := recovered.Enqueue(context.Background(), "jobs", model.EnqueueRequest{Payload: json.RawMessage(`{}`), IdempotencyKey: "enqueue"})
	if err != nil || !wasReplay || replayed.LastLSN != 14 {
		t.Fatalf("replayed enqueue = %+v/%t, %v", replayed, wasReplay, err)
	}
}

func TestMutationResponsesCarryDurableLSN(t *testing.T) {
	engine, _, _ := newTestService(t, model.FIFO, false, 3)
	message := enqueueTest(t, engine, `{}`, 0, 0, "message")
	if message.LastLSN == 0 {
		t.Fatal("enqueue response omitted durable LSN")
	}
	delivery := receiveTest(t, engine, "receive")
	if delivery.Message.LastLSN <= message.LastLSN {
		t.Fatalf("receive LSN %d <= enqueue LSN %d", delivery.Message.LastLSN, message.LastLSN)
	}
}

func TestCapacityBounds(t *testing.T) {
	store := &memoryJournal{}
	clock := newFakeClock(time.Now())
	created, err := New(store, clock, Options{Limits: Limits{MaxMessages: 1, MaxMessagesPerQueue: 1, MaxPayloadBytes: 2}})
	if err != nil {
		t.Fatal(err)
	}
	engine := created.(*service)
	_, _, err = engine.CreateQueue(context.Background(), model.QueueConfig{Name: "jobs", Ordering: model.FIFO, DefaultVisibilityTimeout: time.Minute, MaxDeliveries: 1}, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := engine.Enqueue(context.Background(), "jobs", model.EnqueueRequest{Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := engine.Enqueue(context.Background(), "jobs", model.EnqueueRequest{Payload: json.RawMessage(`{}`)}); !IsCode(err, CodeCapacityExceeded) {
		t.Fatalf("capacity error = %v", err)
	}
}

func TestAdministrativeReadsExtendReadyAndClose(t *testing.T) {
	engine, _, _ := newTestService(t, model.FIFO, false, 3)
	message := enqueueTest(t, engine, `{}`, 0, 0, "message")
	queues, err := engine.ListQueues(context.Background())
	if err != nil || len(queues) != 1 || queues[0].Counts.Ready != 1 {
		t.Fatalf("queues = %+v, %v", queues, err)
	}
	queue, err := engine.GetQueue(context.Background(), "jobs")
	if err != nil || queue.Config.Name != "jobs" {
		t.Fatalf("queue = %+v, %v", queue, err)
	}
	delivery := receiveTest(t, engine, "receive")
	extended, replayed, err := engine.Extend(context.Background(), "jobs", model.ExtendRequest{
		MessageID: message.ID, Receipt: delivery.Receipt, VisibilityTimeout: 2 * time.Minute, IdempotencyKey: "extend",
	})
	if err != nil || replayed || !extended.LeaseUntil.After(delivery.LeaseUntil) {
		t.Fatalf("extend = %+v/%t, %v", extended, replayed, err)
	}
	extendedReplay, replayed, err := engine.Extend(context.Background(), "jobs", model.ExtendRequest{
		MessageID: message.ID, Receipt: delivery.Receipt, VisibilityTimeout: 2 * time.Minute, IdempotencyKey: "extend",
	})
	if err != nil || !replayed || !extendedReplay.LeaseUntil.Equal(extended.LeaseUntil) {
		t.Fatalf("extend replay = %+v/%t, %v", extendedReplay, replayed, err)
	}
	stats, err := engine.Stats(context.Background())
	if err != nil || stats.Queues != 1 || stats.Messages.InFlight != 1 || stats.DurableLSN == 0 {
		t.Fatalf("stats = %+v, %v", stats, err)
	}
	if err := engine.Ready(); err != nil {
		t.Fatal(err)
	}
	if err := engine.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := engine.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.GetQueue(context.Background(), "jobs"); !IsCode(err, CodeClosed) {
		t.Fatalf("closed get error = %v", err)
	}
	if err := engine.Ready(); !IsCode(err, CodeClosed) {
		t.Fatalf("closed ready error = %v", err)
	}
}

func TestValidationCursorAndErrors(t *testing.T) {
	if _, err := New(nil, newFakeClock(time.Now()), Options{}); !IsCode(err, CodeInvalid) {
		t.Fatalf("nil journal error = %v", err)
	}
	if _, err := New(&memoryJournal{}, newFakeClock(time.Now()), Options{Limits: Limits{MaxMessages: -1}}); !IsCode(err, CodeInvalid) {
		t.Fatalf("invalid limits error = %v", err)
	}
	engine, _, _ := newTestService(t, model.FIFO, false, 3)
	if _, _, err := engine.CreateQueue(context.Background(), model.QueueConfig{Name: "bad/name", Ordering: model.FIFO, DefaultVisibilityTimeout: time.Second, MaxDeliveries: 1}, ""); !IsCode(err, CodeInvalid) {
		t.Fatalf("invalid name error = %v", err)
	}
	if _, err := engine.GetQueue(context.Background(), "missing"); !IsCode(err, CodeNotFound) {
		t.Fatalf("missing queue error = %v", err)
	}
	if _, err := engine.ListMessages(context.Background(), "jobs", model.ListFilter{Cursor: "not-a-cursor"}); !IsCode(err, CodeInvalid) {
		t.Fatalf("cursor error = %v", err)
	}
	message := enqueueTest(t, engine, `{}`, 0, 0, "message")
	page, err := engine.ListMessages(context.Background(), "jobs", model.ListFilter{Limit: 1})
	if err != nil || len(page.Messages) != 1 || page.Messages[0].ID != message.ID {
		t.Fatalf("page = %+v, %v", page, err)
	}
	scope := cursorScope("jobs", "", false)
	if _, err := decodeCursor(encodeCursor(listCursor{Scope: scope, SnapshotLSN: message.LastLSN, HighWater: message.Sequence, Sequence: message.Sequence})); err != nil {
		t.Fatal(err)
	}
	wrapped := &Error{Code: CodeConflict, Message: "conflict", Cause: errors.New("cause")}
	if !errors.Is(wrapped, wrapped.Cause) || !IsCode(wrapped, CodeConflict) {
		t.Fatalf("wrapped error = %v", wrapped)
	}
	if conflict("x").Error() != "x" || invalid("y").Error() != "y" {
		t.Fatal("error constructors changed")
	}
	cursor := encodeCursor(listCursor{
		Scope: cursorScope("jobs", "", false), SnapshotLSN: 7, HighWater: 42, Sequence: 42, SnapshotGeneration: 3,
		SnapshotSecond: 42, SnapshotNanosecond: 7,
	})
	if cursor != "v3."+cursorScope("jobs", "", false)+".00000000000000000007.00000000000000000042.00000000000000000042.00000000000000000003.09223372036854775850.000000007" {
		t.Fatalf("cursor = %q", cursor)
	}
}

func TestRecoveryRejectsInvalidEnvelopesAndLeaseState(t *testing.T) {
	for name, payload := range map[string][]byte{
		"malformed": []byte(`{`),
		"version":   []byte(`{"kind":"state","version":99,"state":{}}`),
		"kind":      []byte(`{"kind":"unknown","version":1}`),
		"delta":     []byte(`{"kind":"delta","version":1}`),
	} {
		t.Run(name, func(t *testing.T) {
			store := &memoryJournal{records: []journal.Record{{LSN: 1, Payload: payload}}}
			if _, err := New(store, newFakeClock(time.Now()), Options{}); !IsCode(err, CodeStorageUnavailable) {
				t.Fatalf("error = %v", err)
			}
		})
	}
	invalidState := persistedState{Version: stateVersion, NextSequence: 1, Queues: map[string]*queueState{
		"jobs": {Config: model.QueueConfig{Name: "jobs", Ordering: model.FIFO, DefaultVisibilityTimeout: time.Second, MaxDeliveries: 1}, Messages: map[string]*model.Message{
			"m": {ID: "m", Queue: "jobs", State: model.StateLeased},
		}, Receipts: map[string]string{}, AckedAt: map[string]time.Time{}, AckedReceipts: map[string]ackReceipt{}},
	}, Idempotency: map[string]idempotencyRecord{}}
	stateBytes, _ := json.Marshal(invalidState)
	envelope, _ := json.Marshal(persistedEnvelope{Kind: "state", Version: stateVersion, State: stateBytes})
	if _, err := New(&memoryJournal{records: []journal.Record{{LSN: 1, Payload: envelope}}}, newFakeClock(time.Now()), Options{}); !IsCode(err, CodeStorageUnavailable) {
		t.Fatalf("invalid lease recovery error = %v", err)
	}
}

func TestStateDeltaRoundTripEveryFieldFamily(t *testing.T) {
	now := time.Now().UTC()
	before := persistedState{Version: stateVersion, NextSequence: 2, Queues: map[string]*queueState{
		"change": {
			Config: model.QueueConfig{Name: "change", Ordering: model.FIFO},
			Messages: map[string]*model.Message{
				"update": {ID: "update", State: model.StateReady},
				"delete": {ID: "delete", State: model.StateReady},
			},
			Receipts: map[string]string{"delete": "old"}, AckedAt: map[string]time.Time{"delete": now},
			AckedReceipts: map[string]ackReceipt{"delete": {MessageID: "delete", ExpiresAt: now}},
		},
		"delete-queue": {Config: model.QueueConfig{Name: "delete-queue"}, Messages: map[string]*model.Message{}, Receipts: map[string]string{}, AckedAt: map[string]time.Time{}, AckedReceipts: map[string]ackReceipt{}},
	}, Idempotency: map[string]idempotencyRecord{"delete": {Key: "delete"}}}
	later := now.Add(time.Minute)
	after := persistedState{Version: stateVersion, NextSequence: 5, Queues: map[string]*queueState{
		"change": {
			Config: model.QueueConfig{Name: "change", Ordering: model.LIFO},
			Messages: map[string]*model.Message{
				"update": {ID: "update", State: model.StateLeased, DeliveryCount: 1},
				"insert": {ID: "insert", State: model.StateDelayed},
			},
			Receipts: map[string]string{"update": "new"}, AckedAt: map[string]time.Time{"update": later},
			AckedReceipts: map[string]ackReceipt{"update": {MessageID: "update", ExpiresAt: later}},
		},
		"insert-queue": {Config: model.QueueConfig{Name: "insert-queue"}, Messages: map[string]*model.Message{}, Receipts: map[string]string{}, AckedAt: map[string]time.Time{}, AckedReceipts: map[string]ackReceipt{}},
	}, Idempotency: map[string]idempotencyRecord{"insert": {Key: "insert"}}}

	delta := diffState(before, after)
	clone, err := cloneStateForCheckpoint(before)
	if err != nil {
		t.Fatal(err)
	}
	applyDelta(&clone, delta)
	if !reflect.DeepEqual(clone, after) {
		t.Fatalf("round trip mismatch\n got: %#v\nwant: %#v\ndelta: %#v", clone, after, delta)
	}
	if len(delta.DeleteQueues) != 1 || len(delta.DeleteMessages["change"]) != 1 || len(delta.DeleteIdempotency) != 1 {
		t.Fatalf("delete delta = %#v", delta)
	}
	if delta.Receipts["change"]["delete"] != nil || delta.AckedAt["change"]["delete"] != nil || delta.AckedReceipts["change"]["delete"] != nil {
		t.Fatalf("pointer deletes missing: %#v", delta)
	}

	normalized := persistedState{Queues: map[string]*queueState{"change": {}}}
	receipt := "receipt"
	ackedAt := later
	ackedReceipt := ackReceipt{MessageID: "message", ExpiresAt: later}
	applyDelta(&normalized, stateDelta{
		Receipts:      map[string]map[string]*string{"change": {"message": &receipt}},
		AckedAt:       map[string]map[string]*time.Time{"change": {"message": &ackedAt}},
		AckedReceipts: map[string]map[string]*ackReceipt{"change": {"receipt": &ackedReceipt}},
	})
	queue := normalized.Queues["change"]
	if queue.Receipts["message"] != receipt || !queue.AckedAt["message"].Equal(ackedAt) || queue.AckedReceipts["receipt"] != ackedReceipt {
		t.Fatalf("nil index maps were not reconstructed: %#v", queue)
	}
}

func TestQueueAndOperationValidationMatrix(t *testing.T) {
	engine, _, _ := newTestService(t, model.FIFO, false, 3)
	ctx := context.Background()
	for _, config := range []model.QueueConfig{
		{Name: "x", Ordering: "unknown", DefaultVisibilityTimeout: time.Second, MaxDeliveries: 1},
		{Name: "x", Ordering: model.FIFO, DefaultDelay: -1, DefaultVisibilityTimeout: time.Second, MaxDeliveries: 1},
		{Name: "x", Ordering: model.FIFO, DefaultVisibilityTimeout: 0, MaxDeliveries: 1},
		{Name: "x", Ordering: model.FIFO, DefaultVisibilityTimeout: time.Second, MaxDeliveries: 0},
	} {
		if _, _, err := engine.CreateQueue(ctx, config, ""); !IsCode(err, CodeInvalid) {
			t.Fatalf("config %+v error = %v", config, err)
		}
	}
	if _, _, err := engine.CreateQueue(ctx, model.QueueConfig{Name: "jobs", Ordering: model.FIFO, DefaultVisibilityTimeout: time.Second, MaxDeliveries: 1}, ""); !IsCode(err, CodeConflict) {
		t.Fatalf("duplicate queue error = %v", err)
	}

	negative := -time.Second
	priority := int32(1)
	for _, request := range []model.EnqueueRequest{
		{Payload: json.RawMessage(`{`)},
		{Payload: json.RawMessage(`{}`), Delay: &negative},
		{Payload: json.RawMessage(`{}`), Priority: &priority},
	} {
		if _, _, err := engine.Enqueue(ctx, "jobs", request); !IsCode(err, CodeInvalid) {
			t.Fatalf("enqueue %+v error = %v", request, err)
		}
	}
	if _, _, err := engine.Enqueue(ctx, "missing", model.EnqueueRequest{Payload: json.RawMessage(`{}`)}); !IsCode(err, CodeNotFound) {
		t.Fatalf("missing enqueue error = %v", err)
	}

	for _, request := range []model.ReceiveRequest{{VisibilityTimeout: -1}, {WaitTimeout: -1}, {VisibilityTimeout: engine.limits.MaxVisibilityTimeout + 1}, {WaitTimeout: engine.limits.MaxWaitTimeout + 1}} {
		if _, _, err := engine.Receive(ctx, "jobs", request); !IsCode(err, CodeInvalid) {
			t.Fatalf("receive %+v error = %v", request, err)
		}
	}
	if _, _, err := engine.Receive(ctx, "missing", model.ReceiveRequest{IdempotencyKey: "empty"}); !IsCode(err, CodeNotFound) {
		t.Fatalf("missing receive error = %v", err)
	}

	if _, err := engine.Ack(ctx, "jobs", model.AckRequest{}); !IsCode(err, CodeInvalid) {
		t.Fatalf("empty ack error = %v", err)
	}
	if _, err := engine.Ack(ctx, "missing", model.AckRequest{MessageID: "m", Receipt: "r"}); !IsCode(err, CodeNotFound) {
		t.Fatalf("missing ack error = %v", err)
	}
	if _, _, err := engine.Nack(ctx, "jobs", model.NackRequest{}); !IsCode(err, CodeInvalid) {
		t.Fatalf("empty nack error = %v", err)
	}
	if _, _, err := engine.Nack(ctx, "jobs", model.NackRequest{MessageID: "m", Receipt: "r", Delay: -1}); !IsCode(err, CodeInvalid) {
		t.Fatalf("negative nack error = %v", err)
	}
	if _, _, err := engine.Extend(ctx, "jobs", model.ExtendRequest{}); !IsCode(err, CodeInvalid) {
		t.Fatalf("empty extend error = %v", err)
	}
	if _, _, err := engine.Extend(ctx, "jobs", model.ExtendRequest{MessageID: "m", Receipt: "r", VisibilityTimeout: -1}); !IsCode(err, CodeInvalid) {
		t.Fatalf("invalid extend error = %v", err)
	}
	if _, _, err := engine.Redrive(ctx, "jobs", model.RedriveRequest{}); !IsCode(err, CodeInvalid) {
		t.Fatalf("empty redrive error = %v", err)
	}
	if _, _, err := engine.Redrive(ctx, "jobs", model.RedriveRequest{MessageID: "m", Delay: -1}); !IsCode(err, CodeInvalid) {
		t.Fatalf("negative redrive error = %v", err)
	}
	if _, _, err := engine.Redrive(ctx, "missing", model.RedriveRequest{MessageID: "m"}); !IsCode(err, CodeNotFound) {
		t.Fatalf("missing redrive queue = %v", err)
	}
	if _, _, err := engine.Redrive(ctx, "jobs", model.RedriveRequest{MessageID: "m"}); !IsCode(err, CodeNotFound) {
		t.Fatalf("missing redrive message = %v", err)
	}
	if _, err := engine.ListMessages(ctx, "jobs", model.ListFilter{Limit: -1}); !IsCode(err, CodeInvalid) {
		t.Fatalf("list limit error = %v", err)
	}
	if _, err := engine.ListDeadLetters(ctx, "missing", model.ListFilter{}); !IsCode(err, CodeNotFound) {
		t.Fatalf("missing dead list error = %v", err)
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := engine.ListQueues(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("list cancel = %v", err)
	}
	if _, err := engine.GetQueue(cancelled, "jobs"); !errors.Is(err, context.Canceled) {
		t.Fatalf("get cancel = %v", err)
	}
	if _, _, err := engine.Receive(cancelled, "jobs", model.ReceiveRequest{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("receive cancel = %v", err)
	}
	if _, err := engine.Stats(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("stats cancel = %v", err)
	}
	if err := engine.Compact(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("compact cancel = %v", err)
	}
}

func TestMaterializationAndTombstonePruning(t *testing.T) {
	engine, _, clock := newTestService(t, model.FIFO, false, 1)
	delayed := enqueueTest(t, engine, `{}`, 0, time.Minute, "delayed")
	ready := enqueueTest(t, engine, `{}`, 0, 0, "ready")
	delivery := receiveTest(t, engine, "receive")
	if delivery.Message.ID != ready.ID {
		t.Fatalf("received %s want %s", delivery.Message.ID, ready.ID)
	}
	if _, err := engine.Ack(context.Background(), "jobs", model.AckRequest{MessageID: ready.ID, Receipt: delivery.Receipt}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(engine.limits.AckTombstoneRetention + time.Minute)
	engine.mu.Lock()
	changed := engine.materializeLocked(engine.state.Queues["jobs"], clock.Now())
	changed = engine.pruneRetentionLocked(engine.state.Queues["jobs"], clock.Now()) || changed
	_, ackedExists := engine.state.Queues["jobs"].Messages[ready.ID]
	state := engine.state.Queues["jobs"].Messages[delayed.ID].State
	engine.mu.Unlock()
	if !changed || ackedExists || state != model.StateReady {
		t.Fatalf("changed=%t acked=%t state=%s", changed, ackedExists, state)
	}
}

func TestRecoveredStateInvariantVariants(t *testing.T) {
	now := time.Now()
	cases := map[string]persistedState{
		"nil-queue":            {Version: stateVersion, Queues: map[string]*queueState{"q": nil}},
		"receipt-unknown":      {Version: stateVersion, Queues: map[string]*queueState{"q": {Messages: map[string]*model.Message{}, Receipts: map[string]string{"m": "r"}}}},
		"receipt-nonleased":    {Version: stateVersion, Queues: map[string]*queueState{"q": {Messages: map[string]*model.Message{"m": {ID: "m", State: model.StateReady}}, Receipts: map[string]string{"m": "r"}}}},
		"leased-missing-times": {Version: stateVersion, Queues: map[string]*queueState{"q": {Messages: map[string]*model.Message{"m": {ID: "m", State: model.StateLeased}}, Receipts: map[string]string{"m": "r"}}}},
		"nonleased-times":      {Version: stateVersion, Queues: map[string]*queueState{"q": {Messages: map[string]*model.Message{"m": {ID: "m", State: model.StateReady, LeasedAt: &now, LeaseUntil: &now}}}}},
	}
	for name, state := range cases {
		t.Run(name, func(t *testing.T) {
			service := &service{state: state}
			if err := service.validateRecoveredState(); err == nil {
				t.Fatal("invalid state accepted")
			}
		})
	}
	valid := &service{
		limits: DefaultLimits(),
		state: persistedState{Version: stateVersion, NextSequence: 1, Queues: map[string]*queueState{
			"q": {Config: model.QueueConfig{Name: "q", Ordering: model.FIFO, DefaultVisibilityTimeout: time.Second, MaxDeliveries: 1, CreatedAt: now}, Messages: nil, Receipts: nil, AckedAt: nil, AckedReceipts: nil},
		}, Idempotency: nil},
	}
	if err := valid.validateRecoveredState(); err != nil {
		t.Fatal(err)
	}
	queue := valid.state.Queues["q"]
	if valid.state.NextSequence != 1 || queue.Messages == nil || queue.Receipts == nil || queue.AckedAt == nil || queue.AckedReceipts == nil || valid.state.Idempotency == nil {
		t.Fatalf("normalized = %#v", valid.state)
	}
}

func TestRemainingEngineHelpersAndEmptyReceiveReplay(t *testing.T) {
	engine, _, clock := newTestService(t, model.FIFO, false, 3)
	ctx := context.Background()
	delivery, replayed, err := engine.Receive(ctx, "jobs", model.ReceiveRequest{IdempotencyKey: "empty"})
	if err != nil || delivery != nil || replayed {
		t.Fatalf("empty receive = %+v/%t, %v", delivery, replayed, err)
	}
	delivery, replayed, err = engine.Receive(ctx, "jobs", model.ReceiveRequest{IdempotencyKey: "empty"})
	if err != nil || delivery != nil || !replayed {
		t.Fatalf("empty replay = %+v/%t, %v", delivery, replayed, err)
	}

	engine.mu.Lock()
	engine.state.Idempotency["expired"] = idempotencyRecord{ExpiresAt: clock.Now().Add(-time.Second)}
	engine.pruneIdempotencyLocked(clock.Now())
	_, exists := engine.state.Idempotency["expired"]
	engine.mu.Unlock()
	if exists {
		t.Fatal("expired idempotency record retained")
	}

	now := clock.Now()
	past := now.Add(-time.Hour)
	if got, err := boundedAvailableAt(now, 0, time.Hour, &past); err != nil || !got.Equal(now) {
		t.Fatalf("clamped time = %v, %v", got, err)
	}
	future := now.Add(time.Hour)
	if got, err := boundedAvailableAt(now, 0, time.Hour, &future); err != nil || !got.Equal(future) {
		t.Fatalf("requested future = %v, %v", got, err)
	}
	if got := normalizeReason("  failure  "); got != "failure" {
		t.Fatalf("reason = %q", got)
	}

	if err := validateQueueName(""); !IsCode(err, CodeInvalid) {
		t.Fatalf("empty name = %v", err)
	}
	if err := validateQueueName(strings.Repeat("x", 129)); !IsCode(err, CodeInvalid) {
		t.Fatalf("long name = %v", err)
	}
	if err := validateQueueName("valid.name-1"); err != nil {
		t.Fatal(err)
	}

	if _, err := cloneQueueState(nil); err != nil {
		t.Fatal(err)
	}
	if result := appendMapSlice(nil, "q", "id"); len(result["q"]) != 1 {
		t.Fatalf("map slice = %#v", result)
	}
	if !deltaHasChanges(stateDelta{NextSequence: 2}, 1) || deltaHasChanges(stateDelta{NextSequence: 1}, 1) {
		t.Fatal("delta change detection failed")
	}
}

func TestSnapshotCursorExcludesLaterEnqueuesAndParsesExactly(t *testing.T) {
	engine, _, _ := newTestService(t, model.FIFO, false, 3)
	first := enqueueTest(t, engine, `{"id":1}`, 0, 0, "first")
	second := enqueueTest(t, engine, `{"id":2}`, 0, 0, "second")
	third := enqueueTest(t, engine, `{"id":3}`, 0, 0, "third")

	page, err := engine.ListMessages(context.Background(), "jobs", model.ListFilter{Limit: 1})
	if err != nil || len(page.Messages) != 1 || page.Messages[0].ID != first.ID || page.NextCursor == "" {
		t.Fatalf("first page = %+v, %v", page, err)
	}
	snapshotLSN := page.SnapshotLSN
	later := enqueueTest(t, engine, `{"id":4}`, 0, 0, "later")
	page, err = engine.ListMessages(context.Background(), "jobs", model.ListFilter{Limit: 10, Cursor: page.NextCursor})
	if err != nil || page.SnapshotLSN != snapshotLSN || len(page.Messages) != 2 || page.Messages[0].ID != second.ID || page.Messages[1].ID != third.ID {
		t.Fatalf("continued page = %+v, %v", page, err)
	}
	for _, message := range page.Messages {
		if message.ID == later.ID {
			t.Fatal("continued page included post-snapshot enqueue")
		}
	}

	valid := encodeCursor(listCursor{Scope: cursorScope("jobs", "", false), SnapshotLSN: 1, HighWater: 2, Sequence: 1})
	for _, malformed := range []string{"1", valid + "x", valid[:len(valid)-1], "v1." + valid[3:], encodeCursor(listCursor{Scope: cursorScope("jobs", "", false), SnapshotLSN: 1, HighWater: 1, Sequence: 2})} {
		if _, err := decodeCursor(malformed); !IsCode(err, CodeInvalid) {
			t.Fatalf("cursor %q error = %v", malformed, err)
		}
	}
}

func TestCursorRejectsCrossScopeReuse(t *testing.T) {
	engine, _, _ := newTestService(t, model.FIFO, false, 3)
	for _, key := range []string{"first", "second"} {
		enqueueTest(t, engine, `{}`, 0, 0, key)
	}
	if _, _, err := engine.CreateQueue(context.Background(), model.QueueConfig{Name: "other", Ordering: model.FIFO, DefaultVisibilityTimeout: time.Minute, MaxDeliveries: 3}, "other"); err != nil {
		t.Fatal(err)
	}
	page, err := engine.ListMessages(context.Background(), "jobs", model.ListFilter{Limit: 1})
	if err != nil || page.NextCursor == "" {
		t.Fatalf("live page = %+v, %v", page, err)
	}
	for name, list := range map[string]func() error{
		"queue": func() error {
			_, err := engine.ListMessages(context.Background(), "other", model.ListFilter{Limit: 1, Cursor: page.NextCursor})
			return err
		},
		"state": func() error {
			_, err := engine.ListMessages(context.Background(), "jobs", model.ListFilter{State: model.StateReady, Limit: 1, Cursor: page.NextCursor})
			return err
		},
		"live-to-dead": func() error {
			_, err := engine.ListDeadLetters(context.Background(), "jobs", model.ListFilter{Limit: 1, Cursor: page.NextCursor})
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := list(); !IsCode(err, CodeInvalid) {
				t.Fatalf("scope reuse error = %v", err)
			}
		})
	}

	deadEngine, _, _ := newTestService(t, model.FIFO, false, 1)
	for _, key := range []string{"dead-first", "dead-second"} {
		message := enqueueTest(t, deadEngine, `{}`, 0, 0, key)
		delivery := receiveTest(t, deadEngine, "receive-"+key)
		if _, _, err := deadEngine.Nack(context.Background(), "jobs", model.NackRequest{MessageID: message.ID, Receipt: delivery.Receipt, IdempotencyKey: "nack-" + key}); err != nil {
			t.Fatal(err)
		}
	}
	deadPage, err := deadEngine.ListDeadLetters(context.Background(), "jobs", model.ListFilter{Limit: 1})
	if err != nil || deadPage.NextCursor == "" {
		t.Fatalf("dead page = %+v, %v", deadPage, err)
	}
	if _, err := deadEngine.ListMessages(context.Background(), "jobs", model.ListFilter{Limit: 1, Cursor: deadPage.NextCursor}); !IsCode(err, CodeInvalid) {
		t.Fatalf("dead-to-live error = %v", err)
	}
}

func TestCursorSnapshotExcludesLaterStateMutations(t *testing.T) {
	engine, _, _ := newTestService(t, model.FIFO, false, 3)
	first := enqueueTest(t, engine, `{"id":1}`, 0, 0, "first")
	second := enqueueTest(t, engine, `{"id":2}`, 0, 0, "second")
	third := enqueueTest(t, engine, `{"id":3}`, 0, 0, "third")
	page, err := engine.ListMessages(context.Background(), "jobs", model.ListFilter{Limit: 1})
	if err != nil || len(page.Messages) != 1 || page.Messages[0].ID != first.ID || page.NextCursor == "" {
		t.Fatalf("first page = %+v, %v", page, err)
	}
	firstDelivery := receiveTest(t, engine, "receive-first")
	if _, err := engine.Ack(context.Background(), "jobs", model.AckRequest{MessageID: first.ID, Receipt: firstDelivery.Receipt}); err != nil {
		t.Fatal(err)
	}
	secondDelivery := receiveTest(t, engine, "receive-second")
	if secondDelivery.Message.ID != second.ID {
		t.Fatalf("second delivery = %s", secondDelivery.Message.ID)
	}
	page, err = engine.ListMessages(context.Background(), "jobs", model.ListFilter{Limit: 10, Cursor: page.NextCursor})
	if err != nil || len(page.Messages) != 1 || page.Messages[0].ID != third.ID {
		t.Fatalf("continued snapshot = %+v, %v", page, err)
	}
}

func TestRedriveRejectsPriorityOverrideWhenDisabled(t *testing.T) {
	engine, _, _ := newTestService(t, model.FIFO, false, 1)
	message := enqueueTest(t, engine, `{}`, 0, 0, "message")
	delivery := receiveTest(t, engine, "receive")
	if _, _, err := engine.Nack(context.Background(), "jobs", model.NackRequest{MessageID: message.ID, Receipt: delivery.Receipt}); err != nil {
		t.Fatal(err)
	}
	priority := int32(99)
	if _, _, err := engine.Redrive(context.Background(), "jobs", model.RedriveRequest{MessageID: message.ID, Priority: &priority, IdempotencyKey: "redrive"}); !IsCode(err, CodeInvalid) {
		t.Fatalf("priority-disabled redrive error = %v", err)
	}
}

func TestRedriveSourceLastLSNMatchesStoredSourceAndReplay(t *testing.T) {
	engine, _, _ := newTestService(t, model.FIFO, false, 1)
	message := enqueueTest(t, engine, `{}`, 0, 0, "message")
	delivery := receiveTest(t, engine, "receive")
	dead, _, err := engine.Nack(context.Background(), "jobs", model.NackRequest{MessageID: message.ID, Receipt: delivery.Receipt, IdempotencyKey: "nack"})
	if err != nil {
		t.Fatal(err)
	}
	result, replayed, err := engine.Redrive(context.Background(), "jobs", model.RedriveRequest{MessageID: message.ID, IdempotencyKey: "redrive"})
	if err != nil || replayed {
		t.Fatalf("redrive = %+v/%t, %v", result, replayed, err)
	}
	if result.Source.LastLSN != dead.LastLSN || result.Child.LastLSN <= dead.LastLSN {
		t.Fatalf("redrive LSNs = source %d child %d, dead source %d", result.Source.LastLSN, result.Child.LastLSN, dead.LastLSN)
	}
	page, err := engine.ListDeadLetters(context.Background(), "jobs", model.ListFilter{})
	if err != nil || len(page.Messages) != 1 || page.Messages[0].LastLSN != result.Source.LastLSN {
		t.Fatalf("stored source = %+v, %v", page, err)
	}
	again, replayed, err := engine.Redrive(context.Background(), "jobs", model.RedriveRequest{MessageID: message.ID, IdempotencyKey: "redrive"})
	if err != nil || !replayed || again.Source.LastLSN != result.Source.LastLSN || again.Child.LastLSN != result.Child.LastLSN {
		t.Fatalf("redrive replay = %+v/%t, %v", again, replayed, err)
	}
}

func TestRedriveMaterializedSourceLastLSNSurvivesRestartAndReplay(t *testing.T) {
	engine, store, clock := newTestService(t, model.FIFO, false, 1)
	message := enqueueTest(t, engine, `{}`, 0, 0, "message")
	receiveTest(t, engine, "receive")
	clock.Advance(time.Minute)
	result, replayed, err := engine.Redrive(context.Background(), "jobs", model.RedriveRequest{MessageID: message.ID, IdempotencyKey: "redrive"})
	if err != nil || replayed || result.Source.State != model.StateDead || result.Source.LastLSN != result.Child.LastLSN {
		t.Fatalf("redrive = %+v/%t, %v", result, replayed, err)
	}
	recoveredService, err := New(store, clock, Options{})
	if err != nil {
		t.Fatal(err)
	}
	recovered := recoveredService.(*service)
	page, err := recovered.ListDeadLetters(context.Background(), "jobs", model.ListFilter{})
	if err != nil || len(page.Messages) != 1 || page.Messages[0].LastLSN != result.Source.LastLSN {
		t.Fatalf("recovered source = %+v, %v", page, err)
	}
	again, replayed, err := recovered.Redrive(context.Background(), "jobs", model.RedriveRequest{MessageID: message.ID, IdempotencyKey: "redrive"})
	if err != nil || !replayed || again.Source.LastLSN != result.Source.LastLSN || again.Child.LastLSN != result.Child.LastLSN {
		t.Fatalf("replayed redrive = %+v/%t, %v", again, replayed, err)
	}
}

func TestRecoveryMaterializesExpiredLeasesBeforeInFlightCapacity(t *testing.T) {
	now := time.Date(2026, 8, 26, 20, 0, 0, 0, time.UTC)
	leasedAt := now.Add(-2 * time.Minute)
	leaseUntil := now.Add(-time.Minute)
	state := persistedState{Version: stateVersion, NextSequence: 3, Queues: map[string]*queueState{
		"jobs": {
			Config: model.QueueConfig{Name: "jobs", Ordering: model.FIFO, DefaultVisibilityTimeout: time.Minute, MaxDeliveries: 2, CreatedAt: now.Add(-time.Hour)},
			Messages: map[string]*model.Message{
				"retry": {ID: "retry", Queue: "jobs", Payload: json.RawMessage(`{}`), Sequence: 1, EnqueuedAt: leasedAt, AvailableAt: leasedAt, State: model.StateLeased, DeliveryCount: 1, LeaseEpoch: 1, LeasedAt: &leasedAt, LeaseUntil: &leaseUntil, LastLSN: 1},
				"dead":  {ID: "dead", Queue: "jobs", Payload: json.RawMessage(`{}`), Sequence: 2, EnqueuedAt: leasedAt, AvailableAt: leasedAt, State: model.StateLeased, DeliveryCount: 2, LeaseEpoch: 2, LeasedAt: &leasedAt, LeaseUntil: &leaseUntil, LastLSN: 1},
			},
			Receipts: map[string]string{"retry": "retry.1.0123456789abcdef0123456789abcdef", "dead": "dead.2.abcdef0123456789abcdef0123456789"}, AckedAt: map[string]time.Time{}, AckedReceipts: map[string]ackReceipt{},
		},
	}, Idempotency: map[string]idempotencyRecord{}}
	stateBytes, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := json.Marshal(persistedEnvelope{Kind: "state", Version: stateVersion, State: stateBytes})
	if err != nil {
		t.Fatal(err)
	}
	store := &memoryJournal{records: []journal.Record{{LSN: 1, Payload: envelope}}}
	created, err := New(store, newFakeClock(now), Options{Limits: Limits{MaxInFlight: 1, MaxInFlightPerQueue: 1}})
	if err != nil {
		t.Fatalf("recover expired leases: %v", err)
	}
	engine := created.(*service)
	if engine.totalInFlight != 0 || engine.inFlightByQueue["jobs"] != 0 {
		t.Fatalf("recovered in-flight counters = %d/%d", engine.totalInFlight, engine.inFlightByQueue["jobs"])
	}
	if got := engine.state.Queues["jobs"].Messages["retry"].State; got != model.StateReady {
		t.Fatalf("retry state = %s", got)
	}
	if got := engine.state.Queues["jobs"].Messages["dead"].State; got != model.StateDead {
		t.Fatalf("dead state = %s", got)
	}
	if len(engine.state.Queues["jobs"].Receipts) != 0 {
		t.Fatalf("recovered receipts = %#v", engine.state.Queues["jobs"].Receipts)
	}
}

func TestRecoveryRejectsQueueDefaultsBeyondCurrentLimits(t *testing.T) {
	now := time.Date(2026, 8, 26, 20, 0, 0, 0, time.UTC)
	state := persistedState{Version: stateVersion, NextSequence: 1, Queues: map[string]*queueState{
		"jobs": {
			Config:   model.QueueConfig{Name: "jobs", Ordering: model.FIFO, DefaultDelay: 2 * time.Hour, DefaultVisibilityTimeout: 2 * time.Hour, MaxDeliveries: 1, CreatedAt: now},
			Messages: map[string]*model.Message{}, Receipts: map[string]string{}, AckedAt: map[string]time.Time{}, AckedReceipts: map[string]ackReceipt{},
		},
	}, Idempotency: map[string]idempotencyRecord{}}
	stateBytes, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := json.Marshal(persistedEnvelope{Kind: "state", Version: stateVersion, State: stateBytes})
	if err != nil {
		t.Fatal(err)
	}
	store := &memoryJournal{records: []journal.Record{{LSN: 1, Payload: envelope}}}
	limits := Limits{MaxDelay: time.Minute, MaxVisibilityTimeout: time.Minute}
	if _, err := New(store, newFakeClock(now), Options{Limits: limits}); !IsCode(err, CodeStorageUnavailable) {
		t.Fatalf("recovery error = %v", err)
	}
	if _, err := boundedAvailableAt(now, 2*time.Minute, time.Minute, nil); !IsCode(err, CodeInvalid) {
		t.Fatalf("bounded helper error = %v", err)
	}
}

func TestCursorRetentionSnapshotSurvivesMutationAndRejectsAfterCompaction(t *testing.T) {
	engine, _, clock := newTestService(t, model.FIFO, false, 1)
	ackedIDs := make([]string, 0, 3)
	for _, key := range []string{"acked-1", "acked-2", "acked-3"} {
		message := enqueueTest(t, engine, `{}`, 0, 0, key)
		delivery := receiveTest(t, engine, "receive-"+key)
		if _, err := engine.Ack(context.Background(), "jobs", model.AckRequest{MessageID: message.ID, Receipt: delivery.Receipt}); err != nil {
			t.Fatal(err)
		}
		ackedIDs = append(ackedIDs, message.ID)
	}
	dead := enqueueTest(t, engine, `{}`, 0, 0, "dead")
	deadDelivery := receiveTest(t, engine, "receive-dead")
	if _, _, err := engine.Nack(context.Background(), "jobs", model.NackRequest{MessageID: dead.ID, Receipt: deadDelivery.Receipt}); err != nil {
		t.Fatal(err)
	}

	page, err := engine.ListMessages(context.Background(), "jobs", model.ListFilter{State: model.StateAcked, Limit: 1})
	if err != nil || len(page.Messages) != 1 || page.Messages[0].ID != ackedIDs[0] || page.NextCursor == "" {
		t.Fatalf("first page = %+v, %v", page, err)
	}
	cursor := page.NextCursor
	clock.Advance(engine.limits.AckTombstoneRetention)
	if _, _, err := engine.Redrive(context.Background(), "jobs", model.RedriveRequest{MessageID: dead.ID, IdempotencyKey: "redrive"}); err != nil {
		t.Fatal(err)
	}
	page, err = engine.ListMessages(context.Background(), "jobs", model.ListFilter{State: model.StateAcked, Limit: 10, Cursor: cursor})
	if err != nil || len(page.Messages) != 2 || page.Messages[0].ID != ackedIDs[1] || page.Messages[1].ID != ackedIDs[2] {
		t.Fatalf("continued page after redrive = %+v, %v", page, err)
	}
	if err := engine.Compact(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.ListMessages(context.Background(), "jobs", model.ListFilter{State: model.StateAcked, Limit: 10, Cursor: cursor}); !IsCode(err, CodeInvalid) {
		t.Fatalf("old cursor after compaction error = %v", err)
	}
	fresh, err := engine.ListMessages(context.Background(), "jobs", model.ListFilter{State: model.StateAcked, Limit: 10})
	if err != nil || len(fresh.Messages) != 0 {
		t.Fatalf("fresh page after compaction = %+v, %v", fresh, err)
	}
}

func TestCursorSnapshotFreezesTimeDerivedMembership(t *testing.T) {
	engine, _, clock := newTestService(t, model.FIFO, false, 3)
	first := enqueueTest(t, engine, `{"id":1}`, 0, 0, "first")
	second := enqueueTest(t, engine, `{"id":2}`, 0, 0, "second")
	delayed := enqueueTest(t, engine, `{"id":3}`, 0, time.Minute, "delayed")
	page, err := engine.ListMessages(context.Background(), "jobs", model.ListFilter{State: model.StateReady, Limit: 1})
	if err != nil || len(page.Messages) != 1 || page.Messages[0].ID != first.ID || page.NextCursor == "" {
		t.Fatalf("first page = %+v, %v", page, err)
	}
	clock.Advance(time.Minute)
	page, err = engine.ListMessages(context.Background(), "jobs", model.ListFilter{State: model.StateReady, Limit: 10, Cursor: page.NextCursor})
	if err != nil || len(page.Messages) != 1 || page.Messages[0].ID != second.ID {
		t.Fatalf("continued page = %+v, %v", page, err)
	}
	for _, message := range page.Messages {
		if message.ID == delayed.ID {
			t.Fatal("continued snapshot included time-promoted message")
		}
	}
}

func TestCursorSnapshotFreezesLeaseExpiryMembership(t *testing.T) {
	engine, _, clock := newTestService(t, model.FIFO, false, 3)
	first := enqueueTest(t, engine, `{"id":1}`, 0, 0, "first")
	second := enqueueTest(t, engine, `{"id":2}`, 0, 0, "second")
	leased := enqueueTest(t, engine, `{"id":3}`, 0, 0, "leased")
	delivery := receiveTest(t, engine, "receive")
	if delivery.Message.ID != first.ID {
		t.Fatalf("leased %s want %s", delivery.Message.ID, first.ID)
	}
	page, err := engine.ListMessages(context.Background(), "jobs", model.ListFilter{State: model.StateReady, Limit: 1})
	if err != nil || len(page.Messages) != 1 || page.Messages[0].ID != second.ID || page.NextCursor == "" {
		t.Fatalf("first page = %+v, %v", page, err)
	}
	clock.Advance(time.Minute)
	page, err = engine.ListMessages(context.Background(), "jobs", model.ListFilter{State: model.StateReady, Limit: 10, Cursor: page.NextCursor})
	if err != nil || len(page.Messages) != 1 || page.Messages[0].ID != leased.ID {
		t.Fatalf("continued page = %+v, %v", page, err)
	}
	for _, message := range page.Messages {
		if message.ID == first.ID {
			t.Fatal("continued snapshot included time-expired lease")
		}
	}
}

func TestReturnedMessageTimesDoNotAliasLiveState(t *testing.T) {
	engine, _, clock := newTestService(t, model.FIFO, false, 1)
	message := enqueueTest(t, engine, `{}`, 0, 0, "message")
	delivery := receiveTest(t, engine, "receive")
	originalLeasedAt := *delivery.Message.LeasedAt
	originalLeaseUntil := *delivery.Message.LeaseUntil
	*delivery.Message.LeasedAt = delivery.Message.LeasedAt.Add(-time.Hour)
	*delivery.Message.LeaseUntil = delivery.Message.LeaseUntil.Add(-time.Hour)

	leasedPage, err := engine.ListMessages(context.Background(), "jobs", model.ListFilter{State: model.StateLeased})
	if err != nil || len(leasedPage.Messages) != 1 {
		t.Fatalf("leased page = %+v, %v", leasedPage, err)
	}
	*leasedPage.Messages[0].LeasedAt = leasedPage.Messages[0].LeasedAt.Add(-2 * time.Hour)
	*leasedPage.Messages[0].LeaseUntil = leasedPage.Messages[0].LeaseUntil.Add(-2 * time.Hour)
	engine.mu.Lock()
	stored := engine.state.Queues["jobs"].Messages[message.ID]
	if !stored.LeasedAt.Equal(originalLeasedAt) || !stored.LeaseUntil.Equal(originalLeaseUntil) {
		engine.mu.Unlock()
		t.Fatalf("stored lease mutated: %+v", stored)
	}
	engine.mu.Unlock()

	dead, _, err := engine.Nack(context.Background(), "jobs", model.NackRequest{MessageID: message.ID, Receipt: delivery.Receipt, IdempotencyKey: "nack"})
	if err != nil || dead.DeadAt == nil {
		t.Fatalf("dead nack = %+v, %v", dead, err)
	}
	originalDeadAt := *dead.DeadAt
	*dead.DeadAt = dead.DeadAt.Add(-time.Hour)
	deadPage, err := engine.ListDeadLetters(context.Background(), "jobs", model.ListFilter{})
	if err != nil || len(deadPage.Messages) != 1 || deadPage.Messages[0].DeadAt == nil {
		t.Fatalf("dead page = %+v, %v", deadPage, err)
	}
	*deadPage.Messages[0].DeadAt = deadPage.Messages[0].DeadAt.Add(-2 * time.Hour)

	redrive, _, err := engine.Redrive(context.Background(), "jobs", model.RedriveRequest{MessageID: message.ID, IdempotencyKey: "redrive"})
	if err != nil || redrive.Source.DeadAt == nil {
		t.Fatalf("redrive = %+v, %v", redrive, err)
	}
	*redrive.Source.DeadAt = redrive.Source.DeadAt.Add(-3 * time.Hour)
	engine.mu.Lock()
	stored = engine.state.Queues["jobs"].Messages[message.ID]
	storedDeadAt := *stored.DeadAt
	counts := counts(engine.state.Queues["jobs"], clock.Now())
	engine.mu.Unlock()
	if !storedDeadAt.Equal(originalDeadAt) {
		t.Fatalf("stored dead time = %v, want %v", storedDeadAt, originalDeadAt)
	}
	if counts.Dead != 1 || counts.Ready != 1 || counts.Total != 2 {
		t.Fatalf("counts after caller mutation = %+v", counts)
	}
}

func TestAbsoluteSchedulingAndClockArithmeticAreBounded(t *testing.T) {
	engine, _, clock := newTestService(t, model.FIFO, false, 3)
	tooLate := clock.Now().Add(engine.limits.MaxDelay + time.Nanosecond)
	if _, _, err := engine.Enqueue(context.Background(), "jobs", model.EnqueueRequest{Payload: json.RawMessage(`{}`), AvailableAt: &tooLate}); !IsCode(err, CodeInvalid) {
		t.Fatalf("enqueue available-at error = %v", err)
	}
	message := enqueueTest(t, engine, `{}`, 0, 0, "message")
	delivery := receiveTest(t, engine, "receive")
	if _, _, err := engine.Nack(context.Background(), "jobs", model.NackRequest{MessageID: message.ID, Receipt: delivery.Receipt, Delay: engine.limits.MaxDelay + time.Nanosecond}); !IsCode(err, CodeInvalid) {
		t.Fatalf("nack delay error = %v", err)
	}
	if _, err := checkedAdd(time.Unix(1<<63-2, 0), 10*time.Second); !IsCode(err, CodeInvalid) {
		t.Fatalf("overflow error = %v", err)
	}
	if _, err := checkedAdd(clock.Now(), -time.Nanosecond); !IsCode(err, CodeInvalid) {
		t.Fatalf("negative duration error = %v", err)
	}
	overflowingNow := time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)
	if _, err := boundedAvailableAt(overflowingNow, 0, time.Second, nil); !IsCode(err, CodeInvalid) {
		t.Fatalf("max delay overflow error = %v", err)
	}
	if _, err := boundedAvailableAt(overflowingNow, time.Second, 0, nil); !IsCode(err, CodeInvalid) {
		t.Fatalf("delay overflow error = %v", err)
	}
	requested := clock.Now().Add(engine.limits.MaxDelay + time.Nanosecond)
	if _, err := boundedAvailableAt(clock.Now(), 0, engine.limits.MaxDelay, &requested); !IsCode(err, CodeInvalid) {
		t.Fatalf("requested bound error = %v", err)
	}
}

func TestExpiredAckTombstoneReleasesMessageCapacity(t *testing.T) {
	store := &memoryJournal{}
	clock := newFakeClock(time.Date(2026, 8, 26, 20, 0, 0, 0, time.UTC))
	created, err := New(store, clock, Options{Limits: Limits{MaxMessages: 1, MaxMessagesPerQueue: 1, AckTombstoneRetention: time.Second}})
	if err != nil {
		t.Fatal(err)
	}
	engine := created.(*service)
	if _, _, err := engine.CreateQueue(context.Background(), model.QueueConfig{Name: "jobs", Ordering: model.FIFO, DefaultVisibilityTimeout: time.Second, MaxDeliveries: 1}, ""); err != nil {
		t.Fatal(err)
	}
	message := enqueueTest(t, engine, `{}`, 0, 0, "first")
	delivery := receiveTest(t, engine, "receive")
	if _, err := engine.Ack(context.Background(), "jobs", model.AckRequest{MessageID: message.ID, Receipt: delivery.Receipt}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	engine.mu.Lock()
	engine.pruneRetentionLocked(engine.state.Queues["jobs"], clock.Now())
	total := engine.totalMessages
	engine.mu.Unlock()
	if total != 0 {
		t.Fatalf("total messages after prune = %d", total)
	}
	if _, _, err := engine.Enqueue(context.Background(), "jobs", model.EnqueueRequest{Payload: json.RawMessage(`{}`), IdempotencyKey: "second"}); err != nil {
		t.Fatalf("enqueue after tombstone expiry: %v", err)
	}
}

func TestWaiterAndInFlightAdmissionAccounting(t *testing.T) {
	store := &memoryJournal{}
	clock := newFakeClock(time.Date(2026, 8, 26, 20, 0, 0, 0, time.UTC))
	created, err := New(store, clock, Options{Limits: Limits{MaxWaiters: 1, MaxWaitersPerQueue: 1, MaxInFlight: 1, MaxInFlightPerQueue: 1}})
	if err != nil {
		t.Fatal(err)
	}
	engine := created.(*service)
	if err := engine.registerWaiter("missing"); !IsCode(err, CodeNotFound) {
		t.Fatalf("missing queue waiter error = %v", err)
	}
	if _, _, err := engine.CreateQueue(context.Background(), model.QueueConfig{Name: "jobs", Ordering: model.FIFO, DefaultVisibilityTimeout: time.Minute, MaxDeliveries: 3}, ""); err != nil {
		t.Fatal(err)
	}
	if err := engine.registerWaiter("jobs"); err != nil {
		t.Fatal(err)
	}
	if err := engine.registerWaiter("jobs"); !IsCode(err, CodeCapacityExceeded) {
		t.Fatalf("second waiter error = %v", err)
	}
	engine.releaseWaiter("jobs")
	engine.releaseWaiter("jobs")
	if engine.totalWaiters != 0 || engine.waitersByQueue["jobs"] != 0 {
		t.Fatalf("waiter counters = %d/%d", engine.totalWaiters, engine.waitersByQueue["jobs"])
	}

	first := enqueueTest(t, engine, `{"id":1}`, 0, 0, "first")
	second := enqueueTest(t, engine, `{"id":2}`, 0, 0, "second")
	firstDelivery := receiveTest(t, engine, "first-receive")
	if firstDelivery.Message.ID != first.ID {
		t.Fatalf("first delivery = %s", firstDelivery.Message.ID)
	}
	if _, _, err := engine.Receive(context.Background(), "jobs", model.ReceiveRequest{}); !IsCode(err, CodeCapacityExceeded) {
		t.Fatalf("second receive error = %v", err)
	}
	if _, err := engine.Ack(context.Background(), "jobs", model.AckRequest{MessageID: first.ID, Receipt: firstDelivery.Receipt}); err != nil {
		t.Fatal(err)
	}
	secondDelivery := receiveTest(t, engine, "second-receive")
	if secondDelivery.Message.ID != second.ID || engine.totalInFlight != 1 || engine.inFlightByQueue["jobs"] != 1 {
		t.Fatalf("second delivery/counters = %+v %d/%d", secondDelivery, engine.totalInFlight, engine.inFlightByQueue["jobs"])
	}
}

func TestRecoveryRejectsAdversarialDeltasWithoutPanicking(t *testing.T) {
	cases := map[string]stateDelta{
		"unknown-queue-message": {NextSequence: 2, UpsertMessages: map[string]map[string]*model.Message{"missing": {"m": {ID: "m"}}}},
		"nil-queue":             {UpsertQueues: map[string]*queueState{"q": nil}},
		"unknown-receipt-queue": {Receipts: map[string]map[string]*string{"missing": {"m": nil}}},
	}
	for name, delta := range cases {
		t.Run(name, func(t *testing.T) {
			payload, err := json.Marshal(persistedEnvelope{Kind: "delta", Version: stateVersion, Delta: &delta})
			if err != nil {
				t.Fatal(err)
			}
			store := &memoryJournal{records: []journal.Record{{LSN: 1, Payload: payload}}}
			if _, err := New(store, newFakeClock(time.Now()), Options{}); !IsCode(err, CodeStorageUnavailable) {
				t.Fatalf("recovery error = %v", err)
			}
		})
	}
}

func validRecoveredState(now time.Time) persistedState {
	leasedAt := now
	leaseUntil := now.Add(time.Minute)
	deadAt := now
	return persistedState{
		Version: stateVersion, NextSequence: 5,
		Queues: map[string]*queueState{"jobs": {
			Config: model.QueueConfig{Name: "jobs", Ordering: model.FIFO, DefaultVisibilityTimeout: time.Minute, MaxDeliveries: 2, CreatedAt: now},
			Messages: map[string]*model.Message{
				"ready":  {ID: "ready", Queue: "jobs", Payload: json.RawMessage(`{}`), Sequence: 1, EnqueuedAt: now, AvailableAt: now, State: model.StateReady, LastLSN: 1},
				"leased": {ID: "leased", Queue: "jobs", Payload: json.RawMessage(`{}`), Sequence: 2, EnqueuedAt: now, AvailableAt: now, State: model.StateLeased, DeliveryCount: 1, LeaseEpoch: 1, LeasedAt: &leasedAt, LeaseUntil: &leaseUntil, LastLSN: 2},
				"acked":  {ID: "acked", Queue: "jobs", Payload: json.RawMessage(`{}`), Sequence: 3, EnqueuedAt: now, AvailableAt: now, State: model.StateAcked, DeliveryCount: 1, LeaseEpoch: 1, LastLSN: 3},
				"dead":   {ID: "dead", Queue: "jobs", Payload: json.RawMessage(`{}`), Sequence: 4, EnqueuedAt: now, AvailableAt: now, State: model.StateDead, DeliveryCount: 2, LeaseEpoch: 2, DeadAt: &deadAt, LastLSN: 4},
			},
			Receipts:      map[string]string{"leased": "leased.1.0123456789abcdef0123456789abcdef"},
			AckedAt:       map[string]time.Time{"acked": now},
			AckedReceipts: map[string]ackReceipt{"acked.1.abcdef0123456789abcdef0123456789": {MessageID: "acked", ExpiresAt: now.Add(time.Hour)}},
		}},
		Idempotency: map[string]idempotencyRecord{},
	}
}

func TestRecoveredSemanticValidationMatrix(t *testing.T) {
	now := time.Date(2026, 8, 26, 20, 0, 0, 0, time.UTC)
	if err := validatePersistedState(validRecoveredState(now), 4); err != nil {
		t.Fatalf("valid state rejected: %v", err)
	}
	cases := map[string]func(*persistedState){
		"queue-name":        func(state *persistedState) { state.Queues["jobs"].Config.Name = "other" },
		"ordering":          func(state *persistedState) { state.Queues["jobs"].Config.Ordering = "bad" },
		"config":            func(state *persistedState) { state.Queues["jobs"].Config.MaxDeliveries = 0 },
		"nil-message":       func(state *persistedState) { state.Queues["jobs"].Messages["ready"] = nil },
		"message-id":        func(state *persistedState) { state.Queues["jobs"].Messages["ready"].ID = "other" },
		"payload":           func(state *persistedState) { state.Queues["jobs"].Messages["ready"].Payload = json.RawMessage(`{`) },
		"sequence":          func(state *persistedState) { state.Queues["jobs"].Messages["ready"].Sequence = 2 },
		"last-lsn":          func(state *persistedState) { state.Queues["jobs"].Messages["ready"].LastLSN = 5 },
		"deliverable-count": func(state *persistedState) { state.Queues["jobs"].Messages["ready"].DeliveryCount = 2 },
		"leased-count":      func(state *persistedState) { state.Queues["jobs"].Messages["leased"].DeliveryCount = 0 },
		"acked-index":       func(state *persistedState) { delete(state.Queues["jobs"].AckedAt, "acked") },
		"dead-count":        func(state *persistedState) { state.Queues["jobs"].Messages["dead"].DeliveryCount = 1 },
		"unknown-state":     func(state *persistedState) { state.Queues["jobs"].Messages["ready"].State = "unknown" },
		"receipt-index":     func(state *persistedState) { state.Queues["jobs"].Receipts["missing"] = "receipt" },
		"acked-time":        func(state *persistedState) { state.Queues["jobs"].AckedAt["missing"] = now },
		"acked-receipt": func(state *persistedState) {
			state.Queues["jobs"].AckedReceipts["bad"] = ackReceipt{MessageID: "missing", ExpiresAt: now}
		},
		"idempotency": func(state *persistedState) {
			state.Idempotency["bad"] = idempotencyRecord{Operation: operationEnqueue, Queue: "jobs", Key: "key"}
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			state, err := cloneStateForCheckpoint(validRecoveredState(now))
			if err != nil {
				t.Fatal(err)
			}
			mutate(&state)
			if err := validatePersistedState(state, 4); err == nil {
				t.Fatal("invalid recovered state accepted")
			}
		})
	}
}

func TestRecoveredDeltaValidationMatrix(t *testing.T) {
	now := time.Date(2026, 8, 26, 20, 0, 0, 0, time.UTC)
	base := validRecoveredState(now)
	cases := map[string]stateDelta{
		"sequence-regression":    {NextSequence: 4},
		"unknown-delete-queue":   {DeleteQueues: []string{"missing"}},
		"duplicate-delete-queue": {DeleteQueues: []string{"jobs", "jobs"}},
		"delete-upsert-queue":    {DeleteQueues: []string{"jobs"}, UpsertQueues: map[string]*queueState{"jobs": base.Queues["jobs"]}},
		"nil-message":            {UpsertMessages: map[string]map[string]*model.Message{"jobs": {"bad": nil}}},
		"unknown-message-delete": {DeleteMessages: map[string][]string{"missing": {"id"}}},
		"unknown-acked-time":     {AckedAt: map[string]map[string]*time.Time{"missing": {"id": nil}}},
		"unknown-acked-receipt":  {AckedReceipts: map[string]map[string]*ackReceipt{"missing": {"id": nil}}},
	}
	for name, delta := range cases {
		t.Run(name, func(t *testing.T) {
			if err := validateStateDelta(base, delta, 5); err == nil {
				t.Fatal("invalid recovered delta accepted")
			}
		})
	}
}

func TestReceiveLongPollTimeoutAndListSnapshotValidation(t *testing.T) {
	engine, _, clock := newTestService(t, model.FIFO, false, 3)
	result := make(chan error, 1)
	go func() {
		delivery, _, err := engine.Receive(context.Background(), "jobs", model.ReceiveRequest{WaitTimeout: time.Second})
		if err == nil && delivery != nil {
			err = errors.New("long poll returned unexpected delivery")
		}
		result <- err
	}()
	for index := 0; index < 100; index++ {
		engine.mu.Lock()
		registered := engine.totalWaiters == 1
		engine.mu.Unlock()
		if registered {
			break
		}
		time.Sleep(time.Millisecond)
	}
	clock.Advance(time.Second)
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("long poll did not time out")
	}
	if _, err := engine.ListMessages(context.Background(), "jobs", model.ListFilter{State: "invalid"}); !IsCode(err, CodeInvalid) {
		t.Fatalf("invalid state error = %v", err)
	}
	futureCursor := encodeCursor(listCursor{Scope: cursorScope("jobs", "", false), SnapshotLSN: 100, HighWater: 1})
	if _, err := engine.ListMessages(context.Background(), "jobs", model.ListFilter{Cursor: futureCursor}); !IsCode(err, CodeInvalid) {
		t.Fatalf("future snapshot error = %v", err)
	}
}

func TestReceiveLongPollCancellationReleasesWaiter(t *testing.T) {
	engine, _, _ := newTestService(t, model.FIFO, false, 3)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, _, err := engine.Receive(ctx, "jobs", model.ReceiveRequest{WaitTimeout: time.Second})
		result <- err
	}()
	registered := false
	for index := 0; index < 100; index++ {
		engine.mu.Lock()
		registered = engine.totalWaiters == 1 && engine.waitersByQueue["jobs"] == 1
		engine.mu.Unlock()
		if registered {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !registered {
		t.Fatal("long poll waiter was not registered")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("long poll cancellation error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("long poll did not observe cancellation")
	}
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if engine.totalWaiters != 0 || engine.waitersByQueue["jobs"] != 0 {
		t.Fatalf("waiter counters after cancellation = %d/%d", engine.totalWaiters, engine.waitersByQueue["jobs"])
	}
}

func TestCloseWakesLongPollAndReleasesWaiter(t *testing.T) {
	engine, _, _ := newTestService(t, model.FIFO, false, 3)
	result := make(chan error, 1)
	go func() {
		_, _, err := engine.Receive(context.Background(), "jobs", model.ReceiveRequest{WaitTimeout: time.Second})
		result <- err
	}()
	registered := false
	for index := 0; index < 100; index++ {
		engine.mu.Lock()
		registered = engine.totalWaiters == 1 && engine.waitersByQueue["jobs"] == 1
		engine.mu.Unlock()
		if registered {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !registered {
		t.Fatal("long poll waiter was not registered")
	}
	if err := engine.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if !IsCode(err, CodeClosed) {
			t.Fatalf("long poll close error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("long poll was not woken by close")
	}
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if engine.totalWaiters != 0 || engine.waitersByQueue["jobs"] != 0 {
		t.Fatalf("waiter counters after close = %d/%d", engine.totalWaiters, engine.waitersByQueue["jobs"])
	}
}

func TestCloseHonorsContextAndReportsStorageFailure(t *testing.T) {
	t.Run("canceled-context", func(t *testing.T) {
		engine, store, _ := newTestService(t, model.FIFO, false, 3)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := engine.Close(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("close error = %v", err)
		}
		if store.closed {
			t.Fatal("canceled close closed storage")
		}
	})
	t.Run("storage-failure", func(t *testing.T) {
		engine, store, _ := newTestService(t, model.FIFO, false, 3)
		store.closeError = errors.New("close failed")
		if err := engine.Close(context.Background()); !IsCode(err, CodeStorageUnavailable) {
			t.Fatalf("close error = %v", err)
		}
	})
}

func TestFailedUnknownQueueMutationsDoNotLeakInFlightEntries(t *testing.T) {
	engine, _, _ := newTestService(t, model.FIFO, false, 3)
	for index := 0; index < 100; index++ {
		queue := fmt.Sprintf("missing-%d", index)
		if _, _, err := engine.Enqueue(context.Background(), queue, model.EnqueueRequest{Payload: json.RawMessage(`{}`)}); !IsCode(err, CodeNotFound) {
			t.Fatalf("enqueue %s error = %v", queue, err)
		}
	}
	if len(engine.inFlightByQueue) != 0 {
		t.Fatalf("failed mutations leaked in-flight keys: %#v", engine.inFlightByQueue)
	}
}

func TestCheckpointCloneRejectsInvalidPersistedPayload(t *testing.T) {
	now := time.Date(2026, 8, 26, 20, 0, 0, 0, time.UTC)
	state := validRecoveredState(now)
	state.Queues["jobs"].Messages["ready"].Payload = json.RawMessage{0xff}
	if _, err := cloneStateForCheckpoint(state); err == nil {
		t.Fatal("invalid persisted payload cloned successfully")
	}
}

func TestCursorParserRejectsStrictV2Corruption(t *testing.T) {
	valid := encodeCursor(listCursor{
		Scope: cursorScope("jobs", model.StateReady, false), SnapshotLSN: 1, HighWater: 2, Sequence: 1,
		SnapshotSecond: 42, SnapshotNanosecond: 7,
	})
	parts := strings.Split(valid, ".")
	if len(parts) != 8 {
		t.Fatalf("cursor parts = %#v", parts)
	}
	withPart := func(index int, value string) string {
		copy := append([]string(nil), parts...)
		copy[index] = value
		return strings.Join(copy, ".")
	}
	for name, cursor := range map[string]string{
		"old-version":         "v2." + valid[3:],
		"truncated":           valid[:len(valid)-1],
		"extended":            valid + "0",
		"scope-digits":        withPart(1, strings.Repeat("z", 32)),
		"timestamp-digits":    withPart(6, strings.Repeat("x", 20)),
		"nanosecond-overflow": withPart(7, "1000000000"),
		"timestamp-separator": valid[:119] + ":" + valid[120:],
		"sequence-order":      encodeCursor(listCursor{Scope: cursorScope("jobs", model.StateReady, false), SnapshotLSN: 1, HighWater: 1, Sequence: 2, SnapshotSecond: 42}),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeCursor(cursor); !IsCode(err, CodeInvalid) {
				t.Fatalf("cursor %q error = %v", cursor, err)
			}
		})
	}

	engine, _, _ := newTestService(t, model.FIFO, false, 3)
	for name, cursor := range map[string]string{
		"zero-snapshot":   encodeCursor(listCursor{Scope: cursorScope("jobs", "", false), HighWater: 1, SnapshotSecond: 42}),
		"future-snapshot": encodeCursor(listCursor{Scope: cursorScope("jobs", "", false), SnapshotLSN: 100, HighWater: 1, SnapshotSecond: 42}),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := engine.ListMessages(context.Background(), "jobs", model.ListFilter{Cursor: cursor}); !IsCode(err, CodeInvalid) {
				t.Fatalf("cursor validation error = %v", err)
			}
		})
	}
}

func TestExpiredIdempotencyRecordReleasesCapacityDurably(t *testing.T) {
	store := &memoryJournal{}
	clock := newFakeClock(time.Date(2026, 8, 26, 20, 0, 0, 0, time.UTC))
	created, err := New(store, clock, Options{Limits: Limits{MaxIdempotencyRecords: 1, IdempotencyRetention: time.Second}})
	if err != nil {
		t.Fatal(err)
	}
	engine := created.(*service)
	if _, _, err := engine.CreateQueue(context.Background(), model.QueueConfig{Name: "jobs", Ordering: model.FIFO, DefaultVisibilityTimeout: time.Minute, MaxDeliveries: 1}, ""); err != nil {
		t.Fatal(err)
	}
	first := enqueueTest(t, engine, `{}`, 0, 0, "first")
	clock.Advance(time.Second)
	second := enqueueTest(t, engine, `{}`, 0, 0, "second")
	if first.ID == second.ID || len(engine.state.Idempotency) != 1 {
		t.Fatalf("replacement state = first %s second %s records %d", first.ID, second.ID, len(engine.state.Idempotency))
	}
	if _, exists := engine.state.Idempotency[idempotencyID(operationEnqueue, "jobs", "first")]; exists {
		t.Fatal("expired idempotency record retained after replacement")
	}
	recoveredService, err := New(store, clock, Options{Limits: Limits{MaxIdempotencyRecords: 1, IdempotencyRetention: time.Second}})
	if err != nil {
		t.Fatal(err)
	}
	recovered := recoveredService.(*service)
	replayed, wasReplay, err := recovered.Enqueue(context.Background(), "jobs", model.EnqueueRequest{Payload: json.RawMessage(`{}`), IdempotencyKey: "second"})
	if err != nil || !wasReplay || replayed.ID != second.ID {
		t.Fatalf("recovered replay = %+v/%t, %v", replayed, wasReplay, err)
	}
}

func TestExpiredIdempotencyPruningRollsBackOnPersistenceFailure(t *testing.T) {
	store := &memoryJournal{}
	clock := newFakeClock(time.Date(2026, 8, 26, 20, 0, 0, 0, time.UTC))
	created, err := New(store, clock, Options{Limits: Limits{MaxIdempotencyRecords: 1, IdempotencyRetention: time.Second}})
	if err != nil {
		t.Fatal(err)
	}
	engine := created.(*service)
	if _, _, err := engine.CreateQueue(context.Background(), model.QueueConfig{Name: "jobs", Ordering: model.FIFO, DefaultVisibilityTimeout: time.Minute, MaxDeliveries: 1}, ""); err != nil {
		t.Fatal(err)
	}
	enqueueTest(t, engine, `{}`, 0, 0, "first")
	clock.Advance(time.Second)
	store.appendError = errors.New("disk full")
	if _, _, err := engine.Enqueue(context.Background(), "jobs", model.EnqueueRequest{Payload: json.RawMessage(`{}`), IdempotencyKey: "second"}); !IsCode(err, CodeStorageUnavailable) {
		t.Fatalf("replacement error = %v", err)
	}
	if _, exists := engine.state.Idempotency[idempotencyID(operationEnqueue, "jobs", "first")]; !exists {
		t.Fatal("failed replacement did not restore expired record")
	}
	if _, exists := engine.state.Idempotency[idempotencyID(operationEnqueue, "jobs", "second")]; exists {
		t.Fatal("failed replacement retained attempted record")
	}
}

func TestMutationCoordinatorBatchesConcurrentEnqueuesInOrder(t *testing.T) {
	engine, store, _ := newTestService(t, model.FIFO, false, 3)
	store.mu.Lock()
	baselineCalls := store.batchCalls
	store.mu.Unlock()
	const count = 32
	start := make(chan struct{})
	messages := make(chan model.Message, count)
	errors := make(chan error, count)
	var wait sync.WaitGroup
	for index := 0; index < count; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			<-start
			message, _, err := engine.Enqueue(context.Background(), "jobs", model.EnqueueRequest{Payload: json.RawMessage(`{}`), IdempotencyKey: fmt.Sprintf("batch-%d", index)})
			if err != nil {
				errors <- err
				return
			}
			messages <- message
		}(index)
	}
	close(start)
	wait.Wait()
	close(messages)
	close(errors)
	for err := range errors {
		t.Fatal(err)
	}
	seenLSN := make(map[uint64]bool, count)
	for message := range messages {
		if seenLSN[message.LastLSN] {
			t.Fatalf("duplicate committed LSN %d", message.LastLSN)
		}
		seenLSN[message.LastLSN] = true
	}
	store.mu.Lock()
	calls := store.batchCalls - baselineCalls
	maxBatch := 0
	for _, size := range store.batchSizes[baselineCalls:] {
		maxBatch = max(maxBatch, size)
	}
	store.mu.Unlock()
	if calls >= count || maxBatch < 2 {
		t.Fatalf("coordinator did not batch: calls=%d mutations=%d maxBatch=%d", calls, count, maxBatch)
	}
	page, err := engine.ListMessages(context.Background(), "jobs", model.ListFilter{Limit: count})
	if err != nil || len(page.Messages) != count {
		t.Fatalf("page = %+v, %v", page, err)
	}
	for index := 1; index < len(page.Messages); index++ {
		if page.Messages[index-1].Sequence >= page.Messages[index].Sequence || page.Messages[index-1].LastLSN >= page.Messages[index].LastLSN {
			t.Fatalf("order at %d = seq %d/%d lsn %d/%d", index, page.Messages[index-1].Sequence, page.Messages[index].Sequence, page.Messages[index-1].LastLSN, page.Messages[index].LastLSN)
		}
	}
}

func TestMutationCoordinatorInvalidNeighborAndBatchFailureIsolation(t *testing.T) {
	t.Run("invalid-neighbor", func(t *testing.T) {
		engine, _, _ := newTestService(t, model.FIFO, false, 3)
		start := make(chan struct{})
		results := make(chan error, 3)
		var wait sync.WaitGroup
		for index := 0; index < 3; index++ {
			wait.Add(1)
			go func(index int) {
				defer wait.Done()
				<-start
				payload := json.RawMessage(`{}`)
				if index == 1 {
					payload = json.RawMessage(`{`)
				}
				_, _, err := engine.Enqueue(context.Background(), "jobs", model.EnqueueRequest{Payload: payload, IdempotencyKey: fmt.Sprintf("mixed-%d", index)})
				results <- err
			}(index)
		}
		close(start)
		wait.Wait()
		close(results)
		invalids, successes := 0, 0
		for err := range results {
			if IsCode(err, CodeInvalid) {
				invalids++
			} else if err == nil {
				successes++
			} else {
				t.Fatal(err)
			}
		}
		if invalids != 1 || successes != 2 {
			t.Fatalf("results invalid=%d successes=%d", invalids, successes)
		}
	})

	t.Run("batch-failure", func(t *testing.T) {
		engine, store, _ := newTestService(t, model.FIFO, false, 3)
		store.appendError = errors.New("disk full")
		start := make(chan struct{})
		results := make(chan error, 8)
		var wait sync.WaitGroup
		for index := 0; index < 8; index++ {
			wait.Add(1)
			go func(index int) {
				defer wait.Done()
				<-start
				_, _, err := engine.Enqueue(context.Background(), "jobs", model.EnqueueRequest{Payload: json.RawMessage(`{}`), IdempotencyKey: fmt.Sprintf("failure-%d", index)})
				results <- err
			}(index)
		}
		close(start)
		wait.Wait()
		close(results)
		for err := range results {
			if !IsCode(err, CodeStorageUnavailable) {
				t.Fatalf("batch failure error = %v", err)
			}
		}
		engine.mu.Lock()
		messageCount := len(engine.state.Queues["jobs"].Messages)
		nextSequence := engine.state.NextSequence
		_, createRecordExists := engine.state.Idempotency[idempotencyID(operationCreateQueue, "jobs", "create")]
		idempotencyCount := len(engine.state.Idempotency)
		engine.mu.Unlock()
		if messageCount != 0 || nextSequence != 1 || idempotencyCount != 1 || !createRecordExists || engine.totalMessages != 0 {
			t.Fatalf("rollback state messages=%d next=%d idempotency=%d create=%t total=%d", messageCount, nextSequence, idempotencyCount, createRecordExists, engine.totalMessages)
		}
	})
}

func TestMutationCoordinatorCancellationAndCloseDrain(t *testing.T) {
	engine, _, _ := newTestService(t, model.FIFO, false, 3)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := engine.Enqueue(cancelled, "jobs", model.EnqueueRequest{Payload: json.RawMessage(`{}`), IdempotencyKey: "cancelled"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-admission cancellation = %v", err)
	}

	result := make(chan mutationResult, 1)
	request := mutationRequest{
		ctx: context.Background(), queueName: "jobs", operation: operationEnqueue, result: result,
		mutation: func() error {
			queue := engine.state.Queues["jobs"]
			message := &model.Message{ID: "drained", Queue: "jobs", Payload: json.RawMessage(`{}`), Sequence: engine.state.NextSequence, EnqueuedAt: engine.clock.Now(), AvailableAt: engine.clock.Now(), State: model.StateReady}
			engine.state.NextSequence++
			queue.Messages[message.ID] = message
			engine.totalMessages++
			return nil
		},
	}
	engine.submitMu.RLock()
	engine.mutationCh <- request
	engine.submitMu.RUnlock()
	if err := engine.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if committed := <-result; committed.err != nil || committed.lsn == 0 {
		t.Fatalf("drained mutation result = %+v", committed)
	}
	engine.mu.Lock()
	_, drained := engine.state.Queues["jobs"].Messages["drained"]
	engine.mu.Unlock()
	if !drained {
		t.Fatal("close did not drain accepted mutation")
	}
	if _, _, err := engine.Enqueue(context.Background(), "jobs", model.EnqueueRequest{Payload: json.RawMessage(`{}`)}); !IsCode(err, CodeClosed) {
		t.Fatalf("post-close enqueue = %v", err)
	}
}

func TestMutationCoordinatorRealJournalCoalescesAndRecovers(t *testing.T) {
	dir := t.TempDir()
	var syncMu sync.Mutex
	walSyncs := 0
	store, err := journal.Open(journal.Config{Dir: dir, Faults: journal.FaultHooks{BeforeSync: func(path string) error {
		if filepath.Ext(path) == ".wal" {
			syncMu.Lock()
			walSyncs++
			syncMu.Unlock()
		}
		return nil
	}}})
	if err != nil {
		t.Fatal(err)
	}
	created, err := New(store, newFakeClock(time.Date(2026, 8, 26, 20, 0, 0, 0, time.UTC)), Options{})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	engine := created.(*service)
	if _, _, err := engine.CreateQueue(context.Background(), model.QueueConfig{Name: "jobs", Ordering: model.FIFO, DefaultVisibilityTimeout: time.Minute, MaxDeliveries: 3}, "create"); err != nil {
		t.Fatal(err)
	}
	syncMu.Lock()
	baseline := walSyncs
	syncMu.Unlock()

	const count = 24
	start := make(chan struct{})
	var wait sync.WaitGroup
	for index := 0; index < count; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			<-start
			if _, _, err := engine.Enqueue(context.Background(), "jobs", model.EnqueueRequest{Payload: json.RawMessage(`{}`), IdempotencyKey: fmt.Sprintf("real-%d", index)}); err != nil {
				t.Error(err)
			}
		}(index)
	}
	close(start)
	wait.Wait()
	syncMu.Lock()
	enqueueSyncs := walSyncs - baseline
	baseline = walSyncs
	syncMu.Unlock()
	if enqueueSyncs >= count {
		t.Fatalf("enqueue WAL syncs=%d mutations=%d", enqueueSyncs, count)
	}

	deliveries := make(chan *model.Delivery, count)
	start = make(chan struct{})
	for index := 0; index < count; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			delivery, _, err := engine.Receive(context.Background(), "jobs", model.ReceiveRequest{})
			if err != nil {
				t.Error(err)
				return
			}
			deliveries <- delivery
		}()
	}
	close(start)
	wait.Wait()
	close(deliveries)
	if len(deliveries) != count {
		t.Fatalf("deliveries=%d want=%d", len(deliveries), count)
	}
	syncMu.Lock()
	receiveSyncs := walSyncs - baseline
	syncMu.Unlock()
	if receiveSyncs >= count {
		t.Fatalf("receive WAL syncs=%d reservations=%d", receiveSyncs, count)
	}
	if err := engine.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	reopened, err := journal.Open(journal.Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	recoveredService, err := New(reopened, newFakeClock(time.Date(2026, 8, 26, 20, 0, 0, 0, time.UTC)), Options{})
	if err != nil {
		_ = reopened.Close()
		t.Fatal(err)
	}
	recovered := recoveredService.(*service)
	page, err := recovered.ListMessages(context.Background(), "jobs", model.ListFilter{State: model.StateLeased, Limit: count})
	if err != nil || len(page.Messages) != count {
		t.Fatalf("recovered page messages=%d, %v", len(page.Messages), err)
	}
	for index := 1; index < len(page.Messages); index++ {
		if page.Messages[index-1].Sequence >= page.Messages[index].Sequence || page.Messages[index-1].LastLSN >= page.Messages[index].LastLSN {
			t.Fatalf("recovered order at %d", index)
		}
	}
	if err := recovered.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

type blockingBatchJournal struct {
	memoryJournal
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (store *blockingBatchJournal) AppendBatch(ctx context.Context, records []journal.Record) ([]uint64, error) {
	store.once.Do(func() { close(store.started) })
	<-store.release
	return store.memoryJournal.AppendBatch(ctx, records)
}

func TestMutationCoordinatorSpeculativeReplayWaitsForDurability(t *testing.T) {
	engine, store, _ := newTestService(t, model.FIFO, false, 3)
	store.appendError = errors.New("disk full")
	start := make(chan struct{})
	results := make(chan error, 2)
	for index := 0; index < 2; index++ {
		go func() {
			<-start
			_, _, err := engine.Enqueue(context.Background(), "jobs", model.EnqueueRequest{Payload: json.RawMessage(`{}`), IdempotencyKey: "same"})
			results <- err
		}()
	}
	close(start)
	for index := 0; index < 2; index++ {
		if err := <-results; !IsCode(err, CodeStorageUnavailable) {
			t.Fatalf("speculative replay result %d = %v", index, err)
		}
	}
	engine.mu.Lock()
	_, messageExists := engine.state.Queues["jobs"].Messages["same"]
	_, idempotencyExists := engine.state.Idempotency[idempotencyID(operationEnqueue, "jobs", "same")]
	engine.mu.Unlock()
	if messageExists || idempotencyExists {
		t.Fatalf("failed speculative replay retained state message=%t idempotency=%t", messageExists, idempotencyExists)
	}
}

func TestMutationCoordinatorReplaySurvivesNeighborPersistenceFailure(t *testing.T) {
	engine, store, _ := newTestService(t, model.FIFO, false, 3)
	original := enqueueTest(t, engine, `{}`, 0, 0, "original")
	store.appendError = errors.New("disk full")
	type outcome struct {
		message  model.Message
		replayed bool
		err      error
	}
	start := make(chan struct{})
	results := make(chan outcome, 2)
	for _, key := range []string{"original", "new"} {
		go func(key string) {
			<-start
			message, replayed, err := engine.Enqueue(context.Background(), "jobs", model.EnqueueRequest{Payload: json.RawMessage(`{}`), IdempotencyKey: key})
			results <- outcome{message, replayed, err}
		}(key)
	}
	close(start)
	first, second := <-results, <-results
	outcomes := []outcome{first, second}
	replaySuccess, storageFailure := 0, 0
	for _, result := range outcomes {
		if result.err == nil && result.replayed && result.message.ID == original.ID {
			replaySuccess++
		} else if IsCode(result.err, CodeStorageUnavailable) {
			storageFailure++
		} else {
			t.Fatalf("unexpected outcome = %+v", result)
		}
	}
	if replaySuccess != 1 || storageFailure != 1 {
		t.Fatalf("replay=%d storageFailure=%d", replaySuccess, storageFailure)
	}
	page, err := engine.ListMessages(context.Background(), "jobs", model.ListFilter{})
	if err != nil || len(page.Messages) != 1 || page.Messages[0].ID != original.ID {
		t.Fatalf("state after failure = %+v, %v", page, err)
	}
}

type malformedLSNJournal struct {
	memoryJournal
	lsns []uint64
}

func (store *malformedLSNJournal) AppendBatch(context.Context, []journal.Record) ([]uint64, error) {
	return append([]uint64(nil), store.lsns...), nil
}

func TestMutationCoordinatorReprocessesSpeculativeSemanticOutcome(t *testing.T) {
	for _, failure := range []bool{false, true} {
		t.Run(fmt.Sprintf("failure=%t", failure), func(t *testing.T) {
			engine, store, _ := newTestService(t, model.FIFO, false, 3)
			if failure {
				store.appendError = errors.New("disk full")
			}
			makeRequest := func() mutationRequest {
				return mutationRequest{
					ctx: context.Background(), queueName: "new", operation: operationCreateQueue, result: make(chan mutationResult, 1),
					mutation: func() error {
						if _, exists := engine.state.Queues["new"]; exists {
							return conflict("queue already exists")
						}
						engine.state.Queues["new"] = &queueState{Config: model.QueueConfig{Name: "new", Ordering: model.FIFO, DefaultVisibilityTimeout: time.Minute, MaxDeliveries: 1, CreatedAt: engine.clock.Now()}, Messages: map[string]*model.Message{}, Receipts: map[string]string{}, AckedAt: map[string]time.Time{}, AckedReceipts: map[string]ackReceipt{}}
						return nil
					},
				}
			}
			first, second := makeRequest(), makeRequest()
			engine.processMutationRequests([]mutationRequest{first, second})
			firstResult, secondResult := <-first.result, <-second.result
			if failure {
				if !IsCode(firstResult.err, CodeStorageUnavailable) || !IsCode(secondResult.err, CodeStorageUnavailable) {
					t.Fatalf("failure results = %+v / %+v", firstResult, secondResult)
				}
			} else if firstResult.err != nil || !IsCode(secondResult.err, CodeConflict) {
				t.Fatalf("success results = %+v / %+v", firstResult, secondResult)
			}
		})
	}
}

func TestMutationCoordinatorRejectsMalformedLSNResults(t *testing.T) {
	for name, lsns := range map[string][]uint64{
		"short": {},
		"extra": {1, 2},
		"zero":  {0},
	} {
		t.Run(name, func(t *testing.T) {
			store := &malformedLSNJournal{lsns: lsns}
			clock := newFakeClock(time.Date(2026, 8, 26, 20, 0, 0, 0, time.UTC))
			created, err := New(store, clock, Options{})
			if err != nil {
				t.Fatal(err)
			}
			engine := created.(*service)
			if _, _, err := engine.CreateQueue(context.Background(), model.QueueConfig{Name: "jobs", Ordering: model.FIFO, DefaultVisibilityTimeout: time.Minute, MaxDeliveries: 1}, ""); !IsCode(err, CodeStorageUnavailable) {
				t.Fatalf("malformed LSN error = %v", err)
			}
			engine.mu.Lock()
			_, exists := engine.state.Queues["jobs"]
			engine.mu.Unlock()
			if exists {
				t.Fatal("malformed LSN result retained speculative queue")
			}
		})
	}
}

func TestMutationCoordinatorEmptyReceiveCapturesWakeAtomically(t *testing.T) {
	engine, _, _ := newTestService(t, model.FIFO, false, 3)
	_, _, wake, _, err := engine.receiveOnce(context.Background(), "jobs", model.ReceiveRequest{}, "")
	if err != nil || wake == nil {
		t.Fatalf("empty receive = wake %v, %v", wake, err)
	}
	enqueueTest(t, engine, `{}`, 0, 0, "wake")
	select {
	case <-wake:
	case <-time.After(time.Second):
		t.Fatal("captured empty-receive wake was not closed by enqueue")
	}
	delivery := receiveTest(t, engine, "receive-wake")
	if delivery == nil {
		t.Fatal("woken receiver found no delivery")
	}
}

func TestMutationCoordinatorAdmissionFailsFastWhenSaturated(t *testing.T) {
	store := &blockingBatchJournal{started: make(chan struct{}), release: make(chan struct{})}
	clock := newFakeClock(time.Date(2026, 8, 26, 20, 0, 0, 0, time.UTC))
	created, err := New(store, clock, Options{})
	if err != nil {
		t.Fatal(err)
	}
	engine := created.(*service)
	createDone := make(chan error, 1)
	go func() {
		_, _, err := engine.CreateQueue(context.Background(), model.QueueConfig{Name: "jobs", Ordering: model.FIFO, DefaultVisibilityTimeout: time.Minute, MaxDeliveries: 1}, "")
		createDone <- err
	}()
	<-store.started
	for index := 0; index < mutationQueueCapacity; index++ {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		engine.mutationCh <- mutationRequest{ctx: ctx, result: make(chan mutationResult, 1)}
	}
	started := time.Now()
	if _, _, err := engine.Enqueue(context.Background(), "jobs", model.EnqueueRequest{Payload: json.RawMessage(`{}`)}); !IsCode(err, CodeCapacityExceeded) {
		t.Fatalf("saturated admission error = %v", err)
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("saturated admission took %v", elapsed)
	}
	close(store.release)
	if err := <-createDone; err != nil {
		t.Fatal(err)
	}
	if err := engine.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestMutationCoordinatorFreezesQueuedRequestInputs(t *testing.T) {
	store := &blockingBatchJournal{started: make(chan struct{}), release: make(chan struct{})}
	clock := newFakeClock(time.Date(2026, 8, 26, 20, 0, 0, 0, time.UTC))
	created, err := New(store, clock, Options{})
	if err != nil {
		t.Fatal(err)
	}
	engine := created.(*service)
	createDone := make(chan error, 1)
	go func() {
		_, _, err := engine.CreateQueue(context.Background(), model.QueueConfig{Name: "jobs", Ordering: model.FIFO, PriorityEnabled: true, DefaultVisibilityTimeout: time.Minute, MaxDeliveries: 1}, "")
		createDone <- err
	}()
	<-store.started

	payload := json.RawMessage(`{"value":1}`)
	priority := int32(7)
	delay := time.Minute
	availableAt := clock.Now().Add(2 * time.Minute)
	result := make(chan model.Message, 1)
	errors := make(chan error, 1)
	go func() {
		message, _, err := engine.Enqueue(context.Background(), "jobs", model.EnqueueRequest{Payload: payload, Priority: &priority, Delay: &delay, AvailableAt: &availableAt, IdempotencyKey: "frozen"})
		if err != nil {
			errors <- err
			return
		}
		result <- message
	}()
	queued := <-engine.mutationCh
	payload[9] = '9'
	priority = 99
	delay = 3 * time.Minute
	availableAt = clock.Now().Add(4 * time.Minute)
	engine.mutationCh <- queued
	close(store.release)
	if err := <-createDone; err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errors:
		t.Fatal(err)
	case message := <-result:
		if string(message.Payload) != `{"value":1}` || message.Priority != 7 || !message.AvailableAt.Equal(clock.Now().Add(2*time.Minute)) {
			t.Fatalf("frozen message = %+v", message)
		}
	case <-time.After(time.Second):
		t.Fatal("frozen enqueue did not complete")
	}
}

func TestMutationCoordinatorClosingVisibilityAndFinalization(t *testing.T) {
	store := &blockingBatchJournal{started: make(chan struct{}), release: make(chan struct{})}
	clock := newFakeClock(time.Date(2026, 8, 26, 20, 0, 0, 0, time.UTC))
	created, err := New(store, clock, Options{})
	if err != nil {
		t.Fatal(err)
	}
	engine := created.(*service)
	createDone := make(chan error, 1)
	go func() {
		_, _, err := engine.CreateQueue(context.Background(), model.QueueConfig{Name: "jobs", Ordering: model.FIFO, DefaultVisibilityTimeout: time.Minute, MaxDeliveries: 1}, "")
		createDone <- err
	}()
	<-store.started
	closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	closeResult := make(chan error, 1)
	go func() { closeResult <- engine.Close(closeCtx) }()
	for !engine.closing.Load() {
		time.Sleep(time.Millisecond)
	}
	if err := <-closeResult; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timed close = %v", err)
	}
	close(store.release)
	if err := <-createDone; err != nil {
		t.Fatal(err)
	}
	if err := engine.Ready(); !IsCode(err, CodeClosed) {
		t.Fatalf("ready during close = %v", err)
	}
	if _, err := engine.ListQueues(context.Background()); !IsCode(err, CodeClosed) {
		t.Fatalf("list during close = %v", err)
	}
	if err := engine.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	closed := store.closed
	store.mu.Unlock()
	if !closed {
		t.Fatal("background finalizer did not close journal")
	}
}

func TestMutationCoordinatorSerializesCompactionWithBlockedBatch(t *testing.T) {
	store := &blockingBatchJournal{started: make(chan struct{}), release: make(chan struct{})}
	clock := newFakeClock(time.Date(2026, 8, 26, 20, 0, 0, 0, time.UTC))
	created, err := New(store, clock, Options{})
	if err != nil {
		t.Fatal(err)
	}
	engine := created.(*service)
	createDone := make(chan error, 1)
	go func() {
		_, _, err := engine.CreateQueue(context.Background(), model.QueueConfig{Name: "jobs", Ordering: model.FIFO, DefaultVisibilityTimeout: time.Minute, MaxDeliveries: 1}, "")
		createDone <- err
	}()
	<-store.started
	compactDone := make(chan error, 1)
	go func() { compactDone <- engine.Compact(context.Background()) }()
	select {
	case err := <-compactDone:
		t.Fatalf("compaction overtook blocked mutation: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	close(store.release)
	if err := <-createDone; err != nil {
		t.Fatal(err)
	}
	if err := <-compactDone; err != nil {
		t.Fatal(err)
	}
	if _, err := engine.GetQueue(context.Background(), "jobs"); err != nil {
		t.Fatalf("compacted state omitted mutation: %v", err)
	}
}

func TestMutationCoordinatorPostInclusionCancellationCommits(t *testing.T) {
	store := &blockingBatchJournal{started: make(chan struct{}), release: make(chan struct{})}
	clock := newFakeClock(time.Date(2026, 8, 26, 20, 0, 0, 0, time.UTC))
	created, err := New(store, clock, Options{})
	if err != nil {
		t.Fatal(err)
	}
	engine := created.(*service)
	createDone := make(chan error, 1)
	go func() {
		_, _, err := engine.CreateQueue(context.Background(), model.QueueConfig{Name: "jobs", Ordering: model.FIFO, DefaultVisibilityTimeout: time.Minute, MaxDeliveries: 1}, "create")
		createDone <- err
	}()
	<-store.started
	close(store.release)
	if err := <-createDone; err != nil {
		t.Fatal(err)
	}

	store.started = make(chan struct{})
	store.release = make(chan struct{})
	store.once = sync.Once{}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan struct {
		message model.Message
		err     error
	}, 1)
	go func() {
		message, _, err := engine.Enqueue(ctx, "jobs", model.EnqueueRequest{Payload: json.RawMessage(`{}`), IdempotencyKey: "included"})
		result <- struct {
			message model.Message
			err     error
		}{message, err}
	}()
	<-store.started
	cancel()
	close(store.release)
	committed := <-result
	if committed.err != nil || committed.message.LastLSN == 0 {
		t.Fatalf("post-inclusion result = %+v", committed)
	}
	page, err := engine.ListMessages(context.Background(), "jobs", model.ListFilter{})
	if err != nil || len(page.Messages) != 1 || page.Messages[0].ID != committed.message.ID {
		t.Fatalf("committed page = %+v, %v", page, err)
	}
}

func TestMutationCoordinatorBoundsBatchSize(t *testing.T) {
	engine, store, _ := newTestService(t, model.FIFO, false, 3)
	store.mu.Lock()
	baseline := len(store.batchSizes)
	store.mu.Unlock()
	const count = maxMutationBatch*2 + 5
	start := make(chan struct{})
	var wait sync.WaitGroup
	for index := 0; index < count; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			<-start
			if _, _, err := engine.Enqueue(context.Background(), "jobs", model.EnqueueRequest{Payload: json.RawMessage(`{}`), IdempotencyKey: fmt.Sprintf("bound-%d", index)}); err != nil {
				t.Error(err)
			}
		}(index)
	}
	close(start)
	wait.Wait()
	store.mu.Lock()
	sizes := append([]int(nil), store.batchSizes[baseline:]...)
	store.mu.Unlock()
	total := 0
	for _, size := range sizes {
		if size > maxMutationBatch {
			t.Fatalf("batch size %d exceeds %d", size, maxMutationBatch)
		}
		total += size
	}
	if total != count {
		t.Fatalf("batched records=%d want=%d sizes=%v", total, count, sizes)
	}
}

func BenchmarkMutationCoordinatorConcurrentEnqueue(b *testing.B) {
	store := &memoryJournal{}
	clock := newFakeClock(time.Date(2026, 8, 26, 20, 0, 0, 0, time.UTC))
	created, err := New(store, clock, Options{Limits: Limits{MaxMessages: max(b.N, 1), MaxMessagesPerQueue: max(b.N, 1), MaxIdempotencyRecords: max(b.N, 1)}})
	if err != nil {
		b.Fatal(err)
	}
	engine := created.(*service)
	if _, _, err := engine.CreateQueue(context.Background(), model.QueueConfig{Name: "jobs", Ordering: model.FIFO, DefaultVisibilityTimeout: time.Minute, MaxDeliveries: 1}, ""); err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(b.N), "mutations/op")
	b.ResetTimer()
	b.RunParallel(func(parallel *testing.PB) {
		for parallel.Next() {
			if _, _, err := engine.Enqueue(context.Background(), "jobs", model.EnqueueRequest{Payload: json.RawMessage(`{}`)}); err != nil {
				b.Error(err)
			}
		}
	})
	b.StopTimer()
	if err := engine.Close(context.Background()); err != nil {
		b.Fatal(err)
	}
}

func TestSequenceExhaustionNeverWraps(t *testing.T) {
	engine, store, _ := newTestService(t, model.FIFO, false, 3)
	engine.mu.Lock()
	engine.state.NextSequence = math.MaxUint64 - 1
	engine.mu.Unlock()
	message, _, err := engine.Enqueue(context.Background(), "jobs", model.EnqueueRequest{Payload: json.RawMessage(`{}`), IdempotencyKey: "last"})
	if err != nil || message.Sequence != math.MaxUint64-1 {
		t.Fatalf("last assignable enqueue = %+v, %v", message, err)
	}
	store.mu.Lock()
	recordsBefore := len(store.records)
	store.mu.Unlock()
	if _, _, err := engine.Enqueue(context.Background(), "jobs", model.EnqueueRequest{Payload: json.RawMessage(`{}`), IdempotencyKey: "exhausted"}); !IsCode(err, CodeCapacityExceeded) {
		t.Fatalf("exhausted enqueue error = %v", err)
	}
	engine.mu.Lock()
	next := engine.state.NextSequence
	messageCount := len(engine.state.Queues["jobs"].Messages)
	engine.mu.Unlock()
	store.mu.Lock()
	recordsAfter := len(store.records)
	store.mu.Unlock()
	if next != math.MaxUint64 || messageCount != 1 || recordsAfter != recordsBefore {
		t.Fatalf("exhausted state next=%d messages=%d records=%d/%d", next, messageCount, recordsAfter, recordsBefore)
	}
}

func TestRecoveredCapabilityInvariantMatrix(t *testing.T) {
	now := time.Date(2026, 8, 26, 20, 0, 0, 0, time.UTC)
	base := validRecoveredState(now)
	base.Queues["jobs"].Receipts["leased"] = "leased.1.0123456789abcdef0123456789abcdef"
	base.Queues["jobs"].AckedReceipts = map[string]ackReceipt{"acked.1.abcdef0123456789abcdef0123456789": {MessageID: "acked", ExpiresAt: now.Add(time.Hour)}}
	fingerprint := strings.Repeat("a", sha256.Size*2)
	resultBytes, err := json.Marshal(enqueueMutationResult{Message: cloneMessage(base.Queues["jobs"].Messages["ready"])})
	if err != nil {
		t.Fatal(err)
	}
	id := idempotencyID(operationEnqueue, "jobs", "key")
	base.Idempotency[id] = idempotencyRecord{Operation: operationEnqueue, Queue: "jobs", Key: "key", Fingerprint: fingerprint, Result: resultBytes, CreatedAt: now, ExpiresAt: now.Add(time.Hour), LastLSN: 4}
	cases := map[string]func(*persistedState){
		"receipt-format": func(state *persistedState) { state.Queues["jobs"].Receipts["leased"] = "x" },
		"receipt-epoch": func(state *persistedState) {
			state.Queues["jobs"].Receipts["leased"] = "leased.2.0123456789abcdef0123456789abcdef"
		},
		"lease-epoch": func(state *persistedState) { state.Queues["jobs"].Messages["leased"].LeaseEpoch = 2 },
		"dead-count": func(state *persistedState) {
			state.Queues["jobs"].Messages["dead"].DeliveryCount = 3
			state.Queues["jobs"].Messages["dead"].LeaseEpoch = 3
		},
		"dead-time": func(state *persistedState) {
			value := state.Queues["jobs"].Messages["dead"].EnqueuedAt.Add(-time.Second)
			state.Queues["jobs"].Messages["dead"].DeadAt = &value
		},
		"acked-time": func(state *persistedState) {
			state.Queues["jobs"].AckedAt["acked"] = state.Queues["jobs"].Messages["acked"].EnqueuedAt.Add(-time.Second)
		},
		"acked-receipt": func(state *persistedState) {
			state.Queues["jobs"].AckedReceipts = map[string]ackReceipt{"x": {MessageID: "acked", ExpiresAt: now.Add(time.Hour)}}
		},
		"idempotency-zero-lsn": func(state *persistedState) {
			record := state.Idempotency[id]
			record.LastLSN = 0
			state.Idempotency[id] = record
		},
		"idempotency-operation": func(state *persistedState) {
			delete(state.Idempotency, id)
			record := base.Idempotency[id]
			record.Operation = "delete_everything"
			state.Idempotency[idempotencyID(record.Operation, record.Queue, record.Key)] = record
		},
		"idempotency-fingerprint": func(state *persistedState) {
			record := state.Idempotency[id]
			record.Fingerprint = "bad"
			state.Idempotency[id] = record
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			state, err := cloneStateForCheckpoint(base)
			if err != nil {
				t.Fatal(err)
			}
			mutate(&state)
			if err := validatePersistedState(state, 4); err == nil {
				t.Fatal("malformed state accepted")
			}
		})
	}

	longKeyState, err := cloneStateForCheckpoint(base)
	if err != nil {
		t.Fatal(err)
	}
	delete(longKeyState.Idempotency, id)
	record := base.Idempotency[id]
	record.Key = "long"
	longID := idempotencyID(record.Operation, record.Queue, record.Key)
	longKeyState.Idempotency[longID] = record
	service := &service{state: longKeyState, limits: DefaultLimits(), journal: &memoryJournal{records: []journal.Record{{LSN: 4}}}, clock: newFakeClock(now)}
	service.limits.MaxIdempotencyKeyBytes = 3
	if err := service.validateRecoveredState(); err == nil {
		t.Fatal("overlong recovered idempotency key accepted")
	}
}

func TestUTF8PayloadAndReasonInvariants(t *testing.T) {
	engine, store, clock := newTestService(t, model.FIFO, false, 3)
	invalidPayload := json.RawMessage{0x22, 0xff, 0x22}
	if !json.Valid(invalidPayload) || utf8.Valid(invalidPayload) {
		t.Fatal("invalid UTF-8 JSON fixture is not shaped as expected")
	}
	if _, _, err := engine.Enqueue(context.Background(), "jobs", model.EnqueueRequest{Payload: invalidPayload}); !IsCode(err, CodeInvalid) {
		t.Fatalf("invalid UTF-8 payload error = %v", err)
	}

	message := enqueueTest(t, engine, `{}`, 0, 0, "message")
	delivery := receiveTest(t, engine, "receive")
	if _, _, err := engine.Nack(context.Background(), "jobs", model.NackRequest{MessageID: message.ID, Receipt: delivery.Receipt, Reason: string([]byte{0xff})}); !IsCode(err, CodeInvalid) {
		t.Fatalf("invalid UTF-8 reason error = %v", err)
	}
	reason := strings.Repeat("a", 511) + "€"
	nacked, replayed, err := engine.Nack(context.Background(), "jobs", model.NackRequest{MessageID: message.ID, Receipt: delivery.Receipt, Reason: reason, IdempotencyKey: "nack"})
	if err != nil || replayed || nacked.LastFailureReason != strings.Repeat("a", 511) || !utf8.ValidString(nacked.LastFailureReason) {
		t.Fatalf("rune-safe nack = %+v/%t, %v", nacked, replayed, err)
	}
	again, replayed, err := engine.Nack(context.Background(), "jobs", model.NackRequest{MessageID: message.ID, Receipt: delivery.Receipt, Reason: reason, IdempotencyKey: "nack"})
	if err != nil || !replayed || again.LastFailureReason != nacked.LastFailureReason {
		t.Fatalf("rune-safe replay = %+v/%t, %v", again, replayed, err)
	}
	recoveredService, err := New(store, clock, Options{})
	if err != nil {
		t.Fatal(err)
	}
	page, err := recoveredService.ListMessages(context.Background(), "jobs", model.ListFilter{})
	if err != nil || len(page.Messages) != 1 || page.Messages[0].LastFailureReason != nacked.LastFailureReason {
		t.Fatalf("rune-safe recovery = %+v, %v", page, err)
	}

	state := validRecoveredState(clock.Now())
	state.Queues["jobs"].Messages["ready"].Payload = invalidPayload
	if err := validatePersistedState(state, 4); err == nil {
		t.Fatal("invalid UTF-8 recovered payload accepted")
	}
}

func TestStrictIdempotencyResultShapes(t *testing.T) {
	if !validIdempotencyResult(operationReceive, json.RawMessage(`{}`)) {
		t.Fatal("valid committed empty receive result rejected")
	}
	for operation, result := range map[string]json.RawMessage{
		operationCreateQueue: json.RawMessage(`{}`),
		operationEnqueue:     json.RawMessage(`{}`),
		operationAck:         json.RawMessage(`{"acked":false}`),
		operationNack:        json.RawMessage(`{"unknown":1}`),
		operationExtend:      json.RawMessage(`{} {}`),
		operationRedrive:     json.RawMessage(`{"result":{}}`),
	} {
		if validIdempotencyResult(operation, result) {
			t.Fatalf("malformed %s result accepted: %s", operation, result)
		}
	}
}

func TestPersistedZeroSequenceFailsClosed(t *testing.T) {
	state := validRecoveredState(time.Date(2026, 8, 26, 20, 0, 0, 0, time.UTC))
	state.NextSequence = 0
	if err := validatePersistedState(state, 4); err == nil {
		t.Fatal("zero next sequence accepted")
	}
	service := &service{state: state, limits: DefaultLimits(), journal: &memoryJournal{records: []journal.Record{{LSN: 4}}}, clock: newFakeClock(time.Now())}
	if err := service.validateRecoveredState(); err == nil || service.state.NextSequence != 0 {
		t.Fatalf("zero sequence recovery = next %d, err %v", service.state.NextSequence, err)
	}
}

func TestCursorSignedSecondExtremesRoundTrip(t *testing.T) {
	for _, second := range []int64{math.MinInt64, -1, 0, 1, math.MaxInt64} {
		cursor := listCursor{Scope: cursorScope("jobs", "", false), SnapshotLSN: 1, HighWater: 2, Sequence: 1, SnapshotGeneration: 3, SnapshotSecond: second, SnapshotNanosecond: 999_999_999}
		decoded, err := decodeCursor(encodeCursor(cursor))
		if err != nil || decoded != cursor {
			t.Fatalf("cursor second %d round trip = %+v, %v", second, decoded, err)
		}
	}
}

func TestRedriveFreezesPointerInputsBeforeQueuedExecution(t *testing.T) {
	engine, _, clock := newTestService(t, model.FIFO, true, 1)
	source := enqueueTest(t, engine, `{}`, 1, 0, "source")
	delivery := receiveTest(t, engine, "receive")
	if _, _, err := engine.Nack(context.Background(), "jobs", model.NackRequest{MessageID: source.ID, Receipt: delivery.Receipt}); err != nil {
		t.Fatal(err)
	}
	priority := int32(7)
	availableAt := clock.Now().Add(time.Minute)
	frozen := freezeRedriveRequest(model.RedriveRequest{MessageID: source.ID, Priority: &priority, AvailableAt: &availableAt})
	priority = 99
	availableAt = clock.Now().Add(2 * time.Minute)
	result, _, err := engine.Redrive(context.Background(), "jobs", frozen)
	if err != nil {
		t.Fatal(err)
	}
	if result.Child.Priority != 7 || !result.Child.AvailableAt.Equal(clock.Now().Add(time.Minute)) {
		t.Fatalf("redrive child = %+v", result.Child)
	}
}

func TestCommittedEmptyReceiveIdempotencySurvivesRestart(t *testing.T) {
	engine, store, clock := newTestService(t, model.FIFO, false, 3)
	delivery, replayed, err := engine.Receive(context.Background(), "jobs", model.ReceiveRequest{IdempotencyKey: "empty"})
	if err != nil || delivery != nil || replayed {
		t.Fatalf("empty receive = %+v/%t, %v", delivery, replayed, err)
	}
	recoveredService, err := New(store, clock, Options{})
	if err != nil {
		t.Fatal(err)
	}
	recovered := recoveredService.(*service)
	delivery, replayed, err = recovered.Receive(context.Background(), "jobs", model.ReceiveRequest{IdempotencyKey: "empty"})
	if err != nil || delivery != nil || !replayed {
		t.Fatalf("recovered empty replay = %+v/%t, %v", delivery, replayed, err)
	}
}

func TestCoordinatorCanceledRequestNeverExecutesClosure(t *testing.T) {
	engine, store, _ := newTestService(t, model.FIFO, false, 3)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	executed := false
	request := mutationRequest{ctx: ctx, queueName: "jobs", result: make(chan mutationResult, 1), mutation: func() error {
		executed = true
		return nil
	}}
	store.mu.Lock()
	recordsBefore := len(store.records)
	store.mu.Unlock()
	engine.processMutationRequests([]mutationRequest{request})
	result := <-request.result
	store.mu.Lock()
	recordsAfter := len(store.records)
	store.mu.Unlock()
	if !errors.Is(result.err, context.Canceled) || executed || recordsAfter != recordsBefore {
		t.Fatalf("canceled request = result %+v executed=%t records=%d/%d", result, executed, recordsAfter, recordsBefore)
	}
}

func TestMutationCoordinatorInternalFailureBranchesRollback(t *testing.T) {
	t.Run("encode-error", func(t *testing.T) {
		engine, _, _ := newTestService(t, model.FIFO, false, 3)
		request := mutationRequest{ctx: context.Background(), queueName: "jobs", result: make(chan mutationResult, 1), mutation: func() error {
			message := &model.Message{ID: "bad", Queue: "jobs", Payload: json.RawMessage{0xff}, Sequence: engine.state.NextSequence}
			engine.state.NextSequence++
			engine.state.Queues["jobs"].Messages[message.ID] = message
			engine.totalMessages++
			return nil
		}}
		engine.processMutationRequests([]mutationRequest{request})
		if result := <-request.result; !IsCode(result.err, CodeStorageUnavailable) {
			t.Fatalf("encode error result = %+v", result)
		}
		engine.mu.Lock()
		_, exists := engine.state.Queues["jobs"].Messages["bad"]
		engine.mu.Unlock()
		if exists {
			t.Fatal("encode failure retained speculative message")
		}
	})

	t.Run("oversized-record", func(t *testing.T) {
		engine, _, _ := newTestService(t, model.FIFO, false, 3)
		request := mutationRequest{ctx: context.Background(), queueName: "jobs", result: make(chan mutationResult, 1), mutation: func() error {
			message := &model.Message{ID: "large", Queue: "jobs", Payload: json.RawMessage(`"` + strings.Repeat("x", maxMutationBatchBytes) + `"`), Sequence: engine.state.NextSequence}
			engine.state.NextSequence++
			engine.state.Queues["jobs"].Messages[message.ID] = message
			engine.totalMessages++
			return nil
		}}
		engine.processMutationRequests([]mutationRequest{request})
		if result := <-request.result; !IsCode(result.err, CodeCapacityExceeded) {
			t.Fatalf("oversized result = %+v", result)
		}
	})
}

func TestRemainingEngineErrorAndStateBranches(t *testing.T) {
	ctx := context.Background()
	store := &memoryJournal{}
	clock := newFakeClock(time.Now())
	created, err := New(store, clock, Options{Limits: Limits{
		MaxQueues: 1, MaxMessages: 2, MaxMessagesPerQueue: 2, MaxPayloadBytes: 2,
		MaxIdempotencyRecords: 2, MaxIdempotencyKeyBytes: 4, MaxWaitTimeout: time.Second,
		MaxVisibilityTimeout: time.Second, MaxDelay: time.Second, IdempotencyRetention: time.Second,
		AckTombstoneRetention: time.Second, MaxListLimit: 2,
	}})
	if err != nil {
		t.Fatal(err)
	}
	engine := created.(*service)
	config := model.QueueConfig{Name: "jobs", Ordering: model.FIFO, DefaultVisibilityTimeout: time.Second, MaxDeliveries: 2}
	if _, _, err := engine.CreateQueue(ctx, config, "key"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := engine.CreateQueue(ctx, model.QueueConfig{Name: "second", Ordering: model.FIFO, DefaultVisibilityTimeout: time.Second, MaxDeliveries: 1}, ""); !IsCode(err, CodeCapacityExceeded) {
		t.Fatalf("queue capacity = %v", err)
	}
	if _, _, err := engine.CreateQueue(ctx, config, "diff"); !IsCode(err, CodeConflict) {
		t.Fatalf("duplicate conflict = %v", err)
	}
	if _, _, err := engine.CreateQueue(ctx, config, "long-key"); !IsCode(err, CodeInvalid) {
		t.Fatalf("long key = %v", err)
	}
	if _, _, err := engine.Enqueue(ctx, "jobs", model.EnqueueRequest{Payload: json.RawMessage(`{"x":1}`)}); !IsCode(err, CodeCapacityExceeded) {
		t.Fatalf("payload capacity = %v", err)
	}
	message, _, err := engine.Enqueue(ctx, "jobs", model.EnqueueRequest{Payload: json.RawMessage(`{}`), IdempotencyKey: "m1"})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := engine.Enqueue(ctx, "jobs", model.EnqueueRequest{Payload: json.RawMessage(`[]`), IdempotencyKey: "m1"}); !IsCode(err, CodeIdempotencyConflict) {
		t.Fatalf("idempotency conflict = %v", err)
	}

	delivery, _, err := engine.Receive(ctx, "jobs", model.ReceiveRequest{VisibilityTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Ack(ctx, "jobs", model.AckRequest{MessageID: "missing", Receipt: delivery.Receipt}); !IsCode(err, CodeNotFound) {
		t.Fatalf("missing ack message = %v", err)
	}
	if _, err := engine.Ack(ctx, "jobs", model.AckRequest{MessageID: message.ID, Receipt: "wrong"}); !IsCode(err, CodeStaleReceipt) {
		t.Fatalf("stale ack = %v", err)
	}
	if _, _, err := engine.Extend(ctx, "jobs", model.ExtendRequest{MessageID: message.ID, Receipt: "wrong", VisibilityTimeout: time.Second}); !IsCode(err, CodeStaleReceipt) {
		t.Fatalf("stale extend = %v", err)
	}
	nacked, _, err := engine.Nack(ctx, "jobs", model.NackRequest{MessageID: message.ID, Receipt: delivery.Receipt, Delay: time.Second, Reason: strings.Repeat("x", 600)})
	if err != nil || nacked.State != model.StateDelayed || len(nacked.LastFailureReason) != 512 {
		t.Fatalf("nack = %+v, %v", nacked, err)
	}
	if _, _, err := engine.Redrive(ctx, "jobs", model.RedriveRequest{MessageID: message.ID}); !IsCode(err, CodeConflict) {
		t.Fatalf("non-dead redrive = %v", err)
	}

	page, err := engine.ListMessages(ctx, "jobs", model.ListFilter{State: model.StateDelayed, Limit: 1})
	if err != nil || len(page.Messages) != 1 || page.Messages[0].State != model.StateDelayed {
		t.Fatalf("delayed page = %+v, %v", page, err)
	}
	if _, err := engine.ListMessages(ctx, "jobs", model.ListFilter{Limit: 3}); !IsCode(err, CodeInvalid) {
		t.Fatalf("max list = %v", err)
	}
	if _, err := engine.ListMessages(ctx, "missing", model.ListFilter{}); !IsCode(err, CodeNotFound) {
		t.Fatalf("missing list = %v", err)
	}

	waitCtx, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err := engine.Receive(waitCtx, "jobs", model.ReceiveRequest{WaitTimeout: time.Second}); !errors.Is(err, context.Canceled) {
		t.Fatalf("long poll cancel = %v", err)
	}
	store.readOnly, store.readReason = true, "disk failure"
	if err := engine.Ready(); !IsCode(err, CodeStorageUnavailable) {
		t.Fatalf("readonly ready = %v", err)
	}
	store.readOnly = false
	if err := engine.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.ListQueues(ctx); !IsCode(err, CodeClosed) {
		t.Fatalf("closed list = %v", err)
	}
	if _, err := engine.Stats(ctx); !IsCode(err, CodeClosed) {
		t.Fatalf("closed stats = %v", err)
	}
	if err := engine.Compact(ctx); !IsCode(err, CodeClosed) {
		t.Fatalf("closed compact = %v", err)
	}
}
