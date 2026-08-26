package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

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
	checkpointError error
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
	if _, err := decodeCursor(encodeCursor(message.Sequence)); err != nil {
		t.Fatal(err)
	}
	wrapped := &Error{Code: CodeConflict, Message: "conflict", Cause: errors.New("cause")}
	if !errors.Is(wrapped, wrapped.Cause) || !IsCode(wrapped, CodeConflict) {
		t.Fatalf("wrapped error = %v", wrapped)
	}
	if conflict("x").Error() != "x" || invalid("y").Error() != "y" {
		t.Fatal("error constructors changed")
	}
	if encodeCursor(42) != "00000000000000000042" {
		t.Fatalf("cursor = %q", encodeCursor(42))
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
	valid := &service{state: persistedState{Version: stateVersion, Queues: map[string]*queueState{"q": {Messages: nil, Receipts: nil, AckedAt: nil, AckedReceipts: nil}}, Idempotency: nil}}
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
	if got := availableAt(now, -time.Hour, &past); !got.Equal(now) {
		t.Fatalf("clamped time = %v", got)
	}
	future := now.Add(time.Hour)
	if got := availableAt(now, 0, &future); !got.Equal(future) {
		t.Fatalf("requested future = %v", got)
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
