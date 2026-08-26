package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	queueclock "github.com/ElijahUmana/queuemaxxing/internal/clock"
	"github.com/ElijahUmana/queuemaxxing/internal/journal"
	"github.com/ElijahUmana/queuemaxxing/internal/model"
)

type memoryJournal struct {
	mu       sync.Mutex
	records  []journal.Record
	snapshot journal.Snapshot
	closed   bool
}

func (store *memoryJournal) Append(ctx context.Context, transaction journal.TransactionID, payload []byte) (uint64, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
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
	return journal.Stats{DurableLSN: durable, SnapshotGeneration: store.snapshot.Generation}
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
	message, _, err := engine.Enqueue(context.Background(), "jobs", model.EnqueueRequest{Payload: json.RawMessage(payload), Priority: priority, Delay: delay, IdempotencyKey: key})
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
