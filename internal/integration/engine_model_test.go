package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/ElijahUmana/queuemaxxing/internal/engine"
	"github.com/ElijahUmana/queuemaxxing/internal/integration/referencemodel"
	"github.com/ElijahUmana/queuemaxxing/internal/journal"
	"github.com/ElijahUmana/queuemaxxing/internal/model"
)

func durationPointer(value time.Duration) *time.Duration {
	return &value
}

func TestEngineMatchesReferenceOrderingAndRestart(t *testing.T) {
	for _, ordering := range []model.Ordering{model.FIFO, model.LIFO} {
		for _, priority := range []bool{false, true} {
			ordering, priority := ordering, priority
			t.Run(fmt.Sprintf("%s/priority=%t", ordering, priority), func(t *testing.T) {
				directory := filepath.Join(t.TempDir(), "journal")
				now := time.Date(2026, 8, 26, 20, 0, 0, 0, time.UTC)
				clock := newManualClock(now)
				service := openService(t, directory, clock)
				config := model.QueueConfig{
					Name: "jobs", Ordering: ordering, PriorityEnabled: priority,
					DefaultVisibilityTimeout: time.Minute, MaxDeliveries: 3,
				}
				if _, _, err := service.CreateQueue(context.Background(), config, "create"); err != nil {
					t.Fatal(err)
				}
				reference := referencemodel.New(config, now)
				priorities := []int32{1, 3, 3, -1}
				var expectedIDs []string
				for index, messagePriority := range priorities {
					var requestedPriority *int32
					if priority {
						requestedPriority = &messagePriority
					}
					request := model.EnqueueRequest{Payload: json.RawMessage(fmt.Sprintf(`{"index":%d}`, index)), Priority: requestedPriority, IdempotencyKey: fmt.Sprintf("enqueue-%d", index)}
					message, replayed, err := service.Enqueue(context.Background(), "jobs", request)
					if err != nil || replayed {
						t.Fatalf("enqueue %d: replayed=%t err=%v", index, replayed, err)
					}
					if _, err := reference.Enqueue(message.ID, request.Payload, messagePriority, now); err != nil {
						t.Fatal(err)
					}
				}

				if err := service.Close(context.Background()); err != nil {
					t.Fatal(err)
				}
				service = openService(t, directory, clock)
				defer service.Close(context.Background())

				for {
					expected, expectedOK := reference.Receive(time.Minute)
					actual, _, err := service.Receive(context.Background(), "jobs", model.ReceiveRequest{VisibilityTimeout: time.Minute})
					if err != nil {
						t.Fatal(err)
					}
					if !expectedOK {
						if actual != nil {
							t.Fatalf("unexpected delivery %+v", actual)
						}
						break
					}
					if actual == nil || actual.Message.ID != expected.Message.ID {
						t.Fatalf("delivery mismatch: actual=%+v expected=%+v", actual, expected)
					}
					expectedIDs = append(expectedIDs, expected.Message.ID)
					if _, err := service.Ack(context.Background(), "jobs", model.AckRequest{MessageID: actual.Message.ID, Receipt: actual.Receipt}); err != nil {
						t.Fatal(err)
					}
					if err := reference.Ack(expected.Message.ID, expected.Receipt); err != nil {
						t.Fatal(err)
					}
				}
				if len(expectedIDs) != len(priorities) {
					t.Fatalf("drained %d messages, want %d", len(expectedIDs), len(priorities))
				}
				if err := reference.CheckInvariants(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestEngineRestartPreservesDelayedAndAckedState(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "journal")
	now := time.Date(2026, 8, 26, 20, 0, 0, 0, time.UTC)
	clock := newManualClock(now)
	service := openService(t, directory, clock)
	config := model.QueueConfig{Name: "jobs", Ordering: model.FIFO, DefaultVisibilityTimeout: time.Minute, MaxDeliveries: 3}
	if _, _, err := service.CreateQueue(context.Background(), config, "create"); err != nil {
		t.Fatal(err)
	}
	immediate, _, err := service.Enqueue(context.Background(), "jobs", model.EnqueueRequest{Payload: json.RawMessage(`1`), IdempotencyKey: "immediate"})
	if err != nil {
		t.Fatal(err)
	}
	delayed, _, err := service.Enqueue(context.Background(), "jobs", model.EnqueueRequest{Payload: json.RawMessage(`2`), Delay: durationPointer(time.Hour), IdempotencyKey: "delayed"})
	if err != nil {
		t.Fatal(err)
	}
	delivery, _, err := service.Receive(context.Background(), "jobs", model.ReceiveRequest{VisibilityTimeout: time.Minute})
	if err != nil || delivery == nil || delivery.Message.ID != immediate.ID {
		t.Fatalf("delivery=%+v err=%v", delivery, err)
	}
	if _, err := service.Ack(context.Background(), "jobs", model.AckRequest{MessageID: immediate.ID, Receipt: delivery.Receipt, IdempotencyKey: "ack"}); err != nil {
		t.Fatal(err)
	}
	if err := service.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	service = openService(t, directory, clock)
	defer service.Close(context.Background())
	page, err := service.ListMessages(context.Background(), "jobs", model.ListFilter{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	states := make(map[string]model.MessageState)
	for _, message := range page.Messages {
		states[message.ID] = message.State
	}
	if states[immediate.ID] != model.StateAcked || states[delayed.ID] != model.StateDelayed {
		t.Fatalf("restart states = %+v", states)
	}
}

func openService(t *testing.T, directory string, clock *manualClock) engine.Service {
	t.Helper()
	store, err := journal.Open(journal.Config{Dir: directory, Now: clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	service, err := engine.New(store, clock, engine.Options{})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	return service
}
