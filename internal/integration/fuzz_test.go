package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/ElijahUmana/queuemaxxing/internal/integration/referencemodel"
	"github.com/ElijahUmana/queuemaxxing/internal/model"
)

func FuzzEngineMatchesReference(f *testing.F) {
	f.Add([]byte{0, 1, 2, 0, 3, 2, 1})
	f.Add([]byte{1, 0, 0, 2, 3, 1, 2, 3})
	f.Fuzz(func(t *testing.T, operations []byte) {
		const maxOperations = 96
		if len(operations) > maxOperations {
			operations = operations[:maxOperations]
		}
		now := time.Unix(1_700_000_000, 0).UTC()
		clock := newManualClock(now)
		service := openService(t, filepath.Join(t.TempDir(), "journal"), clock)
		defer service.Close(context.Background())
		config := model.QueueConfig{
			Name: "fuzz", Ordering: []model.Ordering{model.FIFO, model.LIFO}[len(operations)%2],
			PriorityEnabled: true, DefaultVisibilityTimeout: time.Second, MaxDeliveries: 3,
		}
		if _, _, err := service.CreateQueue(context.Background(), config, "create"); err != nil {
			t.Fatal(err)
		}
		reference := referencemodel.New(config, now)
		nextID := 0
		for step, operation := range operations {
			switch operation % 4 {
			case 0, 1:
				nextID++
				priority := int32(int8(operation))
				payload := json.RawMessage(fmt.Sprintf(`{"step":%d}`, step))
				actual, _, err := service.Enqueue(context.Background(), "fuzz", model.EnqueueRequest{Payload: payload, Priority: &priority, IdempotencyKey: fmt.Sprintf("enqueue-%d", nextID)})
				if err != nil {
					t.Fatalf("enqueue step %d: %v", step, err)
				}
				if _, err := reference.Enqueue(actual.ID, payload, priority, now); err != nil {
					t.Fatalf("reference enqueue step %d: %v", step, err)
				}
			case 2:
				expected, expectedOK := reference.Receive(time.Second)
				actual, _, err := service.Receive(context.Background(), "fuzz", model.ReceiveRequest{VisibilityTimeout: time.Second})
				if err != nil {
					t.Fatalf("receive step %d: %v", step, err)
				}
				if !expectedOK {
					if actual != nil {
						t.Fatalf("step %d unexpected delivery %+v", step, actual)
					}
					continue
				}
				if actual == nil || actual.Message.ID != expected.Message.ID || actual.Message.DeliveryCount != expected.DeliveryCount {
					t.Fatalf("step %d delivery mismatch actual=%+v expected=%+v", step, actual, expected)
				}
				if _, err := service.Ack(context.Background(), "fuzz", model.AckRequest{MessageID: actual.Message.ID, Receipt: actual.Receipt}); err != nil {
					t.Fatalf("ack step %d: %v", step, err)
				}
				if err := reference.Ack(expected.Message.ID, expected.Receipt); err != nil {
					t.Fatalf("reference ack step %d: %v", step, err)
				}
			case 3:
				delta := time.Duration(operation%5) * time.Millisecond
				clock.Advance(delta)
				reference.Advance(delta)
				now = now.Add(delta)
			}
			if err := reference.CheckInvariants(); err != nil {
				t.Fatalf("reference step %d: %v", step, err)
			}
		}
		page, err := service.ListMessages(context.Background(), "fuzz", model.ListFilter{Limit: maxOperations})
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Messages) != nextID {
			t.Fatalf("listed %d messages, enqueued %d", len(page.Messages), nextID)
		}
	})
}

func FuzzReferenceModelOperations(f *testing.F) {
	f.Add([]byte{0, 0, 1, 2, 3, 4, 5})
	f.Add([]byte{1, 1, 9, 9, 2, 5, 4, 3, 2})
	f.Fuzz(func(t *testing.T, operations []byte) {
		const maxOperations = 4_096
		if len(operations) > maxOperations {
			operations = operations[:maxOperations]
		}
		now := time.Unix(1_700_000_000, 0).UTC()
		queue := referencemodel.New(model.QueueConfig{
			Name: "fuzz", Ordering: []model.Ordering{model.FIFO, model.LIFO}[len(operations)%2],
			PriorityEnabled: len(operations)%3 != 0, MaxDeliveries: 5,
		}, now)
		receipts := make(map[string]string)
		nextID := 0
		for step, operation := range operations {
			switch operation % 6 {
			case 0, 1:
				nextID++
				id := fmt.Sprintf("m-%d", nextID)
				priority := int32(int8(operation))
				if _, err := queue.Enqueue(id, json.RawMessage(fmt.Sprintf(`{"step":%d}`, step)), priority, now.Add(time.Duration(operation%5)*time.Millisecond)); err != nil {
					t.Fatalf("enqueue step %d: %v", step, err)
				}
			case 2:
				if delivery, ok := queue.Receive(time.Duration(operation%5+1) * time.Millisecond); ok {
					receipts[delivery.Message.ID] = delivery.Receipt
				}
			case 3:
				for id, receipt := range receipts {
					_ = queue.Ack(id, receipt)
					delete(receipts, id)
					break
				}
			case 4:
				for id, receipt := range receipts {
					_, _ = queue.Nack(id, receipt, time.Duration(operation%3)*time.Millisecond, "fuzz")
					delete(receipts, id)
					break
				}
			case 5:
				queue.Advance(time.Duration(operation%7) * time.Millisecond)
				now = now.Add(time.Duration(operation%7) * time.Millisecond)
			}
			if err := queue.CheckInvariants(); err != nil {
				t.Fatalf("step %d operation %d: %v", step, operation, err)
			}
		}
	})
}
