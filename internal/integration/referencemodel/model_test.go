package referencemodel

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/ElijahUmana/queuemaxxing/internal/model"
)

func TestOrderingCompositions(t *testing.T) {
	for _, ordering := range []model.Ordering{model.FIFO, model.LIFO} {
		for _, priorityEnabled := range []bool{false, true} {
			name := fmt.Sprintf("%s/priority=%t", ordering, priorityEnabled)
			t.Run(name, func(t *testing.T) {
				now := time.Date(2026, 8, 26, 20, 0, 0, 0, time.UTC)
				queue := New(model.QueueConfig{
					Name:            "jobs",
					Ordering:        ordering,
					PriorityEnabled: priorityEnabled,
					MaxDeliveries:   3,
				}, now)
				priorities := []int32{1, 3, 3, -1}
				for index, priority := range priorities {
					_, err := queue.Enqueue(fmt.Sprintf("m%d", index+1), json.RawMessage(`{"ok":true}`), priority, now)
					if err != nil {
						t.Fatal(err)
					}
				}
				var got []string
				for {
					delivery, ok := queue.Receive(time.Minute)
					if !ok {
						break
					}
					got = append(got, delivery.Message.ID)
					if err := queue.Ack(delivery.Message.ID, delivery.Receipt); err != nil {
						t.Fatal(err)
					}
				}
				want := expectedOrder(ordering, priorityEnabled)
				if fmt.Sprint(got) != fmt.Sprint(want) {
					t.Fatalf("order mismatch: got %v, want %v", got, want)
				}
				if err := queue.CheckInvariants(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestDelayAndLeaseBoundaries(t *testing.T) {
	now := time.Date(2026, 8, 26, 20, 0, 0, 0, time.UTC)
	queue := New(model.QueueConfig{Name: "jobs", Ordering: model.FIFO, MaxDeliveries: 2}, now)
	if _, err := queue.Enqueue("message", json.RawMessage(`1`), 0, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	queue.Advance(time.Second - time.Nanosecond)
	if _, ok := queue.Receive(time.Second); ok {
		t.Fatal("message delivered before its availability boundary")
	}
	queue.Advance(time.Nanosecond)
	first, ok := queue.Receive(time.Second)
	if !ok {
		t.Fatal("message unavailable at its availability boundary")
	}
	queue.Advance(time.Second)
	if err := queue.Ack("message", first.Receipt); !errors.Is(err, ErrNotLeased) {
		t.Fatalf("expired receipt error = %v, want %v", err, ErrNotLeased)
	}
	second, ok := queue.Receive(time.Second)
	if !ok || second.DeliveryCount != 2 {
		t.Fatalf("second delivery = %+v, %t", second, ok)
	}
	queue.Advance(time.Second)
	dead := queue.Messages(model.StateDead)
	if len(dead) != 1 || dead[0].ID != "message" {
		t.Fatalf("dead letters = %+v", dead)
	}
	if err := queue.CheckInvariants(); err != nil {
		t.Fatal(err)
	}
}

func TestStaleReceiptCannotAcknowledgeNewLease(t *testing.T) {
	now := time.Date(2026, 8, 26, 20, 0, 0, 0, time.UTC)
	queue := New(model.QueueConfig{Name: "jobs", Ordering: model.FIFO, MaxDeliveries: 3}, now)
	if _, err := queue.Enqueue("message", json.RawMessage(`1`), 0, now); err != nil {
		t.Fatal(err)
	}
	first, _ := queue.Receive(time.Second)
	queue.Advance(time.Second)
	second, _ := queue.Receive(time.Second)
	if err := queue.Ack("message", first.Receipt); !errors.Is(err, ErrStaleReceipt) {
		t.Fatalf("stale receipt error = %v, want %v", err, ErrStaleReceipt)
	}
	if err := queue.Ack("message", second.Receipt); err != nil {
		t.Fatal(err)
	}
}

func TestRandomOperationHistoriesPreserveInvariants(t *testing.T) {
	const (
		seeds      = 100
		operations = 1_000
	)
	for seed := uint64(0); seed < seeds; seed++ {
		seed := seed
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			now := time.Unix(1_700_000_000, 0).UTC()
			queue := New(model.QueueConfig{
				Name:            "jobs",
				Ordering:        []model.Ordering{model.FIFO, model.LIFO}[seed%2],
				PriorityEnabled: seed%3 != 0,
				MaxDeliveries:   4,
			}, now)
			random := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
			receipts := make(map[string]string)
			nextID := 0
			for step := 0; step < operations; step++ {
				switch random.IntN(6) {
				case 0, 1:
					nextID++
					id := fmt.Sprintf("m-%d", nextID)
					delay := time.Duration(random.IntN(5)) * time.Millisecond
					_, err := queue.Enqueue(id, json.RawMessage(fmt.Sprintf(`{"n":%d}`, nextID)), int32(random.IntN(11)-5), queue.now.Add(delay))
					if err != nil {
						t.Fatalf("step %d: %v", step, err)
					}
				case 2:
					if delivery, ok := queue.Receive(time.Duration(random.IntN(5)+1) * time.Millisecond); ok {
						receipts[delivery.Message.ID] = delivery.Receipt
					}
				case 3:
					for id, receipt := range receipts {
						if err := queue.Ack(id, receipt); err == nil || errors.Is(err, ErrNotLeased) || errors.Is(err, ErrStaleReceipt) {
							delete(receipts, id)
						}
						break
					}
				case 4:
					for id, receipt := range receipts {
						_, err := queue.Nack(id, receipt, time.Duration(random.IntN(4))*time.Millisecond, "generated")
						if err == nil || errors.Is(err, ErrNotLeased) || errors.Is(err, ErrStaleReceipt) {
							delete(receipts, id)
						}
						break
					}
				case 5:
					queue.Advance(time.Duration(random.IntN(5)) * time.Millisecond)
				}
				if err := queue.CheckInvariants(); err != nil {
					t.Fatalf("step %d: %v", step, err)
				}
			}
		})
	}
}

func expectedOrder(ordering model.Ordering, priorityEnabled bool) []string {
	if priorityEnabled {
		if ordering == model.LIFO {
			return []string{"m3", "m2", "m1", "m4"}
		}
		return []string{"m2", "m3", "m1", "m4"}
	}
	if ordering == model.LIFO {
		return []string{"m4", "m3", "m2", "m1"}
	}
	return []string{"m1", "m2", "m3", "m4"}
}
