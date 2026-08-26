package integration

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/ElijahUmana/queuemaxxing/internal/integration/referencemodel"
	"github.com/ElijahUmana/queuemaxxing/internal/model"
)

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
