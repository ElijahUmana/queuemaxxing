package referencemodel

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/ElijahUmana/queuemaxxing/internal/model"
)

var (
	ErrNotFound     = errors.New("not found")
	ErrNotLeased    = errors.New("message is not leased")
	ErrStaleReceipt = errors.New("stale receipt")
)

type Queue struct {
	config    model.QueueConfig
	now       time.Time
	nextSeq   uint64
	messages  map[string]model.Message
	receipts  map[string]string
	nextToken uint64
}

func New(config model.QueueConfig, now time.Time) *Queue {
	return &Queue{
		config:   config,
		now:      now,
		messages: make(map[string]model.Message),
		receipts: make(map[string]string),
	}
}

func (queue *Queue) SetTime(now time.Time) {
	queue.now = now
	queue.promoteAndExpire()
}

func (queue *Queue) Advance(duration time.Duration) {
	queue.SetTime(queue.now.Add(duration))
}

func (queue *Queue) Enqueue(id string, payload json.RawMessage, priority int32, availableAt time.Time) (model.Message, error) {
	if _, exists := queue.messages[id]; exists {
		return model.Message{}, fmt.Errorf("duplicate message %q", id)
	}
	queue.nextSeq++
	state := model.StateReady
	if availableAt.After(queue.now) {
		state = model.StateDelayed
	}
	message := model.Message{
		ID:          id,
		Queue:       queue.config.Name,
		Payload:     slices.Clone(payload),
		Priority:    priority,
		Sequence:    queue.nextSeq,
		EnqueuedAt:  queue.now,
		AvailableAt: availableAt,
		State:       state,
	}
	queue.messages[id] = message
	return cloneMessage(message), nil
}

func (queue *Queue) Receive(visibilityTimeout time.Duration) (*model.Delivery, bool) {
	queue.promoteAndExpire()
	ready := queue.ordered(model.StateReady)
	if len(ready) == 0 {
		return nil, false
	}
	message := ready[0]
	message.State = model.StateLeased
	message.DeliveryCount++
	message.LeaseEpoch++
	leasedAt := queue.now
	leaseUntil := queue.now.Add(visibilityTimeout)
	message.LeasedAt = &leasedAt
	message.LeaseUntil = &leaseUntil
	queue.nextToken++
	receipt := fmt.Sprintf("r-%s-%d-%d", message.ID, message.LeaseEpoch, queue.nextToken)
	message.LeaseToken = receipt
	queue.messages[message.ID] = message
	queue.receipts[message.ID] = receipt
	return &model.Delivery{
		Message:       cloneMessage(message),
		Receipt:       receipt,
		LeaseUntil:    leaseUntil,
		DeliveryCount: message.DeliveryCount,
	}, true
}

func (queue *Queue) Ack(messageID, receipt string) error {
	message, err := queue.validateLease(messageID, receipt)
	if err != nil {
		return err
	}
	message.State = model.StateAcked
	message.LeaseToken = ""
	message.LeasedAt = nil
	message.LeaseUntil = nil
	queue.messages[messageID] = message
	delete(queue.receipts, messageID)
	return nil
}

func (queue *Queue) Nack(messageID, receipt string, delay time.Duration, reason string) (model.Message, error) {
	message, err := queue.validateLease(messageID, receipt)
	if err != nil {
		return model.Message{}, err
	}
	delete(queue.receipts, messageID)
	message.LeaseToken = ""
	message.LeasedAt = nil
	message.LeaseUntil = nil
	message.LastFailureReason = reason
	message.AvailableAt = queue.now.Add(delay)
	message.State = model.StateReady
	if delay > 0 {
		message.State = model.StateDelayed
	}
	queue.messages[messageID] = message
	return cloneMessage(message), nil
}

func (queue *Queue) Extend(messageID, receipt string, visibilityTimeout time.Duration) (model.Delivery, error) {
	message, err := queue.validateLease(messageID, receipt)
	if err != nil {
		return model.Delivery{}, err
	}
	leaseUntil := queue.now.Add(visibilityTimeout)
	message.LeaseUntil = &leaseUntil
	queue.messages[messageID] = message
	return model.Delivery{
		Message:       cloneMessage(message),
		Receipt:       receipt,
		LeaseUntil:    leaseUntil,
		DeliveryCount: message.DeliveryCount,
	}, nil
}

func (queue *Queue) Messages(state model.MessageState) []model.Message {
	queue.promoteAndExpire()
	return queue.ordered(state)
}

func (queue *Queue) Snapshot() map[string]model.Message {
	queue.promoteAndExpire()
	snapshot := make(map[string]model.Message, len(queue.messages))
	for id, message := range queue.messages {
		snapshot[id] = cloneMessage(message)
	}
	return snapshot
}

func (queue *Queue) Counts() model.QueueCounts {
	queue.promoteAndExpire()
	var counts model.QueueCounts
	for _, message := range queue.messages {
		switch message.State {
		case model.StateReady:
			counts.Ready++
		case model.StateDelayed:
			counts.Delayed++
		case model.StateLeased:
			counts.InFlight++
		case model.StateDead:
			counts.Dead++
		case model.StateAcked:
			counts.Acked++
		}
		counts.Total++
	}
	return counts
}

func (queue *Queue) CheckInvariants() error {
	queue.promoteAndExpire()
	sequences := make(map[uint64]string, len(queue.messages))
	for id, message := range queue.messages {
		if id == "" || message.ID != id {
			return fmt.Errorf("message map key %q does not match ID %q", id, message.ID)
		}
		if message.Queue != queue.config.Name {
			return fmt.Errorf("message %q belongs to queue %q", id, message.Queue)
		}
		if other, exists := sequences[message.Sequence]; exists {
			return fmt.Errorf("messages %q and %q share sequence %d", other, id, message.Sequence)
		}
		sequences[message.Sequence] = id
		receipt, hasReceipt := queue.receipts[id]
		if message.State == model.StateLeased {
			if !hasReceipt || receipt == "" || receipt != message.LeaseToken {
				return fmt.Errorf("leased message %q has inconsistent receipt", id)
			}
			if message.LeasedAt == nil || message.LeaseUntil == nil {
				return fmt.Errorf("leased message %q has incomplete lease timestamps", id)
			}
		} else if hasReceipt || message.LeaseToken != "" || message.LeasedAt != nil || message.LeaseUntil != nil {
			return fmt.Errorf("non-leased message %q retains lease state", id)
		}
		if message.State == model.StateDelayed && !message.AvailableAt.After(queue.now) {
			return fmt.Errorf("delayed message %q is already available", id)
		}
		if message.State == model.StateReady && message.AvailableAt.After(queue.now) {
			return fmt.Errorf("ready message %q is not available", id)
		}
	}
	for id := range queue.receipts {
		if _, exists := queue.messages[id]; !exists {
			return fmt.Errorf("receipt references unknown message %q", id)
		}
	}
	counts := queue.Counts()
	if counts.Total != counts.Ready+counts.Delayed+counts.InFlight+counts.Dead+counts.Acked {
		return fmt.Errorf("counts do not conserve messages: %+v", counts)
	}
	return nil
}

func (queue *Queue) validateLease(messageID, receipt string) (model.Message, error) {
	queue.promoteAndExpire()
	message, exists := queue.messages[messageID]
	if !exists {
		return model.Message{}, ErrNotFound
	}
	if message.State != model.StateLeased {
		return model.Message{}, ErrNotLeased
	}
	if receipt == "" || queue.receipts[messageID] != receipt {
		return model.Message{}, ErrStaleReceipt
	}
	return message, nil
}

func (queue *Queue) promoteAndExpire() {
	for id, message := range queue.messages {
		switch message.State {
		case model.StateDelayed:
			if !message.AvailableAt.After(queue.now) {
				message.State = model.StateReady
				queue.messages[id] = message
			}
		case model.StateLeased:
			if message.LeaseUntil != nil && !message.LeaseUntil.After(queue.now) {
				delete(queue.receipts, id)
				message.LeaseToken = ""
				message.LeasedAt = nil
				message.LeaseUntil = nil
				if queue.config.MaxDeliveries > 0 && message.DeliveryCount >= queue.config.MaxDeliveries {
					message.State = model.StateDead
					deadAt := queue.now
					message.DeadAt = &deadAt
				} else {
					message.State = model.StateReady
					message.AvailableAt = queue.now
				}
				queue.messages[id] = message
			}
		}
	}
}

func (queue *Queue) ordered(state model.MessageState) []model.Message {
	messages := make([]model.Message, 0)
	for _, message := range queue.messages {
		if message.State == state {
			messages = append(messages, cloneMessage(message))
		}
	}
	slices.SortStableFunc(messages, func(left, right model.Message) int {
		if state == model.StateDelayed {
			if order := left.AvailableAt.Compare(right.AvailableAt); order != 0 {
				return order
			}
		}
		if queue.config.PriorityEnabled && left.Priority != right.Priority {
			return cmp.Compare(right.Priority, left.Priority)
		}
		if queue.config.Ordering == model.LIFO {
			return cmp.Compare(right.Sequence, left.Sequence)
		}
		return cmp.Compare(left.Sequence, right.Sequence)
	})
	return messages
}

func cloneMessage(message model.Message) model.Message {
	message.Payload = slices.Clone(message.Payload)
	if message.LeasedAt != nil {
		leasedAt := *message.LeasedAt
		message.LeasedAt = &leasedAt
	}
	if message.LeaseUntil != nil {
		leaseUntil := *message.LeaseUntil
		message.LeaseUntil = &leaseUntil
	}
	if message.DeadAt != nil {
		deadAt := *message.DeadAt
		message.DeadAt = &deadAt
	}
	return message
}
