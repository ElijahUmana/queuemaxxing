package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/ElijahUmana/queuemaxxing/internal/model"
)

type queueMutationResult struct {
	Info model.QueueInfo `json:"info"`
}

type enqueueMutationResult struct {
	Message model.Message `json:"message"`
}

type receiveMutationResult struct {
	Delivery *model.Delivery `json:"delivery,omitempty"`
}

type nackMutationResult struct {
	Message model.Message `json:"message"`
}

type extendMutationResult struct {
	Delivery model.Delivery `json:"delivery"`
}

type redriveMutationResult struct {
	Result model.RedriveResult `json:"result"`
}

func (s *service) CreateQueue(ctx context.Context, config model.QueueConfig, idempotencyKey string) (model.QueueInfo, bool, error) {
	if err := validateQueueName(config.Name); err != nil {
		return model.QueueInfo{}, false, err
	}
	if config.Ordering != model.FIFO && config.Ordering != model.LIFO {
		return model.QueueInfo{}, false, invalid("ordering must be fifo or lifo")
	}
	if config.DefaultDelay < 0 || config.DefaultDelay > s.limits.MaxDelay {
		return model.QueueInfo{}, false, invalid("default delay is outside configured bounds")
	}
	if config.DefaultVisibilityTimeout <= 0 || config.DefaultVisibilityTimeout > s.limits.MaxVisibilityTimeout {
		return model.QueueInfo{}, false, invalid("default visibility timeout is outside configured bounds")
	}
	if config.MaxDeliveries == 0 {
		return model.QueueInfo{}, false, invalid("max deliveries must be positive")
	}
	requestFingerprint, err := fingerprint(config)
	if err != nil {
		return model.QueueInfo{}, false, err
	}

	var result queueMutationResult
	replayed := false
	err = s.mutate(ctx, config.Name, operationCreateQueue, idempotencyKey, func() error {
		if found, loadErr := s.loadIdempotencyLocked(operationCreateQueue, config.Name, idempotencyKey, requestFingerprint, &result); found || loadErr != nil {
			replayed = found
			return loadErr
		}
		if _, exists := s.state.Queues[config.Name]; exists {
			return conflict("queue already exists")
		}
		if len(s.state.Queues) >= s.limits.MaxQueues {
			return capacity("queue capacity exceeded")
		}
		config.CreatedAt = s.clock.Now()
		queue := &queueState{
			Config: config, Messages: make(map[string]*model.Message), Receipts: make(map[string]string),
			AckedAt: make(map[string]time.Time), AckedReceipts: make(map[string]ackReceipt),
		}
		s.state.Queues[config.Name] = queue
		result.Info = cloneQueueInfo(queue, s.clock.Now())
		return s.saveIdempotencyLocked(operationCreateQueue, config.Name, idempotencyKey, requestFingerprint, result)
	})
	return result.Info, replayed, err
}

func (s *service) ListQueues(ctx context.Context) ([]model.QueueInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpenLocked(); err != nil {
		return nil, err
	}
	now := s.clock.Now()
	result := make([]model.QueueInfo, 0, len(s.state.Queues))
	for _, queue := range s.state.Queues {
		result = append(result, cloneQueueInfo(queue, now))
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Config.Name < result[j].Config.Name })
	return result, nil
}

func (s *service) GetQueue(ctx context.Context, name string) (model.QueueInfo, error) {
	if err := ctx.Err(); err != nil {
		return model.QueueInfo{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpenLocked(); err != nil {
		return model.QueueInfo{}, err
	}
	queue, exists := s.state.Queues[name]
	if !exists {
		return model.QueueInfo{}, notFound("queue not found")
	}
	return cloneQueueInfo(queue, s.clock.Now()), nil
}

func (s *service) Enqueue(ctx context.Context, queueName string, request model.EnqueueRequest) (model.Message, bool, error) {
	if !json.Valid(request.Payload) {
		return model.Message{}, false, invalid("payload must be valid JSON")
	}
	if len(request.Payload) > s.limits.MaxPayloadBytes {
		return model.Message{}, false, capacity("payload exceeds configured limit")
	}
	if request.Delay < 0 || request.Delay > s.limits.MaxDelay {
		return model.Message{}, false, invalid("delay is outside configured bounds")
	}
	requestFingerprint, err := fingerprint(request)
	if err != nil {
		return model.Message{}, false, err
	}
	var result enqueueMutationResult
	replayed := false
	err = s.mutate(ctx, queueName, operationEnqueue, request.IdempotencyKey, func() error {
		if found, loadErr := s.loadIdempotencyLocked(operationEnqueue, queueName, request.IdempotencyKey, requestFingerprint, &result); found || loadErr != nil {
			replayed = found
			return loadErr
		}
		queue, exists := s.state.Queues[queueName]
		if !exists {
			return notFound("queue not found")
		}
		if !queue.Config.PriorityEnabled && (request.PrioritySet || request.Priority != 0) {
			return invalid("priority is disabled for this queue")
		}
		if queueMessageCount(queue) >= s.limits.MaxMessagesPerQueue || s.totalMessageCountLocked() >= s.limits.MaxMessages {
			return capacity("message capacity exceeded")
		}
		now := s.clock.Now()
		delay := request.Delay
		if !request.DelaySet && delay == 0 {
			delay = queue.Config.DefaultDelay
		}
		visibilityTime := availableAt(now, delay, request.AvailableAt)
		messageID, idErr := newID()
		if idErr != nil {
			return &Error{Code: CodeStorageUnavailable, Message: "generate message id", Cause: idErr}
		}
		state := model.StateReady
		if visibilityTime.After(now) {
			state = model.StateDelayed
		}
		message := &model.Message{
			ID: messageID, Queue: queueName, Payload: append(json.RawMessage(nil), request.Payload...), Priority: request.Priority,
			Sequence: s.state.NextSequence, EnqueuedAt: now, AvailableAt: visibilityTime, State: state,
		}
		s.state.NextSequence++
		queue.Messages[messageID] = message
		message.LastLSN = s.nextLSNLocked()
		result.Message = cloneMessage(message)
		return s.saveIdempotencyLocked(operationEnqueue, queueName, request.IdempotencyKey, requestFingerprint, result)
	})
	return result.Message, replayed, err
}

func (s *service) Receive(ctx context.Context, queueName string, request model.ReceiveRequest) (*model.Delivery, bool, error) {
	if request.VisibilityTimeout < 0 || request.VisibilityTimeout > s.limits.MaxVisibilityTimeout {
		return nil, false, invalid("visibility timeout is outside configured bounds")
	}
	if request.WaitTimeout < 0 || request.WaitTimeout > s.limits.MaxWaitTimeout {
		return nil, false, invalid("wait timeout is outside configured bounds")
	}
	requestFingerprint, err := fingerprint(request)
	if err != nil {
		return nil, false, err
	}
	deadline := s.clock.Now().Add(request.WaitTimeout)
	for {
		delivery, found, wake, nextEvent, receiveErr := s.receiveOnce(ctx, queueName, request, requestFingerprint)
		if receiveErr != nil || delivery != nil || found {
			return delivery, found, receiveErr
		}
		if request.WaitTimeout == 0 {
			return s.finishEmptyReceive(ctx, queueName, request, requestFingerprint)
		}
		now := s.clock.Now()
		if !now.Before(deadline) {
			return s.finishEmptyReceive(ctx, queueName, request, requestFingerprint)
		}
		wait := deadline.Sub(now)
		if !nextEvent.IsZero() && nextEvent.After(now) && nextEvent.Sub(now) < wait {
			wait = nextEvent.Sub(now)
		}
		if wait <= 0 {
			return s.finishEmptyReceive(ctx, queueName, request, requestFingerprint)
		}
		timer := s.clock.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, false, ctx.Err()
		case <-wake:
			timer.Stop()
		case <-timer.C():
		}
	}
}

func (s *service) finishEmptyReceive(ctx context.Context, queueName string, request model.ReceiveRequest, requestFingerprint string) (*model.Delivery, bool, error) {
	if request.IdempotencyKey == "" {
		return nil, false, nil
	}
	result := receiveMutationResult{}
	replayed := false
	err := s.mutate(ctx, queueName, operationReceive, request.IdempotencyKey, func() error {
		if found, loadErr := s.loadIdempotencyLocked(operationReceive, queueName, request.IdempotencyKey, requestFingerprint, &result); found || loadErr != nil {
			replayed = found
			return loadErr
		}
		if _, exists := s.state.Queues[queueName]; !exists {
			return notFound("queue not found")
		}
		return s.saveIdempotencyLocked(operationReceive, queueName, request.IdempotencyKey, requestFingerprint, result)
	})
	return result.Delivery, replayed, err
}

func (s *service) receiveOnce(ctx context.Context, queueName string, request model.ReceiveRequest, requestFingerprint string) (*model.Delivery, bool, <-chan struct{}, time.Time, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, nil, time.Time{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpenLocked(); err != nil {
		return nil, false, nil, time.Time{}, err
	}
	var saved receiveMutationResult
	if found, err := s.loadIdempotencyLocked(operationReceive, queueName, request.IdempotencyKey, requestFingerprint, &saved); found || err != nil {
		return saved.Delivery, found, s.wake, time.Time{}, err
	}
	queue, exists := s.state.Queues[queueName]
	if !exists {
		return nil, false, nil, time.Time{}, notFound("queue not found")
	}
	now := s.clock.Now()
	candidate := selectCandidate(queue, now)
	if candidate == nil {
		return nil, false, s.wake, nextQueueEvent(queue, now), nil
	}
	backup, err := s.backupMutationLocked(queueName, operationReceive, request.IdempotencyKey)
	if err != nil {
		return nil, false, nil, time.Time{}, &Error{Code: CodeStorageUnavailable, Message: "snapshot reservation", Cause: err}
	}
	if candidate.State == model.StateLeased {
		delete(queue.Receipts, candidate.ID)
		clearLease(candidate)
		candidate.LastFailureReason = "visibility_timeout"
	}
	candidate.State = model.StateReady
	visibilityTimeout := request.VisibilityTimeout
	if visibilityTimeout == 0 {
		visibilityTimeout = queue.Config.DefaultVisibilityTimeout
	}
	candidate.State = model.StateLeased
	candidate.DeliveryCount++
	candidate.LeaseEpoch++
	candidate.LeasedAt = timePointer(now)
	leaseUntil := now.Add(visibilityTimeout)
	candidate.LeaseUntil = timePointer(leaseUntil)
	receipt, receiptErr := newReceipt(candidate.ID, candidate.LeaseEpoch)
	if receiptErr != nil {
		s.restoreMutationLocked(backup)
		return nil, false, nil, time.Time{}, &Error{Code: CodeStorageUnavailable, Message: "generate receipt", Cause: receiptErr}
	}
	queue.Receipts[candidate.ID] = receipt
	candidate.LastLSN = s.nextLSNLocked()
	delivery := &model.Delivery{Message: cloneMessage(candidate), Receipt: receipt, LeaseUntil: leaseUntil, DeliveryCount: candidate.DeliveryCount}
	result := receiveMutationResult{Delivery: delivery}
	if err := s.saveIdempotencyLocked(operationReceive, queueName, request.IdempotencyKey, requestFingerprint, result); err != nil {
		s.restoreMutationLocked(backup)
		return nil, false, nil, time.Time{}, err
	}
	if _, err := s.persistDeltaLocked(ctx, s.mutationDeltaLocked(backup)); err != nil {
		s.restoreMutationLocked(backup)
		return nil, false, nil, time.Time{}, &Error{Code: CodeStorageUnavailable, Message: "persist reservation", Cause: err}
	}
	s.notifyLocked()
	return delivery, true, s.wake, time.Time{}, nil
}

func (s *service) Ack(ctx context.Context, queueName string, request model.AckRequest) (bool, error) {
	if request.MessageID == "" || request.Receipt == "" {
		return false, invalid("message id and receipt are required")
	}
	requestFingerprint, err := fingerprint(request)
	if err != nil {
		return false, err
	}
	replayed := false
	err = s.mutate(ctx, queueName, operationAck, request.IdempotencyKey, func() error {
		var previous struct {
			Acked bool `json:"acked"`
		}
		if found, loadErr := s.loadIdempotencyLocked(operationAck, queueName, request.IdempotencyKey, requestFingerprint, &previous); found || loadErr != nil {
			replayed = found
			return loadErr
		}
		queue, message, leaseErr := s.validateLeaseLocked(queueName, request.MessageID, request.Receipt, s.clock.Now())
		if leaseErr != nil {
			if queue != nil {
				if receipt, ok := queue.AckedReceipts[request.Receipt]; ok && receipt.MessageID == request.MessageID && receipt.ExpiresAt.After(s.clock.Now()) {
					replayed = true
					return nil
				}
			}
			return leaseErr
		}
		now := s.clock.Now()
		delete(queue.Receipts, message.ID)
		message.State = model.StateAcked
		message.LeasedAt = nil
		message.LeaseUntil = nil
		queue.AckedAt[message.ID] = now
		queue.AckedReceipts[request.Receipt] = ackReceipt{MessageID: message.ID, ExpiresAt: now.Add(s.limits.AckTombstoneRetention)}
		return s.saveIdempotencyLocked(operationAck, queueName, request.IdempotencyKey, requestFingerprint, struct {
			Acked bool `json:"acked"`
		}{true})
	})
	return replayed, err
}

func (s *service) Nack(ctx context.Context, queueName string, request model.NackRequest) (model.Message, bool, error) {
	if request.MessageID == "" || request.Receipt == "" {
		return model.Message{}, false, invalid("message id and receipt are required")
	}
	if request.Delay < 0 || request.Delay > s.limits.MaxDelay {
		return model.Message{}, false, invalid("delay is outside configured bounds")
	}
	requestFingerprint, err := fingerprint(request)
	if err != nil {
		return model.Message{}, false, err
	}
	var result nackMutationResult
	replayed := false
	err = s.mutate(ctx, queueName, operationNack, request.IdempotencyKey, func() error {
		if found, loadErr := s.loadIdempotencyLocked(operationNack, queueName, request.IdempotencyKey, requestFingerprint, &result); found || loadErr != nil {
			replayed = found
			return loadErr
		}
		queue, message, leaseErr := s.validateLeaseLocked(queueName, request.MessageID, request.Receipt, s.clock.Now())
		if leaseErr != nil {
			return leaseErr
		}
		now := s.clock.Now()
		delete(queue.Receipts, message.ID)
		clearLease(message)
		message.LastFailureReason = normalizeReason(request.Reason)
		if message.DeliveryCount >= queue.Config.MaxDeliveries {
			message.State = model.StateDead
			message.DeadAt = timePointer(now)
		} else {
			message.AvailableAt = now.Add(request.Delay)
			message.State = model.StateReady
			if request.Delay > 0 {
				message.State = model.StateDelayed
			}
		}
		message.LastLSN = s.nextLSNLocked()
		result.Message = cloneMessage(message)
		return s.saveIdempotencyLocked(operationNack, queueName, request.IdempotencyKey, requestFingerprint, result)
	})
	return result.Message, replayed, err
}

func (s *service) Extend(ctx context.Context, queueName string, request model.ExtendRequest) (model.Delivery, bool, error) {
	if request.MessageID == "" || request.Receipt == "" {
		return model.Delivery{}, false, invalid("message id and receipt are required")
	}
	if request.VisibilityTimeout <= 0 || request.VisibilityTimeout > s.limits.MaxVisibilityTimeout {
		return model.Delivery{}, false, invalid("visibility timeout is outside configured bounds")
	}
	requestFingerprint, err := fingerprint(request)
	if err != nil {
		return model.Delivery{}, false, err
	}
	var result extendMutationResult
	replayed := false
	err = s.mutate(ctx, queueName, operationExtend, request.IdempotencyKey, func() error {
		if found, loadErr := s.loadIdempotencyLocked(operationExtend, queueName, request.IdempotencyKey, requestFingerprint, &result); found || loadErr != nil {
			replayed = found
			return loadErr
		}
		_, message, leaseErr := s.validateLeaseLocked(queueName, request.MessageID, request.Receipt, s.clock.Now())
		if leaseErr != nil {
			return leaseErr
		}
		leaseUntil := s.clock.Now().Add(request.VisibilityTimeout)
		message.LeaseUntil = timePointer(leaseUntil)
		message.LastLSN = s.nextLSNLocked()
		result.Delivery = model.Delivery{Message: cloneMessage(message), Receipt: request.Receipt, LeaseUntil: leaseUntil, DeliveryCount: message.DeliveryCount}
		return s.saveIdempotencyLocked(operationExtend, queueName, request.IdempotencyKey, requestFingerprint, result)
	})
	return result.Delivery, replayed, err
}

func (s *service) Redrive(ctx context.Context, queueName string, request model.RedriveRequest) (model.RedriveResult, bool, error) {
	if request.MessageID == "" {
		return model.RedriveResult{}, false, invalid("message id is required")
	}
	if request.Delay < 0 || request.Delay > s.limits.MaxDelay {
		return model.RedriveResult{}, false, invalid("delay is outside configured bounds")
	}
	requestFingerprint, err := fingerprint(request)
	if err != nil {
		return model.RedriveResult{}, false, err
	}
	var result redriveMutationResult
	replayed := false
	err = s.mutate(ctx, queueName, operationRedrive, request.IdempotencyKey, func() error {
		if found, loadErr := s.loadIdempotencyLocked(operationRedrive, queueName, request.IdempotencyKey, requestFingerprint, &result); found || loadErr != nil {
			replayed = found
			return loadErr
		}
		queue, exists := s.state.Queues[queueName]
		if !exists {
			return notFound("queue not found")
		}
		source, exists := queue.Messages[request.MessageID]
		if !exists {
			return notFound("message not found")
		}
		now := s.clock.Now()
		if logicalState(source, now, queue.Config.MaxDeliveries) == model.StateDead && source.State != model.StateDead {
			delete(queue.Receipts, source.ID)
			clearLease(source)
			source.State = model.StateDead
			source.LastFailureReason = "visibility_timeout"
			source.DeadAt = timePointer(now)
		}
		if source.State != model.StateDead {
			return conflict("only dead letters can be redriven")
		}
		if queueMessageCount(queue) >= s.limits.MaxMessagesPerQueue || s.totalMessageCountLocked() >= s.limits.MaxMessages {
			return capacity("message capacity exceeded")
		}
		childID, idErr := newID()
		if idErr != nil {
			return &Error{Code: CodeStorageUnavailable, Message: "generate message id", Cause: idErr}
		}
		priority := source.Priority
		if request.Priority != nil {
			priority = *request.Priority
		}
		visibilityTime := availableAt(now, request.Delay, request.AvailableAt)
		state := model.StateReady
		if visibilityTime.After(now) {
			state = model.StateDelayed
		}
		child := &model.Message{
			ID: childID, Queue: queueName, Payload: append(json.RawMessage(nil), source.Payload...), Priority: priority,
			Sequence: s.state.NextSequence, EnqueuedAt: now, AvailableAt: visibilityTime, State: state, ReplayOf: source.ID,
		}
		s.state.NextSequence++
		queue.Messages[child.ID] = child
		source.LastLSN = s.nextLSNLocked()
		child.LastLSN = source.LastLSN
		result.Result = model.RedriveResult{Source: cloneMessage(source), Child: cloneMessage(child)}
		return s.saveIdempotencyLocked(operationRedrive, queueName, request.IdempotencyKey, requestFingerprint, result)
	})
	return result.Result, replayed, err
}

func (s *service) validateLeaseLocked(queueName, messageID, receipt string, now time.Time) (*queueState, *model.Message, error) {
	queue, exists := s.state.Queues[queueName]
	if !exists {
		return nil, nil, notFound("queue not found")
	}
	message, exists := queue.Messages[messageID]
	if !exists {
		return queue, nil, notFound("message not found")
	}
	if message.State != model.StateLeased || message.LeaseUntil == nil || !now.Before(*message.LeaseUntil) || queue.Receipts[messageID] != receipt {
		return queue, message, &Error{Code: CodeStaleReceipt, Message: "receipt is stale or lease is no longer active"}
	}
	return queue, message, nil
}

func (s *service) materializeLocked(queue *queueState, now time.Time) bool {
	changed := false
	for id, message := range queue.Messages {
		switch message.State {
		case model.StateDelayed:
			if !message.AvailableAt.After(now) {
				message.State = model.StateReady
				changed = true
			}
		case model.StateLeased:
			if message.LeaseUntil != nil && !now.Before(*message.LeaseUntil) {
				delete(queue.Receipts, id)
				clearLease(message)
				message.LastFailureReason = "visibility_timeout"
				if message.DeliveryCount >= queue.Config.MaxDeliveries {
					message.State = model.StateDead
					message.DeadAt = timePointer(now)
				} else {
					message.State = model.StateReady
					message.AvailableAt = now
				}
				changed = true
			}
		case model.StateAcked:
			if ackedAt, exists := queue.AckedAt[id]; exists && !ackedAt.Add(s.limits.AckTombstoneRetention).After(now) {
				delete(queue.Messages, id)
				delete(queue.AckedAt, id)
				changed = true
			}
		}
	}
	for receipt, record := range queue.AckedReceipts {
		if !record.ExpiresAt.After(now) {
			delete(queue.AckedReceipts, receipt)
			changed = true
		}
	}
	return changed
}

func selectCandidate(queue *queueState, now time.Time) *model.Message {
	var selected *model.Message
	for _, message := range queue.Messages {
		state := logicalState(message, now, queue.Config.MaxDeliveries)
		if state != model.StateReady || message.AvailableAt.After(now) || message.DeliveryCount >= queue.Config.MaxDeliveries {
			continue
		}
		if selected == nil || compareMessages(message, selected, queue.Config) {
			selected = message
		}
	}
	return selected
}

func nextQueueEvent(queue *queueState, now time.Time) time.Time {
	var next time.Time
	for _, message := range queue.Messages {
		var candidate time.Time
		state := logicalState(message, now, queue.Config.MaxDeliveries)
		if state == model.StateDelayed {
			candidate = message.AvailableAt
		}
		if state == model.StateLeased && message.LeaseUntil != nil {
			candidate = *message.LeaseUntil
		}
		if candidate.After(now) && (next.IsZero() || candidate.Before(next)) {
			next = candidate
		}
	}
	return next
}

func clearLease(message *model.Message) {
	message.LeaseToken = ""
	message.LeasedAt = nil
	message.LeaseUntil = nil
}

func timePointer(value time.Time) *time.Time { return &value }

func (s *service) ListMessages(ctx context.Context, queueName string, filter model.ListFilter) (model.MessagePage, error) {
	return s.listMessages(ctx, queueName, filter, false)
}

func (s *service) ListDeadLetters(ctx context.Context, queueName string, filter model.ListFilter) (model.MessagePage, error) {
	filter.State = model.StateDead
	return s.listMessages(ctx, queueName, filter, true)
}

func (s *service) listMessages(ctx context.Context, queueName string, filter model.ListFilter, deadOnly bool) (model.MessagePage, error) {
	if err := ctx.Err(); err != nil {
		return model.MessagePage{}, err
	}
	if filter.Limit < 0 || filter.Limit > s.limits.MaxListLimit {
		return model.MessagePage{}, invalid("list limit is outside configured bounds")
	}
	if filter.Limit == 0 {
		filter.Limit = min(100, s.limits.MaxListLimit)
	}
	cursor, err := decodeCursor(filter.Cursor)
	if err != nil {
		return model.MessagePage{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpenLocked(); err != nil {
		return model.MessagePage{}, err
	}
	queue, exists := s.state.Queues[queueName]
	if !exists {
		return model.MessagePage{}, notFound("queue not found")
	}
	now := s.clock.Now()
	matches := make([]*model.Message, 0)
	for _, message := range queue.Messages {
		state := logicalState(message, now, queue.Config.MaxDeliveries)
		if deadOnly && state != model.StateDead {
			continue
		}
		if !deadOnly && filter.State != "" && state != filter.State {
			continue
		}
		if cursor > 0 && message.Sequence <= cursor {
			continue
		}
		matches = append(matches, message)
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i].Sequence < matches[j].Sequence })
	page := model.MessagePage{SnapshotLSN: s.journal.Stats().DurableLSN}
	resultCount := min(len(matches), filter.Limit)
	page.Messages = make([]model.Message, 0, resultCount)
	for _, message := range matches[:resultCount] {
		copy := cloneMessage(message)
		copy.State = logicalState(message, now, queue.Config.MaxDeliveries)
		page.Messages = append(page.Messages, copy)
	}
	if len(matches) > filter.Limit {
		page.NextCursor = encodeCursor(page.Messages[len(page.Messages)-1].Sequence)
	}
	return page, nil
}

func (s *service) Stats(ctx context.Context) (model.ServiceStats, error) {
	if err := ctx.Err(); err != nil {
		return model.ServiceStats{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpenLocked(); err != nil {
		return model.ServiceStats{}, err
	}
	now := s.clock.Now()
	result := model.ServiceStats{Queues: uint64(len(s.state.Queues))}
	for _, queue := range s.state.Queues {
		queueCounts := counts(queue, now)
		result.Messages.Ready += queueCounts.Ready
		result.Messages.Delayed += queueCounts.Delayed
		result.Messages.InFlight += queueCounts.InFlight
		result.Messages.Dead += queueCounts.Dead
		result.Messages.Acked += queueCounts.Acked
		result.Messages.Total += queueCounts.Total
	}
	stats := s.journal.Stats()
	result.DurableLSN = stats.DurableLSN
	result.WALBytes = stats.WALBytes
	result.SnapshotGeneration = stats.SnapshotGeneration
	result.LastSyncAt = stats.LastSyncAt
	result.ReadOnly = stats.ReadOnly
	result.ReadOnlyReason = stats.ReadOnlyReason
	return result, nil
}

func (s *service) Compact(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpenLocked(); err != nil {
		return err
	}
	checkpoint, err := cloneStateForCheckpoint(s.state)
	if err != nil {
		return &Error{Code: CodeStorageUnavailable, Message: "clone checkpoint state", Cause: err}
	}
	now := s.clock.Now()
	original := s.state
	s.state = checkpoint
	for _, queue := range s.state.Queues {
		s.materializeLocked(queue, now)
	}
	s.pruneIdempotencyLocked(now)
	checkpoint = s.state
	s.state = original
	stateBytes, err := json.Marshal(checkpoint)
	if err != nil {
		return &Error{Code: CodeStorageUnavailable, Message: "encode checkpoint state", Cause: err}
	}
	envelopeBytes, err := json.Marshal(persistedEnvelope{Kind: "state", Version: stateVersion, State: stateBytes})
	if err != nil {
		return &Error{Code: CodeStorageUnavailable, Message: "encode checkpoint envelope", Cause: err}
	}
	if err := s.journal.Checkpoint(ctx, s.journal.Stats().DurableLSN, envelopeBytes); err != nil {
		return &Error{Code: CodeStorageUnavailable, Message: "checkpoint queue state", Cause: err}
	}
	s.state = checkpoint
	s.notifyLocked()
	return nil
}

func (s *service) Ready() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOpenLocked(); err != nil {
		return err
	}
	stats := s.journal.Stats()
	if stats.ReadOnly {
		return &Error{Code: CodeStorageUnavailable, Message: fmt.Sprintf("journal is read-only: %s", stats.ReadOnlyReason)}
	}
	return nil
}

func (s *service) Close(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.notifyLocked()
	s.mu.Unlock()
	if err := s.journal.Close(); err != nil {
		return &Error{Code: CodeStorageUnavailable, Message: "close journal", Cause: err}
	}
	return nil
}
