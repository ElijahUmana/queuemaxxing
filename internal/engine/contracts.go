package engine

import (
	"context"
	"errors"

	"github.com/ElijahUmana/queuemaxxing/internal/model"
)

type ErrorCode string

const (
	CodeInvalid             ErrorCode = "invalid_request"
	CodeNotFound            ErrorCode = "not_found"
	CodeConflict            ErrorCode = "conflict"
	CodeStaleReceipt        ErrorCode = "stale_receipt"
	CodeIdempotencyConflict ErrorCode = "idempotency_conflict"
	CodeCapacityExceeded    ErrorCode = "capacity_exceeded"
	CodeStorageUnavailable  ErrorCode = "storage_unavailable"
	CodeClosed              ErrorCode = "closed"
)

type Error struct {
	Code    ErrorCode
	Message string
	Cause   error
}

func (err *Error) Error() string {
	return err.Message
}

func (err *Error) Unwrap() error {
	return err.Cause
}

func IsCode(err error, code ErrorCode) bool {
	var serviceError *Error
	return errors.As(err, &serviceError) && serviceError.Code == code
}

type Service interface {
	CreateQueue(context.Context, model.QueueConfig, string) (model.QueueInfo, bool, error)
	ListQueues(context.Context) ([]model.QueueInfo, error)
	GetQueue(context.Context, string) (model.QueueInfo, error)
	Enqueue(context.Context, string, model.EnqueueRequest) (model.Message, bool, error)
	Receive(context.Context, string, model.ReceiveRequest) (*model.Delivery, bool, error)
	Ack(context.Context, string, model.AckRequest) (bool, error)
	Nack(context.Context, string, model.NackRequest) (model.Message, bool, error)
	Extend(context.Context, string, model.ExtendRequest) (model.Delivery, bool, error)
	ListMessages(context.Context, string, model.ListFilter) (model.MessagePage, error)
	ListDeadLetters(context.Context, string, model.ListFilter) (model.MessagePage, error)
	Redrive(context.Context, string, model.RedriveRequest) (model.RedriveResult, bool, error)
	Stats(context.Context) (model.ServiceStats, error)
	Compact(context.Context) error
	Ready() error
	Close(context.Context) error
}
