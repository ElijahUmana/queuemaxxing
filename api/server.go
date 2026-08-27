package api

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/ElijahUmana/queuemaxxing/internal/engine"
	"github.com/ElijahUmana/queuemaxxing/internal/model"
)

const (
	defaultMaxRequestBytes       = int64(1<<20) + 64<<10
	defaultRequestTimeout        = 35 * time.Second
	defaultMaxConcurrentRequests = 256
	defaultMaxLongPolls          = 64
	defaultListLimit             = 50
	maxListLimit                 = 50
	problemBase                  = "urn:queuemaxxing:problem:"
)

type Options struct {
	Logger                *slog.Logger
	MaxRequestBytes       int64
	RequestTimeout        time.Duration
	MaxConcurrentRequests int
	MaxLongPolls          int
	Now                   func() time.Time
	RequestID             func() string
}

type Server struct {
	service         engine.Service
	logger          *slog.Logger
	maxRequestBytes int64
	requestTimeout  time.Duration
	requestPermits  chan struct{}
	longPollPermits chan struct{}
	now             func() time.Time
	requestID       func() string
	draining        atomic.Bool
	compacting      atomic.Bool
	handler         http.Handler
}

func New(service engine.Service, options Options) (*Server, error) {
	if service == nil {
		return nil, errors.New("API service is required")
	}
	if options.Logger == nil {
		options.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if options.MaxRequestBytes == 0 {
		options.MaxRequestBytes = defaultMaxRequestBytes
	}
	if options.MaxRequestBytes < 1 {
		return nil, errors.New("maximum request bytes must be positive")
	}
	if options.RequestTimeout == 0 {
		options.RequestTimeout = defaultRequestTimeout
	}
	if options.RequestTimeout < 1 {
		return nil, errors.New("request timeout must be positive")
	}
	if options.MaxConcurrentRequests == 0 {
		options.MaxConcurrentRequests = defaultMaxConcurrentRequests
	}
	if options.MaxConcurrentRequests < 1 {
		return nil, errors.New("maximum concurrent requests must be positive")
	}
	if options.MaxLongPolls == 0 {
		options.MaxLongPolls = defaultMaxLongPolls
	}
	if options.MaxLongPolls < 1 {
		return nil, errors.New("maximum long polls must be positive")
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.RequestID == nil {
		options.RequestID = randomRequestID
	}
	server := &Server{
		service: service, logger: options.Logger, maxRequestBytes: options.MaxRequestBytes,
		requestTimeout: options.RequestTimeout, requestPermits: make(chan struct{}, options.MaxConcurrentRequests),
		longPollPermits: make(chan struct{}, options.MaxLongPolls), now: options.Now, requestID: options.RequestID,
	}
	server.handler = server.routes()
	return server, nil
}

func (server *Server) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	server.handler.ServeHTTP(response, request)
}

func (server *Server) SetDraining(draining bool) {
	server.draining.Store(draining)
}

func (server *Server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", server.live)
	mux.HandleFunc("GET /health/ready", server.ready)
	mux.HandleFunc("POST /v1/queues", server.createQueue)
	mux.HandleFunc("GET /v1/queues", server.listQueues)
	mux.HandleFunc("GET /v1/queues/{queue}", server.getQueue)
	mux.HandleFunc("POST /v1/queues/{queue}/messages", server.enqueue)
	mux.HandleFunc("POST /v1/queues/{queue}/messages:receive", server.receive)
	mux.HandleFunc("GET /v1/queues/{queue}/messages", server.listMessages)
	mux.HandleFunc("POST /v1/queues/{queue}/messages/{message}/ack", server.ack)
	mux.HandleFunc("POST /v1/queues/{queue}/messages/{message}/nack", server.nack)
	mux.HandleFunc("POST /v1/queues/{queue}/messages/{message}/extend", server.extend)
	mux.HandleFunc("GET /v1/queues/{queue}/dead-letters", server.listDeadLetters)
	mux.HandleFunc("POST /v1/queues/{queue}/dead-letters/{message}/redrive", server.redrive)
	mux.HandleFunc("GET /v1/stats", server.stats)
	mux.HandleFunc("POST /v1/admin/compact", server.compact)
	dispatch := http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		mux.ServeHTTP(&muxProblemWriter{ResponseWriter: response, server: server, request: request}, request)
	})
	return server.middleware(dispatch)
}

type muxProblemWriter struct {
	http.ResponseWriter
	server      *Server
	request     *http.Request
	intercepted bool
}

func (writer *muxProblemWriter) WriteHeader(status int) {
	switch status {
	case http.StatusMethodNotAllowed:
		writer.intercepted = true
		writer.server.writeProblem(writer.ResponseWriter, writer.request, status, "method_not_allowed", "Method not allowed", "The resource does not support this HTTP method.")
	case http.StatusNotFound:
		writer.intercepted = true
		writer.server.writeProblem(writer.ResponseWriter, writer.request, status, "not_found", "Resource not found", "The requested API resource does not exist.")
	default:
		writer.ResponseWriter.WriteHeader(status)
	}
}

func (writer *muxProblemWriter) Write(data []byte) (int, error) {
	if writer.intercepted {
		return len(data), nil
	}
	return writer.ResponseWriter.Write(data)
}

func (server *Server) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		requestID := request.Header.Get("X-Request-ID")
		if !validRequestID(requestID) {
			requestID = server.requestID()
		}
		response.Header().Set("X-Request-ID", requestID)
		response.Header().Set("X-Content-Type-Options", "nosniff")
		response.Header().Set("Cache-Control", "no-store")
		ctx := context.WithValue(request.Context(), requestIDKey{}, requestID)
		request = request.WithContext(ctx)
		if server.draining.Load() && request.URL.Path != "/health/live" && request.URL.Path != "/health/ready" {
			server.writeProblem(response, request, http.StatusServiceUnavailable, "draining", "Service is draining", "The service is not accepting new operations.")
			return
		}
		if request.URL.Path != "/health/live" && request.URL.Path != "/health/ready" {
			if !server.acquirePermit(server.requestPermits) {
				server.capacityProblem(response, request)
				return
			}
			defer server.releasePermit(server.requestPermits)
		}
		ctx = request.Context()
		if _, hasDeadline := ctx.Deadline(); !hasDeadline {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, server.requestTimeout)
			defer cancel()
		}
		next.ServeHTTP(response, request.WithContext(ctx))
	})
}

type requestIDKey struct{}

func (server *Server) capacityProblem(response http.ResponseWriter, request *http.Request) {
	server.serviceProblem(response, request, &engine.Error{Code: engine.CodeCapacityExceeded, Message: "Retry the operation after capacity becomes available."})
}

func (*Server) acquirePermit(permits chan struct{}) bool {
	select {
	case permits <- struct{}{}:
		return true
	default:
		return false
	}
}

func (*Server) releasePermit(permits chan struct{}) {
	<-permits
}

func (server *Server) live(response http.ResponseWriter, request *http.Request) {
	server.writeJSON(response, request, http.StatusOK, StatusResponse{Status: "ok"})
}

func (server *Server) ready(response http.ResponseWriter, request *http.Request) {
	if server.draining.Load() {
		server.writeProblem(response, request, http.StatusServiceUnavailable, "draining", "Service is draining", "The service is shutting down.")
		return
	}
	if err := server.service.Ready(); err != nil {
		server.writeProblem(response, request, http.StatusServiceUnavailable, "not_ready", "Service is not ready", "Durable queue storage is unavailable.")
		return
	}
	server.writeJSON(response, request, http.StatusOK, StatusResponse{Status: "ready"})
}

func (server *Server) createQueue(response http.ResponseWriter, request *http.Request) {
	var input CreateQueueRequest
	if !server.decode(response, request, &input) {
		return
	}
	key, ok := server.idempotencyKey(response, request, false)
	if !ok {
		return
	}
	defaultDelay, delayOK := milliseconds(input.DefaultDelayMS)
	defaultVisibility, visibilityOK := milliseconds(input.DefaultVisibilityTimeoutMS)
	if !delayOK || !visibilityOK || input.DefaultVisibilityTimeoutMS == 0 || input.MaxDeliveries == 0 {
		server.validationProblem(response, request, "queue durations and max_deliveries are outside the allowed range")
		return
	}
	info, replayed, err := server.service.CreateQueue(request.Context(), model.QueueConfig{
		Name: input.Name, Ordering: model.Ordering(input.Ordering), PriorityEnabled: input.PriorityEnabled,
		DefaultDelay:             defaultDelay,
		DefaultVisibilityTimeout: defaultVisibility,
		MaxDeliveries:            input.MaxDeliveries,
	}, key)
	if err != nil {
		server.serviceProblem(response, request, err)
		return
	}
	status := http.StatusCreated
	if replayed {
		status = http.StatusOK
	}
	server.writeJSON(response, request, status, MutationResponse[Queue]{Data: queueFromModel(info), Replayed: replayed})
}

func (server *Server) listQueues(response http.ResponseWriter, request *http.Request) {
	if !server.rejectQuery(response, request, nil) {
		return
	}
	queues, err := server.service.ListQueues(request.Context())
	if err != nil {
		server.serviceProblem(response, request, err)
		return
	}
	result := make([]Queue, len(queues))
	for index := range queues {
		result[index] = queueFromModel(queues[index])
	}
	server.writeJSON(response, request, http.StatusOK, QueueList{Queues: result})
}

func (server *Server) getQueue(response http.ResponseWriter, request *http.Request) {
	if !server.rejectQuery(response, request, nil) {
		return
	}
	info, err := server.service.GetQueue(request.Context(), request.PathValue("queue"))
	if err != nil {
		server.serviceProblem(response, request, err)
		return
	}
	server.writeJSON(response, request, http.StatusOK, queueFromModel(info))
}

func (server *Server) enqueue(response http.ResponseWriter, request *http.Request) {
	var input EnqueueRequest
	if !server.decode(response, request, &input) {
		return
	}
	if len(input.Payload) == 0 || bytes.Equal(bytes.TrimSpace(input.Payload), []byte("null")) {
		server.validationProblem(response, request, "payload is required and cannot be null")
		return
	}
	delay, ok := server.schedule(response, request, input.DelayMS, input.AvailableAt)
	if !ok {
		return
	}
	key, ok := server.idempotencyKey(response, request, false)
	if !ok {
		return
	}
	message, replayed, err := server.service.Enqueue(request.Context(), request.PathValue("queue"), model.EnqueueRequest{
		Payload: input.Payload, Priority: input.Priority,
		Delay: durationPointer(delay, input.DelayMS != nil), AvailableAt: input.AvailableAt, IdempotencyKey: key,
	})
	if err != nil {
		server.serviceProblem(response, request, err)
		return
	}
	status := http.StatusCreated
	if replayed {
		status = http.StatusOK
	}
	server.writeJSON(response, request, status, MutationResponse[Message]{Data: messageFromModel(message), Replayed: replayed})
}

func (server *Server) receive(response http.ResponseWriter, request *http.Request) {
	var input ReceiveRequest
	if !server.decode(response, request, &input) {
		return
	}
	if input.VisibilityTimeoutMS != nil && *input.VisibilityTimeoutMS <= 0 || input.WaitTimeoutMS != nil && *input.WaitTimeoutMS < 0 {
		server.validationProblem(response, request, "receive timeouts are outside the allowed range")
		return
	}
	var visibilityTimeout, waitTimeout time.Duration
	if input.VisibilityTimeoutMS != nil {
		var durationOK bool
		visibilityTimeout, durationOK = milliseconds(*input.VisibilityTimeoutMS)
		if !durationOK || visibilityTimeout == 0 {
			server.validationProblem(response, request, "visibility_timeout_ms is outside the allowed range")
			return
		}
	}
	if input.WaitTimeoutMS != nil {
		var durationOK bool
		waitTimeout, durationOK = milliseconds(*input.WaitTimeoutMS)
		if !durationOK {
			server.validationProblem(response, request, "wait_timeout_ms is outside the allowed range")
			return
		}
	}
	key, ok := server.idempotencyKey(response, request, false)
	if !ok {
		return
	}
	if waitTimeout > 0 {
		if !server.acquirePermit(server.longPollPermits) {
			server.capacityProblem(response, request)
			return
		}
		defer server.releasePermit(server.longPollPermits)
	}
	delivery, replayed, err := server.service.Receive(request.Context(), request.PathValue("queue"), model.ReceiveRequest{
		VisibilityTimeout: visibilityTimeout, WaitTimeout: waitTimeout, IdempotencyKey: key,
	})
	if err != nil {
		server.serviceProblem(response, request, err)
		return
	}
	result := ReceiveResponse{Messages: []Delivery{}, PolledAt: server.now().UTC(), Replayed: replayed}
	if delivery != nil {
		result.Messages = append(result.Messages, deliveryFromModel(*delivery))
	}
	server.writeJSON(response, request, http.StatusOK, result)
}

func (server *Server) ack(response http.ResponseWriter, request *http.Request) {
	var input ReceiptRequest
	if !server.decode(response, request, &input) {
		return
	}
	if input.ReceiptHandle == "" {
		server.validationProblem(response, request, "receipt_handle is required")
		return
	}
	key, ok := server.idempotencyKey(response, request, false)
	if !ok {
		return
	}
	replayed, err := server.service.Ack(request.Context(), request.PathValue("queue"), model.AckRequest{
		MessageID: request.PathValue("message"), Receipt: input.ReceiptHandle, IdempotencyKey: key,
	})
	if err != nil {
		server.serviceProblem(response, request, err)
		return
	}
	server.writeJSON(response, request, http.StatusOK, AckResponse{MessageID: request.PathValue("message"), State: StateAcked, Replayed: replayed})
}

func (server *Server) nack(response http.ResponseWriter, request *http.Request) {
	var input NackRequest
	if !server.decode(response, request, &input) {
		return
	}
	retryDelay, delayOK := milliseconds(input.RetryDelayMS)
	if input.ReceiptHandle == "" || !delayOK {
		server.validationProblem(response, request, "receipt_handle is required and retry_delay_ms must be in range")
		return
	}
	if len([]byte(input.Reason)) > 512 {
		server.validationProblem(response, request, "reason cannot exceed 512 bytes")
		return
	}
	key, ok := server.idempotencyKey(response, request, false)
	if !ok {
		return
	}
	message, replayed, err := server.service.Nack(request.Context(), request.PathValue("queue"), model.NackRequest{
		MessageID: request.PathValue("message"), Receipt: input.ReceiptHandle,
		Delay: retryDelay, Reason: input.Reason, IdempotencyKey: key,
	})
	if err != nil {
		server.serviceProblem(response, request, err)
		return
	}
	server.writeJSON(response, request, http.StatusOK, MutationResponse[Message]{Data: messageFromModel(message), Replayed: replayed})
}

func (server *Server) extend(response http.ResponseWriter, request *http.Request) {
	var input ExtendRequest
	if !server.decode(response, request, &input) {
		return
	}
	visibilityTimeout, timeoutOK := milliseconds(input.VisibilityTimeoutMS)
	if input.ReceiptHandle == "" || !timeoutOK || visibilityTimeout == 0 {
		server.validationProblem(response, request, "receipt_handle is required and visibility_timeout_ms must be positive and in range")
		return
	}
	key, ok := server.idempotencyKey(response, request, false)
	if !ok {
		return
	}
	delivery, replayed, err := server.service.Extend(request.Context(), request.PathValue("queue"), model.ExtendRequest{
		MessageID: request.PathValue("message"), Receipt: input.ReceiptHandle,
		VisibilityTimeout: visibilityTimeout, IdempotencyKey: key,
	})
	if err != nil {
		server.serviceProblem(response, request, err)
		return
	}
	server.writeJSON(response, request, http.StatusOK, MutationResponse[Delivery]{Data: deliveryFromModel(delivery), Replayed: replayed})
}

func (server *Server) listMessages(response http.ResponseWriter, request *http.Request) {
	filter, ok := server.listFilter(response, request, true)
	if !ok {
		return
	}
	page, err := server.service.ListMessages(request.Context(), request.PathValue("queue"), filter)
	if err != nil {
		server.serviceProblem(response, request, err)
		return
	}
	server.writeJSON(response, request, http.StatusOK, pageFromModel(page))
}

func (server *Server) listDeadLetters(response http.ResponseWriter, request *http.Request) {
	filter, ok := server.listFilter(response, request, false)
	if !ok {
		return
	}
	page, err := server.service.ListDeadLetters(request.Context(), request.PathValue("queue"), filter)
	if err != nil {
		server.serviceProblem(response, request, err)
		return
	}
	server.writeJSON(response, request, http.StatusOK, pageFromModel(page))
}

func (server *Server) redrive(response http.ResponseWriter, request *http.Request) {
	var input RedriveRequest
	if !server.decode(response, request, &input) {
		return
	}
	delay, ok := server.schedule(response, request, input.DelayMS, input.AvailableAt)
	if !ok {
		return
	}
	key, ok := server.idempotencyKey(response, request, true)
	if !ok {
		return
	}
	result, replayed, err := server.service.Redrive(request.Context(), request.PathValue("queue"), model.RedriveRequest{
		MessageID: request.PathValue("message"), Priority: input.Priority, Delay: delay,
		AvailableAt: input.AvailableAt, IdempotencyKey: key,
	})
	if err != nil {
		server.serviceProblem(response, request, err)
		return
	}
	status := http.StatusCreated
	if replayed {
		status = http.StatusOK
	}
	server.writeJSON(response, request, status, MutationResponse[RedriveResult]{Data: RedriveResult{
		Source: messageFromModel(result.Source), Child: messageFromModel(result.Child),
	}, Replayed: replayed})
}

func (server *Server) stats(response http.ResponseWriter, request *http.Request) {
	if !server.rejectQuery(response, request, nil) {
		return
	}
	stats, err := server.service.Stats(request.Context())
	if err != nil {
		server.serviceProblem(response, request, err)
		return
	}
	server.writeJSON(response, request, http.StatusOK, statsFromModel(stats))
}

func (server *Server) compact(response http.ResponseWriter, request *http.Request) {
	if !server.decodeEmpty(response, request) {
		return
	}
	if !server.compacting.CompareAndSwap(false, true) {
		server.writeProblem(response, request, http.StatusConflict, "compaction_in_progress", "Compaction is already running", "Wait for the active compaction to complete.")
		return
	}
	defer server.compacting.Store(false)
	if err := server.service.Compact(request.Context()); err != nil {
		server.serviceProblem(response, request, err)
		return
	}
	server.writeJSON(response, request, http.StatusOK, StatusResponse{Status: "compacted"})
}

func pageFromModel(page model.MessagePage) MessagePage {
	messages := make([]Message, len(page.Messages))
	for index := range page.Messages {
		messages[index] = messageFromModel(page.Messages[index])
	}
	return MessagePage{Messages: messages, NextCursor: page.NextCursor, SnapshotLSN: page.SnapshotLSN}
}

func (server *Server) listFilter(response http.ResponseWriter, request *http.Request, allowState bool) (model.ListFilter, bool) {
	allowed := map[string]bool{"limit": true, "cursor": true}
	if allowState {
		allowed["state"] = true
	}
	query, ok := server.parseQuery(response, request, allowed)
	if !ok {
		return model.ListFilter{}, false
	}
	filter := model.ListFilter{Limit: defaultListLimit, Cursor: query.Get("cursor")}
	if raw := query.Get("limit"); raw != "" {
		limit, err := strconv.Atoi(raw)
		if err != nil || limit < 1 || limit > maxListLimit {
			server.validationProblem(response, request, "limit must be an integer from 1 through 50")
			return model.ListFilter{}, false
		}
		filter.Limit = limit
	}
	if raw := query.Get("state"); raw != "" {
		filter.State = model.MessageState(raw)
		switch filter.State {
		case model.StateDelayed, model.StateReady, model.StateLeased, model.StateAcked, model.StateDead:
		default:
			server.validationProblem(response, request, "state is not recognized")
			return model.ListFilter{}, false
		}
	}
	return filter, true
}

func (server *Server) rejectQuery(response http.ResponseWriter, request *http.Request, allowed map[string]bool) bool {
	_, ok := server.parseQuery(response, request, allowed)
	return ok
}

func (server *Server) parseQuery(response http.ResponseWriter, request *http.Request, allowed map[string]bool) (url.Values, bool) {
	query, err := url.ParseQuery(request.URL.RawQuery)
	if err != nil {
		server.writeProblem(response, request, http.StatusBadRequest, "invalid_query", "Invalid query string", "Query parameters must use valid percent encoding and separators.")
		return nil, false
	}
	for key, values := range query {
		if !allowed[key] || len(values) != 1 || values[0] == "" {
			server.writeProblem(response, request, http.StatusBadRequest, "invalid_query", "Invalid query string", "Query parameters must be recognized, non-empty, and supplied once.")
			return nil, false
		}
	}
	return query, true
}

func (server *Server) decode(response http.ResponseWriter, request *http.Request, destination any) bool {
	if !server.rejectQuery(response, request, nil) {
		return false
	}
	contentTypes := request.Header.Values("Content-Type")
	if len(contentTypes) != 1 {
		server.writeProblem(response, request, http.StatusUnsupportedMediaType, "unsupported_media_type", "Unsupported media type", "Supply exactly one Content-Type header with value application/json.")
		return false
	}
	mediaType, _, err := mime.ParseMediaType(contentTypes[0])
	if err != nil || mediaType != "application/json" {
		server.writeProblem(response, request, http.StatusUnsupportedMediaType, "unsupported_media_type", "Unsupported media type", "Content-Type must be application/json.")
		return false
	}
	body := http.MaxBytesReader(response, request.Body, server.maxRequestBytes)
	defer body.Close()
	encoded, err := io.ReadAll(body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			server.writeProblem(response, request, http.StatusRequestEntityTooLarge, "request_too_large", "Request is too large", "The request body exceeds the configured limit.")
			return false
		}
		server.writeProblem(response, request, http.StatusBadRequest, "invalid_json", "Invalid JSON", "The request body could not be read.")
		return false
	}
	if len(bytes.TrimSpace(encoded)) == 0 {
		server.writeProblem(response, request, http.StatusBadRequest, "invalid_json", "Invalid JSON", "A JSON request body is required.")
		return false
	}
	if !utf8.Valid(encoded) {
		server.writeProblem(response, request, http.StatusBadRequest, "invalid_json", "Invalid JSON", "The request body must contain valid UTF-8.")
		return false
	}
	if err := validateJSONObject(encoded); err != nil {
		server.writeProblem(response, request, http.StatusBadRequest, "invalid_json", "Invalid JSON", err.Error())
		return false
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		server.writeProblem(response, request, http.StatusBadRequest, "invalid_json", "Invalid JSON", "The request body does not match the operation schema.")
		return false
	}
	return true
}

func (server *Server) decodeEmpty(response http.ResponseWriter, request *http.Request) bool {
	if !server.rejectQuery(response, request, nil) {
		return false
	}
	if request.Body == nil || request.ContentLength == 0 {
		return true
	}
	var body struct{}
	return server.decode(response, request, &body)
}

func validateJSONObject(encoded []byte) error {
	if err := validateSurrogateEscapes(encoded); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	first, err := decoder.Token()
	if err != nil {
		return errors.New("request body is not valid JSON")
	}
	if delimiter, ok := first.(json.Delim); !ok || delimiter != '{' {
		return errors.New("top-level JSON value must be an object")
	}
	if err := consumeObject(decoder); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return errors.New("request body must contain exactly one JSON object")
	}
	return nil
}

func validateSurrogateEscapes(encoded []byte) error {
	inString := false
	for index := 0; index < len(encoded); index++ {
		switch encoded[index] {
		case '"':
			inString = !inString
		case '\\':
			if !inString || index+1 >= len(encoded) {
				continue
			}
			index++
			if encoded[index] != 'u' {
				continue
			}
			value, ok := decodeHexQuad(encoded, index+1)
			if !ok {
				return errors.New("request body is not valid JSON")
			}
			index += 4
			switch {
			case value >= 0xd800 && value <= 0xdbff:
				if index+6 >= len(encoded) || encoded[index+1] != '\\' || encoded[index+2] != 'u' {
					return errors.New("JSON strings cannot contain unpaired Unicode surrogates")
				}
				low, valid := decodeHexQuad(encoded, index+3)
				if !valid || low < 0xdc00 || low > 0xdfff {
					return errors.New("JSON strings cannot contain unpaired Unicode surrogates")
				}
				index += 6
			case value >= 0xdc00 && value <= 0xdfff:
				return errors.New("JSON strings cannot contain unpaired Unicode surrogates")
			}
		}
	}
	return nil
}

func decodeHexQuad(encoded []byte, start int) (uint16, bool) {
	if start+4 > len(encoded) {
		return 0, false
	}
	var value uint16
	for _, character := range encoded[start : start+4] {
		value <<= 4
		switch {
		case character >= '0' && character <= '9':
			value |= uint16(character - '0')
		case character >= 'a' && character <= 'f':
			value |= uint16(character-'a') + 10
		case character >= 'A' && character <= 'F':
			value |= uint16(character-'A') + 10
		default:
			return 0, false
		}
	}
	return value, true
}

func consumeObject(decoder *json.Decoder) error {
	keys := make(map[string]struct{})
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return errors.New("request body is not valid JSON")
		}
		key, ok := token.(string)
		if !ok {
			return errors.New("request body is not valid JSON")
		}
		if _, duplicate := keys[key]; duplicate {
			return fmt.Errorf("JSON field %q appears more than once", key)
		}
		keys[key] = struct{}{}
		if err := consumeValue(decoder); err != nil {
			return err
		}
	}
	if _, err := decoder.Token(); err != nil {
		return errors.New("request body is not valid JSON")
	}
	return nil
}

func consumeValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return errors.New("request body is not valid JSON")
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		return consumeObject(decoder)
	case '[':
		for decoder.More() {
			if err := consumeValue(decoder); err != nil {
				return err
			}
		}
		if _, err := decoder.Token(); err != nil {
			return errors.New("request body is not valid JSON")
		}
		return nil
	default:
		return errors.New("request body is not valid JSON")
	}
}

func (server *Server) idempotencyKey(response http.ResponseWriter, request *http.Request, required bool) (string, bool) {
	values := request.Header.Values("Idempotency-Key")
	if len(values) == 0 {
		if required {
			server.writeProblem(response, request, http.StatusBadRequest, "missing_idempotency_key", "Idempotency key is required", "Supply one Idempotency-Key header.")
			return "", false
		}
		return "", true
	}
	if len(values) != 1 {
		server.writeProblem(response, request, http.StatusBadRequest, "invalid_idempotency_key", "Invalid idempotency key", "Supply exactly one Idempotency-Key header.")
		return "", false
	}
	key := values[0]
	if len(key) < 1 || len(key) > 256 || strings.TrimSpace(key) != key {
		server.writeProblem(response, request, http.StatusBadRequest, "invalid_idempotency_key", "Invalid idempotency key", "The idempotency key must contain 1 through 256 bytes without surrounding whitespace.")
		return "", false
	}
	for _, character := range key {
		if character < 0x21 || character > 0x7e {
			server.writeProblem(response, request, http.StatusBadRequest, "invalid_idempotency_key", "Invalid idempotency key", "The idempotency key must contain visible ASCII characters.")
			return "", false
		}
	}
	return key, true
}

func (server *Server) schedule(response http.ResponseWriter, request *http.Request, delayMS *int64, availableAt *time.Time) (time.Duration, bool) {
	if delayMS != nil && availableAt != nil {
		server.validationProblem(response, request, "delay_ms and available_at are mutually exclusive")
		return 0, false
	}
	if delayMS == nil {
		return 0, true
	}
	delay, ok := milliseconds(*delayMS)
	if !ok {
		server.validationProblem(response, request, "delay_ms is outside the allowed range")
		return 0, false
	}
	return delay, true
}

func durationPointer(value time.Duration, present bool) *time.Duration {
	if !present {
		return nil
	}
	return &value
}

func milliseconds(value int64) (time.Duration, bool) {
	const maxMilliseconds = int64(^uint64(0)>>1) / int64(time.Millisecond)
	if value < 0 || value > maxMilliseconds {
		return 0, false
	}
	return time.Duration(value) * time.Millisecond, true
}

func (server *Server) serviceProblem(response http.ResponseWriter, request *http.Request, err error) {
	status := http.StatusInternalServerError
	code := "internal_error"
	title := "Internal server error"
	detail := "The operation could not be completed."
	var serviceError *engine.Error
	if errors.As(err, &serviceError) {
		code = string(serviceError.Code)
		detail = serviceError.Message
		switch serviceError.Code {
		case engine.CodeInvalid:
			status, title = http.StatusUnprocessableEntity, "Request validation failed"
		case engine.CodeNotFound:
			status, title = http.StatusNotFound, "Resource not found"
		case engine.CodeConflict, engine.CodeStaleReceipt, engine.CodeIdempotencyConflict:
			status, title = http.StatusConflict, "Operation conflicts with current state"
		case engine.CodeCapacityExceeded:
			status, title = http.StatusTooManyRequests, "Service capacity exceeded"
			response.Header().Set("Retry-After", "1")
		case engine.CodeStorageUnavailable, engine.CodeClosed:
			status, title = http.StatusServiceUnavailable, "Service unavailable"
		}
	} else if errors.Is(err, context.DeadlineExceeded) {
		status, code, title, detail = http.StatusGatewayTimeout, "deadline_exceeded", "Operation timed out", "The operation exceeded its deadline."
	} else if errors.Is(err, context.Canceled) {
		status, code, title, detail = 499, "request_canceled", "Request canceled", "The request was canceled before completion."
	}
	if status >= 500 {
		server.logger.Error("API operation failed", "request_id", requestIDFrom(request), "method", request.Method, "code", code)
	}
	server.writeProblem(response, request, status, code, title, detail)
}

func (server *Server) validationProblem(response http.ResponseWriter, request *http.Request, detail string) {
	server.writeProblem(response, request, http.StatusUnprocessableEntity, "invalid_request", "Request validation failed", detail)
}

func (server *Server) writeProblem(response http.ResponseWriter, request *http.Request, status int, code, title, detail string) {
	server.writeJSON(response, request, status, Problem{
		Type: problemBase + code, Title: title, Status: status, Code: code, Detail: detail,
		RequestID: requestIDFrom(request),
	})
}

func (server *Server) writeJSON(response http.ResponseWriter, request *http.Request, status int, value any) {
	response.Header().Set("Content-Type", "application/json")
	if _, ok := value.(Problem); ok {
		response.Header().Set("Content-Type", "application/problem+json")
	}
	response.WriteHeader(status)
	if err := json.NewEncoder(response).Encode(value); err != nil {
		server.logger.Error("write API response", "request_id", requestIDFrom(request), "method", request.Method, "route", request.Pattern, "status", status, "error_type", fmt.Sprintf("%T", err))
	}
}

func requestIDFrom(request *http.Request) string {
	requestID, _ := request.Context().Value(requestIDKey{}).(string)
	return requestID
}

func validRequestID(value string) bool {
	if len(value) < 1 || len(value) > 128 {
		return false
	}
	for _, character := range value {
		if character < 0x21 || character > 0x7e {
			return false
		}
	}
	return true
}

func randomRequestID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 16)
	}
	return "req_" + hex.EncodeToString(value[:])
}
