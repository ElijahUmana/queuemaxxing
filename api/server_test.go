package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ElijahUmana/queuemaxxing/internal/engine"
	"github.com/ElijahUmana/queuemaxxing/internal/model"
)

type fakeService struct {
	createQueue func(context.Context, model.QueueConfig, string) (model.QueueInfo, bool, error)
	enqueue     func(context.Context, string, model.EnqueueRequest) (model.Message, bool, error)
	receive     func(context.Context, string, model.ReceiveRequest) (*model.Delivery, bool, error)
	list        func(context.Context, string, model.ListFilter) (model.MessagePage, error)
	ready       error
}

func (fake *fakeService) CreateQueue(ctx context.Context, config model.QueueConfig, key string) (model.QueueInfo, bool, error) {
	if fake.createQueue != nil {
		return fake.createQueue(ctx, config, key)
	}
	return model.QueueInfo{}, false, nil
}
func (*fakeService) ListQueues(context.Context) ([]model.QueueInfo, error) { return nil, nil }
func (*fakeService) GetQueue(context.Context, string) (model.QueueInfo, error) {
	return model.QueueInfo{}, nil
}
func (fake *fakeService) Enqueue(ctx context.Context, queue string, request model.EnqueueRequest) (model.Message, bool, error) {
	if fake.enqueue != nil {
		return fake.enqueue(ctx, queue, request)
	}
	return model.Message{}, false, nil
}
func (fake *fakeService) Receive(ctx context.Context, queue string, request model.ReceiveRequest) (*model.Delivery, bool, error) {
	if fake.receive != nil {
		return fake.receive(ctx, queue, request)
	}
	return nil, false, nil
}
func (*fakeService) Ack(context.Context, string, model.AckRequest) (bool, error) {
	return false, nil
}
func (*fakeService) Nack(context.Context, string, model.NackRequest) (model.Message, bool, error) {
	return model.Message{}, false, nil
}
func (*fakeService) Extend(context.Context, string, model.ExtendRequest) (model.Delivery, bool, error) {
	return model.Delivery{}, false, nil
}
func (fake *fakeService) ListMessages(ctx context.Context, queue string, filter model.ListFilter) (model.MessagePage, error) {
	if fake.list != nil {
		return fake.list(ctx, queue, filter)
	}
	return model.MessagePage{}, nil
}
func (*fakeService) ListDeadLetters(context.Context, string, model.ListFilter) (model.MessagePage, error) {
	return model.MessagePage{}, nil
}
func (*fakeService) Redrive(context.Context, string, model.RedriveRequest) (model.RedriveResult, bool, error) {
	return model.RedriveResult{}, false, nil
}
func (*fakeService) Stats(context.Context) (model.ServiceStats, error) {
	return model.ServiceStats{}, nil
}
func (*fakeService) Compact(context.Context) error { return nil }
func (fake *fakeService) Ready() error             { return fake.ready }
func (*fakeService) Close(context.Context) error   { return nil }

func newTestServer(t *testing.T, service engine.Service, options Options) *Server {
	t.Helper()
	options.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	options.RequestID = func() string { return "req_test" }
	server, err := New(service, options)
	if err != nil {
		t.Fatal(err)
	}
	return server
}

func perform(server http.Handler, method, target, body, contentType string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, target, strings.NewReader(body))
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	return response
}

func decodeProblem(t *testing.T, response *httptest.ResponseRecorder) Problem {
	t.Helper()
	var problem Problem
	if err := json.Unmarshal(response.Body.Bytes(), &problem); err != nil {
		t.Fatal(err)
	}
	return problem
}

func TestCreateQueueConvertsMillisecondsAndIdempotency(t *testing.T) {
	createdAt := time.Date(2026, 8, 26, 20, 0, 0, 0, time.UTC)
	service := &fakeService{createQueue: func(_ context.Context, config model.QueueConfig, key string) (model.QueueInfo, bool, error) {
		if config.DefaultDelay != 1500*time.Millisecond || config.DefaultVisibilityTimeout != 30*time.Second {
			t.Fatalf("unexpected durations: %+v", config)
		}
		if key != "create-demo" {
			t.Fatalf("idempotency key = %q", key)
		}
		config.CreatedAt = createdAt
		return model.QueueInfo{Config: config}, true, nil
	}}
	server := newTestServer(t, service, Options{})
	request := httptest.NewRequest(http.MethodPost, "/v1/queues", strings.NewReader(`{"name":"demo","ordering":"fifo","priority_enabled":true,"default_delay_ms":1500,"default_visibility_timeout_ms":30000,"max_deliveries":3}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", "create-demo")
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	var result MutationResponse[Queue]
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if !result.Replayed || result.Data.Config.DefaultDelayMS != 1500 {
		t.Fatalf("result = %+v", result)
	}
}

func TestStrictJSONRejectsUnknownDuplicateTrailingAndNonObject(t *testing.T) {
	server := newTestServer(t, &fakeService{}, Options{})
	tests := []string{
		`{"name":"q","ordering":"fifo","priority_enabled":false,"default_delay_ms":0,"default_visibility_timeout_ms":1,"max_deliveries":1,"unknown":true}`,
		`{"name":"q","name":"other","ordering":"fifo","priority_enabled":false,"default_delay_ms":0,"default_visibility_timeout_ms":1,"max_deliveries":1}`,
		`{"name":"q"} {"name":"other"}`,
		`[]`,
	}
	for _, body := range tests {
		response := perform(server, http.MethodPost, "/v1/queues", body, "application/json")
		if response.Code != http.StatusBadRequest {
			t.Fatalf("body %s: status = %d, response = %s", body, response.Code, response.Body.String())
		}
		if problem := decodeProblem(t, response); problem.Code != "invalid_json" || problem.RequestID != "req_test" {
			t.Fatalf("problem = %+v", problem)
		}
	}
}

func TestRequestContentTypeAndSizeAreEnforced(t *testing.T) {
	server := newTestServer(t, &fakeService{}, Options{MaxRequestBytes: 32})
	response := perform(server, http.MethodPost, "/v1/queues", `{}`, "text/plain")
	if response.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("content type status = %d", response.Code)
	}
	response = perform(server, http.MethodPost, "/v1/queues", strings.Repeat("x", 33), "application/json")
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("size status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestDurationOverflowIsRejectedBeforeServiceCall(t *testing.T) {
	called := false
	service := &fakeService{createQueue: func(context.Context, model.QueueConfig, string) (model.QueueInfo, bool, error) {
		called = true
		return model.QueueInfo{}, false, nil
	}}
	server := newTestServer(t, service, Options{})
	response := perform(server, http.MethodPost, "/v1/queues", `{"name":"q","ordering":"fifo","priority_enabled":false,"default_delay_ms":9223372036854775807,"default_visibility_timeout_ms":1,"max_deliveries":1}`, "application/json")
	if response.Code != http.StatusUnprocessableEntity || called {
		t.Fatalf("status = %d, called = %t", response.Code, called)
	}
}

func TestEnqueuePreservesFieldPresence(t *testing.T) {
	service := &fakeService{enqueue: func(_ context.Context, _ string, request model.EnqueueRequest) (model.Message, bool, error) {
		if !request.PrioritySet || request.Priority != 0 || !request.DelaySet || request.Delay != 0 {
			t.Fatalf("request did not preserve explicit zero values: %+v", request)
		}
		return model.Message{ID: "m", Payload: json.RawMessage(`{}`)}, false, nil
	}}
	server := newTestServer(t, service, Options{})
	response := perform(server, http.MethodPost, "/v1/queues/q/messages", `{"payload":{},"priority":0,"delay_ms":0}`, "application/json")
	if response.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestEnqueueRejectsConflictingScheduleFields(t *testing.T) {
	server := newTestServer(t, &fakeService{}, Options{})
	response := perform(server, http.MethodPost, "/v1/queues/q/messages", `{"payload":{},"delay_ms":0,"available_at":"2026-08-26T20:00:00Z"}`, "application/json")
	if response.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestReceiveTimeoutReturnsEmptyArray(t *testing.T) {
	now := time.Date(2026, 8, 26, 20, 0, 0, 0, time.UTC)
	service := &fakeService{receive: func(_ context.Context, _ string, request model.ReceiveRequest) (*model.Delivery, bool, error) {
		if request.WaitTimeout != 20*time.Second || request.VisibilityTimeout != 30*time.Second {
			t.Fatalf("request = %+v", request)
		}
		return nil, false, nil
	}}
	server := newTestServer(t, service, Options{Now: func() time.Time { return now }})
	response := perform(server, http.MethodPost, "/v1/queues/q/messages:receive", `{"wait_timeout_ms":20000,"visibility_timeout_ms":30000}`, "application/json")
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d", response.Code)
	}
	if !strings.Contains(response.Body.String(), `"messages":[]`) {
		t.Fatalf("body = %s", response.Body.String())
	}
}

func TestListQueryIsStrictAndTranslated(t *testing.T) {
	service := &fakeService{list: func(_ context.Context, queue string, filter model.ListFilter) (model.MessagePage, error) {
		if queue != "q" || filter.Limit != 25 || filter.Cursor != "cursor" || filter.State != model.StateReady {
			t.Fatalf("queue/filter = %q/%+v", queue, filter)
		}
		return model.MessagePage{Messages: []model.Message{}, SnapshotLSN: 7}, nil
	}}
	server := newTestServer(t, service, Options{})
	response := perform(server, http.MethodGet, "/v1/queues/q/messages?limit=25&cursor=cursor&state=ready", "", "")
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	response = perform(server, http.MethodGet, "/v1/queues/q/messages?limit=1&limit=2", "", "")
	if response.Code != http.StatusBadRequest {
		t.Fatalf("duplicate query status = %d", response.Code)
	}
}

func TestEngineErrorsHaveStableStatuses(t *testing.T) {
	codes := map[engine.ErrorCode]int{
		engine.CodeInvalid: http.StatusUnprocessableEntity, engine.CodeNotFound: http.StatusNotFound,
		engine.CodeConflict: http.StatusConflict, engine.CodeStaleReceipt: http.StatusConflict,
		engine.CodeIdempotencyConflict: http.StatusConflict, engine.CodeCapacityExceeded: http.StatusTooManyRequests,
		engine.CodeStorageUnavailable: http.StatusServiceUnavailable, engine.CodeClosed: http.StatusServiceUnavailable,
	}
	for code, status := range codes {
		service := &fakeService{enqueue: func(context.Context, string, model.EnqueueRequest) (model.Message, bool, error) {
			return model.Message{}, false, &engine.Error{Code: code, Message: "failure"}
		}}
		server := newTestServer(t, service, Options{})
		response := perform(server, http.MethodPost, "/v1/queues/q/messages", `{"payload":{"x":1}}`, "application/json")
		if response.Code != status {
			t.Fatalf("code %s: status = %d, want %d", code, response.Code, status)
		}
		if problem := decodeProblem(t, response); problem.Code != string(code) {
			t.Fatalf("code %s: problem = %+v", code, problem)
		}
	}
}

func TestReadinessAndDraining(t *testing.T) {
	server := newTestServer(t, &fakeService{ready: errors.New("disk unavailable")}, Options{})
	response := perform(server, http.MethodGet, "/health/ready", "", "")
	if response.Code != http.StatusServiceUnavailable || strings.Contains(response.Body.String(), "disk unavailable") {
		t.Fatalf("readiness response = %d %s", response.Code, response.Body.String())
	}
	server.SetDraining(true)
	response = perform(server, http.MethodGet, "/v1/stats", "", "")
	if response.Code != http.StatusServiceUnavailable || decodeProblem(t, response).Code != "draining" {
		t.Fatalf("draining response = %d %s", response.Code, response.Body.String())
	}
	response = perform(server, http.MethodGet, "/health/live", "", "")
	if response.Code != http.StatusOK {
		t.Fatalf("liveness status = %d", response.Code)
	}
}

func TestMethodMismatchReturnsProblemJSON(t *testing.T) {
	server := newTestServer(t, &fakeService{}, Options{})
	response := perform(server, http.MethodDelete, "/v1/queues", "", "")
	if response.Code != http.StatusMethodNotAllowed || response.Header().Get("Content-Type") != "application/problem+json" {
		t.Fatalf("response = %d %+v %s", response.Code, response.Header(), response.Body.String())
	}
	if problem := decodeProblem(t, response); problem.Code != "method_not_allowed" {
		t.Fatalf("problem = %+v", problem)
	}
}

func TestUnmatchedRouteReturnsProblemJSON(t *testing.T) {
	server := newTestServer(t, &fakeService{}, Options{})
	response := perform(server, http.MethodGet, "/v1/missing", "", "")
	if response.Code != http.StatusNotFound || response.Header().Get("Content-Type") != "application/problem+json" {
		t.Fatalf("response = %d %+v %s", response.Code, response.Header(), response.Body.String())
	}
}
