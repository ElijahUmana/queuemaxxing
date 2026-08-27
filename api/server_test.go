package api

import (
	"bytes"
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
	listQueues  func(context.Context) ([]model.QueueInfo, error)
	getQueue    func(context.Context, string) (model.QueueInfo, error)
	enqueue     func(context.Context, string, model.EnqueueRequest) (model.Message, bool, error)
	receive     func(context.Context, string, model.ReceiveRequest) (*model.Delivery, bool, error)
	ack         func(context.Context, string, model.AckRequest) (bool, error)
	nack        func(context.Context, string, model.NackRequest) (model.Message, bool, error)
	extend      func(context.Context, string, model.ExtendRequest) (model.Delivery, bool, error)
	list        func(context.Context, string, model.ListFilter) (model.MessagePage, error)
	dead        func(context.Context, string, model.ListFilter) (model.MessagePage, error)
	redrive     func(context.Context, string, model.RedriveRequest) (model.RedriveResult, bool, error)
	stats       func(context.Context) (model.ServiceStats, error)
	compact     func(context.Context) error
	ready       error
}

func (fake *fakeService) CreateQueue(ctx context.Context, config model.QueueConfig, key string) (model.QueueInfo, bool, error) {
	if fake.createQueue != nil {
		return fake.createQueue(ctx, config, key)
	}
	return model.QueueInfo{}, false, nil
}
func (fake *fakeService) ListQueues(ctx context.Context) ([]model.QueueInfo, error) {
	if fake.listQueues != nil {
		return fake.listQueues(ctx)
	}
	return nil, nil
}
func (fake *fakeService) GetQueue(ctx context.Context, name string) (model.QueueInfo, error) {
	if fake.getQueue != nil {
		return fake.getQueue(ctx, name)
	}
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
func (fake *fakeService) Ack(ctx context.Context, queue string, request model.AckRequest) (bool, error) {
	if fake.ack != nil {
		return fake.ack(ctx, queue, request)
	}
	return false, nil
}
func (fake *fakeService) Nack(ctx context.Context, queue string, request model.NackRequest) (model.Message, bool, error) {
	if fake.nack != nil {
		return fake.nack(ctx, queue, request)
	}
	return model.Message{}, false, nil
}
func (fake *fakeService) Extend(ctx context.Context, queue string, request model.ExtendRequest) (model.Delivery, bool, error) {
	if fake.extend != nil {
		return fake.extend(ctx, queue, request)
	}
	return model.Delivery{}, false, nil
}
func (fake *fakeService) ListMessages(ctx context.Context, queue string, filter model.ListFilter) (model.MessagePage, error) {
	if fake.list != nil {
		return fake.list(ctx, queue, filter)
	}
	return model.MessagePage{}, nil
}
func (fake *fakeService) ListDeadLetters(ctx context.Context, queue string, filter model.ListFilter) (model.MessagePage, error) {
	if fake.dead != nil {
		return fake.dead(ctx, queue, filter)
	}
	return model.MessagePage{}, nil
}
func (fake *fakeService) Redrive(ctx context.Context, queue string, request model.RedriveRequest) (model.RedriveResult, bool, error) {
	if fake.redrive != nil {
		return fake.redrive(ctx, queue, request)
	}
	return model.RedriveResult{}, false, nil
}
func (fake *fakeService) Stats(ctx context.Context) (model.ServiceStats, error) {
	if fake.stats != nil {
		return fake.stats(ctx)
	}
	return model.ServiceStats{}, nil
}
func (fake *fakeService) Compact(ctx context.Context) error {
	if fake.compact != nil {
		return fake.compact(ctx)
	}
	return nil
}
func (fake *fakeService) Ready() error           { return fake.ready }
func (*fakeService) Close(context.Context) error { return nil }

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
	return performBytes(server, method, target, []byte(body), contentType)
}

func performBytes(server http.Handler, method, target string, body []byte, contentType string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, target, bytes.NewReader(body))
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	return response
}

func performRawQuery(server http.Handler, method, target, rawQuery string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, target, nil)
	request.URL.RawQuery = rawQuery
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

func TestStrictJSONRejectsMalformedUnicodeBeforeServiceCall(t *testing.T) {
	called := false
	service := &fakeService{enqueue: func(context.Context, string, model.EnqueueRequest) (model.Message, bool, error) {
		called = true
		return model.Message{}, false, nil
	}}
	server := newTestServer(t, service, Options{})
	tests := []struct {
		name string
		body []byte
	}{
		{name: "invalid UTF-8 string", body: []byte{'{', '"', 'p', 'a', 'y', 'l', 'o', 'a', 'd', '"', ':', '"', 0xff, '"', '}'}},
		{name: "invalid UTF-8 nested key", body: []byte{'{', '"', 'p', 'a', 'y', 'l', 'o', 'a', 'd', '"', ':', '{', '"', 0xff, '"', ':', '1', '}', '}'}},
		{name: "lone high surrogate", body: []byte(`{"payload":{"value":"\uD800"}}`)},
		{name: "lone low surrogate", body: []byte(`{"payload":{"value":"\uDC00"}}`)},
		{name: "reversed surrogate pair", body: []byte(`{"payload":{"value":"\uDC00\uD800"}}`)},
		{name: "high surrogate followed by scalar", body: []byte(`{"payload":{"value":"\uD800\u0041"}}`)},
		{name: "surrogate in nested key", body: []byte(`{"payload":{"\uD800":1}}`)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := performBytes(server, http.MethodPost, "/v1/queues/q/messages", test.body, "application/json")
			if response.Code != http.StatusBadRequest || decodeProblem(t, response).Code != "invalid_json" {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
		})
	}
	if called {
		t.Fatal("service called for malformed Unicode")
	}

	response := perform(server, http.MethodPost, "/v1/queues/q/messages", `{"payload":{"value":"\uD83D\uDE00","replacement":"�"}}`, "application/json")
	if response.Code != http.StatusCreated || !called {
		t.Fatalf("valid Unicode status = %d, called = %t, body = %s", response.Code, called, response.Body.String())
	}
}

func TestRequestRejectsDuplicateContentTypeHeaders(t *testing.T) {
	called := false
	service := &fakeService{createQueue: func(context.Context, model.QueueConfig, string) (model.QueueInfo, bool, error) {
		called = true
		return model.QueueInfo{}, false, nil
	}}
	server := newTestServer(t, service, Options{})
	for _, values := range [][]string{{"application/json", "application/json"}, {"application/json", "text/plain"}} {
		request := httptest.NewRequest(http.MethodPost, "/v1/queues", strings.NewReader(`{}`))
		for _, value := range values {
			request.Header.Add("Content-Type", value)
		}
		response := httptest.NewRecorder()
		server.ServeHTTP(response, request)
		if response.Code != http.StatusUnsupportedMediaType || decodeProblem(t, response).Code != "unsupported_media_type" {
			t.Fatalf("values %q = %d %s", values, response.Code, response.Body.String())
		}
	}
	if called {
		t.Fatal("service called for duplicate Content-Type")
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

func TestCreateQueueAcceptsOmittedZeroDefaults(t *testing.T) {
	service := &fakeService{createQueue: func(_ context.Context, config model.QueueConfig, _ string) (model.QueueInfo, bool, error) {
		if config.PriorityEnabled || config.DefaultDelay != 0 {
			t.Fatalf("config = %+v", config)
		}
		return model.QueueInfo{Config: config}, false, nil
	}}
	server := newTestServer(t, service, Options{})
	response := perform(server, http.MethodPost, "/v1/queues", `{"name":"defaults","ordering":"fifo","default_visibility_timeout_ms":1,"max_deliveries":1}`, "application/json")
	if response.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestNackAcceptsOmittedRetryDelay(t *testing.T) {
	service := &fakeService{nack: func(_ context.Context, _ string, request model.NackRequest) (model.Message, bool, error) {
		if request.Delay != 0 {
			t.Fatalf("delay = %s", request.Delay)
		}
		return model.Message{ID: request.MessageID, Queue: "q", Payload: json.RawMessage(`{}`), State: model.StateReady}, false, nil
	}}
	server := newTestServer(t, service, Options{})
	response := perform(server, http.MethodPost, "/v1/queues/q/messages/m/nack", `{"receipt_handle":"r"}`, "application/json")
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestEnqueuePreservesFieldPresence(t *testing.T) {
	service := &fakeService{enqueue: func(_ context.Context, _ string, request model.EnqueueRequest) (model.Message, bool, error) {
		if request.Priority == nil || *request.Priority != 0 || request.Delay == nil || *request.Delay != 0 {
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
		if request.WaitTimeout != 20*time.Second || request.VisibilityTimeout != 30*time.Second || request.IdempotencyKey != "receive-key" {
			t.Fatalf("request = %+v", request)
		}
		return nil, true, nil
	}}
	server := newTestServer(t, service, Options{Now: func() time.Time { return now }})
	request := httptest.NewRequest(http.MethodPost, "/v1/queues/q/messages:receive", strings.NewReader(`{"wait_timeout_ms":20000,"visibility_timeout_ms":30000}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", "receive-key")
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d", response.Code)
	}
	if !strings.Contains(response.Body.String(), `"messages":[]`) || !strings.Contains(response.Body.String(), `"replayed":true`) {
		t.Fatalf("body = %s", response.Body.String())
	}
}

func TestNackRejectsOversizedReason(t *testing.T) {
	server := newTestServer(t, &fakeService{}, Options{})
	body := `{"receipt_handle":"receipt","retry_delay_ms":0,"reason":"` + strings.Repeat("x", 513) + `"}`
	response := perform(server, http.MethodPost, "/v1/queues/q/messages/m/nack", body, "application/json")
	if response.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestMalformedQueryRejectedBeforeServiceCall(t *testing.T) {
	called := false
	service := &fakeService{list: func(context.Context, string, model.ListFilter) (model.MessagePage, error) {
		called = true
		return model.MessagePage{}, nil
	}}
	server := newTestServer(t, service, Options{})
	for _, rawQuery := range []string{"%", "%ZZ", "limit=1;state=ready", "limit=1&limit=2", "limit=", "cursor=", "state="} {
		response := performRawQuery(server, http.MethodGet, "/v1/queues/q/messages", rawQuery)
		if response.Code != http.StatusBadRequest || response.Header().Get("Content-Type") != "application/problem+json" || decodeProblem(t, response).Code != "invalid_query" {
			t.Fatalf("query %q = %d %s", rawQuery, response.Code, response.Body.String())
		}
	}
	if called {
		t.Fatal("service called for malformed query")
	}
}

func TestOpaqueCursorPercentEncodingIsPreserved(t *testing.T) {
	service := &fakeService{list: func(_ context.Context, _ string, filter model.ListFilter) (model.MessagePage, error) {
		if filter.Cursor != "v2.scope/value+suffix=" {
			t.Fatalf("cursor = %q", filter.Cursor)
		}
		return model.MessagePage{Messages: []model.Message{}, SnapshotLSN: 1}, nil
	}}
	server := newTestServer(t, service, Options{})
	response := performRawQuery(server, http.MethodGet, "/v1/queues/q/messages", "cursor=v2.scope%2Fvalue%2Bsuffix%3D")
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
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
	response = perform(server, http.MethodGet, "/v1/queues/q/messages?limit=51", "", "")
	if response.Code != http.StatusUnprocessableEntity {
		t.Fatalf("oversized page status = %d", response.Code)
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
	server = newTestServer(t, &fakeService{}, Options{})
	response = perform(server, http.MethodGet, "/health/ready", "", "")
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "ready") {
		t.Fatalf("ready response = %d %s", response.Code, response.Body.String())
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

func TestRemainingHandlersTranslateSuccess(t *testing.T) {
	now := time.Now().UTC()
	message := model.Message{ID: "m", Queue: "q", Payload: json.RawMessage(`{}`), State: model.StateLeased, LeaseUntil: &now, LastLSN: 9}
	service := &fakeService{
		listQueues: func(context.Context) ([]model.QueueInfo, error) {
			return []model.QueueInfo{{Config: model.QueueConfig{Name: "q", Ordering: model.FIFO, CreatedAt: now}}}, nil
		},
		getQueue: func(_ context.Context, name string) (model.QueueInfo, error) {
			if name != "q" {
				t.Fatalf("name = %q", name)
			}
			return model.QueueInfo{Config: model.QueueConfig{Name: name, Ordering: model.FIFO, CreatedAt: now}}, nil
		},
		ack: func(_ context.Context, queue string, request model.AckRequest) (bool, error) {
			if queue != "q" || request.MessageID != "m" || request.Receipt != "r" || request.IdempotencyKey != "ack-key" {
				t.Fatalf("ack = %q %+v", queue, request)
			}
			return true, nil
		},
		nack: func(_ context.Context, queue string, request model.NackRequest) (model.Message, bool, error) {
			if queue != "q" || request.Delay != time.Second || request.Reason != "retry" {
				t.Fatalf("nack = %q %+v", queue, request)
			}
			copy := message
			copy.State = model.StateDelayed
			return copy, true, nil
		},
		extend: func(_ context.Context, queue string, request model.ExtendRequest) (model.Delivery, bool, error) {
			if queue != "q" || request.VisibilityTimeout != 2*time.Second {
				t.Fatalf("extend = %q %+v", queue, request)
			}
			return model.Delivery{Message: message, Receipt: "r", LeaseUntil: now, DeliveryCount: 1}, true, nil
		},
		dead: func(_ context.Context, queue string, filter model.ListFilter) (model.MessagePage, error) {
			if queue != "q" || filter.Limit != 50 {
				t.Fatalf("dead = %q %+v", queue, filter)
			}
			copy := message
			copy.State = model.StateDead
			return model.MessagePage{Messages: []model.Message{copy}, SnapshotLSN: 9}, nil
		},
		redrive: func(_ context.Context, queue string, request model.RedriveRequest) (model.RedriveResult, bool, error) {
			if queue != "q" || request.MessageID != "m" || request.IdempotencyKey != "redrive-key" {
				t.Fatalf("redrive = %q %+v", queue, request)
			}
			child := message
			child.ID = "child"
			child.ReplayOf = "m"
			return model.RedriveResult{Source: message, Child: child}, true, nil
		},
		stats: func(context.Context) (model.ServiceStats, error) {
			return model.ServiceStats{Queues: 1, DurableLSN: 9, Messages: model.QueueCounts{InFlight: 1, Total: 1}}, nil
		},
		compact: func(context.Context) error { return nil },
	}
	server := newTestServer(t, service, Options{})
	tests := []struct {
		method, target, body, key string
		status                    int
		contains                  string
	}{
		{http.MethodGet, "/v1/queues", "", "", 200, `"name":"q"`},
		{http.MethodGet, "/v1/queues/q", "", "", 200, `"name":"q"`},
		{http.MethodPost, "/v1/queues/q/messages/m/ack", `{"receipt_handle":"r"}`, "ack-key", 200, `"replayed":true`},
		{http.MethodPost, "/v1/queues/q/messages/m/nack", `{"receipt_handle":"r","retry_delay_ms":1000,"reason":"retry"}`, "nack-key", 200, `"state":"delayed"`},
		{http.MethodPost, "/v1/queues/q/messages/m/extend", `{"receipt_handle":"r","visibility_timeout_ms":2000}`, "extend-key", 200, `"receipt_handle":"r"`},
		{http.MethodGet, "/v1/queues/q/dead-letters", "", "", 200, `"state":"dead"`},
		{http.MethodPost, "/v1/queues/q/dead-letters/m/redrive", `{}`, "redrive-key", 200, `"replay_of":"m"`},
		{http.MethodGet, "/v1/stats", "", "", 200, `"durable_lsn":"9"`},
		{http.MethodPost, "/v1/admin/compact", `{}`, "", 200, `"compacted"`},
	}
	for _, test := range tests {
		request := httptest.NewRequest(test.method, test.target, strings.NewReader(test.body))
		if test.body != "" {
			request.Header.Set("Content-Type", "application/json")
		}
		if test.key != "" {
			request.Header.Set("Idempotency-Key", test.key)
		}
		response := httptest.NewRecorder()
		server.ServeHTTP(response, request)
		if response.Code != test.status || !strings.Contains(response.Body.String(), test.contains) {
			t.Fatalf("%s %s = %d %s", test.method, test.target, response.Code, response.Body.String())
		}
		if strings.Contains(response.Body.String(), `"last_lsn"`) {
			t.Fatalf("%s %s exposed per-message last_lsn: %s", test.method, test.target, response.Body.String())
		}
	}
}

func TestHandlerValidationAndInternalErrors(t *testing.T) {
	service := &fakeService{
		listQueues: func(context.Context) ([]model.QueueInfo, error) { return nil, errors.New("failed") },
		getQueue:   func(context.Context, string) (model.QueueInfo, error) { return model.QueueInfo{}, errors.New("failed") },
		ack:        func(context.Context, string, model.AckRequest) (bool, error) { return false, errors.New("failed") },
		nack: func(context.Context, string, model.NackRequest) (model.Message, bool, error) {
			return model.Message{}, false, errors.New("failed")
		},
		extend: func(context.Context, string, model.ExtendRequest) (model.Delivery, bool, error) {
			return model.Delivery{}, false, errors.New("failed")
		},
		dead: func(context.Context, string, model.ListFilter) (model.MessagePage, error) {
			return model.MessagePage{}, errors.New("failed")
		},
		redrive: func(context.Context, string, model.RedriveRequest) (model.RedriveResult, bool, error) {
			return model.RedriveResult{}, false, errors.New("failed")
		},
		stats:   func(context.Context) (model.ServiceStats, error) { return model.ServiceStats{}, errors.New("failed") },
		compact: func(context.Context) error { return errors.New("failed") },
	}
	server := newTestServer(t, service, Options{})
	for _, test := range []struct {
		method, target, body, contentType string
		status                            int
	}{
		{http.MethodGet, "/v1/queues?unknown=1", "", "", 400},
		{http.MethodGet, "/v1/queues", "", "", 500},
		{http.MethodGet, "/v1/queues/q", "", "", 500},
		{http.MethodPost, "/v1/queues/q/messages/m/ack", `{}`, "application/json", 422},
		{http.MethodPost, "/v1/queues/q/messages/m/ack", `{"receipt_handle":"r"}`, "application/json", 500},
		{http.MethodPost, "/v1/queues/q/messages/m/nack", `{"receipt_handle":"r","retry_delay_ms":-1}`, "application/json", 422},
		{http.MethodPost, "/v1/queues/q/messages/m/nack", `{"receipt_handle":"r","retry_delay_ms":0}`, "application/json", 500},
		{http.MethodPost, "/v1/queues/q/messages/m/extend", `{"receipt_handle":"r","visibility_timeout_ms":0}`, "application/json", 422},
		{http.MethodPost, "/v1/queues/q/messages/m/extend", `{"receipt_handle":"r","visibility_timeout_ms":1}`, "application/json", 500},
		{http.MethodGet, "/v1/queues/q/dead-letters", "", "", 500},
		{http.MethodPost, "/v1/queues/q/dead-letters/m/redrive", `{}`, "application/json", 400},
		{http.MethodGet, "/v1/stats", "", "", 500},
		{http.MethodPost, "/v1/admin/compact?bad=1", `{}`, "application/json", 400},
		{http.MethodPost, "/v1/admin/compact", `{}`, "application/json", 500},
	} {
		response := perform(server, test.method, test.target, test.body, test.contentType)
		if response.Code != test.status {
			t.Fatalf("%s %s = %d %s, want %d", test.method, test.target, response.Code, response.Body.String(), test.status)
		}
	}
}

func TestServerConstructionParserAndIdempotencyEdges(t *testing.T) {
	if _, err := New(nil, Options{}); err == nil {
		t.Fatal("nil service accepted")
	}
	if _, err := New(&fakeService{}, Options{MaxRequestBytes: -1}); err == nil {
		t.Fatal("negative request limit accepted")
	}
	if _, err := New(&fakeService{}, Options{RequestTimeout: -1}); err == nil {
		t.Fatal("negative timeout accepted")
	}
	if _, err := New(&fakeService{}, Options{MaxConcurrentRequests: -1}); err == nil {
		t.Fatal("negative concurrent request limit accepted")
	}
	if _, err := New(&fakeService{}, Options{MaxLongPolls: -1}); err == nil {
		t.Fatal("negative long-poll limit accepted")
	}

	server := newTestServer(t, &fakeService{}, Options{})
	for _, body := range []string{
		`{"payload":{"nested":{"a":1,"a":2}}}`,
		`{"payload":[{"a":1,"a":2}]}`,
		`{"payload":`,
		`{"payload":{}} trailing`,
		`{"payload":null}`,
	} {
		response := perform(server, http.MethodPost, "/v1/queues/q/messages", body, "application/json")
		if response.Code != http.StatusBadRequest && response.Code != http.StatusUnprocessableEntity {
			t.Fatalf("body %q = %d %s", body, response.Code, response.Body.String())
		}
	}
	for _, values := range [][]string{{"a", "b"}, {" spaced "}, {"line\nbreak"}, {strings.Repeat("x", 257)}} {
		request := httptest.NewRequest(http.MethodPost, "/v1/queues/q/messages", strings.NewReader(`{"payload":{}}`))
		request.Header.Set("Content-Type", "application/json")
		for _, value := range values {
			request.Header.Add("Idempotency-Key", value)
		}
		response := httptest.NewRecorder()
		server.ServeHTTP(response, request)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("keys %q = %d %s", values, response.Code, response.Body.String())
		}
	}
	request := httptest.NewRequest(http.MethodGet, "/health/live", nil)
	request.Header.Set("X-Request-ID", " invalid ")
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Header().Get("X-Request-ID") == " invalid " {
		t.Fatalf("request ID response = %+v", response.Header())
	}
}

func TestConcurrentRequestLimitRejectsAndRecovers(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	service := &fakeService{listQueues: func(context.Context) ([]model.QueueInfo, error) {
		started <- struct{}{}
		<-release
		return nil, nil
	}}
	server := newTestServer(t, service, Options{MaxConcurrentRequests: 1})
	firstDone := make(chan *httptest.ResponseRecorder, 1)
	go func() { firstDone <- perform(server, http.MethodGet, "/v1/queues", "", "") }()
	<-started

	rejected := perform(server, http.MethodGet, "/v1/stats", "", "")
	if rejected.Code != http.StatusTooManyRequests || rejected.Header().Get("Retry-After") != "1" || decodeProblem(t, rejected).Code != "capacity_exceeded" {
		t.Fatalf("rejected = %d %+v %s", rejected.Code, rejected.Header(), rejected.Body.String())
	}
	if live := perform(server, http.MethodGet, "/health/live", "", ""); live.Code != http.StatusOK {
		t.Fatalf("liveness = %d %s", live.Code, live.Body.String())
	}
	close(release)
	if first := <-firstDone; first.Code != http.StatusOK {
		t.Fatalf("first = %d %s", first.Code, first.Body.String())
	}
	if recovered := perform(server, http.MethodGet, "/v1/stats", "", ""); recovered.Code != http.StatusOK {
		t.Fatalf("recovered = %d %s", recovered.Code, recovered.Body.String())
	}
}

func TestLongPollLimitRejectsOnlyPositiveWaits(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	service := &fakeService{receive: func(_ context.Context, _ string, request model.ReceiveRequest) (*model.Delivery, bool, error) {
		if request.WaitTimeout > 0 {
			started <- struct{}{}
			<-release
		}
		return nil, false, nil
	}}
	server := newTestServer(t, service, Options{MaxConcurrentRequests: 3, MaxLongPolls: 1})
	firstDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		firstDone <- perform(server, http.MethodPost, "/v1/queues/q/messages:receive", `{"wait_timeout_ms":1000}`, "application/json")
	}()
	<-started

	rejected := perform(server, http.MethodPost, "/v1/queues/q/messages:receive", `{"wait_timeout_ms":1000}`, "application/json")
	if rejected.Code != http.StatusTooManyRequests || rejected.Header().Get("Retry-After") != "1" || decodeProblem(t, rejected).Code != "capacity_exceeded" {
		t.Fatalf("rejected = %d %+v %s", rejected.Code, rejected.Header(), rejected.Body.String())
	}
	immediate := perform(server, http.MethodPost, "/v1/queues/q/messages:receive", `{"wait_timeout_ms":0}`, "application/json")
	if immediate.Code != http.StatusOK {
		t.Fatalf("immediate = %d %s", immediate.Code, immediate.Body.String())
	}
	close(release)
	if first := <-firstDone; first.Code != http.StatusOK {
		t.Fatalf("first = %d %s", first.Code, first.Body.String())
	}
}

type failingResponseWriter struct {
	header http.Header
	error  error
}

func (writer *failingResponseWriter) Header() http.Header { return writer.header }
func (*failingResponseWriter) WriteHeader(int)            {}
func (writer *failingResponseWriter) Write([]byte) (int, error) {
	return 0, writer.error
}

func TestResponseWriteFailureLogsBoundedContext(t *testing.T) {
	const secret = "secret-receipt-idempotency-path"
	var output bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&output, nil))
	server, err := New(&fakeService{}, Options{Logger: logger, RequestID: func() string { return "req_write_failure" }})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "/v1/queues/"+secret, nil)
	writer := &failingResponseWriter{header: make(http.Header), error: errors.New(secret)}
	server.ServeHTTP(writer, request)
	logged := output.String()
	for _, expected := range []string{"req_write_failure", "method=GET", `route="GET /v1/queues/{queue}"`, "status=200", "error_type"} {
		if !strings.Contains(logged, expected) {
			t.Fatalf("log missing %q: %s", expected, logged)
		}
	}
	if strings.Contains(logged, secret) || strings.Contains(logged, "/v1/queues/"+secret) {
		t.Fatalf("log exposed secret: %s", logged)
	}
}

func TestServiceStatsRedactInternalStorageReason(t *testing.T) {
	const path = "/private/storage/secret-sentinel.wal"
	service := &fakeService{stats: func(context.Context) (model.ServiceStats, error) {
		return model.ServiceStats{ReadOnly: true, ReadOnlyReason: "rename " + path + ": permission denied"}, nil
	}}
	server := newTestServer(t, service, Options{})
	response := perform(server, http.MethodGet, "/v1/stats", "", "")
	if response.Code != http.StatusOK || strings.Contains(response.Body.String(), path) || strings.Contains(response.Body.String(), "permission denied") || !strings.Contains(response.Body.String(), `"read_only_reason":"storage operation failed"`) {
		t.Fatalf("response = %d %s", response.Code, response.Body.String())
	}
}

func TestServiceErrorLogRedactsPathsAndDetails(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&output, nil))
	service := &fakeService{getQueue: func(context.Context, string) (model.QueueInfo, error) {
		return model.QueueInfo{}, errors.New("storage /private/data/queue secret-receipt failed")
	}}
	server, err := New(service, Options{Logger: logger, RequestID: func() string { return "req_redacted" }})
	if err != nil {
		t.Fatal(err)
	}
	response := perform(server, http.MethodGet, "/v1/queues/private-queue-id", "", "")
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d %s", response.Code, response.Body.String())
	}
	logged := output.String()
	for _, secret := range []string{"/private/data/queue", "secret-receipt", "private-queue-id", "/v1/queues"} {
		if strings.Contains(logged, secret) {
			t.Fatalf("log exposed %q: %s", secret, logged)
		}
	}
	if !strings.Contains(logged, "req_redacted") || !strings.Contains(logged, "internal_error") {
		t.Fatalf("log missing bounded diagnostics: %s", logged)
	}
}

func TestCompactRequiresExplicitJSONObject(t *testing.T) {
	called := 0
	server := newTestServer(t, &fakeService{compact: func(context.Context) error {
		called++
		return nil
	}}, Options{})
	tests := []struct {
		name, body, contentType string
		status                  int
	}{
		{name: "no body or content type", status: http.StatusUnsupportedMediaType},
		{name: "empty JSON", contentType: "application/json", status: http.StatusBadRequest},
		{name: "plain text", body: `{}`, contentType: "text/plain", status: http.StatusUnsupportedMediaType},
		{name: "form", body: `x=y`, contentType: "application/x-www-form-urlencoded", status: http.StatusUnsupportedMediaType},
		{name: "valid object", body: `{}`, contentType: "application/json", status: http.StatusOK},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/v1/admin/compact", strings.NewReader(test.body))
			if test.contentType != "" {
				request.Header.Set("Content-Type", test.contentType)
			}
			request.Header.Set("Origin", "https://hostile.example")
			response := httptest.NewRecorder()
			server.ServeHTTP(response, request)
			if response.Code != test.status {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
			if response.Header().Get("Access-Control-Allow-Origin") != "" {
				t.Fatalf("permissive CORS header = %q", response.Header().Get("Access-Control-Allow-Origin"))
			}
		})
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/admin/compact", strings.NewReader(`{}`))
	request.Header.Add("Content-Type", "application/json")
	request.Header.Add("Content-Type", "application/json")
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("duplicate Content-Type status = %d", response.Code)
	}
	if called != 1 {
		t.Fatalf("compact calls = %d, want 1", called)
	}
}

func TestServiceProblemContextAndCompactionConflict(t *testing.T) {
	server := newTestServer(t, &fakeService{enqueue: func(context.Context, string, model.EnqueueRequest) (model.Message, bool, error) {
		return model.Message{}, false, context.DeadlineExceeded
	}}, Options{})
	response := perform(server, http.MethodPost, "/v1/queues/q/messages", `{"payload":{}}`, "application/json")
	if response.Code != http.StatusGatewayTimeout {
		t.Fatalf("deadline = %d %s", response.Code, response.Body.String())
	}

	server = newTestServer(t, &fakeService{enqueue: func(context.Context, string, model.EnqueueRequest) (model.Message, bool, error) {
		return model.Message{}, false, context.Canceled
	}}, Options{})
	response = perform(server, http.MethodPost, "/v1/queues/q/messages", `{"payload":{}}`, "application/json")
	if response.Code != 499 {
		t.Fatalf("canceled = %d %s", response.Code, response.Body.String())
	}

	started, release := make(chan struct{}), make(chan struct{})
	server = newTestServer(t, &fakeService{compact: func(context.Context) error { close(started); <-release; return nil }}, Options{})
	firstDone := make(chan *httptest.ResponseRecorder, 1)
	go func() { firstDone <- perform(server, http.MethodPost, "/v1/admin/compact", `{}`, "application/json") }()
	<-started
	second := perform(server, http.MethodPost, "/v1/admin/compact", `{}`, "application/json")
	if second.Code != http.StatusConflict {
		t.Fatalf("second compaction = %d %s", second.Code, second.Body.String())
	}
	close(release)
	if first := <-firstDone; first.Code != http.StatusOK {
		t.Fatalf("first compaction = %d", first.Code)
	}
}

func TestReceiveListAndScheduleValidationEdges(t *testing.T) {
	server := newTestServer(t, &fakeService{}, Options{})
	for _, test := range []struct {
		method, target, body, contentType string
		status                            int
	}{
		{http.MethodPost, "/v1/queues/q/messages:receive", `{"visibility_timeout_ms":-1}`, "application/json", 422},
		{http.MethodPost, "/v1/queues/q/messages:receive", `{"visibility_timeout_ms":9223372036854775807}`, "application/json", 422},
		{http.MethodPost, "/v1/queues/q/messages:receive", `{"wait_timeout_ms":-1}`, "application/json", 422},
		{http.MethodPost, "/v1/queues/q/messages:receive", `{"wait_timeout_ms":9223372036854775807}`, "application/json", 422},
		{http.MethodPost, "/v1/queues/q/messages", `{"payload":null}`, "application/json", 422},
		{http.MethodPost, "/v1/queues/q/messages", `{"payload":{},"delay_ms":-1}`, "application/json", 422},
		{http.MethodPost, "/v1/queues/q/dead-letters/m/redrive", `{"delay_ms":0,"available_at":"2026-08-26T20:00:00Z"}`, "application/json", 422},
		{http.MethodGet, "/v1/queues/q/messages?state=unknown", "", "", 422},
		{http.MethodGet, "/v1/queues/q/messages?cursor=a&cursor=b", "", "", 400},
	} {
		response := perform(server, test.method, test.target, test.body, test.contentType)
		if response.Code != test.status {
			t.Fatalf("%s %s = %d %s", test.method, test.target, response.Code, response.Body.String())
		}
	}
}

func TestStrictParserAndRequestIDHelperBranches(t *testing.T) {
	for _, encoded := range [][]byte{
		[]byte(`{"scalar":1,"array":[true,null,"x"],"object":{"nested":2}}`),
		[]byte(`{"array":[`),
		[]byte(`{"object":{"x":1`),
	} {
		err := validateJSONObject(encoded)
		if bytes.Contains(encoded, []byte(`"scalar"`)) && err != nil {
			t.Fatal(err)
		}
		if !bytes.Contains(encoded, []byte(`"scalar"`)) && err == nil {
			t.Fatalf("invalid JSON accepted: %s", encoded)
		}
	}
	for value, want := range map[string]bool{
		"request-1":              true,
		"":                       false,
		strings.Repeat("x", 129): false,
		"line\nbreak":            false,
	} {
		if got := validRequestID(value); got != want {
			t.Fatalf("validRequestID(%q) = %t", value, got)
		}
	}
	if id := randomRequestID(); id == "" {
		t.Fatal("empty random request ID")
	}

	server := newTestServer(t, &fakeService{}, Options{})
	request := httptest.NewRequest(http.MethodPost, "/v1/admin/compact", nil)
	request.ContentLength = -1
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("unknown-length empty body status = %d", response.Code)
	}
}
