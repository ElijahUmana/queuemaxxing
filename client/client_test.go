package client

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ElijahUmana/queuemaxxing/api"
	queueclock "github.com/ElijahUmana/queuemaxxing/internal/clock"
	"github.com/ElijahUmana/queuemaxxing/internal/engine"
	"github.com/ElijahUmana/queuemaxxing/internal/journal"
)

func TestClientCreateQueueSetsHeadersAndDecodesResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/base/v1/queues" {
			t.Fatalf("request = %s %s", request.Method, request.URL.Path)
		}
		if request.Header.Get("Idempotency-Key") != "key-1" || request.Header.Get("User-Agent") != "test-client" {
			t.Fatalf("headers = %+v", request.Header)
		}
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"data":{"config":{"name":"q","ordering":"fifo","priority_enabled":false,"default_delay_ms":0,"default_visibility_timeout_ms":30000,"max_deliveries":3,"created_at":"2026-08-26T20:00:00Z"},"counts":{"ready":0,"delayed":0,"in_flight":0,"dead":0,"acked":0,"total":0}},"replayed":false}`))
	}))
	defer server.Close()
	client, err := New(server.URL+"/base", Options{HTTPClient: server.Client(), UserAgent: "test-client"})
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.CreateQueue(context.Background(), api.CreateQueueRequest{Name: "q"}, "key-1")
	if err != nil {
		t.Fatal(err)
	}
	if result.Data.Config.Name != "q" || result.Replayed {
		t.Fatalf("result = %+v", result)
	}
}

func TestClientRealQueueLifecycle(t *testing.T) {
	store, err := journal.Open(journal.Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	service, err := engine.New(store, queueclock.Real{}, engine.Options{})
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	handler, err := api.New(service, api.Options{Logger: logger, RequestID: func() string { return "req_lifecycle" }})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	client, err := New(server.URL, Options{HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	t.Cleanup(func() {
		server.Close()
		_ = service.Close(context.Background())
	})

	created, err := client.CreateQueue(ctx, api.CreateQueueRequest{
		Name: "jobs", Ordering: api.FIFO, PriorityEnabled: true,
		DefaultDelayMS: 0, DefaultVisibilityTimeoutMS: 30_000, MaxDeliveries: 1,
	}, "create-jobs")
	if err != nil || created.Data.Config.Name != "jobs" || created.Replayed {
		t.Fatalf("create = %+v, %v", created, err)
	}
	queues, err := client.ListQueues(ctx)
	if err != nil || len(queues.Queues) != 1 {
		t.Fatalf("queues = %+v, %v", queues, err)
	}
	queue, err := client.GetQueue(ctx, "jobs")
	if err != nil || queue.Config.Ordering != api.FIFO {
		t.Fatalf("queue = %+v, %v", queue, err)
	}

	priority := int32(7)
	delay := int64(0)
	enqueued, err := client.Enqueue(ctx, "jobs", api.EnqueueRequest{
		Payload: json.RawMessage(`{"task":"render"}`), Priority: &priority, DelayMS: &delay,
	}, "enqueue-render")
	if err != nil || enqueued.Data.ID == "" || enqueued.Data.LastLSN == 0 {
		t.Fatalf("enqueue = %+v, %v", enqueued, err)
	}
	received, err := client.ReceiveIdempotent(ctx, "jobs", api.ReceiveRequest{VisibilityTimeoutMS: int64Pointer(30_000)}, "receive-render")
	if err != nil || len(received.Messages) != 1 || received.Replayed {
		t.Fatalf("receive = %+v, %v", received, err)
	}
	replayed, err := client.ReceiveIdempotent(ctx, "jobs", api.ReceiveRequest{VisibilityTimeoutMS: int64Pointer(30_000)}, "receive-render")
	if err != nil || len(replayed.Messages) != 1 || !replayed.Replayed || replayed.Messages[0].ReceiptHandle != received.Messages[0].ReceiptHandle {
		t.Fatalf("replayed receive = %+v, %v", replayed, err)
	}
	delivery := received.Messages[0]
	extended, err := client.Extend(ctx, "jobs", delivery.Message.ID, api.ExtendRequest{
		ReceiptHandle: delivery.ReceiptHandle, VisibilityTimeoutMS: 60_000,
	}, "extend-render")
	if err != nil || extended.Data.LeaseExpiresAt.Before(delivery.LeaseExpiresAt) {
		t.Fatalf("extend = %+v, %v", extended, err)
	}
	nacked, err := client.Nack(ctx, "jobs", delivery.Message.ID, api.NackRequest{
		ReceiptHandle: delivery.ReceiptHandle, RetryDelayMS: 0, Reason: "deterministic failure",
	}, "nack-render")
	if err != nil || nacked.Data.State != api.StateDead {
		t.Fatalf("nack = %+v, %v", nacked, err)
	}
	dead, err := client.ListDeadLetters(ctx, "jobs", 50, "")
	if err != nil || len(dead.Messages) != 1 || dead.Messages[0].ID != delivery.Message.ID {
		t.Fatalf("dead letters = %+v, %v", dead, err)
	}
	redriven, err := client.Redrive(ctx, "jobs", delivery.Message.ID, api.RedriveRequest{}, "redrive-render")
	if err != nil || redriven.Data.Child.ReplayOf != delivery.Message.ID {
		t.Fatalf("redrive = %+v, %v", redriven, err)
	}
	childDelivery, err := client.Receive(ctx, "jobs", api.ReceiveRequest{})
	if err != nil || len(childDelivery.Messages) != 1 || childDelivery.Messages[0].Message.ID != redriven.Data.Child.ID {
		t.Fatalf("child receive = %+v, %v", childDelivery, err)
	}
	acked, err := client.Ack(ctx, "jobs", redriven.Data.Child.ID, childDelivery.Messages[0].ReceiptHandle, "ack-child")
	if err != nil || acked.State != api.StateAcked {
		t.Fatalf("ack = %+v, %v", acked, err)
	}
	page, err := client.ListMessages(ctx, "jobs", ListFilter{Limit: 50})
	if err != nil || len(page.Messages) != 2 {
		t.Fatalf("messages = %+v, %v", page, err)
	}
	stats, err := client.Stats(ctx)
	if err != nil || stats.Queues != 1 || stats.Messages.Dead != 1 || stats.Messages.Acked != 1 {
		t.Fatalf("stats = %+v, %v", stats, err)
	}
	if err := client.Compact(ctx); err != nil {
		t.Fatal(err)
	}
	if err := client.Live(ctx); err != nil {
		t.Fatal(err)
	}
	if err := client.Ready(ctx); err != nil {
		t.Fatal(err)
	}
}

func int64Pointer(value int64) *int64 { return &value }

func TestClientEscapesPathSegments(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.EscapedPath() != "/v1/queues/a%2Fb" {
			t.Fatalf("escaped path = %q", request.URL.EscapedPath())
		}
		_, _ = response.Write([]byte(`{"config":{"name":"a/b","ordering":"fifo","priority_enabled":false,"default_delay_ms":0,"default_visibility_timeout_ms":1,"max_deliveries":1,"created_at":"2026-08-26T20:00:00Z"},"counts":{"ready":0,"delayed":0,"in_flight":0,"dead":0,"acked":0,"total":0}}`))
	}))
	defer server.Close()
	client, err := New(server.URL, Options{HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.GetQueue(context.Background(), "a/b"); err != nil {
		t.Fatal(err)
	}
}

func TestClientReturnsTypedProblem(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "application/problem+json")
		response.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(response).Encode(api.Problem{Status: http.StatusConflict, Code: "stale_receipt", Detail: "stale", RequestID: "req_1"})
	}))
	defer server.Close()
	client, err := New(server.URL, Options{HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Ack(context.Background(), "q", "m", "receipt", "")
	if !IsProblem(err, "stale_receipt") || !strings.Contains(err.Error(), "req_1") {
		t.Fatalf("error = %v", err)
	}
}

func TestClientRejectsOversizedAndMalformedResponses(t *testing.T) {
	for name, body := range map[string]string{
		"oversized": strings.Repeat("x", 65),
		"malformed": `{`,
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
				_, _ = response.Write([]byte(body))
			}))
			defer server.Close()
			client, err := New(server.URL, Options{HTTPClient: server.Client(), MaxResponseBytes: 64})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := client.ListQueues(context.Background()); err == nil {
				t.Fatal("expected response error")
			}
		})
	}
}

func TestClientRejectsUnsafeBaseURLs(t *testing.T) {
	for _, raw := range []string{"file:///tmp/socket", "https://user@example.com", "https://example.com?q=1", "https://example.com#fragment"} {
		if _, err := New(raw, Options{}); err == nil {
			t.Fatalf("expected %q to be rejected", raw)
		}
	}
	if _, err := New("http://example.com", Options{MaxResponseBytes: -1}); err == nil {
		t.Fatal("negative response limit accepted")
	}
}

func TestListQueryIncludesAllFilters(t *testing.T) {
	query := listQuery(ListFilter{State: api.StateDelayed, Limit: 25, Cursor: "cursor"}, true)
	if query.Get("state") != "delayed" || query.Get("limit") != "25" || query.Get("cursor") != "cursor" {
		t.Fatalf("query = %s", query.Encode())
	}
	if query := listQuery(ListFilter{}, false); len(query) != 0 {
		t.Fatalf("empty query = %s", query.Encode())
	}
}
