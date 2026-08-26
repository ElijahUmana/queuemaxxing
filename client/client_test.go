package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ElijahUmana/queuemaxxing/api"
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
}
