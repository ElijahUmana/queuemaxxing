package contract_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	queueapi "github.com/ElijahUmana/queuemaxxing/api"
)

const maxResponseBytes = 2 << 20

type responseEnvelope struct {
	Data     json.RawMessage `json:"data"`
	Replayed *bool           `json:"replayed,omitempty"`
}

type problem struct {
	Type      string `json:"type"`
	Title     string `json:"title"`
	Status    int    `json:"status"`
	Code      string `json:"code"`
	Detail    string `json:"detail"`
	RequestID string `json:"request_id"`
}

func TestPublicAPIContract(t *testing.T) {
	baseURL := strings.TrimSuffix(os.Getenv("QMAX_TEST_URL"), "/")
	if baseURL == "" {
		t.Skip("QMAX_TEST_URL is not set; run against a real qmax process")
	}
	client := &http.Client{Timeout: 10 * time.Second}
	queueName := fmt.Sprintf("contract-%d", time.Now().UnixNano())

	createBody := fmt.Sprintf(`{"name":%q,"ordering":"fifo","priority_enabled":true,"default_delay_ms":0,"default_visibility_timeout_ms":30000,"max_deliveries":3}`, queueName)
	create := requestJSON(t, client, http.MethodPost, baseURL+"/v1/queues", createBody, map[string]string{"Idempotency-Key": "create-" + queueName})
	if create.StatusCode != http.StatusCreated {
		t.Fatalf("create queue status = %d, body = %s", create.StatusCode, create.Body)
	}
	assertJSONContentType(t, create)
	assertMutationEnvelope(t, create.Body)

	replay := requestJSON(t, client, http.MethodPost, baseURL+"/v1/queues", createBody, map[string]string{"Idempotency-Key": "create-" + queueName})
	if replay.StatusCode != http.StatusOK {
		t.Fatalf("replayed create status = %d, body = %s", replay.StatusCode, replay.Body)
	}
	envelope := assertMutationEnvelope(t, replay.Body)
	if envelope.Replayed == nil || !*envelope.Replayed {
		t.Fatalf("replayed create response = %s", replay.Body)
	}

	conflictBody := strings.Replace(createBody, `"fifo"`, `"lifo"`, 1)
	conflict := requestJSON(t, client, http.MethodPost, baseURL+"/v1/queues", conflictBody, map[string]string{"Idempotency-Key": "create-" + queueName})
	if conflict.StatusCode != http.StatusConflict {
		t.Fatalf("idempotency conflict status = %d, body = %s", conflict.StatusCode, conflict.Body)
	}
	assertProblem(t, conflict, "idempotency_conflict")

	enqueueBody := `{"payload":{"value":"<img src=x onerror=globalThis.__qmaxXSS=1>"},"priority":7,"delay_ms":0}`
	enqueue := requestJSON(t, client, http.MethodPost, baseURL+"/v1/queues/"+queueName+"/messages", enqueueBody, map[string]string{"Idempotency-Key": "enqueue-" + queueName})
	if enqueue.StatusCode != http.StatusCreated {
		t.Fatalf("enqueue status = %d, body = %s", enqueue.StatusCode, enqueue.Body)
	}
	assertMutationEnvelope(t, enqueue.Body)

	receive := requestJSON(t, client, http.MethodPost, baseURL+"/v1/queues/"+queueName+"/messages:receive", `{"visibility_timeout_ms":30000,"wait_timeout_ms":0}`, nil)
	if receive.StatusCode != http.StatusOK {
		t.Fatalf("receive status = %d, body = %s", receive.StatusCode, receive.Body)
	}
	var delivery queueapi.ReceiveResponse
	decodeStrict(t, receive.Body, &delivery)
	if len(delivery.Messages) != 1 || delivery.Messages[0].Message.ID == "" || delivery.Messages[0].ReceiptHandle == "" {
		t.Fatalf("invalid receive response: %s", receive.Body)
	}
	if delivery.Messages[0].Message.Sequence == 0 {
		t.Fatalf("sequence must be populated: %s", receive.Body)
	}
	if bytes.Contains(receive.Body, []byte(`"last_lsn"`)) {
		t.Fatalf("receive response exposed per-message last_lsn: %s", receive.Body)
	}

	messageID := delivery.Messages[0].Message.ID
	ackBody := fmt.Sprintf(`{"receipt_handle":%q}`, delivery.Messages[0].ReceiptHandle)
	ack := requestJSON(t, client, http.MethodPost, baseURL+"/v1/queues/"+queueName+"/messages/"+messageID+"/ack", ackBody, map[string]string{"Idempotency-Key": "ack-" + queueName})
	if ack.StatusCode != http.StatusOK {
		t.Fatalf("ack status = %d, body = %s", ack.StatusCode, ack.Body)
	}

	stale := requestJSON(t, client, http.MethodPost, baseURL+"/v1/queues/"+queueName+"/messages/"+messageID+"/ack", `{"receipt_handle":"wrong"}`, map[string]string{"Idempotency-Key": "stale-" + queueName})
	if stale.StatusCode != http.StatusConflict {
		t.Fatalf("stale receipt status = %d, body = %s", stale.StatusCode, stale.Body)
	}
	assertProblem(t, stale, "stale_receipt")
}

func TestMalformedRequests(t *testing.T) {
	baseURL := strings.TrimSuffix(os.Getenv("QMAX_TEST_URL"), "/")
	if baseURL == "" {
		t.Skip("QMAX_TEST_URL is not set; run against a real qmax process")
	}
	client := &http.Client{Timeout: 10 * time.Second}
	cases := []struct {
		name        string
		contentType string
		body        []byte
		wantCode    string
	}{
		{name: "wrong content type", contentType: "text/plain", body: []byte(`{}`), wantCode: "unsupported_media_type"},
		{name: "unknown field", contentType: "application/json", body: []byte(`{"name":"x","ordering":"fifo","priority_enabled":false,"default_delay_ms":0,"default_visibility_timeout_ms":1,"max_deliveries":1,"unknown":true}`), wantCode: "invalid_json"},
		{name: "trailing JSON", contentType: "application/json", body: []byte(`{} {}`), wantCode: "invalid_json"},
		{name: "duplicate key", contentType: "application/json", body: []byte(`{"name":"a","name":"b"}`), wantCode: "invalid_json"},
		{name: "null", contentType: "application/json", body: []byte(`null`), wantCode: "invalid_json"},
		{name: "invalid UTF-8", contentType: "application/json", body: []byte{'{', '"', 'n', 'a', 'm', 'e', '"', ':', '"', 0xff, '"', '}'}, wantCode: "invalid_json"},
		{name: "unpaired surrogate", contentType: "application/json", body: []byte(`{"name":"\uD800"}`), wantCode: "invalid_json"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			request, err := http.NewRequestWithContext(context.Background(), http.MethodPost, baseURL+"/v1/queues", bytes.NewReader(testCase.body))
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Content-Type", testCase.contentType)
			response, err := client.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			result := readResponse(t, response)
			if result.StatusCode != http.StatusBadRequest && result.StatusCode != http.StatusUnsupportedMediaType {
				t.Fatalf("status = %d, body = %s", result.StatusCode, result.Body)
			}
			assertProblem(t, result, testCase.wantCode)
		})
	}
}

func TestOptionalZeroDefaults(t *testing.T) {
	baseURL := strings.TrimSuffix(os.Getenv("QMAX_TEST_URL"), "/")
	if baseURL == "" {
		t.Skip("QMAX_TEST_URL is not set; run against a real qmax process")
	}
	client := &http.Client{Timeout: 10 * time.Second}
	queueName := fmt.Sprintf("defaults-%d", time.Now().UnixNano())
	body := fmt.Sprintf(`{"name":%q,"ordering":"fifo","default_visibility_timeout_ms":30000,"max_deliveries":2}`, queueName)
	created := requestJSON(t, client, http.MethodPost, baseURL+"/v1/queues", body, nil)
	if created.StatusCode != http.StatusCreated {
		t.Fatalf("create status = %d, body = %s", created.StatusCode, created.Body)
	}
	enqueued := requestJSON(t, client, http.MethodPost, baseURL+"/v1/queues/"+queueName+"/messages", `{"payload":{}}`, nil)
	if enqueued.StatusCode != http.StatusCreated {
		t.Fatalf("enqueue status = %d, body = %s", enqueued.StatusCode, enqueued.Body)
	}
	received := requestJSON(t, client, http.MethodPost, baseURL+"/v1/queues/"+queueName+"/messages:receive", `{}`, nil)
	if received.StatusCode != http.StatusOK {
		t.Fatalf("receive status = %d, body = %s", received.StatusCode, received.Body)
	}
	var delivery queueapi.ReceiveResponse
	decodeStrict(t, received.Body, &delivery)
	nacked := requestJSON(t, client, http.MethodPost, baseURL+"/v1/queues/"+queueName+"/messages/"+delivery.Messages[0].Message.ID+"/nack", fmt.Sprintf(`{"receipt_handle":%q}`, delivery.Messages[0].ReceiptHandle), nil)
	if nacked.StatusCode != http.StatusOK {
		t.Fatalf("nack status = %d, body = %s", nacked.StatusCode, nacked.Body)
	}
}

func TestDuplicateContentTypeHeaders(t *testing.T) {
	baseURL := strings.TrimSuffix(os.Getenv("QMAX_TEST_URL"), "/")
	if baseURL == "" {
		t.Skip("QMAX_TEST_URL is not set; run against a real qmax process")
	}
	client := &http.Client{Timeout: 10 * time.Second}
	for _, values := range [][]string{{"application/json", "application/json"}, {"application/json", "text/plain"}} {
		request, err := http.NewRequestWithContext(context.Background(), http.MethodPost, baseURL+"/v1/queues", strings.NewReader(`{}`))
		if err != nil {
			t.Fatal(err)
		}
		for _, value := range values {
			request.Header.Add("Content-Type", value)
		}
		result, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		response := readResponse(t, result)
		if response.StatusCode != http.StatusUnsupportedMediaType {
			t.Fatalf("headers %q status = %d, body = %s", values, response.StatusCode, response.Body)
		}
		assertProblem(t, response, "unsupported_media_type")
	}
}

func TestCompactRequiresJSON(t *testing.T) {
	baseURL := strings.TrimSuffix(os.Getenv("QMAX_TEST_URL"), "/")
	if baseURL == "" {
		t.Skip("QMAX_TEST_URL is not set; run against a real qmax process")
	}
	client := &http.Client{Timeout: 10 * time.Second}
	for _, test := range []struct {
		body, contentType string
		status            int
	}{
		{status: http.StatusUnsupportedMediaType},
		{contentType: "application/json", status: http.StatusBadRequest},
		{body: `{}`, contentType: "text/plain", status: http.StatusUnsupportedMediaType},
		{body: `x=y`, contentType: "application/x-www-form-urlencoded", status: http.StatusUnsupportedMediaType},
		{body: `{}`, contentType: "application/json", status: http.StatusOK},
	} {
		request, err := http.NewRequestWithContext(context.Background(), http.MethodPost, baseURL+"/v1/admin/compact", strings.NewReader(test.body))
		if err != nil {
			t.Fatal(err)
		}
		if test.contentType != "" {
			request.Header.Set("Content-Type", test.contentType)
		}
		request.Header.Set("Origin", "https://hostile.example")
		result, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		response := readResponse(t, result)
		if response.StatusCode != test.status || response.Header.Get("Access-Control-Allow-Origin") != "" {
			t.Fatalf("contentType=%q status=%d ACAO=%q body=%s", test.contentType, response.StatusCode, response.Header.Get("Access-Control-Allow-Origin"), response.Body)
		}
	}
}

func TestMalformedQueries(t *testing.T) {
	baseURL := strings.TrimSuffix(os.Getenv("QMAX_TEST_URL"), "/")
	if baseURL == "" {
		t.Skip("QMAX_TEST_URL is not set; run against a real qmax process")
	}
	client := &http.Client{Timeout: 10 * time.Second}
	for _, rawQuery := range []string{"%", "%ZZ", "limit=1;state=ready", "limit=1&limit=2", "limit=", "cursor=", "state="} {
		request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, baseURL+"/v1/queues/query-test/messages", nil)
		if err != nil {
			t.Fatal(err)
		}
		request.URL.RawQuery = rawQuery
		result, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		response := readResponse(t, result)
		if response.StatusCode != http.StatusBadRequest {
			t.Fatalf("query %q status = %d, body = %s", rawQuery, response.StatusCode, response.Body)
		}
		assertProblem(t, response, "invalid_query")
	}
}

type response struct {
	StatusCode int
	Header     http.Header
	Body       []byte
}

func requestJSON(t *testing.T, client *http.Client, method, url, body string, headers map[string]string) response {
	t.Helper()
	request, err := http.NewRequestWithContext(context.Background(), method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	result, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return readResponse(t, result)
}

func readResponse(t *testing.T, result *http.Response) response {
	t.Helper()
	defer result.Body.Close()
	body, err := io.ReadAll(io.LimitReader(result.Body, maxResponseBytes+1))
	if err != nil {
		t.Fatal(err)
	}
	if len(body) > maxResponseBytes {
		t.Fatal("response exceeds test safety limit")
	}
	return response{StatusCode: result.StatusCode, Header: result.Header.Clone(), Body: body}
}

func assertJSONContentType(t *testing.T, result response) {
	t.Helper()
	if got := result.Header.Get("Content-Type"); !strings.HasPrefix(got, "application/json") && !strings.HasPrefix(got, "application/problem+json") {
		t.Fatalf("Content-Type = %q", got)
	}
}

func assertMutationEnvelope(t *testing.T, body []byte) responseEnvelope {
	t.Helper()
	var envelope responseEnvelope
	decodeStrict(t, body, &envelope)
	if len(envelope.Data) == 0 || bytes.Equal(envelope.Data, []byte("null")) || envelope.Replayed == nil {
		t.Fatalf("invalid mutation envelope: %s", body)
	}
	return envelope
}

func assertProblem(t *testing.T, result response, code string) {
	t.Helper()
	assertJSONContentType(t, result)
	var decoded problem
	decodeStrict(t, result.Body, &decoded)
	if decoded.Status != result.StatusCode || decoded.Code != code || decoded.Type == "" || decoded.Title == "" || decoded.Detail == "" || decoded.RequestID == "" {
		t.Fatalf("invalid problem response: status=%d body=%s", result.StatusCode, result.Body)
	}
}

func decodeStrict(t *testing.T, body []byte, target any) {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		t.Fatalf("decode %s: %v", body, err)
	}
	if decoder.More() {
		t.Fatalf("trailing JSON in %s", body)
	}
}
