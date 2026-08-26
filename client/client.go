package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/ElijahUmana/queuemaxxing/api"
)

const defaultMaxResponseBytes = int64(4 << 20)

type ListFilter struct {
	State  api.MessageState
	Limit  int
	Cursor string
}

type Options struct {
	HTTPClient       *http.Client
	MaxResponseBytes int64
	UserAgent        string
}

type Client struct {
	baseURL          *url.URL
	httpClient       *http.Client
	maxResponseBytes int64
	userAgent        string
}

type ProblemError struct {
	Problem api.Problem
}

func (err *ProblemError) Error() string {
	if err.Problem.RequestID == "" {
		return fmt.Sprintf("queue API: %s (%s)", err.Problem.Detail, err.Problem.Code)
	}
	return fmt.Sprintf("queue API: %s (%s, request %s)", err.Problem.Detail, err.Problem.Code, err.Problem.RequestID)
}

func IsProblem(err error, code string) bool {
	var problem *ProblemError
	return errors.As(err, &problem) && problem.Problem.Code == code
}

func New(baseURL string, options Options) (*Client, error) {
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("parse queue API URL: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, errors.New("queue API URL scheme must be http or https")
	}
	if parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, errors.New("queue API URL must contain only scheme, host, and optional base path")
	}
	parsed.Path = strings.TrimSuffix(parsed.Path, "/")
	if options.HTTPClient == nil {
		options.HTTPClient = &http.Client{Timeout: 40 * time.Second}
	}
	if options.MaxResponseBytes == 0 {
		options.MaxResponseBytes = defaultMaxResponseBytes
	}
	if options.MaxResponseBytes < 1 {
		return nil, errors.New("maximum response bytes must be positive")
	}
	if options.UserAgent == "" {
		options.UserAgent = "queuemaxxing-go-client/1"
	}
	return &Client{
		baseURL: parsed, httpClient: options.HTTPClient,
		maxResponseBytes: options.MaxResponseBytes, userAgent: options.UserAgent,
	}, nil
}

func (client *Client) CreateQueue(ctx context.Context, request api.CreateQueueRequest, idempotencyKey string) (api.MutationResponse[api.Queue], error) {
	return doJSON[api.MutationResponse[api.Queue]](client, ctx, http.MethodPost, "/v1/queues", nil, request, idempotencyKey)
}

func (client *Client) ListQueues(ctx context.Context) (api.QueueList, error) {
	return doJSON[api.QueueList](client, ctx, http.MethodGet, "/v1/queues", nil, nil, "")
}

func (client *Client) GetQueue(ctx context.Context, queue string) (api.Queue, error) {
	return doJSON[api.Queue](client, ctx, http.MethodGet, path("/v1/queues", queue), nil, nil, "")
}

func (client *Client) Enqueue(ctx context.Context, queue string, request api.EnqueueRequest, idempotencyKey string) (api.MutationResponse[api.Message], error) {
	return doJSON[api.MutationResponse[api.Message]](client, ctx, http.MethodPost, path("/v1/queues", queue, "messages"), nil, request, idempotencyKey)
}

func (client *Client) Receive(ctx context.Context, queue string, request api.ReceiveRequest) (api.ReceiveResponse, error) {
	return doJSON[api.ReceiveResponse](client, ctx, http.MethodPost, path("/v1/queues", queue, "messages:receive"), nil, request, "")
}

func (client *Client) Ack(ctx context.Context, queue, messageID, receipt, idempotencyKey string) (api.AckResponse, error) {
	return doJSON[api.AckResponse](client, ctx, http.MethodPost, path("/v1/queues", queue, "messages", messageID, "ack"), nil, api.ReceiptRequest{ReceiptHandle: receipt}, idempotencyKey)
}

func (client *Client) Nack(ctx context.Context, queue, messageID string, request api.NackRequest, idempotencyKey string) (api.MutationResponse[api.Message], error) {
	return doJSON[api.MutationResponse[api.Message]](client, ctx, http.MethodPost, path("/v1/queues", queue, "messages", messageID, "nack"), nil, request, idempotencyKey)
}

func (client *Client) Extend(ctx context.Context, queue, messageID string, request api.ExtendRequest, idempotencyKey string) (api.MutationResponse[api.Delivery], error) {
	return doJSON[api.MutationResponse[api.Delivery]](client, ctx, http.MethodPost, path("/v1/queues", queue, "messages", messageID, "extend"), nil, request, idempotencyKey)
}

func (client *Client) ListMessages(ctx context.Context, queue string, filter ListFilter) (api.MessagePage, error) {
	query := listQuery(filter, true)
	return doJSON[api.MessagePage](client, ctx, http.MethodGet, path("/v1/queues", queue, "messages"), query, nil, "")
}

func (client *Client) ListDeadLetters(ctx context.Context, queue string, limit int, cursor string) (api.MessagePage, error) {
	query := listQuery(ListFilter{Limit: limit, Cursor: cursor}, false)
	return doJSON[api.MessagePage](client, ctx, http.MethodGet, path("/v1/queues", queue, "dead-letters"), query, nil, "")
}

func (client *Client) Redrive(ctx context.Context, queue, messageID string, request api.RedriveRequest, idempotencyKey string) (api.MutationResponse[api.RedriveResult], error) {
	return doJSON[api.MutationResponse[api.RedriveResult]](client, ctx, http.MethodPost, path("/v1/queues", queue, "dead-letters", messageID, "redrive"), nil, request, idempotencyKey)
}

func (client *Client) Stats(ctx context.Context) (api.ServiceStats, error) {
	return doJSON[api.ServiceStats](client, ctx, http.MethodGet, "/v1/stats", nil, nil, "")
}

func (client *Client) Compact(ctx context.Context) error {
	_, err := doJSON[api.StatusResponse](client, ctx, http.MethodPost, "/v1/admin/compact", nil, struct{}{}, "")
	return err
}

func (client *Client) Live(ctx context.Context) error {
	_, err := doJSON[api.StatusResponse](client, ctx, http.MethodGet, "/health/live", nil, nil, "")
	return err
}

func (client *Client) Ready(ctx context.Context) error {
	_, err := doJSON[api.StatusResponse](client, ctx, http.MethodGet, "/health/ready", nil, nil, "")
	return err
}

func listQuery(filter ListFilter, state bool) url.Values {
	query := make(url.Values)
	if filter.Limit > 0 {
		query.Set("limit", strconv.Itoa(filter.Limit))
	}
	if filter.Cursor != "" {
		query.Set("cursor", filter.Cursor)
	}
	if state && filter.State != "" {
		query.Set("state", string(filter.State))
	}
	return query
}

func doJSON[T any](client *Client, ctx context.Context, method, requestPath string, query url.Values, input any, idempotencyKey string) (T, error) {
	var zero T
	var body io.Reader
	if input != nil {
		encoded, err := json.Marshal(input)
		if err != nil {
			return zero, fmt.Errorf("encode queue API request: %w", err)
		}
		body = bytes.NewReader(encoded)
	}
	endpoint := *client.baseURL
	decodedRequestPath, err := url.PathUnescape(requestPath)
	if err != nil {
		return zero, fmt.Errorf("decode queue API request path: %w", err)
	}
	endpoint.Path = strings.TrimSuffix(client.baseURL.Path, "/") + decodedRequestPath
	endpoint.RawPath = strings.TrimSuffix(client.baseURL.EscapedPath(), "/") + requestPath
	endpoint.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, method, endpoint.String(), body)
	if err != nil {
		return zero, fmt.Errorf("create queue API request: %w", err)
	}
	request.Header.Set("Accept", "application/json, application/problem+json")
	request.Header.Set("User-Agent", client.userAgent)
	if input != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if idempotencyKey != "" {
		request.Header.Set("Idempotency-Key", idempotencyKey)
	}
	response, err := client.httpClient.Do(request)
	if err != nil {
		return zero, fmt.Errorf("send queue API request: %w", err)
	}
	defer response.Body.Close()
	limited := io.LimitReader(response.Body, client.maxResponseBytes+1)
	encoded, err := io.ReadAll(limited)
	if err != nil {
		return zero, fmt.Errorf("read queue API response: %w", err)
	}
	if int64(len(encoded)) > client.maxResponseBytes {
		return zero, errors.New("queue API response exceeds configured limit")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		var problem api.Problem
		if err := json.Unmarshal(encoded, &problem); err != nil || problem.Code == "" {
			return zero, fmt.Errorf("queue API returned HTTP %d with an invalid problem response", response.StatusCode)
		}
		return zero, &ProblemError{Problem: problem}
	}
	if err := json.Unmarshal(encoded, &zero); err != nil {
		return zero, fmt.Errorf("decode queue API response: %w", err)
	}
	return zero, nil
}

func path(prefix string, segments ...string) string {
	var builder strings.Builder
	builder.WriteString(strings.TrimSuffix(prefix, "/"))
	for _, segment := range segments {
		builder.WriteByte('/')
		builder.WriteString(url.PathEscape(segment))
	}
	return builder.String()
}
