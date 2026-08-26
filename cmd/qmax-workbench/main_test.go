package main

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandlerServesWorkbench(t *testing.T) {
	handler, err := newHandler("http://127.0.0.1:8080", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}

	request := httptest.NewRequest(http.MethodGet, "/", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}
	if !strings.Contains(response.Body.String(), "Queue workbench") {
		t.Fatalf("body does not contain workbench title")
	}
	if response.Header().Get("Content-Security-Policy") == "" {
		t.Fatal("missing content security policy")
	}
}

func TestHandlerProxiesOnlyAPIPath(t *testing.T) {
	upstreamRequest := make(chan *http.Request, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		upstreamRequest <- request.Clone(request.Context())
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()

	handler, err := newHandler(upstream.URL+"/base", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}

	request := httptest.NewRequest(http.MethodGet, "/api/v1/queues?limit=10", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}
	proxied := <-upstreamRequest
	if proxied.URL.Path != "/base/v1/queues" {
		t.Fatalf("path = %q, want %q", proxied.URL.Path, "/base/v1/queues")
	}
	if proxied.URL.Query().Get("limit") != "10" {
		t.Fatalf("query was not preserved")
	}
	if proxied.Header.Get("X-Qmax-Workbench") != "1" {
		t.Fatalf("workbench header was not set")
	}
}

func TestHandlerRejectsInvalidUpstream(t *testing.T) {
	_, err := newHandler("file:///tmp/socket", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err == nil {
		t.Fatal("expected invalid upstream error")
	}
}

func TestHealthIsLocal(t *testing.T) {
	handler, err := newHandler("http://127.0.0.1:1", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}

	request := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}
	if response.Body.String() != `{"status":"ok"}` {
		t.Fatalf("body = %q", response.Body.String())
	}
}
