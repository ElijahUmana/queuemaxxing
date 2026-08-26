package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestParseServeConfig(t *testing.T) {
	config, err := parseServeConfig([]string{"--listen", "127.0.0.1:9090", "--data-dir", t.TempDir(), "--shutdown-timeout", "3s"})
	if err != nil {
		t.Fatal(err)
	}
	if config.listenAddress != "127.0.0.1:9090" || config.shutdownTimeout != 3*time.Second {
		t.Fatalf("config = %+v", config)
	}
}

func TestParseServeConfigRejectsUnexpectedArguments(t *testing.T) {
	if _, err := parseServeConfig([]string{"unexpected"}); err == nil {
		t.Fatal("expected unexpected-argument error")
	}
}

func TestHealthcheckUsesReadinessEndpoint(t *testing.T) {
	requestedPath := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		requestedPath <- request.URL.Path
		response.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	if err := healthcheck(context.Background(), healthConfig{baseURL: server.URL + "/base", timeout: time.Second}); err != nil {
		t.Fatal(err)
	}
	if path := <-requestedPath; path != "/base/health/ready" {
		t.Fatalf("path = %q, want %q", path, "/base/health/ready")
	}
}

func TestHealthcheckRejectsUnreadyAndUnsafeURLs(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	if err := healthcheck(context.Background(), healthConfig{baseURL: server.URL, timeout: time.Second}); err == nil {
		t.Fatal("expected readiness error")
	}
	if err := healthcheck(context.Background(), healthConfig{baseURL: "file:///tmp/qmax", timeout: time.Second}); err == nil {
		t.Fatal("expected unsafe URL error")
	}
}

func TestRunCLIRejectsUnknownCommand(t *testing.T) {
	err := runCLI(context.Background(), []string{"unknown"})
	if err == nil || !strings.Contains(err.Error(), `unknown command "unknown"`) {
		t.Fatalf("error = %v", err)
	}
}
