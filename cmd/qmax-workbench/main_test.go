package main

import (
	"bytes"
	"context"
	"flag"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
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

func TestWorkbenchConfigAndEnvironment(t *testing.T) {
	oldArgs, oldFlags := os.Args, flag.CommandLine
	t.Cleanup(func() { os.Args, flag.CommandLine = oldArgs, oldFlags })
	t.Setenv("QMAX_WORKBENCH_LISTEN", "127.0.0.1:19081")
	t.Setenv("QMAX_API_URL", "http://127.0.0.1:19080")
	os.Args = []string{"qmax-workbench"}
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	config := parseConfig()
	if config.listenAddress != "127.0.0.1:19081" || config.apiURL != "http://127.0.0.1:19080" {
		t.Fatalf("config = %+v", config)
	}
	const unset = "QMAX_WORKBENCH_UNSET"
	_ = os.Unsetenv(unset)
	if value := envOrDefault(unset, "fallback"); value != "fallback" {
		t.Fatalf("fallback = %q", value)
	}
}

func TestWorkbenchStaticAndRouteErrors(t *testing.T) {
	handler, err := newHandler("http://127.0.0.1:1", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		method, target string
		status         int
	}{
		{http.MethodGet, "/assets/app.js", http.StatusOK},
		{http.MethodGet, "/missing", http.StatusNotFound},
		{http.MethodPost, "/", http.StatusMethodNotAllowed},
		{http.MethodGet, "/api/v1/queues", http.StatusBadGateway},
	} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(test.method, test.target, nil))
		if response.Code != test.status {
			t.Fatalf("%s %s = %d, want %d: %s", test.method, test.target, response.Code, test.status, response.Body.String())
		}
		if test.target == "/assets/app.js" && !strings.Contains(response.Header().Get("Cache-Control"), "max-age=300") {
			t.Fatalf("asset cache header = %q", response.Header().Get("Cache-Control"))
		}
		if test.target == "/api/v1/queues" && response.Header().Get("Content-Type") != "application/problem+json" {
			t.Fatalf("proxy error content type = %q", response.Header().Get("Content-Type"))
		}
	}
}

func TestRunWorkbenchServesAndDrains(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	go func() { done <- run(ctx, config{listenAddress: address, apiURL: "http://127.0.0.1:1"}, logger) }()
	client := &http.Client{Timeout: time.Second}
	deadline := time.Now().Add(5 * time.Second)
	for {
		response, requestErr := client.Get("http://" + address + "/healthz")
		if requestErr == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("workbench not ready: %v", requestErr)
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("workbench did not drain")
	}
}

func TestRunWorkbenchRejectsConfigurationAndBusyPort(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := run(context.Background(), config{listenAddress: "127.0.0.1:0", apiURL: "file:///tmp/socket"}, logger); err == nil || !strings.Contains(err.Error(), "configure workbench") {
		t.Fatalf("configuration error = %v", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err := run(context.Background(), config{listenAddress: listener.Addr().String(), apiURL: "http://127.0.0.1:1"}, logger); err == nil || !strings.Contains(err.Error(), "serve workbench") {
		t.Fatalf("listener error = %v", err)
	}
}

func TestWorkbenchMainSubprocessSuccess(t *testing.T) {
	if os.Getenv("QMAX_WORKBENCH_SUCCESS_HELPER") == "1" {
		os.Args = []string{"qmax-workbench", "-listen", os.Getenv("QMAX_WORKBENCH_TEST_ADDR"), "-api-url", "http://127.0.0.1:1"}
		flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
		main()
		return
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	command := exec.Command(os.Args[0], "-test.run", "^TestWorkbenchMainSubprocessSuccess$")
	command.Env = append(os.Environ(), "QMAX_WORKBENCH_SUCCESS_HELPER=1", "QMAX_WORKBENCH_TEST_ADDR="+address)
	var output bytes.Buffer
	command.Stdout, command.Stderr = &output, &output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Timeout: time.Second}
	deadline := time.Now().Add(5 * time.Second)
	for {
		response, requestErr := client.Get("http://" + address + "/healthz")
		if requestErr == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				break
			}
		}
		if time.Now().After(deadline) {
			_ = command.Process.Kill()
			t.Fatalf("main subprocess not ready: %v", requestErr)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := command.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err != nil {
		t.Fatalf("error=%v output=%s", err, output.String())
	}
}

func TestWorkbenchMainSubprocessFailure(t *testing.T) {
	if os.Getenv("QMAX_WORKBENCH_MAIN_HELPER") == "1" {
		os.Args = []string{"qmax-workbench", "-api-url", "file:///tmp/socket"}
		flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
		main()
		return
	}
	command := exec.Command(os.Args[0], "-test.run", "^TestWorkbenchMainSubprocessFailure$")
	command.Env = append(os.Environ(), "QMAX_WORKBENCH_MAIN_HELPER=1")
	if output, err := command.CombinedOutput(); err == nil || !strings.Contains(string(output), "queue workbench stopped") {
		t.Fatalf("error=%v output=%s", err, output)
	}
}
