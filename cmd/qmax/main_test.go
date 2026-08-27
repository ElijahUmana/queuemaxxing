package main

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
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

func TestParseServeConfigRequiresNonLoopbackOptIn(t *testing.T) {
	for _, address := range []string{"0.0.0.0:8080", "[::]:8080", "192.0.2.1:8080"} {
		if _, err := parseServeConfig([]string{"--listen", address, "--data-dir", t.TempDir()}); err == nil {
			t.Fatalf("accepted non-loopback address %q without opt-in", address)
		}
		config, err := parseServeConfig([]string{"--listen", address, "--data-dir", t.TempDir(), "--allow-non-loopback"})
		if err != nil || !config.allowNonLoopback {
			t.Fatalf("opted-in address %q = %+v, %v", address, config, err)
		}
	}
	for _, address := range []string{"127.0.0.1:8080", "[::1]:8080"} {
		if _, err := parseServeConfig([]string{"--listen", address, "--data-dir", t.TempDir()}); err != nil {
			t.Fatalf("loopback address %q rejected: %v", address, err)
		}
	}
	for _, address := range []string{"127.0.0.1", "localhost", ":", "localhost:8080", "example.com:8080", "LOCALHOST:8080"} {
		if _, err := parseServeConfig([]string{"--listen", address, "--data-dir", t.TempDir()}); err == nil {
			t.Fatalf("malformed address %q accepted", address)
		}
	}
}

func TestParseServeConfigNonLoopbackEnvironmentOptIn(t *testing.T) {
	t.Setenv("QMAX_LISTEN", "0.0.0.0:8080")
	t.Setenv("QMAX_ALLOW_NON_LOOPBACK", "true")
	config, err := parseServeConfig(nil)
	if err != nil || !config.allowNonLoopback {
		t.Fatalf("config = %+v, %v", config, err)
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
	if err := runCLI(context.Background(), []string{"healthcheck", "--timeout", "0s"}); err == nil {
		t.Fatal("invalid healthcheck command accepted")
	}
	if err := runCLI(context.Background(), []string{"serve", "unexpected"}); err == nil {
		t.Fatal("unexpected serve argument accepted")
	}
	if err := runCLI(context.Background(), []string{"help"}); err != nil {
		t.Fatal(err)
	}
}

func TestCLIParsingHelpAndEnvironment(t *testing.T) {
	t.Setenv("QMAX_LISTEN", "127.0.0.1:12345")
	t.Setenv("QMAX_DATA_DIR", t.TempDir())
	serve, err := parseServeConfig(nil)
	if err != nil || serve.listenAddress != "127.0.0.1:12345" {
		t.Fatalf("serve = %+v, %v", serve, err)
	}
	if _, err := parseServeConfig([]string{"--shutdown-timeout", "0s"}); err == nil {
		t.Fatal("expected zero shutdown timeout rejection")
	}
	if _, err := parseHealthConfig([]string{"--timeout", "0s"}); err == nil {
		t.Fatal("expected zero health timeout rejection")
	}
	if _, err := parseHealthConfig([]string{"unexpected"}); err == nil {
		t.Fatal("expected unexpected health argument rejection")
	}
	var usage bytes.Buffer
	printUsage(&usage)
	if !strings.Contains(usage.String(), "qmax serve") || !strings.Contains(usage.String(), "qmax healthcheck") {
		t.Fatalf("usage = %q", usage.String())
	}
	if err := runCLI(context.Background(), []string{"help"}); err != nil {
		t.Fatal(err)
	}
}

func TestRunCLIEmptyArgumentsUsesDefaultServe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	t.Setenv("QMAX_LISTEN", "127.0.0.1:0")
	t.Setenv("QMAX_DATA_DIR", t.TempDir())
	if err := runCLI(ctx, nil); err != nil {
		t.Fatal(err)
	}
}

func TestRunCLIHealthcheckSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/health/ready" {
			t.Fatalf("path = %q", request.URL.Path)
		}
		response.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	if err := runCLI(context.Background(), []string{"healthcheck", "--url", server.URL, "--timeout", "1s"}); err != nil {
		t.Fatal(err)
	}
}

func TestRunCLIExplicitServeAndDrain(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runCLI(ctx, []string{"serve", "--listen", address, "--data-dir", t.TempDir()}) }()
	client := &http.Client{Timeout: time.Second}
	deadline := time.Now().Add(5 * time.Second)
	for {
		response, requestErr := client.Get("http://" + address + "/health/ready")
		if requestErr == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("serve command not ready: %v", requestErr)
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestRunCLIDirectFlagsServeAndDrain(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- runCLI(ctx, []string{"--listen", address, "--data-dir", t.TempDir(), "--shutdown-timeout", "5s"})
	}()
	client := &http.Client{Timeout: time.Second}
	deadline := time.Now().Add(5 * time.Second)
	for {
		response, requestErr := client.Get("http://" + address + "/health/ready")
		if requestErr == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("direct-flag server not ready: %v", requestErr)
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestRunServesAndDrains(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, config{listenAddress: address, dataDirectory: t.TempDir(), shutdownTimeout: 5 * time.Second})
	}()
	client := &http.Client{Timeout: time.Second}
	deadline := time.Now().Add(5 * time.Second)
	for {
		response, requestErr := client.Get("http://" + address + "/health/ready")
		if requestErr == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("server did not become ready: %v", requestErr)
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
		t.Fatal("server did not drain")
	}
}

type fakeDrainer struct {
	draining bool
}

func (drainer *fakeDrainer) SetDraining(draining bool) {
	drainer.draining = draining
}

type fakeHTTPShutdowner struct {
	shutdown func(context.Context) error
	close    func() error
}

func (server *fakeHTTPShutdowner) Shutdown(ctx context.Context) error { return server.shutdown(ctx) }
func (server *fakeHTTPShutdowner) Close() error                       { return server.close() }

func TestDrainHTTPOrdersShutdownBeforeClose(t *testing.T) {
	drainer := &fakeDrainer{}
	closed := false
	server := &fakeHTTPShutdowner{
		shutdown: func(context.Context) error {
			if !drainer.draining || closed {
				t.Fatalf("shutdown observed draining=%t closed=%t", drainer.draining, closed)
			}
			return nil
		},
		close: func() error {
			closed = true
			return nil
		},
	}
	if err := drainHTTP(drainer, server, time.Second); err != nil {
		t.Fatal(err)
	}
	if closed {
		t.Fatal("forced close used after graceful shutdown")
	}
}

func TestDrainHTTPForcesCloseAndPreservesErrors(t *testing.T) {
	drainer := &fakeDrainer{}
	shutdownErr := errors.New("shutdown failed")
	closeErr := errors.New("close failed")
	server := &fakeHTTPShutdowner{
		shutdown: func(context.Context) error { return shutdownErr },
		close:    func() error { return closeErr },
	}
	err := drainHTTP(drainer, server, time.Second)
	if !drainer.draining || !errors.Is(err, shutdownErr) || !errors.Is(err, closeErr) {
		t.Fatalf("draining=%t error=%v", drainer.draining, err)
	}
}

func TestHealthcheckRejectsMalformedURLAndTransportFailure(t *testing.T) {
	for _, config := range []healthConfig{
		{baseURL: "://", timeout: time.Second},
		{baseURL: "https://user@example.com", timeout: time.Second},
		{baseURL: "http://127.0.0.1:1", timeout: 10 * time.Millisecond},
	} {
		if err := healthcheck(context.Background(), config); err == nil {
			t.Fatalf("healthcheck accepted %+v", config)
		}
	}
}

func TestMainEnvironmentFallback(t *testing.T) {
	const name = "QMAX_TEST_ENV"
	_ = os.Unsetenv(name)
	if value := envOrDefault(name, "fallback"); value != "fallback" {
		t.Fatalf("fallback = %q", value)
	}
	t.Setenv(name, "configured")
	if value := envOrDefault(name, "fallback"); value != "configured" {
		t.Fatalf("configured = %q", value)
	}
}

func TestMainHelpWrapper(t *testing.T) {
	oldArgs := os.Args
	t.Cleanup(func() { os.Args = oldArgs })
	os.Args = []string{"qmax", "help"}
	main()
}

func TestMainSubprocessDispatch(t *testing.T) {
	if os.Getenv("QMAX_MAIN_HELPER") == "1" {
		os.Args = append([]string{"qmax"}, strings.Fields(os.Getenv("QMAX_MAIN_ARGS"))...)
		main()
		return
	}
	for _, test := range []struct {
		name string
		args []string
		ok   bool
	}{
		{name: "help", args: []string{"help"}, ok: true},
		{name: "unknown", args: []string{"unknown"}, ok: false},
		{name: "bad-health", args: []string{"healthcheck", "--url", "file:///tmp/x"}, ok: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			command := exec.Command(os.Args[0], "-test.run", "^TestMainSubprocessDispatch$")
			command.Env = append(os.Environ(), "QMAX_MAIN_HELPER=1", "QMAX_MAIN_ARGS="+strings.Join(test.args, " "))
			output, err := command.CombinedOutput()
			if test.ok && err != nil {
				t.Fatalf("error = %v\n%s", err, output)
			}
			if !test.ok && err == nil {
				t.Fatalf("expected failure: %s", output)
			}
		})
	}
}
