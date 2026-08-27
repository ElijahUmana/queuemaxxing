package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/netip"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	workbenchweb "github.com/ElijahUmana/queuemaxxing/web"
)

const (
	defaultListenAddress = "127.0.0.1:8081"
	defaultAPIURL        = "http://127.0.0.1:8080"
)

type config struct {
	listenAddress    string
	apiURL           string
	allowNonLoopback bool
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(context.Background(), parseConfig(), logger); err != nil {
		logger.Error("queue workbench stopped", "error", err)
		os.Exit(1)
	}
}

func run(parent context.Context, config config, logger *slog.Logger) error {
	if err := validateListenAddress(config.listenAddress, config.allowNonLoopback); err != nil {
		return err
	}
	handler, err := newHandler(config.apiURL, logger)
	if err != nil {
		return fmt.Errorf("configure workbench: %w", err)
	}
	server := &http.Server{
		Addr: config.listenAddress, Handler: handler,
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second,
		WriteTimeout: 65 * time.Second, IdleTimeout: 90 * time.Second,
	}
	ctx, stop := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	defer stop()
	errCh := make(chan error, 1)
	go func() {
		logger.Info("queue workbench listening", "address", config.listenAddress, "api", config.apiURL)
		errCh <- server.ListenAndServe()
	}()
	select {
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve workbench: %w", err)
		}
		return nil
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			_ = server.Close()
			return fmt.Errorf("shut down workbench: %w", err)
		}
		return nil
	}
}

func parseConfig() config {
	var cfg config
	flag.StringVar(&cfg.listenAddress, "listen", envOrDefault("QMAX_WORKBENCH_LISTEN", defaultListenAddress), "workbench listen address")
	flag.StringVar(&cfg.apiURL, "api-url", envOrDefault("QMAX_API_URL", defaultAPIURL), "qmax public API base URL")
	flag.BoolVar(&cfg.allowNonLoopback, "allow-non-loopback", envBool("QMAX_WORKBENCH_ALLOW_NON_LOOPBACK"), "allow listening beyond loopback when protected by external authentication and TLS")
	flag.Parse()
	return cfg
}

func validateListenAddress(address string, allowNonLoopback bool) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("invalid listen address: %w", err)
	}
	if port == "" {
		return errors.New("listen address must include a port")
	}
	parsed, err := netip.ParseAddr(host)
	if err != nil {
		return errors.New("listen address host must be an IP address")
	}
	if !parsed.IsLoopback() && !allowNonLoopback {
		return errors.New("non-loopback listen address requires --allow-non-loopback and external authentication and TLS")
	}
	return nil
}

func envBool(name string) bool {
	value, err := strconv.ParseBool(os.Getenv(name))
	return err == nil && value
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

type proxyProblem struct {
	Type      string `json:"type"`
	Title     string `json:"title"`
	Status    int    `json:"status"`
	Code      string `json:"code"`
	Detail    string `json:"detail"`
	RequestID string `json:"request_id"`
}

func proxyRequestID(request *http.Request) string {
	requestIDs := request.Header.Values("X-Request-ID")
	if len(requestIDs) == 1 && validRequestID(requestIDs[0]) {
		return requestIDs[0]
	}
	var value [16]byte
	if _, err := rand.Read(value[:]); err == nil {
		return "req_" + hex.EncodeToString(value[:])
	}
	return "req_" + strconv.FormatInt(time.Now().UnixNano(), 16)
}

func validRequestID(value string) bool {
	if len(value) < 1 || len(value) > 128 {
		return false
	}
	for _, character := range value {
		if character < 0x21 || character > 0x7e {
			return false
		}
	}
	return true
}

func newHandler(apiURL string, logger *slog.Logger) (http.Handler, error) {
	upstream, err := url.Parse(apiURL)
	if err != nil {
		return nil, fmt.Errorf("parse API URL: %w", err)
	}
	if upstream.Scheme != "http" && upstream.Scheme != "https" {
		return nil, fmt.Errorf("API URL scheme must be http or https")
	}
	if upstream.Host == "" || upstream.User != nil || upstream.RawQuery != "" || upstream.Fragment != "" {
		return nil, fmt.Errorf("API URL must contain only scheme, host, and optional base path")
	}
	staticFiles, err := fs.Sub(workbenchweb.Static, "static")
	if err != nil {
		return nil, fmt.Errorf("open embedded workbench assets: %w", err)
	}
	proxy := httputil.NewSingleHostReverseProxy(upstream)
	originalDirector := proxy.Director
	proxy.Director = func(request *http.Request) {
		incomingPath := request.URL.Path
		originalDirector(request)
		request.URL.Path = joinURLPath(upstream.Path, strings.TrimPrefix(incomingPath, "/api"))
		request.URL.RawPath = ""
		request.Host = upstream.Host
		request.Header.Set("X-Qmax-Workbench", "1")
	}
	proxy.ErrorHandler = func(response http.ResponseWriter, request *http.Request, proxyErr error) {
		requestID := proxyRequestID(request)
		logger.Warn("queue API unavailable", "request_id", requestID, "method", request.Method, "route", "api_proxy", "error_type", fmt.Sprintf("%T", proxyErr))
		response.Header().Set("Content-Type", "application/problem+json")
		response.Header().Set("X-Request-ID", requestID)
		response.WriteHeader(http.StatusBadGateway)
		_ = json.NewEncoder(response).Encode(proxyProblem{
			Type: "urn:queuemaxxing:problem:api_unavailable", Title: "Queue API unavailable",
			Status: http.StatusBadGateway, Code: "api_unavailable", Detail: "The queue API is unavailable.", RequestID: requestID,
		})
	}
	mux := http.NewServeMux()
	mux.Handle("/api/", proxy)
	mux.HandleFunc("GET /healthz", func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"status":"ok"}`))
	})
	mux.Handle("GET /assets/", http.StripPrefix("/assets/", cacheStatic(http.FileServer(http.FS(staticFiles)))))
	mux.HandleFunc("/", func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			response.Header().Set("Allow", http.MethodGet)
			http.Error(response, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if request.URL.Path != "/" {
			http.NotFound(response, request)
			return
		}
		response.Header().Set("Cache-Control", "no-store")
		http.ServeFileFS(response, request, staticFiles, "index.html")
	})
	return securityHeaders(mux), nil
}

func joinURLPath(basePath, requestPath string) string {
	return strings.TrimSuffix(basePath, "/") + "/" + strings.TrimPrefix(requestPath, "/")
}

func cacheStatic(next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Cache-Control", "public, max-age=300")
		next.ServeHTTP(response, request)
	})
}

func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Security-Policy", "default-src 'self'; connect-src 'self'; img-src 'self' data:; style-src 'self'; script-src 'self'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'")
		response.Header().Set("Referrer-Policy", "no-referrer")
		response.Header().Set("X-Content-Type-Options", "nosniff")
		response.Header().Set("X-Frame-Options", "DENY")
		next.ServeHTTP(response, request)
	})
}
