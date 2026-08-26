package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
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
	listenAddress string
	apiURL        string
}

func main() {
	cfg := parseConfig()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	handler, err := newHandler(cfg.apiURL, logger)
	if err != nil {
		logger.Error("configure workbench", "error", err)
		os.Exit(1)
	}

	server := &http.Server{
		Addr:              cfg.listenAddress,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      65 * time.Second,
		IdleTimeout:       90 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		logger.Info("queue workbench listening", "address", cfg.listenAddress, "api", cfg.apiURL)
		errCh <- server.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			logger.Error("serve workbench", "error", err)
			os.Exit(1)
		}
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Error("shut down workbench", "error", err)
			os.Exit(1)
		}
	}
}

func parseConfig() config {
	var cfg config
	flag.StringVar(&cfg.listenAddress, "listen", envOrDefault("QMAX_WORKBENCH_LISTEN", defaultListenAddress), "workbench listen address")
	flag.StringVar(&cfg.apiURL, "api-url", envOrDefault("QMAX_API_URL", defaultAPIURL), "qmax public API base URL")
	flag.Parse()
	return cfg
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
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
		logger.Warn("queue API unavailable", "method", request.Method, "path", request.URL.Path, "error", proxyErr)
		response.Header().Set("Content-Type", "application/json")
		response.WriteHeader(http.StatusBadGateway)
		_, _ = response.Write([]byte(`{"error":{"code":"api_unavailable","message":"The queue API is unavailable."}}`))
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
