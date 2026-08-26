package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ElijahUmana/queuemaxxing/api"
	queueclock "github.com/ElijahUmana/queuemaxxing/internal/clock"
	"github.com/ElijahUmana/queuemaxxing/internal/engine"
	"github.com/ElijahUmana/queuemaxxing/internal/journal"
)

const (
	defaultListenAddress   = "127.0.0.1:8080"
	defaultDataDirectory   = "./qmax-data"
	defaultShutdownTimeout = 15 * time.Second
	defaultHealthURL       = "http://127.0.0.1:8080"
	defaultHealthTimeout   = 2 * time.Second
)

type config struct {
	listenAddress   string
	dataDirectory   string
	shutdownTimeout time.Duration
}

type healthConfig struct {
	baseURL string
	timeout time.Duration
}

func main() {
	if err := runCLI(context.Background(), os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		slog.Error("queue command failed", "error", err)
		os.Exit(1)
	}
}

func runCLI(ctx context.Context, args []string) error {
	if len(args) == 0 || strings.HasPrefix(args[0], "-") {
		config, err := parseServeConfig(args)
		if err != nil {
			return err
		}
		return run(ctx, config)
	}

	switch args[0] {
	case "serve":
		config, err := parseServeConfig(args[1:])
		if err != nil {
			return err
		}
		return run(ctx, config)
	case "healthcheck":
		config, err := parseHealthConfig(args[1:])
		if err != nil {
			return err
		}
		return healthcheck(ctx, config)
	case "help":
		printUsage(os.Stdout)
		return nil
	default:
		return fmt.Errorf("unknown command %q; expected serve or healthcheck", args[0])
	}
}

func run(parent context.Context, config config) error {
	store, err := journal.Open(journal.Config{Dir: config.dataDirectory})
	if err != nil {
		return fmt.Errorf("open queue journal: %w", err)
	}
	service, err := engine.New(store, queueclock.Real{}, engine.Options{})
	if err != nil {
		_ = store.Close()
		return fmt.Errorf("open queue engine: %w", err)
	}
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	apiServer, err := api.New(service, api.Options{Logger: logger})
	if err != nil {
		_ = service.Close(context.Background())
		return fmt.Errorf("configure queue API: %w", err)
	}
	httpServer := &http.Server{
		Addr: config.listenAddress, Handler: apiServer,
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 40 * time.Second,
		WriteTimeout: 40 * time.Second, IdleTimeout: 90 * time.Second,
		MaxHeaderBytes: 32 << 10,
	}
	ctx, stop := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	defer stop()
	serveError := make(chan error, 1)
	go func() {
		logger.Info("queue API listening", "address", config.listenAddress, "data_directory", config.dataDirectory)
		serveError <- httpServer.ListenAndServe()
	}()
	select {
	case err := <-serveError:
		if !errors.Is(err, http.ErrServerClosed) {
			_ = service.Close(context.Background())
			return fmt.Errorf("serve queue API: %w", err)
		}
		return nil
	case <-ctx.Done():
		apiServer.SetDraining(true)
		shutdownContext, cancel := context.WithTimeout(context.Background(), config.shutdownTimeout)
		defer cancel()
		if err := httpServer.Shutdown(shutdownContext); err != nil {
			_ = httpServer.Close()
			_ = service.Close(shutdownContext)
			return fmt.Errorf("shut down queue API: %w", err)
		}
		if err := service.Close(shutdownContext); err != nil {
			return fmt.Errorf("close queue service: %w", err)
		}
		return nil
	}
}

func healthcheck(ctx context.Context, config healthConfig) error {
	baseURL, err := url.Parse(config.baseURL)
	if err != nil {
		return fmt.Errorf("parse healthcheck URL: %w", err)
	}
	if baseURL.Scheme != "http" && baseURL.Scheme != "https" {
		return errors.New("healthcheck URL scheme must be http or https")
	}
	if baseURL.Host == "" || baseURL.User != nil || baseURL.RawQuery != "" || baseURL.Fragment != "" {
		return errors.New("healthcheck URL must contain only scheme, host, and optional base path")
	}
	baseURL.Path = strings.TrimSuffix(baseURL.Path, "/") + "/health/ready"
	baseURL.RawPath = ""

	requestContext, cancel := context.WithTimeout(ctx, config.timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(requestContext, http.MethodGet, baseURL.String(), nil)
	if err != nil {
		return fmt.Errorf("create healthcheck request: %w", err)
	}
	response, err := (&http.Client{Timeout: config.timeout}).Do(request)
	if err != nil {
		return fmt.Errorf("request queue readiness: %w", err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 64<<10))
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("queue readiness returned HTTP %d", response.StatusCode)
	}
	return nil
}

func parseServeConfig(args []string) (config, error) {
	var config config
	flags := flag.NewFlagSet("qmax serve", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	flags.StringVar(&config.listenAddress, "listen", envOrDefault("QMAX_LISTEN", defaultListenAddress), "HTTP listen address")
	flags.StringVar(&config.dataDirectory, "data-dir", envOrDefault("QMAX_DATA_DIR", defaultDataDirectory), "durable queue data directory")
	flags.DurationVar(&config.shutdownTimeout, "shutdown-timeout", defaultShutdownTimeout, "graceful shutdown deadline")
	if err := flags.Parse(args); err != nil {
		return config, err
	}
	if flags.NArg() != 0 {
		return config, fmt.Errorf("unexpected serve arguments: %s", strings.Join(flags.Args(), " "))
	}
	if config.listenAddress == "" || config.dataDirectory == "" || config.shutdownTimeout <= 0 {
		return config, errors.New("listen address, data directory, and positive shutdown timeout are required")
	}
	return config, nil
}

func parseHealthConfig(args []string) (healthConfig, error) {
	var config healthConfig
	flags := flag.NewFlagSet("qmax healthcheck", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	flags.StringVar(&config.baseURL, "url", envOrDefault("QMAX_URL", defaultHealthURL), "queue API base URL")
	flags.DurationVar(&config.timeout, "timeout", defaultHealthTimeout, "healthcheck deadline")
	if err := flags.Parse(args); err != nil {
		return config, err
	}
	if flags.NArg() != 0 {
		return config, fmt.Errorf("unexpected healthcheck arguments: %s", strings.Join(flags.Args(), " "))
	}
	if config.timeout <= 0 {
		return config, errors.New("healthcheck timeout must be positive")
	}
	return config, nil
}

func printUsage(writer io.Writer) {
	_, _ = fmt.Fprintln(writer, "Usage:")
	_, _ = fmt.Fprintln(writer, "  qmax serve [--listen address] [--data-dir path]")
	_, _ = fmt.Fprintln(writer, "  qmax healthcheck [--url base-url] [--timeout duration]")
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
