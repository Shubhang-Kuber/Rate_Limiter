// Command server is an HTTP server protected by a per-client token-bucket
// rate limiter.
//
//	GET /         rate limited; 200 "hello" or 429 with Retry-After
//	GET /healthz  never rate limited
//
// Usage:
//
//	server -addr :8080 -capacity 10 -rate 5 -idle-ttl 10m -cleanup-interval 0
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"tokenbucket/ratelimiter"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	capacity := flag.Float64("capacity", 10, "bucket capacity (burst size), > 0")
	rate := flag.Float64("rate", 5, "refill rate in tokens per second, >= 0 (0 = fixed allowance)")
	idleTTL := flag.Duration("idle-ttl", 10*time.Minute, "evict a client's bucket after this much inactivity, > 0")
	cleanup := flag.Duration("cleanup-interval", 0, "janitor period, >= 0 (0 = max(idle-ttl/2, 1s))")
	shutdownTimeout := flag.Duration("shutdown-timeout", 10*time.Second, "grace period for in-flight requests on SIGINT/SIGTERM")
	flag.Parse()
	if flag.NArg() > 0 {
		fmt.Fprintf(os.Stderr, "unexpected arguments: %v\n", flag.Args())
		flag.Usage()
		os.Exit(2)
	}

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))

	// No silent defaults: an invalid value is reported with the
	// constructor's own error and the process exits non-zero.
	limiter, err := ratelimiter.NewLimiter(ratelimiter.Config{
		Capacity:        *capacity,
		RefillRate:      *rate,
		IdleTTL:         *idleTTL,
		CleanupInterval: *cleanup,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid configuration: %v\n", err)
		os.Exit(2)
	}
	if *shutdownTimeout <= 0 {
		limiter.Close()
		fmt.Fprintf(os.Stderr, "invalid configuration: -shutdown-timeout must be > 0, got %v\n", *shutdownTimeout)
		os.Exit(2)
	}

	srv := &http.Server{
		Addr:              *addr,
		Handler:           newMux(limiter),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	slog.Info("token-bucket server listening", "addr", *addr, "capacity", *capacity,
		"rate", *rate, "idle_ttl", *idleTTL, "cleanup_interval", *cleanup)
	if err := serve(ctx, srv, limiter, *shutdownTimeout); err != nil {
		slog.Error("server exited with error", "err", err)
		stop()
		os.Exit(1)
	}
	slog.Info("server stopped cleanly")
}

// newMux wires the routes. /healthz is registered outside the middleware so
// that health checks are never rate limited.
func newMux(l *ratelimiter.Limiter) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.Handle("/", rateLimit(l, http.HandlerFunc(hello)))
	return mux
}

// hello is the protected endpoint.
func hello(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte("hello from the token bucket\n"))
}

// serve runs srv until ctx is cancelled (SIGINT/SIGTERM) or the listener
// fails, then shuts down in order: stop accepting and drain in-flight
// requests (srv.Shutdown with a timeout, forced srv.Close if that expires),
// then stop the limiter's janitor. Every error is collected and returned;
// http.ErrServerClosed is the expected result of Shutdown and is not one.
func serve(ctx context.Context, srv *http.Server, l *ratelimiter.Limiter, timeout time.Duration) error {
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()

	select {
	case err := <-errCh:
		// ListenAndServe never returns nil; failing before a signal means
		// the listener could not start (e.g. port in use).
		l.Close()
		return fmt.Errorf("listen: %w", err)
	case <-ctx.Done():
	}
	slog.Info("signal received, shutting down", "timeout", timeout)

	var errs []error
	shCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := srv.Shutdown(shCtx); err != nil {
		errs = append(errs, fmt.Errorf("graceful shutdown: %w", err))
		slog.Warn("graceful shutdown did not finish, forcing close", "err", err)
		if cerr := srv.Close(); cerr != nil {
			errs = append(errs, fmt.Errorf("force close: %w", cerr))
		}
	}
	if err := <-errCh; err != nil && !errors.Is(err, http.ErrServerClosed) {
		errs = append(errs, fmt.Errorf("serve: %w", err))
	}
	// Handlers no longer run, so stopping the janitor last is safe. (The
	// token-bucket Limiter keeps answering after Close anyway.)
	l.Close()
	return errors.Join(errs...)
}
