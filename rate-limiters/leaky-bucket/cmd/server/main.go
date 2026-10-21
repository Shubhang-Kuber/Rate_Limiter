// Command server is an HTTP server whose requests are shaped by a per-client
// leaky bucket: each client's requests wait in a bounded FIFO queue and are
// released to the handler at a fixed outflow rate. A full queue gets 429.
//
//	GET /         queued, then served at the outflow rate; 429 if queue full
//	GET /healthz  never rate limited
//
// Usage:
//
//	server -addr :8080 -queue 10 -rate 5 -idle-ttl 1m
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"leakybucket/ratelimiter"
)

// handlerBudget is the time allowed for writing a response once a request
// leaves the queue. It is added on top of the maximum queue wait.
const handlerBudget = 10 * time.Second

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	queue := flag.Int("queue", 10, "per-client queue capacity, >= 1")
	rate := flag.Float64("rate", 5, "outflow rate in requests per second per client, > 0")
	idleTTL := flag.Duration("idle-ttl", time.Minute, "retire a client's worker after its queue is empty this long, > 0")
	shutdownTimeout := flag.Duration("shutdown-timeout", 10*time.Second, "grace period for queued/in-flight requests on SIGINT/SIGTERM")
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
		QueueCapacity: *queue,
		OutflowRate:   *rate,
		IdleTTL:       *idleTTL,
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
		WriteTimeout:      writeTimeout(*queue, limiter.Interval()),
		IdleTimeout:       60 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	slog.Info("leaky-bucket server listening", "addr", *addr, "queue", *queue, "rate", *rate,
		"interval", limiter.Interval(), "idle_ttl", *idleTTL, "write_timeout", srv.WriteTimeout)
	if err := serve(ctx, srv, limiter, *shutdownTimeout); err != nil {
		slog.Error("server exited with error", "err", err)
		stop()
		os.Exit(1)
	}
	slog.Info("server stopped cleanly")
}

// writeTimeout returns the server's WriteTimeout. In net/http the write
// deadline is armed when the request headers have been read, so it also
// covers the time a request spends WAITING IN THE QUEUE. The longest wait is
// queue × interval (up to one interval to the next tick, then one interval
// per request ahead of us), so the timeout must exceed that, or the very
// requests the limiter accepted would have their responses cut off. We use
// maxWait + handlerBudget. If that does not fit a time.Duration (absurdly
// slow rates), it returns 0, meaning no write timeout: ReadHeaderTimeout
// and IdleTimeout still bound idle and slow-header connections.
func writeTimeout(queue int, interval time.Duration) time.Duration {
	if queue <= 0 || interval <= 0 {
		return handlerBudget
	}
	if interval > (math.MaxInt64-handlerBudget)/time.Duration(queue) {
		return 0
	}
	return time.Duration(queue)*interval + handlerBudget
}

// newMux wires the routes. /healthz is registered outside the middleware so
// that health checks are never queued or rate limited.
func newMux(l queuer) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.Handle("/", leakyBucket(l, http.HandlerFunc(hello)))
	return mux
}

// hello is the protected endpoint.
func hello(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte("hello from the leaky bucket\n"))
}

// serve runs srv until ctx is cancelled (SIGINT/SIGTERM) or the listener
// fails, then shuts down. Ordering matters because in-flight handlers may
// be blocked waiting for their queue tickets:
//
//  1. srv.Shutdown(timeout) stops accepting connections and waits for active
//     requests. The limiter is still running, so queued requests keep
//     draining at the outflow rate and are served normally.
//  2. If that times out, requests are still queued. limiter.Close() releases
//     every waiting ticket. Each handler wakes, sees Closed() == true and
//     answers 503. Then a second, short Shutdown waits for those 503s to be
//     written. Only if that also expires do we force srv.Close().
//  3. On a clean step 1, no handler is waiting any more, and limiter.Close()
//     just stops the workers (and releases tickets abandoned by clients
//     that disconnected).
//
// This cannot deadlock. Close never waits on a handler: it marks the limiter
// closed, releases tickets with close() (which never blocks), and waits only
// for its own worker goroutines. A handler never holds a limiter lock while
// waiting on a ticket. So a handler blocked in WaitTicket is always
// released by Close, and Shutdown's wait always ends.
//
// Every error is collected and returned. http.ErrServerClosed is the
// expected result of Shutdown and is not one.
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
	limiterClosed := false

	shCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := srv.Shutdown(shCtx); err != nil {
		errs = append(errs, fmt.Errorf("graceful shutdown: %w", err))
		slog.Warn("graceful shutdown timed out; releasing queued requests with 503", "err", err)

		l.Close() // releases every waiting ticket; handlers answer 503
		limiterClosed = true

		graceCtx, cancelGrace := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelGrace()
		if err := srv.Shutdown(graceCtx); err != nil {
			errs = append(errs, fmt.Errorf("shutdown after releasing queue: %w", err))
			slog.Warn("handlers still active after releasing queue, forcing close", "err", err)
			if cerr := srv.Close(); cerr != nil {
				errs = append(errs, fmt.Errorf("force close: %w", cerr))
			}
		}
	}
	if err := <-errCh; err != nil && !errors.Is(err, http.ErrServerClosed) {
		errs = append(errs, fmt.Errorf("serve: %w", err))
	}
	if !limiterClosed {
		l.Close()
	}
	return errors.Join(errs...)
}
