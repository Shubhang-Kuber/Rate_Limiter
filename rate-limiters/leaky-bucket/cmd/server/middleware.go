package main

import (
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"leakybucket/ratelimiter"
)

// Response headers set on every rate-limited (protected) response.
const (
	headerLimit      = "X-RateLimit-Limit"     // the per-client queue capacity
	headerRemaining  = "X-RateLimit-Remaining" // free queue slots after this decision
	headerRetryAfter = "Retry-After"
)

// queuer is the part of *ratelimiter.Limiter the middleware needs.
type queuer interface {
	Submit(key string) (ratelimiter.Decision, <-chan struct{})
	Closed() bool
}

// leakyBucket wraps next with per-client leaky-bucket SHAPING, not just
// counting. Each request:
//
//  1. Submit: try to take a place in the client's bounded FIFO queue. This
//     never blocks.
//     - Queue full: 429 at once, with Retry-After from the Decision.
//     - Limiter closed (shutting down; Submit also reports Allowed=false):
//     503, since this is not the client's fault and retrying here is
//     pointless.
//  2. Queued: set the headers, then BLOCK until the client's worker dequeues
//     the ticket. Workers dequeue one request per interval (1/rate), so
//     however bursty the arrivals, requests reach next at the fixed outflow
//     rate. This wait is the algorithm; it is not an error path.
//  3. If the client goes away while waiting (request context done), return
//     without calling next and without writing: nobody is there to read it.
//     The queue slot stays occupied until the worker reaches it, which is
//     textbook leaky-bucket behaviour (the queue holds requests, not callers).
//  4. If the ticket fired but the limiter is now closed, the ticket was
//     released by Close during shutdown, not by the worker: answer 503
//     instead of serving, so shutdown does not suddenly let the whole
//     backlog through at once. The limiter sets its closed flag before it
//     releases any ticket, so a ticket released by Close always sees
//     Closed() == true here. (The converse race, a ticket dequeued normally
//     an instant before Close, also gets 503. That is harmless at shutdown.)
//
// X-RateLimit-Limit and X-RateLimit-Remaining are set on every response this
// middleware produces: 200, 429 and 503.
func leakyBucket(l queuer, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := clientKey(r)
		d, ticket := l.Submit(key)

		h := w.Header()
		h.Set(headerLimit, strconv.Itoa(d.Limit))
		h.Set(headerRemaining, strconv.Itoa(d.Remaining))

		if !d.Allowed {
			if l.Closed() {
				http.Error(w, "server shutting down", http.StatusServiceUnavailable)
				return
			}
			if v, ok := retryAfterSeconds(d.RetryAfter); ok {
				h.Set(headerRetryAfter, v)
			}
			http.Error(w, "rate limit exceeded: queue full", http.StatusTooManyRequests)
			return
		}

		// Queued: wait for our turn at the fixed outflow rate.
		if err := ratelimiter.WaitTicket(r.Context(), ticket); err != nil {
			// Client disconnected (or its context was cancelled). Writing a
			// response would go nowhere; just stop. net/http finishes the
			// dead exchange itself.
			slog.Debug("client gave up while queued", "key", key, "err", err)
			return
		}
		if l.Closed() {
			http.Error(w, "server shutting down", http.StatusServiceUnavailable)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// retryAfterSeconds converts a wait duration into a Retry-After value.
//
// Retry-After in its delta-seconds form must be a whole number of seconds,
// so the exact duration has to be rounded. We round UP (ceil), and any
// positive duration yields at least 1. Rounding down would tell a client to
// come back before a queue slot has freed: with a 400ms outflow interval the
// next dequeue is under a second away, and floor would say "0", inviting an
// immediate retry that is rejected again. Ceil means a client that obeys the
// header is never early. The cost is waiting at most one second longer than
// needed.
//
// It returns ok == false for d <= 0 (nothing meaningful to advertise; the
// limiter reports 0 when it is closed).
func retryAfterSeconds(d time.Duration) (string, bool) {
	if d <= 0 {
		return "", false
	}
	// Integer ceil, no float rounding and no overflow.
	secs := int64(d / time.Second)
	if d%time.Second != 0 {
		secs++
	}
	return strconv.FormatInt(secs, 10), true
}

// clientKey returns the rate-limit key for r.
//
// It is the first (leftmost) entry of X-Forwarded-For, trimmed, when that
// entry is present and non-empty. Otherwise it is the host part of
// r.RemoteAddr (IPv4 or IPv6, port stripped), or the raw RemoteAddr if it
// cannot be split.
//
// SECURITY TRADEOFF: X-Forwarded-For is an ordinary request header, written
// by the client, so it is trivially spoofable. It is only trustworthy when a
// reverse proxy that we control sits in front of this server and overwrites
// (not appends to) the header. Exposed directly to the internet, an attacker
// can rotate fake XFF values on every request, which (1) dodges the limit
// entirely, because every request lands in a fresh, empty queue, and (2)
// creates an unbounded number of keys. For a leaky bucket that is
// especially costly: every key is a live worker goroutine, a ticker and a
// queue buffer until it retires after IdleTTL. This is a known, deliberate
// choice for deployments behind a trusted proxy. To run without one, key on
// RemoteAddr only.
func clientKey(r *http.Request) string {
	// Header.Get returns the first XFF header line; its first comma-separated
	// element is the leftmost (original client) entry.
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		first, _, _ := strings.Cut(xff, ",")
		if k := strings.TrimSpace(first); k != "" {
			return k
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
