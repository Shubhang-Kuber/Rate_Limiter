package main

import (
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"tokenbucket/ratelimiter"
)

// Response headers set on every rate-limited (protected) response.
const (
	headerLimit      = "X-RateLimit-Limit"
	headerRemaining  = "X-RateLimit-Remaining"
	headerRetryAfter = "Retry-After"
)

// allower is the part of *ratelimiter.Limiter the middleware needs.
type allower interface {
	Allow(key string) ratelimiter.Decision
}

// rateLimit wraps next with a per-client token-bucket check.
//
// Every response it produces, allowed or rejected, carries X-RateLimit-Limit
// and X-RateLimit-Remaining from the Decision. A rejected request gets 429.
// Its Retry-After header comes from the Decision's actual RetryAfter,
// rounded up to whole seconds (see retryAfterSeconds).
//
// RetryNever: if the decision says the request can never succeed by waiting
// (refill rate 0, so the allowance is spent for good until the idle bucket is
// evicted), the response is still 429 but has NO Retry-After header. Any
// number we put there would be a lie that invites clients to retry in a loop.
// RFC 9110 makes Retry-After optional, and its absence tells a well-behaved
// client "no promise about when", which is exactly the truth here.
func rateLimit(l allower, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		d := l.Allow(clientKey(r))

		h := w.Header()
		h.Set(headerLimit, strconv.Itoa(d.Limit))
		h.Set(headerRemaining, strconv.Itoa(d.Remaining))

		if d.Allowed {
			next.ServeHTTP(w, r)
			return
		}
		if d.RetryAfter != ratelimiter.RetryNever {
			if v, ok := retryAfterSeconds(d.RetryAfter); ok {
				h.Set(headerRetryAfter, v)
			}
		}
		http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
	})
}

// retryAfterSeconds converts a wait duration into a Retry-After value.
//
// Retry-After in its delta-seconds form must be a whole number of seconds,
// so the exact duration has to be rounded. We round UP (ceil), and any
// positive duration yields at least 1. Rounding down would tell a client to
// come back before the bucket has the token: a 2.5s wait floored to "2"
// guarantees the retry is rejected again, and a 300ms wait floored to "0"
// invites an immediate retry storm. Ceil means a client that obeys the header
// is never early. The cost is waiting at most one second longer than needed.
//
// It returns ok == false for d <= 0 (nothing meaningful to advertise; this
// includes the RetryNever sentinel, which is negative).
func retryAfterSeconds(d time.Duration) (string, bool) {
	if d <= 0 {
		return "", false
	}
	// Integer ceil, no float rounding and no overflow: d/time.Second is at
	// most ~9.2e9, and we add at most 1.
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
// entirely, because every request lands in a fresh, full bucket, and (2)
// creates an unbounded number of keys, each costing a bucket until the
// janitor evicts it after IdleTTL. This is a known, deliberate choice for
// deployments behind a trusted proxy. To run without one, key on RemoteAddr
// only.
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
