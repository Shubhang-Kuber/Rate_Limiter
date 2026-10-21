package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"leakybucket/ratelimiter"
)

func newTestLimiter(t *testing.T, queue int, rate float64) *ratelimiter.Limiter {
	t.Helper()
	l, err := ratelimiter.NewLimiter(ratelimiter.Config{
		QueueCapacity: queue,
		OutflowRate:   rate,
		IdleTTL:       time.Minute,
	})
	if err != nil {
		t.Fatalf("NewLimiter: %v", err)
	}
	t.Cleanup(l.Close)
	return l
}

// countingHandler is a protected handler that records how often it ran.
type countingHandler struct{ n atomic.Int64 }

func (c *countingHandler) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	c.n.Add(1)
	_, _ = w.Write([]byte("served\n"))
}

// signalingQueuer wraps a real limiter and reports each completed Submit, so
// tests know a request is in the queue (and not merely about to Submit).
type signalingQueuer struct {
	*ratelimiter.Limiter
	submitted chan ratelimiter.Decision
}

func (s *signalingQueuer) Submit(key string) (ratelimiter.Decision, <-chan struct{}) {
	d, t := s.Limiter.Submit(key)
	s.submitted <- d
	return d, t
}

func newRequest(ctx context.Context, path, remote, xff string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, path, nil).WithContext(ctx)
	r.RemoteAddr = remote
	if xff != "" {
		r.Header.Set("X-Forwarded-For", xff)
	}
	return r
}

func do(h http.Handler, path, remote, xff string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	h.ServeHTTP(w, newRequest(context.Background(), path, remote, xff))
	return w
}

func requireHeaders(t *testing.T, w *httptest.ResponseRecorder, limit, remaining string) {
	t.Helper()
	if got := w.Header().Get(headerLimit); got != limit {
		t.Errorf("%s = %q, want %q", headerLimit, got, limit)
	}
	if got := w.Header().Get(headerRemaining); got != remaining {
		t.Errorf("%s = %q, want %q", headerRemaining, got, remaining)
	}
}

func TestHeadersOnAllowedResponse(t *testing.T) {
	next := &countingHandler{}
	h := leakyBucket(newTestLimiter(t, 3, 100), next)

	w := do(h, "/", "192.0.2.1:1", "")
	if w.Code != 200 || next.n.Load() != 1 {
		t.Fatalf("status %d, served %d; want 200, 1", w.Code, next.n.Load())
	}
	requireHeaders(t, w, "3", "2") // we held 1 of 3 slots
	if w.Header().Get(headerRetryAfter) != "" {
		t.Errorf("Retry-After on allowed response")
	}
}

// Queue full: 429 immediately (no waiting for a slot), with headers and a
// Retry-After derived from the time to the next tick.
func TestQueueFullRejectsImmediately(t *testing.T) {
	l := newTestLimiter(t, 2, 0.25) // 4s interval: nothing dequeues during the test
	next := &countingHandler{}
	h := leakyBucket(l, next)

	// Fill the client's queue directly; these tickets are released by
	// l.Close at cleanup.
	for i := 0; i < 2; i++ {
		if d, _ := l.Submit("192.0.2.1"); !d.Allowed {
			t.Fatalf("fill %d rejected", i)
		}
	}

	start := time.Now()
	w := do(h, "/", "192.0.2.1:1", "")
	elapsed := time.Since(start)

	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("status %d, want 429", w.Code)
	}
	if elapsed > time.Second {
		t.Errorf("rejection took %v; must not wait", elapsed)
	}
	requireHeaders(t, w, "2", "0")
	ra, err := strconv.Atoi(w.Header().Get(headerRetryAfter))
	// Time to the next tick is in (0, 4s], so ceil gives 1..4.
	if err != nil || ra < 1 || ra > 4 {
		t.Errorf("Retry-After = %q, want an integer in [1,4]", w.Header().Get(headerRetryAfter))
	}
	if next.n.Load() != 0 {
		t.Errorf("next called on rejection")
	}
}

func TestRetryAfterSeconds(t *testing.T) {
	tests := []struct {
		d      time.Duration
		want   string
		wantOK bool
	}{
		{-1, "", false},
		{0, "", false},
		{1, "1", true},
		{400 * time.Millisecond, "1", true},
		{time.Second, "1", true},
		{time.Second + 1, "2", true},
		{2500 * time.Millisecond, "3", true},
		{4 * time.Second, "4", true},
		{time.Duration(1<<63 - 1), "9223372037", true},
	}
	for _, tc := range tests {
		got, ok := retryAfterSeconds(tc.d)
		if got != tc.want || ok != tc.wantOK {
			t.Errorf("retryAfterSeconds(%v) = (%q, %v), want (%q, %v)", tc.d, got, ok, tc.want, tc.wantOK)
		}
	}
}

// Allowed requests are SHAPED: they are delayed until the worker dequeues
// them, one per interval, in FIFO order.
func TestAllowedRequestsAreDelayedAtOutflowRate(t *testing.T) {
	const interval = 100 * time.Millisecond
	l := newTestLimiter(t, 3, 10) // 100ms interval
	sq := &signalingQueuer{Limiter: l, submitted: make(chan ratelimiter.Decision, 3)}
	next := &countingHandler{}
	h := leakyBucket(sq, next)

	start := time.Now()
	done := make([]time.Duration, 3)
	codes := make([]int, 3)
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			w := do(h, "/", "192.0.2.1:1", "")
			codes[i] = w.Code
			done[i] = time.Since(start)
		}(i)
		<-sq.submitted // serialize submission so request i holds position i+1
	}
	wg.Wait()

	for i := range codes {
		if codes[i] != 200 {
			t.Fatalf("request %d: status %d", i, codes[i])
		}
	}
	// Request at position p is released at about p*interval after the bucket
	// was created. Tolerant bounds: never early by more than scheduling
	// slop, and generous on the late side for -race and busy CI.
	for i, d := range done {
		lo := time.Duration(i+1)*interval - 30*time.Millisecond
		if d < lo {
			t.Errorf("request %d finished after %v, want >= %v (not shaped)", i, d, lo)
		}
		if d > 3*time.Second {
			t.Errorf("request %d finished after %v, far too late", i, d)
		}
	}
	// Consecutive releases are about one interval apart.
	if gap := done[2] - done[0]; gap < 2*interval-40*time.Millisecond {
		t.Errorf("first-to-third gap %v, want ~%v", gap, 2*interval)
	}
	if next.n.Load() != 3 {
		t.Errorf("served %d, want 3", next.n.Load())
	}
}

// A client that disconnects while queued: the handler returns promptly,
// next is never called, and nothing hangs.
func TestClientCancelWhileQueued(t *testing.T) {
	l := newTestLimiter(t, 2, 0.2) // 5s interval
	sq := &signalingQueuer{Limiter: l, submitted: make(chan ratelimiter.Decision, 1)}
	next := &countingHandler{}
	h := leakyBucket(sq, next)

	ctx, cancel := context.WithCancel(context.Background())
	returned := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, newRequest(ctx, "/", "192.0.2.1:1", ""))
		returned <- w
	}()
	if d := <-sq.submitted; !d.Allowed {
		t.Fatalf("request was not queued")
	}
	cancel()

	select {
	case w := <-returned:
		if next.n.Load() != 0 {
			t.Errorf("next called for a cancelled request")
		}
		if w.Body.Len() != 0 {
			t.Errorf("wrote a body to a gone client: %q", w.Body.String())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not return after client cancel")
	}
}

// Close while requests are waiting on tickets: each one is released and
// answers 503 instead of being served.
func TestCloseReleasesWaitersWith503(t *testing.T) {
	l, err := ratelimiter.NewLimiter(ratelimiter.Config{QueueCapacity: 3, OutflowRate: 0.2, IdleTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	sq := &signalingQueuer{Limiter: l, submitted: make(chan ratelimiter.Decision, 3)}
	next := &countingHandler{}
	h := leakyBucket(sq, next)

	results := make(chan *httptest.ResponseRecorder, 3)
	for i := 0; i < 3; i++ {
		go func() { results <- do(h, "/", "192.0.2.1:1", "") }()
		if d := <-sq.submitted; !d.Allowed {
			t.Fatalf("request %d not queued", i)
		}
	}

	closed := make(chan struct{})
	go func() { l.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("limiter.Close hung with waiting handlers (deadlock)")
	}

	for i := 0; i < 3; i++ {
		select {
		case w := <-results:
			if w.Code != http.StatusServiceUnavailable {
				t.Errorf("waiter %d: status %d, want 503", i, w.Code)
			}
			if w.Header().Get(headerLimit) != "3" || w.Header().Get(headerRemaining) == "" {
				t.Errorf("waiter %d: missing rate-limit headers: %v", i, w.Header())
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("waiter %d not released by Close", i)
		}
	}
	if next.n.Load() != 0 {
		t.Errorf("next called %d times after Close", next.n.Load())
	}
}

// A request arriving after Close is rejected by Submit; that is 503, not 429.
func TestClosedLimiterRejectsWith503(t *testing.T) {
	l := newTestLimiter(t, 2, 10)
	l.Close()
	w := do(leakyBucket(l, &countingHandler{}), "/", "192.0.2.1:1", "")
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status %d, want 503", w.Code)
	}
	requireHeaders(t, w, "2", "0")
	if w.Header().Get(headerRetryAfter) != "" {
		t.Errorf("unexpected Retry-After on 503: %q", w.Header().Get(headerRetryAfter))
	}
}

func TestHealthzNeverLimited(t *testing.T) {
	l := newTestLimiter(t, 1, 0.2) // 5s interval
	h := newMux(l)
	if d, _ := l.Submit("192.0.2.1"); !d.Allowed { // fill the only slot
		t.Fatal("fill rejected")
	}
	if w := do(h, "/", "192.0.2.1:1", ""); w.Code != 429 {
		t.Fatalf("/ status %d, want 429 (queue full)", w.Code)
	}
	for i := 0; i < 20; i++ {
		start := time.Now()
		w := do(h, "/healthz", "192.0.2.1:1", "")
		if w.Code != 200 || w.Body.String() != "ok\n" {
			t.Fatalf("/healthz #%d: %d %q", i, w.Code, w.Body.String())
		}
		if time.Since(start) > 500*time.Millisecond {
			t.Fatalf("/healthz was queued")
		}
		if w.Header().Get(headerLimit) != "" {
			t.Fatalf("/healthz has rate-limit headers")
		}
	}
	if w := do(h, "/", "192.0.2.1:1", ""); w.Code != 429 {
		t.Fatalf("after healthz: %d, want 429", w.Code)
	}
}

func TestClientKey(t *testing.T) {
	tests := []struct {
		name   string
		remote string
		xff    []string
		want   string
	}{
		{"remote ipv4", "203.0.113.7:5555", nil, "203.0.113.7"},
		{"remote ipv6", "[2001:db8::1]:443", nil, "2001:db8::1"},
		{"remote without port falls back to raw", "203.0.113.7", nil, "203.0.113.7"},
		{"remote garbage falls back to raw", "not an address", nil, "not an address"},
		{"xff leftmost wins", "10.0.0.1:1", []string{"198.51.100.1, 10.1.1.1, 10.2.2.2"}, "198.51.100.1"},
		{"xff trimmed", "10.0.0.1:1", []string{"\t 198.51.100.1  ,10.1.1.1"}, "198.51.100.1"},
		{"xff ipv6", "10.0.0.1:1", []string{"2001:db8::42"}, "2001:db8::42"},
		{"xff first header line wins", "10.0.0.1:1", []string{"198.51.100.1", "198.51.100.2"}, "198.51.100.1"},
		{"xff whitespace only falls back", "10.0.0.1:1", []string{"  "}, "10.0.0.1"},
		{"xff empty leftmost falls back", "[2001:db8::9]:1", []string{",198.51.100.9"}, "2001:db8::9"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			r.RemoteAddr = tc.remote
			for _, v := range tc.xff {
				r.Header.Add("X-Forwarded-For", v)
			}
			if got := clientKey(r); got != tc.want {
				t.Errorf("clientKey = %q, want %q", got, tc.want)
			}
		})
	}
}

// One client's full queue does not affect another client, through HTTP.
func TestPerKeyIsolation(t *testing.T) {
	l := newTestLimiter(t, 1, 1) // 1s interval: A's slot stays full while we probe
	srv := httptest.NewServer(newMux(l))
	defer srv.Close()

	get := func(xff string) *http.Response {
		t.Helper()
		req, _ := http.NewRequest(http.MethodGet, srv.URL+"/", nil)
		req.Header.Set("X-Forwarded-For", xff)
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp
	}

	// Fill client A's single slot directly, then A over HTTP is rejected...
	if d, _ := l.Submit("198.51.100.1"); !d.Allowed {
		t.Fatal("fill rejected")
	}
	if resp := get("198.51.100.1"); resp.StatusCode != 429 {
		t.Fatalf("A: %d, want 429", resp.StatusCode)
	}
	// ...while client B has its own queue and is served.
	resp := get("198.51.100.2")
	if resp.StatusCode != 200 {
		t.Fatalf("B: %d, want 200", resp.StatusCode)
	}
	if resp.Header.Get(headerLimit) != "1" || resp.Header.Get(headerRemaining) != "0" {
		t.Errorf("B headers: %v", resp.Header)
	}
}

func TestWriteTimeoutExceedsMaxQueueWait(t *testing.T) {
	if got, maxWait := writeTimeout(10, 500*time.Millisecond), 5*time.Second; got <= maxWait {
		t.Errorf("writeTimeout = %v, want > %v", got, maxWait)
	}
	if got := writeTimeout(1000, time.Duration(1<<62)); got != 0 {
		t.Errorf("overflowing writeTimeout = %v, want 0 (unset)", got)
	}
}
