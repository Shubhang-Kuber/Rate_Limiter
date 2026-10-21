package main

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"tokenbucket/ratelimiter"
)

// fakeClock is a manually advanced, goroutine-safe clock (the limiter's
// janitor reads it concurrently with the test).
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{t: time.Unix(1_700_000_000, 0)} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// newTestLimiter builds a Limiter, optionally on a fake clock, closed at test end.
func newTestLimiter(t *testing.T, capacity, rate float64, clk *fakeClock) *ratelimiter.Limiter {
	t.Helper()
	var opts []ratelimiter.Option
	if clk != nil {
		opts = append(opts, ratelimiter.WithClock(clk.Now))
	}
	l, err := ratelimiter.NewLimiter(ratelimiter.Config{
		Capacity:   capacity,
		RefillRate: rate,
		IdleTTL:    time.Hour,
	}, opts...)
	if err != nil {
		t.Fatalf("NewLimiter: %v", err)
	}
	t.Cleanup(l.Close)
	return l
}

// do sends one request through h. remote is RemoteAddr; xff, if non-empty,
// is sent as X-Forwarded-For.
func do(h http.Handler, path, remote, xff string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodGet, path, nil)
	r.RemoteAddr = remote
	if xff != "" {
		r.Header.Set("X-Forwarded-For", xff)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

type want struct {
	code       int
	limit      string
	remaining  string
	retryAfter string // "" means the header must be absent
}

func check(t *testing.T, i int, w *httptest.ResponseRecorder, exp want) {
	t.Helper()
	if w.Code != exp.code {
		t.Errorf("req %d: status = %d, want %d", i, w.Code, exp.code)
	}
	if got := w.Header().Get(headerLimit); got != exp.limit {
		t.Errorf("req %d: %s = %q, want %q", i, headerLimit, got, exp.limit)
	}
	if got := w.Header().Get(headerRemaining); got != exp.remaining {
		t.Errorf("req %d: %s = %q, want %q", i, headerRemaining, got, exp.remaining)
	}
	got, present := w.Header()[headerRetryAfter]
	switch {
	case exp.retryAfter == "" && present:
		t.Errorf("req %d: unexpected %s = %q", i, headerRetryAfter, got)
	case exp.retryAfter != "" && w.Header().Get(headerRetryAfter) != exp.retryAfter:
		t.Errorf("req %d: %s = %q, want %q", i, headerRetryAfter, w.Header().Get(headerRetryAfter), exp.retryAfter)
	}
}

func TestHeadersOnAllowedAndRejected(t *testing.T) {
	clk := newFakeClock()
	h := newMux(newTestLimiter(t, 2, 1, clk))

	steps := []want{
		{code: 200, limit: "2", remaining: "1"},
		{code: 200, limit: "2", remaining: "0"},
		{code: 429, limit: "2", remaining: "0", retryAfter: "1"},
		{code: 429, limit: "2", remaining: "0", retryAfter: "1"},
	}
	for i, exp := range steps {
		w := do(h, "/", "192.0.2.1:1234", "")
		check(t, i, w, exp)
		if exp.code == 200 && w.Body.String() == "" {
			t.Errorf("req %d: empty body on allowed response", i)
		}
	}
	// After a full refill the client is allowed again, with fresh headers.
	clk.Advance(2 * time.Second)
	check(t, 99, do(h, "/", "192.0.2.1:1234", ""), want{code: 200, limit: "2", remaining: "1"})
}

func TestRetryAfterValues(t *testing.T) {
	tests := []struct {
		name     string
		capacity float64
		rate     float64
		advance  time.Duration // clock advance after draining, before the rejected request
		want     string
	}{
		// 1 token deficit at 0.5/s = exactly 2s.
		{"cap1 rate0.5 exact", 1, 0.5, 0, "2"},
		// 1 token at 0.4/s = 2.5s, must round UP to 3 (floor would say 2,
		// and a retry at 2s would be rejected again).
		{"fractional rounds up", 1, 0.4, 0, "3"},
		// After 1.5s the bucket holds 0.75 tokens: 0.25 deficit at 0.5/s =
		// 0.5s left -> 1.
		{"sub-second rounds up to 1", 1, 0.5, 1500 * time.Millisecond, "1"},
		// 1/3s at 3/s -> 0.333s -> 1, never 0.
		{"never zero", 1, 3, 0, "1"},
		// 0.5s into a 2s wait: 1.5s left -> 2.
		{"partial refill", 1, 0.5, 500 * time.Millisecond, "2"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clk := newFakeClock()
			h := newMux(newTestLimiter(t, tc.capacity, tc.rate, clk))
			if w := do(h, "/", "192.0.2.1:1", ""); w.Code != 200 {
				t.Fatalf("first request: status %d", w.Code)
			}
			clk.Advance(tc.advance)
			w := do(h, "/", "192.0.2.1:1", "")
			if w.Code != http.StatusTooManyRequests {
				t.Fatalf("status = %d, want 429", w.Code)
			}
			if got := w.Header().Get(headerRetryAfter); got != tc.want {
				t.Errorf("Retry-After = %q, want %q", got, tc.want)
			}
		})
	}
}

// With the real clock a few microseconds pass between the two requests, so
// the exact wait is a hair under 2s; ceil must still say 2 (floor would say 1).
func TestRetryAfterRealClock(t *testing.T) {
	h := newMux(newTestLimiter(t, 1, 0.5, nil))
	do(h, "/", "192.0.2.1:1", "")
	w := do(h, "/", "192.0.2.1:1", "")
	if w.Code != 429 || w.Header().Get(headerRetryAfter) != "2" {
		t.Fatalf("got %d Retry-After %q, want 429 \"2\"", w.Code, w.Header().Get(headerRetryAfter))
	}
}

// Rate 0: the decision's RetryAfter is RetryNever. The middleware answers 429
// with the X-RateLimit-* headers but no Retry-After at all.
func TestRetryNeverOmitsRetryAfter(t *testing.T) {
	h := newMux(newTestLimiter(t, 1, 0, newFakeClock()))
	check(t, 0, do(h, "/", "192.0.2.1:1", ""), want{code: 200, limit: "1", remaining: "0"})
	for i := 1; i <= 3; i++ {
		check(t, i, do(h, "/", "192.0.2.1:1", ""), want{code: 429, limit: "1", remaining: "0"})
	}
}

func TestRetryAfterSeconds(t *testing.T) {
	tests := []struct {
		d      time.Duration
		want   string
		wantOK bool
	}{
		{ratelimiter.RetryNever, "", false},
		{-5 * time.Second, "", false},
		{0, "", false},
		{1, "1", true},
		{300 * time.Millisecond, "1", true},
		{999 * time.Millisecond, "1", true},
		{time.Second, "1", true},
		{time.Second + 1, "2", true},
		{2 * time.Second, "2", true},
		{2500 * time.Millisecond, "3", true},
		{time.Hour, "3600", true},
		{time.Duration(1<<63 - 1), "9223372037", true},
	}
	for _, tc := range tests {
		got, ok := retryAfterSeconds(tc.d)
		if got != tc.want || ok != tc.wantOK {
			t.Errorf("retryAfterSeconds(%v) = (%q, %v), want (%q, %v)", tc.d, got, ok, tc.want, tc.wantOK)
		}
	}
}

func TestHealthzNeverLimited(t *testing.T) {
	h := newMux(newTestLimiter(t, 1, 0, newFakeClock()))
	do(h, "/", "192.0.2.1:1", "")
	if w := do(h, "/", "192.0.2.1:1", ""); w.Code != 429 {
		t.Fatalf("limit not exhausted: status %d", w.Code)
	}
	for i := 0; i < 50; i++ {
		w := do(h, "/healthz", "192.0.2.1:1", "")
		if w.Code != 200 {
			t.Fatalf("/healthz #%d: status %d, want 200", i, w.Code)
		}
		if w.Header().Get(headerLimit) != "" || w.Header().Get(headerRemaining) != "" {
			t.Fatalf("/healthz carries rate-limit headers: %v", w.Header())
		}
	}
	// And /healthz did not consume or reset anything.
	if w := do(h, "/", "192.0.2.1:1", ""); w.Code != 429 {
		t.Fatalf("after healthz: status %d, want 429", w.Code)
	}
}

func TestClientKey(t *testing.T) {
	tests := []struct {
		name   string
		remote string
		xff    []string // header lines
		want   string
	}{
		{"remote ipv4", "203.0.113.7:5555", nil, "203.0.113.7"},
		{"remote ipv6", "[2001:db8::1]:443", nil, "2001:db8::1"},
		{"remote ipv6 zone", "[fe80::1%eth0]:80", nil, "fe80::1%eth0"},
		{"remote without port falls back to raw", "203.0.113.7", nil, "203.0.113.7"},
		{"remote garbage falls back to raw", "not an address", nil, "not an address"},
		{"xff single", "10.0.0.1:1", []string{"198.51.100.1"}, "198.51.100.1"},
		{"xff leftmost wins", "10.0.0.1:1", []string{"198.51.100.1, 10.1.1.1, 10.2.2.2"}, "198.51.100.1"},
		{"xff trimmed", "10.0.0.1:1", []string{"   198.51.100.1  ,10.1.1.1"}, "198.51.100.1"},
		{"xff ipv6", "10.0.0.1:1", []string{"2001:db8::42, 10.1.1.1"}, "2001:db8::42"},
		{"xff first header line wins", "10.0.0.1:1", []string{"198.51.100.1", "198.51.100.2"}, "198.51.100.1"},
		{"xff whitespace only falls back", "10.0.0.1:1", []string{"   "}, "10.0.0.1"},
		{"xff empty leftmost falls back", "10.0.0.1:1", []string{" , 198.51.100.9"}, "10.0.0.1"},
		{"xff empty value falls back", "10.0.0.1:1", []string{""}, "10.0.0.1"},
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

func TestPerKeyIsolation(t *testing.T) {
	h := newMux(newTestLimiter(t, 1, 0, newFakeClock()))

	// Client A (via XFF) spends its single token.
	if w := do(h, "/", "10.0.0.1:1", "198.51.100.1"); w.Code != 200 {
		t.Fatalf("A first: %d", w.Code)
	}
	if w := do(h, "/", "10.0.0.1:1", "198.51.100.1"); w.Code != 429 {
		t.Fatalf("A second: %d, want 429", w.Code)
	}
	// Client B behind the same proxy has its own bucket.
	if w := do(h, "/", "10.0.0.1:1", "198.51.100.2"); w.Code != 200 {
		t.Fatalf("B: %d, want 200", w.Code)
	}
	// The proxy itself (no XFF) is keyed by RemoteAddr host: separate again,
	// and the source port does not matter.
	if w := do(h, "/", "10.0.0.1:1", ""); w.Code != 200 {
		t.Fatalf("proxy: %d, want 200", w.Code)
	}
	if w := do(h, "/", "10.0.0.1:2", ""); w.Code != 429 {
		t.Fatalf("proxy other port: %d, want 429 (same host key)", w.Code)
	}
	// IPv6 client.
	if w := do(h, "/", "[2001:db8::1]:9", ""); w.Code != 200 {
		t.Fatalf("ipv6: %d, want 200", w.Code)
	}
}

// Concurrent requests through a real HTTP server: exactly capacity succeed.
func TestConcurrentOverHTTP(t *testing.T) {
	srv := httptest.NewServer(newMux(newTestLimiter(t, 5, 0, newFakeClock())))
	defer srv.Close()

	const n = 20
	var mu sync.Mutex
	codes := map[int]int{}
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, _ := http.NewRequest(http.MethodGet, srv.URL+"/", nil)
			req.Header.Set("X-Forwarded-For", "198.51.100.77")
			resp, err := srv.Client().Do(req)
			if err != nil {
				t.Error(err)
				return
			}
			resp.Body.Close()
			if resp.Header.Get(headerLimit) != "5" || resp.Header.Get(headerRemaining) == "" {
				t.Errorf("missing headers: %v", resp.Header)
			}
			mu.Lock()
			codes[resp.StatusCode]++
			mu.Unlock()
		}()
	}
	wg.Wait()
	if codes[200] != 5 || codes[429] != n-5 {
		t.Fatalf("codes = %v, want 5x200 and %dx429", codes, n-5)
	}
}
