package ratelimiter

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"runtime"
	"sync"
	"testing"
	"time"
)

// Tests are deliberately not t.Parallel(): several of them assert that
// runtime.NumGoroutine returns to a baseline.

func mustLimiter(t *testing.T, cfg Config) *Limiter {
	t.Helper()
	l, err := NewLimiter(cfg)
	if err != nil {
		t.Fatalf("NewLimiter(%+v): %v", cfg, err)
	}
	return l
}

// waitReleased fails the test if ticket is not released within d.
func waitReleased(t *testing.T, ticket <-chan struct{}, d time.Duration, what string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	if err := WaitTicket(ctx, ticket); err != nil {
		t.Fatalf("%s: ticket not released within %v: %v", what, d, err)
	}
}

// waitGoroutines polls until NumGoroutine <= base or the timeout expires.
func waitGoroutines(t *testing.T, base int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		n := runtime.NumGoroutine()
		if n <= base {
			return
		}
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<16)
			buf = buf[:runtime.Stack(buf, true)]
			t.Fatalf("goroutines did not return to baseline: have %d, want <= %d\n%s", n, base, buf)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", timeout, what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestAcceptUpToCapacity(t *testing.T) {
	l := mustLimiter(t, Config{QueueCapacity: 5, OutflowRate: 1, IdleTTL: time.Minute})
	defer l.Close()

	var prevWait time.Duration
	for i := 0; i < 5; i++ {
		d, tk := l.Submit("k")
		if !d.Allowed || tk == nil {
			t.Fatalf("request %d: want allowed with ticket, got %+v (ticket nil=%v)", i, d, tk == nil)
		}
		if d.Limit != 5 || d.Remaining != 5-(i+1) {
			t.Fatalf("request %d: Limit=%d Remaining=%d", i, d.Limit, d.Remaining)
		}
		if d.RetryAfter != 0 {
			t.Fatalf("request %d: RetryAfter=%v on allowed", i, d.RetryAfter)
		}
		if d.Wait <= prevWait {
			t.Fatalf("request %d: Wait %v not > previous %v", i, d.Wait, prevWait)
		}
		prevWait = d.Wait
	}
	d, tk := l.Submit("k")
	if d.Allowed || tk != nil || d.Remaining != 0 || d.Wait != 0 {
		t.Fatalf("6th request: want rejection, got %+v", d)
	}
}

func TestRejectImmediatelyWhenFull(t *testing.T) {
	l := mustLimiter(t, Config{QueueCapacity: 3, OutflowRate: 2, IdleTTL: time.Minute}) // 500ms interval
	defer l.Close()
	for i := 0; i < 3; i++ {
		if d, _ := l.Submit("k"); !d.Allowed {
			t.Fatalf("fill %d rejected", i)
		}
	}
	start := time.Now()
	d, tk := l.Submit("k")
	took := time.Since(start)
	if d.Allowed || tk != nil {
		t.Fatalf("want rejection, got %+v", d)
	}
	if took > 50*time.Millisecond { // interval is 500ms
		t.Fatalf("rejection took %v; Submit must not block", took)
	}
}

func TestRetryAfterBounds(t *testing.T) {
	l := mustLimiter(t, Config{QueueCapacity: 2, OutflowRate: 10, IdleTTL: time.Minute}) // 100ms
	defer l.Close()
	iv := l.Interval()
	if iv != 100*time.Millisecond {
		t.Fatalf("Interval=%v", iv)
	}
	rejections := 0
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		d, _ := l.Submit("k")
		if !d.Allowed {
			rejections++
			if d.RetryAfter <= 0 || d.RetryAfter > iv {
				t.Fatalf("RetryAfter=%v, want in (0, %v]", d.RetryAfter, iv)
			}
			if d.Wait != 0 {
				t.Fatalf("Wait=%v on rejection", d.Wait)
			}
		}
		time.Sleep(7 * time.Millisecond)
	}
	if rejections == 0 {
		t.Fatal("expected some rejections")
	}
}

// TestDequeueTimingFIFO checks the defining property of the leaky bucket on
// a real clock: requests leave one per interval, in arrival order.
func TestDequeueTimingFIFO(t *testing.T) {
	l := mustLimiter(t, Config{QueueCapacity: 5, OutflowRate: 20, IdleTTL: time.Minute}) // 50ms
	defer l.Close()

	const n = 5
	released := make([]time.Time, n)
	var (
		mu    sync.Mutex
		order []int
		wg    sync.WaitGroup
	)
	for i := 0; i < n; i++ {
		d, tk := l.Submit("k")
		if !d.Allowed {
			t.Fatalf("request %d rejected", i)
		}
		wg.Add(1)
		go func(i int, tk <-chan struct{}) {
			defer wg.Done()
			select {
			case <-tk:
			case <-time.After(3 * time.Second):
				return
			}
			now := time.Now()
			mu.Lock()
			released[i] = now
			order = append(order, i)
			mu.Unlock()
		}(i, tk)
	}
	wg.Wait()

	if len(order) != n {
		t.Fatalf("only %d/%d released", len(order), n)
	}
	for i := 0; i < n; i++ {
		if order[i] != i {
			t.Fatalf("release order %v is not FIFO", order)
		}
	}
	for i := 1; i < n; i++ {
		gap := released[i].Sub(released[i-1])
		t.Logf("gap %d->%d: %v", i-1, i, gap)
		if gap < 30*time.Millisecond || gap > 120*time.Millisecond {
			t.Fatalf("gap %d->%d = %v, want ~50ms (30-120ms)", i-1, i, gap)
		}
	}
}

// TestQueueFullWithOldRequests reproduces the documented con of the leaky
// bucket: a burst that fills the queue first starves, then delays, a later
// legitimate request. The legit request is (1) rejected outright while the
// burst occupies every slot, and (2) once a slot frees, admitted at the BACK
// of the line, so it waits about queue-depth × interval behind stale work.
// A token bucket would not do this: it would simply have no tokens, and the
// legit request would succeed as soon as one token refilled.
func TestQueueFullWithOldRequests(t *testing.T) {
	const capacity = 5
	l := mustLimiter(t, Config{QueueCapacity: capacity, OutflowRate: 10, IdleTTL: time.Minute}) // 100ms
	defer l.Close()
	iv := l.Interval()

	// Burst fills the queue.
	burst := make([]<-chan struct{}, 0, capacity)
	for i := 0; i < capacity; i++ {
		d, tk := l.Submit("shared")
		if !d.Allowed {
			t.Fatalf("burst %d rejected", i)
		}
		burst = append(burst, tk)
	}

	// (1) Legit request rejected while the old burst holds every slot.
	d, tk := l.Submit("shared")
	if d.Allowed || tk != nil {
		t.Fatalf("legit request should be rejected while queue is full, got %+v", d)
	}

	if d.RetryAfter <= 0 || d.RetryAfter > iv {
		t.Fatalf("RetryAfter=%v, want in (0, %v]", d.RetryAfter, iv)
	}

	// (2) Retry until the first slot frees. Poll tightly rather than
	// sleeping exactly RetryAfter: RetryAfter follows the ticker schedule,
	// and if the worker is a little late for a due tick, sleeping a full
	// RetryAfter could let a second tick pass, so the legit request would
	// no longer land exactly behind capacity-1 old requests.
	var legit <-chan struct{}
	waitFor(t, 2*time.Second, "a slot to free", func() bool {
		d, tk = l.Submit("shared")
		if d.Allowed {
			legit = tk
			return true
		}
		return false
	})
	admitted := time.Now()
	// It sits behind the rest of the burst (capacity-1 old requests), so it
	// is last in line and the queue is full again.
	if d.Remaining != 0 {
		t.Fatalf("legit admitted with Remaining=%d, want 0 (behind old burst)", d.Remaining)
	}
	if d.Wait < time.Duration(capacity-1)*iv {
		t.Fatalf("estimated Wait=%v, want >= %v (queue depth × interval)", d.Wait, time.Duration(capacity-1)*iv)
	}

	waitReleased(t, legit, 3*time.Second, "legit")
	delay := time.Since(admitted)
	t.Logf("legit request delayed %v behind old burst (estimate %v)", delay, d.Wait)
	// Real delay ~ capacity × interval = 500ms; tolerant bounds for Docker.
	if delay < time.Duration(capacity-2)*iv || delay > time.Duration(capacity+6)*iv {
		t.Fatalf("legit delay %v, want ≈ %v", delay, time.Duration(capacity)*iv)
	}
	// FIFO: every old burst request was released before the legit one.
	for i, b := range burst {
		select {
		case <-b:
		default:
			t.Fatalf("burst request %d not yet released although legit one was", i)
		}
	}
}

func TestPerKeyIsolation(t *testing.T) {
	l := mustLimiter(t, Config{QueueCapacity: 2, OutflowRate: 1, IdleTTL: time.Minute})
	defer l.Close()
	for i := 0; i < 2; i++ {
		l.Submit("A")
	}
	if d, _ := l.Submit("A"); d.Allowed {
		t.Fatal("A should be full")
	}
	d, tk := l.Submit("B")
	if !d.Allowed || tk == nil || d.Remaining != 1 {
		t.Fatalf("B must be unaffected by A, got %+v", d)
	}
	if l.Len() != 2 {
		t.Fatalf("Len=%d, want 2", l.Len())
	}
}

func TestAbandonedTicketKeepsSlot(t *testing.T) {
	l := mustLimiter(t, Config{QueueCapacity: 2, OutflowRate: 10, IdleTTL: time.Minute}) // 100ms
	defer l.Close()
	_, first := l.Submit("k")
	_, second := l.Submit("k")

	// Caller of the first request gives up (client disconnect).
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	if err := WaitTicket(ctx, first); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want DeadlineExceeded, got %v", err)
	}
	// The abandoned request still occupies its slot.
	if d, _ := l.Submit("k"); d.Allowed {
		t.Fatal("abandoned request should still hold its slot")
	}
	// The worker is not stalled by nobody listening on the first ticket.
	waitReleased(t, second, 2*time.Second, "second")
	select {
	case <-first: // closed by the worker even though no one waited
	default:
		t.Fatal("abandoned ticket should have been closed by the worker")
	}
	if err := WaitTicket(context.Background(), nil); !errors.Is(err, ErrNoTicket) {
		t.Fatalf("WaitTicket(nil) = %v", err)
	}
}

func TestIdleRetirement(t *testing.T) {
	base := runtime.NumGoroutine()
	l := mustLimiter(t, Config{QueueCapacity: 3, OutflowRate: 50, IdleTTL: 100 * time.Millisecond}) // 20ms
	defer l.Close()

	_, a := l.Submit("a")
	_, b := l.Submit("b")
	if l.Len() != 2 {
		t.Fatalf("Len=%d, want 2", l.Len())
	}
	waitReleased(t, a, time.Second, "a")
	waitReleased(t, b, time.Second, "b")

	waitFor(t, 2*time.Second, "idle buckets to retire", func() bool { return l.Len() == 0 })
	waitGoroutines(t, base, 2*time.Second)
	if got := l.retired.Load(); got != 2 {
		t.Fatalf("retired=%d, want 2", got)
	}

	// A retired key works again with a fresh bucket.
	d, a2 := l.Submit("a")
	if !d.Allowed || d.Remaining != 2 {
		t.Fatalf("fresh bucket: %+v", d)
	}
	if l.Len() != 1 {
		t.Fatalf("Len=%d after reuse, want 1", l.Len())
	}
	waitReleased(t, a2, time.Second, "a after retirement")
	l.Close()
	waitGoroutines(t, base, 2*time.Second)
}

// TestRetireEnqueueRace hammers Submit while IdleTTL is tiny, so workers
// constantly retire and buckets are constantly recreated. A request
// enqueued into a bucket whose worker has exited would never be released.
// Every Allowed ticket must be released (timeout guard).
func TestRetireEnqueueRace(t *testing.T) {
	base := runtime.NumGoroutine()
	l := mustLimiter(t, Config{QueueCapacity: 3, OutflowRate: 2000, IdleTTL: time.Microsecond}) // 500µs
	defer l.Close()

	const (
		workers = 32
		perG    = 300
		keys    = 16
	)
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		allowed int
		failure error
	)
	for g := 0; g < workers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(g), 42))
			var pending []<-chan struct{}
			n := 0
			for i := 0; i < perG; i++ {
				key := fmt.Sprintf("k%d", rng.IntN(keys))
				d, tk := l.Submit(key)
				if d.Allowed {
					n++
					if tk == nil {
						mu.Lock()
						failure = errors.New("allowed with nil ticket")
						mu.Unlock()
						return
					}
					if rng.IntN(2) == 0 {
						pending = append(pending, tk)
					} else {
						select {
						case <-tk:
						case <-time.After(5 * time.Second):
							mu.Lock()
							failure = fmt.Errorf("ticket for %s never released", key)
							mu.Unlock()
							return
						}
					}
				}
				switch rng.IntN(4) {
				case 0:
					time.Sleep(time.Duration(rng.IntN(3000)) * time.Microsecond)
				case 1:
					runtime.Gosched()
				}
			}
			for _, tk := range pending {
				select {
				case <-tk:
				case <-time.After(5 * time.Second):
					mu.Lock()
					failure = errors.New("pending ticket never released")
					mu.Unlock()
					return
				}
			}
			mu.Lock()
			allowed += n
			mu.Unlock()
		}(g)
	}
	wg.Wait()
	if failure != nil {
		t.Fatal(failure)
	}
	t.Logf("allowed=%d retirements=%d", allowed, l.retired.Load())
	if allowed == 0 {
		t.Fatal("no requests allowed")
	}
	if l.retired.Load() == 0 {
		t.Fatal("no retirements happened; race was not exercised")
	}
	l.Close()
	waitGoroutines(t, base, 2*time.Second)
}

func TestCloseReleasesQueuedAndNoLeaks(t *testing.T) {
	base := runtime.NumGoroutine()
	l := mustLimiter(t, Config{QueueCapacity: 5, OutflowRate: 1, IdleTTL: time.Minute}) // 1s interval
	var tickets []<-chan struct{}
	for _, k := range []string{"x", "y", "z"} {
		for i := 0; i < 5; i++ {
			d, tk := l.Submit(k)
			if !d.Allowed {
				t.Fatalf("%s/%d rejected", k, i)
			}
			tickets = append(tickets, tk)
		}
	}
	start := time.Now()
	l.Close()
	if took := time.Since(start); took > 500*time.Millisecond {
		t.Fatalf("Close took %v", took)
	}
	// Close released every queued ticket (well before 5 × 1s intervals).
	for i, tk := range tickets {
		select {
		case <-tk:
		default:
			t.Fatalf("ticket %d not released by Close", i)
		}
	}
	if !l.Closed() || l.Len() != 0 {
		t.Fatalf("Closed=%v Len=%d", l.Closed(), l.Len())
	}
	d, tk := l.Submit("x")
	if d.Allowed || tk != nil || d.RetryAfter != 0 || d.Limit != 5 {
		t.Fatalf("Submit after Close: %+v", d)
	}
	l.Close() // idempotent
	waitGoroutines(t, base, 2*time.Second)
}

func TestConcurrentClose(t *testing.T) {
	base := runtime.NumGoroutine()
	l := mustLimiter(t, Config{QueueCapacity: 4, OutflowRate: 100, IdleTTL: time.Millisecond})
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				if _, tk := l.Submit(fmt.Sprintf("k%d", i%3)); tk != nil {
					select {
					case <-tk:
					case <-time.After(5 * time.Second):
						t.Error("ticket never released")
						return
					}
				}
			}
		}(g)
	}
	time.Sleep(100 * time.Millisecond)
	var cw sync.WaitGroup
	for i := 0; i < 3; i++ {
		cw.Add(1)
		go func() { defer cw.Done(); l.Close() }()
	}
	cw.Wait()
	close(stop)
	wg.Wait()
	waitGoroutines(t, base, 2*time.Second)
}

func TestConfigValidation(t *testing.T) {
	good := Config{QueueCapacity: 1, OutflowRate: 1, IdleTTL: time.Second}
	cases := map[string]func(*Config){
		"zero capacity":     func(c *Config) { c.QueueCapacity = 0 },
		"negative capacity": func(c *Config) { c.QueueCapacity = -3 },
		"zero rate":         func(c *Config) { c.OutflowRate = 0 },
		"negative rate":     func(c *Config) { c.OutflowRate = -1 },
		"NaN rate":          func(c *Config) { c.OutflowRate = math.NaN() },
		"+Inf rate":         func(c *Config) { c.OutflowRate = math.Inf(1) },
		"rate too high":     func(c *Config) { c.OutflowRate = 1e12 },
		"rate too low":      func(c *Config) { c.OutflowRate = 1e-12 },
		"zero TTL":          func(c *Config) { c.IdleTTL = 0 },
		"negative TTL":      func(c *Config) { c.IdleTTL = -time.Second },
		"negative cleanup":  func(c *Config) { c.CleanupInterval = -1 },
	}
	for name, mut := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := good
			mut(&cfg)
			l, err := NewLimiter(cfg)
			if err == nil || l != nil {
				t.Fatalf("want error, got limiter=%v err=%v", l, err)
			}
			if !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("error %v does not wrap ErrInvalidConfig", err)
			}
		})
	}
	l, err := NewLimiter(good)
	if err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	l.Close()
}
