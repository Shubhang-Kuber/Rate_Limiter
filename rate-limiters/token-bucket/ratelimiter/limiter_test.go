package ratelimiter

import (
	"errors"
	"fmt"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func mustLimiter(t *testing.T, cfg Config, opts ...Option) *Limiter {
	t.Helper()
	l, err := NewLimiter(cfg, opts...)
	if err != nil {
		t.Fatalf("NewLimiter(%+v): %v", cfg, err)
	}
	t.Cleanup(l.Close)
	return l
}

func (l *Limiter) has(key string) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	_, ok := l.buckets[key]
	return ok
}

func TestNewLimiterValidation(t *testing.T) {
	ok := Config{Capacity: 5, RefillRate: 1, IdleTTL: time.Minute}
	cases := []struct {
		name string
		mod  func(*Config)
		want error
	}{
		{"zero capacity", func(c *Config) { c.Capacity = 0 }, ErrInvalidCapacity},
		{"inf capacity", func(c *Config) { c.Capacity = math.Inf(1) }, ErrInvalidCapacity},
		{"negative rate", func(c *Config) { c.RefillRate = -1 }, ErrInvalidRefillRate},
		{"NaN rate", func(c *Config) { c.RefillRate = math.NaN() }, ErrInvalidRefillRate},
		{"zero TTL", func(c *Config) { c.IdleTTL = 0 }, ErrInvalidIdleTTL},
		{"negative TTL", func(c *Config) { c.IdleTTL = -time.Second }, ErrInvalidIdleTTL},
		{"negative cleanup", func(c *Config) { c.CleanupInterval = -1 }, ErrInvalidCleanupInterval},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := ok
			tc.mod(&cfg)
			l, err := NewLimiter(cfg)
			if !errors.Is(err, tc.want) || l != nil {
				t.Fatalf("got (%v, %v), want %v", l, err, tc.want)
			}
		})
	}
	if _, err := NewLimiter(ok, WithClock(nil)); !errors.Is(err, ErrNilClock) {
		t.Fatalf("nil clock: got %v", err)
	}
}

func TestLimiterDefaultCleanupInterval(t *testing.T) {
	cases := []struct {
		ttl, interval, want time.Duration
	}{
		{10 * time.Second, 0, 5 * time.Second},
		{100 * time.Millisecond, 0, MinDefaultCleanupInterval},
		{time.Minute, 7 * time.Millisecond, 7 * time.Millisecond},
	}
	for _, tc := range cases {
		l := mustLimiter(t, Config{Capacity: 1, RefillRate: 1, IdleTTL: tc.ttl, CleanupInterval: tc.interval})
		if l.cfg.CleanupInterval != tc.want {
			t.Errorf("TTL %v, interval %v: resolved %v, want %v", tc.ttl, tc.interval, l.cfg.CleanupInterval, tc.want)
		}
	}
}

func TestLimiterPerKeyIsolation(t *testing.T) {
	clk := newFakeClock()
	l := mustLimiter(t, Config{Capacity: 3, RefillRate: 1, IdleTTL: time.Hour}, WithClock(clk.Now))
	for i := 0; i < 3; i++ {
		if !l.Allow("A").Allowed {
			t.Fatalf("A request %d denied", i)
		}
	}
	if d := l.Allow("A"); d.Allowed || d.RetryAfter != time.Second {
		t.Fatalf("drained A: %+v", d)
	}
	for i := 0; i < 3; i++ {
		d := l.Allow("B")
		if !d.Allowed || d.Remaining != 2-i || d.Limit != 3 {
			t.Fatalf("B request %d affected by A: %+v", i, d)
		}
	}
	if l.Len() != 2 {
		t.Fatalf("Len = %d, want 2", l.Len())
	}
}

func TestLimiterConcurrentManyKeys(t *testing.T) {
	const keys, perKeyG, perG, capacity = 64, 16, 8, 50
	clk := newFakeClock() // frozen: no refill, so per-key totals are exact
	l := mustLimiter(t, Config{Capacity: capacity, RefillRate: 10, IdleTTL: time.Hour}, WithClock(clk.Now))
	var counts [keys]atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for k := 0; k < keys; k++ {
		key := fmt.Sprintf("client-%d", k)
		for g := 0; g < perKeyG; g++ {
			wg.Add(1)
			go func(k int) {
				defer wg.Done()
				<-start
				for i := 0; i < perG; i++ {
					if l.Allow(key).Allowed {
						counts[k].Add(1)
					}
					_ = l.Len()
				}
			}(k)
		}
	}
	close(start)
	wg.Wait()
	for k := range counts {
		if got := counts[k].Load(); got != capacity {
			t.Errorf("key %d: allowed %d, want exactly %d", k, got, capacity)
		}
	}
	if l.Len() != keys {
		t.Fatalf("Len = %d, want %d", l.Len(), keys)
	}
}

func TestLimiterJanitorEvictsIdleRealTime(t *testing.T) {
	l := mustLimiter(t, Config{
		Capacity: 5, RefillRate: 100,
		IdleTTL: 50 * time.Millisecond, CleanupInterval: 10 * time.Millisecond,
	})
	l.Allow("idle")
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				l.Allow("active")
				time.Sleep(5 * time.Millisecond)
			}
		}
	}()
	deadline := time.Now().Add(3 * time.Second)
	for l.has("idle") && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	// Let a few more sweeps run while "active" keeps being used.
	time.Sleep(150 * time.Millisecond)
	activePresent := l.has("active")
	close(stop)
	wg.Wait()
	if l.has("idle") {
		t.Fatal("idle key was not evicted")
	}
	if !activePresent {
		t.Fatal("active key was evicted")
	}
	if n := l.Len(); n != 1 {
		t.Fatalf("Len = %d, want 1", n)
	}
}

func TestLimiterEvictIdleDeterministic(t *testing.T) {
	clk := newFakeClock()
	// Huge cleanup interval: the background janitor never fires; we drive
	// evictIdle by hand.
	l := mustLimiter(t, Config{Capacity: 5, RefillRate: 1, IdleTTL: time.Minute, CleanupInterval: time.Hour}, WithClock(clk.Now))
	l.Allow("a")
	l.Allow("b")
	clk.Advance(30 * time.Second)
	l.Allow("b")
	clk.Advance(30 * time.Second) // a idle exactly TTL: kept (strictly greater evicts)
	if n := l.evictIdle(clk.Now()); n != 0 {
		t.Fatalf("evicted %d at exactly TTL, want 0", n)
	}
	clk.Advance(time.Nanosecond)
	if n := l.evictIdle(clk.Now()); n != 1 || l.has("a") || !l.has("b") {
		t.Fatalf("evicted %d, has(a)=%v has(b)=%v; want only a evicted", n, l.has("a"), l.has("b"))
	}
	if l.Len() != 1 {
		t.Fatalf("Len = %d, want 1", l.Len())
	}
	// A returning evicted key starts with a full bucket.
	if d := l.Allow("a"); !d.Allowed || d.Remaining != 4 {
		t.Fatalf("recreated a: %+v", d)
	}
}

func TestLimiterEvictionRace(t *testing.T) {
	clk := newFakeClock()
	l := mustLimiter(t, Config{Capacity: 5, RefillRate: 1, IdleTTL: time.Minute, CleanupInterval: time.Hour}, WithClock(clk.Now))
	l.Allow("k")

	// A caller has looked the bucket up but not locked it yet...
	stale := l.bucket("k")
	clk.Advance(2 * time.Minute)
	// ...when the janitor evicts it.
	if n := l.evictIdle(clk.Now()); n != 1 {
		t.Fatalf("evicted %d, want 1", n)
	}
	stale.mu.Lock()
	dead, tokens := stale.dead, stale.tokens
	stale.mu.Unlock()
	if !dead {
		t.Fatal("evicted bucket not marked dead")
	}
	// The caller's request must land on the live bucket, not the orphan.
	if d := l.Allow("k"); !d.Allowed || d.Remaining != 4 {
		t.Fatalf("Allow after eviction: %+v", d)
	}
	stale.mu.Lock()
	after := stale.tokens
	stale.mu.Unlock()
	if after != tokens {
		t.Fatalf("orphaned bucket was consumed from: %v -> %v", tokens, after)
	}
	if live := l.bucket("k"); live == stale {
		t.Fatal("dead bucket still in map")
	}

	// A bucket that is locked (in use) at sweep time is never evicted.
	live := l.bucket("k")
	clk.Advance(2 * time.Minute)
	live.mu.Lock()
	n := l.evictIdle(clk.Now())
	live.mu.Unlock()
	if n != 0 || !l.has("k") {
		t.Fatalf("in-use bucket evicted (n=%d)", n)
	}
}

func TestLimiterEvictionStress(t *testing.T) {
	// Janitor sweeping every millisecond with a 1ms TTL while many goroutines
	// hammer a few keys: exercises the dead-bucket retry path under -race.
	l := mustLimiter(t, Config{Capacity: 10, RefillRate: 1000, IdleTTL: time.Millisecond, CleanupInterval: time.Millisecond})
	var wg sync.WaitGroup
	for g := 0; g < 64; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			key := fmt.Sprintf("k%d", g%4)
			for i := 0; i < 300; i++ {
				d := l.Allow(key)
				if d.Remaining < 0 || d.Remaining > 10 {
					t.Errorf("Remaining out of range: %+v", d)
					return
				}
				if i%50 == 0 {
					time.Sleep(2 * time.Millisecond)
				}
			}
		}(g)
	}
	wg.Wait()
}

func TestLimiterCloseStopsJanitor(t *testing.T) {
	waitFor := func(cond func() bool) bool {
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if cond() {
				return true
			}
			time.Sleep(5 * time.Millisecond)
		}
		return cond()
	}
	runtime.GC()
	baseline := runtime.NumGoroutine()

	ls := make([]*Limiter, 5)
	for i := range ls {
		l, err := NewLimiter(Config{Capacity: 1, RefillRate: 1, IdleTTL: time.Second, CleanupInterval: time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		l.Allow("x")
		ls[i] = l
	}
	if !waitFor(func() bool { return runtime.NumGoroutine() >= baseline+len(ls) }) {
		t.Fatalf("janitors not running: %d goroutines, baseline %d", runtime.NumGoroutine(), baseline)
	}
	for _, l := range ls {
		l.Close()
	}
	// Second Close, including concurrently, must be a safe no-op.
	var wg sync.WaitGroup
	for _, l := range ls {
		for j := 0; j < 3; j++ {
			wg.Add(1)
			go func(l *Limiter) { defer wg.Done(); l.Close() }(l)
		}
	}
	wg.Wait()
	if !waitFor(func() bool { return runtime.NumGoroutine() <= baseline }) {
		t.Fatalf("goroutine leak: %d goroutines, baseline %d", runtime.NumGoroutine(), baseline)
	}
	// The limiter still answers after Close; it just no longer evicts.
	if d := ls[0].Allow("y"); !d.Allowed {
		t.Fatalf("Allow after Close: %+v", d)
	}
}
