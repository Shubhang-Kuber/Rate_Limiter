package ratelimiter

// QA / adversarial tests: goroutine leaks at scale, Close under load,
// Decision invariants at extreme rates, degenerate configuration.

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

// checkDecision asserts the invariants every Decision must satisfy.
func checkDecision(t *testing.T, d Decision, l *Limiter, where string) {
	t.Helper()
	if d.Limit != l.cfg.QueueCapacity || d.Remaining < 0 || d.Remaining > d.Limit {
		t.Fatalf("%s: bad Limit/Remaining: %+v", where, d)
	}
	if d.Wait < 0 || d.RetryAfter < 0 {
		t.Fatalf("%s: negative duration: %+v", where, d)
	}
	if d.Allowed {
		if d.RetryAfter != 0 || d.Wait <= 0 {
			t.Fatalf("%s: allowed decision: %+v", where, d)
		}
	} else if d.Wait != 0 || (!l.Closed() && (d.RetryAfter <= 0 || d.RetryAfter > l.interval)) {
		t.Fatalf("%s: rejected decision: %+v (interval %v)", where, d, l.interval)
	}
}

// BUG (fixed): with a tiny outflow rate the interval is huge (1e18ns here,
// still a valid config) and Wait = untilNextTick + (pos-1)*interval
// overflowed int64, reporting a NEGATIVE Wait from position 10 onwards.
func TestQAWaitNoOverflowTinyRate(t *testing.T) {
	l := mustLimiter(t, Config{QueueCapacity: 20, OutflowRate: 1e-9, IdleTTL: time.Hour})
	defer l.Close()
	if iv := l.Interval(); iv < 999_999_999*time.Second || iv > 1_000_000_001*time.Second {
		t.Fatalf("interval=%v, want ~1e18ns", iv)
	}
	prev := time.Duration(0)
	for i := 0; i < 20; i++ {
		d, _ := l.Submit("k")
		checkDecision(t, d, l, fmt.Sprintf("pos %d", i+1))
		if d.Wait < prev {
			t.Fatalf("pos %d: Wait=%v decreased from %v (overflow)", i+1, d.Wait, prev)
		}
		prev = d.Wait
	}
	if prev != time.Duration(math.MaxInt64) {
		t.Fatalf("last Wait=%v, want saturated MaxInt64", prev)
	}
	d, _ := l.Submit("k") // full
	checkDecision(t, d, l, "full")
}

// Decision invariants across the whole valid rate range, including the
// fastest (MinInterval) and slowest (interval near MaxInt64) configs.
func TestQADecisionInvariantsExtremeRates(t *testing.T) {
	for _, rate := range []float64{1e6, 1000, 1, 1e-3, 1e-9, 1.1e-10} {
		t.Run(fmt.Sprint(rate), func(t *testing.T) {
			l := mustLimiter(t, Config{QueueCapacity: 8, OutflowRate: rate, IdleTTL: time.Hour})
			defer l.Close()
			for i := 0; i < 50; i++ {
				d, _ := l.Submit("k")
				checkDecision(t, d, l, fmt.Sprintf("submit %d", i))
			}
		})
	}
}

// Degenerate configs are rejected with an error wrapping ErrInvalidConfig;
// boundary values are accepted.
func TestQAConfigDegenerate(t *testing.T) {
	ok := Config{QueueCapacity: 1, OutflowRate: 1, IdleTTL: time.Second}
	mut := func(f func(*Config)) Config { c := ok; f(&c); return c }
	bad := map[string]Config{
		"cap0":         mut(func(c *Config) { c.QueueCapacity = 0 }),
		"capNeg":       mut(func(c *Config) { c.QueueCapacity = -1 }),
		"capMinInt":    mut(func(c *Config) { c.QueueCapacity = math.MinInt }),
		"rate0":        mut(func(c *Config) { c.OutflowRate = 0 }),
		"rateNegZero":  mut(func(c *Config) { c.OutflowRate = math.Copysign(0, -1) }),
		"rateNeg":      mut(func(c *Config) { c.OutflowRate = -1 }),
		"rateNaN":      mut(func(c *Config) { c.OutflowRate = math.NaN() }),
		"rateInf":      mut(func(c *Config) { c.OutflowRate = math.Inf(1) }),
		"rateTooHigh":  mut(func(c *Config) { c.OutflowRate = 1.5e9 }), // interval < 1ns
		"rateAboveMin": mut(func(c *Config) { c.OutflowRate = 1.1e6 }), // interval < MinInterval
		"rateTooLow":   mut(func(c *Config) { c.OutflowRate = 1e-10 }), // interval > MaxInt64
		"rateDenormal": mut(func(c *Config) { c.OutflowRate = math.SmallestNonzeroFloat64 }),
		"ttl0":         mut(func(c *Config) { c.IdleTTL = 0 }),
		"ttlNeg":       mut(func(c *Config) { c.IdleTTL = -time.Second }),
		"cleanupNeg":   mut(func(c *Config) { c.CleanupInterval = -1 }),
	}
	for name, c := range bad {
		if _, err := NewLimiter(c); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("%s: err=%v, want ErrInvalidConfig", name, err)
		}
	}
	good := map[string]Config{
		"rate1e6":    mut(func(c *Config) { c.OutflowRate = 1e6 }), // exactly MinInterval
		"cleanup0":   mut(func(c *Config) { c.CleanupInterval = 0 }),
		"ttl1ns":     mut(func(c *Config) { c.IdleTTL = 1 }),
		"hugeCap":    mut(func(c *Config) { c.QueueCapacity = 1 << 20 }),
		"slowestOK":  mut(func(c *Config) { c.OutflowRate = 1.1e-10 }),
		"fractional": mut(func(c *Config) { c.OutflowRate = 3 }),
	}
	for name, c := range good {
		l, err := NewLimiter(c)
		if err != nil {
			t.Errorf("%s: unexpected err %v", name, err)
			continue
		}
		l.Close()
	}
	// The interval is rounded up (Team Lead fix after QA's design note), so
	// the real outflow never exceeds the configured rate: rate 3 gives
	// 333.333334ms, not the truncated 333.333333ms (3.000000003/s).
	l := mustLimiter(t, mut(func(c *Config) { c.OutflowRate = 3 }))
	defer l.Close()
	if l.Interval() != 333333334*time.Nanosecond {
		t.Fatalf("rate 3 interval=%v, want 333.333334ms", l.Interval())
	}
}

// Many distinct keys: one worker per key while active, and ALL retire after
// IdleTTL, returning NumGoroutine to the baseline. 10,000 keys normally,
// 5,000 under -race (detector limit 8128 live goroutines).
func TestQAGoroutineLeakManyKeysRetire(t *testing.T) {
	runtime.GC()
	base := runtime.NumGoroutine()
	l := mustLimiter(t, Config{QueueCapacity: 2, OutflowRate: 20, IdleTTL: 100 * time.Millisecond}) // 50ms
	defer l.Close()

	tickets := make([]<-chan struct{}, 0, qaLeakKeys)
	for i := 0; i < qaLeakKeys; i++ {
		d, tk := l.Submit(fmt.Sprintf("k%d", i))
		if !d.Allowed {
			t.Fatalf("key %d rejected: %+v", i, d)
		}
		tickets = append(tickets, tk)
	}
	peak := runtime.NumGoroutine()
	t.Logf("keys=%d goroutines: base=%d peak=%d (delta %d) Len=%d", qaLeakKeys, base, peak, peak-base, l.Len())
	if peak-base < qaLeakKeys || l.Len() != qaLeakKeys {
		t.Fatalf("want one worker per key: delta=%d Len=%d", peak-base, l.Len())
	}
	for i, tk := range tickets {
		waitReleased(t, tk, 5*time.Second, fmt.Sprintf("key %d", i))
	}
	start := time.Now()
	waitFor(t, 10*time.Second, "all workers to retire", func() bool { return l.Len() == 0 })
	waitGoroutines(t, base, 5*time.Second)
	t.Logf("all %d workers retired %v after the last release; goroutines now %d; retired counter=%d",
		qaLeakKeys, time.Since(start), runtime.NumGoroutine(), l.retired.Load())
	if got := l.retired.Load(); got != qaLeakKeys {
		t.Fatalf("retired=%d, want %d", got, qaLeakKeys)
	}
}

// Many distinct keys with full queues: Close must release every ticket and
// return only once every worker has exited.
func TestQAGoroutineLeakManyKeysClose(t *testing.T) {
	runtime.GC()
	base := runtime.NumGoroutine()
	l := mustLimiter(t, Config{QueueCapacity: 3, OutflowRate: 0.5, IdleTTL: time.Hour}) // 2s interval
	var tickets []<-chan struct{}
	for i := 0; i < qaLeakKeys; i++ {
		for j := 0; j < 3; j++ {
			_, tk := l.Submit(fmt.Sprintf("k%d", i))
			tickets = append(tickets, tk)
		}
	}
	peak := runtime.NumGoroutine()
	start := time.Now()
	l.Close()
	took := time.Since(start)
	for i, tk := range tickets {
		select {
		case <-tk:
		default:
			t.Fatalf("ticket %d not released by Close", i)
		}
	}
	waitGoroutines(t, base, 2*time.Second)
	t.Logf("keys=%d tickets=%d goroutines: base=%d peak=%d after Close=%d; Close took %v",
		qaLeakKeys, len(tickets), base, peak, runtime.NumGoroutine(), took)
}

// Close in the middle of a Submit storm over many keys with a tiny IdleTTL
// (so workers are also retiring concurrently). Every Allowed ticket must be
// released, every Submit after Close returns must reject with a nil ticket,
// and no goroutine may leak.
func TestQACloseDuringSubmitStormWithRetirement(t *testing.T) {
	base := runtime.NumGoroutine()
	l := mustLimiter(t, Config{QueueCapacity: 2, OutflowRate: 1000, IdleTTL: time.Nanosecond})
	var (
		wg               sync.WaitGroup
		closed           atomic.Bool
		allowed, lateBad atomic.Int64
	)
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 4000; i++ {
				wasClosed := closed.Load() // read before Submit
				d, tk := l.Submit(fmt.Sprintf("k%d", (g*7919+i)%257))
				if wasClosed && (d.Allowed || tk != nil || d.RetryAfter != 0) {
					lateBad.Add(1)
				}
				if d.Allowed != (tk != nil) {
					t.Errorf("Allowed=%v but ticket nil=%v", d.Allowed, tk == nil)
					return
				}
				if d.Allowed {
					allowed.Add(1)
					if i%3 == 0 { // wait on some, abandon others
						select {
						case <-tk:
						case <-time.After(5 * time.Second):
							t.Error("storm ticket never released")
							return
						}
					}
				}
			}
		}(g)
	}
	time.Sleep(30 * time.Millisecond)
	l.Close()
	closed.Store(true)
	wg.Wait()
	if lateBad.Load() != 0 {
		t.Fatalf("%d Submits after Close returned non-rejection", lateBad.Load())
	}
	if l.Len() != 0 {
		t.Fatalf("Len=%d after Close", l.Len())
	}
	waitGoroutines(t, base, 3*time.Second)
	t.Logf("allowed=%d retired=%d before/around Close", allowed.Load(), l.retired.Load())
}

// Len counts only live buckets: it never exceeds the number of distinct keys
// even under retire/recreate churn, and is 0 once everything has retired.
func TestQALenAccounting(t *testing.T) {
	l := mustLimiter(t, Config{QueueCapacity: 1, OutflowRate: 2000, IdleTTL: time.Microsecond})
	defer l.Close()
	const keys = 8
	stop := make(chan struct{})
	var maxLen atomic.Int64
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if n := int64(l.Len()); n > maxLen.Load() {
				maxLen.Store(n)
			}
			runtime.Gosched()
		}
	}()
	for i := 0; i < 3000; i++ {
		l.Submit(fmt.Sprintf("k%d", i%keys))
		if i%50 == 0 {
			time.Sleep(time.Millisecond)
		}
	}
	close(stop)
	wg.Wait()
	if maxLen.Load() > keys {
		t.Fatalf("Len peaked at %d > %d distinct keys", maxLen.Load(), keys)
	}
	waitFor(t, 3*time.Second, "Len to reach 0", func() bool { return l.Len() == 0 })
}
