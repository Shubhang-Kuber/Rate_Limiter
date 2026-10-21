package ratelimiter

// QA / adversarial tests. These target races, goroutine leaks, degenerate
// configuration and clock behaviour that the functional tests do not cover.

import (
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// qaLimiter builds a Limiter on a fake clock whose janitor effectively never
// fires on its own (tests drive evictIdle directly).
func qaLimiter(t *testing.T, capacity, rate float64, ttl time.Duration, clk *fakeClock) *Limiter {
	t.Helper()
	return mustLimiter(t, Config{
		Capacity: capacity, RefillRate: rate, IdleTTL: ttl, CleanupInterval: time.Hour,
	}, WithClock(clk.Now))
}

// checkSane asserts invariants every Decision must satisfy.
func checkSane(t *testing.T, d Decision, capacity float64, where string) {
	t.Helper()
	if d.Remaining < 0 || d.Remaining > floorInt(capacity) {
		t.Fatalf("%s: Remaining=%d outside [0,%d]: %+v", where, d.Remaining, floorInt(capacity), d)
	}
	if d.Allowed && d.RetryAfter != 0 {
		t.Fatalf("%s: allowed with RetryAfter=%v", where, d.RetryAfter)
	}
	if !d.Allowed && d.RetryAfter != RetryNever && d.RetryAfter <= 0 {
		t.Fatalf("%s: denied with non-positive RetryAfter=%v", where, d.RetryAfter)
	}
}

// --- goroutine / memory behaviour -------------------------------------------

// 100k keys must cost zero goroutines (lazy refill), and the janitor must
// evict every one of them once idle.
func TestQANoGoroutinePerKeyAndFullEviction(t *testing.T) {
	const keys = 100_000
	clk := newFakeClock()
	l := mustLimiter(t, Config{
		Capacity: 5, RefillRate: 1, IdleTTL: time.Minute, CleanupInterval: 200 * time.Millisecond,
	}, WithClock(clk.Now))

	runtime.GC()
	base := runtime.NumGoroutine() // includes the janitor
	for i := 0; i < keys; i++ {
		if d := l.Allow(fmt.Sprintf("k%d", i)); !d.Allowed {
			t.Fatalf("key %d denied on first request", i)
		}
	}
	after := runtime.NumGoroutine()
	t.Logf("goroutines: base=%d after %d keys=%d (delta %d), Len=%d", base, keys, after, after-base, l.Len())
	if after > base {
		t.Fatalf("lazy refill must not spawn goroutines: base=%d after=%d", base, after)
	}
	if l.Len() != keys {
		t.Fatalf("Len=%d, want %d", l.Len(), keys)
	}

	// Not yet idle: the real janitor runs repeatedly but must keep everything.
	time.Sleep(450 * time.Millisecond)
	if l.Len() != keys {
		t.Fatalf("janitor evicted non-idle buckets: Len=%d", l.Len())
	}
	clk.Advance(time.Minute + time.Nanosecond)
	deadline := time.Now().Add(5 * time.Second)
	for l.Len() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("janitor did not evict all idle buckets: Len=%d", l.Len())
		}
		time.Sleep(5 * time.Millisecond)
	}

	l.Close()
	deadline = time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() >= base { // janitor gone => strictly below base
		if time.Now().After(deadline) {
			t.Fatalf("janitor goroutine still running after Close: %d >= %d", runtime.NumGoroutine(), base)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Close is idempotent, concurrent-safe, and the Limiter keeps working after.
func TestQACloseConcurrentAndUseAfterClose(t *testing.T) {
	base := runtime.NumGoroutine()
	l := mustLimiter(t, Config{Capacity: 2, RefillRate: 0, IdleTTL: time.Millisecond, CleanupInterval: time.Millisecond})
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); l.Close() }()
	}
	wg.Wait()
	l.Close()
	if d := l.Allow("a"); !d.Allowed {
		t.Fatalf("Allow after Close: %+v", d)
	}
	l.Allow("a")
	if d := l.Allow("a"); d.Allowed || d.RetryAfter != RetryNever {
		t.Fatalf("third Allow after Close with rate 0: %+v", d)
	}
	time.Sleep(10 * time.Millisecond)
	if l.Len() != 1 {
		t.Fatalf("closed limiter must not evict: Len=%d", l.Len())
	}
	if n := runtime.NumGoroutine(); n > base {
		t.Fatalf("goroutines after Close: %d > base %d", n, base)
	}
}

// --- races -----------------------------------------------------------------

// Callers hammer a few keys while a sweeper evicts constantly and the clock
// jumps past IdleTTL between sweeps, maximising dead-bucket retries. Every
// call must return (no livelock) and every bucket instance must respect its
// capacity: with rate 0, each live bucket instance grants at most Capacity.
func TestQAEvictionRetryNoLivelock(t *testing.T) {
	clk := newFakeClock()
	const capacity = 3
	l := qaLimiter(t, capacity, 0, time.Millisecond, clk)
	defer l.Close()

	var stop atomic.Bool
	var sweeps, evicted atomic.Int64
	sweeperDone := make(chan struct{})
	go func() {
		defer close(sweeperDone)
		for !stop.Load() {
			clk.Advance(2 * time.Millisecond)
			evicted.Add(int64(l.evictIdle(clk.Now())))
			sweeps.Add(1)
			runtime.Gosched()
		}
	}()

	const workers, calls = 8, 3000
	var allowed atomic.Int64
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < calls; i++ {
				d := l.Allow(fmt.Sprintf("k%d", i%4))
				if d.Allowed {
					allowed.Add(1)
				}
				if d.Limit != capacity || d.Remaining < 0 || d.Remaining > capacity {
					t.Errorf("bad decision %+v", d)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	stop.Store(true)
	<-sweeperDone
	t.Logf("%d calls in %v, %d sweeps, %d evictions, %d allowed", workers*calls, time.Since(start), sweeps.Load(), evicted.Load(), allowed.Load())
	// Each bucket instance ever created (4 keys initially + one per eviction)
	// can grant at most `capacity` tokens with rate 0. More would mean a
	// consumption applied to an orphaned bucket was double counted, or a
	// bucket was overspent.
	maxAllowed := (4 + evicted.Load()) * capacity
	if allowed.Load() > maxAllowed {
		t.Fatalf("allowed %d > %d (instances x capacity)", allowed.Load(), maxAllowed)
	}
}

// --- clock behaviour --------------------------------------------------------

// A backward jump (small and huge) never over-fills, never goes negative,
// never stalls refill, and never yields an absurd RetryAfter. Returning to the
// original time over-credits by at most the jump (documented), capped at
// Capacity.
func TestQAClockBackwardThenForward(t *testing.T) {
	for _, jump := range []time.Duration{time.Millisecond, time.Second, time.Hour, 10 * 365 * 24 * time.Hour} {
		t.Run(jump.String(), func(t *testing.T) {
			clk := newFakeClock()
			const capacity, rate = 10.0, 2.0
			b := mustBucket(t, capacity, rate, clk)
			if d := b.AllowN(capacity); !d.Allowed || d.Remaining != 0 {
				t.Fatalf("drain: %+v", d)
			}
			t0 := clk.Now()

			clk.Advance(-jump)
			d := b.Allow()
			checkSane(t, d, capacity, "after backward jump")
			if d.Allowed {
				t.Fatalf("backward jump credited tokens: %+v", d)
			}
			if d.RetryAfter != 500*time.Millisecond {
				t.Fatalf("RetryAfter after backward jump = %v, want 500ms", d.RetryAfter)
			}

			// No stall: refill resumes immediately from the rebased timestamp.
			clk.Advance(500 * time.Millisecond)
			if d := b.Allow(); !d.Allowed {
				t.Fatalf("refill stalled after backward jump of %v: %+v", jump, d)
			}

			// Return to t0 + 0: documented over-credit, bounded by the jump
			// and capped at Capacity.
			clk.Set(t0)
			d = b.AllowN(0)
			checkSane(t, d, capacity, "back at t0")
			credit := (jump - 500*time.Millisecond).Seconds() * rate
			want := floorInt(math.Min(math.Max(credit, 0), capacity))
			if d.Remaining != want {
				t.Fatalf("over-credit after returning to t0: Remaining=%d want %d", d.Remaining, want)
			}
			if b.tokens > capacity || b.tokens < 0 {
				t.Fatalf("tokens out of range: %v", b.tokens)
			}
		})
	}
}

// Clock jitter (back and forth by 1ms, as a non-monotonic clock under NTP
// slewing might do) keeps every invariant. The documented over-credit is
// observable: each oscillation can credit the jitter again.
func TestQAClockJitterInvariants(t *testing.T) {
	clk := newFakeClock()
	const capacity, rate = 5.0, 1.0
	b := mustBucket(t, capacity, rate, clk)
	rng := rand.New(rand.NewPCG(1, 2))
	for i := 0; i < 20000; i++ {
		clk.Advance(time.Duration(rng.IntN(2001)-1000) * time.Microsecond)
		d := b.AllowN(float64(rng.IntN(3)))
		checkSane(t, d, capacity, fmt.Sprintf("step %d", i))
		if b.tokens < 0 || b.tokens > capacity || math.IsNaN(b.tokens) {
			t.Fatalf("step %d: tokens=%v", i, b.tokens)
		}
	}
}

// The janitor must eventually evict buckets after the clock jumps backwards.
// Before the fix, a bucket last used at T was kept until the clock got back
// past T+IdleTTL: after a 10-year backward step, that is 10 years, so every
// key seen before the step leaked.
func TestQAJanitorEvictsAfterBackwardJump(t *testing.T) {
	for _, jump := range []time.Duration{30 * time.Second, time.Hour, 10 * 365 * 24 * time.Hour} {
		t.Run(jump.String(), func(t *testing.T) {
			clk := newFakeClock()
			const ttl = time.Minute
			l := qaLimiter(t, 5, 1, ttl, clk)
			defer l.Close()
			l.Allow("old")
			clk.Advance(-jump)
			l.Allow("fresh") // used after the jump

			// Simulate a janitor sweeping every ttl/2 for 3*ttl of clock time.
			for i := 0; i < 6; i++ {
				clk.Advance(ttl / 2)
				l.evictIdle(clk.Now())
			}
			if l.has("old") {
				t.Fatalf("bucket idle since before a %v backward jump survived 3x IdleTTL of sweeps", jump)
			}
			if l.has("fresh") {
				t.Fatalf("bucket idle for 3x IdleTTL after the jump was not evicted")
			}
		})
	}
}

// The backward-jump rebase done by the janitor must not evict a bucket early
// or alter its balance beyond what its own next call would do.
func TestQAJanitorBackwardRebaseKeepsBalance(t *testing.T) {
	clk := newFakeClock()
	const ttl = time.Minute
	l := qaLimiter(t, 4, 1, ttl, clk)
	defer l.Close()
	l.AllowN("k", 4) // drained at T
	clk.Advance(-time.Hour)
	if n := l.evictIdle(clk.Now()); n != 0 {
		t.Fatalf("evicted %d buckets right after backward jump", n)
	}
	// Still drained: a rebase adds nothing.
	if d := l.Allow("k"); d.Allowed || d.RetryAfter != time.Second {
		t.Fatalf("after rebase: %+v", d)
	}
	clk.Advance(2 * time.Second)
	if d := l.AllowN("k", 0); d.Remaining != 2 {
		t.Fatalf("refill after rebase: Remaining=%d, want 2", d.Remaining)
	}
}

// A forward jump past IdleTTL evicts (expected); the next request sees a full
// bucket, consistent with lazy refill over that interval.
func TestQAClockForwardJumpEvicts(t *testing.T) {
	clk := newFakeClock()
	l := qaLimiter(t, 3, 1, time.Minute, clk)
	defer l.Close()
	l.AllowN("k", 3)
	clk.Advance(1000 * time.Hour)
	if n := l.evictIdle(clk.Now()); n != 1 {
		t.Fatalf("evicted %d, want 1", n)
	}
	if d := l.AllowN("k", 0); d.Remaining != 3 {
		t.Fatalf("after eviction: %+v", d)
	}
}

// --- RetryAfter contract ----------------------------------------------------

// Property: when denied with a finite RetryAfter, advancing the clock by
// exactly RetryAfter must allow the same request. Runs 20000 random cases per
// capacity decade from 1 to 1e7 with rates 1e-6..1e6 tokens/s and fractional n.
//
// Regression for the RetryAfter snap bug: before the fix, 1-3 cases per
// decade from 1e3 upward failed (waiting RetryAfter was short by up to
// 1e-12*rate tokens, more than the 1e-9 epsilon once rate > 1000/s).
// Capacities >= 1e7 are excluded on purpose: there the float64 resolution of
// the balance exceeds the absolute 1e-9 epsilon (TestQAHugeCapacityRetryAfter).
func TestQARetryAfterIsSufficient(t *testing.T) {
	for dec := 0; dec < 7; dec++ {
		rng := rand.New(rand.NewPCG(uint64(dec), 7))
		for i := 0; i < 20000; i++ {
			capacity := math.Pow(10, float64(dec)+rng.Float64())
			rate := math.Pow(10, rng.Float64()*12-6)
			n := rng.Float64() * capacity
			if !retryAfterSufficient(t, capacity, rate, n, rng) {
				t.Fatalf("decade %d case %d: cap=%v rate=%v n=%v: denied after waiting RetryAfter", dec, i, capacity, rate, n)
			}
		}
	}
}

// retryAfterSufficient builds a bucket with a random balance, asks for n and,
// if denied with a finite RetryAfter, waits exactly that long and retries.
func retryAfterSufficient(t *testing.T, capacity, rate, n float64, rng *rand.Rand) bool {
	t.Helper()
	clk := newFakeClock()
	b := mustBucket(t, capacity, rate, clk)
	b.AllowN(capacity * rng.Float64()) // random starting balance
	clk.Advance(time.Duration(rng.Int64N(int64(time.Second))))
	d := b.AllowN(n)
	if d.Allowed {
		return true
	}
	if d.RetryAfter == RetryNever {
		t.Fatalf("RetryNever for n=%v <= capacity=%v rate=%v", n, capacity, rate)
	}
	if d.RetryAfter == time.Duration(math.MaxInt64) {
		// Saturated: the true wait exceeds time.Duration (~292 years) and
		// cannot be represented. Not a contract violation.
		return true
	}
	clk.Advance(d.RetryAfter)
	return b.AllowN(n).Allowed
}

// Design note, measured only: with Capacity >= 1e7 the float64 spacing of the
// balance exceeds the absolute 1e-9 epsilon, so a few percent of denials need
// one more ~1ns retry after waiting RetryAfter.
func TestQAHugeCapacityRetryAfter(t *testing.T) {
	rng := rand.New(rand.NewPCG(9, 9))
	fails, total := 0, 5000
	for i := 0; i < total; i++ {
		capacity := math.Pow(10, 7+rng.Float64()*5)
		if !retryAfterSufficient(t, capacity, math.Pow(10, rng.Float64()*12-6), rng.Float64()*capacity, rng) {
			fails++
		}
	}
	t.Logf("capacity 1e7..1e12: %d/%d cases denied after waiting RetryAfter (float64 resolution, design note)", fails, total)
}

// --- degenerate configuration ------------------------------------------------

func TestQADegenerateConfigRejected(t *testing.T) {
	nan, inf := math.NaN(), math.Inf(1)
	bad := []struct {
		capacity, rate float64
		want           error
	}{
		{0, 1, ErrInvalidCapacity},
		{-1, 1, ErrInvalidCapacity},
		{math.Copysign(0, -1), 1, ErrInvalidCapacity},
		{nan, 1, ErrInvalidCapacity},
		{inf, 1, ErrInvalidCapacity},
		{-inf, 1, ErrInvalidCapacity},
		{1, -1, ErrInvalidRefillRate},
		{1, -math.SmallestNonzeroFloat64, ErrInvalidRefillRate},
		{1, nan, ErrInvalidRefillRate},
		{1, inf, ErrInvalidRefillRate},
	}
	for _, tc := range bad {
		if _, err := NewTokenBucket(tc.capacity, tc.rate); !errors.Is(err, tc.want) {
			t.Errorf("NewTokenBucket(%v,%v) err=%v, want %v", tc.capacity, tc.rate, err, tc.want)
		}
		_, err := NewLimiter(Config{Capacity: tc.capacity, RefillRate: tc.rate, IdleTTL: time.Second})
		if !errors.Is(err, tc.want) {
			t.Errorf("NewLimiter(%v,%v) err=%v, want %v", tc.capacity, tc.rate, err, tc.want)
		}
	}
	for _, ttl := range []time.Duration{0, -1, math.MinInt64} {
		if _, err := NewLimiter(Config{Capacity: 1, RefillRate: 1, IdleTTL: ttl}); !errors.Is(err, ErrInvalidIdleTTL) {
			t.Errorf("IdleTTL=%v err=%v", ttl, err)
		}
	}
	if _, err := NewLimiter(Config{Capacity: 1, RefillRate: 1, IdleTTL: 1, CleanupInterval: -1}); !errors.Is(err, ErrInvalidCleanupInterval) {
		t.Errorf("CleanupInterval=-1 err=%v", err)
	}
	if _, err := NewTokenBucket(1, 1, WithClock(nil)); !errors.Is(err, ErrNilClock) {
		t.Errorf("nil clock err=%v", err)
	}
	// A nil Option is ignored, not a panic.
	if _, err := NewTokenBucket(1, 1, nil); err != nil {
		t.Errorf("nil option: %v", err)
	}
}

// Extreme but valid magnitudes must never produce NaN/Inf balances, negative
// Remaining or RetryAfter, or integer overflow.
func TestQAExtremeMagnitudes(t *testing.T) {
	clk := newFakeClock()

	// Huge rate: elapsed*rate overflows to +Inf, must clamp to capacity.
	b := mustBucket(t, 10, math.MaxFloat64, clk)
	b.AllowN(10)
	clk.Advance(time.Nanosecond)
	if d := b.AllowN(0); d.Remaining != 10 || math.IsInf(b.tokens, 0) || math.IsNaN(b.tokens) {
		t.Fatalf("huge rate: %+v tokens=%v", d, b.tokens)
	}
	b.AllowN(10)
	// Same instant (elapsed 0): empty, and RetryAfter rounds up to 1ns.
	if d := b.Allow(); d.Allowed || d.RetryAfter != 1 {
		t.Fatalf("huge rate, same instant: %+v", d)
	}

	// Tiny rate: RetryAfter saturates at MaxInt64 instead of overflowing.
	b = mustBucket(t, 1, 1e-12, clk)
	b.Allow()
	if d := b.Allow(); d.Allowed || d.RetryAfter != time.Duration(math.MaxInt64) {
		t.Fatalf("tiny rate: %+v", d)
	}
	b = mustBucket(t, 1, math.SmallestNonzeroFloat64, clk)
	b.Allow()
	if d := b.Allow(); d.Allowed || d.RetryAfter != time.Duration(math.MaxInt64) {
		t.Fatalf("denormal rate: %+v", d)
	}

	// Huge capacity: Limit/Remaining clamp to MaxInt, never negative.
	b = mustBucket(t, math.MaxFloat64, 0, clk)
	if d := b.Allow(); !d.Allowed || d.Limit != math.MaxInt || d.Remaining != math.MaxInt {
		t.Fatalf("MaxFloat64 capacity: %+v", d)
	}
	// AllowN with absurd n.
	for _, n := range []float64{math.NaN(), math.Inf(1), math.Inf(-1), -1, -math.SmallestNonzeroFloat64} {
		if d := b.AllowN(n); d.Allowed || d.RetryAfter != RetryNever {
			t.Fatalf("AllowN(%v): %+v", n, d)
		}
	}

	// Clock at the extremes of time.Time: Sub saturates, no panic/NaN.
	clk.Set(time.Time{})
	b = mustBucket(t, 2, 1, clk)
	b.AllowN(2)
	clk.Set(time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC))
	if d := b.AllowN(0); d.Remaining != 2 {
		t.Fatalf("saturated forward elapsed: %+v", d)
	}
	clk.Set(time.Time{})
	if d := b.AllowN(0); d.Remaining != 2 || b.tokens != 2 {
		t.Fatalf("saturated backward elapsed: %+v tokens=%v", d, b.tokens)
	}
}

// Documents a footgun (design note, not a bug): Capacity in (0,1) passes
// validation but Limit reports 0 and Allow() (n=1) can never succeed, with
// RetryAfter == RetryNever. Only AllowN(n <= capacity) works.
func TestQAFractionalCapacityBelowOne(t *testing.T) {
	clk := newFakeClock()
	b := mustBucket(t, 0.5, 100, clk)
	d := b.Allow()
	if d.Allowed || d.Limit != 0 || d.RetryAfter != RetryNever {
		t.Fatalf("capacity 0.5 Allow: %+v", d)
	}
	clk.Advance(time.Hour)
	if d := b.Allow(); d.Allowed {
		t.Fatalf("capacity 0.5 Allow eventually allowed: %+v", d)
	}
	if d := b.AllowN(0.5); !d.Allowed {
		t.Fatalf("capacity 0.5 AllowN(0.5): %+v", d)
	}
}

// Documents float64 precision loss (design note): above 2^53 tokens, consuming
// one token is below the float64 resolution and does not change the balance.
func TestQAHugeCapacityPrecision(t *testing.T) {
	clk := newFakeClock()
	b := mustBucket(t, 1e18, 0, clk)
	before := b.tokens
	for i := 0; i < 100; i++ {
		b.Allow()
	}
	if b.tokens != before {
		t.Logf("capacity 1e18: 100 Allows changed balance %v -> %v", before, b.tokens)
	} else {
		t.Logf("capacity 1e18: 100 Allows did not change the balance (float64 resolution is %v)", math.Nextafter(1e18, 2e18)-1e18)
	}
}

// The keyed Limiter rejects Capacity in (0,1) (Team Lead fix after QA's
// design note): such a Limiter would deny every Allow forever.
func TestQALimiterRejectsCapacityBelowOne(t *testing.T) {
	for _, c := range []float64{0.5, 0.999} {
		_, err := NewLimiter(Config{Capacity: c, RefillRate: 1, IdleTTL: time.Minute})
		if !errors.Is(err, ErrInvalidCapacity) {
			t.Errorf("capacity %v: err=%v, want ErrInvalidCapacity", c, err)
		}
	}
	l, err := NewLimiter(Config{Capacity: 1, RefillRate: 1, IdleTTL: time.Minute})
	if err != nil {
		t.Fatalf("capacity 1: %v", err)
	}
	l.Close()
}
