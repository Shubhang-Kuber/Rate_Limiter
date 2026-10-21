package ratelimiter

import (
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func mustBucket(t *testing.T, capacity, rate float64, clk *fakeClock) *TokenBucket {
	t.Helper()
	var opts []Option
	if clk != nil {
		opts = append(opts, WithClock(clk.Now))
	}
	b, err := NewTokenBucket(capacity, rate, opts...)
	if err != nil {
		t.Fatalf("NewTokenBucket(%v, %v): %v", capacity, rate, err)
	}
	return b
}

func TestBucketBurstThenDeny(t *testing.T) {
	clk := newFakeClock()
	b := mustBucket(t, 5, 1, clk)
	for i := 0; i < 5; i++ {
		d := b.Allow()
		if !d.Allowed {
			t.Fatalf("request %d denied within burst", i)
		}
		if d.Limit != 5 || d.Remaining != 4-i || d.RetryAfter != 0 {
			t.Fatalf("request %d: got %+v", i, d)
		}
	}
	d := b.Allow()
	if d.Allowed || d.Remaining != 0 || d.Limit != 5 {
		t.Fatalf("request past burst: got %+v", d)
	}
}

func TestBucketRefillFakeClock(t *testing.T) {
	clk := newFakeClock()
	b := mustBucket(t, 5, 1, clk)
	for i := 0; i < 5; i++ {
		b.Allow()
	}
	clk.Advance(999 * time.Millisecond)
	if b.Allow().Allowed {
		t.Fatal("allowed before a full token accumulated")
	}
	clk.Advance(time.Millisecond)
	if !b.Allow().Allowed {
		t.Fatal("denied after exactly 1s at 1 token/s")
	}
	if b.Allow().Allowed {
		t.Fatal("second request allowed with only one refilled token")
	}
	clk.Advance(3 * time.Second)
	for i := 0; i < 3; i++ {
		if !b.Allow().Allowed {
			t.Fatalf("refilled request %d denied", i)
		}
	}
	if b.Allow().Allowed {
		t.Fatal("allowed more than refilled")
	}
}

func TestBucketRefillRealTime(t *testing.T) {
	// 50 tokens/s = one token every 20ms.
	b := mustBucket(t, 2, 50, nil)
	if !b.Allow().Allowed || !b.Allow().Allowed {
		t.Fatal("initial burst denied")
	}
	time.Sleep(100 * time.Millisecond)
	for i := 0; i < 2; i++ {
		if !b.Allow().Allowed {
			t.Fatalf("request %d denied after real refill", i)
		}
	}
	// Even after a long sleep the balance is capped at capacity.
	time.Sleep(100 * time.Millisecond)
	if d := b.Allow(); !d.Allowed || d.Remaining > 1 {
		t.Fatalf("after cap-limited refill got %+v", d)
	}
}

func TestBucketFractionalRate(t *testing.T) {
	clk := newFakeClock()
	b := mustBucket(t, 10, 2.5, clk)
	for i := 0; i < 10; i++ {
		b.Allow()
	}
	clk.Advance(400 * time.Millisecond) // 0.4s * 2.5/s = exactly 1 token
	if !b.Allow().Allowed {
		t.Fatal("1 token at 2.5/s after 400ms should be available")
	}
	if b.Allow().Allowed {
		t.Fatal("only one token should have refilled")
	}
	clk.Advance(time.Second) // 2.5 tokens
	if d := b.Allow(); !d.Allowed || d.Remaining != 1 {
		t.Fatalf("after 1s: %+v", d)
	}
	if !b.Allow().Allowed {
		t.Fatal("second of 2.5 tokens denied")
	}
	if b.Allow().Allowed { // 0.5 left
		t.Fatal("half a token must not be enough")
	}
	clk.Advance(200 * time.Millisecond) // 0.5 + 0.5 = 1
	if !b.Allow().Allowed {
		t.Fatal("0.5 + 0.2s*2.5 = 1 token, should be allowed")
	}
	b.mu.Lock()
	tokens := b.tokens
	b.mu.Unlock()
	if math.Abs(tokens) > 1e-9 {
		t.Fatalf("tokens = %v, want 0", tokens)
	}
}

func TestBucketNeverExceedsCapacity(t *testing.T) {
	clk := newFakeClock()
	b := mustBucket(t, 3, 100, clk)
	b.Allow()
	clk.Advance(24 * time.Hour)
	if d := b.AllowN(0); d.Remaining != 3 {
		t.Fatalf("after long idle Remaining = %d, want 3", d.Remaining)
	}
	b.mu.Lock()
	tokens := b.tokens
	b.mu.Unlock()
	if tokens != 3 {
		t.Fatalf("tokens = %v, want exactly capacity 3", tokens)
	}
	allowed := 0
	for i := 0; i < 10; i++ {
		if b.Allow().Allowed {
			allowed++
		}
	}
	if allowed != 3 {
		t.Fatalf("allowed %d after idle, want 3", allowed)
	}
}

func TestBucketRetryAfterFakeClock(t *testing.T) {
	clk := newFakeClock()
	b := mustBucket(t, 1, 2, clk)
	if !b.Allow().Allowed {
		t.Fatal("first request denied")
	}
	d := b.Allow()
	if d.Allowed || d.RetryAfter != 500*time.Millisecond {
		t.Fatalf("got %+v, want denied with RetryAfter 500ms", d)
	}
	clk.Advance(499 * time.Millisecond)
	d = b.Allow()
	if d.Allowed || d.RetryAfter != time.Millisecond {
		t.Fatalf("after 499ms got %+v, want RetryAfter 1ms", d)
	}
	clk.Advance(time.Millisecond)
	if !b.Allow().Allowed {
		t.Fatal("denied after waiting exactly RetryAfter")
	}

	// Non-terminating rate: 1/3 s rounds up to the next nanosecond, so
	// waiting RetryAfter is always sufficient.
	b3 := mustBucket(t, 1, 3, clk)
	b3.Allow()
	d = b3.Allow()
	if d.RetryAfter != 333333334*time.Nanosecond {
		t.Fatalf("rate 3: RetryAfter = %v, want 333.333334ms", d.RetryAfter)
	}
	clk.Advance(d.RetryAfter)
	if !b3.Allow().Allowed {
		t.Fatal("rate 3: denied after waiting RetryAfter")
	}
}

func TestBucketRetryAfterRealClock(t *testing.T) {
	b := mustBucket(t, 1, 2, nil)
	b.Allow()
	d := b.Allow()
	if d.Allowed {
		t.Fatal("second request allowed")
	}
	if d.RetryAfter <= 400*time.Millisecond || d.RetryAfter > 500*time.Millisecond {
		t.Fatalf("RetryAfter = %v, want ~500ms", d.RetryAfter)
	}
}

func TestBucketRateZero(t *testing.T) {
	clk := newFakeClock()
	b := mustBucket(t, 3, 0, clk)
	for i := 0; i < 3; i++ {
		if !b.Allow().Allowed {
			t.Fatalf("allowance request %d denied", i)
		}
	}
	clk.Advance(365 * 24 * time.Hour)
	d := b.Allow()
	if d.Allowed || d.RetryAfter != RetryNever || d.Remaining != 0 {
		t.Fatalf("rate 0 after allowance: got %+v, want denied with RetryNever", d)
	}
}

func TestBucketAllowN(t *testing.T) {
	clk := newFakeClock()
	b := mustBucket(t, 5, 1, clk)
	if d := b.AllowN(3); !d.Allowed || d.Remaining != 2 {
		t.Fatalf("AllowN(3): %+v", d)
	}
	d := b.AllowN(3)
	if d.Allowed || d.Remaining != 2 || d.RetryAfter != time.Second {
		t.Fatalf("AllowN(3) short by 1: %+v (must consume nothing)", d)
	}
	if d := b.AllowN(2); !d.Allowed || d.Remaining != 0 {
		t.Fatalf("AllowN(2): %+v", d)
	}
	if d := b.AllowN(0); !d.Allowed {
		t.Fatalf("AllowN(0) must be allowed: %+v", d)
	}
	if d := b.AllowN(6); d.Allowed || d.RetryAfter != RetryNever {
		t.Fatalf("AllowN(6) > capacity: %+v, want RetryNever", d)
	}
	for _, n := range []float64{-1, math.NaN(), math.Inf(1), math.Inf(-1)} {
		if d := b.AllowN(n); d.Allowed || d.RetryAfter != RetryNever {
			t.Fatalf("AllowN(%v): %+v, want denied with RetryNever", n, d)
		}
	}
	clk.Advance(1500 * time.Millisecond)
	if d := b.AllowN(1.5); !d.Allowed || d.Remaining != 0 {
		t.Fatalf("AllowN(1.5) after 1.5s: %+v", d)
	}
}

func TestBucketClockBackward(t *testing.T) {
	clk := newFakeClock()
	start := clk.Now()
	b := mustBucket(t, 5, 1, clk)
	clk.Advance(100 * time.Second)
	for i := 0; i < 5; i++ {
		b.Allow()
	}
	clk.Set(start.Add(50 * time.Second)) // jump back 50s
	d := b.Allow()
	if d.Allowed || d.Remaining != 0 {
		t.Fatalf("backward jump must not refill: %+v", d)
	}
	if d.RetryAfter != time.Second {
		t.Fatalf("RetryAfter after backward jump = %v, want 1s", d.RetryAfter)
	}
	b.mu.Lock()
	last := b.last
	b.mu.Unlock()
	if !last.Equal(start.Add(50 * time.Second)) {
		t.Fatalf("timestamp not rebased: last = %v", last)
	}
	// Refill resumes immediately as time moves forward from the new value.
	clk.Advance(time.Second)
	if !b.Allow().Allowed {
		t.Fatal("refill did not resume after backward jump")
	}
}

func TestBucketConcurrentNoRefill(t *testing.T) {
	const capacity = 100
	cases := []struct {
		name string
		b    *TokenBucket
	}{
		{"rate0", mustBucket(t, capacity, 0, nil)},
		{"frozenClock", mustBucket(t, capacity, 1000, newFakeClock())},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			const goroutines, perG = 600, 5
			var allowed atomic.Int64
			var wg sync.WaitGroup
			start := make(chan struct{})
			for g := 0; g < goroutines; g++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					for i := 0; i < perG; i++ {
						if tc.b.Allow().Allowed {
							allowed.Add(1)
						}
					}
				}()
			}
			close(start)
			wg.Wait()
			if got := allowed.Load(); got != capacity {
				t.Fatalf("allowed %d, want exactly %d", got, capacity)
			}
		})
	}
}

func TestNewTokenBucketValidation(t *testing.T) {
	cases := []struct {
		name     string
		capacity float64
		rate     float64
		want     error
	}{
		{"zero capacity", 0, 1, ErrInvalidCapacity},
		{"negative capacity", -1, 1, ErrInvalidCapacity},
		{"NaN capacity", math.NaN(), 1, ErrInvalidCapacity},
		{"inf capacity", math.Inf(1), 1, ErrInvalidCapacity},
		{"negative rate", 1, -0.5, ErrInvalidRefillRate},
		{"NaN rate", 1, math.NaN(), ErrInvalidRefillRate},
		{"inf rate", 1, math.Inf(1), ErrInvalidRefillRate},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := NewTokenBucket(tc.capacity, tc.rate)
			if !errors.Is(err, tc.want) || b != nil {
				t.Fatalf("got (%v, %v), want error %v", b, err, tc.want)
			}
		})
	}
	if _, err := NewTokenBucket(1, 1, WithClock(nil)); !errors.Is(err, ErrNilClock) {
		t.Fatalf("nil clock: got %v", err)
	}
	if b, err := NewTokenBucket(0.5, 0); err != nil || b == nil {
		t.Fatalf("rate 0 / fractional capacity should be valid: %v", err)
	}
}
