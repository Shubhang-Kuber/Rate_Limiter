package ratelimiter

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"time"
)

// RetryNever is the RetryAfter value of a denied Decision that no amount of
// waiting will turn into an allowed one: the refill rate is 0, or the request
// asks for more tokens than the bucket's capacity, or n is invalid.
const RetryNever time.Duration = -1

// epsilon absorbs float64 rounding in refill arithmetic (for example
// 0.4s * 2.5/s landing at 0.9999999999999999 tokens). A request may succeed
// when the bucket is short by at most 1e-9 of a token; the balance is then
// clamped at 0, so the error never accumulates.
const epsilon = 1e-9

// Decision is the result of one rate-limit check.
type Decision struct {
	// Allowed reports whether the request may proceed. Tokens were consumed
	// only if Allowed is true.
	Allowed bool
	// Limit is the bucket capacity, rounded down to a whole number.
	Limit int
	// Remaining is the number of whole tokens left after this decision
	// (rounded down, never negative).
	Remaining int
	// RetryAfter is 0 when Allowed is true. When denied, it is the exact time
	// until enough tokens will have accumulated for the same request, that is,
	// deficit/rate rounded up to the next nanosecond. It is RetryNever (-1)
	// when the request can never succeed by waiting: the refill rate is 0, the
	// request is larger than capacity, or n is invalid.
	RetryAfter time.Duration
}

// Errors returned by the constructors. Returned errors wrap these, so test
// for them with errors.Is.
var (
	ErrInvalidCapacity        = errors.New("ratelimiter: capacity must be > 0 and finite")
	ErrInvalidRefillRate      = errors.New("ratelimiter: refill rate must be >= 0 and finite")
	ErrInvalidIdleTTL         = errors.New("ratelimiter: idle TTL must be > 0")
	ErrInvalidCleanupInterval = errors.New("ratelimiter: cleanup interval must be >= 0")
	ErrNilClock               = errors.New("ratelimiter: clock function must not be nil")
)

// Option customises a TokenBucket or Limiter.
type Option func(*options)

type options struct {
	now func() time.Time
}

// WithClock replaces the time source (default time.Now). Intended for tests
// and simulations. See the package documentation for how a clock that moves
// backwards is handled.
func WithClock(now func() time.Time) Option {
	return func(o *options) { o.now = now }
}

func buildOptions(opts []Option) (options, error) {
	o := options{now: time.Now}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	if o.now == nil {
		return o, ErrNilClock
	}
	return o, nil
}

func validateRates(capacity, refillRate float64) error {
	if math.IsNaN(capacity) || math.IsInf(capacity, 0) || capacity <= 0 {
		return fmt.Errorf("%w (got %v)", ErrInvalidCapacity, capacity)
	}
	if math.IsNaN(refillRate) || math.IsInf(refillRate, 0) || refillRate < 0 {
		return fmt.Errorf("%w (got %v)", ErrInvalidRefillRate, refillRate)
	}
	return nil
}

// TokenBucket is a single token bucket. It is safe for concurrent use.
//
// It refills lazily (see the package documentation): there is no background
// goroutine, the balance is brought up to date on each call.
type TokenBucket struct {
	mu       sync.Mutex
	capacity float64
	rate     float64 // tokens per second
	now      func() time.Time

	// Guarded by mu.
	tokens float64
	last   time.Time // time of the last refill computation, also "last used"
	dead   bool      // set by Limiter's janitor once evicted from the map
}

// NewTokenBucket returns a full bucket holding capacity tokens and refilling
// at refillRate tokens per second. capacity must be > 0 and finite;
// refillRate must be >= 0 and finite (0 means a fixed allowance that never
// refills).
func NewTokenBucket(capacity, refillRate float64, opts ...Option) (*TokenBucket, error) {
	if err := validateRates(capacity, refillRate); err != nil {
		return nil, err
	}
	o, err := buildOptions(opts)
	if err != nil {
		return nil, err
	}
	return newBucket(capacity, refillRate, o.now), nil
}

// newBucket builds a full bucket from already-validated parameters.
func newBucket(capacity, refillRate float64, now func() time.Time) *TokenBucket {
	return &TokenBucket{
		capacity: capacity,
		rate:     refillRate,
		now:      now,
		tokens:   capacity,
		last:     now(),
	}
}

// Allow consumes one token if available.
func (b *TokenBucket) Allow() Decision { return b.AllowN(1) }

// AllowN consumes n tokens atomically if all n are available, and consumes
// nothing otherwise. n == 0 is always allowed and works as a non-consuming
// probe. A negative, NaN or infinite n is denied with RetryAfter == RetryNever.
func (b *TokenBucket) AllowN(n float64) Decision {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.allowLocked(n)
}

// allowLocked refills and then tries to consume n tokens. b.mu must be held.
func (b *TokenBucket) allowLocked(n float64) Decision {
	b.refillLocked(b.now())

	d := Decision{Limit: floorInt(b.capacity)}
	if math.IsNaN(n) || math.IsInf(n, 0) || n < 0 {
		d.Remaining = floorInt(b.tokens)
		d.RetryAfter = RetryNever
		return d
	}
	if b.tokens+epsilon >= n {
		b.tokens -= n
		if b.tokens < 0 {
			b.tokens = 0
		}
		d.Allowed = true
		d.Remaining = floorInt(b.tokens)
		return d
	}
	d.Remaining = floorInt(b.tokens)
	d.RetryAfter = b.retryAfterLocked(n)
	return d
}

// refillLocked applies lazy refill up to now. b.mu must be held.
//
// This is why there is no per-bucket ticker: the balance at any instant is a
// pure function of (tokens, last, now), so we only compute it when someone
// asks. With 1M clients a ticker design would need 1M goroutines and timers
// running forever even while every client is idle; this costs nothing until
// a request arrives.
func (b *TokenBucket) refillLocked(now time.Time) {
	elapsed := now.Sub(b.last)
	if elapsed < 0 {
		// The (injected) clock went backwards. Add nothing, and rebase the
		// timestamp so that refill resumes as soon as time moves forward
		// again, rather than stalling until the clock passes the old value.
		b.last = now
		return
	}
	if elapsed == 0 {
		return
	}
	if b.rate > 0 && b.tokens < b.capacity {
		b.tokens += elapsed.Seconds() * b.rate
		if b.tokens > b.capacity {
			b.tokens = b.capacity
		}
	}
	b.last = now
}

// retryAfterLocked returns how long until n tokens will be available.
// b.mu must be held and b.tokens must be < n.
func (b *TokenBucket) retryAfterLocked(n float64) time.Duration {
	if b.rate == 0 || n > b.capacity+epsilon {
		return RetryNever
	}
	deficit := n - b.tokens
	exact := deficit / b.rate * float64(time.Second)
	// Round up so that waiting RetryAfter is always enough, but first snap
	// values within 0.001ns of a whole nanosecond to it: float error such as
	// 1000000.0000000009ns must report 1ms, not 1.000001ms.
	//
	// QA fix: snapping down under-reports by (exact-ns) nanoseconds, i.e.
	// (exact-ns)*rate/1e9 tokens. The old comment claimed the 1e-9 token
	// epsilon covers 0.001ns "for any rate below 1000 tokens per
	// nanosecond", but the bound is 1000 tokens per SECOND; above that a
	// caller who waited exactly RetryAfter could be denied again (seen at
	// realistic rates such as 1600/s and 270000/s). Snap down only while
	// the under-report stays within half the epsilon. Snapping up is always
	// safe.
	ns := math.Ceil(exact)
	if r := math.Round(exact); math.Abs(exact-r) <= 1e-3 &&
		(r >= exact || (exact-r)*b.rate/float64(time.Second) <= epsilon/2) {
		ns = r
	}
	if ns >= math.MaxInt64 {
		return time.Duration(math.MaxInt64)
	}
	if ns < 1 {
		ns = 1
	}
	return time.Duration(ns)
}

// floorInt rounds x down to an int (tolerating float error of epsilon),
// clamped to [0, MaxInt].
func floorInt(x float64) int {
	f := math.Floor(x + epsilon)
	if f <= 0 {
		return 0
	}
	if f >= math.MaxInt {
		return math.MaxInt
	}
	return int(f)
}
