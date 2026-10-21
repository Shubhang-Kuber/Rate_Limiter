package ratelimiter

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// ErrInvalidConfig is wrapped by every configuration validation error.
var ErrInvalidConfig = errors.New("ratelimiter: invalid config")

// MinInterval is the shortest accepted outflow interval, so OutflowRate is at
// most 1e6/s per key. Every key has its own worker and ticker, and a sub-µs
// ticker is effectively a busy-spinning goroutine per client.
const MinInterval = time.Microsecond

// ErrNoTicket is returned by WaitTicket when the ticket is nil (for example,
// a rejection caused by a closed limiter).
var ErrNoTicket = errors.New("ratelimiter: no ticket (rejected or limiter closed)")

// Decision is the outcome of a Submit.
type Decision struct {
	Allowed bool
	// Limit is the queue capacity.
	Limit int
	// Remaining is the number of free queue slots after this decision
	// (never negative).
	Remaining int
	// RetryAfter is 0 if allowed. On rejection it is the time until the
	// worker's next dequeue frees a slot (next tick - now), in (0, interval]
	// for a live bucket, and 0 if the limiter is closed. It follows the
	// ticker's schedule (start + k×interval). If the worker goroutine is
	// running late for a tick that is already due, the slot may in fact
	// free a little sooner, so the value errs on the conservative side.
	RetryAfter time.Duration
	// Wait is 0 if rejected. If allowed, it estimates the time until this
	// request is dequeued: time to the next tick + (position-1) × interval.
	Wait time.Duration
}

// Config configures a Limiter.
type Config struct {
	// QueueCapacity is the per-key queue size (>= 1).
	QueueCapacity int
	// OutflowRate is the number of requests per second leaving each key's
	// queue (> 0, finite). The interval 1/OutflowRate is rounded up to a
	// whole nanosecond, must fit a time.Duration and be at least MinInterval.
	OutflowRate float64
	// IdleTTL retires a bucket's worker after its queue has been empty for
	// this long (> 0).
	IdleTTL time.Duration
	// CleanupInterval is accepted for API compatibility and must be >= 0.
	// It is unused because workers retire themselves and there is no
	// janitor (see package doc).
	CleanupInterval time.Duration
}

// validate checks cfg and returns the outflow interval.
func (cfg Config) validate() (time.Duration, error) {
	if cfg.QueueCapacity < 1 {
		return 0, fmt.Errorf("%w: QueueCapacity must be >= 1, got %d", ErrInvalidConfig, cfg.QueueCapacity)
	}
	if math.IsNaN(cfg.OutflowRate) || math.IsInf(cfg.OutflowRate, 0) || cfg.OutflowRate <= 0 {
		return 0, fmt.Errorf("%w: OutflowRate must be > 0 and finite, got %v", ErrInvalidConfig, cfg.OutflowRate)
	}
	// Round the interval UP so the real outflow never exceeds the configured
	// rate (truncation would let rate 7e8 run at 1e9/s).
	ns := math.Ceil(float64(time.Second) / cfg.OutflowRate)
	if ns < float64(MinInterval) {
		return 0, fmt.Errorf("%w: OutflowRate %v is too high (interval < %v)", ErrInvalidConfig, cfg.OutflowRate, MinInterval)
	}
	if ns >= math.MaxInt64 {
		return 0, fmt.Errorf("%w: OutflowRate %v is too low (interval overflows time.Duration)", ErrInvalidConfig, cfg.OutflowRate)
	}
	if cfg.IdleTTL <= 0 {
		return 0, fmt.Errorf("%w: IdleTTL must be > 0, got %v", ErrInvalidConfig, cfg.IdleTTL)
	}
	if cfg.CleanupInterval < 0 {
		return 0, fmt.Errorf("%w: CleanupInterval must be >= 0, got %v", ErrInvalidConfig, cfg.CleanupInterval)
	}
	return time.Duration(ns), nil
}

// Limiter is a keyed leaky-bucket limiter: one queue + worker per live key.
// It is safe for concurrent use.
type Limiter struct {
	cfg      Config
	interval time.Duration

	mu      sync.RWMutex // guards buckets and closed; taken before bucket.mu
	buckets map[string]*bucket
	closed  bool

	wg      sync.WaitGroup // one count per live worker goroutine
	retired atomic.Int64   // buckets retired for idleness (observability/tests)
}

// NewLimiter validates cfg and returns a ready Limiter.
func NewLimiter(cfg Config) (*Limiter, error) {
	interval, err := cfg.validate()
	if err != nil {
		return nil, err
	}
	return &Limiter{
		cfg:      cfg,
		interval: interval,
		buckets:  make(map[string]*bucket),
	}, nil
}

// Interval returns the fixed outflow interval (1/OutflowRate).
func (l *Limiter) Interval() time.Duration { return l.interval }

// Submit tries to enqueue a request for key. It never blocks waiting for
// space. If Allowed, the returned channel is closed when the worker dequeues
// the request (the caller may proceed), or when the limiter is closed. If
// rejected, the channel is nil.
func (l *Limiter) Submit(key string) (Decision, <-chan struct{}) {
	// Fast path: existing live bucket, shared map lock only.
	l.mu.RLock()
	b, closed := l.buckets[key], l.closed
	l.mu.RUnlock()
	if closed {
		return l.closedDecision(), nil
	}
	if b != nil {
		if d, t, ok := b.tryEnqueue(); ok {
			return d, t
		}
		// b retired between lookup and enqueue: fall through.
	}

	// Slow path: create (or replace a retired) bucket under the map lock.
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return l.closedDecision(), nil
	}
	if b = l.buckets[key]; b != nil {
		if d, t, ok := b.tryEnqueue(); ok {
			return d, t
		}
		// Retired but not yet removed by its worker. Replace it. The old
		// worker's onRetire only deletes the map entry if it still points
		// at the old bucket.
	}
	b = newBucket(key, l.cfg.QueueCapacity, l.interval)
	// Enqueue before the worker starts, so a tiny IdleTTL cannot retire
	// the fresh bucket before its first request is in.
	d, t, _ := b.tryEnqueue()
	l.buckets[key] = b
	l.wg.Add(1)
	go b.run(l.cfg.IdleTTL, l.removeBucket, l.wg.Done)
	return d, t
}

// removeBucket is called by a worker after it has marked itself retired.
func (l *Limiter) removeBucket(b *bucket) {
	l.mu.Lock()
	if l.buckets[b.key] == b {
		delete(l.buckets, b.key)
	}
	l.mu.Unlock()
	l.retired.Add(1)
}

func (l *Limiter) closedDecision() Decision {
	return Decision{Allowed: false, Limit: l.cfg.QueueCapacity}
}

// Len returns the number of live buckets, which is the number of live worker
// goroutines. A worker that has just retired may be counted for a moment
// until it removes itself.
func (l *Limiter) Len() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.buckets)
}

// Closed reports whether Close has been called. A caller whose ticket fired
// can use it to tell "released at shutdown" from "dequeued normally".
func (l *Limiter) Closed() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.closed
}

// Close stops every worker, releases (closes) every ticket still queued, and
// waits for all worker goroutines to exit. It is idempotent and safe to call
// concurrently. After Close, Submit always rejects.
func (l *Limiter) Close() {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		l.wg.Wait()
		return
	}
	l.closed = true
	bs := make([]*bucket, 0, len(l.buckets))
	for _, b := range l.buckets {
		bs = append(bs, b)
	}
	l.buckets = make(map[string]*bucket)
	l.mu.Unlock()

	for _, b := range bs {
		b.shutdown()
	}
	l.wg.Wait()
}

// WaitTicket blocks until ticket is released (nil error) or ctx is done
// (ctx.Err()). A nil ticket returns ErrNoTicket at once. Giving up does not
// free the queue slot. The request stays queued until the worker dequeues it.
func WaitTicket(ctx context.Context, ticket <-chan struct{}) error {
	if ticket == nil {
		return ErrNoTicket
	}
	select {
	case <-ticket:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
