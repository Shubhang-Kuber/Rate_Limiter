package ratelimiter

import (
	"fmt"
	"sync"
	"time"
)

// MinDefaultCleanupInterval is the lower bound applied when CleanupInterval
// is left at 0 and derived from IdleTTL, so a tiny TTL does not turn the
// janitor into a busy loop. An explicit CleanupInterval is used as given.
const MinDefaultCleanupInterval = time.Second

// Config configures a keyed Limiter. Every key gets its own bucket with the
// same Capacity and RefillRate.
type Config struct {
	Capacity        float64       // max tokens / burst size; >= 1 and finite
	RefillRate      float64       // tokens per second; >= 0 and finite (0 = no refill)
	IdleTTL         time.Duration // evict buckets idle longer than this; > 0
	CleanupInterval time.Duration // janitor period; 0 => max(IdleTTL/2, MinDefaultCleanupInterval)
}

func (c Config) validate() error {
	if err := validateRates(c.Capacity, c.RefillRate); err != nil {
		return err
	}
	// A keyed Limiter needs room for at least one whole request. With a
	// Capacity in (0,1), Allow is denied forever (Limit 0, RetryNever), so a
	// server misconfigured that way would reject all traffic. A standalone
	// TokenBucket still accepts fractional capacities for AllowN use.
	if c.Capacity < 1 {
		return fmt.Errorf("%w: a keyed Limiter needs Capacity >= 1 (got %v)", ErrInvalidCapacity, c.Capacity)
	}
	if c.IdleTTL <= 0 {
		return fmt.Errorf("%w (got %v)", ErrInvalidIdleTTL, c.IdleTTL)
	}
	if c.CleanupInterval < 0 {
		return fmt.Errorf("%w (got %v)", ErrInvalidCleanupInterval, c.CleanupInterval)
	}
	return nil
}

// Limiter is a keyed rate limiter holding one TokenBucket per key, with a
// single background janitor that evicts idle buckets. It is safe for
// concurrent use. Call Close when done to stop the janitor.
type Limiter struct {
	cfg Config // validated, CleanupInterval resolved
	now func() time.Time

	mu      sync.RWMutex // guards buckets (the map only, not bucket contents)
	buckets map[string]*TokenBucket

	stop      chan struct{}
	done      chan struct{}
	closeOnce sync.Once
}

// NewLimiter validates cfg and returns a Limiter with its janitor running.
func NewLimiter(cfg Config, opts ...Option) (*Limiter, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	o, err := buildOptions(opts)
	if err != nil {
		return nil, err
	}
	if cfg.CleanupInterval == 0 {
		cfg.CleanupInterval = max(cfg.IdleTTL/2, MinDefaultCleanupInterval)
	}
	l := &Limiter{
		cfg:     cfg,
		now:     o.now,
		buckets: make(map[string]*TokenBucket),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	go l.janitor()
	return l, nil
}

// Allow consumes one token from key's bucket, creating a full bucket for a
// key not seen before (or evicted).
func (l *Limiter) Allow(key string) Decision { return l.AllowN(key, 1) }

// AllowN consumes n tokens from key's bucket, all-or-nothing. See
// TokenBucket.AllowN for the handling of n.
func (l *Limiter) AllowN(key string, n float64) Decision {
	for {
		b := l.bucket(key)
		b.mu.Lock()
		if b.dead {
			// The janitor evicted this bucket between our lookup and our
			// lock. Using it would apply the consumption to an orphan, so
			// look the key up again (it gets a fresh bucket). This can only
			// happen to a bucket that was idle longer than IdleTTL.
			b.mu.Unlock()
			continue
		}
		d := b.allowLocked(n)
		b.mu.Unlock()
		return d
	}
}

// bucket returns key's bucket, creating it if needed. The map lock is held
// only for the lookup/insert, never during bucket arithmetic.
func (l *Limiter) bucket(key string) *TokenBucket {
	l.mu.RLock()
	b, ok := l.buckets[key]
	l.mu.RUnlock()
	if ok {
		return b
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if b, ok := l.buckets[key]; ok { // another goroutine created it first
		return b
	}
	b = newBucket(l.cfg.Capacity, l.cfg.RefillRate, l.now)
	l.buckets[key] = b
	return b
}

// Len returns the number of live (not yet evicted) buckets.
func (l *Limiter) Len() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.buckets)
}

// Close stops the janitor and waits for it to exit. It is idempotent and safe
// to call concurrently. The Limiter keeps working after Close, but idle
// buckets are no longer evicted.
func (l *Limiter) Close() {
	l.closeOnce.Do(func() { close(l.stop) })
	<-l.done
}

func (l *Limiter) janitor() {
	defer close(l.done)
	t := time.NewTicker(l.cfg.CleanupInterval)
	defer t.Stop()
	for {
		select {
		case <-l.stop:
			return
		case <-t.C:
			l.evictIdle(l.now())
		}
	}
}

// evictIdle removes buckets whose last use is more than IdleTTL before now and
// returns how many were removed.
//
// It holds the map write lock for the sweep so no new lookup can hand out a
// bucket while it is being judged. A bucket whose mutex is held right now is
// in active use and is skipped (TryLock). An evicted bucket is marked dead
// under its own mutex, so a caller that fetched it just before the sweep sees
// the flag when it locks the bucket, and retries instead of consuming from an
// orphan. now is read before the sweep, so a bucket touched after that has
// last > now, a negative idle time, and is kept.
func (l *Limiter) evictIdle(now time.Time) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	evicted := 0
	for key, b := range l.buckets {
		if !b.mu.TryLock() {
			continue
		}
		idle := now.Sub(b.last)
		if idle < -l.cfg.IdleTTL {
			// QA fix: b.last is more than IdleTTL in the future, which only
			// happens when the (injected) clock moved backwards. Without
			// this, a bucket untouched since before the jump was kept until
			// the clock caught up again (forever, for a large jump), leaking
			// every such key. Rebase it exactly as the bucket's own next call
			// would (refillLocked with negative elapsed: no tokens added,
			// last = now), so it becomes evictable IdleTTL from now. The
			// -IdleTTL threshold leaves the benign case alone: a bucket
			// touched after now was read has last slightly after now.
			b.refillLocked(now)
		} else if idle > l.cfg.IdleTTL {
			b.dead = true
			delete(l.buckets, key)
			evicted++
		}
		b.mu.Unlock()
	}
	return evicted
}
