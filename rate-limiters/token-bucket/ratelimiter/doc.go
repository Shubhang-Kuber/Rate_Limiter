// Package ratelimiter implements a token-bucket rate limiter: a single
// [TokenBucket] and a keyed [Limiter] that holds one bucket per client key.
//
// # Algorithm
//
// A bucket holds up to Capacity tokens (the burst size) and gains RefillRate
// tokens per second. Each request consumes one token (or n with AllowN); a
// request is allowed only if enough tokens are present, otherwise it is denied
// and nothing is consumed (all-or-nothing). Token counts are float64 so that
// fractional rates such as 2.5 tokens/second refill exactly rather than being
// rounded to whole tokens per tick.
//
// # Lazy refill (the central design decision)
//
// Buckets are refilled lazily: there is no ticker and no goroutine per bucket.
// Each bucket remembers the timestamp of its last update; on every call it
// computes elapsed = now - last, adds elapsed*rate tokens (capped at Capacity)
// and moves last to now. The result is mathematically identical to a ticker
// that adds tokens continuously, but the cost is O(1) arithmetic per request
// and zero cost while a bucket is idle.
//
// The alternative, a ticker goroutine per bucket, does not scale: with one
// million clients it means one million mostly idle goroutines (each with its
// own stack, at least 2 KiB, so gigabytes of memory) and one million timers
// waking the scheduler, all to update buckets that nobody is reading. It also
// quantises refill to the tick period and needs its own shutdown logic per
// bucket. Lazy refill avoids all of that.
//
// # Locking
//
// Each TokenBucket has its own mutex; refill-and-consume happens atomically
// under it, so concurrent callers can never over-spend a bucket. The Limiter
// guards its key->bucket map with a sync.RWMutex used only for lookup and
// insertion. The map lock is released before the bucket's math runs, so
// requests for different keys only contend briefly on the read lock, never on
// each other's buckets. Lock order is always map lock -> bucket lock (only the
// janitor holds both); callers never hold the bucket lock while acquiring the
// map lock, so deadlock is impossible.
//
// # Idle eviction and the eviction race
//
// A Limiter runs exactly one janitor goroutine that periodically removes
// buckets whose last use is older than IdleTTL, so memory is bounded by the
// number of recently active keys. Close stops it.
//
// Race: a caller can look a bucket up, release the map lock, and be about to
// lock the bucket when the janitor evicts it. If the caller then consumed from
// that orphaned bucket, its update would be lost and a later request would
// create a brand-new full bucket. This is prevented: the janitor marks a
// bucket dead (under the bucket's own mutex, while holding the map lock)
// before deleting it, and a caller that locks a dead bucket discards it and
// retries the lookup. The janitor also uses TryLock and skips any bucket that
// is locked at that instant, since a locked bucket is by definition in use.
// So no consumption is ever applied to an evicted bucket.
//
// What remains is that an evicted key, on its next request, gets a fresh full
// bucket. That is exact, not an approximation, whenever
// IdleTTL >= Capacity/RefillRate (the time to refill from empty to full):
// a bucket idle for longer than that would have lazily refilled to Capacity
// anyway, so dropping it and recreating it full is indistinguishable.
//
// If IdleTTL < Capacity/RefillRate, eviction can hand a client more tokens
// than the pure algorithm would: a client that drains its bucket and then
// stays idle just over IdleTTL comes back to a full bucket instead of a
// partially refilled one. Its effective long-run rate is then bounded by
// roughly Capacity per IdleTTL rather than RefillRate. With RefillRate == 0
// (time-to-full is infinite) eviction turns a one-time allowance into an
// allowance that resets after IdleTTL of inactivity. Choose
// IdleTTL >= Capacity/RefillRate if exactness matters.
//
// # Clock
//
// Time comes from an injectable clock (see [WithClock]); the default is
// time.Now, whose readings carry Go's monotonic clock, so elapsed time is
// immune to wall-clock adjustments. With an injected clock that moves
// backwards, a call sees a negative elapsed time: it adds no tokens and
// rebases the bucket's timestamp to the new (earlier) time. It never blocks,
// panics or freezes refill. Rebasing means the interval between the new time
// and the old timestamp can be credited a second time once the clock moves
// forward again. That over-credit is bounded by the size of the backwards jump
// and can never take the bucket above Capacity. The alternative, keeping the
// old timestamp, would stop refill entirely until the clock caught up, which
// for a large jump would lock a client out for hours. The janitor applies
// the same rebase to any bucket whose timestamp is more than IdleTTL ahead
// of the clock, so a backwards jump cannot keep idle buckets alive (and
// leak their keys) until the clock catches up; such a bucket is evicted
// within about 2×IdleTTL plus one cleanup interval.
//
// # Rate 0
//
// RefillRate == 0 is valid and means a fixed, non-refilling allowance of
// Capacity tokens. Denied decisions then report RetryAfter == [RetryNever].
package ratelimiter
