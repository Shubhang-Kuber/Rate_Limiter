// Package ratelimiter implements a keyed leaky-bucket rate limiter in the
// "leaky bucket as a queue" form.
//
// # Model
//
// Every key (client) owns a bucket made of two things:
//
//   - a bounded, buffered channel that is the bucket's FIFO queue, with room
//     for Config.QueueCapacity waiting requests, and
//   - exactly one worker goroutine driven by a time.Ticker that fires every
//     1/Config.OutflowRate seconds. On each tick the worker dequeues at most
//     one request and "processes" it by closing that request's ticket channel,
//     which tells the waiting caller it may proceed now.
//
// Requests therefore leave in FIFO order at a fixed rate no matter how
// bursty the arrivals are. That smoothing is the whole point of the
// algorithm. Submit never blocks: if the queue is full, the request is
// rejected immediately with a non-blocking select/default send.
//
// This differs from a token bucket in kind, not just in arithmetic. A token
// bucket admits a burst at once and computes allowance lazily from
// timestamps. Here admission only means "you hold a place in line". The
// caller is released later by an active goroutine, at the outflow rate.
//
// # Known con: old requests hog the queue
//
// A burst that fills the queue occupies every slot. A later, perfectly
// legitimate request is rejected outright. Even once a slot frees, it waits
// behind the whole backlog, roughly (queue depth × interval). Requests whose
// callers already gave up (see "Abandoned tickets") still hold their slots
// until the worker reaches them. This is inherent to the algorithm, and
// TestQueueFullWithOldRequests reproduces it.
//
// # Idle-worker mitigation, and why it is option (a)
//
// A queue+worker bucket needs one live goroutine (plus its ticker) per
// bucket, so it cannot be created lazily and left lying around for millions
// of idle clients the way a lazy-refill token bucket (a struct with two
// numbers) can. Two mitigations exist:
//
//	(a) retire idle buckets: stop the worker and its ticker and remove the
//	    bucket from the map once its queue has been empty for >= IdleTTL.
//	    The next request for that key builds a fresh bucket.
//	(b) a "virtual queue": drop the channel and worker and keep, per key,
//	    the theoretical time at which the queue drains (GCRA-style). A
//	    request is admitted if that time is <= now+capacity×interval, and
//	    told to wait (tat - now).
//
// This package uses (a). Option (b) is a fine meter, but it removes the
// bounded channel and the worker goroutine, which are the architecture this
// package is required to provide: a real FIFO queue drained by a fixed-rate
// worker that actively releases callers. (b) would turn the limiter into
// timestamp arithmetic in which callers sleep on their own. That amounts to
// a token bucket/GCRA with different math. (a) keeps the real queue and
// only bounds how long an unused one stays alive.
//
// Residual cost of (a): every *active* client (one with queued requests, or
// one used within the last IdleTTL) still costs one goroutine, one ticker,
// and one channel buffer of QueueCapacity slots. Memory is therefore
// O(active clients × QueueCapacity), plus goroutine stacks, and an idle
// bucket keeps ticking (waking once per interval) until it retires. Idle
// detection is checked on ticks, so retirement happens within one interval
// after IdleTTL elapses. With very low outflow rates that can take a while.
//
// # Retire-vs-enqueue race
//
// The danger is a request being put into a bucket whose worker has already
// exited. Such a ticket would never be released. It is prevented by one
// invariant:
//
//	A send into bucket.queue happens only while holding bucket.mu and only if
//	bucket.retired is false. bucket.retired is set only while holding
//	bucket.mu, and the worker sets it only after observing (under the same
//	lock) that the queue is empty. The worker dequeues under bucket.mu too.
//
// So once a bucket is marked retired, its queue is empty and can never
// receive another item. After that the worker removes the bucket from the
// limiter's map (only if the map still points at that same bucket) and exits.
// A Submit that finds a retired bucket, whether it looked it up just before
// retirement or the map has not been cleaned yet, falls back to a slow path
// under the limiter's map lock. There it installs a fresh bucket and worker.
//
// Lock order is always Limiter.mu -> bucket.mu. The worker never holds
// bucket.mu while acquiring Limiter.mu, so the two cannot deadlock.
//
// No janitor goroutine is used. Each worker retires itself, so
// Config.CleanupInterval is accepted (and validated as >= 0) for API
// compatibility but currently unused.
//
// # Close semantics
//
// Limiter.Close is idempotent. It marks the limiter closed, marks every
// bucket retired (so nothing more can be enqueued), stops every worker, and
// waits for all of them to exit. Each worker stops its ticker on the way out.
// Requests still queued at Close are RELEASED: their ticket channels are
// closed, exactly as if they had been dequeued, so no caller hangs forever.
// A caller that needs to tell "released by Close" from "released by the
// worker" can check Limiter.Closed. After Close, Submit returns a rejection
// (Allowed=false, RetryAfter=0) with a nil ticket, and Closed reports true.
//
// # Abandoned tickets
//
// A caller may stop waiting (client disconnect) by selecting on its ticket
// together with ctx.Done(), or by using WaitTicket. The abandoned request
// keeps its queue slot until the worker dequeues it. That is textbook
// leaky-bucket behaviour: the queue holds requests, not callers. The worker
// releases a ticket by close(), which never blocks, so an abandoned ticket
// can never stall the worker.
//
// # Clock
//
// The outflow cadence comes from time.Ticker, which runs on the runtime's
// monotonic clock and does not jump when the wall clock is changed. The
// Wait and RetryAfter estimates use time.Since on a time.Now() taken at
// bucket creation, which also carries a monotonic reading, and negative
// values are clamped to 0.
package ratelimiter
