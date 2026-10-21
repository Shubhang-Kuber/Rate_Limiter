package ratelimiter

import (
	"math"
	"sync"
	"time"
)

// ticket is the per-request release signal. The worker closes it when the
// request is dequeued, or when the limiter shuts down. close() never blocks,
// so a caller that abandoned its ticket cannot stall the worker.
type ticket = chan struct{}

// bucket is one key's leaky bucket: a bounded FIFO queue (a buffered channel)
// drained by exactly one worker goroutine at a fixed interval.
type bucket struct {
	key      string
	capacity int
	interval time.Duration

	// queue is the leaky bucket itself. Sends happen only under mu with
	// retired == false (see the invariant in doc.go).
	queue chan ticket

	// start is when the ticker was created. Ticker fires happen at
	// start + k*interval (time.Ticker keeps its phase and drops missed
	// ticks rather than bunching them), so this is enough to estimate the
	// next dequeue. It carries a monotonic clock reading.
	start  time.Time
	ticker *time.Ticker

	// stop is closed by Limiter.Close (only) to shut the worker down.
	stop chan struct{}

	mu sync.Mutex
	// retired is true once the bucket can never accept another request.
	// Guarded by mu.
	retired bool
	// lastActive is the last time a request was enqueued or dequeued.
	// Guarded by mu.
	lastActive time.Time
}

// newBucket creates a bucket and its ticker but does not start the worker,
// so the caller can enqueue the first request before the worker can
// possibly decide the bucket is idle.
func newBucket(key string, capacity int, interval time.Duration) *bucket {
	now := time.Now()
	return &bucket{
		key:        key,
		capacity:   capacity,
		interval:   interval,
		queue:      make(chan ticket, capacity),
		start:      now,
		ticker:     time.NewTicker(interval),
		stop:       make(chan struct{}),
		lastActive: now,
	}
}

// untilNextTick returns the time from now until the ticker's next fire, in
// (0, interval]. It uses monotonic time (time.Since on a time.Now value), so
// wall-clock jumps do not affect it. A negative elapsed value is impossible
// with a monotonic clock, but it is clamped defensively anyway.
func (b *bucket) untilNextTick(now time.Time) time.Duration {
	elapsed := now.Sub(b.start)
	if elapsed < 0 {
		elapsed = 0
	}
	next := (elapsed/b.interval + 1) * b.interval
	d := next - elapsed
	if d < 0 {
		d = 0
	}
	return d
}

// queueWait returns first + (pos-1)*interval, saturating at the maximum
// Duration.
//
// QA fix: the plain expression overflowed int64 for slow outflow rates that
// validation accepts (for example OutflowRate 1e-9 gives an interval of
// 1e18ns), so a request at queue position >= 10 was told a negative Wait.
func queueWait(first time.Duration, pos int, interval time.Duration) time.Duration {
	const maxWait = time.Duration(math.MaxInt64)
	ahead := time.Duration(pos - 1)
	if ahead <= 0 {
		return first
	}
	if interval > (maxWait-first)/ahead {
		return maxWait
	}
	return first + ahead*interval
}

// tryEnqueue attempts a non-blocking enqueue. ok is false only if the bucket
// is retired, in which case the caller must use a fresh bucket. Otherwise the
// returned Decision says whether the queue had room.
func (b *bucket) tryEnqueue() (d Decision, t <-chan struct{}, ok bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.retired {
		return Decision{}, nil, false
	}
	now := time.Now()
	tk := make(ticket)
	select {
	case b.queue <- tk:
		b.lastActive = now
		// The worker dequeues under mu too, so len(queue) is exact here:
		// this request is at position pos (1-based) in the line.
		pos := len(b.queue)
		return Decision{
			Allowed:   true,
			Limit:     b.capacity,
			Remaining: max(b.capacity-pos, 0),
			Wait:      queueWait(b.untilNextTick(now), pos, b.interval),
		}, tk, true
	default:
		// Queue full: reject immediately, never wait for space.
		return Decision{
			Allowed:    false,
			Limit:      b.capacity,
			Remaining:  max(b.capacity-len(b.queue), 0),
			RetryAfter: b.untilNextTick(now),
		}, nil, true
	}
}

// run is the bucket's worker. It dequeues one request per tick, releases it
// in FIFO order, and retires itself after idleTTL with an empty queue.
// onRetire removes the bucket from the limiter's map. done is called just
// before the goroutine exits.
func (b *bucket) run(idleTTL time.Duration, onRetire func(*bucket), done func()) {
	defer done()
	defer b.ticker.Stop() // no leaked tickers, whichever way we exit

	for {
		select {
		case <-b.stop:
			b.drain()
			return
		case now := <-b.ticker.C:
			b.mu.Lock()
			select {
			case tk := <-b.queue:
				b.lastActive = now
				b.mu.Unlock()
				close(tk) // release: the caller may proceed
			default:
				// Queue is empty, and it cannot change while we hold mu
				// because all sends happen under mu.
				if now.Sub(b.lastActive) >= idleTTL {
					b.retired = true
					b.mu.Unlock()
					onRetire(b)
					return
				}
				b.mu.Unlock()
			}
		}
	}
}

// shutdown marks the bucket retired so nothing more can be enqueued, then
// tells the worker to stop. Called only by Limiter.Close, once per bucket.
func (b *bucket) shutdown() {
	b.mu.Lock()
	b.retired = true
	b.mu.Unlock()
	close(b.stop)
}

// drain releases every request still queued. It runs only after retired is
// set, so no new sends can race with it.
func (b *bucket) drain() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for {
		select {
		case tk := <-b.queue:
			close(tk)
		default:
			return
		}
	}
}
