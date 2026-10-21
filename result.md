# Rate Limiters: Backend Build Result

Written by the Team Lead after every agent's work was reviewed and accepted. **Date:** 2026-10-21.

Scope was backend only:
- two independent Go modules, `token-bucket/` (module `tokenbucket`) and `leaky-bucket/` (module `leakybucket`)
- in each: a core `ratelimiter` package, an HTTP server with rate-limit middleware in `cmd/server`, and test suites

No frontend, visualisation or CLI demo tooling was built.

## Final gate (Team Lead)

**Toolchain.** Go is not installed on the Windows host, and `-race` on Windows also needs a C compiler. With the user's approval, every build and test ran in Docker using `golang:1.23` (go1.23.12 linux/amd64). Both modules use only the standard library and have no third-party dependencies.

**Command,** per module: `gofmt -l . ; go build ./... ; go vet ./... ; go test -race -count=3 ./...`

```
=== token-bucket
gofmt -l: []
build: ok
vet: ok
ok   tokenbucket/cmd/server   1.038s
ok   tokenbucket/ratelimiter  10.593s
=== leaky-bucket
gofmt -l: []
build: ok
vet: ok
ok   leakybucket/cmd/server   4.965s
ok   leakybucket/ratelimiter  9.218s
GATE EXIT=0
```
Each package's suite ran 3 times under the race detector (`-count=3`) with no failures. `gofmt` reported no files in either module.

**Review checklist (Team Lead)**

| Check | Token bucket | Leaky bucket |
|---|---|---|
| Matches the textbook algorithm | Lazy refill, capped at capacity, all-or-nothing consume | Bounded FIFO channel, one worker dequeuing per fixed tick, reject when full |
| Thread safety | Per-bucket mutex. The map RWMutex is held only for lookup and insert. Eviction uses `TryLock` + a `dead` flag + caller retry | Every send into the queue happens under `bucket.mu` with `retired == false`. Lock order is always map, then bucket |
| No goroutine leaks | 100k keys add 0 goroutines; the janitor exits on `Close` | 10k keys: 10k workers, all of which retire back to the baseline count. `Close` releases every queued request |
| No silent error swallowing | Constructors return wrapped sentinel errors; servers exit 2 on bad config | Same |
| Memory under "millions of distinct clients" | Idle buckets are evicted after TTL. Live memory ≈ key arrival rate × TTL × ~150 B (see limitations) | Idle workers retire after TTL. Live cost is a goroutine + ticker + queue per active key (see limitations) |

## 1. Team Lead / Architect

**Assigned:** plan the work, fix the API contracts, assign scopes, review every handoff, have final say on deviations, run the final gate, write this file.

**Did:**
- **Toolchain and contracts.** Set up the Docker toolchain. Fixed the exported API of both `ratelimiter` packages up front, so the HTTP engineer could code against them while QA worked.
- **Scheduling.** Ran the two algorithm engineers in parallel, then the HTTP and QA agents in parallel on disjoint files.
- **Reviews.** Read all implementation code at each handoff.
- **Two fixes, made after QA's design notes:**
  1. **Leaky bucket: interval rounded up, with a minimum** (`leaky-bucket/ratelimiter/limiter.go`).
     - The outflow interval is now `ceil(1s / OutflowRate)` instead of truncated, so the real outflow never exceeds the configured rate. Before, rate 7e8 ran at 1e9/s.
     - A new exported `MinInterval = 1µs` caps the rate at 1e6/s per key, because a sub-µs ticker means a busy-spinning goroutine per client.
     - QA's tests were updated to assert the rounded-up interval (rate 3 → 333.333334ms).
  2. **Token bucket: the keyed `Limiter` requires `Capacity >= 1`** (`token-bucket/ratelimiter/limiter.go`).
     - With a capacity in (0,1), every `Allow()` was denied forever, so a misconfigured server would reject all traffic.
     - A standalone `TokenBucket` still accepts fractional capacities, which are useful with `AllowN`.
     - Test added: `TestQALimiterRejectsCapacityBelowOne`.

**Process note.** The QA agent once ran `docker ps -q | docker kill`, which killed every running container. That included one that probably belonged to the HTTP engineer. Docker Desktop had been stopped before this session, so only the team's containers were affected. Everything was re-run in the final gate above.

## 2. Token Bucket Engineer

**Assigned:** `token-bucket/ratelimiter/`, meaning a lazy-refill float64 bucket, a keyed limiter with a janitor, and the edge-case tests.

**Built:**
- **Files.** `go.mod`, `ratelimiter/doc.go`, `bucket.go`, `limiter.go`, `bucket_test.go`, `limiter_test.go`, and `clock_test.go` (a fake clock).
- **Lazy refill** (`refillLocked`). Tokens are brought up to date on each call as `elapsed × rate`, capped at capacity. There is no ticker or goroutine per bucket; the comment explains that 1M clients would otherwise mean 1M idle goroutines.
- **float64 tokens.** A 1e-9 epsilon absorbs float error, so 2.5/s refills exactly.
- **Keyed `Limiter`.**
  - The `RWMutex` map is held only for lookup and insert, never during the bucket math.
  - ONE janitor goroutine evicts buckets idle longer than `IdleTTL`.
  - `Close` is idempotent and concurrency-safe.
- **Eviction race closed.**
  - The janitor `TryLock`s each bucket, so busy buckets are skipped.
  - It marks evicted buckets `dead` under the bucket mutex.
  - A caller holding a stale pointer sees `dead` and re-looks-up the key.
  - `doc.go` explains why re-creating a full bucket is exact when `IdleTTL >= Capacity/RefillRate`, and what happens when it isn't.
- **`RetryAfter` is the exact deficit ÷ rate**, rounded up to the nanosecond. When waiting can never help (rate 0, n > capacity, or invalid n), it is the sentinel `RetryNever = -1`.
- **Clock is injectable** (`WithClock`, default monotonic `time.Now`). If the clock goes backwards, no tokens are added and the timestamp is rebased, so there is no lock-up.

**Deviations:** constructors take an extra variadic `opts ...Option` for the clock. This is additive and doesn't break the contract.

**Tests:** 21 test functions. Highlights:
- 600 goroutines × 5 calls on one capacity-100 bucket (with rate 0, and with a frozen clock) allow **exactly** 100.
- Per-key isolation; real-time and fake-clock refill; 2.5/s exactness; capacity never exceeded after 24h of idling.
- RetryAfter exactly 500ms / 1ms / 333.333334ms.
- Janitor eviction (real time and deterministic); the eviction race; goroutine count back at baseline after `Close`.
- Config validation.

All pass under `-race`.

**Limitations it flagged:**
- The janitor holds the map write lock for the whole sweep (see Next steps).
- No test deterministically forces the exact "fetched → evicted → locked" interleaving. The stress test covers it under `-race`, but can't prove that branch ran.

## 3. Leaky Bucket Engineer

**Assigned:** `leaky-bucket/ratelimiter/`, meaning a genuine FIFO queue drained at a fixed outflow rate by one worker per bucket, immediate reject when full, an idle-worker mitigation (with the choice justified in code), and tests that measure real timing.

**Built:**
- **Files.** `go.mod`, `ratelimiter/doc.go`, `bucket.go`, `limiter.go`, `limiter_test.go`.
- **Bucket.** A `chan chan struct{}` bounded queue plus one worker goroutine on a `time.Ticker`. The worker dequeues one request per `1/OutflowRate` and *releases* it by closing that request's ticket channel.
- **Admission.** Enqueue is a non-blocking `select`/`default`, so a full queue rejects at once.
- **API.** `Submit(key) (Decision, <-chan struct{})` plus `WaitTicket(ctx, ticket)`, `Closed()` and `Interval()`.
- **Mitigation: option (a), self-retiring workers.** A worker exits when its queue has been empty for `IdleTTL`.
  - `doc.go` justifies (a) over (b), the lazy virtual-queue/GCRA approach: (b) would remove the channel-and-worker structure the spec requires, making it token-bucket-style timestamp maths.
  - `doc.go` also documents the residual cost: O(active clients × QueueCapacity) memory, plus one goroutine and one ticker per active key.
- **Retire-vs-enqueue race.**
  - Sends happen only under `bucket.mu` with `retired == false`. The worker sets `retired` only under that lock and only with an empty queue.
  - A new bucket gets its first request **before** its worker starts.
  - The engineer confirmed that disabling the `retired` check makes `TestRetireEnqueueRace` fail, so the test guards the invariant.
- **`Close`.** Marks the limiter closed first, then releases (closes) every queued ticket and waits for all workers. An abandoned request keeps its queue slot until dequeued, as in the textbook algorithm; `close()` never blocks the worker.

**Deviations:**
- There is no janitor; workers retire themselves. `Config.CleanupInterval` is validated but unused, and this is documented.
- Extra exports: `WaitTicket`, `Closed`, `Interval`, `ErrInvalidConfig`, `ErrNoTicket`, and (added by the Team Lead) `MinInterval`.

**Tests:** 12 functions, all passing under `-race`. Results:
- **FIFO timing on real clocks:** gaps of 49.3–50.4ms at 20/s.
- **Queue full:** rejection in under 50ms with a 500ms interval.
- **The documented con, reproduced** (old requests filling the queue): a legitimate request is first rejected, then delayed ≈ 500ms behind the old burst.
- Also covered: per-key isolation, idle retirement back to the goroutine baseline, the retire/enqueue race test, `Close` releasing tickets, abandoned tickets, and validation.

**Limitations it flagged:**
- `RetryAfter` follows the ticker schedule, so it errs slightly high if the worker runs late.
- An idle bucket still wakes once per interval until it retires.
- One test (`TestQueueFullWithOldRequests`) was flaky at first. It is fixed by polling instead of sleeping exactly `RetryAfter`, and has been stable in every run since.

## 4. HTTP/API Engineer

**Assigned:** `cmd/server` for both modules, covering flags, middleware, headers, 429 handling, the unprotected `/healthz`, and a real curl smoke test.

**Built:** `cmd/server/main.go`, `middleware.go` and `middleware_test.go` in each module.

**Flags:**
- token bucket: `-addr -capacity -rate -idle-ttl -cleanup-interval -shutdown-timeout`
- leaky bucket: `-addr -queue -rate -idle-ttl -shutdown-timeout`
- Invalid config prints the constructor error and exits 2.

**Headers:** `X-RateLimit-Limit` and `X-RateLimit-Remaining` go on every protected response: 200, 429 and 503.

**`Retry-After`:**
- It is the exact `RetryAfter` duration converted to delta-seconds with an integer **ceiling**, at least 1. Flooring would tell clients to retry before capacity is back (2.5s → "2", 300ms → "0").
- The token bucket's `RetryNever` gives 429 with **no** Retry-After, since any number would be false (RFC 9110 makes the header optional).

**Client key:**
- The leftmost `X-Forwarded-For` entry, trimmed. If that is missing or empty, the host from `net.SplitHostPort(RemoteAddr)`; if that fails, the raw `RemoteAddr`.
- A comment flags that XFF is client-controlled. Without a trusted proxy that overwrites it, an attacker can rotate values to dodge the limit and create unbounded keys. This is a documented tradeoff, deliberately not "fixed".

**`/healthz`:** registered outside the middleware. It is never limited and consumes nothing.

**Leaky middleware shapes traffic rather than just counting it:**
1. `Submit` the request. A full queue gives 429; a closed limiter gives 503.
2. Otherwise set the headers and block in `WaitTicket(r.Context(), ticket)` until the worker dequeues the request, then serve it.
3. If the client disconnects, return without writing anything.
4. If the ticket was released by shutdown (`Closed()` is true), answer 503 instead of passing the backlog through.

**Graceful shutdown:**
- On SIGINT/SIGTERM the server calls `Shutdown(timeout)`.
- For the leaky server, if that times out, `limiter.Close()` releases the waiting requests (they get 503) and a second short `Shutdown` lets those responses flush. Only then is `srv.Close()` forced.
- Errors are joined and logged; `ErrServerClosed` is treated as expected.

**Timeouts:** both servers set ReadHeaderTimeout 5s, ReadTimeout 10s and IdleTimeout 60s. WriteTimeout is 10s for the token bucket. For the leaky bucket it is queue × interval + 10s, because the write deadline also covers time spent queued, and it is left unset if that would overflow.

**Deviations:**
- Added the `-shutdown-timeout` flag to both servers.
- A shutdown that hits its timeout exits 1, because it is treated as not clean.
- Go canonicalises header names to `X-Ratelimit-*`; header names are case-insensitive in HTTP.
- `/` is a catch-all, so every path except `/healthz` is protected.

**Tests:** all pass under `-race`. They cover:
- headers on allowed and rejected responses
- Retry-After values: capacity 1 at 0.5/s gives "2"; a 2.5s wait gives "3"; a 1/3s wait gives "1"; MaxInt64 doesn't overflow
- RetryNever sends no header; `/healthz` is never limited
- 13 key-extraction cases (IPv6, zones, empty leftmost entry, multiple header lines)
- per-key isolation; 20 concurrent requests give exactly 5×200 and 15×429
- leaky: staggered release timing, immediate 429 when full, client cancel doesn't hang, `Close` → 503, and the WriteTimeout calculation

**Smoke-test evidence** (curl against the real binaries inside the container):
- **Token bucket, `-capacity 3 -rate 1`:** 200s with Remaining 2/1/0, then `429` with `Retry-After: 1`, `X-Ratelimit-Limit: 3`, `X-Ratelimit-Remaining: 0`.
  - `/healthz` returned 200 while `/` was limited.
  - Separate XFF values got separate limits.
  - `-rate 0.4` gave `Retry-After: 3` (a 2.5s wait, rounded up).
  - `-rate 0` gave 429 with no Retry-After.
  - SIGTERM gave a clean exit 0.
- **Leaky bucket, `-queue 3 -rate 2`, burst of 8 in parallel:**
  ```
  429 t=0.000389s remaining=0 retry-after=1   (x5, all < 1ms)
  200 t=0.486s  remaining=2
  200 t=0.986s  remaining=1
  200 t=1.486s  remaining=0
  ```
  - Requests leave about 0.5s apart, the fixed outflow, and the overflow gets an immediate 429.
  - `/healthz` returned 200 in 0.7ms while the queue was full.
  - A SIGTERM with 3 requests queued and `-shutdown-timeout 1s` gave all three a 503 at t≈1.5s.

## 5. QA / Concurrency Reviewer

**Assigned:** an independent review of both `ratelimiter` packages for races, goroutine leaks, zero/negative config and clock bugs. Add the missing tests directly and report to the Team Lead.

**Bugs found and fixed.** Each has a regression test that failed before the fix.
1. **Token bucket: the janitor never evicted after a backward clock jump** (`limiter.go` `evictIdle`).
   - Buckets untouched since before a jump larger than IdleTTL had negative idle time and were kept until the clock caught up: 1h for a −1h jump, about 10 years for a −10y jump. That is a memory leak.
   - **Fix:** the janitor rebases such buckets, exactly as the bucket's own next call would. They are then evicted within about 2×TTL.
2. **Token bucket: waiting exactly `RetryAfter` could still be denied at rates above 1000 tokens/s.**
   - The nanosecond rounding's error bound was wrong by a factor of 1e9: the comment said "per ns" where it should have said "per s".
   - **Fix:** round down only when the error is at most epsilon/2 tokens.
   - Failures went from 1–3 per 20k random cases to 0 in 140k.
3. **Leaky bucket: `Wait` overflowed to a negative value at very slow rates** (for example 1e-9/s at queue position 10). **Fix:** saturating arithmetic.

**Tests added:**
- `token-bucket/ratelimiter/qa_test.go`, `qa_sweep_bench_test.go`:
  - 100k keys add 0 goroutines and are all evicted
  - no livelock in the eviction retry loop
  - clock backward/forward/jitter invariants (jumps of 1ms to 10y)
  - a RetryAfter property test over 140k cases
  - degenerate and extreme config, plus a sweep-pause measurement
- `leaky-bucket/ratelimiter/qa_test.go`, `qa_race_test.go`, `qa_norace_test.go`:
  - 10k-key leak tests for retirement and for `Close`. This drops to 5k under `-race`, because the race detector caps live goroutines at 8128; build tags select the scale.
  - `Close` during a Submit storm with TTL = 1ns
  - decision invariants at extreme rates
  - the degenerate-config table and `Len` accounting

**Goroutine numbers:**
- **Token bucket:** 100,000 keys → +0 goroutines.
- **Leaky bucket:** 10,000 keys → +10,000 workers, all retired about 154ms after going idle, back to baseline. `Close` with 30,000 queued requests took 4.7ms with no leak.

**Clock behaviour verified:**
- **Token bucket, clock going backwards:** no tokens are added, the balance never goes below 0 or above capacity, and there is no stall.
- **Token bucket, returning to the original time:** this over-credits by min(jump × rate, capacity), which is documented.
- **Leaky bucket:** it uses only monotonic timers, so wall-clock changes have no effect. RetryAfter is in (0, interval], and Wait is always > 0.

**Design notes it raised, and how the Team Lead handled them:**

| Note | Disposition |
|---|---|
| Leaky interval truncation lets the real rate exceed the configured one; a 1ns ticker is accepted | **Fixed** by the Team Lead (rounded up, `MinInterval` 1µs) |
| Token `Capacity` in (0,1) means `Allow` is denied forever | **Fixed** for the keyed `Limiter` (`Capacity >= 1`) |
| Janitor holds the map write lock for an O(N) sweep (≈6ms per 100k keys, ≈60ms per 1M) | Next steps |
| float64 resolution: above 1e7 tokens the absolute epsilon is too small; above 2^53, consuming 1 token is lost to rounding | Next steps |
| `RetryAfter` saturates at MaxInt64 (~292 years) for absurdly low rates | Accepted; next steps (document it) |
| An explicit tiny `CleanupInterval` (e.g. 1ns) makes the janitor busy-loop | Next steps |
| `WithClock(time.Now().UTC())` strips the monotonic reading | Next steps (doc line) |
| Leaky `lastActive` can briefly go backwards (tick timestamp vs enqueue time) | Accepted: at worst a worker retires up to one interval early, and only with an empty queue, so it is safe |

## Deviations from the design constraints (all accepted by the Team Lead)

1. **Leaky bucket:** chose mitigation (a) as self-retiring workers rather than a separate janitor goroutine. This is equivalent in effect, avoids a second goroutine that would have to coordinate with each worker, and is justified in `doc.go`. As a result, `CleanupInterval` is unused there.
2. **Token bucket:** the keyed `Limiter` requires `Capacity >= 1` (stricter than "> 0").
3. **Leaky bucket:** `OutflowRate` is capped at 1e6/s per key (`MinInterval`).
4. **Both servers:** an extra `-shutdown-timeout` flag. The leaky server returns **503** (not 429) when it is shutting down.

Nothing else deviates. Lazy refill, float64 tokens, per-key buckets with a TTL janitor, a bounded-channel FIFO with a fixed-rate worker, immediate rejection when full, the headers, the XFF comment, the unprotected `/healthz`, and vet/build/gofmt are all implemented as specified.

## Known limitations

- **Unbounded keys within one TTL window (both modules).** Neither limiter has a hard cap on live keys. With spoofed `X-Forwarded-For` or genuinely huge client counts, memory grows with key arrival rate × IdleTTL:
  - **Token bucket:** roughly 150 B per idle key, so 1M keys ≈ 150 MB. Cheap, but not bounded.
  - **Leaky bucket:** each active key costs a goroutine (≥ 2–8 KB stack), a ticker and a queue buffer, so 1M active keys ≈ several GB.

  This is the architectural tradeoff the brief anticipated. It is mitigated by retirement, but not eliminated.
- **The leaky queue holds requests, not callers.** A client that disconnects keeps its queue slot until the worker reaches it (textbook behaviour).
- **Timing tests.** The real-time tests use wide tolerances and were stable across every run here (Docker on Windows). Very slow CI machines could still stretch them.

## Next steps (out of scope for this session)

1. **Hard cap on keys** (`MaxKeys`) with an explicit overflow policy (LRU eviction, or reject with 503) in both limiters. For the leaky bucket, consider a cap on the total number of workers.
2. **Shard the token-bucket map**, or sweep in batches (collect candidates under RLock, then delete in small batches with a re-check), to remove the O(N) write-lock pause.
3. **Token bucket robustness:**
   - apply `MinDefaultCleanupInterval` as a floor to explicit values too
   - use a relative epsilon, or cap Capacity (for example ≤ 2^53)
   - document that `RetryAfter` saturates and that a clock without a monotonic reading is a risk
4. **Trusted-proxy configuration:** use XFF only when `RemoteAddr` is in a configured CIDR allowlist, and use the rightmost untrusted hop instead of the leftmost entry.
5. **Distributed limiting** (Redis/GCRA) for multi-instance deployments; today each process limits independently.
6. **Observability:** Prometheus metrics (allowed/rejected counts, queue depth, live keys and workers, retirements).
7. **The deferred frontend:** visualisation, dashboards and CLI demo tooling, planned for a later session.
