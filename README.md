# Token_Bucket_Rate_Limiter

Two per-client rate limiters written in Go, each with an HTTP server in front of it:

| Module | Path | Go module | What it does |
|---|---|---|---|
| Token bucket | [`rate-limiters/token-bucket/`](rate-limiters/token-bucket/) | `tokenbucket` | Allows bursts up to a capacity, then limits clients to a steady refill rate |
| Leaky bucket | [`rate-limiters/leaky-bucket/`](rate-limiters/leaky-bucket/) | `leakybucket` | Puts requests in a FIFO queue and releases them at a fixed outflow rate |

Each module has a core `ratelimiter` package, an HTTP server with rate-limit middleware in `cmd/server`, and its own tests. Both use only the Go standard library (Go 1.23) and have no third-party dependencies. The full build report is in [`result.md`](result.md).

---

## Waterfall procedure followed

The work went through the classic Waterfall phases in order. Each phase was finished and reviewed before the next one started.

### 1. Requirements
- Scope was **backend only**: two separate Go modules, each with a limiter package, an HTTP server and tests. No frontend, visualisation or CLI demo.
- Requirements for both modules:
  - per-client (keyed) limiting
  - thread safety
  - no goroutine leaks
  - no silently swallowed errors
  - memory that stays manageable with millions of distinct clients
- **Token bucket requirements:** lazy refill, `float64` token counts, per-key buckets, and a TTL janitor that removes idle buckets.
- **Leaky bucket requirements:** a real bounded-channel FIFO queue, one worker at a fixed rate per key, immediate rejection when the queue is full, and a plan for idle workers.
- **HTTP requirements:** `X-RateLimit-*` and `Retry-After` headers, `429` on rejection, an unprotected `/healthz`, and a comment noting that `X-Forwarded-For` is controlled by the client.

### 2. System design
- The Team Lead fixed the exported API of both `ratelimiter` packages **before** any code was written. This let the HTTP engineer code against the contracts while the algorithms were still being built.
- The team settled on these designs:
  - **Token bucket:** lazy refill. There is no goroutine per bucket, and refill is computed from timestamps on each call.
  - **Leaky bucket:** a `chan` queue with one ticker worker per key. Idle workers **retire themselves**, chosen over a GCRA-style virtual queue. The reasons are written in `doc.go`.
  - **Locking:** the map lock is always taken before the bucket lock, which rules out deadlock.

### 3. Implementation
- The **Token Bucket** and **Leaky Bucket** engineers built the two algorithms **in parallel**.
- The **HTTP/API** and **QA** agents then worked **in parallel** on separate sets of files.

### 4. Integration & testing (verification)
- Unit tests, middleware tests, QA stress tests and property tests were run under the **race detector**.
- The Team Lead ran the final gate in Docker with `golang:1.23`, once per module:
  ```
  gofmt -l . ; go build ./... ; go vet ./... ; go test -race -count=3 ./...
  ```
  It passed with `GATE EXIT=0`. `gofmt`, `build` and `vet` were clean, and all 4 packages passed 3 runs each under `-race`.

### 5. Deployment / validation
- The real server binaries were started inside the container and tested with `curl`:
  - the headers were correct
  - `429` came back with the right `Retry-After`
  - `/healthz` was never limited
  - separate `X-Forwarded-For` values got separate limits
  - the servers shut down gracefully on SIGTERM

### 6. Maintenance
- Known limitations and next steps are recorded in [`result.md`](result.md#next-steps-out-of-scope-for-this-session):
  - a `MaxKeys` cap
  - a sharded map for the token bucket
  - trusted-proxy settings for `X-Forwarded-For`
  - Redis/GCRA for limiting across several instances
  - Prometheus metrics
  - the frontend that was deferred

---

## Characteristics

### Token bucket (`rate-limiters/token-bucket`)
- **Algorithm:** a bucket holds up to `Capacity` tokens and gains `RefillRate` tokens per second. Each request uses one token (or `n` with `AllowN`). A request is either fully served or rejected; a rejection uses no tokens.
- **Lazy refill:** there is no ticker or goroutine per bucket. On each call, tokens are increased by `elapsed × rate`, up to `Capacity`. Each request is O(1), and an idle bucket costs nothing.
- **`float64` tokens:** a 1e-9 epsilon absorbs rounding error, so fractional rates like 2.5/s refill exactly.
- **Bursts:** up to `Capacity` requests can go through at once, after which traffic is held to the refill rate.
- **Concurrency:**
  - Each bucket has its own mutex.
  - The key → bucket map uses an `RWMutex` that is held only for lookup and insert.
- **Idle eviction:**
  - A single janitor goroutine removes buckets that have been idle longer than `IdleTTL`.
  - Eviction is race-safe: the janitor uses `TryLock` and marks the bucket `dead`, and a caller holding a stale bucket looks the key up again.
- **Clock:** the clock can be injected and defaults to the monotonic clock. If the clock goes backwards, no tokens are added and the bucket's timestamp is reset, so nothing locks up.
- **`RetryAfter`:** the exact wait (missing tokens ÷ rate), rounded up. When waiting can never help, it is `RetryNever = -1`, for example when the rate is 0 or `n` is greater than `Capacity`.

### Leaky bucket (`rate-limiters/leaky-bucket`)
- **Algorithm:** the "leaky bucket as a queue" form.
  - Each key has a bounded channel as its FIFO queue, and one worker goroutine with a `time.Ticker`.
  - On each tick, the worker takes one request off the queue and releases it.
- **Traffic shaping:** requests go out at a fixed rate in FIFO order, however bursty they were on arrival.
- **Immediate rejection:** adding to the queue is a non-blocking `select`/`default`, so a full queue rejects the request at once.
- **Idle workers:** a worker retires once its queue has been empty for `IdleTTL`. The next request from that key creates a new bucket.
- **Retire vs. enqueue race:** a request is only added under `bucket.mu` while `retired == false`, so no ticket can ever be left behind.
- **`Close`:** releases every queued ticket and waits for every worker, so no caller hangs.
- **Known weakness:** old requests can fill the queue. Later legitimate requests are then rejected, or wait behind the backlog. A test reproduces this.

### HTTP servers (`cmd/server` in both modules)
- `/` is rate limited (every path except `/healthz`), and `/healthz` is never limited.
- `X-RateLimit-Limit` and `X-RateLimit-Remaining` are sent on every protected response.
- `Retry-After` is rounded **up** to whole seconds, with a minimum of 1.
- **Client key:** the leftmost `X-Forwarded-For` entry, falling back to the host in `RemoteAddr`.
- **Leaky server:** a request **waits in the queue** before it is served. A full queue gets `429`, and a request caught by shutdown gets `503`.
- **Shutdown:** the servers shut down gracefully on SIGINT/SIGTERM.
- **Bad configuration:** an invalid setting prints the error and exits with code `2`.

---

## Quantifiers (parameters and measured numbers)

### Configuration parameters

| Parameter | Token bucket flag | Leaky bucket flag | Meaning |
|---|---|---|---|
| Burst / size | `-capacity` (default 10, ≥ 1) | `-queue` (default 10, ≥ 1) | Tokens a bucket holds / requests a queue can hold |
| Rate | `-rate` (default 5 tokens/s, ≥ 0; 0 = fixed allowance) | `-rate` (default 5 req/s, > 0, at most 1e6/s per key) | Refill rate / outflow rate |
| Idle TTL | `-idle-ttl` (default 10m) | `-idle-ttl` (default 1m) | When idle state is removed |
| Cleanup | `-cleanup-interval` (0 = max(TTL/2, 1s)) | n/a (workers retire themselves) | How often the janitor runs |
| Shutdown | `-shutdown-timeout` (default 10s) | `-shutdown-timeout` (default 10s) | Time allowed to finish requests on shutdown |
| Address | `-addr` (default `:8080`) | `-addr` (default `:8080`) | Listen address |

- **Leaky bucket interval:** the outflow interval is `ceil(1s / rate)`, with a minimum of `MinInterval = 1µs`.
- **Token bucket exactness:** recreating an evicted bucket as full is exact when `IdleTTL ≥ Capacity / RefillRate`.

### Measured results

| Measurement | Result |
|---|---|
| Token bucket: 600 goroutines × 5 calls on a bucket with capacity 100 | Exactly **100** allowed |
| Token bucket: 100,000 keys | **+0** goroutines, all keys evicted |
| Token bucket: `RetryAfter` property test | **0 failures in 140k** cases (previously 1–3 per 20k) |
| Token bucket: janitor pause | ≈ 6 ms per 100k keys (≈ 60 ms per 1M) |
| Leaky bucket: FIFO gaps at 20/s | 49.3–50.4 ms (target 50 ms) |
| Leaky bucket: 10,000 keys | +10,000 workers, all retired ≈ **154 ms** after going idle |
| Leaky bucket: `Close` with 30,000 queued requests | **4.7 ms**, no leak |
| HTTP: 20 concurrent requests, capacity 5 | Exactly **5 × 200** and **15 × 429** |
| Smoke test, leaky bucket `-queue 3 -rate 2`, burst of 8 | 5 × `429` in < 1 ms, then 3 × `200` at 0.49 s / 0.99 s / 1.49 s |
| Memory per idle key | Token bucket ≈ 150 B; leaky bucket ≈ one goroutine (≥ 2–8 KB), a ticker and a queue buffer |

---

## The agent team

The project was built by a team of five Claude Code agents, with Docker used as the toolchain after the user approved it.

- **Team Lead / Architect:**
  - Planned the phases and fixed the API contracts.
  - Reviewed every handoff and ran the final gate.
  - Fixed two problems that QA raised: leaky-bucket interval rounding plus `MinInterval`, and `Capacity ≥ 1` for the keyed token-bucket limiter.
  - Wrote `result.md`.
- **Token Bucket Engineer:**
  - Built lazy refill, the keyed limiter, the janitor and the eviction-race guard.
  - Wrote **21 tests**, including exact counts under concurrency and fake-clock refill.
- **Leaky Bucket Engineer:**
  - Built the channel queue, ticker workers, self-retiring workers and `Close`.
  - Wrote **12 tests** that check timing on real clocks, the retire/enqueue race and the queue-filling weakness.
- **HTTP/API Engineer:**
  - Built both servers: flags, middleware, headers, `Retry-After` rounding, client-key extraction and graceful shutdown.
  - Wrote middleware tests, including 13 client-key cases.
  - Ran the `curl` smoke tests against the real binaries.
- **QA / Concurrency Reviewer:**
  - Reviewed both packages on their own.
  - Found and fixed **3 bugs**, each with a test that failed before the fix: the janitor did not evict after the clock went backwards, `RetryAfter` was off at high rates, and the leaky `Wait` overflowed.
  - Added tests with 100k/10k keys for leaks, extreme clock jumps and extreme configs.

**How the agents communicated:**
- Agents talked through written messages: each one sent the Team Lead a report at handoff covering what it built, how it differed from the plan, its test results and its known limitations.
- The Team Lead reviewed each report and either accepted it or sent back fixes.
- QA's design notes went to the Team Lead, who marked each one as fixed, accepted, or left for next steps.

**Process note:** at one point the QA agent's `docker kill` stopped every running container, including the HTTP engineer's. Everything was re-run in the final gate.

---

## Running

Each module is self-contained. From a module folder:

```bash
go test -race ./...
go run ./cmd/server -capacity 10 -rate 5          # token-bucket
go run ./cmd/server -queue 10 -rate 5             # leaky-bucket
curl -i http://localhost:8080/
```
