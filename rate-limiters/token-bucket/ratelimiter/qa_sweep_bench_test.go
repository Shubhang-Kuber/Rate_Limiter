package ratelimiter

import (
	"fmt"
	"testing"
	"time"
)

// TestQASweepPause measures how long one janitor sweep holds the map write
// lock (blocking every Allow for every key) at various key counts. It only
// logs; see the QA report. Skipped in -short mode.
func TestQASweepPause(t *testing.T) {
	if testing.Short() {
		t.Skip("measurement only")
	}
	for _, n := range []int{10_000, 100_000} {
		clk := newFakeClock()
		l := qaLimiter(t, 5, 1, time.Minute, clk)
		for i := 0; i < n; i++ {
			l.Allow(fmt.Sprintf("k%d", i))
		}
		start := time.Now()
		l.evictIdle(clk.Now()) // nothing idle: pure scan cost
		scan := time.Since(start)
		clk.Advance(2 * time.Minute)
		start = time.Now()
		ev := l.evictIdle(clk.Now())
		evict := time.Since(start)
		t.Logf("keys=%d: write-lock hold for no-op sweep=%v, full-evict sweep=%v (evicted %d)", n, scan, evict, ev)
		l.Close()
	}
}
