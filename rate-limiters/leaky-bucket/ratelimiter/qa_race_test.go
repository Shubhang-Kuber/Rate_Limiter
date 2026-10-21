//go:build race

package ratelimiter

// The race detector aborts the process above 8128 simultaneously alive
// goroutines, so the large-scale leak test scales down under -race.
const qaLeakKeys = 5000
