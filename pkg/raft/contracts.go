// Package raft defines the boundaries used by the Raft node wrapper.
// The Raft core receives explicit logical ticks and never reads a clock itself.
package raft

import (
	"time"

	raftv1 "github.com/zishan044/raft-replicated-key-value-store.git/pkg/api/raft/v1"
)

// Clock supplies periodic ticks to the node wrapper. Implementations may use
// real time or virtual time controlled by the deterministic simulator.
type Clock interface {
	// NewTicker starts a ticker with a positive interval d.
	// The caller must stop the ticker when it is no longer needed.
	NewTicker(d time.Duration) Ticker
}

// Ticker delivers periodic events until stopped.
type Ticker interface {
	// C returns the same channel on every call. Each event drives one logical
	// tick; the core does not use the event's wall-clock timestamp.
	C() <-chan time.Time

	// Stop releases ticker resources without closing C. Repeated calls are safe.
	Stop()
}

// Transport sends outbound peer messages without blocking the Raft event loop.
type Transport interface {
	// Send routes each message to its To node. It must return promptly without
	// waiting for network I/O or queue space, dropping messages when necessary.
	// Raft must tolerate loss; returning does not imply delivery.
	//
	// Send may retain the batch and its messages after returning. The caller
	// must not mutate or reuse the slice, messages, or nested data after calling
	// Send. Implementations must treat message contents as read-only.
	Send(messages []*raftv1.Message)
}
