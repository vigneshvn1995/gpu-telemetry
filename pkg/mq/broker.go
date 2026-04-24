package mq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path"
	"sync"
	"sync/atomic"
)

// -------------------------------------------------------------------------
// Sentinel errors
// -------------------------------------------------------------------------

// ErrBrokerClosed is returned by all Broker methods after Close has been called.
var ErrBrokerClosed = errors.New("mq: broker is closed")

// -------------------------------------------------------------------------
// Public types
// -------------------------------------------------------------------------

// BrokerStats is a point-in-time snapshot of broker health counters.
// All fields are safe to read without synchronisation because they are copied
// from atomic values at the moment Stats() is called.
type BrokerStats struct {
	TotalPublished   uint64 `json:"total_published"`
	TotalDelivered   uint64 `json:"total_delivered"`
	TotalDropped     uint64 `json:"total_dropped"`
	ActiveTopics     int    `json:"active_topics"`
	TotalSubscribers int    `json:"total_subscribers"`
}

// Broker is the core publish-subscribe interface. All implementations must be
// safe for concurrent use by multiple goroutines.
//
// Topic names are arbitrary strings. Subscribers may use glob patterns
// (e.g. "gpu.*") – see matchTopic for the supported syntax.
type Broker interface {
	// Publish sends msg to every current subscriber of topic.
	// Publish returns as soon as the message has been dispatched to subscriber
	// channels; it does NOT wait for consumers to process the message.
	Publish(ctx context.Context, topic string, msg []byte) error

	// Subscribe registers a new subscription for topic (or a glob pattern) and
	// returns a receive-only channel. The channel is closed automatically when
	// the broker shuts down or the caller unsubscribes.
	Subscribe(ctx context.Context, topic string) (<-chan []byte, error)

	// Unsubscribe removes the subscription identified by the (topic, ch) pair.
	// The channel is closed after removal. Calling Unsubscribe with an unknown
	// channel returns an error but does not panic.
	Unsubscribe(topic string, ch <-chan []byte) error

	// Topics returns a snapshot of all topics that have at least one subscriber.
	Topics() []string

	// Stats returns a snapshot of broker counters.
	Stats() BrokerStats

	// Close gracefully drains in-flight messages, closes all subscriber
	// channels, and releases resources. It is idempotent.
	Close() error
}

// -------------------------------------------------------------------------
// MemoryBroker – in-process implementation
// -------------------------------------------------------------------------

// subscription holds a single subscriber's delivery channel together with the
// topic pattern it registered for (used during fan-out matching).
type subscription struct {
	ch      chan []byte
	pattern string // the pattern passed to Subscribe (may differ from topic key)
}

// MemoryBroker is a thread-safe, in-process Broker backed by buffered Go
// channels. It is the core routing engine used by BrokerServer.
//
// Fan-out policy: messages are dispatched with a non-blocking send. If a
// subscriber's channel is full the message is counted as "dropped" and a
// warning is logged. This prevents a slow consumer from stalling the broker or
// other subscribers (backpressure is the caller's responsibility).
type MemoryBroker struct {
	mu        sync.RWMutex
	topics    map[string][]*subscription // exact topic → subscribers
	bufSize   int
	closed    bool
	closeOnce sync.Once
	done      chan struct{}

	// Counters on the hot path use atomics to avoid lock contention.
	published atomic.Uint64
	delivered atomic.Uint64
	dropped   atomic.Uint64

	logger *slog.Logger
}

// MemoryBrokerOption is a functional option for MemoryBroker.
type MemoryBrokerOption func(*MemoryBroker)

// WithBufferSize sets the per-subscriber channel buffer depth.
// Larger values reduce drop rates for bursty publishers at the cost of memory.
// Default: 256.
func WithBufferSize(n int) MemoryBrokerOption {
	return func(b *MemoryBroker) { b.bufSize = n }
}

// WithBrokerLogger injects a structured logger. Defaults to slog.Default().
func WithBrokerLogger(l *slog.Logger) MemoryBrokerOption {
	return func(b *MemoryBroker) { b.logger = l }
}

// NewMemoryBroker creates and returns a ready-to-use in-memory broker.
func NewMemoryBroker(opts ...MemoryBrokerOption) *MemoryBroker {
	b := &MemoryBroker{
		topics:  make(map[string][]*subscription),
		bufSize: 256,
		done:    make(chan struct{}),
		logger:  slog.Default(),
	}
	for _, o := range opts {
		o(b)
	}
	return b
}

// Publish implements Broker.
//
// Complexity: O(S) where S is the number of subscribers across all matching
// topics. The RLock is held only during the fan-out loop, not during the
// channel sends, so publishers are never blocked by individual slow consumers.
func (b *MemoryBroker) Publish(ctx context.Context, topic string, msg []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return ErrBrokerClosed
	}

	// Collect all matching subscribers under the read lock to keep the
	// critical section short.
	var targets []*subscription
	for t, subs := range b.topics {
		if matchTopic(t, topic) {
			targets = append(targets, subs...)
		}
	}
	b.mu.RUnlock()

	b.published.Add(1)

	if len(targets) == 0 {
		return nil
	}

	// Copy the payload once so every subscriber gets an independent slice.
	cp := make([]byte, len(msg))
	copy(cp, msg)

	for _, s := range targets {
		select {
		case s.ch <- cp:
			b.delivered.Add(1)
		default:
			b.dropped.Add(1)
			b.logger.Warn("subscriber channel full, dropping message",
				"topic", topic,
				"pattern", s.pattern,
			)
		}
	}
	return nil
}

// Subscribe implements Broker.
func (b *MemoryBroker) Subscribe(ctx context.Context, topic string) (<-chan []byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return nil, ErrBrokerClosed
	}

	s := &subscription{
		ch:      make(chan []byte, b.bufSize),
		pattern: topic,
	}
	b.topics[topic] = append(b.topics[topic], s)

	b.logger.Debug("subscriber registered",
		"topic", topic,
		"total_for_topic", len(b.topics[topic]),
	)
	return s.ch, nil
}

// Unsubscribe implements Broker.
func (b *MemoryBroker) Unsubscribe(topic string, ch <-chan []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	subs, ok := b.topics[topic]
	if !ok {
		return fmt.Errorf("mq: no subscriptions found for topic %q", topic)
	}

	for i, s := range subs {
		if s.ch == ch {
			// Swap-delete to avoid allocation.
			subs[i] = subs[len(subs)-1]
			subs[len(subs)-1] = nil
			b.topics[topic] = subs[:len(subs)-1]

			close(s.ch)

			if len(b.topics[topic]) == 0 {
				delete(b.topics, topic)
			}

			b.logger.Debug("subscriber removed", "topic", topic)
			return nil
		}
	}
	return fmt.Errorf("mq: subscription channel not found for topic %q", topic)
}

// Topics implements Broker.
func (b *MemoryBroker) Topics() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]string, 0, len(b.topics))
	for t := range b.topics {
		out = append(out, t)
	}
	return out
}

// Stats implements Broker.
func (b *MemoryBroker) Stats() BrokerStats {
	b.mu.RLock()
	activeTopics := len(b.topics)
	totalSubs := 0
	for _, subs := range b.topics {
		totalSubs += len(subs)
	}
	b.mu.RUnlock()

	return BrokerStats{
		TotalPublished:   b.published.Load(),
		TotalDelivered:   b.delivered.Load(),
		TotalDropped:     b.dropped.Load(),
		ActiveTopics:     activeTopics,
		TotalSubscribers: totalSubs,
	}
}

// Close implements Broker.
func (b *MemoryBroker) Close() error {
	b.closeOnce.Do(func() {
		b.mu.Lock()
		defer b.mu.Unlock()

		b.closed = true
		for topic, subs := range b.topics {
			for _, s := range subs {
				close(s.ch)
			}
			delete(b.topics, topic)
		}
		close(b.done)
		b.logger.Info("broker closed",
			"total_published", b.published.Load(),
			"total_delivered", b.delivered.Load(),
			"total_dropped", b.dropped.Load(),
		)
	})
	return nil
}

// -------------------------------------------------------------------------
// Topic matching
// -------------------------------------------------------------------------

// matchTopic reports whether subscriber pattern matches the published topic.
//
// Supported wildcards (path.Match syntax):
//
//	*  – matches any sequence of non-separator characters
//	?  – matches any single non-separator character
//	** – not supported; use multiple subscriptions if needed
//
// Examples:
//
//	matchTopic("gpu.telemetry", "gpu.telemetry")         → true  (exact)
//	matchTopic("gpu.*", "gpu.telemetry")                 → true
//	matchTopic("*", "gpu.telemetry")                     → false (cross-segment)
func matchTopic(pattern, topic string) bool {
	if pattern == topic {
		return true
	}
	matched, err := path.Match(pattern, topic)
	return err == nil && matched
}
