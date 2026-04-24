package mq

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// -------------------------------------------------------------------------
// Sentinel errors
// -------------------------------------------------------------------------

// ErrClientClosed is returned when operating on a closed client.
var ErrClientClosed = errors.New("mq: client is closed")

// ErrClientNotConnected is returned when the client has no active connection.
var ErrClientNotConnected = errors.New("mq: not connected")

// ErrOutboundFull is returned when the client's outbound buffer is full.
var ErrOutboundFull = errors.New("mq: outbound buffer full")

// -------------------------------------------------------------------------
// Client configuration
// -------------------------------------------------------------------------

// ClientConfig holds tunables for the TCP broker client.
type ClientConfig struct {
	// Addr is the broker TCP address (default ":7777").
	Addr string

	// DialTimeout caps the initial dial attempt. Default: 10 s.
	DialTimeout time.Duration

	// WriteTimeout is the per-frame write deadline. Default: 10 s.
	WriteTimeout time.Duration

	// ReadTimeout is the per-frame read deadline. Default: 90 s
	// (must be larger than server's PingInterval).
	ReadTimeout time.Duration

	// ReconnectBaseDelay is the initial backoff delay on disconnect.
	// Delay doubles on each attempt up to ReconnectMaxDelay. Default: 500 ms.
	ReconnectBaseDelay time.Duration

	// ReconnectMaxDelay caps the exponential backoff. Default: 30 s.
	ReconnectMaxDelay time.Duration

	// OutboundBufSize is the size of the client's internal outbound channel.
	// Default: 1024.
	OutboundBufSize int
}

func (c *ClientConfig) withDefaults() ClientConfig {
	out := *c
	if out.Addr == "" {
		out.Addr = ":7777"
	}
	if out.DialTimeout == 0 {
		out.DialTimeout = 10 * time.Second
	}
	if out.WriteTimeout == 0 {
		out.WriteTimeout = 10 * time.Second
	}
	if out.ReadTimeout == 0 {
		out.ReadTimeout = 90 * time.Second
	}
	if out.ReconnectBaseDelay == 0 {
		out.ReconnectBaseDelay = 500 * time.Millisecond
	}
	if out.ReconnectMaxDelay == 0 {
		out.ReconnectMaxDelay = 30 * time.Second
	}
	if out.OutboundBufSize == 0 {
		out.OutboundBufSize = 1024
	}
	return out
}

// -------------------------------------------------------------------------
// Subscription registry entry
// -------------------------------------------------------------------------

// clientSub tracks a subscription held by a Client.
type clientSub struct {
	topic string
	// Multiple callers may subscribe to the same topic from the same client;
	// each gets its own channel.
	channels []chan []byte
}

// -------------------------------------------------------------------------
// Client
// -------------------------------------------------------------------------

// Client is a thread-safe TCP client for the gpu-telemetry broker. It provides
// Publish and Subscribe methods that map to the underlying wire protocol and
// transparently reconnects on network failure.
//
// Connection management:
//   - The client dials on the first call to Connect or when used implicitly.
//   - On disconnect the client attempts to reconnect with exponential backoff.
//   - Active subscriptions are re-established after every reconnect.
type Client struct {
	cfg    ClientConfig
	logger *slog.Logger

	// conn is the current TCP connection. Protected by connMu.
	connMu    sync.RWMutex
	conn      net.Conn
	connected bool

	// outbound queues frames for the writeLoop.
	outbound chan *Frame

	// subs holds all active subscriptions. Protected by subsMu.
	subsMu sync.RWMutex
	subs   map[string]*clientSub // topic → sub

	closed    atomic.Bool
	closeOnce sync.Once
	closeCh   chan struct{}
}

// NewClient creates a Client using the provided configuration. Call Connect
// (or use Publish/Subscribe which auto-connect) to establish the TCP link.
func NewClient(cfg ClientConfig, logger *slog.Logger) *Client {
	cfg = cfg.withDefaults()
	if logger == nil {
		logger = slog.Default()
	}
	return &Client{
		cfg:      cfg,
		logger:   logger.With("broker_addr", cfg.Addr),
		outbound: make(chan *Frame, cfg.OutboundBufSize),
		subs:     make(map[string]*clientSub),
		closeCh:  make(chan struct{}),
	}
}

// Connect dials the broker and starts the background I/O goroutines.
// It blocks until the connection is established or ctx is cancelled.
// Calling Connect on an already-connected client is a no-op.
func (c *Client) Connect(ctx context.Context) error {
	if c.closed.Load() {
		return ErrClientClosed
	}

	c.connMu.Lock()
	defer c.connMu.Unlock()

	if c.connected {
		return nil
	}

	conn, err := c.dial(ctx)
	if err != nil {
		return err
	}

	c.conn = conn
	c.connected = true
	go c.ioLoop(conn)
	return nil
}

// Publish sends a message to the specified topic on the broker.
//
// payload must be a valid JSON-encoded byte slice. The broker treats it as
// opaque raw JSON and forwards it verbatim to subscribers; double-encoding is
// never applied. Callers should use json.Marshal before passing the payload.
//
// Publish is non-blocking: the frame is placed in an internal buffer and
// flushed by the background write goroutine. If the buffer is full,
// ErrOutboundFull is returned. The caller may retry after a short pause.
func (c *Client) Publish(ctx context.Context, topic string, payload []byte) error {
	if c.closed.Load() {
		return ErrClientClosed
	}
	if err := c.ensureConnected(ctx); err != nil {
		return err
	}

	f := &Frame{
		Type:    FramePub,
		Topic:   topic,
		Payload: json.RawMessage(payload), // payload must already be valid JSON
	}

	select {
	case c.outbound <- f:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-c.closeCh:
		return ErrClientClosed
	default:
		return ErrOutboundFull
	}
}

// Subscribe registers interest in topic and returns a channel on which
// matching messages will be delivered. The returned channel is buffered (256).
//
// If the client is already subscribed to the same topic, a new delivery channel
// is added – this enables fan-out within the process.
//
// The channel is closed when the Client is closed. Callers should range over
// the channel or select with a done channel to detect closure.
func (c *Client) Subscribe(ctx context.Context, topic string) (<-chan []byte, error) {
	if c.closed.Load() {
		return nil, ErrClientClosed
	}
	if err := c.ensureConnected(ctx); err != nil {
		return nil, err
	}

	ch := make(chan []byte, 256)

	c.subsMu.Lock()
	sub, ok := c.subs[topic]
	if !ok {
		sub = &clientSub{topic: topic}
		c.subs[topic] = sub
	}
	sub.channels = append(sub.channels, ch)
	c.subsMu.Unlock()

	// Only send one FrameSub per topic (additional callers share delivery).
	if !ok {
		f := &Frame{Type: FrameSub, Topic: topic}
		select {
		case c.outbound <- f:
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.closeCh:
			return nil, ErrClientClosed
		}
	}

	return ch, nil
}

// Close permanently shuts down the client, closes all subscriber channels, and
// terminates the TCP connection. It is idempotent.
func (c *Client) Close() {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		close(c.closeCh)

		c.connMu.Lock()
		if c.conn != nil {
			_ = c.conn.Close()
		}
		c.connMu.Unlock()

		c.subsMu.Lock()
		for _, sub := range c.subs {
			for _, ch := range sub.channels {
				close(ch)
			}
		}
		c.subs = make(map[string]*clientSub)
		c.subsMu.Unlock()

		c.logger.Info("client closed")
	})
}

// -------------------------------------------------------------------------
// Internal machinery
// -------------------------------------------------------------------------

func (c *Client) ensureConnected(ctx context.Context) error {
	c.connMu.RLock()
	connected := c.connected
	c.connMu.RUnlock()

	if connected {
		return nil
	}
	return c.Connect(ctx)
}

// dial attempts to establish a TCP connection, honouring ctx for cancellation.
func (c *Client) dial(ctx context.Context) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: c.cfg.DialTimeout}
	conn, err := dialer.DialContext(ctx, "tcp", c.cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("mq client: dial %s: %w", c.cfg.Addr, err)
	}
	c.logger.Info("connected to broker", "remote", conn.RemoteAddr())
	return conn, nil
}

// ioLoop runs the read and write goroutines for conn and handles reconnection.
func (c *Client) ioLoop(conn net.Conn) {
	defer func() {
		c.connMu.Lock()
		c.connected = false
		c.connMu.Unlock()
	}()

	var wg sync.WaitGroup
	wg.Add(2)

	// writeLoop feeds outbound frames to the TCP connection.
	go func() {
		defer wg.Done()
		c.writeLoop(conn)
	}()
	// readLoop dispatches incoming frames (messages, acks, pings).
	go func() {
		defer wg.Done()
		c.readLoop(conn)
	}()

	wg.Wait()

	if !c.closed.Load() {
		c.logger.Warn("disconnected from broker, will reconnect")
		go c.reconnectLoop()
	}
}

func (c *Client) writeLoop(conn net.Conn) {
	bw := bufio.NewWriterSize(conn, 64*1024)
	for {
		select {
		case f := <-c.outbound:
			if c.cfg.WriteTimeout > 0 {
				_ = conn.SetWriteDeadline(time.Now().Add(c.cfg.WriteTimeout))
			}
			if err := WriteFrame(bw, f); err != nil {
				return
			}
			// Drain remaining queued frames before flushing.
			for len(c.outbound) > 0 {
				f = <-c.outbound
				if err := WriteFrame(bw, f); err != nil {
					return
				}
			}
			if err := bw.Flush(); err != nil {
				return
			}
		case <-c.closeCh:
			_ = bw.Flush()
			return
		}
	}
}

func (c *Client) readLoop(conn net.Conn) {
	br := bufio.NewReaderSize(conn, 64*1024)
	for {
		if c.cfg.ReadTimeout > 0 {
			_ = conn.SetReadDeadline(time.Now().Add(c.cfg.ReadTimeout))
		}

		f, err := ReadFrame(br)
		if err != nil {
			if !c.closed.Load() && !isClosedErr(err) {
				c.logger.Debug("read error", "err", err)
			}
			_ = conn.Close() // signal writeLoop to exit
			return
		}

		switch f.Type {
		case FrameMsg:
			c.deliverMsg(f)
		case FramePing:
			// Respond immediately via outbound queue.
			select {
			case c.outbound <- &Frame{Type: FramePong, ID: f.ID}:
			default:
			}
		case FrameAck, FrameErr:
			// Future: hook into a pending-ack registry for request-reply patterns.
			if f.Type == FrameErr {
				c.logger.Warn("broker error", "id", f.ID, "error", f.Error)
			}
		}
	}
}

// deliverMsg fans out a received FrameMsg to all subscriber channels for the topic.
func (c *Client) deliverMsg(f *Frame) {
	c.subsMu.RLock()
	sub, ok := c.subs[f.Topic]
	if !ok {
		c.subsMu.RUnlock()
		return
	}
	// Snapshot channels to avoid holding the lock during sends.
	channels := make([]chan []byte, len(sub.channels))
	copy(channels, sub.channels)
	c.subsMu.RUnlock()

	payload := []byte(f.Payload)
	for _, ch := range channels {
		select {
		case ch <- payload:
		default:
			c.logger.Warn("subscriber channel full in client, dropping", "topic", f.Topic)
		}
	}
}

// reconnectLoop retries connecting with exponential backoff plus jitter.
// It re-establishes all active subscriptions once connected.
func (c *Client) reconnectLoop() {
	delay := c.cfg.ReconnectBaseDelay
	for {
		if c.closed.Load() {
			return
		}

		// Jitter: ±25% of current delay.
		jitter := time.Duration(rand.Int64N(int64(delay) / 2))
		sleep := delay - jitter/2 + jitter
		c.logger.Info("reconnecting in", "delay", sleep)

		select {
		case <-time.After(sleep):
		case <-c.closeCh:
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), c.cfg.DialTimeout)
		conn, err := c.dial(ctx)
		cancel()

		if err != nil {
			c.logger.Warn("reconnect failed", "err", err)
			delay = min(delay*2, c.cfg.ReconnectMaxDelay)
			continue
		}

		c.connMu.Lock()
		c.conn = conn
		c.connected = true
		c.connMu.Unlock()

		c.resubscribeAll()
		go c.ioLoop(conn)
		return
	}
}

// resubscribeAll re-sends FrameSub for every active subscription after reconnect.
func (c *Client) resubscribeAll() {
	c.subsMu.RLock()
	topics := make([]string, 0, len(c.subs))
	for t := range c.subs {
		topics = append(topics, t)
	}
	c.subsMu.RUnlock()

	for _, topic := range topics {
		select {
		case c.outbound <- &Frame{Type: FrameSub, Topic: topic}:
		case <-c.closeCh:
			return
		}
	}
	c.logger.Info("resubscribed after reconnect", "topics", len(topics))
}
