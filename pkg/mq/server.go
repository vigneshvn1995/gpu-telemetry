package mq

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// -------------------------------------------------------------------------
// BrokerServer configuration
// -------------------------------------------------------------------------

// ServerConfig holds all tunables for BrokerServer. Zero values use the
// documented defaults so callers only need to set fields they care about.
type ServerConfig struct {
	// Addr is the TCP address to listen on (default ":7777").
	Addr string

	// AdminAddr is the HTTP address for the health/stats endpoint (default ":7778").
	// Set to "-" to disable the admin HTTP server.
	AdminAddr string

	// ReadTimeout is the per-connection read deadline applied before every frame
	// read. Connections that don't send a frame within this window are closed.
	// Default: 60 s.
	ReadTimeout time.Duration

	// WriteTimeout is the per-frame write deadline. Default: 10 s.
	WriteTimeout time.Duration

	// MaxConnections caps the number of simultaneous TCP connections.
	// 0 means unlimited.
	MaxConnections int

	// PingInterval is how often the server sends FramePing to each client.
	// 0 disables pings. Default: 30 s.
	PingInterval time.Duration
}

func (c *ServerConfig) withDefaults() ServerConfig {
	out := *c
	if out.Addr == "" {
		out.Addr = ":7777"
	}
	if out.AdminAddr == "" {
		out.AdminAddr = ":7778"
	}
	if out.ReadTimeout == 0 {
		out.ReadTimeout = 60 * time.Second
	}
	if out.WriteTimeout == 0 {
		out.WriteTimeout = 10 * time.Second
	}
	if out.PingInterval == 0 {
		out.PingInterval = 30 * time.Second
	}
	return out
}

// -------------------------------------------------------------------------
// BrokerServer
// -------------------------------------------------------------------------

// BrokerServer exposes a Broker over TCP. It accepts connections from Streamers
// (publishers) and Collectors (subscribers) that communicate using the
// length-prefixed JSON frame protocol defined in protocol.go.
//
// Each connection runs three goroutines:
//
//  1. readLoop  – decodes frames from the TCP stream and dispatches them.
//  2. writeLoop – serialises outbound frames and flushes the buffered writer.
//  3. forwardLoop (one per subscription) – moves messages from a broker
//     subscription channel into the connection's outbound queue.
//
// This design ensures that only one goroutine ever writes to a given TCP
// connection, eliminating the need for a per-write mutex.
type BrokerServer struct {
	broker Broker
	cfg    ServerConfig
	logger *slog.Logger

	listener  net.Listener
	adminSrv  *http.Server
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
	connCount atomic.Int64
}

// NewBrokerServer creates a BrokerServer that uses the provided broker as its
// routing engine. Call Serve to start accepting connections.
func NewBrokerServer(broker Broker, cfg ServerConfig, logger *slog.Logger) *BrokerServer {
	ctx, cancel := context.WithCancel(context.Background())
	return &BrokerServer{
		broker: broker,
		cfg:    cfg.withDefaults(),
		logger: logger,
		ctx:    ctx,
		cancel: cancel,
	}
}

// Serve starts listening and blocks until ctx is cancelled or a fatal error
// occurs. It is safe to call Serve from a goroutine and stop it via Shutdown.
func (s *BrokerServer) Serve(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return fmt.Errorf("mq server: listen %s: %w", s.cfg.Addr, err)
	}
	return s.ServeListener(ctx, ln)
}

// ServeListener accepts connections from an already-bound listener. This is
// useful in tests where the caller needs to know the OS-assigned port before
// handing off control to the server.
func (s *BrokerServer) ServeListener(ctx context.Context, ln net.Listener) error {
	s.listener = ln
	s.logger.Info("broker TCP server listening", "addr", ln.Addr())

	// Start the optional admin HTTP server.
	if s.cfg.AdminAddr != "-" {
		s.startAdminServer()
	}

	// Accept loop.
	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			case <-s.ctx.Done():
				return nil
			default:
				s.logger.Error("accept error", "err", err)
				// Brief pause to avoid a tight CPU-spinning loop on persistent errors.
				time.Sleep(5 * time.Millisecond)
				continue
			}
		}

		// Enforce connection cap.
		if max := s.cfg.MaxConnections; max > 0 && s.connCount.Load() >= int64(max) {
			s.logger.Warn("connection limit reached, rejecting", "remote", conn.RemoteAddr())
			_ = conn.Close()
			continue
		}

		s.wg.Add(1)
		s.connCount.Add(1)
		go func() {
			defer s.wg.Done()
			defer s.connCount.Add(-1)
			s.handleConn(conn)
		}()
	}
}

// Shutdown gracefully stops the server. In-flight connection handlers are given
// up to the supplied timeout to finish before the function returns.
func (s *BrokerServer) Shutdown(timeout time.Duration) {
	s.cancel()
	if s.listener != nil {
		_ = s.listener.Close()
	}
	if s.adminSrv != nil {
		shutCtx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		_ = s.adminSrv.Shutdown(shutCtx)
	}

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		s.logger.Info("broker server shutdown complete")
	case <-time.After(timeout):
		s.logger.Warn("broker server shutdown timed out", "timeout", timeout)
	}
}

// -------------------------------------------------------------------------
// Admin HTTP server
// -------------------------------------------------------------------------

func (s *BrokerServer) startAdminServer() {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"status":"ok","connections":%d}`, s.connCount.Load())
	})
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		stats := s.broker.Stats()
		if err := json.NewEncoder(w).Encode(stats); err != nil {
			http.Error(w, "encode error", http.StatusInternalServerError)
		}
	})

	s.adminSrv = &http.Server{
		Addr:         s.cfg.AdminAddr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	go func() {
		s.logger.Info("broker admin server listening", "addr", s.cfg.AdminAddr)
		if err := s.adminSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.logger.Error("admin server error", "err", err)
		}
	}()
}

// -------------------------------------------------------------------------
// Per-connection session
// -------------------------------------------------------------------------

// connSession manages the lifecycle of a single TCP connection.
type connSession struct {
	id     string
	conn   net.Conn
	broker Broker
	cfg    ServerConfig
	logger *slog.Logger

	// outbound carries frames from forwardLoops → writeLoop.
	// Buffered to decouple the broker fan-out speed from TCP write speed.
	outbound chan *Frame

	// subs tracks all broker subscriptions opened by this connection so they
	// can be cleaned up on disconnect.
	subsMu sync.Mutex
	subs   []*sessionSub

	ctx    context.Context
	cancel context.CancelFunc
}

type sessionSub struct {
	topic string
	ch    <-chan []byte
}

var connCounter atomic.Uint64

func (s *BrokerServer) handleConn(conn net.Conn) {
	id := fmt.Sprintf("conn-%d", connCounter.Add(1))
	ctx, cancel := context.WithCancel(s.ctx)

	session := &connSession{
		id:       id,
		conn:     conn,
		broker:   s.broker,
		cfg:      s.cfg,
		logger:   s.logger.With("conn_id", id, "remote", conn.RemoteAddr()),
		outbound: make(chan *Frame, 512),
		ctx:      ctx,
		cancel:   cancel,
	}

	session.logger.Info("client connected")
	defer func() {
		cancel()
		_ = conn.Close()
		session.cleanupSubs()
		session.logger.Info("client disconnected")
	}()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		session.writeLoop()
	}()
	go func() {
		defer wg.Done()
		session.readLoop()
	}()

	// Start the ping ticker if configured.
	if s.cfg.PingInterval > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			session.pingLoop(s.cfg.PingInterval)
		}()
	}

	wg.Wait()
}

// readLoop reads frames from the TCP connection and dispatches them.
// It exits when the connection is closed or the session context is cancelled.
func (sess *connSession) readLoop() {
	defer sess.cancel() // signal writeLoop to exit

	br := bufio.NewReaderSize(sess.conn, 64*1024)

	for {
		if sess.cfg.ReadTimeout > 0 {
			_ = sess.conn.SetReadDeadline(time.Now().Add(sess.cfg.ReadTimeout))
		}

		f, err := ReadFrame(br)
		if err != nil {
			if !isClosedErr(err) {
				sess.logger.Debug("read error", "err", err)
			}
			return
		}

		sess.dispatch(f)
	}
}

// writeLoop drains the outbound queue and writes frames to the TCP connection.
func (sess *connSession) writeLoop() {
	bw := bufio.NewWriterSize(sess.conn, 64*1024)

	for {
		select {
		case f := <-sess.outbound:
			if sess.cfg.WriteTimeout > 0 {
				_ = sess.conn.SetWriteDeadline(time.Now().Add(sess.cfg.WriteTimeout))
			}
			if err := WriteFrame(bw, f); err != nil {
				if !isClosedErr(err) {
					sess.logger.Debug("write error", "err", err)
				}
				return
			}
			// Flush remaining frames before blocking again for better throughput.
			for len(sess.outbound) > 0 {
				f = <-sess.outbound
				if err := WriteFrame(bw, f); err != nil {
					return
				}
			}
			if err := bw.Flush(); err != nil {
				return
			}

		case <-sess.ctx.Done():
			// Drain any remaining frames before exiting.
			_ = bw.Flush()
			return
		}
	}
}

// pingLoop sends periodic FramePing probes to detect dead connections.
func (sess *connSession) pingLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			sess.send(&Frame{Type: FramePing})
		case <-sess.ctx.Done():
			return
		}
	}
}

// dispatch routes an inbound frame to the appropriate handler.
func (sess *connSession) dispatch(f *Frame) {
	switch f.Type {
	case FramePub:
		sess.handlePub(f)
	case FrameSub:
		sess.handleSub(f)
	case FrameUnsub:
		sess.handleUnsub(f)
	case FramePong:
		// Keep-alive response – no action needed.
	case FramePing:
		sess.send(&Frame{Type: FramePong, ID: f.ID})
	default:
		sess.logger.Warn("unknown frame type", "type", f.Type)
		sess.sendErr(f.ID, fmt.Sprintf("unknown frame type %q", f.Type))
	}
}

func (sess *connSession) handlePub(f *Frame) {
	if f.Topic == "" {
		sess.sendErr(f.ID, "pub: topic is required")
		return
	}
	if err := sess.broker.Publish(sess.ctx, f.Topic, f.Payload); err != nil {
		sess.sendErr(f.ID, fmt.Sprintf("pub: %v", err))
		return
	}
	if f.ID != "" {
		sess.send(&Frame{Type: FrameAck, ID: f.ID})
	}
	sess.logger.Debug("published", "topic", f.Topic, "bytes", len(f.Payload))
}

func (sess *connSession) handleSub(f *Frame) {
	if f.Topic == "" {
		sess.sendErr(f.ID, "sub: topic is required")
		return
	}
	ch, err := sess.broker.Subscribe(sess.ctx, f.Topic)
	if err != nil {
		sess.sendErr(f.ID, fmt.Sprintf("sub: %v", err))
		return
	}

	sub := &sessionSub{topic: f.Topic, ch: ch}
	sess.subsMu.Lock()
	sess.subs = append(sess.subs, sub)
	sess.subsMu.Unlock()

	// One goroutine per subscription forwards broker messages → outbound queue.
	go sess.forwardLoop(sub)

	if f.ID != "" {
		sess.send(&Frame{Type: FrameAck, ID: f.ID})
	}
	sess.logger.Info("subscribed", "topic", f.Topic)
}

func (sess *connSession) handleUnsub(f *Frame) {
	if f.Topic == "" {
		sess.sendErr(f.ID, "unsub: topic is required")
		return
	}

	sess.subsMu.Lock()
	var remaining []*sessionSub
	var found *sessionSub
	for _, s := range sess.subs {
		if found == nil && s.topic == f.Topic {
			found = s
		} else {
			remaining = append(remaining, s)
		}
	}
	sess.subs = remaining
	sess.subsMu.Unlock()

	if found == nil {
		sess.sendErr(f.ID, fmt.Sprintf("unsub: no subscription for topic %q", f.Topic))
		return
	}

	if err := sess.broker.Unsubscribe(found.topic, found.ch); err != nil {
		sess.logger.Warn("unsubscribe error", "topic", f.Topic, "err", err)
	}

	if f.ID != "" {
		sess.send(&Frame{Type: FrameAck, ID: f.ID})
	}
	sess.logger.Info("unsubscribed", "topic", f.Topic)
}

// forwardLoop reads from a broker subscription channel and pushes FrameMsg
// frames into the connection's outbound queue.
func (sess *connSession) forwardLoop(sub *sessionSub) {
	for {
		select {
		case msg, ok := <-sub.ch:
			if !ok {
				// Channel closed by broker (shutdown or explicit unsubscribe).
				return
			}
			sess.send(&Frame{
				Type:    FrameMsg,
				Topic:   sub.topic,
				Payload: json.RawMessage(msg),
			})
		case <-sess.ctx.Done():
			return
		}
	}
}

// send enqueues a frame for the writeLoop. Drops if the outbound queue is full
// to prevent a slow writer from deadlocking the broker fan-out.
func (sess *connSession) send(f *Frame) {
	select {
	case sess.outbound <- f:
	default:
		sess.logger.Warn("outbound queue full, dropping frame", "type", f.Type)
	}
}

func (sess *connSession) sendErr(id, msg string) {
	sess.send(&Frame{Type: FrameErr, ID: id, Error: msg})
}

// cleanupSubs unsubscribes all active subscriptions for this connection.
func (sess *connSession) cleanupSubs() {
	sess.subsMu.Lock()
	subs := sess.subs
	sess.subs = nil
	sess.subsMu.Unlock()

	for _, s := range subs {
		if err := sess.broker.Unsubscribe(s.topic, s.ch); err != nil {
			sess.logger.Debug("cleanup unsub error", "topic", s.topic, "err", err)
		}
	}
}

// -------------------------------------------------------------------------
// Helpers
// -------------------------------------------------------------------------

// isClosedErr returns true for errors that indicate a normal connection close.
func isClosedErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, net.ErrClosed) {
		return true
	}
	var netErr *net.OpError
	if errors.As(err, &netErr) {
		return !netErr.Temporary()
	}
	return false
}
