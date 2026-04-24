package mq_test

import (
	"context"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gpu-telemetry/pkg/mq"
)

// -------------------------------------------------------------------------
// MemoryBroker unit tests
// -------------------------------------------------------------------------

func TestMemoryBroker_PublishSubscribe(t *testing.T) {
	t.Parallel()
	b := mq.NewMemoryBroker()
	defer b.Close()

	ctx := context.Background()

	ch, err := b.Subscribe(ctx, "test.topic")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	want := []byte("hello broker")
	if err := b.Publish(ctx, "test.topic", want); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case got := <-ch:
		if string(got) != string(want) {
			t.Fatalf("got %q, want %q", got, want)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for message")
	}
}

func TestMemoryBroker_FanOut(t *testing.T) {
	t.Parallel()
	b := mq.NewMemoryBroker()
	defer b.Close()

	ctx := context.Background()
	const numSubs = 5

	channels := make([]<-chan []byte, numSubs)
	for i := range channels {
		ch, err := b.Subscribe(ctx, "fan.out")
		if err != nil {
			t.Fatalf("Subscribe[%d]: %v", i, err)
		}
		channels[i] = ch
	}

	msg := []byte("broadcast")
	if err := b.Publish(ctx, "fan.out", msg); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	for i, ch := range channels {
		select {
		case got := <-ch:
			if string(got) != string(msg) {
				t.Fatalf("sub[%d] got %q, want %q", i, got, msg)
			}
		case <-time.After(time.Second):
			t.Fatalf("sub[%d] timed out", i)
		}
	}
}

func TestMemoryBroker_Unsubscribe(t *testing.T) {
	t.Parallel()
	b := mq.NewMemoryBroker()
	defer b.Close()

	ctx := context.Background()

	ch, err := b.Subscribe(ctx, "unsub.topic")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if err := b.Unsubscribe("unsub.topic", ch); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}

	// Channel must be closed after unsubscribe.
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("channel should be closed")
		}
	case <-time.After(time.Second):
		t.Fatal("channel not closed after Unsubscribe")
	}

	// Publishing to a topic with no subscribers is a no-op.
	if err := b.Publish(ctx, "unsub.topic", []byte("noop")); err != nil {
		t.Fatalf("Publish after unsub: %v", err)
	}
}

func TestMemoryBroker_ConcurrentPublish(t *testing.T) {
	t.Parallel()
	b := mq.NewMemoryBroker(mq.WithBufferSize(1024))
	defer b.Close()

	ctx := context.Background()
	ch, _ := b.Subscribe(ctx, "concurrent")

	const numPublishers = 10
	const msgsPerPublisher = 100

	var wg sync.WaitGroup
	for p := 0; p < numPublishers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for m := 0; m < msgsPerPublisher; m++ {
				_ = b.Publish(ctx, "concurrent", []byte("msg"))
			}
		}(p)
	}
	wg.Wait()

	// Drain the channel (some messages may be dropped if buffer filled up).
	received := 0
drain:
	for {
		select {
		case <-ch:
			received++
		case <-time.After(100 * time.Millisecond):
			break drain
		}
	}

	stats := b.Stats()
	total := int(stats.TotalDelivered + stats.TotalDropped)
	want := numPublishers * msgsPerPublisher
	if total != want {
		t.Errorf("delivered+dropped = %d, want %d", total, want)
	}
	t.Logf("delivered=%d dropped=%d received=%d", stats.TotalDelivered, stats.TotalDropped, received)
}

func TestMemoryBroker_CloseSignalsSubscribers(t *testing.T) {
	t.Parallel()
	b := mq.NewMemoryBroker()

	ctx := context.Background()
	ch, _ := b.Subscribe(ctx, "close.test")

	_ = b.Close()

	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected channel to be closed on broker shutdown")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for channel close")
	}
}

func TestMemoryBroker_PublishAfterClose(t *testing.T) {
	t.Parallel()
	b := mq.NewMemoryBroker()
	_ = b.Close()

	err := b.Publish(context.Background(), "any", []byte("x"))
	if err != mq.ErrBrokerClosed {
		t.Fatalf("expected ErrBrokerClosed, got %v", err)
	}
}

func TestMemoryBroker_WildcardSubscription(t *testing.T) {
	t.Parallel()
	b := mq.NewMemoryBroker()
	defer b.Close()

	ctx := context.Background()

	// Subscribe with a glob pattern.
	ch, err := b.Subscribe(ctx, "gpu.*")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	_ = b.Publish(ctx, "gpu.util", []byte("util-data"))
	_ = b.Publish(ctx, "gpu.mem", []byte("mem-data"))

	received := make(map[string]bool)
	deadline := time.After(time.Second)
	for len(received) < 2 {
		select {
		case msg := <-ch:
			received[string(msg)] = true
		case <-deadline:
			t.Fatalf("timed out; received: %v", received)
		}
	}
}

func TestMemoryBroker_Stats(t *testing.T) {
	t.Parallel()
	b := mq.NewMemoryBroker()
	defer b.Close()

	ctx := context.Background()
	ch, _ := b.Subscribe(ctx, "stats.topic")

	const n = 5
	for i := 0; i < n; i++ {
		_ = b.Publish(ctx, "stats.topic", []byte("x"))
	}
	// Drain to avoid blocking.
	for i := 0; i < n; i++ {
		<-ch
	}

	stats := b.Stats()
	if stats.TotalPublished != n {
		t.Errorf("TotalPublished=%d, want %d", stats.TotalPublished, n)
	}
	if stats.TotalDelivered != n {
		t.Errorf("TotalDelivered=%d, want %d", stats.TotalDelivered, n)
	}
	if stats.ActiveTopics != 1 {
		t.Errorf("ActiveTopics=%d, want 1", stats.ActiveTopics)
	}
}

// -------------------------------------------------------------------------
// TCP integration: BrokerServer + Client round-trip
// -------------------------------------------------------------------------

func TestServerClient_PubSub(t *testing.T) {
	t.Parallel()

	logger := newTestLogger()

	broker := mq.NewMemoryBroker(mq.WithBrokerLogger(logger))
	defer broker.Close()

	srv := mq.NewBrokerServer(broker, mq.ServerConfig{
		Addr:         "127.0.0.1:0", // OS-assigned port
		AdminAddr:    "-",           // disable admin HTTP
		PingInterval: 0,             // disable pings in tests
	}, logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start server in background; grab the port it bound to.
	srvReady := make(chan string, 1)
	go func() {
		// Use a custom listener so we can grab the address before Serve blocks.
		ln, err := listenTCP("127.0.0.1:0")
		if err != nil {
			t.Errorf("listen: %v", err)
			close(srvReady)
			return
		}
		srvReady <- ln.Addr().String()
		_ = srv.ServeListener(ctx, ln)
	}()

	addr := <-srvReady
	if addr == "" {
		t.Fatal("server did not start")
	}

	// Subscriber client.
	subClient := mq.NewClient(mq.ClientConfig{Addr: addr}, logger)
	if err := subClient.Connect(ctx); err != nil {
		t.Fatalf("subscriber connect: %v", err)
	}
	defer subClient.Close()

	msgCh, err := subClient.Subscribe(ctx, "integration.test")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Wait until the broker has registered the subscription before publishing.
	waitForSubscribers(t, broker, 1, 3*time.Second)

	// Publisher client.
	pubClient := mq.NewClient(mq.ClientConfig{Addr: addr}, logger)
	if err := pubClient.Connect(ctx); err != nil {
		t.Fatalf("publisher connect: %v", err)
	}
	defer pubClient.Close()

	want := `{"metric":"GPU_UTIL","value":99}`
	if err := pubClient.Publish(ctx, "integration.test", []byte(want)); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case got := <-msgCh:
		if string(got) != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for message")
	}
}

func TestServerClient_MultipleSubscribers(t *testing.T) {
	t.Parallel()

	logger := newTestLogger()
	broker := mq.NewMemoryBroker()
	defer broker.Close()

	srv := mq.NewBrokerServer(broker, mq.ServerConfig{
		Addr:      "127.0.0.1:0",
		AdminAddr: "-",
	}, logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ln, _ := listenTCP("127.0.0.1:0")
	addr := ln.Addr().String()
	go func() { _ = srv.ServeListener(ctx, ln) }()

	const numSubs = 3
	channels := make([]<-chan []byte, numSubs)
	for i := range channels {
		c := mq.NewClient(mq.ClientConfig{Addr: addr}, logger)
		_ = c.Connect(ctx)
		defer c.Close()
		channels[i], _ = c.Subscribe(ctx, "multi.topic")
	}
	waitForSubscribers(t, broker, numSubs, 3*time.Second)

	pub := mq.NewClient(mq.ClientConfig{Addr: addr}, logger)
	_ = pub.Connect(ctx)
	defer pub.Close()
	_ = pub.Publish(ctx, "multi.topic", []byte(`"broadcast"`))

	var received atomic.Int32
	var wg sync.WaitGroup
	for _, ch := range channels {
		ch := ch
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case <-ch:
				received.Add(1)
			case <-time.After(3 * time.Second):
			}
		}()
	}
	wg.Wait()

	if int(received.Load()) != numSubs {
		t.Errorf("received by %d subscribers, want %d", received.Load(), numSubs)
	}
}

// -------------------------------------------------------------------------
// Helpers
// -------------------------------------------------------------------------

// waitForSubscribers polls broker.Stats() until TotalSubscribers >= want or the
// deadline is reached.  This replaces fragile time.Sleep calls in tests.
func waitForSubscribers(t *testing.T, b *mq.MemoryBroker, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if int(b.Stats().TotalSubscribers) >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d subscribers (got %d)", want, b.Stats().TotalSubscribers)
}

func newTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

func listenTCP(addr string) (net.Listener, error) {
	return net.Listen("tcp", addr)
}

// -------------------------------------------------------------------------
// Additional MemoryBroker coverage
// -------------------------------------------------------------------------

func TestMemoryBroker_Topics(t *testing.T) {
	t.Parallel()
	b := mq.NewMemoryBroker()
	defer b.Close()

	ctx := context.Background()

	if topics := b.Topics(); len(topics) != 0 {
		t.Fatalf("want 0 topics before subscribe, got %v", topics)
	}

	_, _ = b.Subscribe(ctx, "topic.a")
	_, _ = b.Subscribe(ctx, "topic.b")

	topics := b.Topics()
	found := map[string]bool{}
	for _, tp := range topics {
		found[tp] = true
	}
	if !found["topic.a"] || !found["topic.b"] {
		t.Errorf("expected topic.a and topic.b; got %v", topics)
	}
}

func TestMemoryBroker_DoubleClose(t *testing.T) {
	t.Parallel()
	b := mq.NewMemoryBroker()

	if err := b.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	// Second close must be a no-op, not a panic.
	if err := b.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestMemoryBroker_SubscribeAfterClose(t *testing.T) {
	t.Parallel()
	b := mq.NewMemoryBroker()
	_ = b.Close()

	_, err := b.Subscribe(context.Background(), "any")
	if err != mq.ErrBrokerClosed {
		t.Fatalf("expected ErrBrokerClosed, got %v", err)
	}
}

func TestMemoryBroker_UnsubscribeUnknownChannel(t *testing.T) {
	t.Parallel()
	b := mq.NewMemoryBroker()
	defer b.Close()

	// Create a channel that is NOT registered with the broker.
	phantom := make(chan []byte, 1)
	err := b.Unsubscribe("no.such.topic", phantom)
	if err == nil {
		t.Fatal("expected error when unsubscribing unknown channel, got nil")
	}
}

func TestMemoryBroker_PublishNoSubscribers_CountsPublished(t *testing.T) {
	t.Parallel()
	b := mq.NewMemoryBroker()
	defer b.Close()

	ctx := context.Background()
	const n = 3
	for i := 0; i < n; i++ {
		if err := b.Publish(ctx, "empty.topic", []byte("x")); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	stats := b.Stats()
	if stats.TotalPublished != n {
		t.Errorf("TotalPublished = %d, want %d", stats.TotalPublished, n)
	}
	// No subscribers → nothing delivered.
	if stats.TotalDelivered != 0 {
		t.Errorf("TotalDelivered = %d, want 0", stats.TotalDelivered)
	}
}

func TestMemoryBroker_DropCounter(t *testing.T) {
	t.Parallel()
	// Create a broker with buffer size 1 so overflow is easy to trigger.
	b := mq.NewMemoryBroker(mq.WithBufferSize(1))
	defer b.Close()

	ctx := context.Background()
	ch, _ := b.Subscribe(ctx, "drop.test")

	// Publish 20 messages without draining; all but 1 will be dropped.
	const total = 20
	for i := 0; i < total; i++ {
		_ = b.Publish(ctx, "drop.test", []byte("x"))
	}

	stats := b.Stats()
	if stats.TotalPublished != total {
		t.Errorf("TotalPublished = %d, want %d", stats.TotalPublished, total)
	}
	if stats.TotalDelivered+stats.TotalDropped != total {
		t.Errorf("delivered(%d) + dropped(%d) != total(%d)", stats.TotalDelivered, stats.TotalDropped, total)
	}
	if stats.TotalDropped == 0 {
		t.Error("expected some messages to be dropped with buffer size 1")
	}
	// Drain to avoid goroutine leak.
	select {
	case <-ch:
	default:
	}
}

func TestMemoryBroker_CancelledContextPublish(t *testing.T) {
	t.Parallel()
	b := mq.NewMemoryBroker()
	defer b.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // immediately cancel

	err := b.Publish(ctx, "topic", []byte("x"))
	if err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}
}

func TestMemoryBroker_CancelledContextSubscribe(t *testing.T) {
	t.Parallel()
	b := mq.NewMemoryBroker()
	defer b.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := b.Subscribe(ctx, "topic")
	if err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}
}
