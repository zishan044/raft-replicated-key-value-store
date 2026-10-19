package transport

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zishan044/raft-replicated-key-value-store.git/internal/raftcore"
	raftv1 "github.com/zishan044/raft-replicated-key-value-store.git/pkg/api/raft/v1"
	"google.golang.org/grpc"
)

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func serve(t *testing.T, address string, step func(context.Context, *raftv1.Message) error) (string, func()) {
	t.Helper()
	if address == "" {
		address = "127.0.0.1:0"
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(step)
	if err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	stop := sync.OnceFunc(func() {
		server.Stop()
		if err := <-done; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Error(err)
		}
	})
	t.Cleanup(stop)
	return listener.Addr().String(), stop
}

func openTransport(t *testing.T, id uint64, peers map[uint64]string) *Transport {
	t.Helper()
	tr, err := New(id, peers)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := tr.Close(); err != nil {
			t.Error(err)
		}
	})
	return tr
}

func message(to, term uint64) *raftv1.Message {
	return &raftv1.Message{Type: raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE, From: 1, To: to, Term: term}
}

func receive(t *testing.T, messages <-chan *raftv1.Message) *raftv1.Message {
	t.Helper()
	select {
	case msg := <-messages:
		return msg
	case <-testContext(t).Done():
		t.Fatal("peer did not receive a message")
		return nil
	}
}

func TestSendDropsFullQueuesAndInvalidEnvelopes(t *testing.T) {
	// Leave the worker unstarted so overflow can be inspected without timing.
	// Production and this check use the same Send and queue implementation.
	queue := make(chan *raftv1.Message, queueCapacity)
	tr := &Transport{id: 1, peers: map[uint64]*peer{2: {queue: queue}}}
	oversized := message(2, 1)
	oversized.Entries = []*raftv1.Entry{{Data: make([]byte, raftcore.MaxPeerMessageBytes)}}
	wrongSender := message(2, 1)
	wrongSender.From = 3
	tr.Send([]*raftv1.Message{nil, message(99, 1), wrongSender, oversized})
	if len(queue) != 0 {
		t.Fatal("invalid or oversized message was admitted")
	}
	batch := make([]*raftv1.Message, queueCapacity+10)
	for i := range batch {
		batch[i] = message(2, uint64(i+1))
	}
	done := make(chan struct{})
	go func() { tr.Send(batch); close(done) }()
	select {
	case <-done:
	case <-testContext(t).Done():
		t.Fatal("Send blocked on a full queue")
	}
	if len(queue) != queueCapacity {
		t.Fatal("queue exceeded its capacity or dropped admitted messages")
	}
	for i := range queueCapacity {
		if got := <-queue; got != batch[i] {
			t.Fatal("Send changed message ownership or queue order")
		}
	}
}

func TestLoopbackDeliveryAndPeerMapOwnership(t *testing.T) {
	received := make(chan *raftv1.Message, 8)
	address, _ := serve(t, "", func(_ context.Context, msg *raftv1.Message) error {
		received <- msg
		return nil
	})
	peers := map[uint64]string{1: address, 2: address}
	tr := openTransport(t, 1, peers)
	delete(peers, 2)
	if len(tr.peers) != 1 {
		t.Fatal("local entry created a peer connection")
	}
	for _, kind := range []raftv1.MessageType{
		raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE,
		raftv1.MessageType_MESSAGE_TYPE_APPEND_ENTRIES,
		raftv1.MessageType_MESSAGE_TYPE_APPEND_ENTRIES_RESPONSE,
	} {
		msg := message(2, 1)
		msg.Type = kind
		tr.Send([]*raftv1.Message{msg})
		if got := receive(t, received); got.Type != kind || got.From != 1 || got.To != 2 {
			t.Fatalf("message changed during delivery: %v", got)
		}
	}
}

func TestSlowPeerDoesNotBlockHealthyPeerOrSend(t *testing.T) {
	entered := make(chan struct{}, 1)
	var active atomic.Bool
	slow, _ := serve(t, "", func(ctx context.Context, _ *raftv1.Message) error {
		active.Store(true)
		defer active.Store(false)
		select {
		case entered <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return ctx.Err()
	})
	type observation struct {
		term uint64
		slow bool
	}
	seen := make(chan observation, 2)
	healthy, _ := serve(t, "", func(_ context.Context, msg *raftv1.Message) error {
		seen <- observation{term: msg.Term, slow: active.Load()}
		return nil
	})
	tr := openTransport(t, 1, map[uint64]string{2: slow, 3: healthy})
	ctx := testContext(t)
	// Establish the healthy connection before observing concurrent RPCs.
	tr.Send([]*raftv1.Message{message(3, 1)})
	select {
	case <-seen:
	case <-ctx.Done():
		t.Fatal("healthy connection did not start")
	}
	tr.Send([]*raftv1.Message{message(2, 1)})
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("slow RPC did not start")
	}
	batch := make([]*raftv1.Message, queueCapacity+20)
	for i := range batch {
		batch[i] = message(2, 1)
	}
	batch = append(batch, message(3, 2))
	done := make(chan struct{})
	go func() { tr.Send(batch); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("Send waited for a slow peer")
	}
	select {
	case got := <-seen:
		if got.term != 2 || !got.slow {
			t.Fatal("healthy delivery waited for the slow peer to finish")
		}
	case <-ctx.Done():
		t.Fatal("healthy peer was blocked")
	}
}

func TestWorkerDeadlineAdvancesQueue(t *testing.T) {
	entered := make(chan time.Duration, 1)
	received := make(chan *raftv1.Message, 1)
	address, _ := serve(t, "", func(ctx context.Context, msg *raftv1.Message) error {
		if msg.Term == 1 {
			deadline, ok := ctx.Deadline()
			if !ok {
				return errors.New("missing RPC deadline")
			}
			entered <- time.Until(deadline)
			<-ctx.Done()
			return ctx.Err()
		}
		received <- msg
		return nil
	})
	tr := openTransport(t, 1, map[uint64]string{2: address})
	tr.Send([]*raftv1.Message{message(2, 1)})
	select {
	case remaining := <-entered:
		if remaining <= 0 || remaining > rpcTimeout {
			t.Fatalf("RPC deadline is not bounded: %v", remaining)
		}
	case <-testContext(t).Done():
		t.Fatal("RPC did not start")
	}
	tr.Send([]*raftv1.Message{message(2, 2)})
	if receive(t, received).Term != 2 {
		t.Fatal("failed RPC prevented subsequent delivery")
	}
}

func TestReconnectAfterPeerRestart(t *testing.T) {
	received := make(chan *raftv1.Message, 32)
	deliver := func(ctx context.Context, msg *raftv1.Message) error {
		select {
		case received <- msg:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	address, stop := serve(t, "", deliver)
	tr := openTransport(t, 1, map[uint64]string{2: address})
	tr.Send([]*raftv1.Message{message(2, 1)})
	receive(t, received)
	stop()
	tr.Send([]*raftv1.Message{message(2, 2)}) // loss while offline is permitted
	serve(t, address, deliver)
	ctx := testContext(t)
	retry := time.NewTicker(20 * time.Millisecond)
	defer retry.Stop()
	for {
		select {
		case msg := <-received:
			if msg.Term == 3 {
				return
			}
		case <-retry.C:
			tr.Send([]*raftv1.Message{message(2, 3)}) // Raft supplies logical retries
		case <-ctx.Done():
			t.Fatal("existing connection did not recover after restart")
		}
	}
}

func TestConcurrentSendAndCloseCancelsRPCsAndReleasesQueues(t *testing.T) {
	entered := make(chan struct{}, 1)
	exited := make(chan struct{}, 1)
	address, _ := serve(t, "", func(ctx context.Context, _ *raftv1.Message) error {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-ctx.Done()
		select {
		case exited <- struct{}{}:
		default:
		}
		return ctx.Err()
	})
	tr := openTransport(t, 1, map[uint64]string{2: address})
	tr.Send([]*raftv1.Message{message(2, 1)})
	select {
	case <-entered:
	case <-testContext(t).Done():
		t.Fatal("RPC did not start")
	}
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			for range 50 {
				tr.Send([]*raftv1.Message{message(2, 2)})
			}
			if err := tr.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	select {
	case <-exited:
	case <-testContext(t).Done():
		t.Fatal("shutdown did not cancel the active RPC")
	}
	tr.Send([]*raftv1.Message{message(2, 3)})
	if len(tr.peers[2].queue) != 0 {
		t.Fatal("shutdown retained messages or allowed new admission")
	}
}

func TestConfigurationValidationAndEmptyTransport(t *testing.T) {
	for _, peers := range []map[uint64]string{
		{0: "127.0.0.1:1234"}, {2: ""}, {2: "missing-port"},
		{2: ":1234"}, {2: "127.0.0.1:0"}, {2: "127.0.0.1:65536"}, {2: "127.0.0.1:http"},
	} {
		if tr, err := New(1, peers); tr != nil || err == nil {
			t.Fatalf("invalid peer map accepted: %v %v", tr, err)
		}
	}
	if tr, err := New(0, nil); tr != nil || err == nil {
		t.Fatal("zero local ID accepted")
	}
	if server, err := NewServer(nil); server != nil || err == nil {
		t.Fatal("nil Step callback accepted")
	}
	tr := openTransport(t, 1, nil)
	tr.Send([]*raftv1.Message{message(2, 1)})
	if err := tr.Close(); err != nil {
		t.Fatal(err)
	}
}
