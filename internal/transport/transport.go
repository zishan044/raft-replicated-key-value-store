// Package transport delivers Raft messages over unary gRPC calls. Network I/O
// runs in per-peer workers; Send never waits for a connection or queue space.
package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/zishan044/raft-replicated-key-value-store.git/internal/raftcore"
	raftv1 "github.com/zishan044/raft-replicated-key-value-store.git/pkg/api/raft/v1"
	"github.com/zishan044/raft-replicated-key-value-store.git/pkg/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

const (
	queueCapacity = 64
	rpcTimeout    = time.Second
)

type peer struct {
	queue  chan *raftv1.Message
	conn   *grpc.ClientConn
	client raftv1.PeerServiceClient
}

// Transport implements raft.Transport. Peers and message contents are immutable
// after construction/handoff. Send and Close are safe for concurrent use.
type Transport struct {
	id     uint64
	peers  map[uint64]*peer
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	mu     sync.RWMutex // protects admission during Close; never held over I/O
	closed bool
	once   sync.Once
	err    error
}

var _ raft.Transport = (*Transport)(nil)

// New copies a fixed peer map of positive IDs to host:port TCP addresses. An
// optional local entry is validated but creates no connection. Connections are
// lazy: construction does not wait for peers to be reachable.
func New(id uint64, peers map[uint64]string) (*Transport, error) {
	if id == 0 {
		return nil, errors.New("local node ID must be positive")
	}
	for peerID, address := range peers {
		host, port, err := net.SplitHostPort(address)
		n, portErr := strconv.Atoi(port)
		if peerID == 0 || err != nil || host == "" || portErr != nil || n <= 0 || n > 65535 {
			return nil, fmt.Errorf("peer %d requires a positive ID and host:port address", peerID)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	t := &Transport{id: id, peers: make(map[uint64]*peer), ctx: ctx, cancel: cancel}
	for peerID, address := range peers {
		if peerID == id {
			continue
		}
		conn, err := grpc.NewClient("passthrough:///"+address,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(
				grpc.MaxCallSendMsgSize(raftcore.MaxPeerMessageBytes),
				grpc.MaxCallRecvMsgSize(raftcore.MaxPeerMessageBytes),
			),
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff:           backoff.Config{BaseDelay: 100 * time.Millisecond, Multiplier: 1.6, Jitter: 0.2, MaxDelay: time.Second},
				MinConnectTimeout: time.Second,
			}),
		)
		if err != nil {
			return nil, errors.Join(err, t.Close())
		}
		t.peers[peerID] = &peer{queue: make(chan *raftv1.Message, queueCapacity), conn: conn, client: raftv1.NewPeerServiceClient(conn)}
	}
	for _, p := range t.peers {
		t.wg.Add(1)
		go t.run(p)
	}
	return t, nil
}

// Send takes immutable ownership as specified by raft.Transport. Full queues,
// unknown recipients, wrong senders, oversized messages and closed transports
// drop messages. Raft's ticks and responses drive subsequent logical retries.
func (t *Transport) Send(messages []*raftv1.Message) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.closed {
		return
	}
	for _, msg := range messages {
		if msg == nil || msg.From != t.id {
			continue
		}
		p := t.peers[msg.To]
		if p == nil || proto.Size(&raftv1.DeliverRequest{Message: msg}) > raftcore.MaxPeerMessageBytes {
			continue
		}
		select {
		case p.queue <- msg:
		default:
		}
	}
}

func (t *Transport) run(p *peer) {
	defer t.wg.Done()
	for {
		select {
		case <-t.ctx.Done():
			return
		case msg := <-p.queue:
			if t.ctx.Err() != nil {
				return
			}
			// ponytail: one RPC per peer at a time; add pipelining only if
			// measured replication throughput needs it.
			ctx, cancel := context.WithTimeout(t.ctx, rpcTimeout)
			_, _ = p.client.Deliver(ctx, &raftv1.DeliverRequest{Message: msg})
			cancel()
		}
	}
}

// Close stops admission, cancels RPCs, closes connections and joins workers.
// Queue channels stay open so concurrent Send calls cannot panic. Queued
// messages are discarded and released. Repeated calls return the same error.
func (t *Transport) Close() error {
	t.once.Do(func() {
		t.mu.Lock()
		t.closed = true
		t.mu.Unlock()
		t.cancel()
		for _, p := range t.peers {
			t.err = errors.Join(t.err, p.conn.Close())
		}
		t.wg.Wait()
		for _, p := range t.peers {
			for len(p.queue) > 0 {
				<-p.queue
			}
		}
	})
	return t.err
}
