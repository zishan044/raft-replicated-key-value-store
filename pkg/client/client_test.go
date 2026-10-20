package client

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zishan044/raft-replicated-key-value-store.git/internal/node"
	"github.com/zishan044/raft-replicated-key-value-store.git/internal/raftcore"
	kvserver "github.com/zishan044/raft-replicated-key-value-store.git/internal/server"
	"github.com/zishan044/raft-replicated-key-value-store.git/internal/transport"
	kvv1 "github.com/zishan044/raft-replicated-key-value-store.git/pkg/api/kv/v1"
	raftv1 "github.com/zishan044/raft-replicated-key-value-store.git/pkg/api/raft/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type scripted struct {
	kvv1.UnimplementedKVServiceServer
	register func(context.Context) (*kvv1.RegisterClientResponse, error)
	put      func(context.Context, *kvv1.PutRequest) (*kvv1.PutResponse, error)
	cas      func(context.Context, *kvv1.CompareAndSwapRequest) (*kvv1.CompareAndSwapResponse, error)
	get      func(context.Context, *kvv1.GetRequest) (*kvv1.GetResponse, error)
}

func (s *scripted) RegisterClient(ctx context.Context, _ *kvv1.RegisterClientRequest) (*kvv1.RegisterClientResponse, error) {
	if s.register != nil {
		return s.register(ctx)
	}
	return &kvv1.RegisterClientResponse{ClientId: 42}, nil
}
func (s *scripted) Put(ctx context.Context, req *kvv1.PutRequest) (*kvv1.PutResponse, error) {
	return s.put(ctx, req)
}
func (s *scripted) CompareAndSwap(ctx context.Context, req *kvv1.CompareAndSwapRequest) (*kvv1.CompareAndSwapResponse, error) {
	return s.cas(ctx, req)
}
func (s *scripted) Get(ctx context.Context, req *kvv1.GetRequest) (*kvv1.GetResponse, error) {
	if s.get != nil {
		return s.get(ctx, req)
	}
	return &kvv1.GetResponse{Found: true, Value: []byte("available")}, nil
}
func (s *scripted) Status(context.Context, *kvv1.StatusRequest) (*kvv1.StatusResponse, error) {
	return &kvv1.StatusResponse{NodeId: 99, Role: kvv1.NodeRole_NODE_ROLE_FOLLOWER}, nil
}

func serve(t *testing.T, service kvv1.KVServiceServer) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	rpc := grpc.NewServer()
	kvv1.RegisterKVServiceServer(rpc, service)
	done := make(chan error, 1)
	go func() { done <- rpc.Serve(l) }()
	t.Cleanup(func() {
		rpc.Stop()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	return l.Addr().String()
}
func newClient(t *testing.T, seeds ...string) *Client {
	t.Helper()
	c, err := New(Config{Endpoints: seeds})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Error(err)
		}
	})
	return c
}
func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}
func typed(code kvv1.ErrorCode, grpcCode codes.Code) *status.Status {
	s, err := status.New(grpcCode, "scripted outcome").WithDetails(&kvv1.ErrorDetail{Code: code, CurrentRevision: 55})
	if err != nil {
		panic(err)
	}
	return s
}

func TestRedirectCachingAndSequences(t *testing.T) {
	var sequence atomic.Uint64
	leader := serve(t, &scripted{
		put: func(_ context.Context, r *kvv1.PutRequest) (*kvv1.PutResponse, error) {
			if r.Session.ClientId != 42 || r.Session.Sequence != sequence.Add(1) {
				t.Errorf("wrong session: %v", r.Session)
			}
			return &kvv1.PutResponse{Revision: r.Session.Sequence + 100}, nil
		},
		cas: func(_ context.Context, r *kvv1.CompareAndSwapRequest) (*kvv1.CompareAndSwapResponse, error) {
			if r.Session.Sequence != sequence.Add(1) || r.ExpectedRevision != 999 {
				t.Errorf("wrong CAS: %v", r)
			}
			return nil, typed(kvv1.ErrorCode_ERROR_CODE_CAS_CONFLICT, codes.Aborted).Err()
		},
	})
	var redirects atomic.Int32
	follower := serve(t, &scripted{register: func(context.Context) (*kvv1.RegisterClientResponse, error) {
		redirects.Add(1)
		s, err := status.New(codes.FailedPrecondition, "not leader").WithDetails(&kvv1.ErrorDetail{Code: kvv1.ErrorCode_ERROR_CODE_NOT_LEADER, LeaderId: 1, LeaderAddress: leader})
		return nil, errors.Join(err, s.Err())
	}})
	c := newClient(t, follower)
	ctx := testContext(t)
	if revision, err := c.Put(ctx, "key", []byte("first")); err != nil || revision != 101 {
		t.Fatalf("put: %d %v", revision, err)
	}
	if _, err := c.CompareAndSwap(ctx, "key", 999, []byte("conflict")); detailCode(err) != kvv1.ErrorCode_ERROR_CODE_CAS_CONFLICT {
		t.Fatalf("CAS: %v", err)
	}
	if revision, err := c.Put(ctx, "key", []byte("third")); err != nil || revision != 103 {
		t.Fatalf("sequence after conflict: %d %v", revision, err)
	}
	if redirects.Load() != 1 {
		t.Fatalf("leader hint was not cached: %d", redirects.Load())
	}
	if _, err := c.Get(ctx, "key"); err != nil {
		t.Fatal(err)
	}
	if st, err := c.Status(ctx, follower); err != nil || st.GetNodeId() != 99 || st.GetRole() != kvv1.NodeRole_NODE_ROLE_FOLLOWER {
		t.Fatalf("local status: %v %v", st, err)
	}
}

func TestUncertaintySurvivesRedirectAndRetainsCommand(t *testing.T) {
	var resolve atomic.Bool
	var reject atomic.Bool
	var writes atomic.Int32
	var original *kvv1.PutRequest
	var mu sync.Mutex
	leader := serve(t, &scripted{put: func(_ context.Context, r *kvv1.PutRequest) (*kvv1.PutResponse, error) {
		writes.Add(1)
		mu.Lock()
		defer mu.Unlock()
		if original == nil {
			original = proto.Clone(r).(*kvv1.PutRequest)
		}
		if r.Session.Sequence == 1 && !proto.Equal(r, original) {
			t.Errorf("pending command changed: %v", r)
		}
		if reject.Load() {
			return nil, status.Error(codes.InvalidArgument, "rejected before append on this attempt")
		}
		if !resolve.Load() {
			return nil, status.Error(codes.Unavailable, "response lost after applying")
		}
		return &kvv1.PutResponse{Revision: 76 + r.Session.Sequence}, nil
	}})
	var redirects atomic.Int32
	follower := serve(t, &scripted{put: func(context.Context, *kvv1.PutRequest) (*kvv1.PutResponse, error) {
		redirects.Add(1)
		s, err := status.New(codes.FailedPrecondition, "not leader").WithDetails(&kvv1.ErrorDetail{Code: kvv1.ErrorCode_ERROR_CODE_NOT_LEADER, LeaderAddress: leader})
		if err != nil {
			return nil, err
		}
		return nil, s.Err()
	}})
	c := newClient(t, leader, follower)
	value := []byte("original")
	ctx, cancel := context.WithTimeout(testContext(t), 300*time.Millisecond)
	_, err := c.Put(ctx, "key", value)
	cancel()
	if !errors.Is(err, ErrUnknown) || redirects.Load() == 0 {
		t.Fatalf("later redirect erased uncertainty: %v redirects=%d", err, redirects.Load())
	}
	value[0] = 'X'
	before := writes.Load()
	if _, err := c.Put(testContext(t), "new", []byte("forbidden")); !errors.Is(err, ErrPendingWrite) {
		t.Fatalf("new write admitted: %v", err)
	}
	if writes.Load() != before {
		t.Fatal("new write reached server")
	}
	if _, err := c.Get(testContext(t), "key"); err != nil {
		t.Fatal("pending write blocked read: ", err)
	}
	if _, err := c.Status(testContext(t), leader); err != nil {
		t.Fatal("pending write blocked status: ", err)
	}
	// A later pre-append rejection cannot settle an earlier admitted attempt.
	reject.Store(true)
	if _, err := c.RetryPending(testContext(t)); !errors.Is(err, ErrUnknown) {
		t.Fatalf("later rejection erased uncertainty: %v", err)
	}
	reject.Store(false)
	resolve.Store(true)
	revision, err := c.RetryPending(testContext(t))
	if err != nil || revision != 77 {
		t.Fatalf("resolve original result: %d %v", revision, err)
	}
	if _, err := c.RetryPending(testContext(t)); !errors.Is(err, ErrNoPending) {
		t.Fatalf("pending not cleared: %v", err)
	}
	if revision, err := c.Put(testContext(t), "key", []byte("next")); err != nil || revision != 78 {
		t.Fatalf("next sequence: %d %v", revision, err)
	}
}

func TestPendingConflictConsumesSequence(t *testing.T) {
	var resolve atomic.Bool
	var original *kvv1.CompareAndSwapRequest
	var mu sync.Mutex
	endpoint := serve(t, &scripted{
		cas: func(_ context.Context, r *kvv1.CompareAndSwapRequest) (*kvv1.CompareAndSwapResponse, error) {
			mu.Lock()
			defer mu.Unlock()
			if original == nil {
				original = proto.Clone(r).(*kvv1.CompareAndSwapRequest)
			}
			if !proto.Equal(r, original) {
				t.Errorf("CAS retry changed: %v", r)
			}
			if !resolve.Load() {
				return nil, typed(kvv1.ErrorCode_ERROR_CODE_UNKNOWN, codes.Aborted).Err()
			}
			return nil, typed(kvv1.ErrorCode_ERROR_CODE_CAS_CONFLICT, codes.Aborted).Err()
		},
		put: func(_ context.Context, r *kvv1.PutRequest) (*kvv1.PutResponse, error) {
			if r.Session.Sequence != 2 {
				t.Errorf("conflict did not consume sequence: %v", r)
			}
			return &kvv1.PutResponse{Revision: 99}, nil
		},
	})
	c := newClient(t, endpoint)
	ctx, cancel := context.WithTimeout(testContext(t), 60*time.Millisecond)
	_, err := c.CompareAndSwap(ctx, "key", 7, []byte("value"))
	cancel()
	if !errors.Is(err, ErrUnknown) {
		t.Fatal(err)
	}
	resolve.Store(true)
	_, err = c.RetryPending(testContext(t))
	var business *Error
	if !errors.As(err, &business) || business.Detail.Code != kvv1.ErrorCode_ERROR_CODE_CAS_CONFLICT || business.Detail.CurrentRevision != 55 {
		t.Fatalf("cached conflict: %v", err)
	}
	if _, err := c.Put(testContext(t), "key", []byte("after conflict")); err != nil {
		t.Fatal(err)
	}
}

func TestDefiniteRejectionsAndRegistrationFailure(t *testing.T) {
	var busy atomic.Bool
	busy.Store(true)
	var requests atomic.Int32
	endpoint := serve(t, &scripted{put: func(_ context.Context, r *kvv1.PutRequest) (*kvv1.PutResponse, error) {
		requests.Add(1)
		if r.Session.Sequence != 1 {
			t.Errorf("rejection advanced sequence: %v", r)
		}
		if busy.Load() {
			return nil, typed(kvv1.ErrorCode_ERROR_CODE_BUSY, codes.ResourceExhausted).Err()
		}
		if r.Key == "oversized" {
			return nil, status.Error(codes.ResourceExhausted, "too large")
		}
		return &kvv1.PutResponse{Revision: 9}, nil
	}})
	c := newClient(t, endpoint)
	ctx, cancel := context.WithCancel(testContext(t))
	// Cancel only after Busy has reached the client. A deadline racing with
	// another in-flight RPC would legitimately be Unknown instead.
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		err := invoke(ctx, method, req, reply, cc, opts...)
		if detailCode(rpcError(err)) == kvv1.ErrorCode_ERROR_CODE_BUSY {
			cancel()
		}
		return err
	}))
	if err != nil {
		t.Fatal(err)
	}
	c.connections[endpoint] = conn
	_, err = c.Put(ctx, "key", nil)
	cancel()
	if !errors.Is(err, context.Canceled) || errors.Is(err, ErrUnknown) {
		t.Fatalf("busy became uncertain: %v", err)
	}
	busy.Store(false)
	if _, err := c.Put(testContext(t), "oversized", nil); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("permanent rejection: %v", err)
	}
	if _, err := c.Put(testContext(t), "key", nil); err != nil {
		t.Fatal(err)
	}
	if requests.Load() < 3 {
		t.Fatal("did not exercise retry/rejections")
	}
	// Registration can be uncertain without dispatching the user's mutation.
	failed := serve(t, &scripted{register: func(context.Context) (*kvv1.RegisterClientResponse, error) {
		return nil, typed(kvv1.ErrorCode_ERROR_CODE_UNKNOWN, codes.Aborted).Err()
	}})
	unregistered := newClient(t, failed)
	ctx, cancel = context.WithTimeout(testContext(t), 40*time.Millisecond)
	_, err = unregistered.Put(ctx, "never sent", nil)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrUnknown) {
		t.Fatalf("unsent mutation became uncertain: %v", err)
	}
	if _, err := unregistered.RetryPending(testContext(t)); !errors.Is(err, ErrNoPending) {
		t.Fatal(err)
	}
}

func TestWriteSerializationCancellationAndClose(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	endpoint := serve(t, &scripted{put: func(ctx context.Context, r *kvv1.PutRequest) (*kvv1.PutResponse, error) {
		started <- struct{}{}
		select {
		case <-release:
			return &kvv1.PutResponse{Revision: r.Session.Sequence}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}})
	c := newClient(t, endpoint)
	done := make(chan error, 1)
	go func() { _, err := c.Put(testContext(t), "first", nil); done <- err }()
	select {
	case <-started:
	case <-testContext(t).Done():
		t.Fatal("first write never started")
	}
	ctx, cancel := context.WithTimeout(testContext(t), 20*time.Millisecond)
	_, err := c.Put(ctx, "waiting", nil)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	select {
	case <-started:
		t.Fatal("writes ran concurrently")
	default:
	}
	if _, err := c.Get(testContext(t), "key"); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if revision, err := c.Put(testContext(t), "second", nil); err != nil || revision != 2 {
		t.Fatalf("serialized sequence: %d %v", revision, err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Get(testContext(t), "key"); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed client: %v", err)
	}
}

func TestSDKWithPersistentServer(t *testing.T) {
	ctx := testContext(t)
	n, err := node.Open(node.Config{DataDir: t.TempDir(), Raft: raftcore.Config{ID: 1, Voters: []uint64{1}, ElectionTicks: 10, HeartbeatTicks: 2, Seed: 42}, Send: func([]*raftv1.Message) {}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Close() })
	st, err := n.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for range st.Raft.ElectionTimeout {
		if err := n.Tick(ctx); err != nil {
			t.Fatal(err)
		}
	}
	rpc, err := transport.NewServer(n.Step)
	if err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	kvserver.Register(rpc, n, map[uint64]string{1: l.Addr().String()})
	done := make(chan error, 1)
	go func() { done <- rpc.Serve(l) }()
	t.Cleanup(func() {
		rpc.Stop()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	c := newClient(t, l.Addr().String())
	if got, err := c.Get(ctx, "key"); err != nil || got.GetFound() {
		t.Fatalf("missing: %v %v", got, err)
	}
	// Reads must not register a session.
	if c.clientID != 0 {
		t.Fatal("read registered a session")
	}
	revision, err := c.Put(ctx, "key", []byte{0, 255})
	if err != nil {
		t.Fatal(err)
	}
	swapped, err := c.CompareAndSwap(ctx, "key", revision, []byte("new"))
	if err != nil || swapped <= revision {
		t.Fatalf("CAS: %d %v", swapped, err)
	}
	if got, err := c.Get(ctx, "key"); err != nil || string(got.GetValue()) != "new" || got.Revision != swapped {
		t.Fatalf("get: %v %v", got, err)
	}
	if _, err := c.Delete(ctx, "key"); err != nil {
		t.Fatal(err)
	}
	if revision, err := c.Delete(ctx, "key"); err != nil || revision != 0 {
		t.Fatalf("missing delete: %d %v", revision, err)
	}
}

func TestConfigurationAndCanceledInput(t *testing.T) {
	for _, cfg := range []Config{{}, {Endpoints: []string{"host"}}, {Endpoints: []string{"host:0"}}, {Endpoints: []string{"host:1"}, Timeout: -1}} {
		if c, err := New(cfg); err == nil {
			_ = c.Close()
			t.Errorf("accepted %v", cfg)
		}
	}
	c := newClient(t, "127.0.0.1:1")
	if c.timeout != 5*time.Second {
		t.Fatal("wrong default timeout")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Put(ctx, "key", nil); !errors.Is(err, context.Canceled) || errors.Is(err, ErrUnknown) {
		t.Fatal(err)
	}
	if _, err := c.RetryPending(testContext(t)); !errors.Is(err, ErrNoPending) {
		t.Fatal(err)
	}
}

func TestCloseInterruptsAdmittedWrite(t *testing.T) {
	started := make(chan struct{}, 1)
	endpoint := serve(t, &scripted{put: func(ctx context.Context, _ *kvv1.PutRequest) (*kvv1.PutResponse, error) {
		started <- struct{}{}
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	}})
	c := newClient(t, endpoint)
	ctx := testContext(t)
	done := make(chan error, 1)
	go func() { _, err := c.Put(ctx, "key", nil); done <- err }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("write never arrived")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, ErrUnknown) || !errors.Is(err, ErrClosed) {
			t.Fatalf("close lost uncertainty: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("close failed to interrupt write")
	}
}
