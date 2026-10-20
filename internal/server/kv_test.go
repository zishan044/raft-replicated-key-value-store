package server

import (
	"bytes"
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/zishan044/raft-replicated-key-value-store.git/internal/node"
	"github.com/zishan044/raft-replicated-key-value-store.git/internal/raftcore"
	"github.com/zishan044/raft-replicated-key-value-store.git/internal/transport"
	kvv1 "github.com/zishan044/raft-replicated-key-value-store.git/pkg/api/kv/v1"
	raftv1 "github.com/zishan044/raft-replicated-key-value-store.git/pkg/api/raft/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type fixture struct {
	n      *node.Node
	client kvv1.KVServiceClient
	peer   raftv1.PeerServiceClient
	ctx    context.Context
	out    chan *raftv1.Message
}

func newFixture(t *testing.T, voters ...uint64) *fixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	f := &fixture{ctx: ctx, out: make(chan *raftv1.Message, 128)}
	n, err := node.Open(node.Config{DataDir: t.TempDir(), Raft: raftcore.Config{ID: 1, Voters: voters, ElectionTicks: 10, HeartbeatTicks: 2, Seed: 42}, Send: func(messages []*raftv1.Message) {
		for _, msg := range messages {
			select {
			case f.out <- msg:
			default:
			}
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	f.n = n
	t.Cleanup(func() {
		if err := n.Close(); err != nil {
			t.Error(err)
		}
	})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	rpc, err := transport.NewServer(n.Step)
	if err != nil {
		t.Fatal(err)
	}
	Register(rpc, n, map[uint64]string{1: listener.Addr().String(), 2: "127.0.0.1:9002"})
	done := make(chan error, 1)
	go func() { done <- rpc.Serve(listener) }()
	t.Cleanup(func() {
		rpc.Stop()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	f.client = kvv1.NewKVServiceClient(conn)
	f.peer = raftv1.NewPeerServiceClient(conn)
	return f
}
func (f *fixture) local(t *testing.T) node.Status {
	t.Helper()
	st, err := f.n.Status(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	return st
}
func (f *fixture) elect(t *testing.T) {
	t.Helper()
	for range f.local(t).Raft.ElectionTimeout {
		if err := f.n.Tick(f.ctx); err != nil {
			t.Fatal(err)
		}
	}
	st := f.local(t).Raft
	if st.Role != raftcore.Leader {
		if err := f.n.Step(f.ctx, &raftv1.Message{Type: raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE_RESPONSE, From: 2, To: 1, Term: st.HardState.Term}); err != nil {
			t.Fatal(err)
		}
	}
	if f.local(t).Raft.Role != raftcore.Leader {
		t.Fatal("election failed")
	}
}
func errorDetail(t *testing.T, err error, code codes.Code, business kvv1.ErrorCode) *kvv1.ErrorDetail {
	t.Helper()
	if status.Code(err) != code {
		t.Fatalf("got %v, want %v", err, code)
	}
	for _, detail := range status.Convert(err).Details() {
		if detail, ok := detail.(*kvv1.ErrorDetail); ok && detail.Code == business {
			return detail
		}
	}
	t.Fatalf("missing typed detail %v: %v", business, err)
	return nil
}

func TestOperationsOverRPC(t *testing.T) {
	f := newFixture(t, 1)
	f.elect(t)
	registered, err := f.client.RegisterClient(f.ctx, &kvv1.RegisterClientRequest{})
	if err != nil {
		t.Fatal(err)
	}
	session := func(seq uint64) *kvv1.WriteSession {
		return &kvv1.WriteSession{ClientId: registered.ClientId, Sequence: seq}
	}
	put := &kvv1.PutRequest{Session: session(1), Key: "key", Value: []byte{0, 255, 1}}
	written, err := f.client.Put(f.ctx, put)
	if err != nil || written.GetRevision() <= registered.ClientId {
		t.Fatalf("put: %v %v", written, err)
	}
	retry, err := f.client.Put(f.ctx, put)
	if err != nil || !proto.Equal(retry, written) {
		t.Fatalf("retry: %v %v", retry, err)
	}
	before := f.local(t).Raft.LastLogIndex
	got, err := f.client.Get(f.ctx, &kvv1.GetRequest{Key: "key"})
	if err != nil || !got.GetFound() || got.Revision != written.Revision || !bytes.Equal(got.Value, put.Value) {
		t.Fatalf("get: %v %v", got, err)
	}
	if f.local(t).Raft.LastLogIndex != before+1 {
		t.Fatal("read did not add a log entry")
	}
	conflict := &kvv1.CompareAndSwapRequest{Session: session(2), Key: "key", Value: []byte("replacement")}
	_, err = f.client.CompareAndSwap(f.ctx, conflict)
	first := errorDetail(t, err, codes.Aborted, kvv1.ErrorCode_ERROR_CODE_CAS_CONFLICT)
	if first.CurrentRevision != written.Revision {
		t.Fatalf("conflict: %v", first)
	}
	// A second session changes the key; retrying the conflict keeps the old result.
	other, err := f.client.RegisterClient(f.ctx, &kvv1.RegisterClientRequest{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.client.Put(f.ctx, &kvv1.PutRequest{Session: &kvv1.WriteSession{ClientId: other.ClientId, Sequence: 1}, Key: "key", Value: []byte("other")})
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.client.CompareAndSwap(f.ctx, conflict)
	if !proto.Equal(first, errorDetail(t, err, codes.Aborted, kvv1.ErrorCode_ERROR_CODE_CAS_CONFLICT)) {
		t.Fatal("cached conflict changed")
	}
	removed, err := f.client.Delete(f.ctx, &kvv1.DeleteRequest{Session: session(3), Key: "key"})
	if err != nil || removed.GetRevision() == 0 {
		t.Fatalf("delete: %v %v", removed, err)
	}
	removed, err = f.client.Delete(f.ctx, &kvv1.DeleteRequest{Session: session(4), Key: "key"})
	if err != nil || removed.GetRevision() != 0 {
		t.Fatalf("missing delete: %v %v", removed, err)
	}
	got, err = f.client.Get(f.ctx, &kvv1.GetRequest{Key: "key"})
	if err != nil || got.GetFound() || got.GetRevision() != 0 {
		t.Fatalf("missing get: %v %v", got, err)
	}
	created, err := f.client.CompareAndSwap(f.ctx, &kvv1.CompareAndSwapRequest{Session: session(5), Key: "key", Value: []byte("created")})
	if err != nil || created.GetRevision() <= written.Revision {
		t.Fatalf("CAS create: %v %v", created, err)
	}
	st, err := f.client.Status(f.ctx, &kvv1.StatusRequest{})
	if err != nil || st.GetRole() != kvv1.NodeRole_NODE_ROLE_LEADER || st.GetAppliedIndex() != st.GetCommitIndex() {
		t.Fatalf("status: %v %v", st, err)
	}
}

func TestRPCRejectionsAndTypedErrors(t *testing.T) {
	f := newFixture(t, 1, 2, 3)
	// Learn a leader through the same peer RPC listener used by clients.
	if _, err := f.peer.Deliver(f.ctx, &raftv1.DeliverRequest{Message: &raftv1.Message{Type: raftv1.MessageType_MESSAGE_TYPE_APPEND_ENTRIES, From: 2, To: 1, Term: 1}}); err != nil {
		t.Fatal(err)
	}
	_, err := f.client.Get(f.ctx, &kvv1.GetRequest{})
	hint := errorDetail(t, err, codes.FailedPrecondition, kvv1.ErrorCode_ERROR_CODE_NOT_LEADER)
	if hint.LeaderId != 2 || hint.LeaderAddress != "127.0.0.1:9002" {
		t.Fatalf("hint: %v", hint)
	}
	if _, err := f.client.Status(f.ctx, &kvv1.StatusRequest{}); err != nil {
		t.Fatal(err)
	}
	single := newFixture(t, 1)
	single.elect(t)
	before := single.local(t).Raft.LastLogIndex
	_, err = single.client.Put(single.ctx, &kvv1.PutRequest{})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("malformed: %v", err)
	}
	_, err = single.client.Put(single.ctx, &kvv1.PutRequest{Session: &kvv1.WriteSession{ClientId: 2, Sequence: 1}, Value: make([]byte, raftcore.MaxCommandBytes)})
	if status.Code(err) != codes.ResourceExhausted || len(status.Convert(err).Details()) != 0 {
		t.Fatalf("oversized: %v", err)
	}
	if single.local(t).Raft.LastLogIndex != before {
		t.Fatal("malformed/oversized command appended")
	}
	_, err = single.client.Delete(single.ctx, &kvv1.DeleteRequest{Session: &kvv1.WriteSession{ClientId: 999, Sequence: 1}})
	errorDetail(t, err, codes.FailedPrecondition, kvv1.ErrorCode_ERROR_CODE_INVALID_SESSION)
	registered, err := single.client.RegisterClient(single.ctx, &kvv1.RegisterClientRequest{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = single.client.Delete(single.ctx, &kvv1.DeleteRequest{Session: &kvv1.WriteSession{ClientId: registered.ClientId, Sequence: 2}})
	if d := errorDetail(t, err, codes.FailedPrecondition, kvv1.ErrorCode_ERROR_CODE_INVALID_SEQUENCE); d.ExpectedSequence != 1 {
		t.Fatalf("sequence: %v", d)
	}
}

// A majority acknowledgement commits data first. Then all peer traffic stops:
// the old leader must withhold a read of even the already committed value.
func TestLostQuorumReadAndShutdown(t *testing.T) {
	f := newFixture(t, 1, 2, 3)
	f.elect(t)
	acknowledge := func(index uint64) {
		t.Helper()
		for {
			select {
			case msg := <-f.out:
				if msg.To == 2 && len(msg.Entries) > 0 && msg.Entries[len(msg.Entries)-1].Index == index {
					err := f.n.Step(f.ctx, &raftv1.Message{Type: raftv1.MessageType_MESSAGE_TYPE_APPEND_ENTRIES_RESPONSE, From: 2, To: 1, Term: msg.Term, PrevLogIndex: msg.PrevLogIndex, MatchIndex: index})
					if err != nil {
						t.Fatal(err)
					}
					return
				}
			case <-f.ctx.Done():
				t.Fatal("missing replication batch")
			}
		}
	}
	type registration struct {
		result *kvv1.RegisterClientResponse
		err    error
	}
	regDone := make(chan registration, 1)
	go func() {
		r, e := f.client.RegisterClient(f.ctx, &kvv1.RegisterClientRequest{})
		regDone <- registration{r, e}
	}()
	acknowledge(2)
	reg := <-regDone
	if reg.err != nil {
		t.Fatal(reg.err)
	}
	putDone := make(chan error, 1)
	go func() {
		_, err := f.client.Put(f.ctx, &kvv1.PutRequest{Session: &kvv1.WriteSession{ClientId: reg.result.ClientId, Sequence: 1}, Key: "key", Value: []byte("committed")})
		putDone <- err
	}()
	acknowledge(3)
	if err := <-putDone; err != nil {
		t.Fatal(err)
	}
	readDone := make(chan error, 1)
	go func() { _, err := f.client.Get(f.ctx, &kvv1.GetRequest{Key: "key"}); readDone <- err }()
	for f.local(t).Pending == 0 {
		select {
		case err := <-readDone:
			t.Fatalf("read escaped without quorum: %v", err)
		case <-time.After(time.Millisecond):
		}
	}
	// A queued Status request proves the read passed admission without blocking the owner.
	st, err := f.client.Status(f.ctx, &kvv1.StatusRequest{})
	if err != nil || st.GetCommitIndex() != 3 || st.GetLastLogIndex() != 4 {
		t.Fatalf("status: %v %v", st, err)
	}
	select {
	case err := <-readDone:
		t.Fatalf("read completed without quorum: %v", err)
	default:
	}
	if err := f.n.Close(); err != nil {
		t.Fatal(err)
	}
	errorDetail(t, <-readDone, codes.Aborted, kvv1.ErrorCode_ERROR_CODE_UNKNOWN)
	_, err = f.client.Status(f.ctx, &kvv1.StatusRequest{})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("stopped: %v", err)
	}
}

func TestRequestErrorPrecedence(t *testing.T) {
	s := &kvService{}
	errorDetail(t, s.requestError(raftcore.ErrBusy), codes.ResourceExhausted, kvv1.ErrorCode_ERROR_CODE_BUSY)
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded, node.ErrStopped} {
		errorDetail(t, s.requestError(errors.Join(node.ErrUnknown, cause)), codes.Aborted, kvv1.ErrorCode_ERROR_CODE_UNKNOWN)
	}
	if status.Code(s.requestError(context.DeadlineExceeded)) != codes.DeadlineExceeded {
		t.Fatal("pre-admission deadline lost")
	}
}
