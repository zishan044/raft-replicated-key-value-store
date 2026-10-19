package transport

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/zishan044/raft-replicated-key-value-store.git/internal/node"
	"github.com/zishan044/raft-replicated-key-value-store.git/internal/raftcore"
	raftv1 "github.com/zishan044/raft-replicated-key-value-store.git/pkg/api/raft/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func rawClient(t *testing.T, address string) raftv1.PeerServiceClient {
	t.Helper()
	conn, err := grpc.NewClient("passthrough:///"+address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(2*raftcore.MaxPeerMessageBytes)),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return raftv1.NewPeerServiceClient(conn)
}

func TestDeliveryStatusAndServerDeadline(t *testing.T) {
	deadlines := make(chan time.Duration, 8)
	address, _ := serve(t, "", func(ctx context.Context, msg *raftv1.Message) error {
		deadline, ok := ctx.Deadline()
		if !ok {
			return errors.New("missing server deadline")
		}
		deadlines <- time.Until(deadline)
		switch msg.Term {
		case 2:
			return errors.New("invalid message")
		case 3:
			return errors.Join(node.ErrStopped, errors.New("disk failed"))
		case 4:
			return context.Canceled
		case 5:
			return context.DeadlineExceeded
		}
		return nil
	})
	client := rawClient(t, address)
	if _, err := client.Deliver(testContext(t), &raftv1.DeliverRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty request status = %v", err)
	}
	for i, want := range []codes.Code{codes.OK, codes.InvalidArgument, codes.Unavailable, codes.Canceled, codes.DeadlineExceeded} {
		_, err := client.Deliver(testContext(t), &raftv1.DeliverRequest{Message: message(2, uint64(i+1))})
		if status.Code(err) != want {
			t.Fatalf("term %d status = %v, want %v", i+1, err, want)
		}
		if remaining := <-deadlines; remaining <= 0 || remaining > rpcTimeout {
			t.Fatalf("server did not clamp a longer client deadline: %v", remaining)
		}
	}
}

func TestServerAcceptsExactWireLimitAndRejectsOneByteMore(t *testing.T) {
	received := make(chan *raftv1.Message, 2)
	address, _ := serve(t, "", func(_ context.Context, msg *raftv1.Message) error {
		received <- msg
		return nil
	})
	client := rawClient(t, address)
	msg := &raftv1.Message{
		Type: raftv1.MessageType_MESSAGE_TYPE_APPEND_ENTRIES, From: 1, To: 2, Term: 1,
		Entries: []*raftv1.Entry{
			{Index: 1, Term: 1, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL, Data: make([]byte, raftcore.MaxCommandBytes)},
			{Index: 2, Term: 1, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL, Data: make([]byte, raftcore.MaxCommandBytes)},
		},
	}
	req := &raftv1.DeliverRequest{Message: msg}
	excess := proto.Size(req) - raftcore.MaxPeerMessageBytes
	msg.Entries[1].Data = msg.Entries[1].Data[:len(msg.Entries[1].Data)-excess]
	if proto.Size(req) != raftcore.MaxPeerMessageBytes {
		t.Fatal("fixture does not reach the exact wire limit")
	}
	if _, err := client.Deliver(testContext(t), req); err != nil {
		t.Fatalf("exact-size request rejected: %v", err)
	}
	receive(t, received)
	tr := openTransport(t, 1, map[uint64]string{2: address})
	tr.Send([]*raftv1.Message{proto.Clone(msg).(*raftv1.Message)})
	if proto.Size(&raftv1.DeliverRequest{Message: receive(t, received)}) != raftcore.MaxPeerMessageBytes {
		t.Fatal("outbound transport did not deliver the exact-size request")
	}
	msg.Entries[1].Data = append(msg.Entries[1].Data, 0)
	if _, err := client.Deliver(testContext(t), req); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("oversized request status = %v", err)
	}
	// A valid marker must still work; the oversized request never reached Step.
	if _, err := client.Deliver(testContext(t), &raftv1.DeliverRequest{Message: message(2, 2)}); err != nil {
		t.Fatal(err)
	}
	if receive(t, received).Term != 2 || len(received) != 0 {
		t.Fatal("oversized request reached the callback")
	}
}

func TestDeliverUsesPersistentNodeOwnerAndDistinguishesTerminalFailure(t *testing.T) {
	responses := make(chan *raftv1.Message, 8)
	n, err := node.Open(node.Config{
		DataDir: filepath.Join(t.TempDir(), "node"),
		Raft:    raftcore.Config{ID: 1, Voters: []uint64{1, 2, 3}, ElectionTicks: 10, HeartbeatTicks: 2, Seed: 42},
		Send: func(messages []*raftv1.Message) {
			for _, msg := range messages {
				responses <- msg
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Close() })
	address, _ := serve(t, "", n.Step)
	client := rawClient(t, address)
	ctx := testContext(t)
	invalid := message(1, 99)
	invalid.From = 99
	if _, err := client.Deliver(ctx, &raftv1.DeliverRequest{Message: invalid}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid sender status = %v", err)
	}
	before, err := n.Status(ctx)
	if err != nil || before.Raft.HardState.Term != 0 {
		t.Fatalf("invalid input stopped or changed node: %+v %v", before, err)
	}
	vote := message(1, 1)
	vote.From = 2
	if _, err := client.Deliver(ctx, &raftv1.DeliverRequest{Message: vote}); err != nil {
		t.Fatal(err)
	}
	ack := receive(t, responses)
	if ack.Type != raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE_RESPONSE || ack.Reject || ack.To != 2 || ack.From != 1 {
		t.Fatalf("missing separate Raft acknowledgement: %v", ack)
	}
	current, err := n.Status(ctx)
	if err != nil || current.Raft.HardState.Term != 1 || current.Raft.HardState.Vote != 2 {
		t.Fatalf("delivery did not reach persistent owner: %+v %v", current, err)
	}
	badCommand := &raftv1.Message{
		Type: raftv1.MessageType_MESSAGE_TYPE_APPEND_ENTRIES, From: 2, To: 1, Term: 2, LeaderCommit: 1,
		Entries: []*raftv1.Entry{{Index: 1, Term: 2, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL, Data: []byte{0xff}}},
	}
	if _, err := client.Deliver(ctx, &raftv1.DeliverRequest{Message: badCommand}); status.Code(err) != codes.Unavailable {
		t.Fatalf("terminal apply failure reported as malformed peer input: %v", err)
	}
	if err := n.Close(); err == nil || !strings.Contains(err.Error(), "decode command") {
		t.Fatalf("application failure was not terminal: %v", err)
	}
	if _, err := client.Deliver(ctx, &raftv1.DeliverRequest{Message: vote}); status.Code(err) != codes.Unavailable {
		t.Fatalf("stopped owner status = %v", err)
	}
}
