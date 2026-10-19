package node

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/zishan044/raft-replicated-key-value-store.git/internal/kv"
	"github.com/zishan044/raft-replicated-key-value-store.git/internal/raftcore"
	"github.com/zishan044/raft-replicated-key-value-store.git/internal/raftio"
	kvv1 "github.com/zishan044/raft-replicated-key-value-store.git/pkg/api/kv/v1"
	raftv1 "github.com/zishan044/raft-replicated-key-value-store.git/pkg/api/raft/v1"
	"google.golang.org/protobuf/proto"
)

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func config(t *testing.T, voters ...uint64) (Config, <-chan *raftv1.Message) {
	t.Helper()
	out := make(chan *raftv1.Message, 256)
	return Config{
		DataDir: filepath.Join(t.TempDir(), "nested", "node"),
		Raft:    raftcore.Config{ID: 1, Voters: voters, ElectionTicks: 10, HeartbeatTicks: 2, Seed: 42},
		Send: func(messages []*raftv1.Message) {
			for _, msg := range messages {
				select {
				case out <- msg:
				default:
				}
			}
		},
	}, out
}

func openNode(t *testing.T, cfg Config) *Node {
	t.Helper()
	n, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Close() })
	return n
}

func nodeStatus(t *testing.T, n *Node) Status {
	t.Helper()
	s, err := n.Status(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func elect(t *testing.T, n *Node) {
	t.Helper()
	ctx := testContext(t)
	s := nodeStatus(t, n).Raft
	for range s.ElectionTimeout - s.ElectionElapsed {
		if err := n.Tick(ctx); err != nil {
			t.Fatal(err)
		}
	}
	s = nodeStatus(t, n).Raft
	if s.Role != raftcore.Leader {
		if err := n.Step(ctx, &raftv1.Message{
			Type: raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE_RESPONSE, From: 2, To: 1, Term: s.HardState.Term,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if nodeStatus(t, n).Raft.Role != raftcore.Leader {
		t.Fatal("failed to elect test node")
	}
}

func registration() *kvv1.Command {
	return &kvv1.Command{Operation: &kvv1.Command_RegisterClient{RegisterClient: &kvv1.RegisterClientRequest{}}}
}

func put(client, sequence uint64, data string) *kvv1.Command {
	return &kvv1.Command{Operation: &kvv1.Command_Put{Put: &kvv1.PutRequest{
		Session: &kvv1.WriteSession{ClientId: client, Sequence: sequence}, Key: "key", Value: []byte(data),
	}}}
}

func get() *kvv1.Command {
	return &kvv1.Command{Operation: &kvv1.Command_Get{Get: &kvv1.GetRequest{Key: "key"}}}
}

func begin(n *Node, ctx context.Context, cmd *kvv1.Command) <-chan response {
	done := make(chan response, 1)
	go func() {
		result, err := n.Submit(ctx, cmd)
		done <- response{result: result, err: err}
	}()
	return done
}

func receive(t *testing.T, done <-chan response) response {
	t.Helper()
	select {
	case r := <-done:
		return r
	case <-testContext(t).Done():
		t.Fatal("request did not complete")
		return response{}
	}
}

func appendFor(t *testing.T, out <-chan *raftv1.Message, index uint64) *raftv1.Message {
	t.Helper()
	ctx := testContext(t)
	for {
		select {
		case msg := <-out:
			if msg.To == 2 && msg.Type == raftv1.MessageType_MESSAGE_TYPE_APPEND_ENTRIES && len(msg.Entries) > 0 && msg.Entries[len(msg.Entries)-1].Index >= index {
				return msg
			}
		case <-ctx.Done():
			t.Fatalf("no append for index %d", index)
		}
	}
}

func acknowledge(t *testing.T, n *Node, msg *raftv1.Message) {
	t.Helper()
	if err := n.Step(testContext(t), &raftv1.Message{
		Type: raftv1.MessageType_MESSAGE_TYPE_APPEND_ENTRIES_RESPONSE, From: 2, To: 1, Term: msg.Term,
		PrevLogIndex: msg.PrevLogIndex, MatchIndex: msg.Entries[len(msg.Entries)-1].Index,
	}); err != nil {
		t.Fatal(err)
	}
}

func commit(t *testing.T, n *Node, out <-chan *raftv1.Message, cmd *kvv1.Command) *kvv1.CommandResult {
	t.Helper()
	index := nodeStatus(t, n).Raft.LastLogIndex + 1
	done := begin(n, testContext(t), cmd)
	acknowledge(t, n, appendFor(t, out, index))
	r := receive(t, done)
	if r.err != nil {
		t.Fatal(r.err)
	}
	return r.result
}

func TestSingleNodeImmediateCommitAndRestart(t *testing.T) {
	cfg, _ := config(t, 1)
	n := openNode(t, cfg)
	elect(t, n)
	ctx := testContext(t)
	registered, err := n.Submit(ctx, registration())
	if err != nil || registered.GetClientId() != 2 {
		t.Fatalf("immediate registration failed: %v %v", registered, err)
	}
	write := put(2, 1, "persisted")
	result, err := n.Submit(ctx, write)
	if err != nil || result.GetRevision() != 3 {
		t.Fatalf("immediate write failed: %v %v", result, err)
	}
	if s := nodeStatus(t, n); s.Pending != 0 || s.Raft.AppliedIndex != 3 {
		t.Fatalf("reply preceded application/Advance or leaked a waiter: %+v", s)
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
	n = openNode(t, cfg)
	if s := nodeStatus(t, n); s.Raft.AppliedIndex != 3 || s.Raft.Role != raftcore.Follower {
		t.Fatalf("startup did not finish replay before returning: %+v", s)
	}
	elect(t, n)
	result, err = n.Submit(ctx, write)
	if err != nil || result.GetRevision() != 3 {
		t.Fatalf("recovered session did not deduplicate: %v %v", result, err)
	}
	result, err = n.Submit(ctx, get())
	if err != nil || !result.GetFound() || string(result.Value) != "persisted" || result.Revision != 3 {
		t.Fatalf("recovered value differs: %v %v", result, err)
	}
}

func TestCancellationRemovesWaiterButPreservesCommand(t *testing.T) {
	cfg, out := config(t, 1, 2, 3)
	n := openNode(t, cfg)
	elect(t, n)
	client := commit(t, n, out, registration()).ClientId
	ctx, cancel := context.WithCancel(testContext(t))
	cmd := put(client, 1, "retained")
	done := begin(n, ctx, cmd)
	msg := appendFor(t, out, 3)
	cancel()
	r := receive(t, done)
	if !errors.Is(r.err, ErrUnknown) || !errors.Is(r.err, context.Canceled) {
		t.Fatalf("cancellation lost uncertainty: %v", r.err)
	}
	// Cancellation cleanup is an owner event, independent of subsequent ticks.
	deadline := testContext(t)
	for {
		s, err := n.Status(deadline)
		if err != nil {
			t.Fatal(err)
		}
		if s.Pending == 0 {
			break
		}
	}
	cmd.GetPut().Value[0] = 'X' // the runtime already owns serialized command data
	acknowledge(t, n, msg)
	result := commit(t, n, out, put(client, 1, "retained"))
	if result.Revision != 3 {
		t.Fatalf("canceled write was reapplied: %v", result)
	}
	result = commit(t, n, out, get())
	if !result.Found || string(result.Value) != "retained" || result.Revision != 3 {
		t.Fatalf("cancellation removed or changed the command: %v", result)
	}
}

func TestLeadershipLossAndReplacementResolveAllWaiters(t *testing.T) {
	cfg, out := config(t, 1, 2, 3)
	n := openNode(t, cfg)
	elect(t, n)
	first := begin(n, testContext(t), registration())
	appendFor(t, out, 2)
	second := begin(n, testContext(t), registration())
	appendFor(t, out, 3)
	data, err := proto.Marshal(get())
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Step(testContext(t), &raftv1.Message{
		Type: raftv1.MessageType_MESSAGE_TYPE_APPEND_ENTRIES, From: 2, To: 1, Term: 2,
		PrevLogIndex: 1, PrevLogTerm: 1, LeaderCommit: 2,
		Entries: []*raftv1.Entry{{Index: 2, Term: 2, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL, Data: data}},
	}); err != nil {
		t.Fatal(err)
	}
	for _, done := range []<-chan response{first, second} {
		if r := receive(t, done); !errors.Is(r.err, ErrUnknown) || r.result != nil {
			t.Fatalf("replaced/deposed waiter received a result: %+v", r)
		}
	}
	if s := nodeStatus(t, n); s.Pending != 0 || s.Raft.AppliedIndex != 2 {
		t.Fatalf("step-down leaked waiters or omitted replacement application: %+v", s)
	}
	_, err = n.Submit(testContext(t), registration())
	var redirect *NotLeaderError
	if !errors.As(err, &redirect) || redirect.LeaderID != 2 || !errors.Is(err, raftcore.ErrNotLeader) {
		t.Fatalf("missing leader hint: %v", err)
	}
}

func TestOneBatchCompletesMultipleWaitersAfterAdvance(t *testing.T) {
	cfg, out := config(t, 1, 2, 3)
	n := openNode(t, cfg)
	elect(t, n)
	first := begin(n, testContext(t), registration())
	appendFor(t, out, 2)
	second := begin(n, testContext(t), registration())
	acknowledge(t, n, appendFor(t, out, 3))
	for i, done := range []<-chan response{first, second} {
		r := receive(t, done)
		if r.err != nil || r.result.GetClientId() != uint64(i+2) {
			t.Fatalf("batch completed the wrong waiter: %+v", r)
		}
		if s := nodeStatus(t, n); s.Pending != 0 || s.Raft.AppliedIndex != 3 {
			t.Fatalf("success preceded Advance: %+v", s)
		}
	}
}

func TestCloseResolvesPendingAndRestartDoesNotReplayUncommitted(t *testing.T) {
	cfg, out := config(t, 1, 2, 3)
	n := openNode(t, cfg)
	elect(t, n)
	done := begin(n, testContext(t), registration())
	appendFor(t, out, 2)
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
	if r := receive(t, done); !errors.Is(r.err, ErrUnknown) || !errors.Is(r.err, ErrStopped) {
		t.Fatalf("shutdown outcome = %v", r.err)
	}
	if err := n.Close(); err != nil || n.Err() != nil {
		t.Fatalf("repeated normal shutdown failed: %v", err)
	}
	if _, err := n.Status(testContext(t)); !errors.Is(err, ErrStopped) {
		t.Fatal("stopped node accepted input")
	}
	n = openNode(t, cfg)
	if s := nodeStatus(t, n); s.Raft.AppliedIndex != 0 || s.Raft.LastLogIndex != 2 {
		t.Fatalf("restart applied an uncommitted suffix: %+v", s)
	}
}

func TestInvalidInputsAndCanceledAdmissionDoNotStopNode(t *testing.T) {
	cfg, _ := config(t, 1)
	n := openNode(t, cfg)
	ctx := testContext(t)
	before := nodeStatus(t, n)
	if err := n.Step(ctx, nil); err == nil {
		t.Fatal("accepted a nil message")
	}
	if err := n.Step(ctx, &raftv1.Message{From: 99, To: 1, Term: 99}); err == nil {
		t.Fatal("accepted an invalid peer")
	}
	if _, err := n.Submit(ctx, &kvv1.Command{}); err == nil {
		t.Fatal("accepted an empty command")
	}
	if _, err := n.Submit(ctx, put(1, 1, strings.Repeat("x", raftcore.MaxCommandBytes))); !errors.Is(err, raftcore.ErrCommandTooLarge) {
		t.Fatalf("oversized encoded command returned %v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := n.Submit(canceled, registration()); !errors.Is(err, context.Canceled) || errors.Is(err, ErrUnknown) {
		t.Fatalf("pre-admission cancellation was not definite: %v", err)
	}
	if nodeStatus(t, n) != before {
		t.Fatal("invalid request changed status")
	}
	elect(t, n)
	if result, err := n.Submit(ctx, registration()); err != nil || result.GetClientId() != 2 {
		t.Fatalf("valid request failed after rejection: %v %v", result, err)
	}
}

// Use the same callbacks as production with one wrapped WAL save, not a second
// storage implementation or application path.
func openWithSave(t *testing.T, cfg Config, wrap func(func(*raftv1.WALBatch) error) func(*raftv1.WALBatch) error) *Node {
	t.Helper()
	s, state, err := openStorage(cfg)
	if err != nil {
		t.Fatal(err)
	}
	machine := new(kv.Machine)
	core, err := raftio.Restore(cfg.Raft, state, func(e *raftv1.Entry) error { _, err := machine.Apply(e); return err })
	if err != nil {
		_ = s.close()
		t.Fatal(err)
	}
	n, err := start(core, machine, wrap(s.wal.Save), cfg.Send, s.close)
	if err != nil {
		_ = s.close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Close() })
	return n
}

func TestUncertainSaveWithholdsSuccessAndRecovers(t *testing.T) {
	cfg, out := config(t, 1, 2, 3)
	failure := errors.New("injected sync failure")
	n := openWithSave(t, cfg, func(save func(*raftv1.WALBatch) error) func(*raftv1.WALBatch) error {
		return func(batch *raftv1.WALBatch) error {
			if err := save(batch); err != nil {
				return err
			}
			if batch.HardState != nil && batch.HardState.Commit == 2 {
				return failure // complete uncertain batch survives; no dependent output
			}
			return nil
		}
	})
	elect(t, n)
	done := begin(n, testContext(t), registration())
	msg := appendFor(t, out, 2)
	err := n.Step(testContext(t), &raftv1.Message{
		Type: raftv1.MessageType_MESSAGE_TYPE_APPEND_ENTRIES_RESPONSE, From: 2, To: 1, Term: msg.Term,
		PrevLogIndex: msg.PrevLogIndex, MatchIndex: 2,
	})
	if !errors.Is(err, failure) || !errors.Is(err, ErrStopped) {
		t.Fatalf("save failure = %v", err)
	}
	if r := receive(t, done); !errors.Is(r.err, ErrUnknown) || !errors.Is(r.err, failure) || r.result != nil {
		t.Fatalf("uncertain save returned success: %+v", r)
	}
	if err := n.Close(); !errors.Is(err, failure) || !errors.Is(n.Err(), failure) {
		t.Fatalf("storage failure was not terminal: %v", err)
	}
	for len(out) > 0 {
		if msg := <-out; msg.LeaderCommit == 2 {
			t.Fatal("dependent message escaped a failed save")
		}
	}
	n = openNode(t, cfg)
	if nodeStatus(t, n).Raft.AppliedIndex != 2 {
		t.Fatal("complete uncertain committed batch did not replay")
	}
	elect(t, n)
	if result := commit(t, n, out, put(2, 1, "recovered session")); result.Error != nil {
		t.Fatalf("recovered registration is missing: %v", result)
	}
}

func TestCanceledRequestReturnsWhileSaveIsBlocked(t *testing.T) {
	cfg, out := config(t, 1, 2, 3)
	entered := make(chan struct{})
	release, unblock := context.WithCancel(context.Background())
	defer unblock()
	n := openWithSave(t, cfg, func(save func(*raftv1.WALBatch) error) func(*raftv1.WALBatch) error {
		return func(batch *raftv1.WALBatch) error {
			if batch.HardState != nil && batch.HardState.Commit == 2 {
				close(entered)
				<-release.Done()
			}
			return save(batch)
		}
	})
	elect(t, n)
	ctx, cancel := context.WithCancel(testContext(t))
	defer cancel()
	done := begin(n, ctx, registration())
	msg := appendFor(t, out, 2)
	stepDone := make(chan error, 1)
	go func() {
		stepDone <- n.Step(testContext(t), &raftv1.Message{
			Type: raftv1.MessageType_MESSAGE_TYPE_APPEND_ENTRIES_RESPONSE, From: 2, To: 1, Term: msg.Term,
			PrevLogIndex: msg.PrevLogIndex, MatchIndex: 2,
		})
	}()
	select {
	case <-entered:
	case <-testContext(t).Done():
		t.Fatal("commit did not reach the save boundary")
	}
	cancel()
	if r := receive(t, done); !errors.Is(r.err, ErrUnknown) || !errors.Is(r.err, context.Canceled) || r.result != nil {
		t.Fatalf("canceled request waited for I/O or returned success: %+v", r)
	}
	unblock()
	if err := <-stepDone; err != nil {
		t.Fatal(err)
	}
	if s := nodeStatus(t, n); s.Pending != 0 || s.Raft.AppliedIndex != 2 {
		t.Fatalf("completed save leaked the canceled waiter: %+v", s)
	}
}

func TestPartialApplicationFailureWithholdsEarlierResult(t *testing.T) {
	cfg, out := config(t, 1, 2, 3)
	n := openNode(t, cfg)
	elect(t, n)
	done := begin(n, testContext(t), registration())
	appendFor(t, out, 2)
	// Applying index 2 would complete registration, but index 3 is malformed.
	// The entire Ready must finish before any result is released.
	err := n.Step(testContext(t), &raftv1.Message{
		Type: raftv1.MessageType_MESSAGE_TYPE_APPEND_ENTRIES, From: 2, To: 1, Term: 2,
		PrevLogIndex: 2, PrevLogTerm: 1, LeaderCommit: 3,
		Entries: []*raftv1.Entry{{Index: 3, Term: 2, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL, Data: []byte{0xff}}},
	})
	if err == nil || !strings.Contains(err.Error(), "decode command") {
		t.Fatalf("application failure = %v", err)
	}
	if r := receive(t, done); !errors.Is(r.err, ErrUnknown) || r.result != nil {
		t.Fatalf("partial application leaked an earlier result: %+v", r)
	}
	if n.Close() == nil {
		t.Fatal("application failure did not stop the node")
	}
	if reopened, err := Open(cfg); reopened != nil || err == nil || !strings.Contains(err.Error(), "decode command") {
		t.Fatalf("startup ignored committed malformed data: node=%v err=%v", reopened, err)
	}
	// Even failed replay releases the lock for investigation/recovery tooling.
	s, _, err := openStorage(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
}
