package raftcore

import (
	"errors"
	"testing"

	raftv1 "github.com/zishan044/raft-replicated-key-value-store.git/pkg/api/raft/v1"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func checkUncommittedBytes(t *testing.T, core *Core) {
	t.Helper()
	var want uint64
	for _, entry := range core.log[core.hardState.Commit:] {
		want += uint64(proto.Size(entry))
	}
	if core.uncommittedBytes != want {
		t.Fatalf("uncommitted bytes = %d, want %d", core.uncommittedBytes, want)
	}
}

func checkAppendLimit(t *testing.T, msg *raftv1.Message) int {
	t.Helper()
	data, err := proto.Marshal(&raftv1.DeliverRequest{Message: msg})
	if err != nil {
		t.Fatal(err)
	}
	if msg.Type != raftv1.MessageType_MESSAGE_TYPE_APPEND_ENTRIES || len(msg.Entries) > MaxAppendEntries || len(data) > MaxPeerMessageBytes {
		t.Fatalf("unbounded append: type=%v entries=%d encoded bytes=%d", msg.Type, len(msg.Entries), len(data))
	}
	return len(data)
}

func TestBoundedAppendCatchesUpConflictingFollower(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		count, payload, first int
	}{
		{"entry count", 2*MaxAppendEntries + 23, 16, MaxAppendEntries},
		{"encoded bytes", 7, MaxCommandBytes, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config()
			cfg.HardState.Term = 3
			for i := range tc.count {
				cfg.Entries = append(cfg.Entries, &raftv1.Entry{
					Index: uint64(i + 1), Term: 3, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL, Data: make([]byte, tc.payload),
				})
			}
			leader, rd := electLeader(t, cfg)
			last := leader.Status().LastLogIndex
			cfg.ID = 2
			cfg.Entries = entriesWithTerm(tc.count+5, 2)
			follower := newElectionCore(t, cfg)
			lostResponse, sawBatch, converged := false, false, false
			for range 30 {
				for _, msg := range rd.Messages {
					checkAppendLimit(t, msg)
				}
				msg := paperMessageTo(t, rd.Messages, 2)
				reply := stepElection(t, follower, msg)
				checkUncommittedBytes(t, follower)
				ack := reply.Messages[0]
				if !ack.Reject && len(msg.Entries) > 0 && !sawBatch {
					sawBatch = true
					if msg.PrevLogIndex != 0 || len(msg.Entries) != tc.first {
						t.Fatalf("wrong first repaired batch: previous=%d entries=%d", msg.PrevLogIndex, len(msg.Entries))
					}
				}
				if !ack.Reject && len(msg.Entries) > 0 && !lostResponse {
					lostResponse = true
					for range cfg.HeartbeatTicks {
						if err := leader.Tick(); err != nil {
							t.Fatal(err)
						}
					}
					rd = takeReady(t, leader)
					if !proto.Equal(msg, paperMessageTo(t, rd.Messages, 2)) {
						t.Fatal("lost response did not retry the same bounded batch")
					}
					continue
				}
				rd = stepElection(t, leader, ack)
				checkUncommittedBytes(t, leader)
				if ack.MatchIndex < last && leader.Status().HardState.Commit != 0 {
					t.Fatal("bounded replication committed old-term entries without the current-term no-op")
				}
				if follower.Status().AppliedIndex == last {
					converged = true
					break
				}
			}
			if !converged || !lostResponse || !equalEntries(leader.log, follower.log) || leader.Status().AppliedIndex != last {
				t.Fatal("bounded replication failed to converge and apply the complete log")
			}
		})
	}
}

func TestAppendIncludesWrapperAtExactByteLimit(t *testing.T) {
	entries := []*raftv1.Entry{
		{Index: 1, Term: 1, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL, Data: make([]byte, MaxCommandBytes)},
		{Index: 2, Term: 1, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL, Data: make([]byte, MaxCommandBytes)},
		{Index: 3, Term: 1, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL, Data: []byte("next batch")},
	}
	msg := &raftv1.Message{Type: raftv1.MessageType_MESSAGE_TYPE_APPEND_ENTRIES, From: 1, To: 2, Term: 2, Entries: entries[:2]}
	encoded, err := proto.Marshal(&raftv1.DeliverRequest{Message: msg})
	if err != nil {
		t.Fatal(err)
	}
	excess := len(encoded) - MaxPeerMessageBytes
	entries[1].Data = entries[1].Data[:MaxCommandBytes-excess]
	if proto.Size(&raftv1.DeliverRequest{Message: msg}) != MaxPeerMessageBytes {
		t.Fatal("fixture does not reach the exact encoded limit")
	}
	cfg := config()
	cfg.HardState.Term, cfg.Entries = 1, entries
	leader, _ := electLeader(t, cfg)
	reject := appendResponse(2, 2, 3, 0)
	reject.Reject, reject.ConflictIndex = true, 1
	rd := stepElection(t, leader, reject)
	batch := rd.Messages[0]
	if len(batch.Entries) != 2 || checkAppendLimit(t, batch) != MaxPeerMessageBytes {
		t.Fatal("batch did not pack exactly to the byte limit")
	}
	// Growing the second command by one byte must move it to the next batch.
	entries[1].Data = append(entries[1].Data, 0)
	cfg.Entries = entries
	leader, _ = electLeader(t, cfg)
	rd = stepElection(t, leader, reject)
	if len(rd.Messages[0].Entries) != 1 {
		t.Fatal("batch exceeded its byte limit by accepting the next entry")
	}
	checkAppendLimit(t, rd.Messages[0])
}

func TestCommandLimitAndRejectedProposalDoNotMutate(t *testing.T) {
	core, _ := electLeader(t, config())
	before := core.Status()
	if index, term, err := core.Propose(make([]byte, MaxCommandBytes+1)); index != 0 || term != 0 || !errors.Is(err, ErrCommandTooLarge) {
		t.Fatalf("oversized proposal result: index=%d term=%d err=%v", index, term, err)
	}
	if core.Status() != before || core.uncommittedBytes != uint64(proto.Size(core.log[0])) {
		t.Fatal("oversized proposal changed the core")
	}
	if rd := core.Ready(); rd.HardState != nil || len(rd.Entries) != 0 || len(rd.Messages) != 0 {
		t.Fatal("oversized proposal emitted output")
	}
	if _, _, err := core.Propose(make([]byte, MaxCommandBytes)); err != nil {
		t.Fatalf("exact command limit rejected: %v", err)
	}
	rd := takeReady(t, core)
	for _, msg := range rd.Messages {
		checkAppendLimit(t, msg)
	}
	checkUncommittedBytes(t, core)
}

func TestUncommittedLimitAllowsElectionAndReleasesCapacity(t *testing.T) {
	cfg := config()
	cfg.HardState.Term = 1
	// Seed a real nearly-full log with shared fixture data; New owns its copy.
	data := make([]byte, MaxCommandBytes-128)
	for i := range 128 {
		cfg.Entries = append(cfg.Entries, &raftv1.Entry{
			Index: uint64(i + 1), Term: 1, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL, Data: data,
		})
	}
	core, _ := electLeader(t, cfg)
	checkUncommittedBytes(t, core)
	remaining := MaxUncommittedBytes - int(core.uncommittedBytes)
	var fill []byte
	for n := remaining; n > remaining-16; n-- {
		candidate := &raftv1.Entry{Index: core.lastLogIndex() + 1, Term: 2, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL, Data: make([]byte, n)}
		if proto.Size(candidate) == remaining {
			fill = candidate.Data
			break
		}
	}
	if fill == nil {
		t.Fatal("could not construct an exact-capacity proposal")
	}
	if _, _, err := core.Propose(fill); err != nil {
		t.Fatal(err)
	}
	takeReady(t, core)
	checkUncommittedBytes(t, core)
	if core.uncommittedBytes != MaxUncommittedBytes {
		t.Fatal("exact budget was not accepted")
	}
	before := core.Status()
	for _, data := range [][]byte{nil, []byte("busy")} {
		if index, term, err := core.Propose(data); index != 0 || term != 0 || !errors.Is(err, ErrBusy) || core.Status() != before {
			t.Fatalf("full log must reject without append: index=%d term=%d err=%v", index, term, err)
		}
	}
	if rd := core.Ready(); rd.HardState != nil || len(rd.Entries) != 0 || len(rd.Messages) != 0 {
		t.Fatal("busy proposal emitted output")
	}
	// Lose leadership, then win it again with the entire inherited log full.
	stepElection(t, core, &raftv1.Message{
		Type: raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE, From: 2, To: 1, Term: 3,
		LastLogIndex: before.LastLogIndex, LastLogTerm: before.LastLogTerm,
	})
	expireElection(t, core)
	stepElection(t, core, &raftv1.Message{Type: raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE_RESPONSE, From: 2, To: 1, Term: 4})
	checkUncommittedBytes(t, core)
	if core.Status().Role != Leader || core.uncommittedBytes <= MaxUncommittedBytes || core.lastLogIndex() != before.LastLogIndex+1 {
		t.Fatal("full inherited log prevented the mandatory election no-op")
	}
	if _, _, err := core.Propose(nil); !errors.Is(err, ErrBusy) {
		t.Fatal("new leader accepted client work above capacity")
	}
	last := core.lastLogIndex()
	stepElection(t, core, appendResponse(2, 4, last-1, last))
	checkUncommittedBytes(t, core)
	if core.uncommittedBytes != 0 || core.hardState.Commit != last {
		t.Fatal("commit failed to release inherited capacity")
	}
	if _, _, err := core.Propose(make([]byte, MaxCommandBytes)); err != nil {
		t.Fatalf("capacity remained unavailable after commitment: %v", err)
	}
	takeReady(t, core)
	checkUncommittedBytes(t, core)
}

func TestUncommittedBytesFollowRecoveryReplacementAndPartialCommit(t *testing.T) {
	cfg := config()
	cfg.HardState = HardState{Term: 3, Commit: 1}
	cfg.Entries = logWithTerms(1, 2, 2, 3)
	core := newElectionCore(t, cfg)
	checkUncommittedBytes(t, core)
	// Match a prefix, replace the uncommitted tail, and partially commit it.
	msg := appendRequest(4, 2, 2,
		&raftv1.Entry{Index: 3, Term: 4, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL, Data: []byte("new and longer")},
		&raftv1.Entry{Index: 4, Term: 4, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL, Data: []byte("pending")},
	)
	msg.LeaderCommit = 3
	stepElection(t, core, msg)
	checkUncommittedBytes(t, core)
	if core.uncommittedBytes != uint64(proto.Size(core.log[3])) {
		t.Fatal("partial commit did not retain exactly the pending entry's bytes")
	}
	stepElection(t, core, msg) // duplicate append must not count the suffix twice
	checkUncommittedBytes(t, core)
	msg = appendRequest(4, 4, 4)
	msg.LeaderCommit = 4
	stepElection(t, core, msg)
	checkUncommittedBytes(t, core)
	if core.uncommittedBytes != 0 {
		t.Fatal("follower commitment did not release all capacity")
	}
	// Single-node commitment must release bytes synchronously on each proposal.
	cfg = config()
	cfg.Voters = []uint64{1}
	core, _ = electLeader(t, cfg)
	if _, _, err := core.Propose([]byte("immediate")); err != nil {
		t.Fatal(err)
	}
	checkUncommittedBytes(t, core)
	if core.uncommittedBytes != 0 {
		t.Fatal("single-node commit retained proposal bytes")
	}
}

func TestOversizedAppendAndRecoveryRejectBeforeMutation(t *testing.T) {
	for _, tc := range []struct {
		name string
		msg  *raftv1.Message
	}{
		{"command", appendRequest(99, 0, 0, &raftv1.Entry{Index: 1, Term: 99, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL, Data: make([]byte, MaxCommandBytes+1)})},
		{"count", appendRequest(99, 0, 0, entriesWithTerm(MaxAppendEntries+1, 99)...)},
		{"encoded bytes", appendRequest(99, 0, 0,
			&raftv1.Entry{Index: 1, Term: 99, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL, Data: make([]byte, MaxCommandBytes)},
			&raftv1.Entry{Index: 2, Term: 99, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL, Data: make([]byte, MaxCommandBytes)},
		)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			core := newElectionCore(t, config())
			before := core.Status()
			if err := core.Step(tc.msg); err == nil || core.Status() != before || len(core.log) != 0 {
				t.Fatal("oversized append changed the log or term")
			}
			checkUncommittedBytes(t, core)
			if rd := core.Ready(); rd.HardState != nil || len(rd.Messages) != 0 || len(rd.Entries) != 0 {
				t.Fatal("oversized append emitted output")
			}
		})
	}
	for _, unknown := range []bool{false, true} {
		cfg := config()
		cfg.HardState.Term = 1
		entry := &raftv1.Entry{Index: 1, Term: 1, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL}
		if unknown {
			// Unknown wire fields also occupy transport space. Recovery must not
			// accept an entry that no future bounded append could transmit.
			padding := protowire.AppendTag(nil, 100, protowire.BytesType)
			padding = protowire.AppendBytes(padding, make([]byte, MaxPeerMessageBytes))
			entry.ProtoReflect().SetUnknown(padding)
		} else {
			entry.Data = make([]byte, MaxCommandBytes+1)
		}
		cfg.Entries = []*raftv1.Entry{entry}
		if core, err := New(cfg); core != nil || !errors.Is(err, ErrCommandTooLarge) {
			t.Fatalf("startup accepted an unreplicable entry: %v", err)
		}
	}
}
