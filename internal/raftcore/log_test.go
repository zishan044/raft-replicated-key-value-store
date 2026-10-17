package raftcore

import (
	"errors"
	"testing"

	raftv1 "github.com/zishan044/raft-replicated-key-value-store.git/pkg/api/raft/v1"
	"google.golang.org/protobuf/proto"
)

func TestInitialLogValidation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		entries []*raftv1.Entry
		commit  uint64
	}{
		{"nil entry", []*raftv1.Entry{nil}, 0},
		{"zero index", []*raftv1.Entry{{Term: 1, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL}}, 0},
		{"missing prefix", []*raftv1.Entry{{Index: 2, Term: 1, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL}}, 0},
		{"index gap", []*raftv1.Entry{
			{Index: 1, Term: 1, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL},
			{Index: 3, Term: 1, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL},
		}, 0},
		{"duplicate index", []*raftv1.Entry{
			{Index: 1, Term: 1, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL},
			{Index: 1, Term: 1, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL},
		}, 0},
		{"zero term", []*raftv1.Entry{{Index: 1, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL}}, 0},
		{"future term", entriesWithTerm(1, 4), 0},
		{"decreasing terms", []*raftv1.Entry{
			{Index: 1, Term: 2, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL},
			{Index: 2, Term: 1, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL},
		}, 0},
		{"unspecified type", []*raftv1.Entry{{Index: 1, Term: 1}}, 0},
		{"unknown type", []*raftv1.Entry{{Index: 1, Term: 1, Type: 99}}, 0},
		{"no-op data", []*raftv1.Entry{{Index: 1, Term: 1, Type: raftv1.EntryType_ENTRY_TYPE_NO_OP, Data: []byte("command")}}, 0},
		{"commit beyond log", entriesWithTerm(2, 1), 3},
		{"commit with empty log", nil, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config()
			cfg.HardState = HardState{Term: 3, Commit: tc.commit}
			cfg.Entries = tc.entries
			if core, err := New(cfg); err == nil || core != nil {
				t.Fatalf("invalid initial log accepted: core=%v err=%v", core, err)
			}
		})
	}
}

func TestInitialLogReadyOwnershipAndApply(t *testing.T) {
	cfg := config()
	cfg.HardState = HardState{Term: 3, Commit: 2}
	cfg.Entries = []*raftv1.Entry{
		{Index: 1, Term: 1, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL, Data: []byte("put")},
		{Index: 2, Term: 2, Type: raftv1.EntryType_ENTRY_TYPE_NO_OP},
		{Index: 3, Term: 2, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL, Data: []byte("pending")},
	}
	core, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	// The caller owns the original slice, entries and payloads.
	cfg.Entries[0].Data[0] = 'x'
	cfg.Entries[2].Term = 99
	cfg.Entries[1] = nil
	status := core.Status()
	if status.LastLogIndex != 3 || status.LastLogTerm != 2 || status.HardState.Commit != 2 || status.AppliedIndex != 0 {
		t.Fatalf("initial log or unapplied commit is incorrect: %+v", status)
	}
	rd := core.Ready()
	if rd.HardState == nil || rd.HardState.Commit != 2 || len(rd.Entries) != 3 || len(rd.CommittedEntries) != 2 ||
		string(rd.Entries[0].Data) != "put" || rd.CommittedEntries[1].Type != raftv1.EntryType_ENTRY_TYPE_NO_OP {
		t.Fatalf("missing retained log or committed prefix: %+v", rd)
	}
	if core.Status().AppliedIndex != 0 || !errors.Is(core.Tick(), ErrReadyPending) {
		t.Fatal("Ready acknowledged application or allowed input before Advance")
	}
	// Output lists must not alias each other, the core, or subsequent snapshots.
	rd.Entries[0].Data[0] = 'y'
	if string(rd.CommittedEntries[0].Data) != "put" {
		t.Fatal("retained and committed outputs share payload memory")
	}
	rd.CommittedEntries[0].Data[0] = 'z'
	rd.CommittedEntries[1].Term = 99
	rd.Entries[2] = nil
	rd.HardState.Commit = 99
	retry := core.Ready()
	if retry.HardState.Commit != 2 || string(retry.Entries[0].Data) != "put" ||
		string(retry.CommittedEntries[0].Data) != "put" || retry.CommittedEntries[1].Term != 2 || retry.Entries[2].Term != 2 {
		t.Fatal("output mutation changed the outstanding batch")
	}
	if err := core.Advance(); err != nil {
		t.Fatal(err)
	}
	if core.Status().AppliedIndex != 2 || string(retry.Entries[2].Data) != "pending" {
		t.Fatal("Advance did not acknowledge the applied prefix or mutated output")
	}
	if rd := core.Ready(); rd.HardState != nil || len(rd.Entries) != 0 || len(rd.Messages) != 0 || len(rd.CommittedEntries) != 0 {
		t.Fatalf("acknowledged output repeated: %+v", rd)
	}
	if !errors.Is(core.Advance(), ErrNoReady) {
		t.Fatal("application was acknowledged twice")
	}
}

func TestChangedSuffixAndNewCommittedPrefix(t *testing.T) {
	cfg := config()
	cfg.HardState = HardState{Term: 3, Commit: 1}
	cfg.Entries = entriesWithTerm(4, 1)
	core := newElectionCore(t, cfg)
	// Exercise the output contract independently of future append handling.
	core.log = append(core.log[:2], &raftv1.Entry{
		Index: 3, Term: 2, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL, Data: []byte("replacement"),
	})
	core.unstableFrom = 3
	rd := core.Ready()
	if rd.HardState != nil || len(rd.Messages) != 0 || len(rd.CommittedEntries) != 0 ||
		len(rd.Entries) != 1 || rd.Entries[0].Index != 3 || core.Status().LastLogIndex != 3 {
		t.Fatalf("expected only the changed suffix, including removal of the old tail: %+v", rd)
	}
	if err := core.Advance(); err != nil {
		t.Fatal(err)
	}
	core.hardState.Commit = 3
	rd = core.Ready()
	if rd.HardState == nil || rd.HardState.Commit != 3 || len(rd.Entries) != 0 ||
		len(rd.CommittedEntries) != 2 || rd.CommittedEntries[0].Index != 2 || rd.CommittedEntries[1].Index != 3 {
		t.Fatalf("expected newly committed entries without resaving the log: %+v", rd)
	}
	if core.Status().AppliedIndex != 1 {
		t.Fatal("application advanced before acknowledgment")
	}
	if err := core.Advance(); err != nil {
		t.Fatal(err)
	}
	if core.Status().AppliedIndex != 3 {
		t.Fatal("Advance omitted newly applied entries")
	}
	// Like the existing messages-only test, isolate output detection from the
	// producer: an apply-only batch must also require Advance.
	core.appliedIndex = 2
	rd = core.Ready()
	if rd.HardState != nil || len(rd.Entries) != 0 || len(rd.CommittedEntries) != 1 ||
		!errors.Is(core.Tick(), ErrReadyPending) {
		t.Fatalf("apply-only output was not an outstanding batch: %+v", rd)
	}
	if err := core.Advance(); err != nil {
		t.Fatal(err)
	}
}

func TestHigherTermsPreserveCommit(t *testing.T) {
	for _, kind := range []raftv1.MessageType{
		raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE,
		raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE_RESPONSE,
		raftv1.MessageType_MESSAGE_TYPE_HEARTBEAT,
	} {
		t.Run(kind.String(), func(t *testing.T) {
			cfg := config()
			cfg.HardState = HardState{Term: 3, Vote: 1, Commit: 1}
			cfg.Entries = entriesWithTerm(2, 2)
			core := newElectionCore(t, cfg)
			msg := &raftv1.Message{Type: kind, From: 2, To: 1, Term: 4}
			if kind == raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE {
				msg.LastLogIndex, msg.LastLogTerm = 2, 2
			}
			rd := stepElection(t, core, msg)
			wantVote := uint64(0)
			if kind == raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE {
				wantVote = 2
			}
			if rd.HardState == nil || *rd.HardState != (HardState{Term: 4, Vote: wantVote, Commit: 1}) ||
				core.Status().AppliedIndex != 1 || len(rd.CommittedEntries) != 0 {
				t.Fatalf("term change lost commitment or reapplied entries: %+v", rd)
			}
		})
	}
}

func TestElectionMessagesRejectReplicationFields(t *testing.T) {
	for _, kind := range []raftv1.MessageType{
		raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE,
		raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE_RESPONSE,
		raftv1.MessageType_MESSAGE_TYPE_HEARTBEAT,
	} {
		core := newElectionCore(t, config())
		before := core.Status()
		for _, fields := range []*raftv1.Message{
			{PrevLogIndex: 1}, {PrevLogTerm: 1}, {Entries: entriesWithTerm(1, 1)},
			{LeaderCommit: 1}, {MatchIndex: 1}, {ConflictTerm: 1}, {ConflictIndex: 1},
		} {
			msg := proto.Clone(fields).(*raftv1.Message)
			msg.Type, msg.From, msg.To, msg.Term = kind, 2, 1, 99
			if err := core.Step(msg); err == nil || core.Status() != before {
				t.Fatalf("replication fields accepted or mutated election state: %v err=%v", msg, err)
			}
		}
	}
}
