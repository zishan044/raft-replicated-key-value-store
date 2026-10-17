package raftcore

import (
	"errors"
	"fmt"
	"slices"
	"testing"

	raftv1 "github.com/zishan044/raft-replicated-key-value-store.git/pkg/api/raft/v1"
	"google.golang.org/protobuf/proto"
)

func logWithTerms(terms ...uint64) []*raftv1.Entry {
	var entries []*raftv1.Entry
	for i, term := range terms {
		entries = append(entries, &raftv1.Entry{
			Index: uint64(i) + 1, Term: term, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL,
			Data: []byte(fmt.Sprintf("index=%d term=%d", i+1, term)),
		})
	}
	return entries
}

func appendRequest(term, prevIndex, prevTerm uint64, entries ...*raftv1.Entry) *raftv1.Message {
	return &raftv1.Message{
		Type: raftv1.MessageType_MESSAGE_TYPE_APPEND_ENTRIES, From: 2, To: 1, Term: term,
		PrevLogIndex: prevIndex, PrevLogTerm: prevTerm, Entries: entries,
	}
}

func equalEntries(a, b []*raftv1.Entry) bool {
	return slices.EqualFunc(a, b, func(a, b *raftv1.Entry) bool { return proto.Equal(a, b) })
}

func TestAppendValidationBeforeMutation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*raftv1.Message)
	}{
		{"wrong sender", func(m *raftv1.Message) { m.From = 99 }},
		{"self sender", func(m *raftv1.Message) { m.From = 1 }},
		{"wrong recipient", func(m *raftv1.Message) { m.To = 2 }},
		{"zero term", func(m *raftv1.Message) { m.Term = 0 }},
		{"reject flag", func(m *raftv1.Message) { m.Reject = true }},
		{"vote index", func(m *raftv1.Message) { m.LastLogIndex = 1 }},
		{"vote term", func(m *raftv1.Message) { m.LastLogTerm = 1 }},
		{"match index", func(m *raftv1.Message) { m.MatchIndex = 2 }},
		{"conflict term", func(m *raftv1.Message) { m.ConflictTerm = 2 }},
		{"conflict index", func(m *raftv1.Message) { m.ConflictIndex = 1 }},
		{"previous index without term", func(m *raftv1.Message) { m.PrevLogTerm = 0 }},
		{"previous term without index", func(m *raftv1.Message) { m.PrevLogIndex = 0 }},
		{"future previous term", func(m *raftv1.Message) { m.PrevLogTerm = 5 }},
		{"nil entry", func(m *raftv1.Message) { m.Entries[0] = nil }},
		{"zero entry index", func(m *raftv1.Message) { m.Entries[0].Index = 0 }},
		{"index gap", func(m *raftv1.Message) { m.Entries[0].Index = 3 }},
		{"zero entry term", func(m *raftv1.Message) { m.Entries[0].Term = 0 }},
		{"future entry term", func(m *raftv1.Message) { m.Entries[0].Term = 5 }},
		{"entry term before previous", func(m *raftv1.Message) { m.PrevLogTerm, m.Entries[0].Term = 2, 1 }},
		{"unspecified entry type", func(m *raftv1.Message) { m.Entries[0].Type = raftv1.EntryType_ENTRY_TYPE_UNSPECIFIED }},
		{"unknown entry type", func(m *raftv1.Message) { m.Entries[0].Type = 99 }},
		{"no-op data", func(m *raftv1.Message) { m.Entries[0].Type = raftv1.EntryType_ENTRY_TYPE_NO_OP }},
		{"bad later index", func(m *raftv1.Message) {
			m.Entries = append(m.Entries, &raftv1.Entry{Index: 4, Term: 3, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL})
		}},
		{"duplicate index", func(m *raftv1.Message) { m.Entries = append(m.Entries, proto.Clone(m.Entries[0]).(*raftv1.Entry)) }},
		{"decreasing terms", func(m *raftv1.Message) {
			m.Entries = append(m.Entries, &raftv1.Entry{Index: 3, Term: 2, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL})
		}},
		{"index overflow", func(m *raftv1.Message) { m.PrevLogIndex = ^uint64(0) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config()
			cfg.HardState = HardState{Term: 3, Vote: 1, Commit: 1}
			cfg.Entries = logWithTerms(1, 2)
			core := newElectionCore(t, cfg)
			if err := core.Tick(); err != nil {
				t.Fatal(err)
			}
			before := core.Status()
			msg := appendRequest(4, 1, 1, logWithTerms(1, 3)[1:]...)
			tc.change(msg)
			if err := core.Step(msg); err == nil || core.Status() != before || !equalEntries(core.log, cfg.Entries) {
				t.Fatalf("malformed append was accepted or changed state: msg=%v err=%v status=%+v", msg, err, core.Status())
			}
			if rd := core.Ready(); rd.HardState != nil || len(rd.Entries) != 0 || len(rd.Messages) != 0 || len(rd.CommittedEntries) != 0 {
				t.Fatalf("malformed append emitted output: %+v", rd)
			}
		})
	}
}

func TestFollowerAppendLogAndCommit(t *testing.T) {
	noOp := &raftv1.Entry{Index: 2, Term: 5, Type: raftv1.EntryType_ENTRY_TYPE_NO_OP}
	for _, tc := range []struct {
		name                      string
		initial                   []*raftv1.Entry
		commit                    uint64
		request                   *raftv1.Message
		leaderCommit              uint64
		wantLog                   []*raftv1.Entry
		changedFrom, wantCommit   uint64
		reject                    bool
		match, conflictTerm, hint uint64
	}{
		{"empty log", nil, 0, appendRequest(5, 0, 0, logWithTerms(4, 5)...), 1,
			logWithTerms(4, 5), 1, 1, false, 2, 0, 0},
		{"missing previous entry", logWithTerms(1, 1), 1, appendRequest(5, 4, 2), 100,
			logWithTerms(1, 1), 0, 1, true, 0, 0, 3},
		{"conflicting previous term", logWithTerms(1, 2, 2, 3), 1, appendRequest(5, 3, 1), 100,
			logWithTerms(1, 2, 2, 3), 0, 1, true, 0, 2, 2},
		{"conflicting term starts at one", logWithTerms(1, 1, 2), 0, appendRequest(5, 2, 2), 0,
			logWithTerms(1, 1, 2), 0, 0, true, 0, 1, 1},
		{"append missing tail", logWithTerms(1, 2), 1, appendRequest(5, 2, 2, logWithTerms(1, 2, 4, 5)[2:]...), 3,
			logWithTerms(1, 2, 4, 5), 3, 3, false, 4, 0, 0},
		{"replace conflicting tail", logWithTerms(1, 2, 2, 3, 3), 1, appendRequest(5, 1, 1, logWithTerms(1, 4)[1:]...), 0,
			logWithTerms(1, 4), 2, 1, false, 2, 0, 0},
		{"skip matching entries before conflict", logWithTerms(1, 2, 2, 3, 3), 1, appendRequest(5, 1, 1, logWithTerms(1, 2, 4)[1:]...), 100,
			logWithTerms(1, 2, 4), 3, 3, false, 3, 0, 0},
		{"matching shorter request", logWithTerms(1, 2, 3, 4), 1, appendRequest(5, 1, 1, logWithTerms(1, 2)[1:]...), 100,
			logWithTerms(1, 2, 3, 4), 0, 2, false, 2, 0, 0},
		{"empty append bounds commit", logWithTerms(1, 2, 3, 4), 1, appendRequest(5, 2, 2), 100,
			logWithTerms(1, 2, 3, 4), 0, 2, false, 2, 0, 0},
		{"empty append at start", logWithTerms(1, 2), 1, appendRequest(5, 0, 0), 100,
			logWithTerms(1, 2), 0, 1, false, 0, 0, 0},
		{"delayed commit cannot regress", logWithTerms(1, 2, 3), 3, appendRequest(5, 1, 1, logWithTerms(1, 2)[1:]...), 1,
			logWithTerms(1, 2, 3), 0, 3, false, 2, 0, 0},
		{"empty log probe", nil, 0, appendRequest(5, 0, 0), ^uint64(0),
			nil, 0, 0, false, 0, 0, 0},
		{"no-op append", logWithTerms(1), 1, appendRequest(5, 1, 1, noOp), 2,
			[]*raftv1.Entry{logWithTerms(1)[0], noOp}, 2, 2, false, 2, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config()
			cfg.HardState = HardState{Term: 5, Vote: 1, Commit: tc.commit}
			cfg.Entries = tc.initial
			core := newElectionCore(t, cfg)
			tc.request.LeaderCommit = tc.leaderCommit
			if err := core.Step(tc.request); err != nil {
				t.Fatal(err)
			}
			rd := core.Ready()
			wantResponse := &raftv1.Message{
				Type: raftv1.MessageType_MESSAGE_TYPE_APPEND_ENTRIES_RESPONSE, From: 1, To: 2, Term: 5,
				PrevLogIndex: tc.request.PrevLogIndex, Reject: tc.reject, MatchIndex: tc.match,
				ConflictTerm: tc.conflictTerm, ConflictIndex: tc.hint,
			}
			if len(rd.Messages) != 1 || !proto.Equal(rd.Messages[0], wantResponse) {
				t.Fatalf("response: got %v, want %v", rd.Messages, wantResponse)
			}
			var wantChanged []*raftv1.Entry
			if tc.changedFrom != 0 {
				wantChanged = tc.wantLog[tc.changedFrom-1:]
			}
			if !equalEntries(core.log, tc.wantLog) || !equalEntries(rd.Entries, wantChanged) ||
				!equalEntries(rd.CommittedEntries, tc.wantLog[tc.commit:tc.wantCommit]) {
				t.Fatalf("wrong log or output: log=%v changed=%v committed=%v", core.log, rd.Entries, rd.CommittedEntries)
			}
			if status := core.Status(); status.HardState.Commit != tc.wantCommit || status.AppliedIndex != tc.commit ||
				status.LeaderID != 2 || status.Role != Follower || status.HardState.Vote != 1 {
				t.Fatalf("wrong append status before Advance: %+v", status)
			}
			if (tc.wantCommit != tc.commit) != (rd.HardState != nil) {
				t.Fatal("commit changes must be included in the batch before its response is sent")
			}
			if err := core.Advance(); err != nil {
				t.Fatal(err)
			}
			if core.Status().AppliedIndex != tc.wantCommit {
				t.Fatal("Advance did not acknowledge application")
			}
		})
	}
}

func TestAppendTermRoleAndElectionTimer(t *testing.T) {
	for _, role := range []Role{Follower, Candidate, Leader} {
		for _, delta := range []int{-1, 0, 1} {
			for _, missingPrevious := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/term_delta_%d/missing_%t", role, delta, missingPrevious), func(t *testing.T) {
					cfg := config()
					cfg.HardState = HardState{Term: 3, Vote: 3, Commit: 1}
					cfg.Entries = logWithTerms(1, 2)
					core := newElectionCore(t, cfg)
					if role != Follower {
						expireElection(t, core)
					}
					if role == Leader {
						stepElection(t, core, &raftv1.Message{
							Type: raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE_RESPONSE, From: 2, To: 1, Term: core.Status().HardState.Term,
						})
					}
					for range 3 {
						if err := core.Tick(); err != nil {
							t.Fatal(err)
						}
						takeReady(t, core)
					}
					before := core.Status()
					msg := appendRequest(uint64(int(before.HardState.Term)+delta), 2, 2)
					if missingPrevious {
						msg.PrevLogIndex = 3
					}
					rd := stepElection(t, core, msg)
					if delta < 0 {
						if core.Status() != before || rd.HardState != nil {
							t.Fatal("stale append changed state or reset the election timer")
						}
					} else {
						wantState := before.HardState
						if delta > 0 {
							wantState.Term, wantState.Vote = msg.Term, 0
						}
						if status := core.Status(); status.Role != Follower || status.LeaderID != 2 ||
							status.HardState != wantState || status.ElectionElapsed != 0 {
							t.Fatalf("append failed to establish leader contact: %+v", status)
						}
						if (delta > 0) != (rd.HardState != nil) {
							t.Fatal("new term was not retained with its response")
						}
					}
					if len(rd.Messages) != 1 || rd.Messages[0].Term != core.Status().HardState.Term ||
						rd.Messages[0].Reject != (delta < 0 || missingPrevious) || len(rd.Entries) != 0 || len(rd.CommittedEntries) != 0 {
						t.Fatalf("wrong stale/contact response: %+v", rd)
					}
				})
			}
		}
	}
}

func TestAppendCommittedConflictLeavesCoreUnchanged(t *testing.T) {
	for _, term := range []uint64{3, 4, 5} {
		t.Run(fmt.Sprint(term), func(t *testing.T) {
			cfg := config()
			cfg.HardState = HardState{Term: 4, Vote: 1, Commit: 2}
			cfg.Entries = logWithTerms(1, 2, 3)
			core := newElectionCore(t, cfg)
			before := core.Status()
			msg := appendRequest(term, 1, 1, logWithTerms(1, 3)[1:]...)
			err := core.Step(msg)
			if (term >= 4) != (err != nil) || core.Status() != before || !equalEntries(core.log, cfg.Entries) {
				t.Fatalf("committed conflict changed state or had the wrong outcome: err=%v status=%+v", err, core.Status())
			}
			rd := takeReady(t, core)
			if term < 4 {
				if len(rd.Messages) != 1 || !rd.Messages[0].Reject {
					t.Fatal("stale committed conflict should receive a stale-term rejection")
				}
			} else if rd.HardState != nil || len(rd.Entries) != 0 || len(rd.Messages) != 0 || len(rd.CommittedEntries) != 0 {
				t.Fatalf("committed conflict emitted output: %+v", rd)
			}
		})
	}
}

func TestAppendOwnershipDuplicatesAndDelayedRequests(t *testing.T) {
	cfg := config()
	cfg.HardState.Term = 3
	cfg.Entries = logWithTerms(1)
	core := newElectionCore(t, cfg)
	entries := logWithTerms(1, 2, 3)
	msg := appendRequest(3, 1, 1, entries[1:]...)
	msg.LeaderCommit = 2
	if err := core.Step(msg); err != nil {
		t.Fatal(err)
	}
	msg.Entries[0].Data[0] = 'x'
	msg.Entries[1].Term = 99
	msg.Entries[0] = nil
	rd := core.Ready()
	if !equalEntries(rd.Entries, logWithTerms(1, 2, 3)[1:]) ||
		!equalEntries(rd.CommittedEntries, logWithTerms(1, 2)) || rd.Messages[0].MatchIndex != 3 {
		t.Fatal("append retained caller-owned input")
	}
	rd.Entries[0].Data[0] = 'y'
	if !equalEntries(core.Ready().Entries, logWithTerms(1, 2, 3)[1:]) ||
		!equalEntries(rd.CommittedEntries, logWithTerms(1, 2)) || !errors.Is(core.Tick(), ErrReadyPending) {
		t.Fatal("append output is not independently owned and retryable")
	}
	if err := core.Advance(); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		// An older, shorter append can arrive twice after the longer one.
		delayed := appendRequest(3, 1, 1, logWithTerms(1, 2)[1:]...)
		delayed.LeaderCommit = 1
		rd = stepElection(t, core, delayed)
		if len(rd.Entries) != 0 || len(rd.CommittedEntries) != 0 || rd.HardState != nil ||
			rd.Messages[0].MatchIndex != 2 || !equalEntries(core.log, logWithTerms(1, 2, 3)) ||
			core.Status().HardState.Commit != 2 || core.Status().AppliedIndex != 2 {
			t.Fatal("delayed or duplicate append truncated the tail, regressed commit or repeated application")
		}
	}
}

func TestAppendMultipleInputsRetainEarliestChangedSuffix(t *testing.T) {
	cfg := config()
	cfg.HardState = HardState{Term: 3, Commit: 1}
	cfg.Entries = logWithTerms(1, 2, 2, 2)
	core := newElectionCore(t, cfg)
	if err := core.Step(appendRequest(3, 4, 2, logWithTerms(1, 2, 2, 2, 3)[4:]...)); err != nil {
		t.Fatal(err)
	}
	if err := core.Step(appendRequest(4, 1, 1, logWithTerms(1, 4)[1:]...)); err != nil {
		t.Fatal(err)
	}
	rd := takeReady(t, core)
	if !equalEntries(rd.Entries, logWithTerms(1, 4)[1:]) || len(rd.Messages) != 2 ||
		core.Status().LastLogIndex != 2 || rd.HardState == nil || rd.HardState.Term != 4 {
		t.Fatalf("batched appends omitted an earlier replacement: %+v", rd)
	}
}
