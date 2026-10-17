package raftcore

import (
	"errors"
	"fmt"
	"testing"

	raftv1 "github.com/zishan044/raft-replicated-key-value-store.git/pkg/api/raft/v1"
)

func electLeader(t *testing.T, cfg Config) (*Core, Ready) {
	t.Helper()
	core := newElectionCore(t, cfg)
	rd := expireElection(t, core)
	for _, peer := range core.voters {
		if core.Status().Role == Leader {
			break
		}
		if peer != cfg.ID {
			rd = stepElection(t, core, &raftv1.Message{
				Type: raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE_RESPONSE,
				From: peer, To: cfg.ID, Term: core.Status().HardState.Term,
			})
		}
	}
	if core.Status().Role != Leader {
		t.Fatal("test failed to elect a leader")
	}
	return core, rd
}

func appendResponse(from, term, previous, matched uint64) *raftv1.Message {
	return &raftv1.Message{
		Type: raftv1.MessageType_MESSAGE_TYPE_APPEND_ENTRIES_RESPONSE,
		From: from, To: 1, Term: term, PrevLogIndex: previous, MatchIndex: matched,
	}
}

func TestProposeAcceptanceOwnershipAndCommit(t *testing.T) {
	core, err := New(config())
	if err != nil {
		t.Fatal(err)
	}
	if index, term, err := core.Propose(nil); index != 0 || term != 0 || !errors.Is(err, ErrReadyPending) {
		t.Fatal("proposal accepted before initial state was retained")
	}
	takeReady(t, core)
	stepElection(t, core, appendRequest(1, 0, 0))
	for _, role := range []Role{Follower, Candidate} {
		if role == Candidate {
			expireElection(t, core)
		}
		before := core.Status()
		if index, term, err := core.Propose([]byte("rejected")); index != 0 || term != 0 ||
			!errors.Is(err, ErrNotLeader) || core.Status() != before || len(core.log) != 0 {
			t.Fatalf("%s accepted a proposal or changed state", role)
		}
		if role == Follower && before.LeaderID != 2 {
			t.Fatal("follower did not retain the leader hint")
		}
	}
	core, rd := electLeader(t, config())
	if len(rd.Entries) != 1 || rd.Entries[0].Type != raftv1.EntryType_ENTRY_TYPE_NO_OP ||
		rd.Entries[0].Index != 1 || rd.Entries[0].Term != 1 || core.Status().HardState.Commit != 0 {
		t.Fatalf("new leader did not append exactly one uncommitted no-op: %+v", rd)
	}
	for _, p := range core.progress {
		if p.next != 1 || p.match != 0 {
			t.Fatal("peer progress was not initialized before the no-op")
		}
	}
	data := []byte("accepted")
	index, term, err := core.Propose(data)
	if err != nil || index != 2 || term != 1 || core.Status().HardState.Commit != 0 {
		t.Fatalf("proposal did not return local acceptance: index=%d term=%d err=%v", index, term, err)
	}
	data[0] = 'x'
	rd = core.Ready()
	if len(rd.Entries) != 1 || string(rd.Entries[0].Data) != "accepted" || len(rd.CommittedEntries) != 0 || len(rd.Messages) != 2 {
		t.Fatalf("wrong proposal batch: %+v", rd)
	}
	for _, msg := range rd.Messages {
		if msg.Type != raftv1.MessageType_MESSAGE_TYPE_APPEND_ENTRIES || len(msg.Entries) != 2 ||
			string(msg.Entries[1].Data) != "accepted" || msg.PrevLogIndex != 0 {
			t.Fatalf("proposal did not broadcast an owned pending suffix: %v", msg)
		}
	}
	rd.Messages[0].Entries[1].Data[0] = 'y'
	if string(rd.Entries[0].Data) != "accepted" || string(rd.Messages[1].Entries[1].Data) != "accepted" ||
		string(core.Ready().Messages[0].Entries[1].Data) != "accepted" {
		t.Fatal("proposal outputs share mutable payloads")
	}
	if _, _, err := core.Propose(nil); !errors.Is(err, ErrReadyPending) || core.Status().LastLogIndex != 2 {
		t.Fatal("proposal accepted while Ready was outstanding")
	}
	if err := core.Advance(); err != nil {
		t.Fatal(err)
	}
	if err := core.Step(appendResponse(2, 1, 0, 2)); err != nil {
		t.Fatal(err)
	}
	rd = core.Ready()
	if rd.HardState == nil || rd.HardState.Commit != 2 || len(rd.CommittedEntries) != 2 ||
		rd.CommittedEntries[0].Type != raftv1.EntryType_ENTRY_TYPE_NO_OP || string(rd.CommittedEntries[1].Data) != "accepted" ||
		core.Status().AppliedIndex != 0 || len(rd.Messages) != 2 {
		t.Fatalf("majority did not commit the no-op and proposal together: %+v", rd)
	}
	for _, msg := range rd.Messages {
		if msg.LeaderCommit != 2 {
			t.Fatal("commit advance was not broadcast")
		}
	}
	if err := core.Advance(); err != nil {
		t.Fatal(err)
	}
	if rd := stepElection(t, core, appendResponse(2, 1, 0, 2)); len(rd.Messages) != 0 || len(rd.CommittedEntries) != 0 || rd.HardState != nil {
		t.Fatal("duplicate acknowledgment repeated commitment or triggered a reply loop")
	}
	response := appendResponse(3, 2, 0, 0)
	response.Reject = true
	stepElection(t, core, response)
	if _, _, err := core.Propose(nil); !errors.Is(err, ErrNotLeader) || core.Status().HardState.Commit != 2 || core.progress != nil {
		t.Fatal("deposed leader accepted a proposal or lost commitment")
	}
}

func TestLeaderCommitsOnlyThroughCurrentTerm(t *testing.T) {
	cfg := config()
	cfg.HardState.Term = 2
	cfg.Entries = logWithTerms(1, 2, 2)
	core, _ := electLeader(t, cfg)
	// Isolate the commit rule with a partial acknowledgment that excludes the
	// new-term no-op. Full Figure 8 schedules belong to subset 4.
	rd := stepElection(t, core, appendResponse(2, 3, 3, 3))
	if core.progress[2].match != 3 || core.Status().HardState.Commit != 0 || len(rd.CommittedEntries) != 0 || rd.HardState != nil {
		t.Fatal("replicas of an older-term entry were counted as commitment")
	}
	if len(rd.Messages) != 1 || rd.Messages[0].PrevLogIndex != 3 || len(rd.Messages[0].Entries) != 1 ||
		rd.Messages[0].Entries[0].Type != raftv1.EntryType_ENTRY_TYPE_NO_OP {
		t.Fatal("partial acknowledgment did not continue replication")
	}
	rd = stepElection(t, core, appendResponse(2, 3, 3, 4))
	if core.Status().HardState.Commit != 4 || len(rd.CommittedEntries) != 4 ||
		rd.CommittedEntries[3].Term != 3 || rd.CommittedEntries[3].Type != raftv1.EntryType_ENTRY_TYPE_NO_OP {
		t.Fatal("current-term no-op did not indirectly commit the older prefix")
	}
}

func TestLostAppendAcknowledgmentIsRetried(t *testing.T) {
	leader, _ := electLeader(t, config())
	cfg := config()
	cfg.ID = 2
	follower := newElectionCore(t, cfg)
	if _, _, err := leader.Propose([]byte("ack-loss")); err != nil {
		t.Fatal(err)
	}
	initial := takeReady(t, leader)
	first := stepElection(t, follower, initial.Messages[0]) // drop its acknowledgment
	if len(first.Entries) != 2 || leader.progress[2].match != 0 || leader.Status().HardState.Commit != 0 {
		t.Fatal("lost acknowledgment unexpectedly advanced leader progress")
	}
	for range config().HeartbeatTicks {
		if err := leader.Tick(); err != nil {
			t.Fatal(err)
		}
	}
	retry := takeReady(t, leader)
	if len(retry.Messages) != 2 || !equalEntries(retry.Messages[0].Entries, first.Entries) {
		t.Fatal("heartbeat tick did not retry unacknowledged entries")
	}
	reply := stepElection(t, follower, retry.Messages[0])
	if len(reply.Entries) != 0 || len(reply.CommittedEntries) != 0 || len(reply.Messages) != 1 {
		t.Fatal("duplicate append rewrote or prematurely applied the log")
	}
	committed := stepElection(t, leader, reply.Messages[0])
	if leader.Status().HardState.Commit != 2 || len(committed.CommittedEntries) != 2 {
		t.Fatal("retried acknowledgment did not commit the proposal")
	}
	applied := stepElection(t, follower, committed.Messages[0])
	if len(applied.CommittedEntries) != 2 || follower.Status().AppliedIndex != 2 {
		t.Fatal("follower did not apply the newly advertised commitment")
	}
}

func TestQueuedAppendKeepsItsOriginalSuffix(t *testing.T) {
	core, _ := electLeader(t, config())
	if _, _, err := core.Propose([]byte("old")); err != nil {
		t.Fatal(err)
	}
	// Inputs may accumulate before Ready. A later leader can replace an
	// uncommitted suffix without rewriting the already queued old-term message.
	if err := core.Step(appendRequest(2, 1, 1, &raftv1.Entry{
		Index: 2, Term: 2, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL, Data: []byte("new"),
	})); err != nil {
		t.Fatal(err)
	}
	rd := takeReady(t, core)
	if len(rd.Messages) != 3 || len(rd.Entries) != 1 || string(rd.Entries[0].Data) != "new" {
		t.Fatalf("wrong combined step-down batch: %+v", rd)
	}
	for _, msg := range rd.Messages[:2] {
		if msg.Term != 1 || len(msg.Entries) != 2 || msg.Entries[1].Term != 1 || string(msg.Entries[1].Data) != "old" {
			t.Fatal("suffix replacement changed a queued append")
		}
	}
}

func TestLeaderRequiresStrictMajority(t *testing.T) {
	for _, size := range []int{1, 2, 3, 4, 5} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			cfg := config()
			cfg.Voters = make([]uint64, size)
			for i := range cfg.Voters {
				cfg.Voters[i] = uint64(i + 1)
			}
			core, _ := electLeader(t, cfg)
			index, _, err := core.Propose(nil)
			if err != nil {
				t.Fatal(err)
			}
			takeReady(t, core)
			for acknowledgments := 0; acknowledgments <= size/2; acknowledgments++ {
				if acknowledgments != 0 {
					stepElection(t, core, appendResponse(uint64(acknowledgments+1), 1, 0, index))
				}
				wantCommit := uint64(0)
				if acknowledgments+1 > size/2 {
					wantCommit = index
				}
				if core.Status().HardState.Commit != wantCommit {
					t.Fatalf("size=%d peer_acks=%d commit=%d want=%d", size, acknowledgments, core.Status().HardState.Commit, wantCommit)
				}
			}
			if core.log[index-1].Type != raftv1.EntryType_ENTRY_TYPE_NORMAL {
				t.Fatal("empty proposal was confused with an election no-op")
			}
		})
	}
}

func TestLeaderConflictHints(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		conflictTerm, hint, next uint64
	}{
		{"term present", 2, 2, 5},
		{"term absent", 3, 2, 2},
		{"missing previous entry", 0, 3, 3},
		{"hint must decrease probe", 4, 1, 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config()
			cfg.HardState.Term = 4
			cfg.Entries = logWithTerms(1, 1, 2, 2, 4)
			core, _ := electLeader(t, cfg)
			beforeLog := cloneEntries(core.log)
			response := appendResponse(2, 5, 5, 0)
			response.Reject, response.ConflictTerm, response.ConflictIndex = true, tc.conflictTerm, tc.hint
			rd := stepElection(t, core, response)
			if p := core.progress[2]; p.next != tc.next || p.match != 0 {
				t.Fatalf("hint produced wrong progress: %+v", p)
			}
			if len(rd.Messages) != 1 || rd.Messages[0].To != 2 || rd.Messages[0].PrevLogIndex != tc.next-1 ||
				!equalEntries(rd.Messages[0].Entries, beforeLog[tc.next-1:]) || !equalEntries(core.log, beforeLog) ||
				core.progress[3].next != 6 {
				t.Fatalf("hint did not retry only the rejected peer: %+v", rd)
			}
		})
	}
}

func TestDelayedResponsesCannotRegressProgress(t *testing.T) {
	cfg := config()
	cfg.Voters = []uint64{1, 2, 3, 4, 5}
	cfg.HardState.Term = 4
	cfg.Entries = logWithTerms(1, 1, 2, 2, 4)
	core, _ := electLeader(t, cfg)
	oldProbe := appendResponse(2, 5, 4, 0)
	oldProbe.Reject, oldProbe.ConflictIndex = true, 1
	if rd := stepElection(t, core, oldProbe); len(rd.Messages) != 0 || core.progress[2].next != 6 {
		t.Fatal("rejection of an older probe moved nextIndex")
	}
	stepElection(t, core, appendResponse(2, 5, 5, 6))
	for _, response := range []*raftv1.Message{
		appendResponse(2, 5, 5, 6), // duplicate success
		appendResponse(2, 5, 2, 3), // delayed partial success
		{Type: raftv1.MessageType_MESSAGE_TYPE_APPEND_ENTRIES_RESPONSE, From: 2, To: 1, Term: 5, PrevLogIndex: 5, Reject: true, ConflictIndex: 1},
		{Type: raftv1.MessageType_MESSAGE_TYPE_APPEND_ENTRIES_RESPONSE, From: 2, To: 1, Term: 5, PrevLogIndex: 6, Reject: true, ConflictIndex: 1},
		{Type: raftv1.MessageType_MESSAGE_TYPE_APPEND_ENTRIES_RESPONSE, From: 2, To: 1, Term: 4, PrevLogIndex: 6, Reject: true, ConflictIndex: 1},
	} {
		rd := stepElection(t, core, response)
		if p := core.progress[2]; p.next != 7 || p.match != 6 || len(rd.Messages) != 0 || rd.HardState != nil || len(rd.CommittedEntries) != 0 {
			t.Fatalf("stale response changed progress or manufactured a majority: msg=%v progress=%+v", response, p)
		}
	}
}

func TestAppendResponseValidationBeforeMutation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*raftv1.Message)
	}{
		{"zero term", func(m *raftv1.Message) { m.Term = 0 }},
		{"vote index", func(m *raftv1.Message) { m.LastLogIndex = 1 }},
		{"vote term", func(m *raftv1.Message) { m.LastLogTerm = 1 }},
		{"previous term", func(m *raftv1.Message) { m.PrevLogTerm = 1 }},
		{"entries", func(m *raftv1.Message) { m.Entries = []*raftv1.Entry{nil} }},
		{"leader commit", func(m *raftv1.Message) { m.LeaderCommit = 1 }},
		{"success before previous", func(m *raftv1.Message) { m.MatchIndex = 0 }},
		{"success with conflict term", func(m *raftv1.Message) { m.ConflictTerm = 1 }},
		{"success with conflict index", func(m *raftv1.Message) { m.ConflictIndex = 1 }},
		{"rejection with match", func(m *raftv1.Message) { m.Reject = true }},
		{"future conflict term", func(m *raftv1.Message) { m.Reject, m.MatchIndex, m.ConflictTerm, m.ConflictIndex = true, 0, 100, 1 }},
		{"conflict term without index", func(m *raftv1.Message) { m.Reject, m.MatchIndex, m.ConflictTerm = true, 0, 1 }},
		{"hint after previous", func(m *raftv1.Message) { m.Reject, m.MatchIndex, m.ConflictIndex = true, 0, 2 }},
		{"match exceeds log", func(m *raftv1.Message) { m.Term, m.MatchIndex = 1, 2 }},
		{"previous exceeds log", func(m *raftv1.Message) { m.Term, m.PrevLogIndex, m.MatchIndex, m.Reject = 1, 2, 0, true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			core, _ := electLeader(t, config())
			before, progressBefore := core.Status(), *core.progress[2]
			response := appendResponse(2, 99, 1, 1)
			tc.change(response)
			if err := core.Step(response); err == nil || core.Status() != before || *core.progress[2] != progressBefore {
				t.Fatalf("malformed response accepted or changed state: msg=%v err=%v", response, err)
			}
			if rd := core.Ready(); rd.HardState != nil || len(rd.Entries) != 0 || len(rd.Messages) != 0 || len(rd.CommittedEntries) != 0 {
				t.Fatalf("malformed response produced output: %+v", rd)
			}
		})
	}
}

func TestLeadershipChangeResetsProgressAndAppendsOneNoOp(t *testing.T) {
	core, _ := electLeader(t, config())
	stepElection(t, core, appendResponse(2, 1, 0, 1))
	stepElection(t, core, &raftv1.Message{
		Type: raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE, From: 2, To: 1, Term: 2, LastLogIndex: 1, LastLogTerm: 1,
	})
	if core.progress != nil || core.Status().HardState.Commit != 1 {
		t.Fatal("step-down retained leader progress or lost commitment")
	}
	expireElection(t, core)
	stepElection(t, core, appendResponse(2, 3, 0, 0))
	if core.Status().Role != Candidate {
		t.Fatal("an append response was counted as an election vote")
	}
	rd := stepElection(t, core, &raftv1.Message{
		Type: raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE_RESPONSE, From: 2, To: 1, Term: 3,
	})
	if len(rd.Entries) != 1 || rd.Entries[0].Index != 2 || rd.Entries[0].Term != 3 ||
		rd.Entries[0].Type != raftv1.EntryType_ENTRY_TYPE_NO_OP || core.progress[2].match != 0 || core.progress[2].next != 2 {
		t.Fatalf("reelection did not create a fresh no-op and peer progress: %+v", rd)
	}
	stepElection(t, core, &raftv1.Message{
		Type: raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE_RESPONSE, From: 2, To: 1, Term: 3,
	})
	if core.Status().LastLogIndex != 2 {
		t.Fatal("duplicate election response appended another no-op")
	}
}

func TestLeaderReplicationRetriesAndCatchesUpFollowers(t *testing.T) {
	for _, size := range []int{3, 5} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			voters := make([]uint64, size)
			for i := range voters {
				voters[i] = uint64(i + 1)
			}
			nodes := make([]*Core, size)
			for i := range nodes {
				cfg := config()
				cfg.ID, cfg.Seed, cfg.Voters, cfg.HardState.Term = voters[i], voters[i], voters, 2
				if i == 0 {
					cfg.Entries = logWithTerms(1, 2)
				}
				nodes[i] = newElectionCore(t, cfg)
			}
			// The leader starts with a prefix absent from every follower. Their
			// rejection hints must drive backup before its no-op can commit.
			deliverMessages(t, nodes, expireElection(t, nodes[0]).Messages, 0)
			for _, core := range nodes {
				if core.Status().HardState.Commit != 3 || !equalEntries(core.log, nodes[0].log) {
					t.Fatal("new leader did not repair missing prefixes and commit its no-op")
				}
			}
			index, _, err := nodes[0].Propose([]byte("replicated"))
			if err != nil {
				t.Fatal(err)
			}
			takeReady(t, nodes[0]) // drop the initial broadcast; ticks must retry it
			for range 6 {
				tickConnectedCores(t, nodes, uint64(size))
			}
			for _, core := range nodes[:size-1] {
				if core.Status().HardState.Commit != index || core.Status().AppliedIndex != index || string(core.log[index-1].Data) != "replicated" {
					t.Fatal("connected majority did not commit the retried proposal")
				}
			}
			if nodes[size-1].Status().LastLogIndex != 3 {
				t.Fatal("isolated follower unexpectedly received the proposal")
			}
			for range 6 {
				tickConnectedCores(t, nodes, 0)
			}
			if nodes[size-1].Status().AppliedIndex != index || !equalEntries(nodes[size-1].log, nodes[0].log) {
				t.Fatal("healed follower failed to catch up")
			}
		})
	}
}

func TestMinorityLeaderCannotCommitAndConvergesAfterHealing(t *testing.T) {
	nodes := make([]*Core, 3)
	for i := range nodes {
		cfg := config()
		cfg.ID, cfg.Seed = uint64(i+1), uint64(i+1)
		nodes[i] = newElectionCore(t, cfg)
	}
	deliverMessages(t, nodes, expireElection(t, nodes[0]).Messages, 0)
	index, _, err := nodes[0].Propose([]byte("minority"))
	if err != nil {
		t.Fatal(err)
	}
	takeReady(t, nodes[0]) // every outbound append is lost in the partition
	for range 60 {
		tickConnectedCores(t, nodes, 1)
	}
	if nodes[0].Status().Role != Leader || nodes[0].Status().HardState.Commit >= index {
		t.Fatal("isolated leader committed its locally accepted proposal")
	}
	for range 60 {
		tickConnectedCores(t, nodes, 0)
	}
	for _, core := range nodes {
		if !equalEntries(core.log, nodes[1].log) || core.Status().AppliedIndex != 2 || core.log[1].Type != raftv1.EntryType_ENTRY_TYPE_NO_OP {
			t.Fatal("healing did not replace the minority's uncommitted proposal")
		}
	}
	if nodes[0].Status().Role != Follower {
		t.Fatal("old leader did not step down after healing")
	}
}
