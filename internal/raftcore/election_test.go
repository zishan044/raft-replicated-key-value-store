package raftcore

import (
	"testing"

	raftv1 "github.com/zishan044/raft-replicated-key-value-store.git/pkg/api/raft/v1"
	"google.golang.org/protobuf/proto"
)

func newElectionCore(t *testing.T, cfg Config) *Core {
	t.Helper()
	core, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	takeReady(t, core)
	return core
}

func takeReady(t *testing.T, core *Core) Ready {
	t.Helper()
	rd := core.Ready()
	if rd.HardState != nil || len(rd.Messages) > 0 {
		if err := core.Advance(); err != nil {
			t.Fatal(err)
		}
	}
	return rd
}

func stepElection(t *testing.T, core *Core, msg *raftv1.Message) Ready {
	t.Helper()
	if err := core.Step(msg); err != nil {
		t.Fatal(err)
	}
	return takeReady(t, core)
}

func expireElection(t *testing.T, core *Core) Ready {
	t.Helper()
	before := core.Status()
	for remaining := before.ElectionTimeout - before.ElectionElapsed; remaining > 0; remaining-- {
		if err := core.Tick(); err != nil {
			t.Fatal(err)
		}
		if remaining > 1 && core.Status().HardState != before.HardState {
			t.Fatal("election started before its deadline")
		}
	}
	return takeReady(t, core)
}

func TestCampaignAndRetry(t *testing.T) {
	cfg := config()
	cfg.HardState.Term = 3
	cfg.LastLogIndex, cfg.LastLogTerm = 10, 2
	core := newElectionCore(t, cfg)
	for term := uint64(4); term <= 5; term++ {
		rd := expireElection(t, core)
		status := core.Status()
		wantState := HardState{Term: term, Vote: 1}
		if status.Role != Candidate || status.LeaderID != 0 || status.HardState != wantState || status.ElectionElapsed != 0 {
			t.Fatalf("unexpected campaign state: %+v", status)
		}
		if rd.HardState == nil || *rd.HardState != wantState || len(rd.Messages) != 2 {
			t.Fatalf("campaign must emit term/self-vote with requests: %+v", rd)
		}
		for i, msg := range rd.Messages {
			want := &raftv1.Message{Type: raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE,
				From: 1, To: uint64(i + 2), Term: term, LastLogIndex: 10, LastLogTerm: 2}
			if !proto.Equal(msg, want) {
				t.Fatalf("campaign request: got %v, want %v", msg, want)
			}
		}
	}
	before := core.Status()
	rd := stepElection(t, core, &raftv1.Message{Type: raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE_RESPONSE, From: 2, To: 1, Term: 4})
	if core.Status() != before || rd.HardState != nil || len(rd.Messages) != 0 {
		t.Fatal("stale response changed the current campaign")
	}
	stepElection(t, core, &raftv1.Message{Type: raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE_RESPONSE, From: 2, To: 1, Term: 5})
	if status := core.Status(); status.Role != Leader || status.LeaderID != 1 || status.HardState != (HardState{Term: 5, Vote: 1}) {
		t.Fatalf("self plus one peer must elect a leader: %+v", status)
	}
}

func TestVotesCountOnce(t *testing.T) {
	cfg := config()
	cfg.Voters = []uint64{1, 2, 3, 4, 5}
	core := newElectionCore(t, cfg)
	expireElection(t, core)
	for _, vote := range []struct {
		from   uint64
		reject bool
		role   Role
	}{
		{2, true, Candidate},
		{2, false, Candidate}, // a later response cannot replace the first
		{3, false, Candidate},
		{3, false, Candidate}, // duplicates cannot manufacture a majority
		{4, true, Candidate},
		{5, false, Leader},
	} {
		stepElection(t, core, &raftv1.Message{Type: raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE_RESPONSE,
			From: vote.from, To: 1, Term: 1, Reject: vote.reject})
		if core.Status().Role != vote.role {
			t.Fatalf("response from %d (reject=%v): got %s, want %s", vote.from, vote.reject, core.Status().Role, vote.role)
		}
	}
}

func TestRequestVoteRules(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		initialVote             uint64
		term, logIndex, logTerm uint64
		grant                   bool
		wantState               HardState
	}{
		{"equal log", 0, 4, 10, 2, true, HardState{4, 2}},
		{"longer log", 0, 4, 11, 2, true, HardState{4, 2}},
		{"shorter log", 0, 4, 9, 2, false, HardState{4, 0}},
		{"newer log term wins over length", 0, 4, 1, 3, true, HardState{4, 2}},
		{"older log term loses despite length", 0, 4, 100, 1, false, HardState{4, 0}},
		{"empty log is stale", 0, 4, 0, 0, false, HardState{4, 0}},
		{"repeat vote for same candidate", 2, 4, 10, 2, true, HardState{4, 2}},
		{"already voted for another", 3, 4, 10, 2, false, HardState{4, 3}},
		{"stale term", 0, 3, 100, 3, false, HardState{4, 0}},
		{"higher term clears old vote", 3, 5, 10, 2, true, HardState{5, 2}},
		{"higher term with stale log", 3, 5, 100, 1, false, HardState{5, 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config()
			cfg.HardState = HardState{Term: 4, Vote: tc.initialVote}
			cfg.LastLogIndex, cfg.LastLogTerm = 10, 2
			core := newElectionCore(t, cfg)
			for range 3 {
				if err := core.Tick(); err != nil {
					t.Fatal(err)
				}
			}
			rd := stepElection(t, core, &raftv1.Message{Type: raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE,
				From: 2, To: 1, Term: tc.term, LastLogIndex: tc.logIndex, LastLogTerm: tc.logTerm})
			status := core.Status()
			if status.HardState != tc.wantState || status.Role != Follower {
				t.Fatalf("unexpected vote state: %+v", status)
			}
			wantElapsed := 3
			if tc.grant || tc.term > 4 {
				wantElapsed = 0
			}
			if status.ElectionElapsed != wantElapsed {
				t.Fatalf("election elapsed: got %d, want %d", status.ElectionElapsed, wantElapsed)
			}
			want := &raftv1.Message{Type: raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE_RESPONSE,
				From: 1, To: 2, Term: tc.wantState.Term, Reject: !tc.grant}
			if len(rd.Messages) != 1 || !proto.Equal(rd.Messages[0], want) {
				t.Fatalf("vote response: got %v, want %v", rd.Messages, want)
			}
			if tc.wantState != cfg.HardState {
				if rd.HardState == nil || *rd.HardState != tc.wantState {
					t.Fatal("response omitted its dependent term/vote state")
				}
			} else if rd.HardState != nil {
				t.Fatal("unchanged election state was re-emitted")
			}
		})
	}
}

func TestHigherTermStepsDownEveryRole(t *testing.T) {
	for _, role := range []Role{Follower, Candidate, Leader} {
		for _, msgType := range []raftv1.MessageType{
			raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE,
			raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE_RESPONSE,
		} {
			t.Run(string(role)+"/"+msgType.String(), func(t *testing.T) {
				core := newElectionCore(t, config())
				if role != Follower {
					expireElection(t, core)
				}
				if role == Leader {
					stepElection(t, core, &raftv1.Message{Type: raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE_RESPONSE, From: 2, To: 1, Term: 1})
				}
				if core.Status().Role != role {
					t.Fatal("test failed to establish starting role")
				}
				term := core.Status().HardState.Term + 1
				rd := stepElection(t, core, &raftv1.Message{Type: msgType, From: 2, To: 1, Term: term})
				vote := uint64(0)
				if msgType == raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE {
					vote = 2
				}
				status := core.Status()
				if status.Role != Follower || status.LeaderID != 0 || status.HardState != (HardState{term, vote}) || status.ElectionElapsed != 0 {
					t.Fatalf("higher term failed to clear old leadership/campaign: %+v", status)
				}
				if rd.HardState == nil || *rd.HardState != status.HardState || len(core.votes) != 0 {
					t.Fatal("higher term failed to output new state or clear campaign votes")
				}
			})
		}
	}
}

func TestSplitVoteRetriesInNewTerm(t *testing.T) {
	var nodes [3]*Core
	var requests, responses []*raftv1.Message
	for i := range nodes {
		cfg := config()
		cfg.ID = uint64(i + 1)
		nodes[i] = newElectionCore(t, cfg)
		requests = append(requests, expireElection(t, nodes[i]).Messages...)
	}
	// Everyone has self-voted before any request is delivered.
	for _, msg := range requests {
		rd := stepElection(t, nodes[msg.To-1], msg)
		if len(rd.Messages) != 1 || !rd.Messages[0].Reject {
			t.Fatal("split-vote candidate granted a second vote")
		}
		responses = append(responses, rd.Messages...)
	}
	for _, msg := range responses {
		stepElection(t, nodes[msg.To-1], msg)
	}
	for _, core := range nodes {
		if core.Status().Role != Candidate || core.Status().HardState.Term != 1 {
			t.Fatal("a split election produced a leader")
		}
	}
	// Only node 1's next deadline expires; both peers adopt term 2 and vote.
	requests = expireElection(t, nodes[0]).Messages
	for _, msg := range requests {
		rd := stepElection(t, nodes[msg.To-1], msg)
		for _, response := range rd.Messages {
			stepElection(t, nodes[0], response)
		}
	}
	if status := nodes[0].Status(); status.Role != Leader || status.HardState.Term != 2 {
		t.Fatalf("retry failed to elect node 1: %+v", status)
	}
}

func TestTimeoutSequenceIsSeededAndBounded(t *testing.T) {
	for _, seed := range []uint64{0, 1, 42} {
		cfg := config()
		cfg.Seed, cfg.ElectionTicks, cfg.HeartbeatTicks = seed, 7, 1
		first, replay := newElectionCore(t, cfg), newElectionCore(t, cfg)
		deadlines := make(map[int]bool)
		for range 20 {
			status := first.Status()
			if status != replay.Status() || status.ElectionTimeout < 7 || status.ElectionTimeout >= 14 {
				t.Fatalf("seed %d produced a different or out-of-range timeout: %+v", seed, status)
			}
			deadlines[status.ElectionTimeout] = true
			a, b := expireElection(t, first), expireElection(t, replay)
			if *a.HardState != *b.HardState || len(a.Messages) != len(b.Messages) {
				t.Fatal("same seed produced different campaigns")
			}
			for i := range a.Messages {
				if !proto.Equal(a.Messages[i], b.Messages[i]) {
					t.Fatal("same seed produced different vote requests")
				}
			}
		}
		if len(deadlines) < 2 {
			t.Fatalf("seed %d never randomized its deadline", seed)
		}
	}
}

func TestSingleVoterAndTermExhaustion(t *testing.T) {
	cfg := config()
	cfg.Voters = []uint64{1}
	core := newElectionCore(t, cfg)
	rd := expireElection(t, core)
	if status := core.Status(); status.Role != Leader || status.LeaderID != 1 || status.HardState != (HardState{1, 1}) || len(rd.Messages) != 0 {
		t.Fatalf("single voter failed to self-elect: %+v", status)
	}
	for range 2 * cfg.ElectionTicks {
		if err := core.Tick(); err != nil {
			t.Fatal(err)
		}
		takeReady(t, core)
	}
	if core.Status().Role != Leader || core.Status().HardState.Term != 1 {
		t.Fatal("leader campaigned again on its election timeout")
	}
	cfg.HardState.Term = ^uint64(0)
	core = newElectionCore(t, cfg)
	for range core.Status().ElectionTimeout - 1 {
		if err := core.Tick(); err != nil {
			t.Fatal(err)
		}
	}
	before := core.Status()
	if err := core.Tick(); err == nil || core.Status() != before {
		t.Fatal("term exhaustion must return an error without wrapping or changing state")
	}
}
