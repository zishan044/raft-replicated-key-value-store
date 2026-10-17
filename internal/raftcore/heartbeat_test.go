package raftcore

import (
	"fmt"
	"testing"

	raftv1 "github.com/zishan044/raft-replicated-key-value-store.git/pkg/api/raft/v1"
	"google.golang.org/protobuf/proto"
)

func TestHeartbeatTiming(t *testing.T) {
	cfg := config()
	cfg.HeartbeatTicks = 3
	core := newElectionCore(t, cfg)
	expireElection(t, core)
	rd := stepElection(t, core, &raftv1.Message{Type: raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE_RESPONSE, From: 2, To: 1, Term: 1})
	if len(rd.Entries) != 1 || rd.Entries[0].Type != raftv1.EntryType_ENTRY_TYPE_NO_OP || len(rd.Messages) != 2 {
		t.Fatalf("election must append and broadcast a no-op: %+v", rd)
	}
	for _, msg := range rd.Messages {
		if msg.Type != raftv1.MessageType_MESSAGE_TYPE_APPEND_ENTRIES || msg.PrevLogIndex != 0 || !equalEntries(msg.Entries, rd.Entries) {
			t.Fatalf("election did not send its no-op: %v", msg)
		}
	}
	// Catch both peers up so idle ticks send empty appends rather than retries.
	for _, peer := range []uint64{2, 3} {
		stepElection(t, core, &raftv1.Message{
			Type: raftv1.MessageType_MESSAGE_TYPE_APPEND_ENTRIES_RESPONSE,
			From: peer, To: 1, Term: 1, MatchIndex: 1,
		})
	}
	for range 3 {
		for tick := 1; tick <= cfg.HeartbeatTicks; tick++ {
			if err := core.Tick(); err != nil {
				t.Fatal(err)
			}
			rd := takeReady(t, core)
			if tick != cfg.HeartbeatTicks {
				if len(rd.Messages) != 0 {
					t.Fatal("heartbeat emitted before its interval")
				}
				continue
			}
			if rd.HardState != nil || len(rd.Entries) != 0 || len(rd.Messages) != 2 {
				t.Fatalf("expected two idle appends: %+v", rd)
			}
			for i, msg := range rd.Messages {
				want := &raftv1.Message{
					Type: raftv1.MessageType_MESSAGE_TYPE_APPEND_ENTRIES,
					From: 1, To: uint64(i + 2), Term: 1,
					PrevLogIndex: 1, PrevLogTerm: 1, LeaderCommit: 1,
				}
				if !proto.Equal(msg, want) {
					t.Fatalf("idle append: got %v, want %v", msg, want)
				}
			}
		}
	}
}

func TestEmptyAppendPreservesVoteInTerm(t *testing.T) {
	core := newElectionCore(t, config())
	expireElection(t, core)
	rd := stepElection(t, core, appendRequest(1, 0, 0))
	if core.Status().Role != Follower || core.Status().HardState.Vote != 1 || len(rd.Messages) != 1 || rd.Messages[0].Reject {
		t.Fatal("same-term leader contact did not preserve the candidate's vote")
	}
	rd = stepElection(t, core, &raftv1.Message{Type: raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE, From: 2, To: 1, Term: 1})
	if len(rd.Messages) != 1 || !rd.Messages[0].Reject {
		t.Fatal("same-term leader contact allowed a second vote")
	}
}

func TestStaleEmptyAppendDoesNotSuppressElection(t *testing.T) {
	cfg := config()
	cfg.HardState.Term = 2
	core := newElectionCore(t, cfg)
	for range core.Status().ElectionTimeout - 1 {
		if err := core.Tick(); err != nil {
			t.Fatal(err)
		}
	}
	stepElection(t, core, appendRequest(1, 0, 0))
	if err := core.Tick(); err != nil {
		t.Fatal(err)
	}
	rd := takeReady(t, core)
	if core.Status().Role != Candidate || rd.HardState == nil || rd.HardState.Term != 3 {
		t.Fatal("stale empty append suppressed an election")
	}
}

// Direct delivery only: fault scheduling and virtual network queues belong to
// the simulator. IDs in these tests are contiguous and start at 1.
func deliverMessages(t *testing.T, nodes []*Core, messages []*raftv1.Message, isolated uint64) {
	t.Helper()
	for i := 0; i < len(messages); i++ {
		msg := messages[i]
		if msg.From == isolated || msg.To == isolated {
			continue
		}
		rd := stepElection(t, nodes[msg.To-1], msg)
		messages = append(messages, rd.Messages...)
	}
}

func tickConnectedCores(t *testing.T, nodes []*Core, isolated uint64) {
	t.Helper()
	var messages []*raftv1.Message
	for _, core := range nodes {
		if err := core.Tick(); err != nil {
			t.Fatal(err)
		}
		messages = append(messages, takeReady(t, core).Messages...)
	}
	deliverMessages(t, nodes, messages, isolated)
}

func TestConnectedLeadershipAndIsolation(t *testing.T) {
	for _, size := range []int{3, 5} {
		t.Run(fmt.Sprintf("%d_nodes", size), func(t *testing.T) {
			voters := make([]uint64, size)
			for i := range voters {
				voters[i] = uint64(i + 1)
			}
			nodes := make([]*Core, size)
			for i := range nodes {
				cfg := config()
				cfg.ID, cfg.Seed, cfg.Voters = voters[i], voters[i], voters
				nodes[i] = newElectionCore(t, cfg)
			}
			deliverMessages(t, nodes, expireElection(t, nodes[0]).Messages, 0)
			for range 100 {
				tickConnectedCores(t, nodes, 0)
				for i, core := range nodes {
					wantRole := Follower
					if i == 0 {
						wantRole = Leader
					}
					if status := core.Status(); status.Role != wantRole || status.LeaderID != 1 || status.HardState.Term != 1 {
						t.Fatalf("healthy cluster lost its leader: %+v", status)
					}
				}
			}
			var newLeader uint64
			for tick := 0; tick < 200 && newLeader == 0; tick++ {
				tickConnectedCores(t, nodes, 1)
				for _, core := range nodes[1:] {
					if status := core.Status(); status.Role == Leader {
						newLeader = status.ID
					}
				}
			}
			if newLeader == 0 {
				t.Fatal("majority did not elect a leader within 200 ticks")
			}
			term := nodes[newLeader-1].Status().HardState.Term
			if term <= 1 || nodes[0].Status().Role != Leader {
				t.Fatal("majority must elect in a newer term; isolated leader awaits higher-term contact")
			}
			for range 40 {
				tickConnectedCores(t, nodes, 1)
				for _, core := range nodes[1:] {
					if status := core.Status(); status.LeaderID != newLeader || status.HardState.Term != term {
						t.Fatalf("majority leadership did not stabilize: %+v", status)
					}
				}
			}
			for range 40 {
				tickConnectedCores(t, nodes, 0)
			}
			for _, core := range nodes {
				wantRole := Follower
				if core.Status().ID == newLeader {
					wantRole = Leader
				}
				if status := core.Status(); status.Role != wantRole || status.LeaderID != newLeader || status.HardState.Term != term {
					t.Fatalf("healed cluster did not converge: %+v", status)
				}
			}
		})
	}
}
