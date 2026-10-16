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
	check := func(rd Ready) {
		t.Helper()
		if rd.HardState != nil || len(rd.Messages) != 2 {
			t.Fatalf("expected two heartbeats without unchanged hard state: %+v", rd)
		}
		for i, msg := range rd.Messages {
			want := &raftv1.Message{Type: raftv1.MessageType_MESSAGE_TYPE_HEARTBEAT, From: 1, To: uint64(i + 2), Term: 1}
			if !proto.Equal(msg, want) {
				t.Fatalf("heartbeat: got %v, want %v", msg, want)
			}
		}
	}
	check(stepElection(t, core, &raftv1.Message{Type: raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE_RESPONSE, From: 2, To: 1, Term: 1}))
	for range 3 {
		for tick := 1; tick <= cfg.HeartbeatTicks; tick++ {
			if err := core.Tick(); err != nil {
				t.Fatal(err)
			}
			rd := takeReady(t, core)
			if tick == cfg.HeartbeatTicks {
				check(rd)
			} else if rd.HardState != nil || len(rd.Messages) != 0 {
				t.Fatal("heartbeat emitted before its interval")
			}
		}
	}
	if status := core.Status(); status.Role != Leader || status.HardState.Term != 1 {
		t.Fatalf("heartbeat ticks disrupted leadership: %+v", status)
	}
}

func TestHeartbeatTermAndRoleHandling(t *testing.T) {
	for _, role := range []Role{Follower, Candidate, Leader} {
		for _, delta := range []int{-1, 0, 1} {
			t.Run(fmt.Sprintf("%s/term_delta_%d", role, delta), func(t *testing.T) {
				cfg := config()
				cfg.HardState = HardState{Term: 2, Vote: 3}
				core := newElectionCore(t, cfg)
				if role != Follower {
					expireElection(t, core)
				}
				if role == Leader {
					stepElection(t, core, &raftv1.Message{Type: raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE_RESPONSE, From: 2, To: 1, Term: 3})
				}
				for range 3 {
					if err := core.Tick(); err != nil {
						t.Fatal(err)
					}
					takeReady(t, core)
				}
				before := core.Status()
				term := uint64(int(before.HardState.Term) + delta)
				rd := stepElection(t, core, &raftv1.Message{Type: raftv1.MessageType_MESSAGE_TYPE_HEARTBEAT, From: 2, To: 1, Term: term})
				status := core.Status()
				if len(rd.Messages) != 0 {
					t.Fatal("heartbeats must not generate acknowledgements in M1")
				}
				if delta < 0 {
					if status != before || rd.HardState != nil {
						t.Fatal("stale heartbeat changed state or reset the election timer")
					}
					return
				}
				wantState := before.HardState
				if delta > 0 {
					wantState = HardState{Term: term}
					if rd.HardState == nil || *rd.HardState != wantState {
						t.Fatal("higher-term heartbeat did not emit the new term")
					}
				} else if rd.HardState != nil {
					t.Fatal("same-term heartbeat rewrote hard state")
				}
				if status.Role != Follower || status.LeaderID != 2 || status.HardState != wantState ||
					status.ElectionElapsed != 0 || len(core.votes) != 0 {
					t.Fatalf("heartbeat did not establish leader contact: %+v", status)
				}
				if delta == 0 {
					rd = stepElection(t, core, &raftv1.Message{Type: raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE, From: 2, To: 1, Term: term})
					if len(rd.Messages) != 1 || !rd.Messages[0].Reject {
						t.Fatal("same-term heartbeat allowed a second vote")
					}
				}
			})
		}
	}
}

func TestHeartbeatValidationAndStaleTimeout(t *testing.T) {
	cfg := config()
	cfg.HardState.Term = 2
	core := newElectionCore(t, cfg)
	before := core.Status()
	for _, msg := range []*raftv1.Message{
		{Type: raftv1.MessageType_MESSAGE_TYPE_HEARTBEAT, From: 2, To: 1},
		{Type: raftv1.MessageType_MESSAGE_TYPE_HEARTBEAT, From: 9, To: 1, Term: 99},
		{Type: raftv1.MessageType_MESSAGE_TYPE_HEARTBEAT, From: 2, To: 3, Term: 99},
		{Type: raftv1.MessageType_MESSAGE_TYPE_HEARTBEAT, From: 2, To: 1, Term: 99, Reject: true},
		{Type: raftv1.MessageType_MESSAGE_TYPE_HEARTBEAT, From: 2, To: 1, Term: 99, LastLogIndex: 1},
		{Type: raftv1.MessageType_MESSAGE_TYPE_HEARTBEAT, From: 2, To: 1, Term: 99, LastLogTerm: 1},
	} {
		if err := core.Step(msg); err == nil || core.Status() != before {
			t.Fatalf("invalid heartbeat changed state or was accepted: %v, err=%v", msg, err)
		}
	}
	for range before.ElectionTimeout - 1 {
		if err := core.Tick(); err != nil {
			t.Fatal(err)
		}
	}
	stepElection(t, core, &raftv1.Message{Type: raftv1.MessageType_MESSAGE_TYPE_HEARTBEAT, From: 2, To: 1, Term: 1})
	if err := core.Tick(); err != nil {
		t.Fatal(err)
	}
	rd := takeReady(t, core)
	if core.Status().Role != Candidate || rd.HardState == nil || rd.HardState.Term != 3 {
		t.Fatal("stale heartbeat suppressed an election")
	}
}

// Direct delivery only: fault scheduling and virtual network queues belong to
// the simulator in subset 4. IDs in these tests are contiguous and start at 1.
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
