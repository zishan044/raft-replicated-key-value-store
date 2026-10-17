package sim

import (
	"fmt"
	"strings"
	"testing"

	"github.com/zishan044/raft-replicated-key-value-store.git/internal/raftcore"
	raftv1 "github.com/zishan044/raft-replicated-key-value-store.git/pkg/api/raft/v1"
)

func invariantView(id uint64, role raftcore.Role, term uint64, terms ...uint64) nodeView {
	view := nodeView{status: raftcore.Status{ID: id, Role: role, HardState: raftcore.HardState{Term: term}}}
	for i, entryTerm := range terms {
		view.entries = append(view.entries, &raftv1.Entry{
			Index: uint64(i) + 1, Term: entryTerm, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL,
			Data: []byte(fmt.Sprintf("index=%d term=%d", i+1, entryTerm)),
		})
		view.status.LastLogIndex, view.status.LastLogTerm = uint64(i)+1, entryTerm
	}
	return view
}

func TestLogInvariantCheckersDetectInvalidFixtures(t *testing.T) {
	leader := invariantView(1, raftcore.Leader, 3, 1, 3)
	truncated := invariantView(1, raftcore.Leader, 3, 1)
	rewritten := invariantView(1, raftcore.Leader, 3, 1, 3)
	rewritten.entries[0].Data = []byte("rewritten")
	committed := invariantView(1, raftcore.Leader, 1, 1)
	committed.status.HardState.Commit = 1
	for _, tc := range []struct {
		name  string
		views []nodeView
		want  string
	}{
		{"election safety remembers a deposed leader", []nodeView{
			leader, invariantView(1, raftcore.Follower, 4, 1, 3), invariantView(2, raftcore.Leader, 3, 1, 3),
		}, "election safety"},
		{"leader cannot truncate", []nodeView{leader, truncated}, "leader append-only"},
		{"leader cannot overwrite", []nodeView{leader, rewritten}, "leader append-only"},
		{"matching endpoint needs matching prefix", []nodeView{
			invariantView(1, raftcore.Follower, 3, 1, 3), invariantView(2, raftcore.Follower, 3, 2, 3),
		}, "log matching"},
		{"later leader lacks a committed entry", []nodeView{
			committed, invariantView(2, raftcore.Leader, 2, 2),
		}, "leader completeness"},
		{"late commitment checks deposed higher-term leaders", []nodeView{
			invariantView(2, raftcore.Leader, 2, 2), invariantView(2, raftcore.Follower, 3, 2), committed,
		}, "leader completeness"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newSim(t, 42)
			for i, view := range tc.views {
				err := s.checkLogs(view, true)
				if i < len(tc.views)-1 {
					if err != nil {
						t.Fatalf("fixture setup failed: %v", err)
					}
				} else if err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("planted violation not detected: got %v, want %s", err, tc.want)
				}
			}
		})
	}
}

func TestStateMachineSafetyRemembersApplication(t *testing.T) {
	s := newSim(t, 42)
	first := invariantView(1, raftcore.Follower, 1, 1).entries[0]
	if err := s.checkApplied(1, first); err != nil {
		t.Fatal(err)
	}
	if err := s.checkApplied(2, first); err != nil {
		t.Fatal("identical commands applied by different nodes must be accepted")
	}
	different := invariantView(2, raftcore.Follower, 1, 1).entries[0]
	different.Data = []byte("different command in the same term")
	if err := s.checkApplied(2, different); err == nil || !strings.Contains(err.Error(), "state machine safety") {
		t.Fatalf("planted application conflict not detected: %v", err)
	}
}

func TestOldLeaderAndLaggingFollowerMayLackNewerCommitments(t *testing.T) {
	s := newSim(t, 42)
	old := invariantView(1, raftcore.Leader, 1, 1)
	newer := invariantView(2, raftcore.Leader, 2, 1, 2)
	newer.status.HardState.Commit = 2
	for _, view := range []nodeView{old, newer, old, invariantView(3, raftcore.Follower, 3)} {
		if err := s.checkLogs(view, true); err != nil {
			t.Fatalf("checker rejected a valid older leader or lagging follower: %v", err)
		}
	}
}

func TestInvariantFailureStopsProposalAndTickInputs(t *testing.T) {
	s := newSim(t, 42)
	// An actual elected leader will commit/apply its no-op at index 1. Plant a
	// conflicting application history to verify the checker is wired into finish.
	s.applied = invariantView(1, raftcore.Follower, 99, 99).entries
	err := s.Run(80)
	if err == nil || !strings.Contains(err.Error(), "state machine safety") || !strings.Contains(err.Error(), "seed=42 tick=") {
		t.Fatalf("simulation did not stop on the planted application history: %v", err)
	}
	trace, now := s.Trace(), s.Now()
	_, _, proposalErr := s.Propose(1, []byte("after failure"))
	if proposalErr != err || s.Tick() != err || s.Run(1) != err || s.Trace() != trace || s.Now() != now {
		t.Fatal("simulation accepted input after its invariant failed")
	}
}
