package raftcore

import (
	"errors"
	"slices"
	"testing"

	raftv1 "github.com/zishan044/raft-replicated-key-value-store.git/pkg/api/raft/v1"
)

func TestOrderedApplyRetryNoOpsAndAdvance(t *testing.T) {
	cfg := config()
	cfg.HardState.Term = 3
	cfg.Entries = []*raftv1.Entry{
		{Index: 1, Term: 1, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL, Data: []byte("A")},
		{Index: 2, Term: 2, Type: raftv1.EntryType_ENTRY_TYPE_NO_OP},
		{Index: 3, Term: 2, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL, Data: []byte("B")},
		{Index: 4, Term: 3, Type: raftv1.EntryType_ENTRY_TYPE_NO_OP},
		{Index: 5, Term: 3, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL, Data: []byte("C")},
	}
	node := &paperNode{core: newElectionCore(t, cfg)}
	commit := appendRequest(3, 5, 3)
	commit.LeaderCommit = 5
	if err := node.core.Step(commit); err != nil {
		t.Fatal(err)
	}
	rd := node.core.Ready()
	if !equalEntries(rd.CommittedEntries, cfg.Entries) || node.core.Status().AppliedIndex != 0 {
		t.Fatal("committed batch was omitted, reordered or acknowledged early")
	}
	// Simulate an apply consumer stopping partway through an outstanding batch.
	// Its own applied index records completed work; Advance is still withheld.
	node.apply(t, rd.CommittedEntries[:3])
	if !slices.Equal(node.commands, []string{"A", "B"}) || node.core.Status().AppliedIndex != 0 {
		t.Fatal("no-op changed application state or partial application acknowledged the batch")
	}
	rd.CommittedEntries[4].Data[0] = 'x'
	retry := node.core.Ready()
	if !equalEntries(retry.CommittedEntries, cfg.Entries) || !errors.Is(node.core.Step(commit), ErrReadyPending) {
		t.Fatal("outstanding apply batch was not stable or accepted another input")
	}
	// Resume after the last completed index, rather than executing A and B twice.
	node.apply(t, retry.CommittedEntries[len(node.applied):])
	if !slices.Equal(node.commands, []string{"A", "B", "C"}) || len(node.applied) != 5 || node.core.Status().AppliedIndex != 0 {
		t.Fatal("retry duplicated a command or skipped a no-op position")
	}
	if err := node.core.Advance(); err != nil {
		t.Fatal(err)
	}
	if node.core.Status().AppliedIndex != 5 {
		t.Fatal("Advance did not acknowledge the entire applied batch")
	}
	rd = node.core.Ready()
	if rd.HardState != nil || len(rd.Entries) != 0 || len(rd.Messages) != 0 || len(rd.CommittedEntries) != 0 ||
		!errors.Is(node.core.Advance(), ErrNoReady) {
		t.Fatal("acknowledged batch was emitted or acknowledged twice")
	}
	if err := node.core.Step(commit); err != nil {
		t.Fatal(err)
	}
	if rd := node.ready(t); len(rd.CommittedEntries) != 0 {
		t.Fatal("duplicate commit advertisement reapplied entries")
	}
	pending := appendRequest(3, 5, 3, &raftv1.Entry{
		Index: 6, Term: 3, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL, Data: []byte("D"),
	})
	pending.LeaderCommit = 5
	if err := node.core.Step(pending); err != nil {
		t.Fatal(err)
	}
	if rd := node.ready(t); len(rd.Entries) != 1 || len(rd.CommittedEntries) != 0 || len(node.commands) != 3 {
		t.Fatal("retaining an uncommitted entry executed its command")
	}
	last := appendRequest(3, 6, 3)
	last.LeaderCommit = 6
	if err := node.core.Step(last); err != nil {
		t.Fatal(err)
	}
	rd = node.ready(t)
	if len(rd.CommittedEntries) != 1 || rd.CommittedEntries[0].Index != 6 || len(node.applied) != 6 ||
		node.core.Status().AppliedIndex != 6 || !slices.Equal(node.commands, []string{"A", "B", "C", "D"}) {
		t.Fatal("later commitment did not apply exactly the newly committed suffix")
	}
}
