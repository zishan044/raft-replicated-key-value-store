package raftcore

import (
	"slices"
	"testing"

	raftv1 "github.com/zishan044/raft-replicated-key-value-store.git/pkg/api/raft/v1"
	"google.golang.org/protobuf/proto"
)

// These fixtures use the log shapes in Figures 7 and 8 of
// https://raft.github.io/raft.pdf (extended version, pages 7 and 8).
// Historical logs are seeded; holding messages and stopping ticks models
// unavailability without adding crash recovery or changing election behavior.
type paperNode struct {
	core     *Core
	applied  []*raftv1.Entry
	commands []string
}

func (n *paperNode) apply(t *testing.T, entries []*raftv1.Entry) {
	t.Helper()
	for _, entry := range entries {
		if entry.Index != uint64(len(n.applied))+1 || entry.Index > n.core.Status().HardState.Commit {
			t.Fatalf("node %d: application out of order or beyond commit: %v", n.core.id, entry)
		}
		n.applied = append(n.applied, entry)
		switch entry.Type {
		case raftv1.EntryType_ENTRY_TYPE_NORMAL:
			n.commands = append(n.commands, string(entry.Data))
		case raftv1.EntryType_ENTRY_TYPE_NO_OP:
		default:
			t.Fatalf("unexpected applied entry type: %v", entry.Type)
		}
	}
}

func (n *paperNode) ready(t *testing.T) Ready {
	t.Helper()
	rd := n.core.Ready()
	n.apply(t, rd.CommittedEntries)
	if rd.HardState != nil || len(rd.Entries) != 0 || len(rd.Messages) != 0 || len(rd.CommittedEntries) != 0 {
		if err := n.core.Advance(); err != nil {
			t.Fatal(err)
		}
	}
	if n.core.Status().AppliedIndex != uint64(len(n.applied)) {
		t.Fatal("core and recording state machine disagree on application")
	}
	return rd
}

func paperNodes(t *testing.T, term, commit uint64, logs ...[]*raftv1.Entry) []*paperNode {
	t.Helper()
	voters := make([]uint64, len(logs))
	for i := range voters {
		voters[i] = uint64(i + 1)
	}
	nodes := make([]*paperNode, len(logs))
	for i, entries := range logs {
		cfg := config()
		cfg.ID, cfg.Voters, cfg.HardState, cfg.Entries = voters[i], voters, HardState{Term: term, Commit: commit}, entries
		core, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		nodes[i] = &paperNode{core: core}
		nodes[i].ready(t)
	}
	return nodes
}

func paperStep(t *testing.T, nodes []*paperNode, msg *raftv1.Message) Ready {
	t.Helper()
	n := nodes[msg.To-1]
	if err := n.core.Step(msg); err != nil {
		t.Fatalf("deliver %v: %v", msg, err)
	}
	return n.ready(t)
}

func paperDeliver(t *testing.T, nodes []*paperNode, messages []*raftv1.Message, unavailable ...uint64) {
	t.Helper()
	for i := 0; i < len(messages); i++ {
		if i >= 1000 {
			t.Fatal("paper scenario failed to drain its message queue")
		}
		msg := messages[i]
		if slices.Contains(unavailable, msg.From) || slices.Contains(unavailable, msg.To) {
			continue
		}
		messages = append(messages, paperStep(t, nodes, msg).Messages...)
	}
}

// Deliver real vote requests and responses only to the selected voters. Return
// the original requests and hold the newly elected leader's append broadcasts.
func paperCampaign(t *testing.T, nodes []*paperNode, candidate uint64, voters ...uint64) (requests, appends []*raftv1.Message) {
	t.Helper()
	core := nodes[candidate-1].core
	if core.Status().Role == Leader {
		t.Fatal("campaign fixture must first depose an existing leader")
	}
	requests = expireElection(t, core).Messages // no apply output before a multi-node election wins
	for _, request := range requests {
		if slices.Contains(voters, request.To) {
			for _, response := range paperStep(t, nodes, request).Messages {
				appends = append(appends, paperStep(t, nodes, response).Messages...)
			}
		}
	}
	return requests, appends
}

func paperMessageTo(t *testing.T, messages []*raftv1.Message, id uint64) *raftv1.Message {
	t.Helper()
	for _, msg := range messages {
		if msg.To == id {
			return msg
		}
	}
	t.Fatalf("missing captured message to node %d", id)
	return nil
}

func assertPaperState(t *testing.T, node *paperNode, log []*raftv1.Entry, commit uint64, commands []string) {
	t.Helper()
	if !equalEntries(node.core.log, log) || node.core.Status().HardState.Commit != commit ||
		node.core.Status().AppliedIndex != commit || !equalEntries(node.applied, log[:commit]) || !slices.Equal(node.commands, commands) {
		t.Fatalf("node %d: wrong log/commit/application: status=%+v log=%v applied=%v commands=%v",
			node.core.id, node.core.Status(), node.core.log, node.applied, node.commands)
	}
}

func TestPaperFigure7FollowerLogShapes(t *testing.T) {
	leaderLog := logWithTerms(1, 1, 1, 4, 4, 5, 5, 6, 6, 6)
	want := append(cloneEntries(leaderLog), &raftv1.Entry{Index: 11, Term: 8, Type: raftv1.EntryType_ENTRY_TYPE_NO_OP})
	var commands []string
	for _, entry := range leaderLog {
		commands = append(commands, string(entry.Data))
	}
	for _, tc := range []struct {
		name string
		log  []*raftv1.Entry
	}{
		{"a_missing_one", logWithTerms(1, 1, 1, 4, 4, 5, 5, 6, 6)},
		{"b_missing_multiple_terms", logWithTerms(1, 1, 1, 4)},
		{"c_extra_same_term", logWithTerms(1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 6)},
		{"d_extra_later_term", logWithTerms(1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 7, 7)},
		{"e_missing_and_extra", logWithTerms(1, 1, 1, 4, 4, 4, 4)},
		{"f_multiple_conflicting_terms", logWithTerms(1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Only the common first three entries are initially committed. Voter
			// 3 can elect node 1 even when follower (d)'s newer log denies a vote.
			nodes := paperNodes(t, 7, 3, leaderLog, tc.log, leaderLog)
			_, appends := paperCampaign(t, nodes, 1, 3)
			if nodes[0].core.Status().Role != Leader || nodes[0].core.Status().HardState.Term != 8 {
				t.Fatal("Figure 7 fixture did not elect the term-8 leader")
			}
			paperDeliver(t, nodes, appends)
			for _, node := range nodes {
				assertPaperState(t, node, want, 11, commands)
			}
		})
	}
}

// Build Figure 8(a)-(c) through actual elections and follower responses. No
// no-op is disabled: S5's term-3 no-op is the conflicting entry at index 2,
// and S1's term-4 no-op occupies index 3 as in panel (c).
func figure8AtC(t *testing.T) (nodes []*paperNode, term4Votes, pendingNoOps []*raftv1.Message) {
	t.Helper()
	nodes = paperNodes(t, 2, 1,
		logWithTerms(1, 2), logWithTerms(1, 2), logWithTerms(1), logWithTerms(1), logWithTerms(1))
	term3Votes, held := paperCampaign(t, nodes, 5, 3, 4)
	if nodes[4].core.Status().Role != Leader || len(held) != 4 {
		t.Fatal("Figure 8(b): S5 was not elected by S3, S4 and itself")
	}
	wantS5 := append(logWithTerms(1), &raftv1.Entry{Index: 2, Term: 3, Type: raftv1.EntryType_ENTRY_TYPE_NO_OP})
	assertPaperState(t, nodes[4], wantS5, 1, []string{"index=1 term=1"})
	// A delayed term-3 vote request lets the returning S1 learn the term before
	// its next timeout. S1 denies this stale-log candidate, then campaigns at 4.
	paperStep(t, nodes, paperMessageTo(t, term3Votes, 1))
	term4Votes, held = paperCampaign(t, nodes, 1, 2, 3)
	if nodes[0].core.Status().Role != Leader || nodes[0].core.Status().HardState.Term != 4 {
		t.Fatal("Figure 8(c): S1 was not elected at term 4")
	}
	// Full-suffix batching sends the no-op alongside the old entry. The paper's
	// intermediate state needs a valid prefix-only batch: hold the captured
	// no-op back while real follower replies acknowledge only through index 2.
	toS2 := proto.Clone(paperMessageTo(t, held, 2)).(*raftv1.Message)
	toS2.Entries = nil // prevIndex=2 proves S2 already holds the old entry
	ack := paperStep(t, nodes, toS2).Messages[0]
	pendingNoOps = append(pendingNoOps, paperStep(t, nodes, ack).Messages...)
	reject := paperStep(t, nodes, paperMessageTo(t, held, 3)).Messages[0]
	retry := paperStep(t, nodes, reject).Messages[0]
	if retry.PrevLogIndex != 1 || len(retry.Entries) != 2 || retry.Entries[1].Type != raftv1.EntryType_ENTRY_TYPE_NO_OP {
		t.Fatal("Figure 8(c): missing the old entry followed by the election no-op")
	}
	toS3 := proto.Clone(retry).(*raftv1.Message)
	toS3.Entries = toS3.Entries[:1]
	ack = paperStep(t, nodes, toS3).Messages[0]
	pendingNoOps = append(pendingNoOps, paperStep(t, nodes, ack).Messages...)
	wantS1 := append(logWithTerms(1, 2), &raftv1.Entry{Index: 3, Term: 4, Type: raftv1.EntryType_ENTRY_TYPE_NO_OP})
	assertPaperState(t, nodes[0], wantS1, 1, []string{"index=1 term=1"})
	for _, id := range []uint64{2, 3} {
		assertPaperState(t, nodes[id-1], logWithTerms(1, 2), 1, []string{"index=1 term=1"})
		if nodes[0].core.progress[id].match != 2 {
			t.Fatal("Figure 8(c): old-term entry does not have a confirmed majority")
		}
	}
	assertPaperState(t, nodes[3], logWithTerms(1), 1, []string{"index=1 term=1"})
	assertPaperState(t, nodes[4], wantS5, 1, []string{"index=1 term=1"})
	return nodes, term4Votes, pendingNoOps
}

func TestPaperFigure8OldTermMajorityCanBeOverwritten(t *testing.T) {
	nodes, term4Votes, _ := figure8AtC(t)
	// S1 is now unavailable. S5 learns term 4 from S1's delayed vote request,
	// steps down, then wins term 5 with votes from S2 and S3 plus its own vote.
	paperStep(t, nodes, paperMessageTo(t, term4Votes, 5))
	_, appends := paperCampaign(t, nodes, 5, 2, 3)
	if nodes[4].core.Status().Role != Leader || nodes[4].core.Status().HardState.Term != 5 {
		t.Fatal("Figure 8(d): S5 could not win after old-term majority replication")
	}
	paperDeliver(t, nodes, appends, 1)
	want := append(logWithTerms(1),
		&raftv1.Entry{Index: 2, Term: 3, Type: raftv1.EntryType_ENTRY_TYPE_NO_OP},
		&raftv1.Entry{Index: 3, Term: 5, Type: raftv1.EntryType_ENTRY_TYPE_NO_OP})
	for _, node := range nodes[1:] {
		assertPaperState(t, node, want, 3, []string{"index=1 term=1"})
	}
	if nodes[0].core.Status().HardState.Commit != 1 || len(nodes[0].applied) != 1 {
		t.Fatal("unavailable S1 applied its uncommitted old-term entry")
	}
	// Heal S1 and let an ordinary heartbeat trigger its conflict repair.
	for range config().HeartbeatTicks {
		if err := nodes[4].core.Tick(); err != nil {
			t.Fatal(err)
		}
	}
	paperDeliver(t, nodes, nodes[4].ready(t).Messages)
	for _, node := range nodes {
		assertPaperState(t, node, want, 3, []string{"index=1 term=1"})
	}
}

func TestPaperFigure8CurrentTermCommitProtectsPrefix(t *testing.T) {
	nodes, term4Votes, pendingNoOps := figure8AtC(t)
	// Deliver the genuine held no-ops to S2 and S3. Their current-term majority
	// commits index 3 and the older entry at 2. Keep S4 and S5 unavailable.
	paperDeliver(t, nodes, pendingNoOps, 4, 5)
	committed := append(logWithTerms(1, 2), &raftv1.Entry{Index: 3, Term: 4, Type: raftv1.EntryType_ENTRY_TYPE_NO_OP})
	commands := []string{"index=1 term=1", "index=2 term=2"}
	for _, node := range nodes[:3] {
		assertPaperState(t, node, committed, 3, commands)
	}
	// Even with S1 unavailable, S5 can obtain only its own and S4's votes.
	// S2 and S3 reject its stale log, preserving their commit and applied index.
	paperStep(t, nodes, paperMessageTo(t, term4Votes, 5))
	_, appends := paperCampaign(t, nodes, 5, 2, 3, 4)
	if nodes[4].core.Status().Role != Candidate || nodes[4].core.Status().HardState.Term != 5 || len(appends) != 0 {
		t.Fatal("Figure 8(e): stale-log S5 won an election after current-term commitment")
	}
	if nodes[4].core.votes[2] || nodes[4].core.votes[3] || !nodes[4].core.votes[4] {
		t.Fatal("Figure 8(e): election failed for an unexpected reason")
	}
	for _, node := range nodes[:3] {
		assertPaperState(t, node, committed, 3, commands)
	}
	// An eligible later leader contains the committed prefix before replicating
	// anything. Its normal no-op then lets all five nodes converge after healing.
	_, appends = paperCampaign(t, nodes, 2, 3, 4)
	if nodes[1].core.Status().Role != Leader || nodes[1].core.Status().HardState.Term != 6 ||
		!equalEntries(nodes[1].core.log[:3], committed) {
		t.Fatal("later leader is missing the already committed prefix")
	}
	want := append(cloneEntries(committed), &raftv1.Entry{Index: 4, Term: 6, Type: raftv1.EntryType_ENTRY_TYPE_NO_OP})
	paperDeliver(t, nodes, appends)
	for _, node := range nodes {
		assertPaperState(t, node, want, 4, commands)
	}
}
