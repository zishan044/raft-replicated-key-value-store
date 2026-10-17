package raftcore

import (
	"errors"
	"slices"
	"testing"

	raftv1 "github.com/zishan044/raft-replicated-key-value-store.git/pkg/api/raft/v1"
)

func config() Config {
	return Config{ID: 1, Voters: []uint64{3, 1, 2}, ElectionTicks: 10, HeartbeatTicks: 2, Seed: 42}
}

func entriesWithTerm(count int, term uint64) []*raftv1.Entry {
	entries := make([]*raftv1.Entry, count)
	for i := range entries {
		entries[i] = &raftv1.Entry{Index: uint64(i) + 1, Term: term, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL}
	}
	return entries
}

func TestConfigValidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*Config)
	}{
		{"zero ID", func(c *Config) { c.ID = 0 }},
		{"empty membership", func(c *Config) { c.Voters = nil }},
		{"missing self", func(c *Config) { c.Voters = []uint64{2, 3} }},
		{"zero voter", func(c *Config) { c.Voters = []uint64{0, 1} }},
		{"duplicate voter", func(c *Config) { c.Voters = []uint64{1, 2, 1} }},
		{"zero heartbeat", func(c *Config) { c.HeartbeatTicks = 0 }},
		{"negative heartbeat", func(c *Config) { c.HeartbeatTicks = -1 }},
		{"zero election", func(c *Config) { c.ElectionTicks = 0 }},
		{"negative election", func(c *Config) { c.ElectionTicks = -1 }},
		{"equal intervals", func(c *Config) { c.ElectionTicks = c.HeartbeatTicks }},
		{"short election", func(c *Config) { c.ElectionTicks = 1 }},
		{"overflow interval", func(c *Config) { c.ElectionTicks = int(^uint(0) >> 1) }},
		{"vote without term", func(c *Config) { c.HardState.Vote = 1 }},
		{"vote outside membership", func(c *Config) { c.HardState = HardState{Term: 2, Vote: 9} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config()
			tc.change(&cfg)
			if core, err := New(cfg); err == nil || core != nil {
				t.Fatalf("invalid config accepted: core=%v err=%v", core, err)
			}
		})
	}
}

func TestInitialStateAndDeterminism(t *testing.T) {
	cfg := config()
	cfg.HardState = HardState{Term: 4, Vote: 2}
	cfg.Entries = entriesWithTerm(9, 3)
	core, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	status := core.Status()
	if status != replay.Status() || status.ID != 1 || status.Role != Follower || status.LeaderID != 0 ||
		status.HardState != cfg.HardState || status.LastLogIndex != 9 || status.LastLogTerm != 3 ||
		status.ElectionElapsed != 0 || status.ElectionTimeout < 10 || status.ElectionTimeout >= 20 {
		t.Fatalf("unexpected initial status: %+v", status)
	}
	if !slices.Equal(cfg.Voters, []uint64{3, 1, 2}) {
		t.Fatal("New mutated caller membership")
	}
	cfg.Voters[0] = 99
	if !slices.Equal(core.voters, []uint64{1, 2, 3}) {
		t.Fatal("core retained caller membership")
	}
	status.HardState.Term = 99
	if core.Status().HardState.Term != 4 {
		t.Fatal("status aliases core state")
	}
	// A single voter and seed zero are both valid configurations.
	cfg = config()
	cfg.Voters, cfg.Seed = []uint64{1}, 0
	if _, err := New(cfg); err != nil {
		t.Fatal(err)
	}
}

func TestReadyLifecycleAndOwnership(t *testing.T) {
	core, err := New(config())
	if err != nil {
		t.Fatal(err)
	}
	if !errors.Is(core.Advance(), ErrNoReady) || !errors.Is(core.Tick(), ErrReadyPending) {
		t.Fatal("initial state must be consumed before accepting input")
	}
	// Inject output to test ownership separately from election transitions.
	core.messages = []*raftv1.Message{{Type: raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE, From: 1, To: 2, Term: 1}}
	rd := core.Ready()
	if rd.HardState == nil || *rd.HardState != (HardState{}) || len(rd.Messages) != 1 {
		t.Fatalf("missing initial output: %+v", rd)
	}
	if !errors.Is(core.Tick(), ErrReadyPending) || !errors.Is(core.Step(rd.Messages[0]), ErrReadyPending) {
		t.Fatal("input accepted with an outstanding batch")
	}
	rd.HardState.Term = 99
	rd.Messages[0].Term = 99
	rd.Messages[0] = nil
	retry := core.Ready()
	if *retry.HardState != (HardState{}) || retry.Messages[0].Term != 1 {
		t.Fatal("caller mutation changed the outstanding batch")
	}
	if err := core.Advance(); err != nil {
		t.Fatal(err)
	}
	if rd := core.Ready(); rd.HardState != nil || len(rd.Messages) != 0 {
		t.Fatalf("acknowledged output was repeated: %+v", rd)
	}
	if !errors.Is(core.Advance(), ErrNoReady) {
		t.Fatal("an empty Ready must not create an outstanding batch")
	}
	if err := core.Tick(); err != nil || core.Status().ElectionElapsed != 1 {
		t.Fatalf("tick failed after Advance: %v", err)
	}
	// Simulate a later term change and a messages-only batch independently.
	core.hardState = HardState{Term: 2, Vote: 1}
	rd = core.Ready()
	if rd.HardState == nil || *rd.HardState != core.hardState {
		t.Fatal("changed election state was not emitted")
	}
	if err := core.Advance(); err != nil {
		t.Fatal(err)
	}
	core.messages = retry.Messages
	rd = core.Ready()
	if rd.HardState != nil || len(rd.Messages) != 1 {
		t.Fatal("messages-only batch re-emitted unchanged election state")
	}
	if err := core.Advance(); err != nil {
		t.Fatal(err)
	}
	if retry.Messages[0].Term != 1 {
		t.Fatal("Advance mutated a previously returned message")
	}
}

func TestStepBoundary(t *testing.T) {
	core, err := New(config())
	if err != nil {
		t.Fatal(err)
	}
	core.Ready()
	if err := core.Advance(); err != nil {
		t.Fatal(err)
	}
	before := core.Status()
	for _, msg := range []*raftv1.Message{
		nil,
		{From: 0, To: 1},
		{From: 9, To: 1},
		{From: 1, To: 1},
		{From: 2, To: 3},
	} {
		if err := core.Step(msg); err == nil || errors.Is(err, ErrUnsupportedMessage) {
			t.Fatalf("invalid envelope was not rejected: %v, err=%v", msg, err)
		}
	}
	for _, msgType := range []raftv1.MessageType{
		raftv1.MessageType_MESSAGE_TYPE_PRE_VOTE,
		raftv1.MessageType_MESSAGE_TYPE_PRE_VOTE_RESPONSE,
		raftv1.MessageType(5), // retired standalone heartbeat
		raftv1.MessageType_MESSAGE_TYPE_UNSPECIFIED,
		999,
	} {
		msg := &raftv1.Message{Type: msgType, From: 2, To: 1, Term: 9}
		if err := core.Step(msg); !errors.Is(err, ErrUnsupportedMessage) {
			t.Fatalf("unimplemented message was not rejected: %v", err)
		}
	}
	for _, msg := range []*raftv1.Message{
		{Type: raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE, From: 2, To: 1},
		{Type: raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE_RESPONSE, From: 2, To: 1},
		{Type: raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE, From: 9, To: 1, Term: 99},
		{Type: raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE, From: 2, To: 3, Term: 99},
		{Type: raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE, From: 2, To: 1, Term: 99, Reject: true},
		{Type: raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE, From: 2, To: 1, Term: 99, LastLogIndex: 1},
		{Type: raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE, From: 2, To: 1, Term: 99, LastLogTerm: 1},
		{Type: raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE, From: 2, To: 1, Term: 99, LastLogIndex: 1, LastLogTerm: 100},
	} {
		if err := core.Step(msg); err == nil {
			t.Fatalf("malformed election message accepted: %v", msg)
		}
	}
	if core.Status() != before {
		t.Fatal("rejected input changed core state")
	}
}
