package sim

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/zishan044/raft-replicated-key-value-store.git/internal/raftcore"
	raftv1 "github.com/zishan044/raft-replicated-key-value-store.git/pkg/api/raft/v1"
	"google.golang.org/protobuf/proto"
)

func newSim(t *testing.T, seed uint64) *Simulator {
	t.Helper()
	s, err := New(Config{Seed: seed, Voters: []uint64{3, 1, 2}})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func run(t *testing.T, s *Simulator, ticks int) {
	t.Helper()
	if err := s.Run(ticks); err != nil {
		t.Fatalf("%v\n%s", err, s.Trace())
	}
}

func faults(t *testing.T, s *Simulator, f Faults) {
	t.Helper()
	if err := s.SetFaults(f); err != nil {
		t.Fatal(err)
	}
}

func block(t *testing.T, s *Simulator, from, to uint64, blocked bool) {
	t.Helper()
	if err := s.SetBlocked(from, to, blocked); err != nil {
		t.Fatal(err)
	}
}

// Inject only into the private network for focused transport tests. Public
// simulation inputs are ticks and fault controls, never arbitrary Raft messages.
func heartbeat(from, to, term uint64) *raftv1.Message {
	return &raftv1.Message{Type: raftv1.MessageType_MESSAGE_TYPE_HEARTBEAT, From: from, To: to, Term: term}
}

func TestConfigurationAndControlValidation(t *testing.T) {
	for _, cfg := range []Config{
		{},
		{Voters: []uint64{0, 1}},
		{Voters: []uint64{1, 2, 1}},
		{Voters: []uint64{1}, ElectionTicks: -1},
		{Voters: []uint64{1}, HeartbeatTicks: -1},
		{Voters: []uint64{1}, ElectionTicks: 2, HeartbeatTicks: 2},
	} {
		if s, err := New(cfg); err == nil || s != nil {
			t.Fatalf("invalid configuration accepted: %+v", cfg)
		}
	}
	voters := []uint64{9, 2, 7}
	s, err := New(Config{Voters: voters})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(voters, []uint64{9, 2, 7}) {
		t.Fatal("New sorted caller membership in place")
	}
	voters[0] = 99
	if !slices.Equal(s.voters, []uint64{2, 7, 9}) {
		t.Fatal("simulator retained caller membership")
	}
	before := s.Trace()
	for _, f := range []Faults{
		{DropPercent: -1}, {DropPercent: 101}, {DuplicatePercent: -1}, {DuplicatePercent: 101},
		{MinDelay: -1, MaxDelay: 1}, {MinDelay: 0, MaxDelay: 2},
		{MinDelay: 2, MaxDelay: 1}, {MinDelay: 1, MaxDelay: int(^uint(0) >> 1)},
	} {
		if err := s.SetFaults(f); err == nil {
			t.Fatalf("invalid fault settings accepted: %+v", f)
		}
	}
	for _, pair := range []link{{2, 2}, {0, 2}, {2, 99}} {
		if err := s.SetBlocked(pair.from, pair.to, true); err == nil {
			t.Fatal("invalid partition link accepted")
		}
	}
	if s.Run(-1) == nil || s.Now() != 0 || s.Trace() != before {
		t.Fatal("invalid controls mutated the simulation")
	}
	if _, ok := s.SavedState(99); ok {
		t.Fatal("unknown node has saved state")
	}
}

func TestReliableClockOrderingAndMemory(t *testing.T) {
	s := newSim(t, 42)
	for range 80 {
		run(t, s, 1)
		for _, status := range s.Statuses() {
			if saved, ok := s.SavedState(status.ID); !ok || saved != status.HardState {
				t.Fatalf("node %d did not save its election state", status.ID)
			}
		}
		for _, event := range s.queue {
			if event.at <= s.Now() || s.stored[event.message.From].Term < event.message.Term {
				t.Fatal("message enqueued without a future deadline or before its term was saved")
			}
		}
	}
	var leader uint64
	for _, status := range s.Statuses() {
		if status.Role == raftcore.Leader {
			if leader != 0 {
				t.Fatal("reliable cluster has multiple current leaders")
			}
			leader = status.ID
		}
	}
	if leader == 0 || s.Now() != 80 {
		t.Fatal("logical ticks failed to elect a leader")
	}
	// End-of-tick memory checks alone would miss saving after enqueue. Check the
	// chronological trace too, including the vote each granted response relies on.
	saved := make(map[uint64]raftcore.HardState)
	for _, line := range strings.Split(s.Trace(), "\n") {
		var tick, due int
		var id, term, vote, sequence, from, to uint64
		var kind string
		var reject bool
		if strings.Contains(line, " save ") {
			if _, err := fmt.Sscanf(line, "tick=%d save node=%d term=%d vote=%d", &tick, &id, &term, &vote); err != nil {
				t.Fatal(err)
			}
			saved[id] = raftcore.HardState{Term: term, Vote: vote}
		} else if strings.Contains(line, " enqueue ") {
			if _, err := fmt.Sscanf(line, "tick=%d enqueue id=%d due=%d %s from=%d to=%d term=%d reject=%t",
				&tick, &sequence, &due, &kind, &from, &to, &term, &reject); err != nil {
				t.Fatal(err)
			}
			state := saved[from]
			if state.Term != term || (kind == "MESSAGE_TYPE_REQUEST_VOTE" && state.Vote != from) ||
				(kind == "MESSAGE_TYPE_REQUEST_VOTE_RESPONSE" && !reject && state.Vote != to) {
				t.Fatalf("message preceded its dependent saved state: %s", line)
			}
		}
	}
	snapshots := s.Statuses()
	snapshots[0].HardState.Term = 999
	if s.Statuses()[0].HardState.Term == 999 {
		t.Fatal("status inspection changed the core")
	}
	// Same deadline: preserve send order unless explicit reordering is enabled.
	s = newSim(t, 0)
	s.enqueue(heartbeat(1, 2, 1))
	s.enqueue(heartbeat(1, 2, 2))
	run(t, s, 1)
	trace := s.Trace()
	first, second := strings.Index(trace, "deliver id=1 "), strings.Index(trace, "deliver id=2 ")
	if first < 0 || second <= first || s.stored[2].Term != 2 {
		t.Fatal("equal-time delivery did not preserve insertion order")
	}
}

func TestInitialLogBatchRetainedAndApplied(t *testing.T) {
	s := newSim(t, 42)
	entries := []*raftv1.Entry{
		{Index: 1, Term: 1, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL, Data: []byte("first")},
		{Index: 2, Term: 3, Type: raftv1.EntryType_ENTRY_TYPE_NO_OP},
		{Index: 3, Term: 3, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL, Data: []byte("pending")},
	}
	core, err := raftcore.New(raftcore.Config{
		ID: 1, Voters: s.voters, Seed: 42, ElectionTicks: 10, HeartbeatTicks: 2,
		HardState: raftcore.HardState{Term: 3, Commit: 2}, Entries: entries,
	})
	if err != nil {
		t.Fatal(err)
	}
	s.nodes[1] = core
	// Retaining an incoming suffix must discard a previously retained longer
	// tail. The initial batch replaces from index 1, including a new term at 2.
	s.storedEntries[1] = []*raftv1.Entry{
		entries[0],
		{Index: 2, Term: 2, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL},
		{Index: 3, Term: 2, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL},
		{Index: 4, Term: 2, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL},
	}
	if err := s.finish(core, core.Status()); err != nil {
		t.Fatal(err)
	}
	if len(s.storedEntries[1]) != 3 || len(s.appliedEntries[1]) != 2 || core.Status().AppliedIndex != 2 ||
		s.stored[1] != (raftcore.HardState{Term: 3, Commit: 2}) {
		t.Fatal("log batch was not retained and applied before Advance")
	}
	for i, entry := range entries {
		if !proto.Equal(s.storedEntries[1][i], entry) {
			t.Fatalf("wrong retained entry at index %d", i+1)
		}
		if i < 2 && !proto.Equal(s.appliedEntries[1][i], entry) {
			t.Fatalf("wrong applied entry at index %d", i+1)
		}
	}
	trace := s.Trace()
	retained := strings.Index(trace, "save_entries node=1 first=1 count=3")
	committed := strings.Index(trace, "save node=1 term=3 vote=0 commit=2")
	first := strings.Index(trace, "apply node=1 index=1 ")
	second := strings.Index(trace, "apply node=1 index=2 ")
	if retained < 0 || committed <= retained || first <= committed || second <= first {
		t.Fatal("entries and commitment were not retained before ordered application")
	}
	if err := s.finish(core, core.Status()); err != nil {
		t.Fatal(err)
	}
	if s.Trace() != trace || len(s.appliedEntries[1]) != 2 {
		t.Fatal("an acknowledged batch was processed twice")
	}
	// New copied the input; later caller mutations cannot affect retained/apply
	// history, whose entries are independently owned Ready output.
	entries[0].Data[0] = 'x'
	if string(s.storedEntries[1][0].Data) != "first" || string(s.appliedEntries[1][0].Data) != "first" {
		t.Fatal("retained or applied entries alias constructor input")
	}
}

func TestDropAndDuplication(t *testing.T) {
	s := newSim(t, 0)
	faults(t, s, Faults{DropPercent: 100})
	s.enqueue(heartbeat(1, 2, 1))
	if len(s.queue) != 0 || !strings.Contains(s.Trace(), "drop random") {
		t.Fatal("100% loss did not drop the message")
	}
	faults(t, s, Faults{DuplicatePercent: 100})
	s.enqueue(heartbeat(1, 2, 1))
	if len(s.queue) != 2 || s.queue[0].sequence == s.queue[1].sequence {
		t.Fatal("duplication did not create two independently scheduled deliveries")
	}
	run(t, s, 1)
	if len(s.queue) != 0 || strings.Count(s.Trace(), "deliver id=") != 2 || s.stored[2].Term != 1 {
		t.Fatal("duplicates were lost, recursively duplicated, or changed the term")
	}
	faults(t, s, Faults{DuplicatePercent: 100, MinDelay: 2, MaxDelay: 5})
	for range 8 {
		s.enqueue(heartbeat(1, 2, 2))
	}
	different := false
	for i, event := range s.queue {
		if delay := event.at - s.Now(); delay < 2 || delay > 5 {
			t.Fatal("delivery delay fell outside the configured range")
		}
		if i%2 == 1 && event.at != s.queue[i-1].at {
			different = true
		}
	}
	if len(s.queue) != 16 || !different {
		t.Fatal("duplicate copies did not receive independent delays")
	}
}

func TestDelayAndReordering(t *testing.T) {
	s := newSim(t, 0)
	faults(t, s, Faults{MinDelay: 3, MaxDelay: 3})
	s.enqueue(heartbeat(1, 2, 1))
	run(t, s, 2)
	if s.stored[2].Term != 0 || len(s.queue) != 1 {
		t.Fatal("delayed traffic arrived early")
	}
	run(t, s, 1)
	if s.stored[2].Term != 1 || len(s.queue) != 0 {
		t.Fatal("delayed traffic missed its deadline")
	}
	// A later send with a shorter delay must overtake the first message.
	s = newSim(t, 0)
	faults(t, s, Faults{MinDelay: 3, MaxDelay: 3})
	s.enqueue(heartbeat(1, 2, 1))
	faults(t, s, Faults{})
	s.enqueue(heartbeat(1, 2, 2))
	run(t, s, 3)
	trace := s.Trace()
	if strings.Index(trace, "deliver id=2 ") >= strings.Index(trace, "deliver id=1 ") || s.stored[2].Term != 2 {
		t.Fatal("delay did not reorder traffic or stale delivery regressed term")
	}
	// Shuffle a same-tick batch; do not rely on one particular permutation.
	s = newSim(t, 0)
	faults(t, s, Faults{Reorder: true})
	for term := uint64(1); term <= 8; term++ {
		s.enqueue(heartbeat(1, 2, term))
	}
	run(t, s, 1)
	last := -1
	inOrder := true
	trace = s.Trace()
	for sequence := 1; sequence <= 8; sequence++ {
		position := strings.Index(trace, fmt.Sprintf("deliver id=%d ", sequence))
		if position < 0 {
			t.Fatal("shuffling lost a delivery")
		}
		if position < last {
			inOrder = false
		}
		last = position
	}
	if inOrder || s.stored[2].Term != 8 {
		t.Fatal("explicit reordering did not shuffle the batch or regressed term")
	}
}

func TestDirectedPartitionsAndHealing(t *testing.T) {
	s := newSim(t, 0)
	block(t, s, 1, 2, true)
	s.enqueue(heartbeat(1, 2, 1))
	s.enqueue(heartbeat(2, 1, 1))
	run(t, s, 1)
	if s.stored[1].Term != 1 || s.stored[2].Term != 0 {
		t.Fatal("directed partition blocked the wrong direction")
	}
	block(t, s, 2, 1, true)
	s.enqueue(heartbeat(2, 1, 2))
	if len(s.queue) != 0 {
		t.Fatal("symmetric partition allowed reverse traffic")
	}
	block(t, s, 1, 2, false)
	s.enqueue(heartbeat(1, 2, 2))
	block(t, s, 1, 2, true)
	run(t, s, 1)
	if s.stored[2].Term != 0 || !strings.Contains(s.Trace(), "drop partition delivery=") {
		t.Fatal("partition did not drop already queued traffic at delivery")
	}
	s.Heal()
	faults(t, s, Faults{MinDelay: 100, MaxDelay: 100})
	s.enqueue(heartbeat(1, 2, 3))
	s.enqueue(heartbeat(2, 1, 4))
	faults(t, s, Faults{DropPercent: 100, DuplicatePercent: 100, Reorder: true})
	block(t, s, 1, 2, true)
	serial := s.serial
	s.Heal()
	run(t, s, 1)
	if s.stored[1].Term != 4 || s.stored[2].Term != 3 || len(s.queue) != 0 || s.serial != serial {
		t.Fatal("healing discarded, duplicated, or failed to drain old traffic in one tick")
	}
	s.enqueue(heartbeat(1, 2, 5))
	if len(s.queue) != 1 || s.queue[0].at != s.Now()+1 {
		t.Fatal("healing did not restore reliable one-tick delivery")
	}
}

func TestSeedReplayAndCombinedFaultRecovery(t *testing.T) {
	schedule := func(seed uint64) *Simulator {
		s := newSim(t, seed)
		faults(t, s, Faults{DropPercent: 15, DuplicatePercent: 30, MinDelay: 1, MaxDelay: 15, Reorder: true})
		block(t, s, 1, 2, true)
		block(t, s, 2, 1, true)
		run(t, s, 40)
		block(t, s, 2, 1, false)
		run(t, s, 40)
		s.Heal()
		run(t, s, 1) // drain before beginning the recovery measurement
		run(t, s, 200)
		before := s.Statuses()
		leaders := 0
		for _, status := range before {
			if status.Role == raftcore.Leader {
				leaders++
			}
		}
		if leaders != 1 {
			t.Fatalf("seed %d failed to converge after healing\n%s", seed, s.Trace())
		}
		run(t, s, 40)
		for i, status := range s.Statuses() {
			if status.Role != before[i].Role || status.HardState.Term != before[i].HardState.Term || status.LeaderID != before[i].LeaderID {
				t.Fatalf("seed %d did not retain stable leadership\n%s", seed, s.Trace())
			}
		}
		return s
	}
	first, replay := schedule(123), schedule(123)
	if first.Trace() != replay.Trace() {
		t.Fatal("identical seeded control schedules produced different traces")
	}
	other := schedule(124)
	if slices.Equal(first.Statuses(), other.Statuses()) && first.serial == other.serial {
		t.Fatal("different seeds did not affect timeouts or the network")
	}
}

func TestElectionSafetyRemembersPastLeaders(t *testing.T) {
	s := newSim(t, 42)
	first := raftcore.Status{ID: 1, Role: raftcore.Leader, HardState: raftcore.HardState{Term: 7}}
	if err := s.checkLeader(first); err != nil {
		t.Fatal(err)
	}
	first.Role = raftcore.Follower
	if err := s.checkLeader(first); err != nil {
		t.Fatal(err)
	}
	other := raftcore.Status{ID: 2, Role: raftcore.Leader, HardState: raftcore.HardState{Term: 7}}
	if err := s.checkLeader(other); err == nil || !strings.Contains(err.Error(), "nodes 1 and 2") {
		t.Fatal("checker forgot the old leader after it stepped down")
	}
	other.HardState.Term = 8
	if err := s.checkLeader(other); err != nil {
		t.Fatal("leaders in different terms are allowed")
	}
	// Plant a conflicting history, then let an actual leader transition hit it.
	s = newSim(t, 42)
	for term := uint64(1); term <= 10; term++ {
		s.leaders[term] = 999
	}
	err := s.Run(100)
	if err == nil || !strings.Contains(err.Error(), "seed=42 tick=") || !strings.Contains(err.Error(), "election safety") {
		t.Fatalf("simulator did not stop on the planted violation: %v", err)
	}
	trace, now := s.Trace(), s.Now()
	if s.Tick() != err || s.Run(1) != err || s.Now() != now || s.Trace() != trace {
		t.Fatal("simulation continued after its invariant failed")
	}
}
