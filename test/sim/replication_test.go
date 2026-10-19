package sim_test

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/zishan044/raft-replicated-key-value-store.git/internal/raftcore"
	"github.com/zishan044/raft-replicated-key-value-store.git/internal/sim"
	"google.golang.org/protobuf/proto"
)

var (
	seedCount  = flag.Int("seeds", 500, "number of consecutive seeds, starting at zero")
	replaySeed = flag.Uint64("seed", 0, "replay this seed instead of running the campaign")
	trace      = flag.Bool("trace", false, "print successful simulation traces (use with -v)")
)

var profiles = []string{
	"healthy", "drop", "delay", "duplicate", "reorder",
	"symmetric-partition", "asymmetric-partition", "leader-isolation", "combined", "split-votes",
}

func TestSeededReplication(t *testing.T) {
	runSeeded(t, runSchedule)
}

func runSeeded(t *testing.T, schedule func(*testing.T, uint64) *sim.Simulator) {
	t.Helper()
	replay := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "seed" {
			replay = true
		}
	})
	count := *seedCount
	if replay {
		count = 1
	} else if count <= 0 {
		t.Fatal("seeds must be positive")
	}
	for i := range count {
		seed := uint64(i)
		if replay {
			seed = *replaySeed
		}
		if !t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			s := schedule(t, seed)
			if *trace {
				t.Logf("\n%s", s.Trace())
			}
		}) {
			return // first failure already includes its seed and full trace
		}
	}
}

func runSchedule(t *testing.T, seed uint64) *sim.Simulator {
	t.Helper()
	size := 3 + 2*int(seed%2)
	voters := make([]uint64, size)
	for i := range voters {
		voters[i] = uint64(i + 1)
	}
	s, err := sim.New(sim.Config{Seed: seed, Voters: voters})
	if err != nil {
		t.Fatal(err)
	}
	// Every profile gets both cluster sizes. Control randomness has its own
	// stream, separate from the simulator's node and network streams.
	profile := profiles[(seed/2)%uint64(len(profiles))]
	rng := rand.New(rand.NewPCG(seed, ^uint64(0)))
	t.Logf("seed=%d nodes=%d profile=%s", seed, size, profile)
	var isolated, oldTerm uint64
	delayedVotes := false
	switch profile {
	case "drop":
		check(t, s, seed, s.SetFaults(sim.Faults{DropPercent: 20 + rng.IntN(41)}))
	case "delay":
		f := sim.Faults{MinDelay: 1, MaxDelay: 20 + rng.IntN(11)}
		if (seed/20)%2 == 0 {
			f.MinDelay, f.MaxDelay, delayedVotes = 20, 20, true
		}
		check(t, s, seed, s.SetFaults(f))
	case "duplicate":
		check(t, s, seed, s.SetFaults(sim.Faults{DuplicatePercent: 30 + rng.IntN(71)}))
	case "reorder":
		check(t, s, seed, s.SetFaults(sim.Faults{Reorder: true}))
	case "symmetric-partition", "asymmetric-partition":
		minority := make(map[uint64]bool)
		permutation := rng.Perm(size)
		for _, i := range permutation[:1+rng.IntN(size/2)] {
			minority[voters[i]] = true
		}
		for _, from := range voters {
			for _, to := range voters {
				if minority[from] != minority[to] && (profile == "symmetric-partition" || minority[from]) {
					check(t, s, seed, s.SetBlocked(from, to, true))
				}
			}
		}
	case "leader-isolation":
		// Establish a leader within the agreed recovery budget before starting
		// this profile's 200-tick fault phase, so the victim is an actual leader.
		check(t, s, seed, s.Run(200))
		leader, ok := knownLeader(s.Statuses())
		if !ok {
			fail(t, s, seed, "healthy warm-up did not establish a leader")
		}
		isolated, oldTerm = leader.ID, leader.HardState.Term
		_, _, err := s.Propose(leader.ID, []byte(fmt.Sprintf("seed=%d warmup", seed)))
		check(t, s, seed, err)
		check(t, s, seed, s.Run(10))
		for _, peer := range voters {
			if peer != isolated {
				check(t, s, seed, s.SetBlocked(isolated, peer, true))
				check(t, s, seed, s.SetBlocked(peer, isolated, true))
			}
		}
	case "split-votes":
		check(t, s, seed, s.SetFaults(sim.Faults{DropPercent: 100}))
	}
	for tick := 0; tick < 200; tick++ {
		if profile == "combined" && tick%20 == 0 {
			check(t, s, seed, s.SetFaults(sim.Faults{
				DropPercent: rng.IntN(41), DuplicatePercent: rng.IntN(71),
				MinDelay: 1, MaxDelay: 5 + rng.IntN(26), Reorder: true,
			}))
			for _, from := range voters {
				for _, to := range voters {
					if from != to {
						check(t, s, seed, s.SetBlocked(from, to, rng.IntN(4) == 0))
					}
				}
			}
		}
		check(t, s, seed, s.Tick())
		if tick%10 == 0 {
			// Every self-declared leader gets unique commands, including a
			// minority's old leader while a replacement accepts majority writes.
			for _, status := range s.Statuses() {
				if status.Role == raftcore.Leader {
					data := []byte(fmt.Sprintf("seed=%d tick=%d leader=%d", seed, s.Now(), status.ID))
					_, _, err := s.Propose(status.ID, data)
					check(t, s, seed, err)
				}
			}
			id := voters[rng.IntN(size)]
			_, _, err := s.Propose(id, []byte(fmt.Sprintf("seed=%d tick=%d probe=%d", seed, s.Now(), id)))
			if err != nil && !errors.Is(err, raftcore.ErrNotLeader) {
				check(t, s, seed, err)
			}
		}
		if (profile == "split-votes" || delayedVotes) && tick == 18 {
			// Every initial deadline is 10..19, and a second cannot expire
			// before tick 20. At tick 19 everyone has self-voted in term 1.
			if delayedVotes && strings.Contains(s.Trace(), " deliver ") {
				fail(t, s, seed, "vote traffic arrived before its configured delay")
			}
			for _, status := range s.Statuses() {
				if status.Role != raftcore.Candidate || status.HardState != (raftcore.HardState{Term: 1, Vote: status.ID}) {
					fail(t, s, seed, "blackout or delayed votes did not produce the intended split vote")
				}
			}
		}
	}
	if isolated != 0 {
		var majority []raftcore.Status
		for _, status := range s.Statuses() {
			if status.ID != isolated {
				majority = append(majority, status)
			} else if status.Role != raftcore.Leader || status.HardState.Term != oldTerm {
				fail(t, s, seed, "isolated leader changed without contact or CheckQuorum")
			}
		}
		leader, ok := knownLeader(majority)
		if !ok || leader.HardState.Term <= oldTerm {
			fail(t, s, seed, "connected majority failed to replace the isolated leader")
		}
	}
	recoverReplication(t, s, seed)
	return s
}

func recoverReplication(t *testing.T, s *sim.Simulator, seed uint64) {
	t.Helper()
	s.Heal()
	check(t, s, seed, s.Tick()) // drain old traffic before measuring recovery
	var markerIndex, markerTerm, markerLeader uint64
	var marker []byte
	for tick := 0; tick < 200; tick++ {
		check(t, s, seed, s.Tick())
		leader, ok := knownLeader(s.Statuses())
		if ok && (leader.ID != markerLeader || leader.HardState.Term != markerTerm) {
			marker = []byte(fmt.Sprintf("seed=%d recovery_tick=%d leader=%d", seed, tick, leader.ID))
			var err error
			markerIndex, markerTerm, err = s.Propose(leader.ID, marker)
			check(t, s, seed, err)
			markerLeader = leader.ID
		}
	}
	leader, ok := knownLeader(s.Statuses())
	if !ok {
		fail(t, s, seed, "no agreed leader at the 200-tick recovery deadline")
	}
	if markerIndex == 0 || markerLeader != leader.ID || markerTerm != leader.HardState.Term {
		fail(t, s, seed, "no fresh proposal from the recovered leader within 200 ticks")
	}
	canonical, _ := s.AppliedEntries(leader.ID)
	if uint64(len(canonical)) < markerIndex {
		fail(t, s, seed, "recovered leader did not apply its fresh proposal within 200 ticks")
	}
	for _, status := range s.Statuses() {
		applied, _ := s.AppliedEntries(status.ID)
		if uint64(len(applied)) < markerIndex || applied[markerIndex-1].Term != markerTerm ||
			!bytes.Equal(applied[markerIndex-1].Data, marker) {
			fail(t, s, seed, "a node did not apply the fresh recovery proposal within 200 ticks")
		}
		for i := uint64(0); i < markerIndex; i++ {
			if !proto.Equal(applied[i], canonical[i]) {
				fail(t, s, seed, "nodes did not converge through the fresh committed proposal")
			}
		}
	}
	for range 40 {
		check(t, s, seed, s.Tick())
		current, ok := knownLeader(s.Statuses())
		if !ok || current.ID != leader.ID || current.HardState.Term != leader.HardState.Term {
			fail(t, s, seed, "leadership changed during the 40-tick stability window")
		}
	}
}

// Agreement is stronger than finding one node that calls itself leader: every
// connected node must know that leader in the same term, with no candidate left.
func knownLeader(statuses []raftcore.Status) (raftcore.Status, bool) {
	var leader raftcore.Status
	for _, status := range statuses {
		if status.Role == raftcore.Leader {
			if leader.ID != 0 {
				return raftcore.Status{}, false
			}
			leader = status
		}
	}
	if leader.ID == 0 {
		return raftcore.Status{}, false
	}
	for _, status := range statuses {
		if status.LeaderID != leader.ID || status.HardState.Term != leader.HardState.Term ||
			(status.ID != leader.ID && status.Role != raftcore.Follower) {
			return raftcore.Status{}, false
		}
	}
	return leader, true
}

func check(t *testing.T, s *sim.Simulator, seed uint64, err error) {
	t.Helper()
	if err != nil {
		fail(t, s, seed, err.Error())
	}
}

func fail(t *testing.T, s *sim.Simulator, seed uint64, reason string) {
	t.Helper()
	target := "sim"
	if strings.HasPrefix(t.Name(), "TestSeededCrashRecovery/") {
		target = "sim-crash"
	}
	t.Fatalf("%s\nreplay: make %s SEED=%d\n%s", reason, target, seed, s.Trace())
}

func TestConvergenceRequiresAgreement(t *testing.T) {
	leader := raftcore.Status{ID: 1, Role: raftcore.Leader, LeaderID: 1, HardState: raftcore.HardState{Term: 2}}
	follower := raftcore.Status{ID: 2, Role: raftcore.Follower, LeaderID: 1, HardState: raftcore.HardState{Term: 2}}
	if _, ok := knownLeader([]raftcore.Status{leader, follower}); !ok {
		t.Fatal("agreed leader was rejected")
	}
	for _, other := range []raftcore.Status{
		{ID: 2, Role: raftcore.Leader, LeaderID: 2, HardState: raftcore.HardState{Term: 3}},
		{ID: 2, Role: raftcore.Candidate, LeaderID: 1, HardState: raftcore.HardState{Term: 2}},
		{ID: 2, Role: raftcore.Follower, LeaderID: 1, HardState: raftcore.HardState{Term: 1}},
		{ID: 2, Role: raftcore.Follower, LeaderID: 3, HardState: raftcore.HardState{Term: 2}},
	} {
		if _, ok := knownLeader([]raftcore.Status{leader, other}); ok {
			t.Fatal("convergence checker accepted conflicting leadership, role or term")
		}
	}
	if _, ok := knownLeader([]raftcore.Status{follower}); ok {
		t.Fatal("convergence checker accepted a leader hint with no actual leader")
	}
}
