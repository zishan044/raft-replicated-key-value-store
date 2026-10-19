package sim_test

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/zishan044/raft-replicated-key-value-store.git/internal/raftcore"
	"github.com/zishan044/raft-replicated-key-value-store.git/internal/sim"
	"github.com/zishan044/raft-replicated-key-value-store.git/internal/wal"
	raftv1 "github.com/zishan044/raft-replicated-key-value-store.git/pkg/api/raft/v1"
	"google.golang.org/protobuf/proto"
)

func TestSeededCrashRecovery(t *testing.T) { runSeeded(t, runCrashSchedule) }

func runCrashSchedule(t *testing.T, seed uint64) *sim.Simulator {
	t.Helper()
	voters := []uint64{1, 2, 3}
	if seed%2 != 0 {
		voters = append(voters, 4, 5)
	}
	s, err := sim.New(sim.Config{Seed: seed, Voters: voters})
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewPCG(seed, ^uint64(1)))
	profiles := []string{"leader", "follower", "all", "torn-write", "sync-failure", "corruption", "recovery-failure", "combined"}
	profile := profiles[(seed/2)%uint64(len(profiles))]
	t.Logf("seed=%d nodes=%d profile=%s", seed, len(voters), profile)
	check(t, s, seed, s.Run(80))
	leader, ok := knownLeader(s.Statuses())
	if !ok {
		fail(t, s, seed, "healthy warm-up did not establish a leader")
	}
	_, _, err = s.Propose(leader.ID, []byte(fmt.Sprintf("seed=%d durable-baseline", seed)))
	check(t, s, seed, err)
	check(t, s, seed, s.Run(20))
	baseline, _ := s.AppliedEntries(leader.ID)
	if len(baseline) < 2 {
		fail(t, s, seed, "baseline marker was not committed before crashes")
	}
	for _, id := range voters {
		entries, _ := s.AppliedEntries(id)
		if len(entries) != len(baseline) {
			fail(t, s, seed, "baseline was not applied everywhere")
		}
	}
	remembered := baseline
	// Every corruption is explicitly repaired before a restart. Heal does not
	// repair storage, and a failed corrupt restart must not stop other nodes.
	type corruption struct {
		name string
		mask byte
	}
	corrupt := make(map[uint64]corruption)
	repair := func() {
		for _, id := range voters {
			if fault, ok := corrupt[id]; ok {
				check(t, s, seed, s.CorruptDisk(id, fault.name, 0, fault.mask))
				delete(corrupt, id)
			}
		}
	}
	restart := func() {
		for _, i := range rng.Perm(len(voters)) {
			id := voters[i]
			if !s.Running(id) {
				check(t, s, seed, s.Restart(id))
			}
		}
	}
	for tick := 0; tick < 200; tick++ {
		if tick%40 == 0 {
			check(t, s, seed, s.SetFaults(sim.Faults{
				DropPercent: rng.IntN(26), DuplicatePercent: rng.IntN(41),
				MinDelay: 1, MaxDelay: 2 + rng.IntN(9), Reorder: true,
			}))
			for _, from := range voters {
				for _, to := range voters {
					if from != to {
						check(t, s, seed, s.SetBlocked(from, to, rng.IntN(12) == 0))
					}
				}
			}
			kind := profile
			if kind == "combined" {
				kind = profiles[rng.IntN(len(profiles)-1)]
			}
			statuses := s.Statuses()
			var target raftcore.Status
			for _, status := range statuses {
				if status.Role == raftcore.Leader && (target.ID == 0 || status.HardState.Term > target.HardState.Term) {
					target = status
				}
			}
			if kind == "all" {
				for _, id := range voters {
					check(t, s, seed, s.Crash(id))
				}
			} else if len(statuses) > 0 {
				if kind == "follower" || kind == "corruption" || kind == "recovery-failure" {
					var candidates []raftcore.Status
					for _, status := range statuses {
						if status.Role != raftcore.Leader {
							candidates = append(candidates, status)
						}
					}
					if len(candidates) > 0 {
						target = candidates[rng.IntN(len(candidates))]
					}
				}
				if target.ID != 0 {
					switch kind {
					case "leader", "follower":
						check(t, s, seed, s.Crash(target.ID))
					case "torn-write", "sync-failure":
						data := []byte(fmt.Sprintf("seed=%d tick=%d uncertain", seed, tick))
						fault := sim.DiskFault{Op: sim.DiskSync}
						if kind == "torn-write" {
							record, encodeErr := wal.Encode(&raftv1.WALBatch{Entries: []*raftv1.Entry{{
								Index: target.LastLogIndex + 1, Term: target.HardState.Term,
								Type: raftv1.EntryType_ENTRY_TYPE_NORMAL, Data: data,
							}}})
							check(t, s, seed, encodeErr)
							fault = sim.DiskFault{Op: sim.DiskWrite, WriteBytes: 1 + rng.IntN(len(record)-1)}
						}
						check(t, s, seed, s.InjectDiskFault(target.ID, fault))
						_, _, proposalErr := s.Propose(target.ID, data)
						if !errors.Is(proposalErr, sim.ErrDiskFault) || s.Running(target.ID) {
							fail(t, s, seed, "armed disk fault did not stop the proposal's node")
						}
						if kind == "torn-write" {
							check(t, s, seed, s.Crash(target.ID, fault.WriteBytes))
						} else if rng.IntN(2) == 0 {
							check(t, s, seed, s.Crash(target.ID))
						}
					case "corruption":
						check(t, s, seed, s.Crash(target.ID))
						names, _ := s.DiskFiles(target.ID)
						fault := corruption{name: names[0], mask: 1 << rng.IntN(8)}
						check(t, s, seed, s.CorruptDisk(target.ID, fault.name, 0, fault.mask))
						corrupt[target.ID] = fault
						if err := s.Restart(target.ID); !errors.Is(err, wal.ErrCorrupt) || s.Running(target.ID) {
							fail(t, s, seed, "corrupted node was allowed to restart")
						}
					case "recovery-failure":
						check(t, s, seed, s.Crash(target.ID))
						check(t, s, seed, s.InjectDiskFault(target.ID, sim.DiskFault{Op: sim.DiskSync}))
						if err := s.Restart(target.ID); !errors.Is(err, sim.ErrDiskFault) || s.Running(target.ID) {
							fail(t, s, seed, "recovery sync failure was ignored")
						}
					}
				}
			}
		}
		if tick%40 == 20 {
			repair()
			restart()
		}
		check(t, s, seed, s.Tick())
		if tick%10 == 0 {
			for _, status := range s.Statuses() {
				if status.Role == raftcore.Leader {
					_, _, err := s.Propose(status.ID, []byte(fmt.Sprintf("seed=%d tick=%d leader=%d", seed, tick, status.ID)))
					check(t, s, seed, err)
				}
				if status.AppliedIndex > uint64(len(remembered)) {
					remembered, _ = s.AppliedEntries(status.ID)
				}
			}
			_, _, err := s.Propose(voters[rng.IntN(len(voters))], []byte("follower/down probe"))
			if err != nil && !errors.Is(err, raftcore.ErrNotLeader) && !errors.Is(err, sim.ErrNodeDown) {
				check(t, s, seed, err)
			}
		}
	}
	repair()
	// Every seed ends with a power loss of every node, not just agreement among
	// nodes that kept their volatile state. Preserve all previously observed apply.
	for _, id := range voters {
		check(t, s, seed, s.ClearDiskFault(id))
		check(t, s, seed, s.Crash(id))
	}
	restart()
	recoverReplication(t, s, seed)
	for _, id := range voters {
		applied, _ := s.AppliedEntries(id)
		if len(applied) < len(remembered) {
			fail(t, s, seed, "whole-cluster restart lost an applied prefix")
		}
		for i, entry := range remembered {
			if !proto.Equal(applied[i], entry) {
				fail(t, s, seed, "whole-cluster restart changed a previously applied entry")
			}
		}
	}
	return s
}
