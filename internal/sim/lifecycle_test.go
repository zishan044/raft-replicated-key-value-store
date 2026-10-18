package sim

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/zishan044/raft-replicated-key-value-store.git/internal/raftcore"
	"github.com/zishan044/raft-replicated-key-value-store.git/internal/wal"
	raftv1 "github.com/zishan044/raft-replicated-key-value-store.git/pkg/api/raft/v1"
	"google.golang.org/protobuf/proto"
)

func liveLeader(t *testing.T, s *Simulator) raftcore.Status {
	t.Helper()
	var leader raftcore.Status
	for _, status := range s.Statuses() {
		if status.Role == raftcore.Leader {
			if leader.ID != 0 {
				t.Fatalf("multiple live leaders\n%s", s.Trace())
			}
			leader = status
		}
	}
	if leader.ID == 0 {
		t.Fatalf("no live leader\n%s", s.Trace())
	}
	return leader
}

func proposeMarker(t *testing.T, s *Simulator, data string) (uint64, uint64) {
	t.Helper()
	index, term, err := s.Propose(liveLeader(t, s).ID, []byte(data))
	if err != nil {
		t.Fatal(err)
	}
	run(t, s, 20)
	return index, term
}

func assertMarker(t *testing.T, s *Simulator, index, term uint64, data string) {
	t.Helper()
	for _, id := range s.voters {
		entries, ok := s.AppliedEntries(id)
		if !ok || uint64(len(entries)) < index || entries[index-1].Term != term || string(entries[index-1].Data) != data {
			t.Fatalf("node %d lost marker index=%d data=%q\n%s", id, index, data, s.Trace())
		}
	}
}

func TestLeaderFollowerAndWholeClusterCrashRecovery(t *testing.T) {
	for _, size := range []int{3, 5} {
		for _, victim := range []string{"follower", "leader", "all"} {
			t.Run(fmt.Sprintf("nodes_%d_%s", size, victim), func(t *testing.T) {
				voters := make([]uint64, size)
				for i := range voters {
					voters[i] = uint64(i + 1)
				}
				s, err := New(Config{Seed: 42, Voters: voters})
				if err != nil {
					t.Fatal(err)
				}
				run(t, s, 80)
				leader := liveLeader(t, s)
				index, term := proposeMarker(t, s, "before crash")
				ids := slices.Clone(voters)
				if victim == "leader" {
					ids = []uint64{leader.ID}
				} else if victim == "follower" {
					for _, id := range voters {
						if id != leader.ID {
							ids = []uint64{id}
							break
						}
					}
				}
				committed, applied, leaders := len(s.committed), len(s.applied), len(s.leaders)
				oldCores := make(map[uint64]*raftcore.Core)
				for _, id := range ids {
					oldCores[id] = s.nodes[id]
					if err := s.Crash(id); err != nil {
						t.Fatal(err)
					}
					if s.Running(id) || s.observed[id].entries != nil {
						t.Fatal("crashed node retained live incarnation state")
					}
					if _, ok := s.AppliedEntries(id); ok {
						t.Fatal("crashed node exposed volatile application state")
					}
					if _, _, err := s.Propose(id, []byte("down")); !errors.Is(err, ErrNodeDown) {
						t.Fatal("crashed node accepted a proposal")
					}
				}
				if committed != len(s.committed) || applied != len(s.applied) || leaders != len(s.leaders) {
					t.Fatal("crash erased global safety history")
				}
				run(t, s, 80)
				var duringIndex, duringTerm uint64
				if victim != "all" {
					duringIndex, duringTerm = proposeMarker(t, s, "while down")
				} else if len(s.Statuses()) != 0 {
					t.Fatal("all-crashed cluster still had a live node")
				}
				for _, id := range ids {
					if err := s.Restart(id); err != nil {
						t.Fatalf("restart node %d: %v\n%s", id, err, s.Trace())
					}
					core := s.nodes[id]
					if core == oldCores[id] || core.Status().Role != raftcore.Follower || core.Status().LeaderID != 0 || s.incarnations[id] != 1 {
						t.Fatal("restart did not create a fresh follower")
					}
					if len(s.processors) != len(s.nodes) {
						t.Fatal("restart retained a previous core's processor")
					}
				}
				s.Heal()
				run(t, s, 200)
				assertMarker(t, s, index, term, "before crash")
				if duringIndex != 0 {
					assertMarker(t, s, duringIndex, duringTerm, "while down")
				}
				current := liveLeader(t, s)
				for _, status := range s.Statuses() {
					if status.LeaderID != current.ID || status.HardState.Term != current.HardState.Term {
						t.Fatal("cluster did not converge after recovery")
					}
				}
			})
		}
	}
}

func TestCrashKeepsSentMessagesAndDropsTrafficDeliveredWhileDown(t *testing.T) {
	s := newSim(t, 42)
	faults(t, s, Faults{MinDelay: 5, MaxDelay: 5})
	s.enqueue(termMessage(1, 2, 4))
	s.enqueue(termMessage(2, 1, 3))
	if err := s.Crash(1); err != nil || len(s.queue) != 2 {
		t.Fatal("crash discarded already-sent traffic")
	}
	s.Heal()
	if s.Running(1) {
		t.Fatal("network healing restarted a crashed node")
	}
	run(t, s, 1)
	if s.stored[2].Term != 4 || len(s.queue) != 0 || !strings.Contains(s.Trace(), "drop down delivery=") {
		t.Fatal("old outbound traffic was lost or traffic reached a down node")
	}
	block(t, s, 1, 2, true) // partition controls still accept stopped voters
	block(t, s, 1, 2, false)
	if err := s.Restart(1); err != nil || s.nodes[1].Status().HardState.Term != 0 {
		t.Fatal("dropped traffic was revived on restart")
	}
}

func TestVoteSurvivesPowerCrash(t *testing.T) {
	s := newSim(t, 42)
	s.enqueue(&raftv1.Message{Type: raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE, From: 2, To: 1, Term: 5})
	run(t, s, 1)
	if s.stored[1] != (raftcore.HardState{Term: 5, Vote: 2}) {
		t.Fatal("vote was not saved")
	}
	if err := s.Crash(1); err != nil {
		t.Fatal(err)
	}
	if err := s.Restart(1); err != nil {
		t.Fatal(err)
	}
	s.enqueue(&raftv1.Message{Type: raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE, From: 3, To: 1, Term: 5})
	run(t, s, 1)
	if s.stored[1] != (raftcore.HardState{Term: 5, Vote: 2}) {
		t.Fatal("restart allowed a second vote in the same term")
	}
	found := false
	for _, event := range s.queue {
		if msg := event.message; msg.From == 1 && msg.To == 3 && msg.Type == raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE_RESPONSE {
			found = msg.Term == 5 && msg.Reject
		}
	}
	if !found {
		t.Fatal("recovered node did not reject the competing vote")
	}
}

func TestUnpersistedSingleNodeCommitIsNotDurableHistory(t *testing.T) {
	s, err := New(Config{Seed: 42, Voters: []uint64{1}})
	if err != nil {
		t.Fatal(err)
	}
	run(t, s, 30)
	before := len(s.committed)
	if err := s.InjectDiskFault(1, DiskFault{Op: DiskSync}); err != nil {
		t.Fatal(err)
	}
	index, term, err := s.Propose(1, []byte("lost before sync"))
	if !errors.Is(err, ErrDiskFault) || index == 0 || term == 0 || s.Running(1) || len(s.committed) != before || len(s.applied) != before {
		t.Fatalf("unsaved optimistic commit became durable history: %v", err)
	}
	if err := s.Crash(1); err != nil {
		t.Fatal(err)
	}
	if err := s.Restart(1); err != nil {
		t.Fatal(err)
	}
	run(t, s, 30)
	if liveLeader(t, s).HardState.Term <= term {
		t.Fatal("recovered single node did not campaign in a new term")
	}
	for _, e := range s.applied {
		if string(e.Data) == "lost before sync" {
			t.Fatal("unsynced proposal survived a power crash")
		}
	}
}

func TestLostCommitWatermarkPreservesKnownCommitHistory(t *testing.T) {
	s := newSim(t, 42)
	run(t, s, 80)
	leader := liveLeader(t, s)
	oldCommit := leader.HardState.Commit
	index, term, err := s.Propose(leader.ID, []byte("replicated before watermark failure"))
	if err != nil {
		t.Fatal(err)
	}
	// Entries are already synced and will be acknowledged by a follower. Fail
	// only the subsequent save of the leader's new commit watermark.
	if err := s.InjectDiskFault(leader.ID, DiskFault{Op: DiskSync}); err != nil {
		t.Fatal(err)
	}
	run(t, s, 5)
	if s.Running(leader.ID) || uint64(len(s.committed)) < index || string(s.committed[index-1].entry.Data) != "replicated before watermark failure" {
		t.Fatal("failed watermark save erased a commit backed by durable replicas")
	}
	if err := s.Crash(leader.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.Restart(leader.ID); err != nil {
		t.Fatal(err)
	}
	if s.nodes[leader.ID].Status().HardState.Commit != oldCommit {
		t.Fatal("unsynced local commit watermark survived the crash")
	}
	run(t, s, 120)
	assertMarker(t, s, index, term, "replicated before watermark failure")
}

func TestCorruptRestartRefusalLeavesMajorityRunning(t *testing.T) {
	s := newSim(t, 42)
	run(t, s, 80)
	index, term := proposeMarker(t, s, "before corruption")
	leader := liveLeader(t, s)
	var victim uint64
	for _, id := range s.voters {
		if id != leader.ID {
			victim = id
			break
		}
	}
	if err := s.Crash(victim); err != nil {
		t.Fatal(err)
	}
	names, ok := s.DiskFiles(victim)
	if !ok || len(names) == 0 {
		t.Fatal("crashed voter lost its durable files")
	}
	if err := s.CorruptDisk(victim, names[0], 0, 1); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := s.Restart(victim); !errors.Is(err, wal.ErrCorrupt) || s.Running(victim) || s.err != nil {
			t.Fatalf("corrupt restart was accepted or stopped the cluster: %v", err)
		}
		if err := s.Crash(victim); err != nil {
			t.Fatal(err)
		}
	}
	run(t, s, 40)
	duringIndex, duringTerm := proposeMarker(t, s, "majority continued")
	// Only an explicit external repair restores this node; Heal cannot do it.
	s.Heal()
	if s.Running(victim) {
		t.Fatal("healing repaired corrupt storage")
	}
	if err := s.CorruptDisk(victim, names[0], 0, 1); err != nil {
		t.Fatal(err)
	}
	if err := s.Restart(victim); err != nil {
		t.Fatal(err)
	}
	run(t, s, 80)
	assertMarker(t, s, index, term, "before corruption")
	assertMarker(t, s, duringIndex, duringTerm, "majority continued")
}

func TestCrashRestartPreservesApplicationSafetyHistory(t *testing.T) {
	s := newSim(t, 42)
	run(t, s, 80)
	index, _ := proposeMarker(t, s, "durable")
	if err := s.Crash(1); err != nil {
		t.Fatal(err)
	}
	bad := proto.Clone(s.applied[index-1]).(*raftv1.Entry)
	bad.Data = []byte("planted conflicting history")
	s.applied[index-1] = bad
	err := s.Restart(1)
	if err == nil || !strings.Contains(err.Error(), "state machine safety") || s.err == nil || s.Running(1) {
		t.Fatalf("restart erased or ignored historical application: %v", err)
	}
	trace := s.Trace()
	if s.Tick() != err || s.Trace() != trace {
		t.Fatal("actual invariant failure was treated as an expected node fault")
	}
}

func TestDiskFaultCrashRestartTraceReplay(t *testing.T) {
	schedule := func() *Simulator {
		s := newSim(t, 123)
		faults(t, s, Faults{DropPercent: 20, DuplicatePercent: 30, MinDelay: 1, MaxDelay: 10, Reorder: true})
		run(t, s, 100)
		s.Heal()
		run(t, s, 100)
		index, term := proposeMarker(t, s, "replay marker")
		victim := liveLeader(t, s).ID
		if err := s.InjectDiskFault(victim, DiskFault{Op: DiskWrite, WriteBytes: 13}); err != nil {
			t.Fatal(err)
		}
		if _, _, err := s.Propose(victim, []byte("uncertain write")); !errors.Is(err, ErrDiskFault) || s.Running(victim) {
			t.Fatalf("disk fault did not stop the affected node: %v", err)
		}
		if err := s.Crash(victim, 13); err != nil {
			t.Fatal(err)
		}
		run(t, s, 80)
		if err := s.InjectDiskFault(victim, DiskFault{Op: DiskTruncate}); err != nil {
			t.Fatal(err)
		}
		if err := s.Restart(victim); !errors.Is(err, ErrDiskFault) || s.Running(victim) || s.err != nil {
			t.Fatal("failed tail repair did not leave the node down")
		}
		if err := s.Restart(victim); err != nil {
			t.Fatalf("second tail recovery failed: %v", err)
		}
		run(t, s, 120)
		assertMarker(t, s, index, term, "replay marker")
		if !strings.Contains(s.Trace(), "op=truncate") || !strings.Contains(s.Trace(), "restart_rejected") || !strings.Contains(s.Trace(), "disk_stop") {
			t.Fatal("trace omitted disk/lifecycle decisions")
		}
		return s
	}
	first, second := schedule(), schedule()
	if first.Trace() != second.Trace() {
		t.Fatal("same seed and disk controls produced different traces")
	}
}

func TestLifecycleControlValidation(t *testing.T) {
	s := newSim(t, 42)
	before := s.Trace()
	for _, control := range []func() error{
		func() error { return s.Crash(99) },
		func() error { return s.Crash(1, -1) },
		func() error { return s.Crash(1, 1, 2) },
		func() error { return s.Restart(99) },
		func() error { return s.Restart(1) },
		func() error { return s.InjectDiskFault(99, DiskFault{Op: DiskWrite}) },
		func() error { return s.InjectDiskFault(1, DiskFault{Op: DiskWrite, After: -1}) },
		func() error { return s.ClearDiskFault(99) },
		func() error { return s.CorruptDisk(1, "any", 0, 1) },
	} {
		if err := control(); err == nil || s.Trace() != before {
			t.Fatal("invalid lifecycle control changed the simulator")
		}
	}
	if files, ok := s.DiskFiles(99); ok || files != nil || s.Running(99) {
		t.Fatal("unknown node returned disk or liveness state")
	}
	files, _ := s.DiskFiles(1)
	files[0] = "mutated"
	fresh, _ := s.DiskFiles(1)
	if bytes.Equal([]byte(files[0]), []byte(fresh[0])) {
		t.Fatal("disk inspection aliases the namespace")
	}
	if err := s.InjectDiskFault(1, DiskFault{Op: DiskWrite}); err != nil {
		t.Fatal(err)
	}
	if err := s.ClearDiskFault(1); err != nil {
		t.Fatal(err)
	}
	run(t, s, 30)
	if !s.Running(1) {
		t.Fatal("cleared fault still stopped a node")
	}
}
