package sim

import (
	"errors"
	"fmt"
	"math"
	"math/rand/v2"

	"github.com/zishan044/raft-replicated-key-value-store.git/internal/raftcore"
	"github.com/zishan044/raft-replicated-key-value-store.git/internal/raftio"
	"github.com/zishan044/raft-replicated-key-value-store.git/internal/wal"
	raftv1 "github.com/zishan044/raft-replicated-key-value-store.git/pkg/api/raft/v1"
)

func (s *Simulator) nodeConfig(id, incarnation uint64) raftcore.Config {
	// Preserve the original first-boot stream. Restarts have their own stream,
	// independent of network randomness and of other nodes' restart counts.
	seed := rand.NewPCG(s.seed, id).Uint64()
	if incarnation != 0 {
		seed = rand.NewPCG(seed, incarnation).Uint64()
	}
	return raftcore.Config{
		ID: id, Voters: s.voters, Seed: seed,
		ElectionTicks: s.config.ElectionTicks, HeartbeatTicks: s.config.HeartbeatTicks,
	}
}

func (s *Simulator) Running(id uint64) bool { return s.nodes[id] != nil }

// Crash stops a configured node and power-crashes its disk. By default all
// unsynced writes are lost; one optional byte count retains a torn appended tail
// in each durably named file. It may also power-crash an already stopped node.
// Previously sent messages remain queued. Pending disk faults remain armed.
func (s *Simulator) Crash(id uint64, keepTailBytes ...int) error {
	if s.err != nil {
		return s.err
	}
	if s.disks[id] == nil || len(keepTailBytes) > 1 || (len(keepTailBytes) == 1 && keepTailBytes[0] < 0) {
		return errors.New("crash requires a voter and an optional nonnegative tail length")
	}
	keep := 0
	if len(keepTailBytes) == 1 {
		keep = keepTailBytes[0]
	}
	s.log("crash node=%d keep_tail=%d", id, keep)
	if err := s.stop(id); err != nil {
		return s.fail(err)
	}
	return s.disks[id].Crash(keep)
}

// stop discards only incarnation-local state. An injected I/O failure stops the
// process without a power loss; its unsynced disk image survives until Crash or
// Restart. Close does not sync. Global safety histories and queued traffic stay.
func (s *Simulator) stop(id uint64) error {
	delete(s.processors, s.nodes[id])
	delete(s.nodes, id)
	delete(s.observed, id)
	delete(s.appliedEntries, id)
	delete(s.stored, id)
	delete(s.storedEntries, id)
	if writer := s.wals[id]; writer != nil {
		delete(s.wals, id)
		return writer.Close()
	}
	return nil
}

// Restart recovers a stopped node's WAL and rebuilds a fresh application/core.
// Corrupt storage or injected recovery I/O failures leave that node down; they
// do not stop surviving nodes. A detected safety violation remains terminal.
func (s *Simulator) Restart(id uint64) error {
	if s.err != nil {
		return s.err
	}
	disk := s.disks[id]
	if disk == nil || s.Running(id) || s.incarnations[id] == math.MaxUint64 {
		return errors.New("restart requires a stopped voter and an available incarnation")
	}
	s.log("restart node=%d incarnation=%d", id, s.incarnations[id]+1)
	writer, state, err := wal.OpenFS(disk, ".")
	if err != nil {
		s.log("restart_rejected node=%d reason=%v", id, err)
		return err
	}
	s.wals[id], s.stored[id], s.storedEntries[id] = writer, state.HardState, state.Entries
	var applicationErr error
	core, err := raftio.Restore(s.nodeConfig(id, s.incarnations[id]+1), state, func(entry *raftv1.Entry) error {
		applicationErr = s.apply(id, entry)
		return applicationErr
	})
	if err != nil {
		err = errors.Join(err, s.stop(id))
		s.log("restart_rejected node=%d reason=%v", id, err)
		if applicationErr != nil {
			return s.fail(err)
		}
		return err
	}
	s.nodes[id] = core
	s.incarnations[id]++
	s.log("restarted node=%d seed=%d status=%+v", id, s.nodeConfig(id, s.incarnations[id]).Seed, core.Status())
	if err := s.finish(core, core.Status()); err != nil {
		return s.fail(errors.Join(err, s.stop(id)))
	}
	return nil
}

func (s *Simulator) InjectDiskFault(id uint64, fault DiskFault) error {
	if s.err != nil {
		return s.err
	}
	if s.disks[id] == nil {
		return errors.New("disk fault must target a voter")
	}
	return s.disks[id].Inject(fault)
}

func (s *Simulator) ClearDiskFault(id uint64) error {
	if s.err != nil {
		return s.err
	}
	if s.disks[id] == nil {
		return errors.New("disk fault must target a voter")
	}
	s.disks[id].ClearFault()
	return nil
}

// DiskFiles returns a sorted, caller-owned list for fault targeting/inspection.
func (s *Simulator) DiskFiles(id uint64) ([]string, bool) {
	if disk := s.disks[id]; disk != nil {
		return disk.Files(), true
	}
	return nil, false
}

// CorruptDisk flips a synced byte on a stopped node. Restart must detect it;
// this is persistent corruption, not a repairable incomplete tail.
func (s *Simulator) CorruptDisk(id uint64, name string, offset int64, mask byte) error {
	if s.err != nil {
		return s.err
	}
	if s.disks[id] == nil || s.Running(id) {
		return fmt.Errorf("corruption requires a stopped voter: %d", id)
	}
	return s.disks[id].Flip(name, offset, mask)
}
