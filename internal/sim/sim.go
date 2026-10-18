// Package sim drives Raft cores with logical time and a seeded, in-memory
// network. All methods run synchronously; no goroutines or wall clocks are used.
package sim

import (
	"cmp"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"

	"github.com/zishan044/raft-replicated-key-value-store.git/internal/raftcore"
	"github.com/zishan044/raft-replicated-key-value-store.git/internal/raftio"
	"github.com/zishan044/raft-replicated-key-value-store.git/internal/wal"
	raftv1 "github.com/zishan044/raft-replicated-key-value-store.git/pkg/api/raft/v1"
	"google.golang.org/protobuf/proto"
)

type Config struct {
	Seed           uint64
	Voters         []uint64 // copied and sorted; positive, unique, and nonempty
	ElectionTicks  int      // zero defaults to 10
	HeartbeatTicks int      // zero defaults to 2
}

// Faults apply to newly sent messages. Partitions also drop queued messages at
// delivery. Duplicate messages get independent delays, and are not duplicated
// recursively. Reorder shuffles deliveries due on the same logical tick.
type Faults struct {
	DropPercent      int
	DuplicatePercent int
	MinDelay         int // both delays zero means reliable one-tick delivery
	MaxDelay         int // otherwise 1 <= MinDelay <= MaxDelay
	Reorder          bool
}

type link struct{ from, to uint64 }

type delivery struct {
	at       int
	sequence uint64
	message  *raftv1.Message // immutable, including across duplicate deliveries
}

// Simulator must be owned by one goroutine. Invariant and unexpected failures
// are terminal. Injected disk failures stop only the affected node.
type Simulator struct {
	config         Config
	seed           uint64
	now            int
	voters         []uint64
	nodes          map[uint64]*raftcore.Core
	processors     map[*raftcore.Core]*raftio.Processor
	disks          map[uint64]*Disk
	wals           map[uint64]*wal.WAL
	incarnations   map[uint64]uint64
	stored         map[uint64]raftcore.HardState
	storedEntries  map[uint64][]*raftv1.Entry
	appliedEntries map[uint64][]*raftv1.Entry
	leaders        map[uint64]uint64 // term -> first leader ever observed in that term
	leaderTerms    []uint64          // observation order, for deterministic history checks
	leaderLogs     map[uint64][]*raftv1.Entry
	observed       map[uint64]nodeView
	committed      []commitRecord  // first observed commitment at each index, never discarded
	applied        []*raftv1.Entry // canonical application history, across all nodes
	rng            *rand.Rand
	faults         Faults
	blocked        map[link]bool
	queue          []delivery
	serial         uint64
	trace          []string
	err            error
}

func New(cfg Config) (*Simulator, error) {
	cfg.Voters = slices.Clone(cfg.Voters)
	slices.Sort(cfg.Voters)
	if len(cfg.Voters) == 0 {
		return nil, errors.New("simulator requires at least one voter")
	}
	for i, id := range cfg.Voters {
		if id == 0 || (i > 0 && id == cfg.Voters[i-1]) {
			return nil, errors.New("simulator voters must be positive and unique")
		}
	}
	if cfg.ElectionTicks == 0 {
		cfg.ElectionTicks = 10
	}
	if cfg.HeartbeatTicks == 0 {
		cfg.HeartbeatTicks = 2
	}
	s := &Simulator{
		config: cfg, seed: cfg.Seed, voters: cfg.Voters,
		nodes: make(map[uint64]*raftcore.Core), stored: make(map[uint64]raftcore.HardState),
		processors: make(map[*raftcore.Core]*raftio.Processor),
		disks:      make(map[uint64]*Disk), wals: make(map[uint64]*wal.WAL), incarnations: make(map[uint64]uint64),
		storedEntries: make(map[uint64][]*raftv1.Entry), appliedEntries: make(map[uint64][]*raftv1.Entry),
		leaders: make(map[uint64]uint64), blocked: make(map[link]bool),
		leaderLogs: make(map[uint64][]*raftv1.Entry), observed: make(map[uint64]nodeView),
		rng: rand.New(rand.NewPCG(cfg.Seed, 0)), faults: Faults{MinDelay: 1, MaxDelay: 1},
	}
	s.log("config %+v", cfg)
	for _, id := range s.voters {
		nodeCfg := s.nodeConfig(id, 0)
		core, err := raftcore.New(nodeCfg)
		if err != nil {
			return nil, err
		}
		disk := NewDisk()
		disk.trace = func(event string) { s.log("disk node=%d %s", id, event) }
		writer, _, err := wal.OpenFS(disk, ".")
		if err != nil {
			return nil, err
		}
		s.disks[id], s.wals[id] = disk, writer
		s.nodes[id] = core
		s.log("node id=%d seed=%d timeout=%d", id, nodeCfg.Seed, core.Status().ElectionTimeout)
		if err := s.finish(core, core.Status()); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// SetFaults changes network behavior without rewriting already queued delays.
func (s *Simulator) SetFaults(f Faults) error {
	if f.MinDelay == 0 && f.MaxDelay == 0 {
		f.MinDelay, f.MaxDelay = 1, 1
	}
	if f.DropPercent < 0 || f.DropPercent > 100 || f.DuplicatePercent < 0 || f.DuplicatePercent > 100 ||
		f.MinDelay < 1 || f.MaxDelay < f.MinDelay || f.MaxDelay > int(^uint(0)>>1)/2 {
		return errors.New("invalid fault percentages or delivery delay range")
	}
	s.faults = f
	s.log("faults %+v", f)
	return nil
}

// SetBlocked controls one directed link. Block both directions for a symmetric
// partition, or multiple links to isolate a node/group. Unblocking never revives
// messages already dropped, but leaves other in-flight messages intact.
func (s *Simulator) SetBlocked(from, to uint64, blocked bool) error {
	if from == to || !slices.Contains(s.voters, from) || !slices.Contains(s.voters, to) {
		return errors.New("partition link must connect two distinct voters")
	}
	if blocked {
		s.blocked[link{from, to}] = true
	} else {
		delete(s.blocked, link{from, to})
	}
	s.log("link from=%d to=%d blocked=%t", from, to, blocked)
	return nil
}

// Heal restores reliable one-tick delivery, removes every partition, and makes
// all old in-flight messages due on the next Tick. It does not discard traffic.
// After that Tick, recovery tests can start their liveness measurement.
func (s *Simulator) Heal() {
	s.faults = Faults{MinDelay: 1, MaxDelay: 1}
	clear(s.blocked)
	for i := range s.queue {
		s.queue[i].at = s.now + 1
	}
	s.log("heal pending=%d", len(s.queue))
}

// Tick first ticks every core in ascending ID order, then delivers due traffic.
// New messages always wait at least one tick, preventing zero-time feedback.
func (s *Simulator) Tick() error {
	if s.err != nil {
		return s.err
	}
	s.now++
	s.log("tick")
	for _, id := range s.voters {
		core := s.nodes[id]
		if core == nil {
			continue
		}
		before := core.Status()
		if err := core.Tick(); err != nil {
			return s.fail(err)
		}
		if err := s.finish(core, before); err != nil {
			if errors.Is(err, ErrDiskFault) {
				continue
			}
			return s.fail(err)
		}
	}
	// ponytail: scan the small v0 queue; use a heap if long-delay workloads make
	// scanning queued messages a measured bottleneck.
	var due []delivery
	future := s.queue[:0]
	for _, event := range s.queue {
		if event.at <= s.now {
			due = append(due, event)
		} else {
			future = append(future, event)
		}
	}
	clear(s.queue[len(future):])
	s.queue = future
	slices.SortFunc(due, func(a, b delivery) int {
		if order := cmp.Compare(a.at, b.at); order != 0 {
			return order
		}
		return cmp.Compare(a.sequence, b.sequence)
	})
	if s.faults.Reorder {
		s.rng.Shuffle(len(due), func(i, j int) { due[i], due[j] = due[j], due[i] })
	}
	for _, event := range due {
		msg := event.message
		if s.nodes[msg.To] == nil {
			s.log("drop down delivery=%d node=%d", event.sequence, msg.To)
			continue
		}
		if s.blocked[link{msg.From, msg.To}] {
			s.log("drop partition delivery=%d", event.sequence)
			continue
		}
		s.log("deliver id=%d %s", event.sequence, describe(msg))
		core := s.nodes[msg.To]
		before := core.Status()
		if err := core.Step(msg); err != nil {
			return s.fail(err)
		}
		if err := s.finish(core, before); err != nil {
			if errors.Is(err, ErrDiskFault) {
				continue
			}
			return s.fail(err)
		}
	}
	return nil
}

func (s *Simulator) Run(ticks int) error {
	if s.err != nil {
		return s.err
	}
	if ticks < 0 {
		return errors.New("tick count must not be negative")
	}
	for range ticks {
		if err := s.Tick(); err != nil {
			return err
		}
	}
	return nil
}

// Propose delivers an opaque command to one node and processes its Ready batch.
// ErrNotLeader is a normal rejection. Acceptance returns a local log position;
// inspect AppliedEntries to establish application. A disk fault returns the
// accepted index/term with ErrDiskFault: its persistence outcome is uncertain.
// Other core failures are terminal.
func (s *Simulator) Propose(id uint64, data []byte) (index, term uint64, err error) {
	if s.err != nil {
		return 0, 0, s.err
	}
	core := s.nodes[id]
	if core == nil {
		if s.disks[id] != nil {
			return 0, 0, ErrNodeDown
		}
		return 0, 0, errors.New("proposal must target a voter")
	}
	before := core.Status()
	s.log("propose node=%d data=%x", id, data)
	index, term, err = core.Propose(data)
	if err != nil {
		s.log("proposal_rejected node=%d reason=%v", id, err)
		if errors.Is(err, raftcore.ErrNotLeader) {
			return 0, 0, err
		}
		return 0, 0, s.fail(err)
	}
	s.log("proposal_accepted node=%d index=%d term=%d", id, index, term)
	if err := s.finish(core, before); err != nil {
		if errors.Is(err, ErrDiskFault) {
			return index, term, err
		}
		return 0, 0, s.fail(err)
	}
	return index, term, nil
}

func (s *Simulator) Now() int { return s.now }

// Statuses returns running nodes' snapshots in ascending voter ID order.
func (s *Simulator) Statuses() []raftcore.Status {
	statuses := make([]raftcore.Status, 0, len(s.voters))
	for _, id := range s.voters {
		if core := s.nodes[id]; core != nil {
			statuses = append(statuses, core.Status())
		}
	}
	return statuses
}

// SavedState returns the running node's last successfully saved/recovered state.
func (s *Simulator) SavedState(id uint64) (raftcore.HardState, bool) {
	state, ok := s.stored[id]
	return state, ok
}

// AppliedEntries returns independent copies, including no-op positions.
func (s *Simulator) AppliedEntries(id uint64) ([]*raftv1.Entry, bool) {
	if s.nodes[id] == nil {
		return nil, false
	}
	var entries []*raftv1.Entry
	for _, entry := range s.appliedEntries[id] {
		entries = append(entries, proto.Clone(entry).(*raftv1.Entry))
	}
	return entries, true
}

// Trace includes the seed, configuration, controls, deliveries and transitions.
// Repeating the same control calls at the same ticks reproduces it byte for byte.
func (s *Simulator) Trace() string { return strings.Join(s.trace, "\n") + "\n" }

func (s *Simulator) finish(core *raftcore.Core, before raftcore.Status) error {
	status := core.Status()
	if before.Role != status.Role || before.HardState != status.HardState || before.LeaderID != status.LeaderID ||
		before.LastLogIndex != status.LastLogIndex || before.LastLogTerm != status.LastLogTerm {
		s.log("state %+v", status)
	}
	rd := core.Ready()
	previous, seen := s.observed[status.ID]
	changed := !seen || len(rd.Entries) > 0 || status.LastLogIndex != uint64(len(previous.entries)) || status.LastLogTerm != previous.status.LastLogTerm
	entries := previous.entries
	if changed {
		entries = core.Log()
	}
	// Count a commit only through the matching, locally durable log prefix.
	// A single voter's new entry may still be unsaved; conversely, losing the
	// commit-watermark write must not erase a commit whose entries were synced.
	retained := status
	stored := s.storedEntries[status.ID]
	limit := min(status.HardState.Commit, uint64(len(stored)), uint64(len(entries)))
	retained.HardState.Commit = 0
	for i := uint64(0); i < limit && sameEntry(entries[i], stored[i]); i++ {
		retained.HardState.Commit = i + 1
	}
	if err := s.checkLogs(nodeView{status: retained, entries: entries}, changed); err != nil {
		return err
	}
	processor := s.processors[core]
	if processor == nil {
		var err error
		processor, err = raftio.New(core,
			func(batch *raftv1.WALBatch) error {
				if err := s.wals[status.ID].Save(batch); err != nil {
					return err
				}
				return s.save(status.ID, batch)
			},
			func(messages []*raftv1.Message) {
				for _, msg := range messages {
					s.enqueue(msg)
				}
			},
			func(entry *raftv1.Entry) error { return s.apply(status.ID, entry) },
		)
		if err != nil {
			return err
		}
		s.processors[core] = processor
	}
	// ponytail: inspect a second Ready snapshot to keep the processor independent;
	// share one batch if profiling shows snapshot copies dominate simulation.
	if err := processor.Process(); err != nil {
		if errors.Is(err, ErrDiskFault) {
			s.log("disk_stop node=%d reason=%v", status.ID, err)
			if closeErr := s.stop(status.ID); closeErr != nil {
				return fmt.Errorf("stop node after %v: %w", err, closeErr)
			}
		}
		return err
	}
	return s.checkLogs(nodeView{status: core.Status(), entries: entries}, false)
}

func (s *Simulator) save(id uint64, batch *raftv1.WALBatch) error {
	if len(batch.Entries) > 0 {
		first := batch.Entries[0].Index
		stored := s.storedEntries[id]
		if first == 0 || first > uint64(len(stored))+1 {
			return fmt.Errorf("node %d: retained log has a gap before index %d", id, first)
		}
		// Save takes ownership of the immutable batch entries.
		s.storedEntries[id] = append(stored[:first-1], batch.Entries...)
		s.log("save_entries node=%d first=%d count=%d", id, first, len(batch.Entries))
		for _, entry := range batch.Entries {
			s.log("save_entry node=%d index=%d term=%d type=%s data=%x", id, entry.Index, entry.Term, entry.Type, entry.Data)
		}
	}
	if hs := batch.HardState; hs != nil {
		old := s.stored[id]
		s.stored[id] = raftcore.HardState{Term: hs.Term, Vote: hs.Vote, Commit: hs.Commit}
		s.log("save node=%d term=%d vote=%d commit=%d", id, hs.Term, hs.Vote, hs.Commit)
		if hs.Commit != old.Commit {
			s.log("commit node=%d index=%d", id, hs.Commit)
		}
	}
	return nil
}

func (s *Simulator) apply(id uint64, entry *raftv1.Entry) error {
	stored := s.storedEntries[id]
	if entry.Index != uint64(len(s.appliedEntries[id]))+1 || entry.Index > s.stored[id].Commit ||
		entry.Index > uint64(len(stored)) || !proto.Equal(entry, stored[entry.Index-1]) {
		return fmt.Errorf("node %d: application at index %d is out of order or not retained and committed", id, entry.Index)
	}
	if err := s.checkApplied(id, entry); err != nil {
		return err
	}
	s.appliedEntries[id] = append(s.appliedEntries[id], entry)
	s.log("apply node=%d index=%d term=%d type=%s data=%x", id, entry.Index, entry.Term, entry.Type, entry.Data)
	return nil
}

func (s *Simulator) checkLeader(status raftcore.Status) error {
	if status.Role == raftcore.Leader {
		term := status.HardState.Term
		if leader, exists := s.leaders[term]; exists {
			if leader != status.ID {
				return fmt.Errorf("election safety: nodes %d and %d became leader in term %d", leader, status.ID, term)
			}
		} else {
			s.leaders[term] = status.ID
			s.leaderTerms = append(s.leaderTerms, term)
			s.log("leader node=%d term=%d", status.ID, term)
		}
	}
	return nil
}

func (s *Simulator) enqueue(msg *raftv1.Message) {
	if s.blocked[link{msg.From, msg.To}] {
		s.log("drop partition send %s", describe(msg))
		return
	}
	if s.faults.DropPercent > 0 && s.rng.IntN(100) < s.faults.DropPercent {
		s.log("drop random %s", describe(msg))
		return
	}
	copies := 1
	if s.faults.DuplicatePercent > 0 && s.rng.IntN(100) < s.faults.DuplicatePercent {
		copies++
		s.log("duplicate %s", describe(msg))
	}
	for range copies {
		delay := s.faults.MinDelay + s.rng.IntN(s.faults.MaxDelay-s.faults.MinDelay+1)
		s.serial++
		s.queue = append(s.queue, delivery{at: s.now + delay, sequence: s.serial, message: msg})
		s.log("enqueue id=%d due=%d %s", s.serial, s.now+delay, describe(msg))
	}
}

func (s *Simulator) log(format string, args ...any) {
	s.trace = append(s.trace, fmt.Sprintf("tick=%d ", s.now)+fmt.Sprintf(format, args...))
}

func (s *Simulator) fail(err error) error {
	s.err = fmt.Errorf("seed=%d tick=%d: %w", s.seed, s.now, err)
	s.log("failure %v", s.err)
	return s.err
}

func describe(msg *raftv1.Message) string {
	return fmt.Sprintf("%s from=%d to=%d term=%d reject=%t last_index=%d last_term=%d prev_index=%d prev_term=%d entries=%d commit=%d match=%d conflict_term=%d conflict_index=%d",
		msg.Type, msg.From, msg.To, msg.Term, msg.Reject, msg.LastLogIndex, msg.LastLogTerm,
		msg.PrevLogIndex, msg.PrevLogTerm, len(msg.Entries), msg.LeaderCommit, msg.MatchIndex, msg.ConflictTerm, msg.ConflictIndex)
}
