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
	raftv1 "github.com/zishan044/raft-replicated-key-value-store.git/pkg/api/raft/v1"
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

// Simulator must be owned by one goroutine. A failed Tick is terminal; later
// Tick/Run calls return the original failure with its seed and logical time.
type Simulator struct {
	seed    uint64
	now     int
	voters  []uint64
	nodes   map[uint64]*raftcore.Core
	stored  map[uint64]raftcore.HardState
	leaders map[uint64]uint64 // term -> first leader ever observed in that term
	rng     *rand.Rand
	faults  Faults
	blocked map[link]bool
	queue   []delivery
	serial  uint64
	trace   []string
	err     error
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
		seed: cfg.Seed, voters: cfg.Voters,
		nodes: make(map[uint64]*raftcore.Core), stored: make(map[uint64]raftcore.HardState),
		leaders: make(map[uint64]uint64), blocked: make(map[link]bool),
		rng: rand.New(rand.NewPCG(cfg.Seed, 0)), faults: Faults{MinDelay: 1, MaxDelay: 1},
	}
	s.log("config %+v", cfg)
	for _, id := range s.voters {
		// Node IDs are positive, so these streams are distinct from the network's.
		seed := rand.NewPCG(cfg.Seed, id).Uint64()
		core, err := raftcore.New(raftcore.Config{
			ID: id, Voters: s.voters, Seed: seed,
			ElectionTicks: cfg.ElectionTicks, HeartbeatTicks: cfg.HeartbeatTicks,
		})
		if err != nil {
			return nil, err
		}
		s.nodes[id] = core
		s.log("node id=%d seed=%d timeout=%d", id, seed, core.Status().ElectionTimeout)
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
	if from == to || s.nodes[from] == nil || s.nodes[to] == nil {
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
		before := core.Status()
		if err := core.Tick(); err != nil {
			return s.fail(err)
		}
		if err := s.finish(core, before); err != nil {
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

func (s *Simulator) Now() int { return s.now }

// Statuses returns independent value snapshots in ascending voter ID order.
func (s *Simulator) Statuses() []raftcore.Status {
	statuses := make([]raftcore.Status, 0, len(s.voters))
	for _, id := range s.voters {
		statuses = append(statuses, s.nodes[id].Status())
	}
	return statuses
}

func (s *Simulator) SavedState(id uint64) (raftcore.HardState, bool) {
	state, ok := s.stored[id]
	return state, ok
}

// Trace includes the seed, configuration, controls, deliveries and transitions.
// Repeating the same control calls at the same ticks reproduces it byte for byte.
func (s *Simulator) Trace() string { return strings.Join(s.trace, "\n") + "\n" }

func (s *Simulator) finish(core *raftcore.Core, before raftcore.Status) error {
	status := core.Status()
	if before.Role != status.Role || before.HardState != status.HardState || before.LeaderID != status.LeaderID {
		s.log("state %+v", status)
	}
	if err := s.checkLeader(status); err != nil {
		return err
	}
	rd := core.Ready()
	if rd.HardState != nil {
		s.stored[status.ID] = *rd.HardState
		s.log("save node=%d term=%d vote=%d", status.ID, rd.HardState.Term, rd.HardState.Vote)
	}
	for _, msg := range rd.Messages {
		s.enqueue(msg)
	}
	if rd.HardState != nil || len(rd.Messages) > 0 {
		return core.Advance()
	}
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
	return fmt.Sprintf("%s from=%d to=%d term=%d reject=%t last_index=%d last_term=%d",
		msg.Type, msg.From, msg.To, msg.Term, msg.Reject, msg.LastLogIndex, msg.LastLogTerm)
}
