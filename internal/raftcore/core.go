// Package raftcore implements the single-threaded Raft state machine. Callers
// supply logical ticks and messages; the core performs no I/O and reads no clock.
// Leader heartbeats are added in subset 3; this subset implements elections.
package raftcore

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"

	raftv1 "github.com/zishan044/raft-replicated-key-value-store.git/pkg/api/raft/v1"
	"google.golang.org/protobuf/proto"
)

type Role string

const (
	Follower  Role = "follower"
	Candidate Role = "candidate"
	Leader    Role = "leader"
)

var (
	ErrReadyPending       = errors.New("advance the outstanding Ready before supplying input")
	ErrNoReady            = errors.New("no outstanding Ready to advance")
	ErrUnsupportedMessage = errors.New("message type is not implemented")
)

// HardState is the election state to retain before releasing dependent messages.
// M1 consumers retain it in memory; this does not provide crash durability.
type HardState struct {
	Term uint64
	Vote uint64 // zero means no vote in this term
}

type Config struct {
	ID     uint64
	Voters []uint64 // the complete fixed membership, including ID; copied by New

	// Intervals must be positive and ElectionTicks must exceed HeartbeatTicks.
	// The election deadline is sampled from [ElectionTicks, 2*ElectionTicks).
	ElectionTicks  int
	HeartbeatTicks int
	Seed           uint64 // zero is a valid deterministic seed

	HardState HardState
	// Fixed metadata until M2 introduces the mutable log.
	LastLogIndex uint64
	LastLogTerm  uint64
}

// Ready is a caller-owned snapshot. A nil HardState means it is unchanged.
// Process exactly one snapshot per batch: retain HardState, release Messages,
// then Advance. Repeated Ready calls return independent copies of the same batch.
type Ready struct {
	HardState *HardState
	Messages  []*raftv1.Message
}

// Status is a value snapshot and can be changed without affecting the core.
type Status struct {
	ID              uint64
	Role            Role
	LeaderID        uint64
	HardState       HardState
	LastLogIndex    uint64
	LastLogTerm     uint64
	ElectionElapsed int
	ElectionTimeout int
}

// Core must be owned by one goroutine. It is not safe for concurrent use.
type Core struct {
	id             uint64
	voters         []uint64
	electionTicks  int
	heartbeatTicks int
	rng            *rand.Rand
	role           Role
	leaderID       uint64
	hardState      HardState
	lastLogIndex   uint64
	lastLogTerm    uint64
	votes          map[uint64]bool

	electionElapsed int
	electionTimeout int
	messages        []*raftv1.Message
	savedState      HardState
	initialized     bool
	readyPending    bool
}

func New(cfg Config) (*Core, error) {
	if cfg.ID == 0 {
		return nil, errors.New("node ID must be positive")
	}
	voters := slices.Clone(cfg.Voters)
	slices.Sort(voters)
	for i, id := range voters {
		if id == 0 || (i > 0 && id == voters[i-1]) {
			return nil, errors.New("voters must be positive and unique")
		}
	}
	if !slices.Contains(voters, cfg.ID) {
		return nil, errors.New("voters must include the local node")
	}
	if cfg.HeartbeatTicks <= 0 || cfg.ElectionTicks <= cfg.HeartbeatTicks || cfg.ElectionTicks > int(^uint(0)>>1)/2 {
		return nil, errors.New("election ticks must exceed positive heartbeat ticks and allow doubling without overflow")
	}
	if cfg.HardState.Vote != 0 && (cfg.HardState.Term == 0 || !slices.Contains(voters, cfg.HardState.Vote)) {
		return nil, errors.New("initial vote must name a voter in a positive term")
	}
	if (cfg.LastLogIndex == 0) != (cfg.LastLogTerm == 0) || cfg.LastLogTerm > cfg.HardState.Term {
		return nil, errors.New("last log index and term must both be zero or positive, with log term no greater than current term")
	}
	rng := rand.New(rand.NewPCG(cfg.Seed, 0))
	return &Core{
		id:              cfg.ID,
		voters:          voters,
		electionTicks:   cfg.ElectionTicks,
		heartbeatTicks:  cfg.HeartbeatTicks,
		rng:             rng,
		role:            Follower,
		hardState:       cfg.HardState,
		lastLogIndex:    cfg.LastLogIndex,
		lastLogTerm:     cfg.LastLogTerm,
		electionTimeout: cfg.ElectionTicks + rng.IntN(cfg.ElectionTicks),
	}, nil
}

// Tick advances logical election time, campaigning when a non-leader times out.
// Consume the initial Ready before supplying the first input.
func (c *Core) Tick() error {
	if !c.initialized || c.readyPending {
		return ErrReadyPending
	}
	if c.role == Leader {
		return nil
	}
	if c.electionElapsed == c.electionTimeout-1 && c.hardState.Term == ^uint64(0) {
		return errors.New("cannot start election: term exhausted")
	}
	c.electionElapsed++
	if c.electionElapsed >= c.electionTimeout {
		c.role = Candidate
		c.leaderID = 0
		c.hardState.Term++
		c.hardState.Vote = c.id
		c.votes = map[uint64]bool{c.id: true}
		c.resetElectionTimer()
		for _, peer := range c.voters {
			if peer != c.id {
				c.messages = append(c.messages, &raftv1.Message{
					Type:         raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE,
					From:         c.id,
					To:           peer,
					Term:         c.hardState.Term,
					LastLogIndex: c.lastLogIndex,
					LastLogTerm:  c.lastLogTerm,
				})
			}
		}
		c.maybeBecomeLeader()
	}
	return nil
}

// Step handles RequestVote and its response without retaining the input.
// Unsupported or malformed messages never change term, vote, or election time.
func (c *Core) Step(msg *raftv1.Message) error {
	if !c.initialized || c.readyPending {
		return ErrReadyPending
	}
	if msg == nil || msg.To != c.id || msg.From == c.id || !slices.Contains(c.voters, msg.From) {
		return errors.New("message must be addressed to this node from another voter")
	}
	if msg.Type != raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE && msg.Type != raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE_RESPONSE {
		return fmt.Errorf("%w: %s", ErrUnsupportedMessage, msg.Type)
	}
	if msg.Term == 0 {
		return errors.New("election messages require a positive term")
	}
	if msg.Type == raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE &&
		(msg.Reject || (msg.LastLogIndex == 0) != (msg.LastLogTerm == 0) || msg.LastLogTerm > msg.Term) {
		return errors.New("invalid RequestVote log metadata or reject flag")
	}
	if msg.Term > c.hardState.Term {
		c.role = Follower
		c.leaderID = 0
		c.hardState = HardState{Term: msg.Term}
		c.votes = nil
		c.resetElectionTimer()
	}
	if msg.Type == raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE {
		upToDate := msg.LastLogTerm > c.lastLogTerm ||
			(msg.LastLogTerm == c.lastLogTerm && msg.LastLogIndex >= c.lastLogIndex)
		grant := msg.Term == c.hardState.Term && upToDate &&
			(c.hardState.Vote == 0 || c.hardState.Vote == msg.From)
		if grant {
			c.hardState.Vote = msg.From
			c.resetElectionTimer()
		}
		c.messages = append(c.messages, &raftv1.Message{
			Type:   raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE_RESPONSE,
			From:   c.id,
			To:     msg.From,
			Term:   c.hardState.Term,
			Reject: !grant,
		})
	} else if msg.Term == c.hardState.Term && c.role == Candidate {
		if _, seen := c.votes[msg.From]; !seen {
			c.votes[msg.From] = !msg.Reject
			c.maybeBecomeLeader()
		}
	}
	return nil
}

func (c *Core) resetElectionTimer() {
	c.electionElapsed = 0
	c.electionTimeout = c.electionTicks + c.rng.IntN(c.electionTicks)
}

func (c *Core) maybeBecomeLeader() {
	granted := 0
	for _, voter := range c.voters {
		if c.votes[voter] {
			granted++
		}
	}
	if granted > len(c.voters)/2 {
		c.role = Leader
		c.leaderID = c.id
		c.electionElapsed = 0
		c.votes = nil
	}
}

// Ready emits initial state once, then only state changes and queued messages.
// Calling Ready with no output does not create a batch requiring Advance.
func (c *Core) Ready() Ready {
	var rd Ready
	if !c.initialized || c.hardState != c.savedState {
		state := c.hardState
		rd.HardState = &state
	}
	for _, msg := range c.messages {
		rd.Messages = append(rd.Messages, proto.Clone(msg).(*raftv1.Message))
	}
	if rd.HardState != nil || len(rd.Messages) != 0 {
		c.readyPending = true
	}
	return rd
}

// Advance acknowledges that the outstanding batch was processed successfully.
// On a save/send failure, retry that batch rather than advancing the core.
func (c *Core) Advance() error {
	if !c.readyPending {
		return ErrNoReady
	}
	c.savedState = c.hardState
	c.initialized = true
	c.messages = nil
	c.readyPending = false
	return nil
}

func (c *Core) Status() Status {
	return Status{
		ID:              c.id,
		Role:            c.role,
		LeaderID:        c.leaderID,
		HardState:       c.hardState,
		LastLogIndex:    c.lastLogIndex,
		LastLogTerm:     c.lastLogTerm,
		ElectionElapsed: c.electionElapsed,
		ElectionTimeout: c.electionTimeout,
	}
}
