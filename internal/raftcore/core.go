// Package raftcore implements the single-threaded Raft state machine. Callers
// supply logical ticks and messages; the core performs no I/O and reads no clock.
// It implements elections and in-memory log replication with ordered apply.
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
	ErrNotLeader          = errors.New("not leader")
)

type progress struct {
	next, match uint64
}

// HardState is retained with log entries before releasing dependent messages.
// Consumers currently retain it in memory; this does not provide crash durability.
type HardState struct {
	Term   uint64
	Vote   uint64 // zero means no vote in this term
	Commit uint64
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
	// Entries is the complete initial log, starting at index 1; New copies it.
	// The initial Ready emits the entire log for retention and its committed
	// prefix for application. There is no previously applied state or snapshot.
	Entries []*raftv1.Entry
}

// Ready is a caller-owned snapshot. A nil HardState means it is unchanged.
// Process exactly one snapshot per batch: retain Entries and HardState, release
// Messages, apply CommittedEntries in order, then Advance. Repeated Ready calls
// return independent copies of the same batch, including nested entry data.
type Ready struct {
	HardState *HardState
	// Entries replaces the retained suffix starting at Entries[0].Index.
	Entries  []*raftv1.Entry
	Messages []*raftv1.Message
	// CommittedEntries includes no-ops; consumers advance their applied position
	// for every entry, executing only normal commands.
	CommittedEntries []*raftv1.Entry
}

// Status is a value snapshot and can be changed without affecting the core.
type Status struct {
	ID              uint64
	Role            Role
	LeaderID        uint64
	HardState       HardState
	LastLogIndex    uint64
	LastLogTerm     uint64
	AppliedIndex    uint64 // acknowledged by Advance, never merely by Ready
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
	log            []*raftv1.Entry // index i is stored at log[i-1]; no compaction yet
	unstableFrom   uint64          // first index needing retention; last index + 1 if none
	appliedIndex   uint64
	votes          map[uint64]bool
	progress       map[uint64]*progress // remote voters only; reset on leadership change

	electionElapsed  int
	electionTimeout  int
	heartbeatElapsed int
	messages         []*raftv1.Message
	savedState       HardState
	initialized      bool
	readyPending     bool
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
	if err := ValidateEntries(cfg.Entries, 0, 0, cfg.HardState.Term); err != nil {
		return nil, err
	}
	if cfg.HardState.Commit > uint64(len(cfg.Entries)) {
		return nil, errors.New("initial commit index exceeds the last log index")
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
		log:             cloneEntries(cfg.Entries),
		unstableFrom:    1,
		electionTimeout: cfg.ElectionTicks + rng.IntN(cfg.ElectionTicks),
	}, nil
}

// Tick campaigns when a non-leader times out, or emits periodic leader heartbeats.
// Consume the initial Ready before supplying the first input.
func (c *Core) Tick() error {
	if !c.initialized || c.readyPending {
		return ErrReadyPending
	}
	if c.role == Leader {
		c.heartbeatElapsed++
		if c.heartbeatElapsed >= c.heartbeatTicks {
			c.heartbeatElapsed = 0
			c.broadcastAppend()
		}
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
					LastLogIndex: c.lastLogIndex(),
					LastLogTerm:  c.lastLogTerm(),
				})
			}
		}
		c.maybeBecomeLeader()
	}
	return nil
}

// Step handles votes, AppendEntries and append responses, copying retained
// entries. Unsupported, malformed or committed-conflicting messages leave the
// core unchanged. Stale appends and log-prefix mismatches produce rejections.
func (c *Core) Step(msg *raftv1.Message) error {
	if !c.initialized || c.readyPending {
		return ErrReadyPending
	}
	if msg == nil || msg.To != c.id || msg.From == c.id || !slices.Contains(c.voters, msg.From) {
		return errors.New("message must be addressed to this node from another voter")
	}
	switch msg.Type {
	case raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE:
		if msg.Reject || (msg.LastLogIndex == 0) != (msg.LastLogTerm == 0) || msg.LastLogTerm > msg.Term {
			return errors.New("invalid RequestVote log metadata or reject flag")
		}
	case raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE_RESPONSE:
	case raftv1.MessageType_MESSAGE_TYPE_APPEND_ENTRIES:
		if msg.Reject || msg.LastLogIndex != 0 || msg.LastLogTerm != 0 || msg.MatchIndex != 0 ||
			msg.ConflictTerm != 0 || msg.ConflictIndex != 0 ||
			(msg.PrevLogIndex == 0) != (msg.PrevLogTerm == 0) || msg.PrevLogTerm > msg.Term {
			return errors.New("invalid AppendEntries metadata or response fields")
		}
		if err := ValidateEntries(msg.Entries, msg.PrevLogIndex, msg.PrevLogTerm, msg.Term); err != nil {
			return err
		}
	case raftv1.MessageType_MESSAGE_TYPE_APPEND_ENTRIES_RESPONSE:
		if msg.LastLogIndex != 0 || msg.LastLogTerm != 0 || msg.PrevLogTerm != 0 || len(msg.Entries) != 0 || msg.LeaderCommit != 0 ||
			(msg.Reject && (msg.MatchIndex != 0 || msg.ConflictTerm > msg.Term || msg.ConflictIndex > msg.PrevLogIndex ||
				(msg.ConflictTerm != 0 && msg.ConflictIndex == 0))) ||
			(!msg.Reject && (msg.MatchIndex < msg.PrevLogIndex || msg.ConflictTerm != 0 || msg.ConflictIndex != 0)) {
			return errors.New("invalid AppendEntries response fields")
		}
		if msg.Term == c.hardState.Term && c.role == Leader &&
			(msg.PrevLogIndex > c.lastLogIndex() || (!msg.Reject && msg.MatchIndex > c.lastLogIndex())) {
			return errors.New("append response exceeds the leader log")
		}
	default:
		return fmt.Errorf("%w: %s", ErrUnsupportedMessage, msg.Type)
	}
	if msg.Type != raftv1.MessageType_MESSAGE_TYPE_APPEND_ENTRIES && msg.Type != raftv1.MessageType_MESSAGE_TYPE_APPEND_ENTRIES_RESPONSE &&
		(msg.PrevLogIndex != 0 || msg.PrevLogTerm != 0 || len(msg.Entries) != 0 ||
			msg.LeaderCommit != 0 || msg.MatchIndex != 0 || msg.ConflictTerm != 0 || msg.ConflictIndex != 0) {
		return errors.New("election messages must not carry replication fields")
	}
	if msg.Term == 0 {
		return errors.New("messages require a positive term")
	}
	if msg.Type == raftv1.MessageType_MESSAGE_TYPE_APPEND_ENTRIES {
		return c.stepAppend(msg)
	}
	if msg.Term > c.hardState.Term {
		c.becomeFollower(msg.Term, 0)
	}
	if msg.Type == raftv1.MessageType_MESSAGE_TYPE_APPEND_ENTRIES_RESPONSE {
		if msg.Term == c.hardState.Term && c.role == Leader {
			c.stepAppendResponse(msg)
		}
		return nil
	}
	if msg.Type == raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE {
		upToDate := msg.LastLogTerm > c.lastLogTerm() ||
			(msg.LastLogTerm == c.lastLogTerm() && msg.LastLogIndex >= c.lastLogIndex())
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

// Propose appends an owned copy of data on the leader and returns its position.
// Success means local acceptance, not commitment; observe Ready for commitment.
// On ErrNotLeader, Status supplies the most recently known LeaderID.
func (c *Core) Propose(data []byte) (index, term uint64, err error) {
	if !c.initialized || c.readyPending {
		return 0, 0, ErrReadyPending
	}
	if c.role != Leader {
		return 0, 0, ErrNotLeader
	}
	index = c.appendEntry(raftv1.EntryType_ENTRY_TYPE_NORMAL, data)
	c.maybeCommit()
	c.broadcastAppend()
	return index, c.hardState.Term, nil
}

func (c *Core) stepAppend(msg *raftv1.Message) error {
	response := &raftv1.Message{
		Type: raftv1.MessageType_MESSAGE_TYPE_APPEND_ENTRIES_RESPONSE,
		From: c.id, To: msg.From, Term: c.hardState.Term,
		PrevLogIndex: msg.PrevLogIndex, Reject: true,
	}
	if msg.Term < c.hardState.Term {
		c.messages = append(c.messages, response)
		return nil
	}
	appendFrom := len(msg.Entries)
	switch {
	case msg.PrevLogIndex > c.lastLogIndex():
		response.ConflictIndex = c.lastLogIndex() + 1
	case msg.PrevLogIndex > 0 && c.log[msg.PrevLogIndex-1].Term != msg.PrevLogTerm:
		response.ConflictTerm = c.log[msg.PrevLogIndex-1].Term
		response.ConflictIndex = msg.PrevLogIndex
		for response.ConflictIndex > 1 && c.log[response.ConflictIndex-2].Term == response.ConflictTerm {
			response.ConflictIndex--
		}
	default:
		response.Reject = false
		response.MatchIndex = msg.PrevLogIndex + uint64(len(msg.Entries))
		for i, entry := range msg.Entries {
			if entry.Index > c.lastLogIndex() || c.log[entry.Index-1].Term != entry.Term {
				if entry.Index <= c.hardState.Commit {
					return fmt.Errorf("append conflicts with committed entry at index %d", entry.Index)
				}
				appendFrom = i
				break
			}
		}
	}
	// All error-producing validation precedes mutation. Even a prefix mismatch
	// proves leader contact in this term and resets the election timeout.
	c.becomeFollower(msg.Term, msg.From)
	response.Term = c.hardState.Term
	if !response.Reject {
		if appendFrom < len(msg.Entries) {
			first := msg.Entries[appendFrom].Index
			c.log = append(c.log[:first-1], cloneEntries(msg.Entries[appendFrom:])...)
			c.unstableFrom = min(c.unstableFrom, first)
		}
		c.hardState.Commit = max(c.hardState.Commit, min(msg.LeaderCommit, response.MatchIndex))
	}
	c.messages = append(c.messages, response)
	return nil
}

func (c *Core) resetElectionTimer() {
	c.electionElapsed = 0
	c.electionTimeout = c.electionTicks + c.rng.IntN(c.electionTicks)
}

func (c *Core) becomeFollower(term, leaderID uint64) {
	if term > c.hardState.Term {
		c.hardState.Term = term
		c.hardState.Vote = 0
	}
	c.role = Follower
	c.leaderID = leaderID
	c.votes = nil
	c.progress = nil
	c.heartbeatElapsed = 0
	c.resetElectionTimer()
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
		c.heartbeatElapsed = 0
		c.votes = nil
		c.progress = make(map[uint64]*progress)
		for _, peer := range c.voters {
			if peer != c.id {
				c.progress[peer] = &progress{next: c.lastLogIndex() + 1}
			}
		}
		c.appendEntry(raftv1.EntryType_ENTRY_TYPE_NO_OP, nil)
		c.maybeCommit()
		c.broadcastAppend()
	}
}

func (c *Core) appendEntry(kind raftv1.EntryType, data []byte) uint64 {
	index := c.lastLogIndex() + 1
	c.log = append(c.log, &raftv1.Entry{Index: index, Term: c.hardState.Term, Type: kind, Data: slices.Clone(data)})
	return index
}

func (c *Core) broadcastAppend() {
	for _, peer := range c.voters {
		if peer != c.id {
			c.sendAppend(peer)
		}
	}
}

func (c *Core) sendAppend(peer uint64) {
	// ponytail: full-suffix retries duplicate bytes; add bounded batches and
	// pipelining if measured replication throughput needs them.
	previous := c.progress[peer].next - 1
	var previousTerm uint64
	if previous > 0 {
		previousTerm = c.log[previous-1].Term
	}
	c.messages = append(c.messages, &raftv1.Message{
		Type: raftv1.MessageType_MESSAGE_TYPE_APPEND_ENTRIES,
		From: c.id, To: peer, Term: c.hardState.Term,
		PrevLogIndex: previous, PrevLogTerm: previousTerm, LeaderCommit: c.hardState.Commit,
		// Own the slice before future suffix replacement; log entry objects are immutable.
		Entries: slices.Clone(c.log[previous:]),
	})
}

func (c *Core) stepAppendResponse(msg *raftv1.Message) {
	p := c.progress[msg.From]
	if msg.Reject {
		// Correlate the rejection with the current probe, and never undo a
		// successful acknowledgment even if an older rejection arrives later.
		if msg.PrevLogIndex != p.next-1 || msg.PrevLogIndex <= p.match || msg.ConflictIndex == 0 {
			return
		}
		next := msg.ConflictIndex
		if msg.ConflictTerm != 0 {
			for i := c.lastLogIndex(); i > 0; i-- {
				if c.log[i-1].Term == msg.ConflictTerm {
					next = i + 1
					break
				}
			}
		}
		p.next = max(p.match+1, min(next, p.next-1))
		c.sendAppend(msg.From)
		return
	}
	if msg.MatchIndex <= p.match {
		return
	}
	p.match = msg.MatchIndex
	p.next = max(p.next, p.match+1)
	if c.maybeCommit() {
		c.broadcastAppend()
	} else if p.next <= c.lastLogIndex() {
		c.sendAppend(msg.From)
	}
}

func (c *Core) maybeCommit() bool {
	matched := make([]uint64, 0, len(c.voters))
	for _, peer := range c.voters {
		if peer == c.id {
			matched = append(matched, c.lastLogIndex())
		} else {
			matched = append(matched, c.progress[peer].match)
		}
	}
	slices.Sort(matched)
	// The largest index held by a strict majority, including even-sized groups.
	index := matched[(len(matched)-1)/2]
	if index > c.hardState.Commit && c.log[index-1].Term == c.hardState.Term {
		c.hardState.Commit = index
		return true
	}
	return false
}

// Ready emits state changes, changed log suffixes, messages and committed entries.
// Calling Ready with no output does not create a batch requiring Advance.
func (c *Core) Ready() Ready {
	var rd Ready
	if !c.initialized || c.hardState != c.savedState {
		state := c.hardState
		rd.HardState = &state
	}
	rd.Entries = cloneEntries(c.log[c.unstableFrom-1:])
	for _, msg := range c.messages {
		rd.Messages = append(rd.Messages, proto.Clone(msg).(*raftv1.Message))
	}
	rd.CommittedEntries = cloneEntries(c.log[c.appliedIndex:c.hardState.Commit])
	if rd.HardState != nil || len(rd.Entries) != 0 || len(rd.Messages) != 0 || len(rd.CommittedEntries) != 0 {
		c.readyPending = true
	}
	return rd
}

// Advance acknowledges that the outstanding batch was processed successfully.
// On a save/send/apply failure, finish the outstanding batch before advancing.
// Consumers must track partial application so retrying does not execute it twice.
func (c *Core) Advance() error {
	if !c.readyPending {
		return ErrNoReady
	}
	c.savedState = c.hardState
	c.unstableFrom = c.lastLogIndex() + 1
	c.appliedIndex = c.hardState.Commit
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
		LastLogIndex:    c.lastLogIndex(),
		LastLogTerm:     c.lastLogTerm(),
		AppliedIndex:    c.appliedIndex,
		ElectionElapsed: c.electionElapsed,
		ElectionTimeout: c.electionTimeout,
	}
}

// Log returns a caller-owned snapshot of the complete, uncompacted log.
func (c *Core) Log() []*raftv1.Entry { return cloneEntries(c.log) }

func (c *Core) lastLogIndex() uint64 { return uint64(len(c.log)) }

func (c *Core) lastLogTerm() uint64 {
	if len(c.log) == 0 {
		return 0
	}
	return c.log[len(c.log)-1].Term
}

// ValidateEntries checks initial logs, incoming appends and persisted suffixes.
// The preceding position anchors index continuity and nondecreasing terms;
// term is the maximum permitted entry term. An empty suffix is valid.
func ValidateEntries(entries []*raftv1.Entry, prevIndex, prevTerm, term uint64) error {
	if uint64(len(entries)) > ^uint64(0)-prevIndex {
		return errors.New("entry indices overflow uint64")
	}
	for i, entry := range entries {
		if entry == nil || entry.Index != prevIndex+uint64(i)+1 || entry.Term == 0 ||
			entry.Term < prevTerm || entry.Term > term {
			return errors.New("entries must have contiguous indices and positive, nondecreasing terms no greater than current term")
		}
		if entry.Type != raftv1.EntryType_ENTRY_TYPE_NORMAL && entry.Type != raftv1.EntryType_ENTRY_TYPE_NO_OP {
			return errors.New("unsupported entry type")
		}
		if entry.Type == raftv1.EntryType_ENTRY_TYPE_NO_OP && len(entry.Data) != 0 {
			return errors.New("no-op entries must not carry data")
		}
		prevTerm = entry.Term
	}
	return nil
}

func cloneEntries(entries []*raftv1.Entry) []*raftv1.Entry {
	var cloned []*raftv1.Entry
	for _, entry := range entries {
		cloned = append(cloned, proto.Clone(entry).(*raftv1.Entry))
	}
	return cloned
}
