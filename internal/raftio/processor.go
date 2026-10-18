// Package raftio connects the pure Raft core to synchronous persistence,
// nonblocking message delivery and ordered application. It owns no goroutines.
package raftio

import (
	"errors"
	"fmt"

	"github.com/zishan044/raft-replicated-key-value-store.git/internal/raftcore"
	"github.com/zishan044/raft-replicated-key-value-store.git/internal/wal"
	raftv1 "github.com/zishan044/raft-replicated-key-value-store.git/pkg/api/raft/v1"
)

// Processor consumes Ready batches for one core. The caller must own the core
// exclusively and process after every input, including its initial Ready.
// Callbacks must not reenter or mutate the core. Send must return promptly,
// dropping messages rather than waiting for delivery or queue space.
//
// Any failure is terminal: discard this processor and volatile application
// state, reopen/recover storage, then create a fresh core before resuming.
type Processor struct {
	core  *raftcore.Core
	save  func(*raftv1.WALBatch) error
	send  func([]*raftv1.Message)
	apply func(*raftv1.Entry) error
	err   error
}

// New attaches callbacks without consuming output. Save must durably retain
// its batch before succeeding. Save and send may retain their immutable input;
// apply must treat entries as read-only, including no-ops that advance position.
func New(core *raftcore.Core, save func(*raftv1.WALBatch) error, send func([]*raftv1.Message), apply func(*raftv1.Entry) error) (*Processor, error) {
	if core == nil || save == nil || send == nil || apply == nil {
		return nil, errors.New("core and save/send/apply callbacks are required")
	}
	return &Processor{core: core, save: save, send: send, apply: apply}, nil
}

// Process saves persistence changes before sending messages, applies committed
// entries, then advances the core. Empty and messages-only batches skip Save.
// A failed batch is never retried, sent or advanced by subsequent calls.
func (p *Processor) Process() error {
	if p.err != nil {
		return p.err
	}
	rd := p.core.Ready()
	if rd.HardState == nil && len(rd.Entries) == 0 && len(rd.Messages) == 0 && len(rd.CommittedEntries) == 0 {
		return nil
	}
	if rd.HardState != nil || len(rd.Entries) > 0 {
		batch := &raftv1.WALBatch{Entries: rd.Entries}
		if hs := rd.HardState; hs != nil {
			batch.HardState = &raftv1.PersistedHardState{Term: hs.Term, Vote: hs.Vote, Commit: hs.Commit}
		}
		if err := p.save(batch); err != nil {
			p.err = fmt.Errorf("persist Ready: %w", err)
			return p.err
		}
	}
	if len(rd.Messages) > 0 {
		p.send(rd.Messages)
	}
	p.err = applyAndAdvance(p.core, rd.CommittedEntries, p.apply)
	return p.err
}

// Restore constructs a fresh follower from state returned by a successfully
// synced WAL recovery. cfg supplies identity, membership and timing; its initial
// hard state and entries are replaced by state. Application must start empty.
// The initial committed prefix is replayed without resaving the recovered log.
// On failure, no core is returned and partial application must be discarded.
func Restore(cfg raftcore.Config, state wal.State, apply func(*raftv1.Entry) error) (*raftcore.Core, error) {
	if apply == nil {
		return nil, errors.New("application callback is required")
	}
	cfg.HardState, cfg.Entries = state.HardState, state.Entries
	core, err := raftcore.New(cfg)
	if err != nil {
		return nil, err
	}
	rd := core.Ready()
	if err := applyAndAdvance(core, rd.CommittedEntries, apply); err != nil {
		return nil, err
	}
	return core, nil
}

func applyAndAdvance(core *raftcore.Core, entries []*raftv1.Entry, apply func(*raftv1.Entry) error) error {
	for _, entry := range entries {
		if err := apply(entry); err != nil {
			return fmt.Errorf("apply index %d: %w", entry.Index, err)
		}
	}
	return core.Advance()
}
