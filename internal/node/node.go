// Package node owns a persistent Raft core, KV state machine and request waiters.
// Timing is explicit: callers supply ticks; no clocks run inside the runtime.
package node

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/zishan044/raft-replicated-key-value-store.git/internal/kv"
	"github.com/zishan044/raft-replicated-key-value-store.git/internal/raftcore"
	"github.com/zishan044/raft-replicated-key-value-store.git/internal/raftio"
	kvv1 "github.com/zishan044/raft-replicated-key-value-store.git/pkg/api/kv/v1"
	raftv1 "github.com/zishan044/raft-replicated-key-value-store.git/pkg/api/raft/v1"
	"google.golang.org/protobuf/proto"
)

var (
	ErrStopped = errors.New("node stopped")
	ErrUnknown = errors.New("operation may have applied")
)

type NotLeaderError struct{ LeaderID uint64 }

func (e *NotLeaderError) Error() string {
	return fmt.Sprintf("not leader; known leader: %d", e.LeaderID)
}
func (e *NotLeaderError) Unwrap() error { return raftcore.ErrNotLeader }

type Config struct {
	DataDir string
	// Identity, membership and timing only; initial state comes from the WAL.
	Raft raftcore.Config
	// Send follows raft.Transport.Send's nonblocking, immutable ownership rules.
	Send func([]*raftv1.Message)
}

type Status struct {
	Raft    raftcore.Status
	Pending int
}

type operation uint8

const (
	propose operation = iota
	step
	tick
	status
)

type request struct {
	kind    operation
	ctx     context.Context
	data    []byte
	message *raftv1.Message
	reply   chan response
	// Assigned and read exclusively by the owner goroutine.
	index, term uint64
}

type response struct {
	result *kvv1.CommandResult
	status Status
	err    error
}

type event struct {
	request *request
	cancel  *request
}

// Node is safe for concurrent use. Its owner serializes all core/application I/O.
type Node struct {
	events chan event
	stop   chan struct{}
	done   chan struct{}
	once   sync.Once
	err    error // published by closing done
}

// Open locks storage and replays the committed prefix before starting the owner.
// Failed startup releases all handles. A successful Open must be closed.
func Open(cfg Config) (*Node, error) {
	if cfg.DataDir == "" || cfg.Send == nil {
		return nil, errors.New("data directory and nonblocking send callback are required")
	}
	if cfg.Raft.HardState != (raftcore.HardState{}) || len(cfg.Raft.Entries) != 0 {
		return nil, errors.New("initial Raft state must come from the WAL")
	}
	if _, err := raftcore.New(cfg.Raft); err != nil {
		return nil, err
	}
	s, state, err := openStorage(cfg)
	if err != nil {
		return nil, err
	}
	machine := new(kv.Machine)
	core, err := raftio.Restore(cfg.Raft, state, func(entry *raftv1.Entry) error {
		_, err := machine.Apply(entry)
		return err
	})
	if err != nil {
		return nil, errors.Join(err, s.close())
	}
	n, err := start(core, machine, s.wal.Save, cfg.Send, s.close)
	if err != nil {
		return nil, errors.Join(err, s.close())
	}
	return n, nil
}

// Submit validates and copies a command, then waits for its committed result.
// Errors before handoff are definite rejections. Cancellation after handoff is
// conservatively Unknown; canceling a waiter never cancels its log entry.
// Application failures such as CAS conflicts are returned in result.Error.
func (n *Node) Submit(ctx context.Context, cmd *kvv1.Command) (*kvv1.CommandResult, error) {
	if err := kv.Validate(cmd); err != nil {
		return nil, err
	}
	if proto.Size(cmd) > raftcore.MaxCommandBytes {
		return nil, raftcore.ErrCommandTooLarge
	}
	data, err := proto.Marshal(cmd)
	if err != nil {
		return nil, err
	}
	r := n.call(&request{kind: propose, ctx: ctx, data: data})
	return r.result, r.err
}

// Step copies the peer message. A malformed input returns an error without
// stopping the node. Persistence or application failure is terminal.
func (n *Node) Step(ctx context.Context, msg *raftv1.Message) error {
	if msg == nil {
		return errors.New("peer message is required")
	}
	return n.call(&request{kind: step, ctx: ctx, message: proto.Clone(msg).(*raftv1.Message)}).err
}

func (n *Node) Tick(ctx context.Context) error {
	return n.call(&request{kind: tick, ctx: ctx}).err
}

func (n *Node) Status(ctx context.Context) (Status, error) {
	r := n.call(&request{kind: status, ctx: ctx})
	return r.status, r.err
}

func (n *Node) Done() <-chan struct{} { return n.done }

// Err reports a terminal error once Done closes; normal shutdown returns nil.
func (n *Node) Err() error {
	select {
	case <-n.done:
		return n.err
	default:
		return nil
	}
}

// Close is idempotent. It waits for in-progress synchronous I/O, resolves
// pending requests as Unknown, then closes the WAL and releases the lock.
func (n *Node) Close() error {
	n.once.Do(func() { close(n.stop) })
	<-n.done
	return n.err
}

func (n *Node) call(req *request) response {
	if err := req.ctx.Err(); err != nil {
		return response{err: err}
	}
	req.reply = make(chan response, 1)
	select {
	case <-req.ctx.Done():
		return response{err: req.ctx.Err()}
	case <-n.done:
		return response{err: errors.Join(ErrStopped, n.err)}
	case n.events <- event{request: req}:
	}
	var err error
	select {
	case r := <-req.reply:
		return r
	case <-req.ctx.Done():
		err = req.ctx.Err()
		if req.kind == propose {
			// Do not wait behind synchronous I/O, and do not cancel cleanup
			// when this caller returns. The owner may still hold its waiter.
			go func() {
				select {
				case n.events <- event{cancel: req}:
				case <-n.done:
				}
			}()
		}
	case <-n.done:
		err = errors.Join(ErrStopped, n.err)
	}
	// Prefer a completed result over concurrent cancellation/shutdown.
	select {
	case r := <-req.reply:
		return r
	default:
	}
	if req.kind == propose {
		err = errors.Join(ErrUnknown, err)
	}
	return response{err: err}
}

// Callback boundaries reuse Processor's persistence and fault-injection model.
// The caller relinquishes core and machine ownership on success.
func start(core *raftcore.Core, machine *kv.Machine, save func(*raftv1.WALBatch) error, send func([]*raftv1.Message), closeStorage func() error) (*Node, error) {
	n := &Node{events: make(chan event), stop: make(chan struct{}), done: make(chan struct{})}
	waiters := make(map[uint64]*request)
	type completion struct {
		req    *request
		result *kvv1.CommandResult
		term   uint64
	}
	var completed []completion
	p, err := raftio.New(core, save, send, func(entry *raftv1.Entry) error {
		result, err := machine.Apply(entry)
		if err == nil {
			if req := waiters[entry.Index]; req != nil {
				completed = append(completed, completion{req: req, result: result, term: entry.Term})
			}
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	if err := p.Process(); err != nil {
		return nil, err
	}
	finish := func(req *request, r response) {
		delete(waiters, req.index)
		req.reply <- r
	}
	go func() {
		defer func() {
			for _, req := range waiters {
				finish(req, response{err: errors.Join(ErrUnknown, ErrStopped, n.err)})
			}
			n.err = errors.Join(n.err, closeStorage())
			close(n.done)
		}()
		for {
			select {
			case <-n.stop:
				return
			case ev := <-n.events:
				if req := ev.cancel; req != nil {
					if waiters[req.index] == req {
						finish(req, response{err: errors.Join(ErrUnknown, req.ctx.Err())})
					}
					continue
				}
				req := ev.request
				if err := req.ctx.Err(); err != nil {
					req.reply <- response{err: err}
					continue
				}
				before := core.Status()
				var err error
				switch req.kind {
				case status:
					req.reply <- response{status: Status{Raft: before, Pending: len(waiters)}}
					continue
				case tick:
					err = core.Tick()
				case step:
					err = core.Step(req.message)
				case propose:
					req.index, req.term, err = core.Propose(req.data)
					if errors.Is(err, raftcore.ErrNotLeader) {
						err = &NotLeaderError{LeaderID: before.LeaderID}
					}
					if err == nil {
						waiters[req.index] = req // before Process, including immediate commit
					}
				}
				if err != nil {
					req.reply <- response{err: err}
					if req.kind == tick {
						n.err = err
						return
					}
					continue
				}
				if err := p.Process(); err != nil {
					n.err = err
					if req.kind != propose {
						req.reply <- response{err: err}
					}
					return
				}
				after := core.Status()
				if before.Role == raftcore.Leader && (after.Role != raftcore.Leader || after.HardState.Term != before.HardState.Term) {
					for _, pending := range waiters {
						finish(pending, response{err: ErrUnknown})
					}
				}
				for _, c := range completed {
					if waiters[c.req.index] == c.req {
						r := response{result: c.result}
						if c.term != c.req.term {
							r = response{err: ErrUnknown}
						}
						finish(c.req, r)
					}
				}
				completed = nil
				if req.kind != propose {
					req.reply <- response{}
				}
			}
		}
	}()
	return n, nil
}
