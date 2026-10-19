// Package kv implements the deterministic, single-owner KV state machine.
// Its zero value is ready to replay committed Raft entries from index one.
package kv

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"

	kvv1 "github.com/zishan044/raft-replicated-key-value-store.git/pkg/api/kv/v1"
	raftv1 "github.com/zishan044/raft-replicated-key-value-store.git/pkg/api/raft/v1"
	"google.golang.org/protobuf/proto"
)

type value struct {
	data     []byte
	revision uint64
}

type session struct {
	sequence uint64
	command  [sha256.Size]byte
	result   *kvv1.CommandResult
}

// Machine must be owned by one goroutine. Apply returns owned results; callers
// may modify them without changing values or cached session results.
type Machine struct {
	values map[string]value
	// ponytail: sessions live forever; add replicated expiry if their count
	// becomes a memory limit. Never evict unresolved results using local time.
	sessions map[uint64]session
	applied  uint64
}

// Validate checks command shape before proposal, and again during replay.
// Session existence and sequence checks happen at application time because
// earlier committed commands can change them. Empty keys and values are valid.
func Validate(cmd *kvv1.Command) error {
	if cmd == nil {
		return errors.New("command is required")
	}
	var header *kvv1.WriteSession
	switch op := cmd.Operation.(type) {
	case *kvv1.Command_RegisterClient:
		if op != nil && op.RegisterClient != nil {
			return nil
		}
	case *kvv1.Command_Get:
		if op != nil && op.Get != nil {
			return nil
		}
	case *kvv1.Command_Put:
		if op != nil {
			header = op.Put.GetSession()
		}
	case *kvv1.Command_Delete:
		if op != nil {
			header = op.Delete.GetSession()
		}
	case *kvv1.Command_CompareAndSwap:
		if op != nil {
			header = op.CompareAndSwap.GetSession()
		}
	default:
		return errors.New("command operation is required and must be supported")
	}
	if header == nil || header.ClientId == 0 || header.Sequence == 0 {
		return errors.New("writes require a positive client ID and sequence; reads and registration require a request")
	}
	return nil
}

// AppliedIndex includes no-ops and application-level failures.
func (m *Machine) AppliedIndex() uint64 { return m.applied }

// Apply consumes exactly the next committed entry. A Go error means malformed
// input and leaves the machine unchanged; the owner must stop and investigate.
// Business failures are results and still advance the applied index. No-ops
// return a nil result. Retrying a write may add entries, but cannot mutate the
// key or replace its cached result a second time.
func (m *Machine) Apply(entry *raftv1.Entry) (*kvv1.CommandResult, error) {
	if entry == nil || entry.Index == 0 || entry.Index != m.applied+1 {
		return nil, errors.New("entries must apply in order starting at index one")
	}
	if entry.Type == raftv1.EntryType_ENTRY_TYPE_NO_OP {
		if len(entry.Data) != 0 {
			return nil, errors.New("no-op must not contain data")
		}
		m.applied = entry.Index
		return nil, nil
	}
	if entry.Type != raftv1.EntryType_ENTRY_TYPE_NORMAL {
		return nil, errors.New("unsupported entry type")
	}
	cmd := new(kvv1.Command)
	if err := proto.Unmarshal(entry.Data, cmd); err != nil {
		return nil, fmt.Errorf("decode command: %w", err)
	}
	if err := Validate(cmd); err != nil {
		return nil, err
	}
	// Canonical encoding makes identity independent of known-field wire order.
	data, err := (proto.MarshalOptions{Deterministic: true}).Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("encode command identity: %w", err)
	}
	if m.values == nil {
		m.values = make(map[string]value)
		m.sessions = make(map[uint64]session)
	}
	result := m.execute(entry.Index, cmd, sha256.Sum256(data))
	m.applied = entry.Index
	return result, nil
}

func (m *Machine) execute(index uint64, cmd *kvv1.Command, identity [sha256.Size]byte) *kvv1.CommandResult {
	if cmd.GetRegisterClient() != nil {
		m.sessions[index] = session{}
		return &kvv1.CommandResult{ClientId: index}
	}
	if req := cmd.GetGet(); req != nil {
		v, found := m.values[req.Key]
		return &kvv1.CommandResult{Value: slices.Clone(v.data), Revision: v.revision, Found: found}
	}

	var header *kvv1.WriteSession
	switch {
	case cmd.GetPut() != nil:
		header = cmd.GetPut().Session
	case cmd.GetDelete() != nil:
		header = cmd.GetDelete().Session
	default:
		header = cmd.GetCompareAndSwap().Session
	}
	previous, exists := m.sessions[header.ClientId]
	if !exists {
		return &kvv1.CommandResult{Error: &kvv1.ErrorDetail{Code: kvv1.ErrorCode_ERROR_CODE_INVALID_SESSION}}
	}
	if header.Sequence == previous.sequence && identity == previous.command && previous.result != nil {
		return proto.Clone(previous.result).(*kvv1.CommandResult)
	}
	if header.Sequence <= previous.sequence || header.Sequence != previous.sequence+1 {
		return &kvv1.CommandResult{Error: &kvv1.ErrorDetail{
			Code: kvv1.ErrorCode_ERROR_CODE_INVALID_SEQUENCE, ExpectedSequence: previous.sequence + 1,
		}}
	}

	result := m.write(index, cmd)
	m.sessions[header.ClientId] = session{
		sequence: header.Sequence, command: identity, result: proto.Clone(result).(*kvv1.CommandResult),
	}
	return result
}

func (m *Machine) write(index uint64, cmd *kvv1.Command) *kvv1.CommandResult {
	switch op := cmd.Operation.(type) {
	case *kvv1.Command_Put:
		m.values[op.Put.Key] = value{data: slices.Clone(op.Put.Value), revision: index}
	case *kvv1.Command_Delete:
		if _, exists := m.values[op.Delete.Key]; !exists {
			return &kvv1.CommandResult{}
		}
		delete(m.values, op.Delete.Key)
	case *kvv1.Command_CompareAndSwap:
		req := op.CompareAndSwap
		if current := m.values[req.Key].revision; current != req.ExpectedRevision {
			return &kvv1.CommandResult{Error: &kvv1.ErrorDetail{
				Code: kvv1.ErrorCode_ERROR_CODE_CAS_CONFLICT, CurrentRevision: current,
			}}
		}
		m.values[req.Key] = value{data: slices.Clone(req.Value), revision: index}
	}
	return &kvv1.CommandResult{Revision: index}
}
