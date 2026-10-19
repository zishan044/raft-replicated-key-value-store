package kv

import (
	"reflect"
	"testing"

	"github.com/zishan044/raft-replicated-key-value-store.git/internal/raftcore"
	"github.com/zishan044/raft-replicated-key-value-store.git/internal/raftio"
	"github.com/zishan044/raft-replicated-key-value-store.git/internal/wal"
	kvv1 "github.com/zishan044/raft-replicated-key-value-store.git/pkg/api/kv/v1"
	raftv1 "github.com/zishan044/raft-replicated-key-value-store.git/pkg/api/raft/v1"
	"google.golang.org/protobuf/proto"
)

func register() *kvv1.Command {
	return &kvv1.Command{Operation: &kvv1.Command_RegisterClient{RegisterClient: &kvv1.RegisterClientRequest{}}}
}

func put(client, sequence uint64, key, data string) *kvv1.Command {
	return &kvv1.Command{Operation: &kvv1.Command_Put{Put: &kvv1.PutRequest{
		Session: &kvv1.WriteSession{ClientId: client, Sequence: sequence}, Key: key, Value: []byte(data),
	}}}
}

func get(key string) *kvv1.Command {
	return &kvv1.Command{Operation: &kvv1.Command_Get{Get: &kvv1.GetRequest{Key: key}}}
}

func del(client, sequence uint64, key string) *kvv1.Command {
	return &kvv1.Command{Operation: &kvv1.Command_Delete{Delete: &kvv1.DeleteRequest{
		Session: &kvv1.WriteSession{ClientId: client, Sequence: sequence}, Key: key,
	}}}
}

func cas(client, sequence uint64, key string, expected uint64, data string) *kvv1.Command {
	return &kvv1.Command{Operation: &kvv1.Command_CompareAndSwap{CompareAndSwap: &kvv1.CompareAndSwapRequest{
		Session: &kvv1.WriteSession{ClientId: client, Sequence: sequence}, Key: key,
		ExpectedRevision: expected, Value: []byte(data),
	}}}
}

// A nil command represents an election no-op, not an empty normal command.
func entry(t *testing.T, index uint64, cmd *kvv1.Command) *raftv1.Entry {
	t.Helper()
	e := &raftv1.Entry{Term: 1, Index: index, Type: raftv1.EntryType_ENTRY_TYPE_NO_OP}
	if cmd != nil {
		data, err := proto.Marshal(cmd)
		if err != nil {
			t.Fatal(err)
		}
		e.Type, e.Data = raftv1.EntryType_ENTRY_TYPE_NORMAL, data
	}
	return e
}

func apply(t *testing.T, m *Machine, cmd *kvv1.Command) *kvv1.CommandResult {
	t.Helper()
	result, err := m.Apply(entry(t, m.AppliedIndex()+1, cmd))
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func wantResult(t *testing.T, got, want *kvv1.CommandResult) {
	t.Helper()
	if !proto.Equal(got, want) {
		t.Fatalf("result = %v, want %v", got, want)
	}
}

func invalidSequence(expected uint64) *kvv1.CommandResult {
	return &kvv1.CommandResult{Error: &kvv1.ErrorDetail{
		Code: kvv1.ErrorCode_ERROR_CODE_INVALID_SEQUENCE, ExpectedSequence: expected,
	}}
}

func conflict(revision uint64) *kvv1.CommandResult {
	return &kvv1.CommandResult{Error: &kvv1.ErrorDetail{
		Code: kvv1.ErrorCode_ERROR_CODE_CAS_CONFLICT, CurrentRevision: revision,
	}}
}

func TestOperationsAndRevisions(t *testing.T) {
	var m Machine
	steps := []struct {
		name string
		cmd  *kvv1.Command
		want *kvv1.CommandResult
	}{
		{"register", register(), &kvv1.CommandResult{ClientId: 1}},
		{"no-op", nil, nil},
		{"missing", get("a"), &kvv1.CommandResult{}},
		{"put empty value", put(1, 1, "a", ""), &kvv1.CommandResult{Revision: 4}},
		{"existing empty value", get("a"), &kvv1.CommandResult{Found: true, Revision: 4}},
		{"overwrite", put(1, 2, "a", "first"), &kvv1.CommandResult{Revision: 6}},
		{"CAS conflict", cas(1, 3, "a", 4, "bad"), conflict(6)},
		{"conflict leaves value", get("a"), &kvv1.CommandResult{Found: true, Revision: 6, Value: []byte("first")}},
		{"CAS succeeds", cas(1, 4, "a", 6, "second"), &kvv1.CommandResult{Revision: 9}},
		{"CAS updates value", get("a"), &kvv1.CommandResult{Found: true, Revision: 9, Value: []byte("second")}},
		{"delete", del(1, 5, "a"), &kvv1.CommandResult{Revision: 11}},
		{"deleted key", get("a"), &kvv1.CommandResult{}},
		{"delete absent", del(1, 6, "a"), &kvv1.CommandResult{}},
		{"CAS absent conflict", cas(1, 7, "a", 9, "bad"), conflict(0)},
		{"CAS create empty key", cas(1, 8, "", 0, "\xff\x00"), &kvv1.CommandResult{Revision: 15}},
		{"read binary value", get(""), &kvv1.CommandResult{Found: true, Revision: 15, Value: []byte("\xff\x00")}},
		{"CAS create existing conflict", cas(1, 9, "", 0, "bad"), conflict(15)},
	}
	for i, step := range steps {
		t.Run(step.name, func(t *testing.T) {
			wantResult(t, apply(t, &m, step.cmd), step.want)
			if m.AppliedIndex() != uint64(i+1) {
				t.Fatal("application did not advance")
			}
		})
	}
}

func TestWriteRetriesAndInvalidSequences(t *testing.T) {
	var m Machine
	apply(t, &m, register())
	original := put(1, 1, "key", "value")
	wantResult(t, apply(t, &m, original), &kvv1.CommandResult{Revision: 2})
	for range 5 {
		wantResult(t, apply(t, &m, original), &kvv1.CommandResult{Revision: 2})
	}
	// Neither different arguments nor a different operation may reuse a sequence.
	wantResult(t, apply(t, &m, put(1, 1, "key", "changed")), invalidSequence(2))
	wantResult(t, apply(t, &m, del(1, 1, "key")), invalidSequence(2))
	wantResult(t, apply(t, &m, put(1, 3, "key", "gap")), invalidSequence(2))
	wantResult(t, apply(t, &m, put(999, 1, "key", "bad client")), &kvv1.CommandResult{
		Error: &kvv1.ErrorDetail{Code: kvv1.ErrorCode_ERROR_CODE_INVALID_SESSION},
	})
	wantResult(t, apply(t, &m, get("key")), &kvv1.CommandResult{Found: true, Revision: 2, Value: []byte("value")})
	wantResult(t, apply(t, &m, put(1, 2, "key", "next")), &kvv1.CommandResult{Revision: 13})
	wantResult(t, apply(t, &m, original), invalidSequence(3))
	// Each session sequences independently, using its registration's actual index.
	wantResult(t, apply(t, &m, register()), &kvv1.CommandResult{ClientId: 15})
	wantResult(t, apply(t, &m, put(15, 1, "other", "ok")), &kvv1.CommandResult{Revision: 16})
}

func TestConflictAndDeleteResultsAreCached(t *testing.T) {
	var m Machine
	apply(t, &m, register())
	apply(t, &m, put(1, 1, "key", "first"))
	failedCAS := cas(1, 2, "key", 0, "bad")
	wantResult(t, apply(t, &m, failedCAS), conflict(2))
	apply(t, &m, register()) // client 4
	apply(t, &m, put(4, 1, "key", "second"))
	// Return the original conflict even though the current revision changed.
	wantResult(t, apply(t, &m, failedCAS), conflict(2))
	deleted := del(1, 3, "key")
	wantResult(t, apply(t, &m, deleted), &kvv1.CommandResult{Revision: 7})
	apply(t, &m, put(4, 2, "key", "recreated"))
	wantResult(t, apply(t, &m, deleted), &kvv1.CommandResult{Revision: 7})
	wantResult(t, apply(t, &m, get("key")), &kvv1.CommandResult{Found: true, Revision: 8, Value: []byte("recreated")})
	absentDelete := del(1, 4, "missing")
	wantResult(t, apply(t, &m, absentDelete), &kvv1.CommandResult{})
	apply(t, &m, put(4, 3, "missing", "new"))
	wantResult(t, apply(t, &m, absentDelete), &kvv1.CommandResult{})
	wantResult(t, apply(t, &m, get("missing")), &kvv1.CommandResult{Found: true, Revision: 12, Value: []byte("new")})
}

func TestResultOwnershipAndCommandIdentity(t *testing.T) {
	var m Machine
	apply(t, &m, register())
	e := entry(t, 2, put(1, 1, "key", "value"))
	result, err := m.Apply(e)
	if err != nil {
		t.Fatal(err)
	}
	clear(e.Data)
	result.Revision = 999
	wantResult(t, apply(t, &m, put(1, 1, "key", "value")), &kvv1.CommandResult{Revision: 2})
	read := apply(t, &m, get("key"))
	clear(read.Value)
	wantResult(t, apply(t, &m, get("key")), &kvv1.CommandResult{Found: true, Revision: 2, Value: []byte("value")})
	result = apply(t, &m, cas(1, 2, "key", 0, "bad"))
	result.Error.CurrentRevision = 999
	wantResult(t, apply(t, &m, cas(1, 2, "key", 0, "bad")), conflict(2))

	// The same known fields encoded in a different order are the same command.
	var reordered Machine
	apply(t, &reordered, register())
	apply(t, &reordered, put(1, 1, "k", "v"))
	// Command.put { value: "v", key: "k", session { sequence: 1, client_id: 1 } }.
	data := []byte{0x12, 0x0c, 0x1a, 1, 'v', 0x12, 1, 'k', 0x0a, 4, 0x10, 1, 0x08, 1}
	got, err := reordered.Apply(&raftv1.Entry{Index: 3, Term: 1, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL, Data: data})
	if err != nil {
		t.Fatal(err)
	}
	wantResult(t, got, &kvv1.CommandResult{Revision: 2})
}

func TestMalformedEntriesLeaveMachineUnchanged(t *testing.T) {
	invalidCommands := []*kvv1.Command{
		nil, {},
		{Operation: &kvv1.Command_RegisterClient{}},
		{Operation: &kvv1.Command_Get{}},
		{Operation: &kvv1.Command_Put{}},
		{Operation: (*kvv1.Command_Put)(nil)},
		{Operation: &kvv1.Command_Put{Put: &kvv1.PutRequest{}}},
		put(0, 1, "k", "v"), put(1, 0, "k", "v"),
		del(0, 1, "k"), cas(1, 0, "k", 0, "v"),
	}
	for _, cmd := range invalidCommands {
		if err := Validate(cmd); err == nil {
			t.Fatalf("accepted invalid command %v", cmd)
		}
	}
	var m Machine
	apply(t, &m, register())
	before := m.sessions[1]
	badEntries := []*raftv1.Entry{
		nil, entry(t, 0, nil), entry(t, 1, nil), entry(t, 3, nil),
		{Index: 2, Type: raftv1.EntryType_ENTRY_TYPE_NO_OP, Data: []byte("bad")},
		{Index: 2, Type: raftv1.EntryType_ENTRY_TYPE_UNSPECIFIED},
		{Index: 2, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL, Data: []byte{0xff}},
		{Index: 2, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL},
		entry(t, 2, put(1, 0, "key", "bad")),
	}
	for _, e := range badEntries {
		if _, err := m.Apply(e); err == nil {
			t.Fatalf("accepted invalid entry %v", e)
		}
		if m.AppliedIndex() != 1 || len(m.values) != 0 || len(m.sessions) != 1 || !reflect.DeepEqual(before, m.sessions[1]) {
			t.Fatal("invalid input changed the machine")
		}
	}
	wantResult(t, apply(t, &m, put(1, 1, "key", "ok")), &kvv1.CommandResult{Revision: 2})
}

func TestSessionSequenceExhaustion(t *testing.T) {
	var m Machine
	apply(t, &m, register())
	last := put(1, ^uint64(0), "k", "v")
	// Reach the boundary without applying 2^64 entries.
	m.sessions[1] = session{sequence: ^uint64(0) - 1}
	wantResult(t, apply(t, &m, last), &kvv1.CommandResult{Revision: 2})
	wantResult(t, apply(t, &m, last), &kvv1.CommandResult{Revision: 2})
	wantResult(t, apply(t, &m, put(1, 1, "k", "bad")), invalidSequence(0))
}

func TestCommittedWALReplayRestoresValuesAndSessions(t *testing.T) {
	dir := t.TempDir()
	w, _, err := wal.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	commands := []*kvv1.Command{
		register(), nil, put(1, 1, "a", "one"),
		cas(1, 2, "a", 0, "conflict"), register(),
		put(5, 1, "b", "two"), del(5, 2, "b"), get("a"),
	}
	var original Machine
	var entries []*raftv1.Entry
	var results []*kvv1.CommandResult
	for i, cmd := range commands {
		e := entry(t, uint64(i+1), cmd)
		result, err := original.Apply(e)
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, e)
		results = append(results, result)
	}
	if err := w.Save(&raftv1.WALBatch{
		Entries: entries, HardState: &raftv1.PersistedHardState{Term: 1, Commit: uint64(len(entries))},
	}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w, state, err := wal.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := w.Close(); err != nil {
			t.Error(err)
		}
	})
	var restored Machine
	core, err := raftio.Restore(raftcore.Config{
		ID: 1, Voters: []uint64{1, 2, 3}, ElectionTicks: 10, HeartbeatTicks: 2,
	}, state, func(e *raftv1.Entry) error {
		got, err := restored.Apply(e)
		if err == nil {
			wantResult(t, got, results[e.Index-1])
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(original, restored) || core.Status().AppliedIndex != restored.AppliedIndex() {
		t.Fatal("WAL replay changed application state")
	}
	wantResult(t, apply(t, &restored, cas(1, 2, "a", 0, "conflict")), conflict(3))
	wantResult(t, apply(t, &restored, del(5, 2, "b")), &kvv1.CommandResult{Revision: 7})
	wantResult(t, apply(t, &restored, get("a")), &kvv1.CommandResult{Found: true, Revision: 3, Value: []byte("one")})
	wantResult(t, apply(t, &restored, get("b")), &kvv1.CommandResult{})
}
