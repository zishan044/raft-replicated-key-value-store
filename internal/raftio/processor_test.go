package raftio

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"slices"
	"testing"

	"github.com/zishan044/raft-replicated-key-value-store.git/internal/raftcore"
	"github.com/zishan044/raft-replicated-key-value-store.git/internal/wal"
	raftv1 "github.com/zishan044/raft-replicated-key-value-store.git/pkg/api/raft/v1"
)

func config() raftcore.Config {
	return raftcore.Config{ID: 1, Voters: []uint64{1, 2, 3}, ElectionTicks: 10, HeartbeatTicks: 2, Seed: 42}
}

func entry(index, term uint64, data string) *raftv1.Entry {
	return &raftv1.Entry{Index: index, Term: term, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL, Data: []byte(data)}
}

func newProcessor(t *testing.T, core *raftcore.Core, save func(*raftv1.WALBatch) error, send func([]*raftv1.Message), apply func(*raftv1.Entry) error) *Processor {
	t.Helper()
	p, err := New(core, save, send, apply)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func process(t *testing.T, p *Processor) {
	t.Helper()
	if err := p.Process(); err != nil {
		t.Fatal(err)
	}
}

func step(t *testing.T, core *raftcore.Core, msg *raftv1.Message) {
	t.Helper()
	if err := core.Step(msg); err != nil {
		t.Fatal(err)
	}
}

func openWAL(t *testing.T, dir string) (*wal.WAL, wal.State) {
	t.Helper()
	w, state, err := wal.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := w.Close(); err != nil {
			t.Error(err)
		}
	})
	return w, state
}

func walSize(t *testing.T, dir string) int64 {
	t.Helper()
	files, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var size int64
	for _, file := range files {
		info, err := file.Info()
		if err != nil {
			t.Fatal(err)
		}
		size += info.Size()
	}
	return size
}

// Observe real fsync boundaries and optionally fail them. The rest of the
// filesystem implementation is os.File, not a second persistence algorithm.
type observedFS struct {
	dir     string
	events  []string
	syncErr error
}

func (f *observedFS) ReadDir(name string) ([]fs.DirEntry, error) { return os.ReadDir(name) }

func (f *observedFS) OpenFile(name string, flags int, mode fs.FileMode) (wal.File, error) {
	file, err := os.OpenFile(name, flags, mode)
	if err != nil {
		return nil, err
	}
	kind := "file"
	if name == f.dir {
		kind = "dir"
	}
	return &observedFile{File: file, fs: f, kind: kind}, nil
}

type observedFile struct {
	*os.File
	fs   *observedFS
	kind string
}

func (f *observedFile) Sync() error {
	f.fs.events = append(f.fs.events, "sync:"+f.kind)
	if f.fs.syncErr != nil {
		return f.fs.syncErr
	}
	return f.File.Sync()
}

func TestPersistSendApplyAdvanceOrder(t *testing.T) {
	fsys := &observedFS{dir: t.TempDir()}
	w, _, err := wal.OpenFS(fsys, fsys.dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := w.Close(); err != nil {
			t.Error(err)
		}
	}()
	core, err := raftcore.New(config())
	if err != nil {
		t.Fatal(err)
	}
	var sent []*raftv1.Message
	var applied []uint64
	p := newProcessor(t, core, w.Save, func(messages []*raftv1.Message) {
		fsys.events = append(fsys.events, "send")
		sent = append(sent, messages...)
		if core.Status().AppliedIndex != uint64(len(applied)) {
			t.Fatal("advanced before message delivery")
		}
	}, func(e *raftv1.Entry) error {
		fsys.events = append(fsys.events, fmt.Sprintf("apply:%d", e.Index))
		if core.Status().AppliedIndex != 0 || e.Index != uint64(len(applied))+1 {
			t.Fatal("advanced during application or applied out of order")
		}
		applied = append(applied, e.Index)
		return nil
	})
	fsys.events = nil
	process(t, p)
	if !slices.Equal(fsys.events, []string{"sync:file", "sync:dir"}) {
		t.Fatalf("initial persistence order: %v", fsys.events)
	}
	fsys.events = nil
	step(t, core, &raftv1.Message{
		Type: raftv1.MessageType_MESSAGE_TYPE_APPEND_ENTRIES, From: 2, To: 1, Term: 1, LeaderCommit: 2,
		Entries: []*raftv1.Entry{entry(1, 1, "A"), {Index: 2, Term: 1, Type: raftv1.EntryType_ENTRY_TYPE_NO_OP}},
	})
	process(t, p)
	if !slices.Equal(fsys.events, []string{"sync:file", "send", "apply:1", "apply:2"}) || core.Status().AppliedIndex != 2 {
		t.Fatalf("append processing order: %v, status=%+v", fsys.events, core.Status())
	}
	if len(sent) != 1 || sent[0].Reject || sent[0].MatchIndex != 2 {
		t.Fatalf("wrong append response: %v", sent)
	}
	// A duplicate append has only an acknowledgment: no save, no repeated apply.
	fsys.events, sent = nil, nil
	step(t, core, &raftv1.Message{
		Type: raftv1.MessageType_MESSAGE_TYPE_APPEND_ENTRIES, From: 2, To: 1, Term: 1,
		PrevLogIndex: 2, PrevLogTerm: 1, LeaderCommit: 2,
	})
	process(t, p)
	if !slices.Equal(fsys.events, []string{"send"}) || len(applied) != 2 {
		t.Fatalf("messages-only batch performed persistence/application: %v", fsys.events)
	}
	fsys.events = nil
	process(t, p)
	if len(fsys.events) != 0 || !errors.Is(core.Advance(), raftcore.ErrNoReady) {
		t.Fatal("empty batch performed I/O or created a pending Ready")
	}
}

func TestFailedPersistenceWithholdsVoteAndAppendAcknowledgments(t *testing.T) {
	for _, kind := range []raftv1.MessageType{raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE, raftv1.MessageType_MESSAGE_TYPE_APPEND_ENTRIES} {
		t.Run(kind.String(), func(t *testing.T) {
			fsys := &observedFS{dir: t.TempDir()}
			w, _, err := wal.OpenFS(fsys, fsys.dir)
			if err != nil {
				t.Fatal(err)
			}
			core, err := raftcore.New(config())
			if err != nil {
				t.Fatal(err)
			}
			sends, applies := 0, 0
			p := newProcessor(t, core, w.Save, func([]*raftv1.Message) { sends++ }, func(*raftv1.Entry) error { applies++; return nil })
			process(t, p)
			failure := errors.New("fsync failed")
			fsys.syncErr, fsys.events = failure, nil
			msg := &raftv1.Message{Type: kind, From: 2, To: 1, Term: 7}
			if kind == raftv1.MessageType_MESSAGE_TYPE_APPEND_ENTRIES {
				msg.Entries, msg.LeaderCommit = []*raftv1.Entry{entry(1, 7, "A")}, 1
			}
			step(t, core, msg)
			err = p.Process()
			if !errors.Is(err, failure) || sends != 0 || applies != 0 || core.Status().AppliedIndex != 0 {
				t.Fatalf("failed batch escaped: error=%v sends=%d applies=%d status=%+v", err, sends, applies, core.Status())
			}
			events := slices.Clone(fsys.events)
			if p.Process() != err || !slices.Equal(events, fsys.events) {
				t.Fatal("processor retried an uncertain save")
			}
			if !errors.Is(core.Tick(), raftcore.ErrReadyPending) || !errors.Is(core.Step(msg), raftcore.ErrReadyPending) {
				t.Fatal("failed batch allowed further core input")
			}
			if _, _, err := core.Propose(nil); !errors.Is(err, raftcore.ErrReadyPending) {
				t.Fatal("failed batch allowed a proposal")
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			// The complete but unacknowledged batch may survive. Reopening must
			// recover it durably before a new core can resume processing.
			_, state := openWAL(t, fsys.dir)
			if state.HardState.Term != 7 {
				t.Fatal("complete uncertain batch was not recovered")
			}
			if _, err := Restore(config(), state, func(*raftv1.Entry) error { return nil }); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRestartPreservesVoteAndSkipsInitialRewrite(t *testing.T) {
	dir := t.TempDir()
	w, _ := openWAL(t, dir)
	core, err := raftcore.New(config())
	if err != nil {
		t.Fatal(err)
	}
	var sent []*raftv1.Message
	send := func(messages []*raftv1.Message) { sent = append(sent, messages...) }
	apply := func(*raftv1.Entry) error { t.Fatal("empty log applied an entry"); return nil }
	p := newProcessor(t, core, w.Save, send, apply)
	process(t, p)
	step(t, core, &raftv1.Message{Type: raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE, From: 2, To: 1, Term: 7})
	process(t, p)
	if len(sent) != 1 || sent[0].Reject {
		t.Fatal("initial vote was not granted")
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w, state := openWAL(t, dir)
	size := walSize(t, dir)
	core, err = Restore(config(), state, apply)
	if err != nil {
		t.Fatal(err)
	}
	sent = nil
	p = newProcessor(t, core, w.Save, send, apply)
	process(t, p)
	if len(sent) != 0 || walSize(t, dir) != size || core.Status().Role != raftcore.Follower ||
		core.Status().HardState != (raftcore.HardState{Term: 7, Vote: 2}) {
		t.Fatal("restored core rewrote storage, emitted traffic or lost its vote")
	}
	step(t, core, &raftv1.Message{Type: raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE, From: 3, To: 1, Term: 7})
	process(t, p)
	if len(sent) != 1 || !sent[0].Reject || walSize(t, dir) != size {
		t.Fatal("restart permitted a second vote in one term or resaved a messages-only batch")
	}
	sent = nil
	step(t, core, &raftv1.Message{Type: raftv1.MessageType_MESSAGE_TYPE_REQUEST_VOTE, From: 3, To: 1, Term: 8})
	process(t, p)
	if len(sent) != 1 || sent[0].Reject || walSize(t, dir) <= size {
		t.Fatal("higher-term vote was not saved and granted")
	}
}

func TestRestoreReplaysCommittedPrefixAndRetainsPendingSuffix(t *testing.T) {
	dir := t.TempDir()
	w, _ := openWAL(t, dir)
	batch := &raftv1.WALBatch{
		HardState: &raftv1.PersistedHardState{Term: 4, Vote: 2, Commit: 3},
		Entries: []*raftv1.Entry{
			entry(1, 1, "A"), {Index: 2, Term: 2, Type: raftv1.EntryType_ENTRY_TYPE_NO_OP},
			entry(3, 3, "B"), entry(4, 4, "pending"),
		},
	}
	if err := w.Save(batch); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w, state := openWAL(t, dir)
	size := walSize(t, dir)
	var commands []string
	var applied []uint64
	apply := func(e *raftv1.Entry) error {
		if e.Index != uint64(len(applied))+1 {
			t.Fatal("application is not in order")
		}
		applied = append(applied, e.Index)
		if e.Type == raftv1.EntryType_ENTRY_TYPE_NORMAL {
			commands = append(commands, string(e.Data))
		}
		return nil
	}
	core, err := Restore(config(), state, apply)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(commands, []string{"A", "B"}) || !slices.Equal(applied, []uint64{1, 2, 3}) ||
		core.Status().AppliedIndex != 3 || core.Status().LastLogIndex != 4 || walSize(t, dir) != size {
		t.Fatalf("wrong replay: commands=%v applied=%v status=%+v", commands, applied, core.Status())
	}
	state.Entries[3].Data[0] = 'x'
	p := newProcessor(t, core, w.Save, func([]*raftv1.Message) {}, apply)
	process(t, p)
	step(t, core, &raftv1.Message{
		Type: raftv1.MessageType_MESSAGE_TYPE_APPEND_ENTRIES, From: 2, To: 1, Term: 4,
		PrevLogIndex: 4, PrevLogTerm: 4, LeaderCommit: 4,
	})
	process(t, p)
	if !slices.Equal(commands, []string{"A", "B", "pending"}) || core.Status().AppliedIndex != 4 {
		t.Fatal("pending suffix was lost, aliased or applied before commitment")
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	_, state = openWAL(t, dir)
	commands, applied = nil, nil // rebuilding a fresh volatile state machine
	if _, err := Restore(config(), state, apply); err != nil || !slices.Equal(commands, []string{"A", "B", "pending"}) {
		t.Fatalf("second restart did not replay the now-committed suffix: %v", err)
	}
}

func TestApplyFailureIsTerminalAndStartupDiscardsPartialReplay(t *testing.T) {
	dir := t.TempDir()
	w, _ := openWAL(t, dir)
	core, err := raftcore.New(config())
	if err != nil {
		t.Fatal(err)
	}
	failure := errors.New("application failed")
	var applied []uint64
	apply := func(e *raftv1.Entry) error {
		applied = append(applied, e.Index)
		if e.Index == 2 {
			return failure
		}
		return nil
	}
	sent := 0
	p := newProcessor(t, core, w.Save, func([]*raftv1.Message) { sent++ }, apply)
	process(t, p)
	step(t, core, &raftv1.Message{
		Type: raftv1.MessageType_MESSAGE_TYPE_APPEND_ENTRIES, From: 2, To: 1, Term: 1, LeaderCommit: 3,
		Entries: []*raftv1.Entry{entry(1, 1, "A"), entry(2, 1, "B"), entry(3, 1, "C")},
	})
	err = p.Process()
	if !errors.Is(err, failure) || sent != 1 || !slices.Equal(applied, []uint64{1, 2}) || core.Status().AppliedIndex != 0 {
		t.Fatalf("application failure advanced or skipped work: %v", err)
	}
	if p.Process() != err || len(applied) != 2 || sent != 1 || !errors.Is(core.Tick(), raftcore.ErrReadyPending) {
		t.Fatal("partial application was retried or advanced")
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	_, state := openWAL(t, dir)
	size := walSize(t, dir)
	applied = nil
	if failedCore, err := Restore(config(), state, apply); failedCore != nil || !errors.Is(err, failure) || !slices.Equal(applied, []uint64{1, 2}) {
		t.Fatal("startup exposed a partially replayed core")
	}
	applied = nil
	core, err = Restore(config(), state, func(e *raftv1.Entry) error { applied = append(applied, e.Index); return nil })
	if err != nil || !slices.Equal(applied, []uint64{1, 2, 3}) || core.Status().AppliedIndex != 3 || walSize(t, dir) != size {
		t.Fatalf("fresh startup failed or rewrote the log: %v", err)
	}
}

func TestRestoreValidatesMembershipAndConfiguration(t *testing.T) {
	called := false
	apply := func(*raftv1.Entry) error { called = true; return nil }
	for _, state := range []wal.State{
		{HardState: raftcore.HardState{Term: 1, Vote: 99}},
		{HardState: raftcore.HardState{Term: 1, Commit: 1}},
		{HardState: raftcore.HardState{Term: 1}, Entries: []*raftv1.Entry{entry(2, 1, "gap")}},
	} {
		if core, err := Restore(config(), state, apply); err == nil || core != nil || called {
			t.Fatal("invalid recovered state reached application")
		}
	}
	if _, err := Restore(config(), wal.State{}, nil); err == nil {
		t.Fatal("nil application callback accepted")
	}
	core, err := Restore(config(), wal.State{}, apply)
	if err != nil || core.Tick() != nil || called {
		t.Fatal("empty recovered log was not initialized")
	}
	save := func(*raftv1.WALBatch) error { return nil }
	send := func([]*raftv1.Message) {}
	for _, tc := range []struct {
		core  *raftcore.Core
		save  func(*raftv1.WALBatch) error
		send  func([]*raftv1.Message)
		apply func(*raftv1.Entry) error
	}{
		{nil, save, send, apply}, {core, nil, send, apply}, {core, save, nil, apply}, {core, save, send, nil},
	} {
		if p, err := New(tc.core, tc.save, tc.send, tc.apply); err == nil || p != nil {
			t.Fatal("incomplete processor accepted")
		}
	}
}

func TestSingleNodeProposalPersistsBeforeApplication(t *testing.T) {
	cfg := config()
	cfg.Voters = []uint64{1}
	core, err := raftcore.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	fsys := &observedFS{dir: t.TempDir()}
	w, _, err := wal.OpenFS(fsys, fsys.dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := w.Close(); err != nil {
			t.Error(err)
		}
	}()
	var commands []string
	p := newProcessor(t, core, w.Save, func([]*raftv1.Message) { t.Fatal("single node sent peer traffic") }, func(e *raftv1.Entry) error {
		fsys.events = append(fsys.events, "apply")
		if e.Type == raftv1.EntryType_ENTRY_TYPE_NORMAL {
			commands = append(commands, string(e.Data))
		}
		return nil
	})
	process(t, p)
	for core.Status().Role != raftcore.Leader {
		if err := core.Tick(); err != nil {
			t.Fatal(err)
		}
		process(t, p)
	}
	fsys.events = nil
	index, _, err := core.Propose([]byte("write"))
	if err != nil {
		t.Fatal(err)
	}
	process(t, p)
	if !slices.Equal(fsys.events, []string{"sync:file", "apply"}) || !slices.Equal(commands, []string{"write"}) || core.Status().AppliedIndex != index {
		t.Fatal("single-node proposal applied without a synced batch")
	}
}
