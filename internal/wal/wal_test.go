package wal

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/zishan044/raft-replicated-key-value-store.git/internal/raftcore"
	raftv1 "github.com/zishan044/raft-replicated-key-value-store.git/pkg/api/raft/v1"
)

// SetSegmentLimitForTest lets external crash tests exercise rotation with small
// fixtures. It exists only in test builds; production uses 64 MiB segments.
func SetSegmentLimitForTest(w *WAL, limit int64) { w.segmentLimit = limit }

func openWAL(t *testing.T, dir string) (*WAL, State) {
	t.Helper()
	w, state, err := Open(dir)
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

func save(t *testing.T, w *WAL, batch *raftv1.WALBatch) {
	t.Helper()
	if err := w.Save(batch); err != nil {
		t.Fatal(err)
	}
}

func writeSegment(t *testing.T, dir string, sequence uint64, data []byte) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, segmentName(sequence)), data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func readSegment(t *testing.T, dir string, sequence uint64) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, segmentName(sequence)))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func initialBatch() *raftv1.WALBatch {
	return &raftv1.WALBatch{
		HardState: &raftv1.PersistedHardState{Term: 1, Vote: 1, Commit: 1},
		Entries:   []*raftv1.Entry{entry(1, 1, "A")},
	}
}

func nextBatch() *raftv1.WALBatch {
	return &raftv1.WALBatch{
		HardState: &raftv1.PersistedHardState{Term: 2, Vote: 2, Commit: 2},
		Entries:   []*raftv1.Entry{entry(2, 2, "B")},
	}
}

func TestWALSaveReopenAndOwnership(t *testing.T) {
	dir := t.TempDir()
	w, state := openWAL(t, dir)
	assertState(t, state, raftcore.HardState{}, nil)
	if w.segmentLimit != 64<<20 {
		t.Fatalf("unexpected default segment size: %d", w.segmentLimit)
	}
	save(t, w, initialBatch())
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w, snapshot := openWAL(t, dir)
	assertState(t, snapshot, raftcore.HardState{Term: 1, Vote: 1, Commit: 1}, initialBatch().Entries)
	snapshot.HardState.Term = 99
	snapshot.Entries[0].Data[0] = 'x'
	snapshot.Entries[0] = nil
	// Identical committed entries still pass validation after caller mutation.
	save(t, w, initialBatch())
	save(t, w, nextBatch())
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		reopened, state := openWAL(t, dir)
		assertState(t, state, raftcore.HardState{Term: 2, Vote: 2, Commit: 2}, []*raftv1.Entry{entry(1, 1, "A"), entry(2, 2, "B")})
		if err := reopened.Close(); err != nil {
			t.Fatal(err)
		}
	}
	info, err := os.Stat(filepath.Join(dir, segmentName(1)))
	if err != nil || info.Mode().Perm()&0o077 != 0 {
		t.Fatalf("segment permissions: info=%v error=%v", info, err)
	}
}

func TestWALRotationPreservesReplacementHistory(t *testing.T) {
	dir := t.TempDir()
	w, _ := openWAL(t, dir)
	w.segmentLimit = 1 // every batch occupies its own segment
	first := &raftv1.WALBatch{
		HardState: &raftv1.PersistedHardState{Term: 2, Vote: 1, Commit: 1},
		Entries:   []*raftv1.Entry{entry(1, 1, "A"), entry(2, 2, "old"), entry(3, 2, "old tail")},
	}
	save(t, w, first)
	original := readSegment(t, dir, 1)
	save(t, w, &raftv1.WALBatch{
		HardState: &raftv1.PersistedHardState{Term: 3, Commit: 1},
		Entries:   []*raftv1.Entry{entry(2, 3, "C")},
	})
	save(t, w, &raftv1.WALBatch{HardState: &raftv1.PersistedHardState{Term: 3, Vote: 2, Commit: 2}})
	save(t, w, &raftv1.WALBatch{Entries: []*raftv1.Entry{entry(3, 3, "D")}})
	if w.sequence != 4 || !bytes.Equal(original, readSegment(t, dir, 1)) {
		t.Fatal("rotation overwrote an earlier segment")
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		reopened, state := openWAL(t, dir)
		assertState(t, state, raftcore.HardState{Term: 3, Vote: 2, Commit: 2}, []*raftv1.Entry{entry(1, 1, "A"), entry(2, 3, "C"), entry(3, 3, "D")})
		if reopened.sequence != 4 {
			t.Fatal("segment sequence confused with a logical entry index")
		}
		if err := reopened.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestWALExactBoundaryAndOversizedRecords(t *testing.T) {
	dir := t.TempDir()
	w, _ := openWAL(t, dir)
	first := &raftv1.WALBatch{HardState: &raftv1.PersistedHardState{Term: 1, Vote: 1}}
	second := &raftv1.WALBatch{HardState: &raftv1.PersistedHardState{Term: 2, Vote: 1}}
	w.segmentLimit = int64(len(encode(t, first)) + len(encode(t, second)))
	save(t, w, first)
	save(t, w, second)
	if w.sequence != 1 || w.size != w.segmentLimit {
		t.Fatal("rotated before reaching the exact size boundary")
	}
	large := &raftv1.WALBatch{
		HardState: &raftv1.PersistedHardState{Term: 3, Vote: 1},
		Entries:   []*raftv1.Entry{entry(1, 3, strings.Repeat("x", 200))},
	}
	save(t, w, large)
	if w.sequence != 2 || w.size != int64(len(encode(t, large))) {
		t.Fatal("oversized record was split or padded")
	}
	save(t, w, &raftv1.WALBatch{HardState: &raftv1.PersistedHardState{Term: 4, Vote: 1}})
	if w.sequence != 3 || len(readSegment(t, dir, 1)) != int(w.segmentLimit) || len(readSegment(t, dir, 2)) != len(encode(t, large)) {
		t.Fatal("segments did not grow on demand or oversized record shared a segment")
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	_, state := openWAL(t, dir)
	assertState(t, state, raftcore.HardState{Term: 4, Vote: 1}, large.Entries)
}

func TestWALRepairsTailBeforeFurtherSaves(t *testing.T) {
	base := encode(t, initialBatch())
	last := encode(t, nextBatch())
	for _, cut := range []int{1, headerSize - 1, headerSize, len(last) - 1} {
		t.Run(segmentName(uint64(cut)), func(t *testing.T) {
			dir := t.TempDir()
			writeSegment(t, dir, 1, append(bytes.Clone(base), last[:cut]...))
			fsys := newFaultFS(dir)
			w, state, err := OpenFS(fsys, dir)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := w.Close(); err != nil {
					t.Error(err)
				}
			}()
			assertState(t, state, raftcore.HardState{Term: 1, Vote: 1, Commit: 1}, initialBatch().Entries)
			if !bytes.Equal(readSegment(t, dir, 1), base) {
				t.Fatal("incomplete tail was not physically truncated")
			}
			want := []string{"truncate:" + segmentName(1), "sync:" + segmentName(1), "sync:dir"}
			if got := onlyEvents(fsys.events, "truncate:", "sync:"); !reflect.DeepEqual(got, want) {
				t.Fatalf("recovery durability order: got %v, want %v", got, want)
			}
			save(t, w, nextBatch())
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				reopened, got := openWAL(t, dir)
				assertState(t, got, raftcore.HardState{Term: 2, Vote: 2, Commit: 2}, []*raftv1.Entry{entry(1, 1, "A"), entry(2, 2, "B")})
				if err := reopened.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestWALEmptyNewestSegment(t *testing.T) {
	dir := t.TempDir()
	writeSegment(t, dir, 1, encode(t, initialBatch()))
	writeSegment(t, dir, 2, nil) // a crash after creation but before writing
	w, state := openWAL(t, dir)
	assertState(t, state, raftcore.HardState{Term: 1, Vote: 1, Commit: 1}, initialBatch().Entries)
	save(t, w, nextBatch())
	if w.sequence != 2 || len(readSegment(t, dir, 2)) != len(encode(t, nextBatch())) {
		t.Fatal("did not reuse the recovered empty segment")
	}
}

func TestWALRejectsCorruptionWithoutModifyingFiles(t *testing.T) {
	first := encode(t, initialBatch())
	second := encode(t, nextBatch())
	bad := bytes.Clone(second)
	bad[len(bad)-1] ^= 1
	for _, tc := range []struct {
		name  string
		files map[string][]byte
	}{
		{"newest checksum", map[string][]byte{segmentName(1): first, segmentName(2): bad}},
		{"older torn record", map[string][]byte{segmentName(1): first[:len(first)-1], segmentName(2): second}},
		{"missing first", map[string][]byte{segmentName(2): second}},
		{"sequence gap", map[string][]byte{segmentName(1): first, segmentName(3): second}},
		{"malformed name", map[string][]byte{"1.wal": first}},
		{"unexpected file", map[string][]byte{"notes.txt": []byte("not a segment")}},
		{"empty older", map[string][]byte{segmentName(1): nil, segmentName(2): second}},
		{"invalid state", map[string][]byte{segmentName(1): first, segmentName(2): rawBatch(t, &raftv1.WALBatch{HardState: &raftv1.PersistedHardState{Term: 1, Vote: 2, Commit: 1}})}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			for name, data := range tc.files {
				if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			fsys := newFaultFS(dir)
			w, state, err := OpenFS(fsys, dir)
			if w != nil || !errors.Is(err, ErrCorrupt) || fsys.live != 0 {
				t.Fatalf("corrupt open: WAL=%v error=%v live handles=%d", w, err, fsys.live)
			}
			assertState(t, state, raftcore.HardState{}, nil)
			for name, before := range tc.files {
				after, err := os.ReadFile(filepath.Join(dir, name))
				if err != nil || !bytes.Equal(before, after) {
					t.Fatalf("corruption modified %s: %v", name, err)
				}
			}
		})
	}
	// A matching name still must be a regular file.
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, segmentName(1)), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Open(dir); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("directory accepted as a segment: %v", err)
	}
}

// This wrapper injects individual failures around real filesystem operations.
// It is not the durable/unsynced simulated disk planned for subset 4.
type faultFS struct {
	root    string
	events  []string
	errors  map[string]error
	short   int
	zero    bool
	partial int
	live    int
}

func newFaultFS(dir string) *faultFS { return &faultFS{root: dir, errors: make(map[string]error)} }

func (f *faultFS) event(op, name string) error {
	if name == f.root {
		name = "dir"
	} else {
		name = filepath.Base(name)
	}
	key := op + ":" + name
	f.events = append(f.events, key)
	return f.errors[key]
}

func (f *faultFS) OpenFile(name string, flags int, perm fs.FileMode) (File, error) {
	op := "open"
	if flags&os.O_CREATE != 0 {
		op = "create"
	}
	if err := f.event(op, name); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(name, flags, perm)
	if err != nil {
		return nil, err
	}
	f.live++
	return &faultFile{File: file, fs: f, name: name}, nil
}

func (f *faultFS) ReadDir(name string) ([]fs.DirEntry, error) {
	if err := f.event("readdir", name); err != nil {
		return nil, err
	}
	return os.ReadDir(name)
}

type faultFile struct {
	*os.File
	fs   *faultFS
	name string
}

func (f *faultFile) Read(p []byte) (int, error) {
	if err := f.fs.event("read", f.name); err != nil {
		return 0, err
	}
	return f.File.Read(p)
}

func (f *faultFile) Write(p []byte) (int, error) {
	if err := f.fs.event("write", f.name); err != nil {
		if f.fs.partial > 0 {
			n, writeErr := f.File.Write(p[:min(len(p), f.fs.partial)])
			return n, errors.Join(err, writeErr)
		}
		return 0, err
	}
	if f.fs.zero {
		return 0, nil
	}
	if f.fs.short > 0 {
		p = p[:min(len(p), f.fs.short)]
	}
	return f.File.Write(p)
}

func (f *faultFile) Stat() (fs.FileInfo, error) {
	if err := f.fs.event("stat", f.name); err != nil {
		return nil, err
	}
	return f.File.Stat()
}

func (f *faultFile) Sync() error {
	if err := f.fs.event("sync", f.name); err != nil {
		return err
	}
	return f.File.Sync()
}

func (f *faultFile) Truncate(size int64) error {
	if err := f.fs.event("truncate", f.name); err != nil {
		return err
	}
	return f.File.Truncate(size)
}

func (f *faultFile) Close() error {
	err := f.fs.event("close", f.name)
	f.fs.live--
	return errors.Join(err, f.File.Close())
}

func onlyEvents(events []string, prefixes ...string) []string {
	var result []string
	for _, event := range events {
		for _, prefix := range prefixes {
			if strings.HasPrefix(event, prefix) {
				result = append(result, event)
				break
			}
		}
	}
	return result
}

func TestWALShortWritesAndSyncOrder(t *testing.T) {
	dir := t.TempDir()
	fsys := newFaultFS(dir)
	fsys.short = 3
	w, _, err := OpenFS(fsys, dir)
	if err != nil {
		t.Fatal(err)
	}
	fsys.events = nil
	save(t, w, initialBatch())
	want := []string{"create:" + segmentName(1), "sync:" + segmentName(1), "sync:dir"}
	if got := onlyEvents(fsys.events, "create:", "sync:"); !reflect.DeepEqual(got, want) || len(onlyEvents(fsys.events, "write:")) < 2 {
		t.Fatalf("short write/save events: %v", fsys.events)
	}
	fsys.events = nil
	save(t, w, nextBatch())
	want = []string{"sync:" + segmentName(1)}
	if got := onlyEvents(fsys.events, "create:", "sync:"); !reflect.DeepEqual(got, want) {
		t.Fatalf("existing segment used more than one sync: %v", fsys.events)
	}
	w.segmentLimit = 1
	fsys.events = nil
	save(t, w, &raftv1.WALBatch{HardState: &raftv1.PersistedHardState{Term: 3, Commit: 2}})
	want = []string{"close:" + segmentName(1), "create:" + segmentName(2), "sync:" + segmentName(2), "sync:dir"}
	if got := onlyEvents(fsys.events, "close:", "create:", "sync:"); !reflect.DeepEqual(got, want) {
		t.Fatalf("rotation durability order: got %v, want %v", got, want)
	}
	if err := w.Close(); err != nil || fsys.live != 0 {
		t.Fatalf("close error=%v live=%d", err, fsys.live)
	}
	_, state := openWAL(t, dir)
	assertState(t, state, raftcore.HardState{Term: 3, Commit: 2}, []*raftv1.Entry{entry(1, 1, "A"), entry(2, 2, "B")})
}

func TestWALSaveFailuresRequireRecovery(t *testing.T) {
	failure := errors.New("injected I/O failure")
	for _, tc := range []struct {
		name    string
		op      string
		rotate  bool
		zero    bool
		partial int
	}{
		{"create", "create:" + segmentName(1), false, false, 0},
		{"write", "write:" + segmentName(1), false, false, 0},
		{"partial write with error", "write:" + segmentName(1), false, false, 7},
		{"zero progress", "", false, true, 0},
		{"file sync", "sync:" + segmentName(1), false, false, 0},
		{"directory sync", "sync:dir", false, false, 0},
		{"rotation close", "close:" + segmentName(1), true, false, 0},
		{"rotation create", "create:" + segmentName(2), true, false, 0},
		{"rotation write", "write:" + segmentName(2), true, false, 9},
		{"rotation file sync", "sync:" + segmentName(2), true, false, 0},
		{"rotation directory sync", "sync:dir", true, false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			fsys := newFaultFS(dir)
			w, _, err := OpenFS(fsys, dir)
			if err != nil {
				t.Fatal(err)
			}
			batch := initialBatch()
			if tc.rotate {
				save(t, w, initialBatch())
				w.segmentLimit = 1
				batch = nextBatch()
			}
			before := w.state
			fsys.errors[tc.op] = failure
			fsys.zero, fsys.partial = tc.zero, tc.partial
			wantErr := failure
			if tc.zero {
				wantErr = io.ErrShortWrite
			}
			err = w.Save(batch)
			if !errors.Is(err, wantErr) {
				t.Fatalf("save reported wrong outcome: %v", err)
			}
			assertState(t, w.state, before.HardState, before.Entries)
			eventCount := len(fsys.events)
			if again := w.Save(batch); again != err || len(fsys.events) != eventCount {
				t.Fatal("failed WAL attempted more I/O without recovery")
			}
			if err := w.Close(); err != nil || fsys.live != 0 {
				t.Fatalf("cleanup: error=%v live=%d", err, fsys.live)
			}
			// An uncertain complete write may survive; an incomplete one is
			// discarded. Either outcome must be recoverable as a valid prefix.
			_, recovered := openWAL(t, dir)
			if recovered.HardState.Term != before.HardState.Term && recovered.HardState.Term != batch.HardState.Term {
				t.Fatalf("unexpected recovered term: %+v", recovered.HardState)
			}
		})
	}
}

func TestWALRecoveryFailuresReleaseHandles(t *testing.T) {
	failure := errors.New("recovery I/O failure")
	for _, op := range []string{
		"open:dir", "readdir:dir", "open:" + segmentName(1),
		"stat:" + segmentName(1), "read:" + segmentName(1),
		"truncate:" + segmentName(1), "sync:" + segmentName(1), "sync:dir",
		"close:" + segmentName(1),
	} {
		t.Run(op, func(t *testing.T) {
			dir := t.TempDir()
			data := encode(t, initialBatch())
			if strings.HasPrefix(op, "truncate:") {
				data = append(data, []byte("RWAL")...)
			}
			writeSegment(t, dir, 1, data)
			if strings.HasPrefix(op, "close:") {
				writeSegment(t, dir, 2, encode(t, nextBatch()))
			}
			fsys := newFaultFS(dir)
			fsys.errors[op] = failure
			w, state, err := OpenFS(fsys, dir)
			if w != nil || !errors.Is(err, failure) || fsys.live != 0 {
				t.Fatalf("open error=%v WAL=%v live=%d", err, w, fsys.live)
			}
			assertState(t, state, raftcore.HardState{}, nil)
		})
	}
	// Recovery must preserve cleanup errors as well as the primary failure.
	dir := t.TempDir()
	fsys := newFaultFS(dir)
	closeErr := errors.New("directory close failure")
	fsys.errors["readdir:dir"], fsys.errors["close:dir"] = failure, closeErr
	if _, _, err := OpenFS(fsys, dir); !errors.Is(err, failure) || !errors.Is(err, closeErr) || fsys.live != 0 {
		t.Fatalf("lost cleanup error or leaked handles: %v, live=%d", err, fsys.live)
	}
}

func TestWALValidationAndClose(t *testing.T) {
	dir := t.TempDir()
	fsys := newFaultFS(dir)
	w, _, err := OpenFS(fsys, dir)
	if err != nil {
		t.Fatal(err)
	}
	save(t, w, initialBatch())
	fsys.events = nil
	if err := w.Save(&raftv1.WALBatch{HardState: &raftv1.PersistedHardState{Term: 1, Vote: 2, Commit: 1}}); err == nil || len(fsys.events) != 0 {
		t.Fatal("invalid transition reached the filesystem")
	}
	save(t, w, nextBatch())
	fileErr, dirErr := errors.New("file close"), errors.New("directory close")
	fsys.errors["close:"+segmentName(1)], fsys.errors["close:dir"] = fileErr, dirErr
	err = w.Close()
	if !errors.Is(err, fileErr) || !errors.Is(err, dirErr) || fsys.live != 0 {
		t.Fatalf("close error=%v live=%d", err, fsys.live)
	}
	count := len(fsys.events)
	if w.Close() != err || len(fsys.events) != count {
		t.Fatal("Close was not idempotent")
	}
	if err := w.Save(nextBatch()); !errors.Is(err, ErrClosed) || len(fsys.events) != count {
		t.Fatal("closed WAL accepted a save")
	}
}

func TestWALSequenceExhaustion(t *testing.T) {
	w, _ := openWAL(t, t.TempDir())
	save(t, w, initialBatch())
	w.sequence, w.segmentLimit = math.MaxUint64, 1
	if err := w.Save(nextBatch()); err == nil {
		t.Fatal("wrapped segment sequence")
	}
}
