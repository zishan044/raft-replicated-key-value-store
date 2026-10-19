package wal_test

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"slices"
	"testing"

	"github.com/zishan044/raft-replicated-key-value-store.git/internal/sim"
	"github.com/zishan044/raft-replicated-key-value-store.git/internal/wal"
	raftv1 "github.com/zishan044/raft-replicated-key-value-store.git/pkg/api/raft/v1"
)

var errPowerCut = errors.New("power cut at persistence boundary")

// Cut before/after each physical mutation. One-byte writes expose every partial
// record position while still driving the production WAL's short-write loop.
type boundaryFS struct {
	disk   *sim.Disk
	active bool
	cut    int
	keep   int
	fired  bool
	events []string
}

func (f *boundaryFS) point(event string) error {
	if !f.active {
		return nil
	}
	f.events = append(f.events, event)
	if f.cut == len(f.events)-1 {
		f.fired, f.active = true, false
		if err := f.disk.Crash(f.keep); err != nil {
			return err
		}
		return errPowerCut
	}
	return nil
}

func (f *boundaryFS) ReadDir(name string) ([]fs.DirEntry, error) { return f.disk.ReadDir(name) }

func (f *boundaryFS) OpenFile(name string, flags int, mode fs.FileMode) (wal.File, error) {
	create := flags&os.O_CREATE != 0
	if create {
		if err := f.point("before-create"); err != nil {
			return nil, err
		}
	}
	file, err := f.disk.OpenFile(name, flags, mode)
	if err != nil {
		return nil, err
	}
	if create {
		if err := f.point("after-create"); err != nil {
			return nil, err
		}
	}
	return &boundaryFile{File: file, fs: f, dir: name == "."}, nil
}

type boundaryFile struct {
	wal.File
	fs  *boundaryFS
	dir bool
}

func (f *boundaryFile) Write(p []byte) (int, error) {
	if err := f.fs.point("before-write"); err != nil {
		return 0, err
	}
	n, err := f.File.Write(p[:min(1, len(p))])
	if err == nil {
		err = f.fs.point("after-write")
	}
	return n, err
}

func (f *boundaryFile) Sync() error {
	label := "file-sync"
	if f.dir {
		label = "dir-sync"
	}
	if err := f.fs.point("before-" + label); err != nil {
		return err
	}
	if err := f.File.Sync(); err != nil {
		return err
	}
	return f.fs.point("after-" + label)
}

func (f *boundaryFile) Truncate(size int64) error {
	if err := f.fs.point("before-truncate"); err != nil {
		return err
	}
	if err := f.File.Truncate(size); err != nil {
		return err
	}
	return f.fs.point("after-truncate")
}

func (f *boundaryFile) Close() error {
	if err := f.fs.point("before-close"); err != nil {
		return err
	}
	if err := f.File.Close(); err != nil {
		return err
	}
	return f.fs.point("after-close")
}

func boundaryBatch(index, term uint64, data string) *raftv1.WALBatch {
	return &raftv1.WALBatch{
		HardState: &raftv1.PersistedHardState{Term: term, Vote: term, Commit: index},
		Entries:   []*raftv1.Entry{{Index: index, Term: term, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL, Data: []byte(data)}},
	}
}

func boundaryFixture(t *testing.T, scenario string, cut, keep int) (*boundaryFS, error) {
	t.Helper()
	f := &boundaryFS{disk: sim.NewDisk(), cut: cut, keep: keep}
	w, _, err := wal.OpenFS(f, ".")
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Save(boundaryBatch(1, 1, "acknowledged")); err != nil {
		t.Fatal(err)
	}
	if scenario == "rotation" {
		wal.SetSegmentLimitForTest(w, 1)
	}
	if scenario == "recovery" {
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		record, err := wal.Encode(boundaryBatch(2, 2, "uncertain"))
		if err != nil {
			t.Fatal(err)
		}
		file, err := f.disk.OpenFile(f.disk.Files()[0], os.O_RDWR|os.O_APPEND, 0)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.Write(record[:len(record)-1]); err != nil {
			t.Fatal(err)
		}
		if err := file.Sync(); err != nil {
			t.Fatal(err)
		}
		if err := f.disk.Crash(0); err != nil {
			t.Fatal(err)
		}
		f.active = true
		_, _, err = wal.OpenFS(f, ".")
		return f, err
	}
	f.active = true
	return f, w.Save(boundaryBatch(2, 2, "uncertain"))
}

func TestPowerCutAtEveryPersistenceBoundary(t *testing.T) {
	for _, scenario := range []string{"append", "rotation", "recovery"} {
		reference, err := boundaryFixture(t, scenario, -1, 0)
		if err != nil || len(reference.events) == 0 {
			t.Fatalf("calibrate %s: %v", scenario, err)
		}
		required := []string{"before-file-sync", "after-file-sync"}
		if scenario != "append" {
			required = append(required, "before-dir-sync", "after-dir-sync")
		}
		if scenario == "rotation" {
			required = append(required, "before-create", "after-create", "before-close", "after-close")
		}
		if scenario == "recovery" {
			required = append(required, "before-truncate", "after-truncate")
		}
		for _, event := range required {
			if !slices.Contains(reference.events, event) {
				t.Fatalf("%s bypassed a required persistence boundary: %s", scenario, event)
			}
		}
		for cut, event := range reference.events {
			for _, keep := range []int{0, 1024} {
				t.Run(fmt.Sprintf("%s/%d_%s/keep_%d", scenario, cut, event, keep), func(t *testing.T) {
					f, err := boundaryFixture(t, scenario, cut, keep)
					if !f.fired || !errors.Is(err, errPowerCut) {
						t.Fatalf("cut was bypassed: events=%v error=%v", f.events, err)
					}
					w, state, err := wal.OpenFS(f.disk, ".")
					if err != nil {
						t.Fatal(err)
					}
					if len(state.Entries) == 0 || string(state.Entries[0].Data) != "acknowledged" || state.HardState.Commit != uint64(len(state.Entries)) {
						t.Fatal("crash lost acknowledged data or recovered a partial batch")
					}
					if len(state.Entries) > 2 || (len(state.Entries) == 2 && string(state.Entries[1].Data) != "uncertain") || (scenario == "recovery" && len(state.Entries) != 1) {
						t.Fatal("recovered neither the old nor the complete new batch")
					}
					if state.HardState.Term != uint64(len(state.Entries)) || state.HardState.Vote != state.HardState.Term {
						t.Fatal("hard state was separated from its atomic batch")
					}
					// The repaired log must remain writable, and that next successful
					// save must survive another power loss.
					index := uint64(len(state.Entries)) + 1
					if err := w.Save(boundaryBatch(index, state.HardState.Term+1, "after recovery")); err != nil {
						t.Fatal(err)
					}
					if err := f.disk.Crash(0); err != nil {
						t.Fatal(err)
					}
					_, again, err := wal.OpenFS(f.disk, ".")
					if err != nil || again.HardState.Commit != index || string(again.Entries[index-1].Data) != "after recovery" {
						t.Fatalf("repaired WAL did not survive another restart: %v", err)
					}
				})
			}
		}
	}
}
