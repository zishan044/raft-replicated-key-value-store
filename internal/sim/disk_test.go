package sim

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"os"
	"slices"
	"testing"

	"github.com/zishan044/raft-replicated-key-value-store.git/internal/wal"
	raftv1 "github.com/zishan044/raft-replicated-key-value-store.git/pkg/api/raft/v1"
)

func diskOpen(t *testing.T, disk *Disk, name string, flags int) wal.File {
	t.Helper()
	f, err := disk.OpenFile(name, flags, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func diskWrite(t *testing.T, file wal.File, data []byte) {
	t.Helper()
	if n, err := file.Write(data); err != nil || n != len(data) {
		t.Fatalf("write count=%d error=%v", n, err)
	}
}

func diskRead(t *testing.T, disk *Disk, name string) []byte {
	t.Helper()
	file := diskOpen(t, disk, name, os.O_RDONLY)
	data, err := io.ReadAll(file)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	return data
}

func diskSync(t *testing.T, file wal.File) {
	t.Helper()
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}
}

func diskCrash(t *testing.T, disk *Disk, keep int) {
	t.Helper()
	if err := disk.Crash(keep); err != nil {
		t.Fatal(err)
	}
}

func TestDiskSeparatesFileAndDirectoryDurability(t *testing.T) {
	for _, fileSync := range []bool{false, true} {
		for _, dirSync := range []bool{false, true} {
			disk := NewDisk()
			file := diskOpen(t, disk, "test.wal", os.O_CREATE|os.O_EXCL|os.O_RDWR|os.O_APPEND)
			diskWrite(t, file, []byte("bytes"))
			if fileSync {
				diskSync(t, file)
			}
			if dirSync {
				diskSync(t, diskOpen(t, disk, ".", os.O_RDONLY))
			}
			diskCrash(t, disk, 0)
			if dirSync {
				want := ""
				if fileSync {
					want = "bytes"
				}
				if got := string(diskRead(t, disk, "test.wal")); got != want {
					t.Fatalf("fileSync=%t dirSync=%t: recovered %q, want %q", fileSync, dirSync, got, want)
				}
			} else if len(disk.Files()) != 0 {
				t.Fatal("file sync made an unsynced filename durable")
			}
			if _, err := file.Write([]byte("after crash")); !errors.Is(err, fs.ErrClosed) {
				t.Fatal("crash did not invalidate a pre-crash handle")
			}
			if err := file.Sync(); !errors.Is(err, fs.ErrClosed) {
				t.Fatal("stale handle synced a new disk incarnation")
			}
		}
	}
}

func TestDiskLosesUnsyncedAppendsAndRetainsTornPrefix(t *testing.T) {
	for _, keep := range []int{0, 3, 100} {
		disk := NewDisk()
		file := diskOpen(t, disk, "test.wal", os.O_CREATE|os.O_RDWR|os.O_APPEND)
		diskWrite(t, file, []byte("stable"))
		diskSync(t, file)
		diskSync(t, diskOpen(t, disk, ".", os.O_RDONLY))
		diskWrite(t, file, []byte("-unsynced"))
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
		diskCrash(t, disk, keep)
		want := "stable" + "-unsynced"[:min(keep, len("-unsynced"))]
		if got := string(diskRead(t, disk, "test.wal")); got != want {
			t.Fatalf("keep=%d: recovered %q, want %q", keep, got, want)
		}
		diskCrash(t, disk, 0)
		if string(diskRead(t, disk, "test.wal")) != want {
			t.Fatal("retained physical tail disappeared on another crash")
		}
	}
}

func TestDiskTruncationAndAppendAfterRead(t *testing.T) {
	disk := NewDisk()
	file := diskOpen(t, disk, "test.wal", os.O_CREATE|os.O_RDWR|os.O_APPEND)
	diskWrite(t, file, []byte("prefix-tail"))
	diskSync(t, file)
	diskSync(t, diskOpen(t, disk, ".", os.O_RDONLY))
	if err := file.Truncate(6); err != nil {
		t.Fatal(err)
	}
	diskCrash(t, disk, 0)
	if string(diskRead(t, disk, "test.wal")) != "prefix-tail" {
		t.Fatal("unsynced truncation survived a crash")
	}
	file = diskOpen(t, disk, "test.wal", os.O_RDWR|os.O_APPEND)
	if _, err := file.Read(make([]byte, 2)); err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(6); err != nil {
		t.Fatal(err)
	}
	diskWrite(t, file, []byte("-new"))
	diskSync(t, file)
	diskCrash(t, disk, 0)
	if string(diskRead(t, disk, "test.wal")) != "prefix-new" {
		t.Fatal("append used the read cursor or synced truncation was lost")
	}
}

func TestDiskOneShotFaultsAndValidation(t *testing.T) {
	disk := NewDisk()
	file := diskOpen(t, disk, "test.wal", os.O_CREATE|os.O_RDWR|os.O_APPEND)
	if err := disk.Inject(DiskFault{Op: DiskWrite, After: 1, WriteBytes: 3}); err != nil {
		t.Fatal(err)
	}
	diskWrite(t, file, []byte("first"))
	if n, err := file.Write([]byte("second")); n != 3 || !errors.Is(err, ErrDiskFault) {
		t.Fatalf("partial failure: count=%d error=%v", n, err)
	}
	diskWrite(t, file, []byte("last"))
	if string(diskRead(t, disk, "test.wal")) != "firstseclast" {
		t.Fatal("fault did not consume exactly one operation")
	}
	for _, fault := range []DiskFault{{}, {Op: DiskWrite, After: -1}, {Op: DiskWrite, WriteBytes: -1}, {Op: DiskSync, WriteBytes: 1}} {
		if err := disk.Inject(fault); err == nil {
			t.Fatalf("invalid fault accepted: %+v", fault)
		}
	}
	if err := disk.Inject(DiskFault{Op: DiskSync}); err != nil {
		t.Fatal(err)
	}
	if err := file.Sync(); !errors.Is(err, ErrDiskFault) {
		t.Fatal("file sync fault did not fire")
	}
	diskSync(t, file)
	dir := diskOpen(t, disk, ".", os.O_RDONLY)
	if err := disk.Inject(DiskFault{Op: DiskDirSync}); err != nil {
		t.Fatal(err)
	}
	if err := dir.Sync(); !errors.Is(err, ErrDiskFault) {
		t.Fatal("directory sync fault did not fire")
	}
	diskCrash(t, disk, 100)
	if len(disk.Files()) != 0 {
		t.Fatal("retained bytes also retained an unsynced filename")
	}
	if err := disk.Inject(DiskFault{Op: DiskCreate}); err != nil {
		t.Fatal(err)
	}
	if _, err := disk.OpenFile("test.wal", os.O_CREATE|os.O_RDWR, 0o600); !errors.Is(err, ErrDiskFault) || len(disk.Files()) != 0 {
		t.Fatal("failed creation changed the namespace")
	}
	file = diskOpen(t, disk, "test.wal", os.O_CREATE|os.O_RDWR)
	diskWrite(t, file, []byte("unchanged"))
	if err := disk.Inject(DiskFault{Op: DiskTruncate}); err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(0); !errors.Is(err, ErrDiskFault) || string(diskRead(t, disk, "test.wal")) != "unchanged" {
		t.Fatal("failed truncation changed data")
	}
	if err := disk.Inject(DiskFault{Op: DiskWrite}); err != nil {
		t.Fatal(err)
	}
	disk.ClearFault()
	diskWrite(t, file, []byte("cleared"))
	before := diskRead(t, disk, "test.wal")
	if err := disk.Crash(-1); err == nil || !bytes.Equal(before, diskRead(t, disk, "test.wal")) {
		t.Fatal("invalid crash changed disk state")
	}
}

func TestDiskUsesWALRecoveryForLostTornAndCompleteWrites(t *testing.T) {
	batch := &raftv1.WALBatch{HardState: &raftv1.PersistedHardState{Term: 1, Vote: 1}, Entries: []*raftv1.Entry{{Index: 1, Term: 1, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL, Data: []byte("A")}}}
	record, err := wal.Encode(batch)
	if err != nil {
		t.Fatal(err)
	}
	for _, keep := range []int{0, 1, len(record) - 1, len(record)} {
		disk := NewDisk()
		writer, _, err := wal.OpenFS(disk, ".")
		if err != nil {
			t.Fatal(err)
		}
		if err := writer.Save(&raftv1.WALBatch{HardState: &raftv1.PersistedHardState{}}); err != nil {
			t.Fatal(err)
		}
		if err := disk.Inject(DiskFault{Op: DiskSync}); err != nil {
			t.Fatal(err)
		}
		if err := writer.Save(batch); !errors.Is(err, ErrDiskFault) {
			t.Fatal("expected uncertain save")
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
		diskCrash(t, disk, keep)
		writer, state, err := wal.OpenFS(disk, ".")
		if err != nil {
			t.Fatal(err)
		}
		if keep == len(record) {
			if state.HardState.Term != 1 || len(state.Entries) != 1 {
				t.Fatal("complete retained batch was lost")
			}
		} else if state.HardState.Term != 0 || len(state.Entries) != 0 {
			t.Fatal("lost or torn batch was partially recovered")
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
		before := diskRead(t, disk, disk.Files()[0])
		diskCrash(t, disk, 0)
		if !bytes.Equal(before, diskRead(t, disk, disk.Files()[0])) {
			t.Fatal("tail repair was not synced")
		}
		if err := disk.Flip(disk.Files()[0], int64(len(before)-1), 1); err != nil {
			t.Fatal(err)
		}
		diskCrash(t, disk, 0)
		if _, _, err := wal.OpenFS(disk, "."); !errors.Is(err, wal.ErrCorrupt) {
			t.Fatalf("persistent bit flip did not refuse recovery: %v", err)
		}
	}
}

func TestDiskNamespaceAndHandleContracts(t *testing.T) {
	disk := NewDisk()
	for _, name := range []string{"z.wal", "a.wal"} {
		file := diskOpen(t, disk, name, os.O_CREATE|os.O_EXCL|os.O_RDWR)
		diskWrite(t, file, []byte(name))
		diskSync(t, file)
	}
	diskSync(t, diskOpen(t, disk, ".", os.O_RDONLY))
	names := disk.Files()
	if !slices.Equal(names, []string{"a.wal", "z.wal"}) {
		t.Fatal("namespace was not deterministic")
	}
	names[0] = "caller mutation"
	entries, err := disk.ReadDir(".")
	if err != nil || entries[0].Name() != "a.wal" || !entries[0].Type().IsRegular() {
		t.Fatal("directory entries alias caller data or do not describe files")
	}
	if _, err := disk.OpenFile("a.wal", os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600); !errors.Is(err, fs.ErrExist) {
		t.Fatal("exclusive creation overwrote a segment")
	}
	for _, name := range []string{"missing", "../escape", "/absolute"} {
		if _, err := disk.OpenFile(name, os.O_RDONLY, 0); !errors.Is(err, fs.ErrNotExist) {
			t.Fatal("missing or out-of-root path accepted")
		}
	}
	file := diskOpen(t, disk, "a.wal", os.O_RDONLY)
	if _, err := file.Write([]byte("bad")); !errors.Is(err, fs.ErrPermission) {
		t.Fatal("read-only handle allowed a write")
	}
	if err := file.Truncate(0); !errors.Is(err, fs.ErrPermission) {
		t.Fatal("read-only handle allowed truncation")
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Read(make([]byte, 1)); !errors.Is(err, fs.ErrClosed) {
		t.Fatal("closed handle allowed a read")
	}
	for _, tc := range []struct {
		name   string
		offset int64
		mask   byte
	}{{"missing", 0, 1}, {"a.wal", -1, 1}, {"a.wal", 99, 1}, {"a.wal", 0, 0}} {
		if err := disk.Flip(tc.name, tc.offset, tc.mask); err == nil {
			t.Fatal("invalid bit flip accepted")
		}
	}
}
