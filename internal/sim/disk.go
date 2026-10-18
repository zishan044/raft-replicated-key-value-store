package sim

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/zishan044/raft-replicated-key-value-store.git/internal/wal"
)

var (
	ErrDiskFault = errors.New("injected disk failure")
	ErrNodeDown  = errors.New("node is down")
)

type DiskOp string

const (
	DiskCreate   DiskOp = "create"
	DiskWrite    DiskOp = "write"
	DiskSync     DiskOp = "sync"
	DiskDirSync  DiskOp = "dir-sync"
	DiskTruncate DiskOp = "truncate"
)

// DiskFault fails one matching operation after skipping After matching calls.
// A write may first accept WriteBytes bytes, without syncing them. The fault is
// consumed on firing. Installing another fault replaces the pending one.
type DiskFault struct {
	Op         DiskOp
	After      int
	WriteBytes int
}

type diskData struct {
	mode   fs.FileMode
	data   []byte
	synced []byte
	dirty  bool
}

// Disk implements the operations used by wal.FS, with an existing durable root
// directory named ".". File Sync persists bytes; directory Sync persists names.
// It has no clock, goroutines or randomness and must have a single owner.
type Disk struct {
	files      map[string]*diskData
	synced     map[string]*diskData
	namesDirty bool
	epoch      uint64
	fault      *DiskFault
	trace      func(string)
}

func NewDisk() *Disk {
	return &Disk{files: make(map[string]*diskData), synced: make(map[string]*diskData)}
}

// Inject installs a one-shot failure; invalid controls leave the disk unchanged.
func (d *Disk) Inject(f DiskFault) error {
	if f.After < 0 || f.WriteBytes < 0 || (f.Op != DiskWrite && f.WriteBytes != 0) {
		return errors.New("invalid disk fault counters")
	}
	switch f.Op {
	case DiskCreate, DiskWrite, DiskSync, DiskDirSync, DiskTruncate:
	default:
		return errors.New("unsupported disk fault operation")
	}
	d.fault = &f
	d.log("fault op=%s after=%d write_bytes=%d", f.Op, f.After, f.WriteBytes)
	return nil
}

func (d *Disk) ClearFault() {
	d.fault = nil
	d.log("clear_fault")
}

// Crash drops unsynced directory entries and invalidates every open handle.
// keepTailBytes optionally retains that many appended, unsynced bytes in each
// durably named file. Unsynced truncations are lost. The resulting image becomes
// the new stable image, so retained torn tails survive repeated crashes.
func (d *Disk) Crash(keepTailBytes int) error {
	if keepTailBytes < 0 {
		return errors.New("negative retained tail length")
	}
	d.log("crash keep_tail=%d", keepTailBytes)
	for _, name := range diskNames(d.synced) {
		file := d.synced[name]
		if keepTailBytes > 0 && len(file.data) > len(file.synced) && bytes.Equal(file.data[:len(file.synced)], file.synced) {
			count := min(keepTailBytes, len(file.data)-len(file.synced))
			file.synced = append(bytes.Clone(file.synced), file.data[len(file.synced):len(file.synced)+count]...)
		}
		file.data, file.dirty = bytes.Clone(file.synced), false
	}
	d.files = make(map[string]*diskData, len(d.synced))
	for name, file := range d.synced {
		d.files[name] = file
	}
	d.namesDirty = false
	d.epoch++
	return nil
}

// Files returns a sorted copy of the working namespace without changing trace.
func (d *Disk) Files() []string { return diskNames(d.files) }

func diskNames(files map[string]*diskData) []string {
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// Flip injects persistent corruption into a durably named, synced byte. It
// changes both images; a later crash cannot silently restore the original.
func (d *Disk) Flip(name string, offset int64, mask byte) error {
	file := d.synced[name]
	if file == nil || offset < 0 || offset >= int64(len(file.synced)) || mask == 0 {
		return errors.New("bit flip must name a synced byte and nonzero mask")
	}
	file.synced[offset] ^= mask
	if offset < int64(len(file.data)) {
		file.data[offset] ^= mask
	}
	d.log("flip file=%s offset=%d mask=%02x", name, offset, mask)
	return nil
}

func (d *Disk) log(format string, args ...any) {
	if d.trace != nil {
		d.trace(fmt.Sprintf(format, args...))
	}
}

func (d *Disk) before(op DiskOp, name string, count int) *DiskFault {
	d.log("op=%s file=%s bytes=%d", op, name, count)
	if d.fault == nil || d.fault.Op != op {
		return nil
	}
	if d.fault.After > 0 {
		d.fault.After--
		return nil
	}
	fault := d.fault
	d.fault = nil
	d.log("failure op=%s file=%s", op, name)
	return fault
}

func (d *Disk) OpenFile(name string, flags int, perm fs.FileMode) (wal.File, error) {
	name = filepath.Clean(name)
	if name == "." {
		if flags != os.O_RDONLY {
			return nil, fs.ErrPermission
		}
		return &diskHandle{disk: d, name: name, epoch: d.epoch, dir: true}, nil
	}
	if filepath.Dir(name) != "." || name == ".." {
		return nil, fs.ErrNotExist
	}
	file := d.files[name]
	if file != nil && flags&os.O_CREATE != 0 && flags&os.O_EXCL != 0 {
		return nil, fs.ErrExist
	}
	if file == nil {
		if flags&os.O_CREATE == 0 {
			return nil, fs.ErrNotExist
		}
		if d.before(DiskCreate, name, 0) != nil {
			return nil, ErrDiskFault
		}
		file = &diskData{mode: perm.Perm()}
		d.files[name], d.namesDirty = file, true
	}
	if flags&os.O_TRUNC != 0 && flags&(os.O_RDWR|os.O_WRONLY) != 0 {
		file.data, file.dirty = nil, true
	}
	return &diskHandle{disk: d, file: file, name: name, flags: flags, epoch: d.epoch}, nil
}

func (d *Disk) ReadDir(name string) ([]fs.DirEntry, error) {
	if filepath.Clean(name) != "." {
		return nil, fs.ErrNotExist
	}
	entries := make([]fs.DirEntry, 0, len(d.files))
	for _, name := range d.Files() {
		file := d.files[name]
		entries = append(entries, fs.FileInfoToDirEntry(diskInfo{name: name, size: int64(len(file.data)), mode: file.mode}))
	}
	return entries, nil
}

type diskHandle struct {
	disk   *Disk
	file   *diskData
	name   string
	flags  int
	epoch  uint64
	offset int64
	dir    bool
	closed bool
}

func (h *diskHandle) valid() error {
	if h.closed || h.epoch != h.disk.epoch {
		return fs.ErrClosed
	}
	return nil
}

func (h *diskHandle) Read(p []byte) (int, error) {
	if err := h.valid(); err != nil {
		return 0, err
	}
	if h.dir || h.flags&(os.O_RDWR|os.O_WRONLY) == os.O_WRONLY {
		return 0, fs.ErrPermission
	}
	if len(p) == 0 {
		return 0, nil
	}
	if h.offset >= int64(len(h.file.data)) {
		return 0, io.EOF
	}
	n := copy(p, h.file.data[h.offset:])
	h.offset += int64(n)
	return n, nil
}

func (h *diskHandle) Write(p []byte) (int, error) {
	if err := h.valid(); err != nil {
		return 0, err
	}
	if h.dir || h.flags&(os.O_RDWR|os.O_WRONLY) == 0 {
		return 0, fs.ErrPermission
	}
	fault := h.disk.before(DiskWrite, h.name, len(p))
	data := p
	if fault != nil {
		data = p[:min(len(p), fault.WriteBytes)]
	}
	if h.flags&os.O_APPEND != 0 {
		h.offset = int64(len(h.file.data))
	}
	end := h.offset + int64(len(data))
	if end < h.offset || end > int64(int(^uint(0)>>1)) {
		return 0, fs.ErrInvalid
	}
	if end > int64(len(h.file.data)) {
		h.file.data = append(h.file.data, make([]byte, int(end)-len(h.file.data))...)
	}
	copy(h.file.data[h.offset:end], data)
	h.offset, h.file.dirty = end, true
	if fault != nil {
		return len(data), ErrDiskFault
	}
	return len(data), nil
}

func (h *diskHandle) Sync() error {
	if err := h.valid(); err != nil {
		return err
	}
	op := DiskSync
	if h.dir {
		op = DiskDirSync
	}
	if h.disk.before(op, h.name, 0) != nil {
		return ErrDiskFault
	}
	if h.dir {
		if h.disk.namesDirty {
			h.disk.synced = make(map[string]*diskData, len(h.disk.files))
			for name, file := range h.disk.files {
				h.disk.synced[name] = file
			}
			h.disk.namesDirty = false
		}
	} else if h.file.dirty {
		// ponytail: copy the file at sync; use block-level copy-on-write if
		// large WAL fault campaigns make these copies dominate simulation.
		h.file.synced, h.file.dirty = bytes.Clone(h.file.data), false
	}
	return nil
}

func (h *diskHandle) Truncate(size int64) error {
	if err := h.valid(); err != nil {
		return err
	}
	if h.dir || h.flags&(os.O_RDWR|os.O_WRONLY) == 0 {
		return fs.ErrPermission
	}
	if size < 0 || size > int64(int(^uint(0)>>1)) {
		return fs.ErrInvalid
	}
	if h.disk.before(DiskTruncate, h.name, int(size)) != nil {
		return ErrDiskFault
	}
	if size <= int64(len(h.file.data)) {
		h.file.data = h.file.data[:size]
	} else {
		h.file.data = append(h.file.data, make([]byte, int(size)-len(h.file.data))...)
	}
	h.file.dirty = true
	return nil
}

func (h *diskHandle) Stat() (fs.FileInfo, error) {
	if err := h.valid(); err != nil {
		return nil, err
	}
	if h.dir {
		return diskInfo{name: ".", mode: fs.ModeDir | 0o700}, nil
	}
	return diskInfo{name: h.name, size: int64(len(h.file.data)), mode: h.file.mode}, nil
}

func (h *diskHandle) Close() error {
	if err := h.valid(); err != nil {
		return err
	}
	h.closed = true
	return nil
}

type diskInfo struct {
	name string
	size int64
	mode fs.FileMode
}

func (i diskInfo) Name() string       { return i.name }
func (i diskInfo) Size() int64        { return i.size }
func (i diskInfo) Mode() fs.FileMode  { return i.mode }
func (i diskInfo) ModTime() time.Time { return time.Time{} }
func (i diskInfo) IsDir() bool        { return i.mode.IsDir() }
func (i diskInfo) Sys() any           { return nil }
