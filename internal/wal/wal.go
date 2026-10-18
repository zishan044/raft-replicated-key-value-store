package wal

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"

	raftv1 "github.com/zishan044/raft-replicated-key-value-store.git/pkg/api/raft/v1"
	"google.golang.org/protobuf/proto"
)

const segmentBytes = 64 << 20

var ErrClosed = errors.New("WAL is closed")

// File is the file/directory handle used by the WAL. Directory handles only
// need Sync and Close. An implementation must follow os.File's semantics.
type File interface {
	io.Reader
	io.Writer
	io.Closer
	Stat() (fs.FileInfo, error)
	Sync() error
	Truncate(size int64) error
}

// FS supplies the operations needed by the OS and simulated disk backends.
// OpenFile uses os flags and permissions. ReadDir lists direct children only.
type FS interface {
	OpenFile(name string, flag int, perm fs.FileMode) (File, error)
	ReadDir(name string) ([]fs.DirEntry, error)
}

type osFS struct{}

func (osFS) OpenFile(name string, flag int, perm fs.FileMode) (File, error) {
	return os.OpenFile(name, flag, perm)
}

func (osFS) ReadDir(name string) ([]fs.DirEntry, error) { return os.ReadDir(name) }

// WAL is a synchronous, single-owner segmented log. Save succeeds only after
// syncing a complete batch and any newly created directory entry. An I/O error
// disables further saves until the WAL is closed and recovered with Open.
type WAL struct {
	fs           FS
	path         string
	dir          File
	file         File
	sequence     uint64
	size         int64
	segmentLimit int64
	state        State
	err          error
	closed       bool
	closeErr     error
}

// Open recovers a WAL using the OS filesystem. The directory must already
// exist, its creation must be durable, and the caller must ensure exclusive
// ownership until Close. It must contain only WAL segments. No lock is taken.
// The returned State is an independent snapshot owned by the caller.
func Open(dir string) (*WAL, State, error) { return OpenFS(osFS{}, dir) }

// OpenFS is Open with an explicit filesystem, for deterministic disk testing.
// Recovery syncs the newest file (including any tail truncation) and directory
// before returning, so complete writes retained after an uncertain save become
// durable before a recovered node can use them.
func OpenFS(fsys FS, dir string) (_ *WAL, _ State, err error) {
	w := &WAL{fs: fsys, path: dir, segmentLimit: segmentBytes}
	w.dir, err = fsys.OpenFile(dir, os.O_RDONLY, 0)
	if err != nil {
		return nil, State{}, fmt.Errorf("open WAL directory: %w", err)
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, w.Close())
		}
	}()
	entries, err := fsys.ReadDir(dir)
	if err != nil {
		return nil, State{}, fmt.Errorf("list WAL segments: %w", err)
	}
	slices.SortFunc(entries, func(a, b fs.DirEntry) int { return strings.Compare(a.Name(), b.Name()) })
	// Compaction is not implemented: sequences start at one and have no gaps.
	// Validate the full listing before opening or repairing any segment.
	for i, entry := range entries {
		if entry.Name() != segmentName(uint64(i)+1) || !entry.Type().IsRegular() {
			return nil, State{}, fmt.Errorf("%w: unexpected segment %q", ErrCorrupt, entry.Name())
		}
	}
	for i, entry := range entries {
		newest := i == len(entries)-1
		flags := os.O_RDONLY
		if newest {
			flags = os.O_RDWR | os.O_APPEND
		}
		w.file, err = fsys.OpenFile(filepath.Join(dir, entry.Name()), flags, 0)
		if err != nil {
			return nil, State{}, fmt.Errorf("open segment %s: %w", entry.Name(), err)
		}
		info, err := w.file.Stat()
		if err != nil {
			return nil, State{}, fmt.Errorf("stat segment %s: %w", entry.Name(), err)
		}
		if !info.Mode().IsRegular() || (!newest && info.Size() == 0) {
			return nil, State{}, fmt.Errorf("%w: invalid segment %s", ErrCorrupt, entry.Name())
		}
		valid, err := w.state.Replay(w.file, info.Size(), newest)
		if err != nil {
			return nil, State{}, fmt.Errorf("recover segment %s: %w", entry.Name(), err)
		}
		if !newest {
			f := w.file
			w.file = nil
			if err := f.Close(); err != nil {
				return nil, State{}, fmt.Errorf("close recovered segment: %w", err)
			}
			continue
		}
		w.sequence, w.size = uint64(i)+1, valid
		if valid != info.Size() {
			if err := w.file.Truncate(valid); err != nil {
				return nil, State{}, fmt.Errorf("truncate WAL tail: %w", err)
			}
		}
		if err := w.file.Sync(); err != nil {
			return nil, State{}, fmt.Errorf("sync recovered segment: %w", err)
		}
	}
	if err := w.dir.Sync(); err != nil {
		return nil, State{}, fmt.Errorf("sync WAL directory on recovery: %w", err)
	}
	state := State{HardState: w.state.HardState, Entries: make([]*raftv1.Entry, len(w.state.Entries))}
	for i, entry := range w.state.Entries {
		state.Entries[i] = proto.Clone(entry).(*raftv1.Entry)
	}
	return w, state, nil
}

func segmentName(sequence uint64) string {
	return fmt.Sprintf("%020d.wal", sequence)
}

// Save validates and durably appends one batch. Validation errors leave the WAL
// usable and unchanged. On an I/O error, the batch's outcome is uncertain:
// release no dependent messages, stop the node, and recover before resuming.
// Records stay intact; a record larger than 64 MiB occupies its own segment.
func (w *WAL) Save(batch *raftv1.WALBatch) error {
	if w.closed {
		return ErrClosed
	}
	if w.err != nil {
		return w.err
	}
	next := w.state
	if err := next.apply(batch); err != nil {
		return fmt.Errorf("invalid WAL batch: %w", err)
	}
	record, err := Encode(batch)
	if err != nil {
		return err
	}
	created := w.file == nil || (w.size > 0 && int64(len(record)) > w.segmentLimit-w.size)
	if created {
		if err := w.rotate(); err != nil {
			return w.fail(err)
		}
	}
	remaining := record
	for len(remaining) > 0 {
		n, err := w.file.Write(remaining)
		if err != nil {
			return w.fail(fmt.Errorf("write WAL batch: %w", err))
		}
		if n <= 0 || n > len(remaining) {
			return w.fail(io.ErrShortWrite)
		}
		remaining = remaining[n:]
	}
	if err := w.file.Sync(); err != nil {
		return w.fail(fmt.Errorf("sync WAL batch: %w", err))
	}
	if created {
		if err := w.dir.Sync(); err != nil {
			return w.fail(fmt.Errorf("sync new WAL segment directory entry: %w", err))
		}
	}
	w.size += int64(len(record))
	w.state = next
	return nil
}

func (w *WAL) rotate() error {
	if w.sequence == math.MaxUint64 {
		return errors.New("WAL segment sequence exhausted")
	}
	if w.file != nil {
		f := w.file
		w.file = nil
		if err := f.Close(); err != nil {
			return fmt.Errorf("close previous WAL segment: %w", err)
		}
	}
	name := filepath.Join(w.path, segmentName(w.sequence+1))
	f, err := w.fs.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("create WAL segment: %w", err)
	}
	w.file, w.sequence, w.size = f, w.sequence+1, 0
	return nil
}

func (w *WAL) fail(err error) error {
	w.err = err
	return err
}

// Close releases both handles, returning all close failures. It does not retry
// or sync a failed batch. Repeated calls return the same result.
func (w *WAL) Close() error {
	if !w.closed {
		w.closed = true
		if w.file != nil {
			w.closeErr = w.file.Close()
		}
		if w.dir != nil {
			w.closeErr = errors.Join(w.closeErr, w.dir.Close())
		}
	}
	return w.closeErr
}
