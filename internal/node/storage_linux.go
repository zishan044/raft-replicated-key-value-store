package node

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"syscall"

	"github.com/zishan044/raft-replicated-key-value-store.git/internal/wal"
)

var ErrDataDirInUse = errors.New("node data directory is in use")

type identity struct {
	Version int      `json:"version"`
	NodeID  uint64   `json:"node_id"`
	Voters  []uint64 `json:"voters"`
}

type storage struct {
	wal  *wal.WAL
	lock *os.File
}

func (s *storage) close() error {
	var err error
	if s.wal != nil {
		err = s.wal.Close()
	}
	return errors.Join(err, s.lock.Close())
}

func openStorage(cfg Config) (_ *storage, _ wal.State, err error) {
	root, err := filepath.Abs(cfg.DataDir)
	if err != nil {
		return nil, wal.State{}, err
	}
	if err := makeDir(root); err != nil {
		return nil, wal.State{}, fmt.Errorf("create data directory: %w", err)
	}
	lock, err := os.OpenFile(filepath.Join(root, "LOCK"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, wal.State{}, err
	}
	s := &storage{lock: lock}
	defer func() {
		if err != nil {
			err = errors.Join(err, s.close())
		}
	}()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, wal.State{}, ErrDataDirInUse
		}
		return nil, wal.State{}, fmt.Errorf("lock data directory: %w", err)
	}
	want := identity{Version: 1, NodeID: cfg.Raft.ID, Voters: slices.Clone(cfg.Raft.Voters)}
	slices.Sort(want.Voters)
	if err := retainIdentity(root, want); err != nil {
		return nil, wal.State{}, err
	}
	walDir := filepath.Join(root, "wal")
	if err := makeDir(walDir); err != nil {
		return nil, wal.State{}, fmt.Errorf("create WAL directory: %w", err)
	}
	var state wal.State
	s.wal, state, err = wal.Open(walDir)
	if err != nil {
		return nil, wal.State{}, err
	}
	return s, state, nil
}

func retainIdentity(root string, want identity) error {
	path := filepath.Join(root, "identity.json")
	data, err := os.ReadFile(path)
	if err == nil {
		var got identity
		if err := json.Unmarshal(data, &got); err != nil {
			return fmt.Errorf("decode node identity: %w", err)
		}
		if got.Version != want.Version || got.NodeID != want.NodeID || !slices.Equal(got.Voters, want.Voters) {
			return errors.New("data directory identity or voter membership differs from configuration")
		}
		return syncDir(root)
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	// Never assign a new identity to an existing log whose ownership is unknown.
	entries, err := os.ReadDir(filepath.Join(root, "wal"))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if len(entries) != 0 {
		return errors.New("WAL exists without node identity")
	}
	data, err = json.Marshal(want)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(root, "identity-*.tmp")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	_, writeErr := tmp.Write(data)
	if writeErr == nil {
		writeErr = tmp.Sync()
	}
	if err := errors.Join(writeErr, tmp.Close()); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return err
	}
	return syncDir(root)
}

// Create each missing ancestor and sync its parent before creating descendants.
// Also sync an existing directory's parent to retain an uncertain prior create.
func makeDir(path string) error {
	err := os.Mkdir(path, 0o700)
	if errors.Is(err, os.ErrNotExist) {
		if err := makeDir(filepath.Dir(path)); err != nil {
			return err
		}
		err = os.Mkdir(path, 0o700)
	}
	if err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is not a directory", path)
	}
	return syncDir(filepath.Dir(path))
}

func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}
