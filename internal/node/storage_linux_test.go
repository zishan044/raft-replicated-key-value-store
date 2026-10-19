package node

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestDataDirectoryIdentityAndExclusiveOwnership(t *testing.T) {
	cfg, _ := config(t, 3, 1, 2)
	n := openNode(t, cfg)
	if second, err := Open(cfg); second != nil || !errors.Is(err, ErrDataDirInUse) {
		t.Fatalf("second owner was accepted: %v %v", second, err)
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
	identityPath := filepath.Join(cfg.DataDir, "identity.json")
	original, err := os.ReadFile(identityPath)
	if err != nil {
		t.Fatal(err)
	}
	// The voter set is fixed; its input ordering is immaterial.
	cfg.Raft.Voters = []uint64{2, 3, 1}
	n = openNode(t, cfg)
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*Config){
		func(c *Config) { c.Raft.ID = 2 },
		func(c *Config) { c.Raft.Voters = []uint64{1, 2, 3, 4, 5} },
	} {
		wrong := cfg
		change(&wrong)
		if node, err := Open(wrong); node != nil || err == nil || !strings.Contains(err.Error(), "identity") {
			t.Fatalf("identity mismatch accepted: %v %v", node, err)
		}
	}
	current, err := os.ReadFile(identityPath)
	if err != nil || !bytes.Equal(current, original) {
		t.Fatal("failed startup changed the durable identity")
	}
	if err := os.WriteFile(identityPath, []byte("broken metadata"), 0o600); err != nil {
		t.Fatal(err)
	}
	if node, err := Open(cfg); node != nil || err == nil || !strings.Contains(err.Error(), "decode node identity") {
		t.Fatalf("malformed identity accepted: %v %v", node, err)
	}
	// Every failed start must relinquish the lock, including malformed metadata.
	if err := os.WriteFile(identityPath, original, 0o600); err != nil {
		t.Fatal(err)
	}
	openNode(t, cfg)
}

func TestDataDirectoryLockAcrossProcesses(t *testing.T) {
	if dir, ok := os.LookupEnv("RAFT_NODE_LOCK_TEST_DIR"); ok {
		cfg, _ := config(t, 1)
		cfg.DataDir = dir
		if node, err := Open(cfg); node != nil || !errors.Is(err, ErrDataDirInUse) {
			t.Fatalf("child acquired the parent's directory: %v %v", node, err)
		}
		return
	}
	cfg, _ := config(t, 1)
	openNode(t, cfg)
	cmd := exec.CommandContext(testContext(t), os.Args[0], "-test.run=^TestDataDirectoryLockAcrossProcesses$")
	cmd.Env = append(os.Environ(), "RAFT_NODE_LOCK_TEST_DIR="+cfg.DataDir)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("cross-process lock check failed: %v\n%s", err, output)
	}
}

func TestStartupRejectsUnidentifiedWALAndInvalidConfiguration(t *testing.T) {
	cfg, _ := config(t, 1)
	if err := os.MkdirAll(filepath.Join(cfg.DataDir, "wal"), 0o700); err != nil {
		t.Fatal(err)
	}
	walPath := filepath.Join(cfg.DataDir, "wal", "00000000000000000001.wal")
	if err := os.WriteFile(walPath, []byte("unknown owner"), 0o600); err != nil {
		t.Fatal(err)
	}
	if node, err := Open(cfg); node != nil || err == nil || !strings.Contains(err.Error(), "without node identity") {
		t.Fatalf("unidentified WAL adopted: %v %v", node, err)
	}
	if data, err := os.ReadFile(walPath); err != nil || string(data) != "unknown owner" {
		t.Fatal("unidentified WAL was changed")
	}
	for _, change := range []func(*Config){
		func(c *Config) { c.DataDir = "" },
		func(c *Config) { c.Send = nil },
		func(c *Config) { c.Raft.ID = 0 },
		func(c *Config) { c.Raft.HardState.Term = 1 },
		func(c *Config) { c.Raft.Voters = []uint64{1, 1} },
	} {
		invalid, _ := config(t, 1)
		change(&invalid)
		if node, err := Open(invalid); node != nil || err == nil {
			t.Fatalf("invalid config accepted: %v %v", node, err)
		}
	}
	// A regular file in the parent chain must not be treated as a directory.
	cfg, _ = config(t, 1)
	parent := filepath.Dir(cfg.DataDir)
	if err := os.WriteFile(parent, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if node, err := Open(cfg); node != nil || err == nil {
		t.Fatalf("invalid storage path accepted: %v %v", node, err)
	}
}
