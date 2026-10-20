package main

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/zishan044/raft-replicated-key-value-store.git/internal/node"
	"github.com/zishan044/raft-replicated-key-value-store.git/internal/raftcore"
	kvserver "github.com/zishan044/raft-replicated-key-value-store.git/internal/server"
	"github.com/zishan044/raft-replicated-key-value-store.git/internal/transport"
	kvv1 "github.com/zishan044/raft-replicated-key-value-store.git/pkg/api/kv/v1"
	raftv1 "github.com/zishan044/raft-replicated-key-value-store.git/pkg/api/raft/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestArguments(t *testing.T) {
	for _, tc := range []struct {
		args    []string
		code    int
		message string
	}{
		{nil, 2, "usage:"},
		{[]string{"bad"}, 2, "usage:"},
		{[]string{"put", "key"}, 2, "usage:"},
		{[]string{"get", "key", "extra"}, 2, "usage:"},
		{[]string{"cas", "key", "-1", "value"}, 2, "unsigned integer"},
		{[]string{"cas", "key", "bad", "value"}, 2, "unsigned integer"},
		{[]string{"--timeout", "0s", "get", "key"}, 2, "timeout must be positive"},
		{[]string{"--endpoints", "invalid", "status"}, 2, "invalid endpoint"},
		{[]string{"--unknown"}, 2, "flag provided but not defined"},
		{[]string{"--help"}, 0, "usage:"},
	} {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			var out, errOut bytes.Buffer
			code := run(context.Background(), tc.args, &out, &errOut)
			if code != tc.code || !strings.Contains(errOut.String(), tc.message) || out.Len() != 0 {
				t.Fatalf("got %d stdout=%q stderr=%q", code, out.String(), errOut.String())
			}
		})
	}
}

func serveRPC(t *testing.T, rpc *grpc.Server) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- rpc.Serve(l) }()
	t.Cleanup(func() {
		rpc.Stop()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	return l.Addr().String()
}

func TestCommandsAndFreshSessions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	n, err := node.Open(node.Config{DataDir: t.TempDir(), Raft: raftcore.Config{ID: 1, Voters: []uint64{1}, ElectionTicks: 10, HeartbeatTicks: 2, Seed: 42}, Send: func([]*raftv1.Message) {}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Close() })
	st, err := n.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for range st.Raft.ElectionTimeout {
		if err := n.Tick(ctx); err != nil {
			t.Fatal(err)
		}
	}
	rpc, err := transport.NewServer(n.Step)
	if err != nil {
		t.Fatal(err)
	}
	kvserver.Register(rpc, n, nil)
	address := serveRPC(t, rpc)
	call := func(args ...string) (int, string, string) {
		t.Helper()
		var out, errOut bytes.Buffer
		code := run(ctx, append([]string{"--endpoints", address, "--timeout", "2s"}, args...), &out, &errOut)
		return code, out.String(), errOut.String()
	}
	if code, out, err := call("status"); code != 0 || !strings.Contains(out, "node_id=1 role=leader") || err != "" {
		t.Fatalf("status: %d %q %q", code, out, err)
	}
	if code, out, err := call("get", "key"); code != 0 || out != "found=false\n" || err != "" {
		t.Fatalf("missing: %d %q %q", code, out, err)
	}
	code, out, errText := call("put", "key", "first value")
	var first uint64
	if _, err := fmt.Sscanf(out, "revision=%d", &first); code != 0 || err != nil || errText != "" {
		t.Fatalf("put: %d %q %q", code, out, errText)
	}
	if code, out, err := call("get", "key"); code != 0 || out != fmt.Sprintf("value=%q revision=%d\n", "first value", first) || err != "" {
		t.Fatalf("get: %d %q %q", code, out, err)
	}
	before, err := n.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	code, out, errText = call("cas", "key", fmt.Sprint(first), "second")
	var second uint64
	if _, err := fmt.Sscanf(out, "revision=%d", &second); code != 0 || err != nil || errText != "" || second != before.Raft.LastLogIndex+2 {
		t.Fatalf("fresh CAS session: %d %q %q", code, out, errText)
	}
	if code, out, err := call("cas", "key", fmt.Sprint(first), "conflict"); code != 1 || out != "" || !strings.Contains(err, fmt.Sprintf("CAS conflict: current_revision=%d", second)) {
		t.Fatalf("conflict: %d %q %q", code, out, err)
	}
	before, err = n.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if code, out, err := call("del", "key"); code != 0 || out != fmt.Sprintf("revision=%d\n", before.Raft.LastLogIndex+2) || err != "" {
		t.Fatalf("fresh delete session: %d %q %q", code, out, err)
	}
	if code, out, err := call("del", "key"); code != 0 || out != "revision=0\n" || err != "" {
		t.Fatalf("missing delete: %d %q %q", code, out, err)
	}
}

type uncertainService struct {
	kvv1.UnimplementedKVServiceServer
}

func (*uncertainService) RegisterClient(context.Context, *kvv1.RegisterClientRequest) (*kvv1.RegisterClientResponse, error) {
	return &kvv1.RegisterClientResponse{ClientId: 1}, nil
}
func (*uncertainService) Put(context.Context, *kvv1.PutRequest) (*kvv1.PutResponse, error) {
	s, err := status.New(codes.Aborted, "unknown").WithDetails(&kvv1.ErrorDetail{Code: kvv1.ErrorCode_ERROR_CODE_UNKNOWN})
	if err != nil {
		return nil, err
	}
	return nil, s.Err()
}
func TestUncertainExit(t *testing.T) {
	rpc := grpc.NewServer()
	kvv1.RegisterKVServiceServer(rpc, &uncertainService{})
	address := serveRPC(t, rpc)
	var out, errOut bytes.Buffer
	code := run(context.Background(), []string{"--endpoints", address, "--timeout", "120ms", "put", "key", "value"}, &out, &errOut)
	if code != 1 || out.Len() != 0 || !strings.Contains(errOut.String(), "write may have applied") || !strings.Contains(errOut.String(), "session is not saved") {
		t.Fatalf("uncertain exit: %d stdout=%q stderr=%q", code, out.String(), errOut.String())
	}
}
