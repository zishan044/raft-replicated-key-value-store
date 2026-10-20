package main

import (
	"context"
	"fmt"
	"github.com/zishan044/raft-replicated-key-value-store.git/internal/node"
	"github.com/zishan044/raft-replicated-key-value-store.git/internal/raftcore"
	kvv1 "github.com/zishan044/raft-replicated-key-value-store.git/pkg/api/kv/v1"
	raftv1 "github.com/zishan044/raft-replicated-key-value-store.git/pkg/api/raft/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestHealth(t *testing.T) {
	for _, tc := range []struct {
		method string
		path   string
		status int
		body   string
	}{
		{http.MethodGet, "/healthz", http.StatusOK, "ok\n"},
		{http.MethodGet, "/readyz", http.StatusServiceUnavailable, "not ready\n"},
		{http.MethodPost, "/healthz", http.StatusMethodNotAllowed, "method not allowed\n"},
		{http.MethodPost, "/readyz", http.StatusMethodNotAllowed, "method not allowed\n"},
		{http.MethodHead, "/healthz", http.StatusMethodNotAllowed, "method not allowed\n"},
		{http.MethodHead, "/readyz", http.StatusMethodNotAllowed, "method not allowed\n"},
		{http.MethodGet, "/missing", http.StatusNotFound, "404 page not found\n"},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			healthHandler(nil).ServeHTTP(recorder, httptest.NewRequest(tc.method, tc.path, nil))
			if recorder.Code != tc.status || recorder.Body.String() != tc.body {
				t.Fatalf("got (%d, %q), want (%d, %q)", recorder.Code, recorder.Body.String(), tc.status, tc.body)
			}
			if tc.status == http.StatusMethodNotAllowed && recorder.Header().Get("Allow") != http.MethodGet {
				t.Fatal("method rejection must advertise Allow: GET")
			}
		})
	}
}

func TestRunRejectsZeroNodeID(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	err := run(context.Background(), config{nodeID: 0}, logger)
	if err == nil || !strings.Contains(err.Error(), "node-id must be positive") {
		t.Fatalf("expected node ID validation error, got %v", err)
	}
}

func TestRunListenFailure(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	err = run(context.Background(), config{nodeID: 1, dataDir: t.TempDir(), httpAddr: listener.Addr().String(), grpcAddr: "127.0.0.1:0"}, logger)
	if err == nil || !strings.Contains(err.Error(), "listen on") {
		t.Fatalf("expected listen failure, got %v", err)
	}
}

func TestParsePeers(t *testing.T) {
	for _, peers := range []string{"1=127.0.0.1:9001,1=127.0.0.1:9001", "0=host:9001", "abc=host:9001", "1", "2=host:9002", "1=host:9001,"} {
		if _, err := parsePeers(config{nodeID: 1, grpcAddr: "127.0.0.1:9001", peers: peers}); err == nil {
			t.Errorf("accepted %q", peers)
		}
	}
	got, err := parsePeers(config{nodeID: 1, grpcAddr: "127.0.0.1:9001", peers: "2=127.0.0.1:9002,1=127.0.0.1:9001"})
	if err != nil || len(got) != 2 || got[2] != "127.0.0.1:9002" {
		t.Fatalf("peers: %v %v", got, err)
	}
}

func freeAddress(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

func dialKV(t *testing.T, addr string) kvv1.KVServiceClient {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return kvv1.NewKVServiceClient(conn)
}

func eventually(t *testing.T, ctx context.Context, check func() bool) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for !check() {
		select {
		case <-ctx.Done():
			t.Fatal("condition did not converge: ", ctx.Err())
		case <-ticker.C:
		}
	}
}

func TestRunCluster(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	var cfgs [3]config
	var clients [3]kvv1.KVServiceClient
	for i := range cfgs {
		cfgs[i] = config{nodeID: uint64(i + 1), dataDir: t.TempDir(), httpAddr: freeAddress(t), grpcAddr: freeAddress(t)}
		clients[i] = dialKV(t, cfgs[i].grpcAddr)
	}
	peers := fmt.Sprintf("1=%s,2=%s,3=%s", cfgs[0].grpcAddr, cfgs[1].grpcAddr, cfgs[2].grpcAddr)
	var stops [3]context.CancelFunc
	var done [3]chan error
	for i := range cfgs {
		cfgs[i].peers = peers
		var runCtx context.Context
		runCtx, stops[i] = context.WithCancel(ctx)
		done[i] = make(chan error, 1)
		go func(i int) { done[i] <- run(runCtx, cfgs[i], logger) }(i)
	}
	t.Cleanup(func() {
		for _, stop := range stops {
			stop()
		}
		for _, ch := range done {
			if err := <-ch; err != nil {
				t.Error(err)
			}
		}
	})
	httpClient := &http.Client{Timeout: time.Second}
	readiness := func(i int) int {
		resp, err := httpClient.Get("http://" + cfgs[i].httpAddr + "/readyz")
		if err != nil {
			return 0
		}
		defer func() { _ = resp.Body.Close() }()
		return resp.StatusCode
	}
	// Wait until every recovered node knows the leader and has applied its commit.
	eventually(t, ctx, func() bool { return readiness(0) != 0 })
	eventually(t, ctx, func() bool { return readiness(0) == 200 && readiness(1) == 200 && readiness(2) == 200 })
	leader := -1
	eventually(t, ctx, func() bool {
		for i, c := range clients {
			st, err := c.Status(ctx, &kvv1.StatusRequest{})
			if err == nil && st.Role == kvv1.NodeRole_NODE_ROLE_LEADER {
				leader = i
				return true
			}
		}
		return false
	})
	follower := (leader + 1) % 3
	_, err := clients[follower].Get(ctx, &kvv1.GetRequest{Key: "cluster"})
	st := status.Convert(err)
	if st.Code() != codes.FailedPrecondition {
		t.Fatalf("follower redirect: %v", err)
	}
	if len(st.Details()) != 1 {
		t.Fatalf("redirect details: %v", st.Details())
	}
	hint, ok := st.Details()[0].(*kvv1.ErrorDetail)
	if !ok || hint.Code != kvv1.ErrorCode_ERROR_CODE_NOT_LEADER || hint.LeaderAddress != cfgs[leader].grpcAddr {
		t.Fatalf("redirect hint: %v", st.Details())
	}
	registered, err := clients[leader].RegisterClient(ctx, &kvv1.RegisterClientRequest{})
	if err != nil {
		t.Fatal(err)
	}
	written, err := clients[leader].Put(ctx, &kvv1.PutRequest{Session: &kvv1.WriteSession{ClientId: registered.ClientId, Sequence: 1}, Key: "cluster", Value: []byte("works")})
	if err != nil {
		t.Fatal(err)
	}
	got, err := clients[leader].Get(ctx, &kvv1.GetRequest{Key: "cluster"})
	if err != nil || !got.GetFound() || string(got.Value) != "works" || got.Revision != written.Revision {
		t.Fatalf("replicated get: %v %v", got, err)
	}
	// Stop the leader, then require the two remaining daemons to elect and read.
	stops[leader]()
	eventually(t, ctx, func() bool {
		for i, c := range clients {
			if i == leader {
				continue
			}
			callCtx, callCancel := context.WithTimeout(ctx, 100*time.Millisecond)
			st, err := c.Status(callCtx, &kvv1.StatusRequest{})
			callCancel()
			if err == nil && st.Role == kvv1.NodeRole_NODE_ROLE_LEADER && st.LeaderId != uint64(leader+1) {
				got, err := c.Get(ctx, &kvv1.GetRequest{Key: "cluster"})
				return err == nil && got.Found && string(got.Value) == "works" && got.Revision == written.Revision
			}
		}
		return false
	})
}

func TestReadinessTransitions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	n, err := node.Open(node.Config{DataDir: t.TempDir(), Raft: raftcore.Config{ID: 1, Voters: []uint64{1}, ElectionTicks: 10, HeartbeatTicks: 2, Seed: 42}, Send: func([]*raftv1.Message) {}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = n.Close() }()
	check := func(want int) {
		t.Helper()
		recorder := httptest.NewRecorder()
		healthHandler(n).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		if recorder.Code != want {
			t.Fatalf("readiness: %d want %d", recorder.Code, want)
		}
	}
	check(503)
	st, err := n.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for range st.Raft.ElectionTimeout {
		if err := n.Tick(ctx); err != nil {
			t.Fatal(err)
		}
	}
	check(200)
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
	check(503)
}

func TestRunRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cfg := config{nodeID: 1, dataDir: t.TempDir(), httpAddr: freeAddress(t), grpcAddr: freeAddress(t)}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	client := dialKV(t, cfg.grpcAddr)
	start := func() func() {
		runCtx, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- run(runCtx, cfg, logger) }()
		var once sync.Once
		closeRun := func() {
			once.Do(func() {
				stop()
				if err := <-done; err != nil {
					t.Error(err)
				}
			})
		}
		t.Cleanup(closeRun)
		eventually(t, ctx, func() bool {
			callCtx, callCancel := context.WithTimeout(ctx, 100*time.Millisecond)
			defer callCancel()
			st, err := client.Status(callCtx, &kvv1.StatusRequest{})
			return err == nil && st.Role == kvv1.NodeRole_NODE_ROLE_LEADER
		})
		return closeRun
	}
	stop := start()
	registered, err := client.RegisterClient(ctx, &kvv1.RegisterClientRequest{})
	if err != nil {
		t.Fatal(err)
	}
	request := &kvv1.PutRequest{Session: &kvv1.WriteSession{ClientId: registered.ClientId, Sequence: 1}, Key: "restart", Value: []byte("durable")}
	written, err := client.Put(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	stop()
	start()
	retried, err := client.Put(ctx, request)
	if err != nil || retried.GetRevision() != written.Revision {
		t.Fatalf("recovered session: %v %v", retried, err)
	}
	got, err := client.Get(ctx, &kvv1.GetRequest{Key: "restart"})
	if err != nil || !got.GetFound() || string(got.Value) != "durable" || got.Revision != written.Revision {
		t.Fatalf("recovered value: %v %v", got, err)
	}
}

func TestStartupFailureReleasesListeners(t *testing.T) {
	dir := t.TempDir()
	invalidDir := filepath.Join(dir, "file")
	if err := os.WriteFile(invalidDir, []byte("not a directory"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg := config{nodeID: 1, dataDir: invalidDir, httpAddr: freeAddress(t), grpcAddr: freeAddress(t)}
	err := run(context.Background(), cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err == nil || !strings.Contains(err.Error(), "open node") {
		t.Fatalf("startup error: %v", err)
	}
	for _, addr := range []string{cfg.httpAddr, cfg.grpcAddr} {
		l, err := net.Listen("tcp", addr)
		if err != nil {
			t.Fatal("startup leaked listener: ", err)
		}
		_ = l.Close()
	}
}
