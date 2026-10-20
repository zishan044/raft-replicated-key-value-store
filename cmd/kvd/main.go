package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/zishan044/raft-replicated-key-value-store.git/internal/node"
	"github.com/zishan044/raft-replicated-key-value-store.git/internal/raftcore"
	kvserver "github.com/zishan044/raft-replicated-key-value-store.git/internal/server"
	"github.com/zishan044/raft-replicated-key-value-store.git/internal/transport"
)

type config struct {
	nodeID                             uint64
	dataDir, httpAddr, grpcAddr, peers string
}

func main() {
	var cfg config
	flag.Uint64Var(&cfg.nodeID, "node-id", 1, "positive node ID")
	flag.StringVar(&cfg.dataDir, "data-dir", "", "persistent directory (default data/node-<id>)")
	flag.StringVar(&cfg.httpAddr, "http-addr", "127.0.0.1:8080", "HTTP listen address")
	flag.StringVar(&cfg.grpcAddr, "grpc-addr", "127.0.0.1:9000", "shared KV and peer gRPC listen address")
	flag.StringVar(&cfg.peers, "peers", "", "static voters: id=host:port,...; include this node (default single node)")
	flag.Parse()
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil)).With("node_id", cfg.nodeID)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg, logger); err != nil {
		logger.Error("node failed", "error", err)
		os.Exit(1)
	}
}

func parsePeers(cfg config) (map[uint64]string, error) {
	if cfg.nodeID == 0 {
		return nil, errors.New("node-id must be positive")
	}
	if cfg.peers == "" {
		return map[uint64]string{cfg.nodeID: cfg.grpcAddr}, nil
	}
	peers := make(map[uint64]string)
	for item := range strings.SplitSeq(cfg.peers, ",") {
		idText, addr, ok := strings.Cut(strings.TrimSpace(item), "=")
		id, err := strconv.ParseUint(idText, 10, 64)
		if !ok || err != nil || id == 0 || addr == "" {
			return nil, fmt.Errorf("invalid peer %q; want positive-id=host:port", item)
		}
		if _, exists := peers[id]; exists {
			return nil, fmt.Errorf("duplicate peer ID %d", id)
		}
		peers[id] = addr
	}
	if peers[cfg.nodeID] != cfg.grpcAddr {
		return nil, errors.New("peers must include this node with its grpc-addr")
	}
	return peers, nil
}

func run(ctx context.Context, cfg config, logger *slog.Logger) (runErr error) {
	peers, err := parsePeers(cfg)
	if err != nil {
		return err
	}
	// Validate configured endpoints before opening storage or binding listeners.
	// An ephemeral local port is useful for single-node tests; replace it after bind.
	remote := make(map[uint64]string)
	for id, addr := range peers {
		if id != cfg.nodeID {
			remote[id] = addr
		}
	}
	outbound, err := transport.New(cfg.nodeID, remote)
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, outbound.Close()) }()
	httpListener, err := net.Listen("tcp", cfg.httpAddr)
	if err != nil {
		return fmt.Errorf("listen on HTTP %s: %w", cfg.httpAddr, err)
	}
	defer func() { _ = httpListener.Close() }()
	grpcListener, err := net.Listen("tcp", cfg.grpcAddr)
	if err != nil {
		return fmt.Errorf("listen on gRPC %s: %w", cfg.grpcAddr, err)
	}
	defer func() { _ = grpcListener.Close() }()
	peers[cfg.nodeID] = grpcListener.Addr().String()
	if cfg.dataDir == "" {
		cfg.dataDir = fmt.Sprintf("data/node-%d", cfg.nodeID)
	}
	voters := make([]uint64, 0, len(peers))
	for id := range peers {
		voters = append(voters, id)
	}
	n, err := node.Open(node.Config{DataDir: cfg.dataDir, Raft: raftcore.Config{ID: cfg.nodeID, Voters: voters, ElectionTicks: 10, HeartbeatTicks: 2, Seed: rand.Uint64()}, Send: outbound.Send})
	if err != nil {
		return fmt.Errorf("open node: %w", err)
	}
	defer func() { runErr = errors.Join(runErr, n.Close()) }()
	rpc, err := transport.NewServer(n.Step)
	if err != nil {
		return err
	}
	defer rpc.Stop()
	kvserver.Register(rpc, n, peers)
	httpServer := &http.Server{Handler: healthHandler(n), ReadHeaderTimeout: 5 * time.Second}
	defer func() { _ = httpServer.Close() }()
	serveErr := make(chan error, 2)
	var servers sync.WaitGroup
	servers.Go(func() { serveErr <- fmt.Errorf("serve gRPC: %w", rpc.Serve(grpcListener)) })
	servers.Go(func() { serveErr <- fmt.Errorf("serve HTTP: %w", httpServer.Serve(httpListener)) })
	logger.Info("node started", "http_addr", httpListener.Addr().String(), "grpc_addr", grpcListener.Addr().String(), "data_dir", cfg.dataDir)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	running := true
	for running {
		select {
		case <-ctx.Done():
			running = false
		case <-n.Done():
			runErr = errors.Join(node.ErrStopped, n.Err())
			running = false
		case err := <-serveErr:
			runErr = err
			running = false
		case <-ticker.C:
			if err := n.Tick(ctx); err != nil && ctx.Err() == nil {
				runErr = err
				running = false
			}
		}
	}
	logger.Info("node shutting down")
	// Close resolves admitted requests as Unknown before draining gRPC handlers.
	// Synchronous storage I/O must finish before the data-directory lock is released.
	_ = n.Close() // The deferred close adds any storage error to runErr once.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stopped := make(chan struct{})
	go func() { rpc.GracefulStop(); close(stopped) }()
	select {
	case <-stopped:
	case <-shutdownCtx.Done():
		rpc.Stop()
		<-stopped
	}
	runErr = errors.Join(runErr, httpServer.Shutdown(shutdownCtx))
	// Ensure a timed-out HTTP shutdown also releases handlers/connections.
	_ = httpServer.Close()
	servers.Wait()
	logger.Info("node stopped")
	return runErr
}

func healthHandler(n *node.Node) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" && r.URL.Path != "/readyz" {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		code, body := http.StatusOK, "ok\n"
		if r.URL.Path == "/readyz" {
			code, body = http.StatusServiceUnavailable, "not ready\n"
			if n != nil {
				ctx, cancel := context.WithTimeout(r.Context(), time.Second)
				defer cancel()
				st, err := n.Status(ctx)
				if err == nil && st.Raft.LeaderID != 0 && st.Raft.AppliedIndex >= st.Raft.HardState.Commit {
					code, body = http.StatusOK, "ready\n"
				}
			}
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(code)
		_, _ = w.Write([]byte(body))
	})
}
