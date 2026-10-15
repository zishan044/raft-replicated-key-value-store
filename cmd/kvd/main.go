package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	nodeID := flag.Uint64("node-id", 1, "positive node ID")
	httpAddr := flag.String("http-addr", "127.0.0.1:8080", "HTTP listen address")
	flag.Parse()
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil)).With("node_id", *nodeID)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, *nodeID, *httpAddr, logger); err != nil {
		logger.Error("node failed", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, nodeID uint64, httpAddr string, logger *slog.Logger) error {
	if nodeID == 0 {
		return errors.New("node-id must be positive")
	}
	listener, err := net.Listen("tcp", httpAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", httpAddr, err)
	}
	server := &http.Server{
		Handler:           http.HandlerFunc(serveHealth),
		ReadHeaderTimeout: 5 * time.Second,
	}
	defer func() { _ = server.Close() }()

	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(listener) }()
	logger.Info("node started", "http_addr", listener.Addr().String())

	select {
	case err := <-serveErr:
		return fmt.Errorf("serve HTTP: %w", err)
	case <-ctx.Done():
	}

	logger.Info("node shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shut down HTTP: %w", err)
	}
	if err := <-serveErr; !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve HTTP: %w", err)
	}
	logger.Info("node stopped")
	return nil
}

func serveHealth(w http.ResponseWriter, r *http.Request) {
	var status int
	var body string
	switch r.URL.Path {
	case "/healthz":
		status, body = http.StatusOK, "ok\n"
	case "/readyz":
		status, body = http.StatusServiceUnavailable, "not ready\n"
	default:
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(body))
}
