package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
			serveHealth(recorder, httptest.NewRequest(tc.method, tc.path, nil))
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
	err := run(context.Background(), 0, "127.0.0.1:0", logger)
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
	err = run(context.Background(), 1, listener.Addr().String(), logger)
	if err == nil || !strings.Contains(err.Error(), "listen on") {
		t.Fatalf("expected listen failure, got %v", err)
	}
}
