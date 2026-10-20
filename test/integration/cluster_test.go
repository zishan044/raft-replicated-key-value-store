//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	kvv1 "github.com/zishan044/raft-replicated-key-value-store.git/pkg/api/kv/v1"
	"github.com/zishan044/raft-replicated-key-value-store.git/pkg/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type process struct {
	cmd  *exec.Cmd
	done chan struct{}
	err  error // published by closing done
}
type member struct {
	address, httpAddress, dataDir, logPath string
	running                                *process
	raw                                    kvv1.KVServiceClient
}
type cluster struct {
	t           *testing.T
	ctx         context.Context
	root, peers string
	nodes       [3]member
	sdk         *client.Client
}

// The whole test (including builds) is bounded independently of go test's
// timeout, leaving time to kill and reap children and print logs on failure.
func startCluster(t *testing.T) *cluster {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	c := &cluster{t: t, ctx: ctx, root: t.TempDir()}
	var conns []*grpc.ClientConn
	t.Cleanup(func() {
		cancel() // CommandContext kills any children still running on failure.
		for i := range c.nodes {
			if p := c.nodes[i].running; p != nil {
				select {
				case <-p.done:
				case <-time.After(5 * time.Second):
					t.Errorf("node %d not reaped after cancellation", i+1)
				}
			}
		}
		if c.sdk != nil {
			if err := c.sdk.Close(); err != nil {
				t.Error(err)
			}
		}
		for _, conn := range conns {
			if err := conn.Close(); err != nil {
				t.Error(err)
			}
		}
		if t.Failed() {
			for i, n := range c.nodes {
				data, err := os.ReadFile(n.logPath)
				if err == nil {
					if len(data) > 16<<10 {
						data = data[len(data)-(16<<10):]
					}
					t.Logf("node %d logs:\n%s", i+1, data)
				}
			}
		}
	})
	repo, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(c.root, "bin")
	if err := os.Mkdir(bin, 0755); err != nil {
		t.Fatal(err)
	}
	build := exec.CommandContext(ctx, "go", "build", "-race", "-o", bin, "./cmd/kvd", "./cmd/kvctl")
	build.Dir = repo
	build.WaitDelay = 2 * time.Second
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build binaries: %v\n%s", err, output)
	}
	// Hold all six reservations together so no two nodes receive the same port.
	var listeners []net.Listener
	for range 6 {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners = append(listeners, l)
		t.Cleanup(func() { _ = l.Close() })
	}
	var endpoints, peers []string
	for i := range c.nodes {
		n := &c.nodes[i]
		n.address = listeners[i].Addr().String()
		n.httpAddress = listeners[i+3].Addr().String()
		n.dataDir = filepath.Join(c.root, fmt.Sprintf("node-%d", i+1))
		n.logPath = n.dataDir + ".log"
		endpoints = append(endpoints, n.address)
		peers = append(peers, fmt.Sprintf("%d=%s", i+1, n.address))
		conn, err := grpc.NewClient("passthrough:///"+n.address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithConnectParams(grpc.ConnectParams{Backoff: backoff.Config{BaseDelay: 100 * time.Millisecond, Multiplier: 1.6, Jitter: 0.2, MaxDelay: time.Second}, MinConnectTimeout: time.Second}))
		if err != nil {
			t.Fatal(err)
		}
		conns = append(conns, conn)
		n.raw = kvv1.NewKVServiceClient(conn)
	}
	c.peers = strings.Join(peers, ",")
	c.sdk, err = client.New(client.Config{Endpoints: endpoints})
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range listeners {
		if err := l.Close(); err != nil {
			t.Fatal(err)
		}
	}
	for i := range c.nodes {
		c.start(i)
	}
	c.leader()
	return c
}

func (c *cluster) start(i int) {
	c.t.Helper()
	n := &c.nodes[i]
	if n.running != nil {
		c.t.Fatalf("node %d already running", i+1)
	}
	log, err := os.OpenFile(n.logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		c.t.Fatal(err)
	}
	cmd := exec.CommandContext(c.ctx, filepath.Join(c.root, "bin", "kvd"), "--node-id", fmt.Sprint(i+1), "--data-dir", n.dataDir, "--grpc-addr", n.address, "--http-addr", n.httpAddress, "--peers", c.peers)
	// Kill daemons if the test runner itself exits before Cleanup can run.
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
	cmd.Stdout = log
	cmd.Stderr = log
	cmd.WaitDelay = 2 * time.Second
	cmd.Env = append(os.Environ(), "GORACE=halt_on_error=1")
	if err := cmd.Start(); err != nil {
		_ = log.Close()
		c.t.Fatal(err)
	}
	p := &process{cmd: cmd, done: make(chan struct{})}
	n.running = p
	go func() { p.err = cmd.Wait(); _ = log.Close(); close(p.done) }()
}
func (c *cluster) stop(i int, crash bool) {
	c.t.Helper()
	n := &c.nodes[i]
	p := n.running
	if p == nil {
		return
	}
	select {
	case <-p.done:
		c.t.Fatalf("node %d exited unexpectedly: %v", i+1, p.err)
	default:
	}
	var err error
	if crash {
		err = p.cmd.Process.Kill()
	} else {
		err = p.cmd.Process.Signal(syscall.SIGTERM)
	}
	if err != nil {
		c.t.Fatal(err)
	}
	select {
	case <-p.done:
	case <-time.After(7 * time.Second):
		_ = p.cmd.Process.Kill()
		c.t.Fatalf("node %d shutdown timed out", i+1)
	}
	if crash {
		state, ok := p.cmd.ProcessState.Sys().(syscall.WaitStatus)
		if !ok || state.Signal() != syscall.SIGKILL {
			c.t.Fatalf("node %d did not exit from SIGKILL: %v", i+1, p.err)
		}
	} else if p.err != nil {
		c.t.Fatalf("node %d graceful exit: %v", i+1, p.err)
	}
	n.running = nil
}
func (c *cluster) await(description string, check func(context.Context) bool) {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(c.ctx, 15*time.Second)
	defer cancel()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		for i, n := range c.nodes {
			if n.running != nil {
				select {
				case <-n.running.done:
					c.t.Fatalf("node %d exited: %v", i+1, n.running.err)
				default:
				}
			}
		}
		if check(ctx) {
			return
		}
		select {
		case <-ctx.Done():
			c.t.Fatalf("timed out waiting for %s: %v", description, ctx.Err())
		case <-ticker.C:
		}
	}
}
func (c *cluster) local(ctx context.Context, i int) (*kvv1.StatusResponse, error) {
	call, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	return c.sdk.Status(call, c.nodes[i].address)
}
func (c *cluster) leader() int {
	c.t.Helper()
	leader := -1
	c.await("running nodes to agree on a leader", func(ctx context.Context) bool {
		var known, term uint64
		candidate := -1
		for i, n := range c.nodes {
			if n.running == nil {
				continue
			}
			st, err := c.local(ctx, i)
			if err != nil || st.LeaderId == 0 {
				return false
			}
			if known == 0 {
				known, term = st.LeaderId, st.Term
			}
			if known != st.LeaderId || term != st.Term {
				return false
			}
			if st.Role == kvv1.NodeRole_NODE_ROLE_LEADER {
				candidate = i
			}
		}
		if candidate < 0 || known != uint64(candidate+1) {
			return false
		}
		leader = candidate
		return true
	})
	return leader
}
func (c *cluster) applied(index uint64) {
	c.t.Helper()
	c.await(fmt.Sprintf("all running nodes to apply index %d", index), func(ctx context.Context) bool {
		for i, n := range c.nodes {
			if n.running != nil {
				st, err := c.local(ctx, i)
				if err != nil || st.AppliedIndex < index || st.CommitIndex < index {
					return false
				}
			}
		}
		return true
	})
}
func (c *cluster) ready() {
	c.t.Helper()
	httpClient := &http.Client{Timeout: time.Second}
	defer httpClient.CloseIdleConnections()
	c.await("HTTP readiness", func(ctx context.Context) bool {
		for _, n := range c.nodes {
			if n.running == nil {
				continue
			}
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+n.httpAddress+"/readyz", nil)
			if err != nil {
				c.t.Fatal(err)
			}
			resp, err := httpClient.Do(req)
			if err != nil {
				return false
			}
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return false
			}
		}
		return true
	})
}
func (c *cluster) value(key, value string, revision uint64) {
	c.t.Helper()
	got, err := c.sdk.Get(c.ctx, key)
	if err != nil || !got.GetFound() || string(got.Value) != value || got.Revision != revision {
		c.t.Fatalf("get %q: %v %v; want value=%q revision=%d", key, got, err, value, revision)
	}
}
func (c *cluster) cli(args ...string) (string, string, error) {
	c.t.Helper()
	var out, errOut bytes.Buffer
	cmd := exec.CommandContext(c.ctx, filepath.Join(c.root, "bin", "kvctl"), args...)
	cmd.Stdout = &out
	cmd.Stderr = &errOut
	cmd.WaitDelay = 2 * time.Second
	err := cmd.Run()
	return out.String(), errOut.String(), err
}
func rawDetail(t *testing.T, err error, code kvv1.ErrorCode) *kvv1.ErrorDetail {
	t.Helper()
	for _, d := range status.Convert(err).Details() {
		if d, ok := d.(*kvv1.ErrorDetail); ok && d.Code == code {
			return d
		}
	}
	t.Fatalf("missing error detail %s: %v", code, err)
	return nil
}

func TestThreeProcessCluster(t *testing.T) {
	c := startCluster(t)
	c.ready()
	leader := c.leader()
	t.Log("operations and follower redirects")
	follower := (leader + 1) % 3
	_, err := c.nodes[follower].raw.Get(c.ctx, &kvv1.GetRequest{Key: "operations"})
	hint := rawDetail(t, err, kvv1.ErrorCode_ERROR_CODE_NOT_LEADER)
	if status.Code(err) != codes.FailedPrecondition || hint.LeaderId != uint64(leader+1) || hint.LeaderAddress != c.nodes[leader].address {
		t.Fatalf("bad leader hint: %v %v", hint, err)
	}
	revision, err := c.sdk.Put(c.ctx, "operations", []byte("initial"))
	if err != nil {
		t.Fatal(err)
	}
	c.value("operations", "initial", revision)
	swapped, err := c.sdk.CompareAndSwap(c.ctx, "operations", revision, []byte("swapped"))
	if err != nil || swapped <= revision {
		t.Fatalf("CAS: %d %v", swapped, err)
	}
	_, err = c.sdk.CompareAndSwap(c.ctx, "operations", revision, []byte("conflict"))
	var business *client.Error
	if !errors.As(err, &business) || business.Detail.Code != kvv1.ErrorCode_ERROR_CODE_CAS_CONFLICT || business.Detail.CurrentRevision != swapped {
		t.Fatalf("CAS conflict: %v", err)
	}
	deleted, err := c.sdk.Delete(c.ctx, "operations")
	if err != nil || deleted <= swapped {
		t.Fatalf("delete: %d %v", deleted, err)
	}
	if revision, err := c.sdk.Delete(c.ctx, "operations"); err != nil || revision != 0 {
		t.Fatalf("missing delete: %d %v", revision, err)
	}
	if got, err := c.sdk.Get(c.ctx, "operations"); err != nil || got.GetFound() || got.GetRevision() != 0 {
		t.Fatalf("missing get: %v %v", got, err)
	}
	created, err := c.sdk.CompareAndSwap(c.ctx, "operations", 0, []byte("created"))
	if err != nil {
		t.Fatal(err)
	}
	c.value("operations", "created", created)

	t.Log("five identical raw writes and cached conflicts")
	raw := c.nodes[c.leader()].raw
	registered, err := raw.RegisterClient(c.ctx, &kvv1.RegisterClientRequest{})
	if err != nil {
		t.Fatal(err)
	}
	request := &kvv1.PutRequest{Session: &kvv1.WriteSession{ClientId: registered.ClientId, Sequence: 1}, Key: "deduplicated", Value: []byte("original")}
	var original *kvv1.PutResponse
	repeatWrite := func() {
		t.Helper()
		raw := c.nodes[c.leader()].raw
		for i := range 5 {
			response, err := raw.Put(c.ctx, request)
			if err != nil {
				t.Fatalf("write retry %d: %v", i, err)
			}
			if original == nil {
				original = response
			}
			if original.Revision == 0 || !proto.Equal(response, original) {
				t.Fatalf("retry changed result: original=%v retry=%v", original, response)
			}
		}
	}
	repeatWrite()
	c.value("deduplicated", "original", original.Revision)
	conflictClient, err := raw.RegisterClient(c.ctx, &kvv1.RegisterClientRequest{})
	if err != nil {
		t.Fatal(err)
	}
	conflictRequest := &kvv1.CompareAndSwapRequest{Session: &kvv1.WriteSession{ClientId: conflictClient.ClientId, Sequence: 1}, Key: "deduplicated", ExpectedRevision: 0, Value: []byte("must not apply")}
	_, err = raw.CompareAndSwap(c.ctx, conflictRequest)
	conflict := rawDetail(t, err, kvv1.ErrorCode_ERROR_CODE_CAS_CONFLICT)
	if conflict.CurrentRevision != original.Revision {
		t.Fatalf("original conflict: %v", conflict)
	}
	// Changing the key through another session must not change the cached conflict.
	changed, err := c.sdk.Put(c.ctx, "deduplicated", []byte("changed by another session"))
	if err != nil {
		t.Fatal(err)
	}
	repeatConflict := func() {
		t.Helper()
		raw := c.nodes[c.leader()].raw
		for range 5 {
			_, err := raw.CompareAndSwap(c.ctx, conflictRequest)
			if !proto.Equal(rawDetail(t, err, kvv1.ErrorCode_ERROR_CODE_CAS_CONFLICT), conflict) {
				t.Fatal("cached conflict changed")
			}
		}
	}
	repeatConflict()

	t.Log("CLI routing and unsuccessful exit codes")
	seeds := c.nodes[follower].address + "," + c.nodes[leader].address
	out, errOut, err := c.cli("--endpoints", seeds, "put", "cli", "from process")
	if err != nil || errOut != "" || !strings.HasPrefix(out, "revision=") {
		t.Fatalf("CLI put: %q %q %v", out, errOut, err)
	}
	out, errOut, err = c.cli("--endpoints", seeds, "get", "cli")
	if err != nil || errOut != "" || !strings.Contains(out, `value="from process" revision=`) {
		t.Fatalf("CLI get: %q %q %v", out, errOut, err)
	}
	_, errOut, err = c.cli("--endpoints", seeds, "cas", "cli", "0", "conflict")
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 1 || !strings.Contains(errOut, "CAS conflict: current_revision=") {
		t.Fatalf("CLI conflict: %q %v", errOut, err)
	}

	t.Log("leader crash and recovery using the same SDK session")
	leader = c.leader()
	c.stop(leader, true)
	replacement := c.leader()
	if replacement == leader {
		t.Fatal("dead leader retained")
	}
	c.value("operations", "created", created)
	recovered, err := c.sdk.Put(c.ctx, "after-failover", []byte("same client"))
	if err != nil {
		t.Fatal(err)
	}
	c.value("after-failover", "same client", recovered)
	c.start(leader)
	c.leader()
	c.applied(recovered)

	t.Log("lost quorum, Unknown retention, and RetryPending")
	leader = c.leader()
	// A successful Get primes the SDK hint before followers are stopped.
	c.value("after-failover", "same client", recovered)
	for i := range c.nodes {
		if i != leader {
			c.stop(i, false)
		}
	}
	baseline, err := c.local(c.ctx, leader)
	if err != nil {
		t.Fatal(err)
	}
	writeCtx, cancel := context.WithCancel(c.ctx)
	done := make(chan error, 1)
	go func() {
		_, err := c.sdk.Put(writeCtx, "pending", []byte("retained"))
		done <- err
	}()
	// Cancel after observing admission, rather than assuming a short deadline
	// necessarily means the write reached the server.
	c.await("pending write admission", func(ctx context.Context) bool {
		st, err := c.local(ctx, leader)
		return err == nil && st.LastLogIndex > baseline.LastLogIndex
	})
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, client.ErrUnknown) {
			t.Fatalf("lost quorum write: %v", err)
		}
	case <-c.ctx.Done():
		t.Fatal("pending write failed to return")
	}
	if _, err := c.sdk.Put(c.ctx, "forbidden", nil); !errors.Is(err, client.ErrPendingWrite) {
		t.Fatalf("new write bypassed pending: %v", err)
	}
	readBaseline, err := c.local(c.ctx, leader)
	if err != nil {
		t.Fatal(err)
	}
	readCtx, cancelRead := context.WithTimeout(c.ctx, time.Second)
	_, err = c.sdk.Get(readCtx, "operations")
	cancelRead()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("isolated old leader returned read: %v", err)
	}
	readStatus, err := c.local(c.ctx, leader)
	if err != nil || readStatus.LastLogIndex <= readBaseline.LastLogIndex {
		t.Fatalf("read did not reach isolated leader: %v %v", readStatus, err)
	}
	for i := range c.nodes {
		if i != leader {
			c.start(i)
		}
	}
	c.leader()
	pendingRevision, err := c.sdk.RetryPending(c.ctx)
	// The first entry was uncommitted: a replacement leader may overwrite it
	// and apply the identical retry at a later index. Only a completed write
	// promises its original revision (checked by the raw retries above/below).
	if err != nil || pendingRevision <= baseline.LastLogIndex {
		t.Fatalf("pending resolution: revision=%d error=%v; must follow index %d", pendingRevision, err, baseline.LastLogIndex)
	}
	c.value("pending", "retained", pendingRevision)
	barrier, err := c.sdk.Put(c.ctx, "barrier", []byte("before restart"))
	if err != nil {
		t.Fatal(err)
	}
	c.applied(barrier)

	t.Log("whole-cluster crash, replay, and five more identical retries")
	for i := range c.nodes {
		c.stop(i, true)
	}
	for i := range c.nodes {
		c.start(i)
	}
	c.leader()
	c.ready()
	c.value("operations", "created", created)
	c.value("after-failover", "same client", recovered)
	c.value("pending", "retained", pendingRevision)
	c.value("barrier", "before restart", barrier)
	repeatWrite() // All five replies must still carry the very first revision.
	c.value("deduplicated", "changed by another session", changed)
	repeatConflict()
	// Restart also preserved the SDK's session, permitting its next sequence.
	final, err := c.sdk.Put(c.ctx, "final", []byte("after restart"))
	if err != nil {
		t.Fatal(err)
	}
	c.applied(final)
	c.ready()
	for i := range c.nodes {
		c.stop(i, false)
	}
	t.Log("all three processes stopped cleanly")
}
