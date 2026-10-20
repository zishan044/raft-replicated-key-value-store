// Package client provides a context-aware KV client with one sequential write
// session. Keep the Client alive to resolve uncertain writes with RetryPending.
package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"strconv"
	"sync"
	"time"

	kvv1 "github.com/zishan044/raft-replicated-key-value-store.git/pkg/api/kv/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const DefaultTimeout = 5 * time.Second

var (
	ErrUnknown      = errors.New("write may have applied")
	ErrPendingWrite = errors.New("unresolved write; call RetryPending before another write")
	ErrNoPending    = errors.New("no pending write")
	ErrClosed       = errors.New("client closed")
)

// Error carries the server's typed business detail. Unknown also matches
// ErrUnknown through errors.Is; Cause retains the last RPC/context failure.
type Error struct {
	Detail *kvv1.ErrorDetail
	Cause  error
}

func (e *Error) Error() string {
	switch e.Detail.Code {
	case kvv1.ErrorCode_ERROR_CODE_UNKNOWN:
		return fmt.Sprintf("write may have applied: %v", e.Cause)
	case kvv1.ErrorCode_ERROR_CODE_CAS_CONFLICT:
		return fmt.Sprintf("CAS conflict: current_revision=%d", e.Detail.CurrentRevision)
	case kvv1.ErrorCode_ERROR_CODE_INVALID_SEQUENCE:
		return fmt.Sprintf("invalid sequence: expected_sequence=%d", e.Detail.ExpectedSequence)
	case kvv1.ErrorCode_ERROR_CODE_INVALID_SESSION:
		return "invalid session"
	case kvv1.ErrorCode_ERROR_CODE_NOT_LEADER:
		return fmt.Sprintf("not leader: leader_id=%d leader_address=%q", e.Detail.LeaderId, e.Detail.LeaderAddress)
	case kvv1.ErrorCode_ERROR_CODE_BUSY:
		return "uncommitted log is full"
	default:
		return fmt.Sprint(e.Cause)
	}
}
func (e *Error) Unwrap() error { return e.Cause }
func (e *Error) Is(target error) bool {
	return target == ErrUnknown && e.Detail.Code == kvv1.ErrorCode_ERROR_CODE_UNKNOWN
}

type Config struct {
	Endpoints []string      // host:port seeds; leader hints may discover other endpoints
	Timeout   time.Duration // zero uses DefaultTimeout, including lock waits and retries
}

type pendingWrite struct {
	command   *kvv1.Command
	uncertain bool
}

// Client permits concurrent reads/status calls and serializes writes. No session
// or pending command is persisted locally. Close releases its gRPC connections.
type Client struct {
	endpoints   []string
	timeout     time.Duration
	ctx         context.Context
	cancel      context.CancelFunc
	mu          sync.Mutex // connection map and leader hint only; never held during RPCs
	connections map[string]*grpc.ClientConn
	leader      string
	closeOnce   sync.Once
	closeErr    error
	writes      chan struct{}
	// Owned exclusively by the writes gate.
	clientID, sequence uint64
	pending            *pendingWrite
}

func validEndpoint(endpoint string) bool {
	host, port, err := net.SplitHostPort(endpoint)
	n, portErr := strconv.Atoi(port)
	return err == nil && host != "" && portErr == nil && n > 0 && n <= 65535
}

// New validates configuration and creates a lazy client; no network I/O occurs.
func New(cfg Config) (*Client, error) {
	if len(cfg.Endpoints) == 0 {
		return nil, errors.New("at least one endpoint is required")
	}
	if cfg.Timeout < 0 {
		return nil, errors.New("timeout must be positive")
	}
	for _, endpoint := range cfg.Endpoints {
		if !validEndpoint(endpoint) {
			return nil, fmt.Errorf("invalid endpoint %q; want host:port", endpoint)
		}
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = DefaultTimeout
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Client{endpoints: append([]string(nil), cfg.Endpoints...), timeout: cfg.Timeout, ctx: ctx, cancel: cancel, connections: make(map[string]*grpc.ClientConn), writes: make(chan struct{}, 1), sequence: 1}, nil
}

func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		c.cancel()
		c.mu.Lock()
		defer c.mu.Unlock()
		for _, conn := range c.connections {
			c.closeErr = errors.Join(c.closeErr, conn.Close())
		}
	})
	return c.closeErr
}

func (c *Client) operation(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	stop := context.AfterFunc(c.ctx, cancel)
	return ctx, func() { stop(); cancel() }
}
func (c *Client) lockWrite(ctx context.Context) error {
	if c.ctx.Err() != nil {
		return ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-c.ctx.Done():
		return ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	case c.writes <- struct{}{}:
		return nil
	}
}
func (c *Client) connection(endpoint string) (kvv1.KVServiceClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ctx.Err() != nil {
		return nil, ErrClosed
	}
	conn := c.connections[endpoint]
	if conn == nil {
		var err error
		conn, err = grpc.NewClient("passthrough:///"+endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithConnectParams(grpc.ConnectParams{Backoff: backoff.Config{BaseDelay: 100 * time.Millisecond, Multiplier: 1.6, Jitter: 0.2, MaxDelay: time.Second}, MinConnectTimeout: time.Second}))
		if err != nil {
			return nil, err
		}
		c.connections[endpoint] = conn
	}
	return kvv1.NewKVServiceClient(conn), nil
}
func (c *Client) preferred() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.leader != "" {
		return c.leader
	}
	return c.endpoints[0]
}
func (c *Client) remember(endpoint string) {
	c.mu.Lock()
	c.leader = endpoint
	c.mu.Unlock()
}
func (c *Client) next(endpoint string) string {
	c.mu.Lock()
	if c.leader == endpoint {
		c.leader = ""
	}
	c.mu.Unlock()
	for i, seed := range c.endpoints {
		if seed == endpoint {
			return c.endpoints[(i+1)%len(c.endpoints)]
		}
	}
	return c.endpoints[0]
}

func rpcError(err error) error {
	for _, detail := range status.Convert(err).Details() {
		if detail, ok := detail.(*kvv1.ErrorDetail); ok {
			return &Error{Detail: detail, Cause: err}
		}
	}
	return err
}
func detailCode(err error) kvv1.ErrorCode {
	var business *Error
	if errors.As(err, &business) {
		return business.Detail.Code
	}
	return kvv1.ErrorCode_ERROR_CODE_UNSPECIFIED
}
func unknown(cause error) error {
	return &Error{Detail: &kvv1.ErrorDetail{Code: kvv1.ErrorCode_ERROR_CODE_UNKNOWN}, Cause: cause}
}

// retry only resolves a write on success or a replicated application outcome.
// Definite rejections from later attempts cannot erase earlier uncertainty.
func (c *Client) retry(ctx context.Context, cmd *kvv1.Command, write *pendingWrite) (*kvv1.CommandResult, error) {
	endpoint := c.preferred()
	delay := 20 * time.Millisecond
	var last error
	for {
		if ctx.Err() != nil || c.ctx.Err() != nil {
			cause := ctx.Err()
			if c.ctx.Err() != nil {
				cause = ErrClosed
			}
			if write != nil && write.uncertain {
				return nil, unknown(errors.Join(cause, last))
			}
			return nil, cause
		}
		api, err := c.connection(endpoint)
		if err != nil {
			if write != nil && write.uncertain {
				return nil, unknown(err)
			}
			return nil, err
		}
		attempt, cancel := context.WithTimeout(ctx, time.Second)
		result, err := invoke(attempt, api, cmd)
		cancel()
		if err == nil {
			c.remember(endpoint)
			return result, nil
		}
		err = rpcError(err)
		last = err
		code := detailCode(err)
		switch code {
		case kvv1.ErrorCode_ERROR_CODE_CAS_CONFLICT, kvv1.ErrorCode_ERROR_CODE_INVALID_SESSION, kvv1.ErrorCode_ERROR_CODE_INVALID_SEQUENCE:
			c.remember(endpoint)
			return nil, err
		case kvv1.ErrorCode_ERROR_CODE_NOT_LEADER:
			hint := err.(*Error).Detail.LeaderAddress
			if validEndpoint(hint) && hint != endpoint {
				c.remember(hint)
				endpoint = hint
			} else {
				endpoint = c.next(endpoint)
			}
		case kvv1.ErrorCode_ERROR_CODE_BUSY:
			endpoint = c.next(endpoint)
		case kvv1.ErrorCode_ERROR_CODE_UNKNOWN:
			if write != nil {
				write.uncertain = true
			}
			endpoint = c.next(endpoint)
		default:
			switch status.Code(err) {
			case codes.InvalidArgument, codes.ResourceExhausted, codes.Unauthenticated, codes.PermissionDenied, codes.Unimplemented:
				if write != nil && write.uncertain {
					return nil, unknown(err)
				}
				return nil, err
			default:
				// Includes transport loss and attempt cancellation. A
				// dispatched RPC failure cannot tell us whether the entry was appended.
				if write != nil {
					write.uncertain = true
				}
				endpoint = c.next(endpoint)
			}
		}
		// Equal jitter, with exponential growth capped at 250 ms.
		timer := time.NewTimer(delay/2 + time.Duration(rand.Int64N(int64(delay/2))))
		select {
		case <-ctx.Done():
		case <-timer.C:
		}
		timer.Stop()
		delay = min(delay*2, 250*time.Millisecond)
	}
}

func invoke(ctx context.Context, api kvv1.KVServiceClient, cmd *kvv1.Command) (*kvv1.CommandResult, error) {
	switch op := cmd.Operation.(type) {
	case *kvv1.Command_RegisterClient:
		r, err := api.RegisterClient(ctx, op.RegisterClient)
		return &kvv1.CommandResult{ClientId: r.GetClientId()}, err
	case *kvv1.Command_Get:
		r, err := api.Get(ctx, op.Get)
		return &kvv1.CommandResult{Value: r.GetValue(), Revision: r.GetRevision(), Found: r.GetFound()}, err
	case *kvv1.Command_Put:
		r, err := api.Put(ctx, op.Put)
		return &kvv1.CommandResult{Revision: r.GetRevision()}, err
	case *kvv1.Command_Delete:
		r, err := api.Delete(ctx, op.Delete)
		return &kvv1.CommandResult{Revision: r.GetRevision()}, err
	case *kvv1.Command_CompareAndSwap:
		r, err := api.CompareAndSwap(ctx, op.CompareAndSwap)
		return &kvv1.CommandResult{Revision: r.GetRevision()}, err
	default:
		return nil, errors.New("unsupported command")
	}
}

func (c *Client) Put(ctx context.Context, key string, value []byte) (uint64, error) {
	return c.write(ctx, &kvv1.Command{Operation: &kvv1.Command_Put{Put: &kvv1.PutRequest{Key: key, Value: bytes.Clone(value)}}})
}
func (c *Client) Delete(ctx context.Context, key string) (uint64, error) {
	return c.write(ctx, &kvv1.Command{Operation: &kvv1.Command_Delete{Delete: &kvv1.DeleteRequest{Key: key}}})
}
func (c *Client) CompareAndSwap(ctx context.Context, key string, expectedRevision uint64, value []byte) (uint64, error) {
	return c.write(ctx, &kvv1.Command{Operation: &kvv1.Command_CompareAndSwap{CompareAndSwap: &kvv1.CompareAndSwapRequest{Key: key, ExpectedRevision: expectedRevision, Value: bytes.Clone(value)}}})
}
func (c *Client) write(ctx context.Context, cmd *kvv1.Command) (uint64, error) {
	ctx, cancel := c.operation(ctx)
	defer cancel()
	if err := c.lockWrite(ctx); err != nil {
		return 0, err
	}
	defer func() { <-c.writes }()
	if c.pending != nil {
		return 0, ErrPendingWrite
	}
	if c.sequence == 0 {
		return 0, &Error{Detail: &kvv1.ErrorDetail{Code: kvv1.ErrorCode_ERROR_CODE_INVALID_SEQUENCE}, Cause: errors.New("session sequence exhausted")}
	}
	if c.clientID == 0 {
		result, err := c.retry(ctx, &kvv1.Command{Operation: &kvv1.Command_RegisterClient{RegisterClient: &kvv1.RegisterClientRequest{}}}, nil)
		if err != nil {
			return 0, fmt.Errorf("register client: %w", err)
		}
		if result.ClientId == 0 {
			return 0, errors.New("server returned an invalid client ID")
		}
		c.clientID = result.ClientId
	}
	session := &kvv1.WriteSession{ClientId: c.clientID, Sequence: c.sequence}
	switch op := cmd.Operation.(type) {
	case *kvv1.Command_Put:
		op.Put.Session = session
	case *kvv1.Command_Delete:
		op.Delete.Session = session
	case *kvv1.Command_CompareAndSwap:
		op.CompareAndSwap.Session = session
	}
	c.pending = &pendingWrite{command: cmd}
	return c.finish(ctx)
}
func (c *Client) finish(ctx context.Context) (uint64, error) {
	result, err := c.retry(ctx, c.pending.command, c.pending)
	if errors.Is(err, ErrUnknown) {
		return 0, err
	}
	c.pending = nil
	if err != nil {
		if detailCode(err) == kvv1.ErrorCode_ERROR_CODE_CAS_CONFLICT {
			c.sequence++
		}
		return 0, err
	}
	c.sequence++
	return result.Revision, nil
}

// RetryPending resends the exact unresolved command and sequence. A resolved
// CAS conflict consumes its sequence too. ErrNoPending means none is retained.
func (c *Client) RetryPending(ctx context.Context) (uint64, error) {
	ctx, cancel := c.operation(ctx)
	defer cancel()
	if err := c.lockWrite(ctx); err != nil {
		return 0, err
	}
	defer func() { <-c.writes }()
	if c.pending == nil {
		return 0, ErrNoPending
	}
	return c.finish(ctx)
}
func (c *Client) Get(ctx context.Context, key string) (*kvv1.GetResponse, error) {
	ctx, cancel := c.operation(ctx)
	defer cancel()
	result, err := c.retry(ctx, &kvv1.Command{Operation: &kvv1.Command_Get{Get: &kvv1.GetRequest{Key: key}}}, nil)
	if err != nil {
		return nil, err
	}
	return &kvv1.GetResponse{Value: result.Value, Revision: result.Revision, Found: result.Found}, nil
}

// Status queries exactly one endpoint without redirects or retries. An empty
// endpoint selects the first configured seed; this is a local observation.
func (c *Client) Status(ctx context.Context, endpoint string) (*kvv1.StatusResponse, error) {
	ctx, cancel := c.operation(ctx)
	defer cancel()
	if endpoint == "" {
		endpoint = c.endpoints[0]
	}
	if !validEndpoint(endpoint) {
		return nil, fmt.Errorf("invalid endpoint %q", endpoint)
	}
	api, err := c.connection(endpoint)
	if err != nil {
		return nil, err
	}
	result, err := api.Status(ctx, &kvv1.StatusRequest{})
	if err != nil {
		return nil, rpcError(err)
	}
	return result, nil
}
