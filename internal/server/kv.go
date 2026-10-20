// Package server exposes the persistent runtime through the KV gRPC contract.
package server

import (
	"context"
	"errors"
	"maps"
	"time"

	"github.com/zishan044/raft-replicated-key-value-store.git/internal/node"
	"github.com/zishan044/raft-replicated-key-value-store.git/internal/raftcore"
	kvv1 "github.com/zishan044/raft-replicated-key-value-store.git/pkg/api/kv/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type kvService struct {
	kvv1.UnimplementedKVServiceServer
	node      *node.Node
	endpoints map[uint64]string
}

// Register adds KV and local Status to the peer server before Serve is called.
func Register(server *grpc.Server, n *node.Node, endpoints map[uint64]string) {
	kvv1.RegisterKVServiceServer(server, &kvService{node: n, endpoints: maps.Clone(endpoints)})
}

func (s *kvService) submit(ctx context.Context, cmd *kvv1.Command) (*kvv1.CommandResult, error) {
	// Bound calls even when a raw RPC client supplies no deadline.
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	result, err := s.node.Submit(ctx, cmd)
	if err != nil {
		return nil, s.requestError(err)
	}
	if result.Error != nil {
		return nil, businessError(result.Error)
	}
	return result, nil
}

func (s *kvService) RegisterClient(ctx context.Context, req *kvv1.RegisterClientRequest) (*kvv1.RegisterClientResponse, error) {
	result, err := s.submit(ctx, &kvv1.Command{Operation: &kvv1.Command_RegisterClient{RegisterClient: req}})
	if err != nil {
		return nil, err
	}
	return &kvv1.RegisterClientResponse{ClientId: result.ClientId}, nil
}
func (s *kvService) Put(ctx context.Context, req *kvv1.PutRequest) (*kvv1.PutResponse, error) {
	result, err := s.submit(ctx, &kvv1.Command{Operation: &kvv1.Command_Put{Put: req}})
	if err != nil {
		return nil, err
	}
	return &kvv1.PutResponse{Revision: result.Revision}, nil
}
func (s *kvService) Get(ctx context.Context, req *kvv1.GetRequest) (*kvv1.GetResponse, error) {
	result, err := s.submit(ctx, &kvv1.Command{Operation: &kvv1.Command_Get{Get: req}})
	if err != nil {
		return nil, err
	}
	return &kvv1.GetResponse{Value: result.Value, Revision: result.Revision, Found: result.Found}, nil
}
func (s *kvService) Delete(ctx context.Context, req *kvv1.DeleteRequest) (*kvv1.DeleteResponse, error) {
	result, err := s.submit(ctx, &kvv1.Command{Operation: &kvv1.Command_Delete{Delete: req}})
	if err != nil {
		return nil, err
	}
	return &kvv1.DeleteResponse{Revision: result.Revision}, nil
}
func (s *kvService) CompareAndSwap(ctx context.Context, req *kvv1.CompareAndSwapRequest) (*kvv1.CompareAndSwapResponse, error) {
	result, err := s.submit(ctx, &kvv1.Command{Operation: &kvv1.Command_CompareAndSwap{CompareAndSwap: req}})
	if err != nil {
		return nil, err
	}
	return &kvv1.CompareAndSwapResponse{Revision: result.Revision}, nil
}
func (s *kvService) Status(ctx context.Context, _ *kvv1.StatusRequest) (*kvv1.StatusResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	local, err := s.node.Status(ctx)
	if err != nil {
		return nil, s.requestError(err)
	}
	st := local.Raft
	role := kvv1.NodeRole_NODE_ROLE_FOLLOWER
	switch st.Role {
	case raftcore.Candidate:
		role = kvv1.NodeRole_NODE_ROLE_CANDIDATE
	case raftcore.Leader:
		role = kvv1.NodeRole_NODE_ROLE_LEADER
	}
	return &kvv1.StatusResponse{NodeId: st.ID, Role: role, Term: st.HardState.Term, LeaderId: st.LeaderID, CommitIndex: st.HardState.Commit, AppliedIndex: st.AppliedIndex, LastLogIndex: st.LastLogIndex}, nil
}

func (s *kvService) requestError(err error) error {
	var leader *node.NotLeaderError
	switch {
	// Unknown wins over joined cancellation/storage errors after admission.
	case errors.Is(err, node.ErrUnknown):
		return detailError(codes.Aborted, "operation may have applied", &kvv1.ErrorDetail{Code: kvv1.ErrorCode_ERROR_CODE_UNKNOWN})
	case errors.As(err, &leader):
		return detailError(codes.FailedPrecondition, "not leader", &kvv1.ErrorDetail{Code: kvv1.ErrorCode_ERROR_CODE_NOT_LEADER, LeaderId: leader.LeaderID, LeaderAddress: s.endpoints[leader.LeaderID]})
	case errors.Is(err, raftcore.ErrBusy):
		return detailError(codes.ResourceExhausted, "uncommitted log is full", &kvv1.ErrorDetail{Code: kvv1.ErrorCode_ERROR_CODE_BUSY})
	case errors.Is(err, raftcore.ErrCommandTooLarge):
		return status.Error(codes.ResourceExhausted, "serialized command exceeds 512 KiB")
	case errors.Is(err, node.ErrStopped):
		return status.Error(codes.Unavailable, "node stopped")
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()
	default:
		return status.Error(codes.InvalidArgument, err.Error())
	}
}

func businessError(detail *kvv1.ErrorDetail) error {
	switch detail.Code {
	case kvv1.ErrorCode_ERROR_CODE_CAS_CONFLICT:
		return detailError(codes.Aborted, "CAS revision conflict", detail)
	case kvv1.ErrorCode_ERROR_CODE_INVALID_SESSION:
		return detailError(codes.FailedPrecondition, "invalid session", detail)
	case kvv1.ErrorCode_ERROR_CODE_INVALID_SEQUENCE:
		return detailError(codes.FailedPrecondition, "invalid sequence", detail)
	default:
		return status.Error(codes.Internal, "unexpected application error")
	}
}

func detailError(code codes.Code, message string, detail *kvv1.ErrorDetail) error {
	st, err := status.New(code, message).WithDetails(detail)
	if err != nil {
		return status.Error(codes.Internal, "encode error detail")
	}
	return st.Err()
}
