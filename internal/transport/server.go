package transport

import (
	"context"
	"errors"

	"github.com/zishan044/raft-replicated-key-value-store.git/internal/node"
	"github.com/zishan044/raft-replicated-key-value-store.git/internal/raftcore"
	raftv1 "github.com/zishan044/raft-replicated-key-value-store.git/pkg/api/raft/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type peerService struct {
	raftv1.UnimplementedPeerServiceServer
	step func(context.Context, *raftv1.Message) error
}

// NewServer registers peer delivery with the same message limits as Send.
// Supply Node.Step; it respects cancellation while the owner handles I/O.
// The caller owns the server/listener and may register KV before Serve.
func NewServer(step func(context.Context, *raftv1.Message) error) (*grpc.Server, error) {
	if step == nil {
		return nil, errors.New("node Step callback is required")
	}
	server := grpc.NewServer(
		grpc.MaxRecvMsgSize(raftcore.MaxPeerMessageBytes),
		grpc.MaxSendMsgSize(raftcore.MaxPeerMessageBytes),
	)
	raftv1.RegisterPeerServiceServer(server, &peerService{step: step})
	return server, nil
}

func (s *peerService) Deliver(ctx context.Context, req *raftv1.DeliverRequest) (*raftv1.DeliverResponse, error) {
	if req.GetMessage() == nil {
		return nil, status.Error(codes.InvalidArgument, "peer message is required")
	}
	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()
	if err := s.step(ctx, req.Message); err != nil {
		switch {
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			return nil, status.FromContextError(err).Err()
		case errors.Is(err, node.ErrStopped):
			return nil, status.Error(codes.Unavailable, err.Error())
		default:
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
	}
	// This is a delivery response, not a durable Raft vote/append acknowledgement.
	return &raftv1.DeliverResponse{}, nil
}
