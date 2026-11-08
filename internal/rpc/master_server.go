
package rpc

import (
	"context"
	"strings"

	storagepb "github.com/HKNain/distributed-storage-system/api/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type MasterControlServer struct {
	storagepb.UnimplementedMasterControlServiceServer
	registry *NodeRegistry
}

func NewMasterControlServer(registry *NodeRegistry) *MasterControlServer {
	return &MasterControlServer{
		registry: registry,
	}
}

func (s *MasterControlServer) Heartbeat(
	ctx context.Context,
	req *storagepb.HeartbeatRequest,
) (*storagepb.HeartbeatResponse, error) {

	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "heartbeat request is required")
	}

	nodeID := strings.TrimSpace(req.GetNodeId())
	address := strings.TrimSpace(req.GetAddress())

	if nodeID == "" || address == "" {
		return nil, status.Error(
			codes.InvalidArgument,
			"node ID and address are required",
		)
	}

	accepted := s.registry.RecordHeartbeat(nodeID, address)

	if !accepted {
		return nil, status.Error(
			codes.InvalidArgument,
			"invalid heartbeat",
		)
	}

	return &storagepb.HeartbeatResponse{
		Accepted: true,
		Message:  "heartbeat received",
	}, nil
}
