
package rpc

import (
	"fmt"
	"net"

	storagepb "github.com/HKNain/distributed-storage-system/api/proto"
	"google.golang.org/grpc"
)

func StartMasterGRPCServer(
	address string,
	registry *NodeRegistry,
) (*grpc.Server, net.Listener, error) {

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to listen on %s: %w", address, err)
	}

	server := grpc.NewServer()

	masterControl := NewMasterControlServer(registry)

	storagepb.RegisterMasterControlServiceServer(
		server,
		masterControl,
	)

	return server, listener, nil
}
