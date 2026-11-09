package rpc

import (
	"context"
	"log"
	"time"

	storagepb "github.com/HKNain/distributed-storage-system/api/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func RunHeartbeatClient(
	ctx context.Context,
	masterAddress string,
	nodeID string,
	nodeAddress string,
) error {
	conn, err := grpc.NewClient(
		masterAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := storagepb.NewMasterControlServiceClient(conn)

	interval := HeartbeatIntervalFromEnv()

	sendHeartbeat := func() {
		callCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()

		response, err := client.Heartbeat(
			callCtx,
			&storagepb.HeartbeatRequest{
				NodeId:  nodeID,
				Address: nodeAddress,
			},
		)

		if err != nil {
			log.Printf(
				"Heartbeat failed for node %s: %v",
				nodeID,
				err,
			)
			return
		}

		if !response.GetAccepted() {
			log.Printf(
				"Heartbeat rejected for node %s: %s",
				nodeID,
				response.GetMessage(),
			)
			return
		}

		log.Printf("Heartbeat sent: Storage Node %s", nodeID)
	}

	// Register immediately instead of waiting for the first ticker event.
	sendHeartbeat()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("Heartbeat client stopped for node %s", nodeID)
			return nil

		case <-ticker.C:
			sendHeartbeat()
		}
	}
}