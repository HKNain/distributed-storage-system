package rpc

import (
	"context"
	"log"
	"time"
	"errors"
	"strings"
	"sync"

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

func RunHeartbeatClients(
    ctx context.Context,
    masterAddresses []string,
    nodeID string,
    nodeAddress string,
) error {
    if len(masterAddresses) == 0 {
        return errors.New("no master addresses configured")
    }

    var wg sync.WaitGroup

    for _, address := range masterAddresses {
        address = strings.TrimSpace(address)
        if address == "" {
            continue
        }

        wg.Add(1)

        go func(masterAddress string) {
            defer wg.Done()

            if err := RunHeartbeatClient(
                ctx,
                masterAddress,
                nodeID,
                nodeAddress,
            ); err != nil {
                log.Printf(
                    "Heartbeat client failed for master %s: %v",
                    masterAddress,
                    err,
                )
            }
        }(address)
    }

    <-ctx.Done()
    wg.Wait()

    return nil
}