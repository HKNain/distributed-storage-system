package rpc

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	storagepb "github.com/HKNain/distributed-storage-system/api/proto"
	"github.com/HKNain/distributed-storage-system/internal/utils"

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
		logger.LogError(fmt.Errorf("connect heartbeat client to %s: %w", masterAddress, err))
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
			logger.LogCluster(fmt.Sprintf("event=heartbeat node=%q master=%q result=failed error=%q", nodeID, masterAddress, err))
			return
		}

		if !response.GetAccepted() {
			logger.LogCluster(fmt.Sprintf("event=heartbeat node=%q master=%q result=rejected message=%q", nodeID, masterAddress, response.GetMessage()))
			return
		}

		logger.LogCluster(fmt.Sprintf("event=heartbeat node=%q master=%q result=accepted", nodeID, masterAddress))
	}

	// Register immediately instead of waiting for the first ticker event.
	sendHeartbeat()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.LogCluster(fmt.Sprintf("event=heartbeat node=%q result=stopped", nodeID))
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
				logger.LogError(fmt.Errorf("heartbeat client failed for master %s: %w", masterAddress, err))
			}
		}(address)
	}

	<-ctx.Done()
	wg.Wait()

	return nil
}
