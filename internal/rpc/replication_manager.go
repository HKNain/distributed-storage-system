package rpc

import (
	"context"
	"errors"
	"fmt"
)

var (
	ErrInsufficientStorageNodes = errors.New("not enough active storage nodes for replication")
	ErrInvalidReplicaCount      = errors.New("replica count must be greater than zero")
)

type ReplicationManager struct {
	storageManager *StorageClientManager
}

func NewReplicationManager(
	storageManager *StorageClientManager,
) *ReplicationManager {
	return &ReplicationManager{
		storageManager: storageManager,
	}
}

// SelectReplicaNodes selects distinct ACTIVE nodes using the existing
// round-robin selection mechanism. The primary node is excluded.
func (r *ReplicationManager) SelectReplicaNodes(
	primaryNodeID string,
	replicaCount int,
) ([]StorageNodeInfo, error) {
	if r.storageManager == nil {
		return nil, errors.New("storage client manager is not initialized")
	}

	if primaryNodeID == "" {
		return nil, errors.New("primary node ID is required")
	}

	if replicaCount <= 0 {
		return nil, ErrInvalidReplicaCount
	}

	// Determine how many nodes are currently ACTIVE.
	activeNodes := r.storageManager.registry.Snapshot()

	activeCount := 0
	for _, node := range activeNodes {
		if node.State == NodeActive {
			activeCount++
		}
	}

	// We need the primary plus the requested number of replicas.
	if activeCount < replicaCount+1 {
		return nil, fmt.Errorf(
			"%w: need %d active nodes, found %d",
			ErrInsufficientStorageNodes,
			replicaCount+1,
			activeCount,
		)
	}

	selected := make([]StorageNodeInfo, 0, replicaCount)
	selectedIDs := make(map[string]struct{})

	// Each call advances the existing round-robin pointer.
	// Limit attempts to avoid an infinite loop if node states change.
	maxAttempts := activeCount * 2

	for attempt := 0; attempt < maxAttempts; attempt++ {
		if len(selected) == replicaCount {
			break
		}

		node, _, err := r.storageManager.GetNextActiveNode()
		if err != nil {
			return nil, fmt.Errorf("select replica node: %w", err)
		}

		// Never replicate onto the primary node.
		if node.NodeID == primaryNodeID {
			continue
		}

		// Never select the same replica node twice.
		if _, exists := selectedIDs[node.NodeID]; exists {
			continue
		}

		selectedIDs[node.NodeID] = struct{}{}
		selected = append(selected, node)
	}

	if len(selected) != replicaCount {
		return nil, fmt.Errorf(
			"%w: selected %d of %d replicas",
			ErrInsufficientStorageNodes,
			len(selected),
			replicaCount,
		)
	}

	return selected, nil
}

// ReplicateToNode writes a chunk to one selected replica node.
// The caller is responsible for updating replica metadata and WAL.
func (r *ReplicationManager) ReplicateToNode(
	ctx context.Context,
	nodeID string,
	chunkID string,
	data []byte,
	checksum string,
) error {
	if r.storageManager == nil {
		return errors.New("storage client manager is not initialized")
	}

	if nodeID == "" {
		return errors.New("replica node ID is required")
	}

	if chunkID == "" {
		return errors.New("chunk ID is required")
	}

	client, err := r.storageManager.GetClientForNode(nodeID)
	if err != nil {
		return fmt.Errorf(
			"get storage client for replica node %q: %w",
			nodeID,
			err,
		)
	}

	if err := client.StoreChunk(ctx, chunkID, data, checksum); err != nil {
		return fmt.Errorf(
			"store chunk %q on replica node %q: %w",
			chunkID,
			nodeID,
			err,
		)
	}

	return nil
}
