package rpc

import (
	"errors"
	"fmt"
	"sync"
)

var (
	ErrNoActiveStorageNodes = errors.New("no active storage nodes available")
	ErrStorageNodeNotFound  = errors.New("storage node not found")
	ErrStorageNodeNotActive = errors.New("storage node is not active")
)

type managedStorageClient struct {
	address string
	client  *StorageClient
}

type StorageClientManager struct {
	mu        sync.Mutex
	registry  *NodeRegistry
	clients   map[string]*managedStorageClient
	nextIndex uint64
}

func NewStorageClientManager(registry *NodeRegistry) *StorageClientManager {
	return &StorageClientManager{
		registry: registry,
		clients:  make(map[string]*managedStorageClient),
	}
}

// getOrCreateClientLocked reuses an existing connection or creates one.
// The caller must hold m.mu.
func (m *StorageClientManager) getOrCreateClientLocked(
	node StorageNodeInfo,
) (*StorageClient, error) {
	existing, exists := m.clients[node.NodeID]

	if exists && existing.address == node.Address {
		return existing.client, nil
	}

	client, err := NewStorageClient(node.Address)
	if err != nil {
		return nil, fmt.Errorf(
			"create client for node %s at %s: %w",
			node.NodeID,
			node.Address,
			err,
		)
	}

	// Replace an old connection if the node's address changed.
	if exists {
		_ = existing.client.Close()
	}

	m.clients[node.NodeID] = &managedStorageClient{
		address: node.Address,
		client:  client,
	}

	return client, nil
}

// GetNextActiveNode selects the next ACTIVE node using round-robin.
// It returns both node metadata and its reusable storage client.
func (m *StorageClientManager) GetNextActiveNode() (
	StorageNodeInfo,
	*StorageClient,
	error,
) {
	if m.registry == nil {
		return StorageNodeInfo{}, nil, errors.New("node registry is nil")
	}

	nodes := m.registry.Snapshot()

	activeNodes := make([]StorageNodeInfo, 0, len(nodes))

	for _, node := range nodes {
		if node.State == NodeActive {
			activeNodes = append(activeNodes, node)
		}
	}

	if len(activeNodes) == 0 {
		return StorageNodeInfo{}, nil, ErrNoActiveStorageNodes
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	startIndex := int(m.nextIndex % uint64(len(activeNodes)))

	var lastErr error

	for offset := 0; offset < len(activeNodes); offset++ {
		index := (startIndex + offset) % len(activeNodes)
		node := activeNodes[index]

		client, err := m.getOrCreateClientLocked(node)
		if err != nil {
			lastErr = err
			continue
		}

		// The next request starts after the selected node.
		m.nextIndex = uint64(index + 1)

		return node, client, nil
	}

	return StorageNodeInfo{}, nil, fmt.Errorf(
		"could not create a client for any active node: %w",
		lastErr,
	)
}

// GetClientForNode retrieves the client for a specific ACTIVE node.
// This is useful when reading a chunk whose StorageNodeID is persisted.
func (m *StorageClientManager) GetClientForNode(
	nodeID string,
) (*StorageClient, error) {
	if m.registry == nil {
		return nil, errors.New("node registry is nil")
	}

	nodes := m.registry.Snapshot()

	for _, node := range nodes {
		if node.NodeID != nodeID {
			continue
		}

		if node.State != NodeActive {
			return nil, fmt.Errorf(
				"%w: %s (%s)",
				ErrStorageNodeNotActive,
				node.NodeID,
				node.State,
			)
		}

		m.mu.Lock()
		defer m.mu.Unlock()

		return m.getOrCreateClientLocked(node)
	}

	return nil, fmt.Errorf("%w: %s", ErrStorageNodeNotFound, nodeID)
}

// Close closes all managed gRPC connections.
func (m *StorageClientManager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for nodeID, managed := range m.clients {
		if err := managed.client.Close(); err != nil {
			fmt.Printf(
				"Failed to close storage client for %s: %v\n",
				nodeID,
				err,
			)
		}

		delete(m.clients, nodeID)
	}
}
