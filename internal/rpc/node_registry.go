package rpc

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/HKNain/distributed-storage-system/internal/utils"
)

type NodeState string

const (
	NodeActive NodeState = "ACTIVE"
	NodeMissed NodeState = "MISSED"
	NodeDead   NodeState = "DEAD"
)

type StorageNodeInfo struct {
	NodeID        string
	Address       string
	LastHeartbeat time.Time
	State         NodeState
	MissedBeats   int
}

type NodeRegistry struct {
	mu    sync.RWMutex
	nodes map[string]*StorageNodeInfo
}

func NewNodeRegistry() *NodeRegistry {
	return &NodeRegistry{
		nodes: make(map[string]*StorageNodeInfo),
	}
}

func (r *NodeRegistry) RecordHeartbeat(nodeID, address string) bool {
	nodeID = strings.TrimSpace(nodeID)
	address = strings.TrimSpace(address)

	if nodeID == "" || address == "" {
		logger.LogCluster("event=heartbeat result=rejected reason=missing_node_identity")
		return false
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	node, exists := r.nodes[nodeID]

	if !exists {
		node = &StorageNodeInfo{NodeID: nodeID}
		r.nodes[nodeID] = node
		logger.LogCluster(fmt.Sprintf("event=node_registration node=%q address=%q", nodeID, address))
	}

	node.Address = address
	node.LastHeartbeat = time.Now()
	node.State = NodeActive
	node.MissedBeats = 0

	return true
}

// Snapshot returns independent copies of registered node information.
func (r *NodeRegistry) Snapshot() []StorageNodeInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	now := time.Now()
	interval := HeartbeatIntervalFromEnv()
	failureThreshold := FailureThresholdFromEnv()

	if interval <= 0 {
		interval = 2 * time.Second
	}
	if failureThreshold <= 0 {
		failureThreshold = 3
	}

	result := make([]StorageNodeInfo, 0, len(r.nodes))

	for _, node := range r.nodes {
		info := *node

		missed := int(now.Sub(info.LastHeartbeat) / interval)
		if missed < 0 {
			missed = 0
		}

		info.MissedBeats = missed

		switch {
		case missed >= failureThreshold:
			info.State = NodeDead
		case missed > 0:
			info.State = NodeMissed
		default:
			info.State = NodeActive
		}

		result = append(result, info)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].NodeID < result[j].NodeID
	})

	return result
}

// WaitForActiveNodes waits until at least minCount nodes are ACTIVE.
func (r *NodeRegistry) WaitForActiveNodes(
	ctx context.Context,
	minCount int,
) error {
	if minCount < 1 {
		minCount = 1
	}

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		activeCount := 0

		for _, node := range r.Snapshot() {
			if node.State == NodeActive {
				activeCount++
			}
		}

		if activeCount >= minCount {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (r *NodeRegistry) RunMonitor(ctx context.Context) {
	interval := MonitorIntervalFromEnv()
	if interval <= 0 {
		interval = time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			for _, node := range r.Snapshot() {
				switch node.State {
				case NodeActive:
					logger.LogCluster(fmt.Sprintf("event=node_state node=%q state=ACTIVE", node.NodeID))

				case NodeMissed:
					logger.LogCluster(fmt.Sprintf("event=node_state node=%q state=MISSED missed=%d threshold=%d", node.NodeID, node.MissedBeats, FailureThresholdFromEnv()))

				case NodeDead:
					logger.LogCluster(fmt.Sprintf("event=node_state node=%q state=DEAD", node.NodeID))
				}
			}
		}
	}
}
