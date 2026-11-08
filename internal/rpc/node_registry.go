package rpc

import (
	"context"
	"log"
	"sort"
	"strings"
	"sync"
	"time"
)

type NodeState string

const (
	NodeActive  NodeState = "ACTIVE"
	NodeMissed  NodeState = "MISSED"
	NodeDead    NodeState = "DEAD"
)

const (
	HeartbeatInterval = 10 * time.Second
	FailureThreshold  = 3
	MonitorInterval   = 60 * time.Second
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

// RecordHeartbeat registers a node or refreshes its heartbeat.
func (r *NodeRegistry) RecordHeartbeat(nodeID, address string) bool {
	nodeID = strings.TrimSpace(nodeID)
	address = strings.TrimSpace(address)

	if nodeID == "" || address == "" {
		return false
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	node, exists := r.nodes[nodeID]

	if !exists {
		node = &StorageNodeInfo{
			NodeID: nodeID,
		}
		r.nodes[nodeID] = node
	}

	node.Address = address
	node.LastHeartbeat = time.Now()
	node.State = NodeActive
	node.MissedBeats = 0

	return true
}

// Snapshot returns the current health of all registered nodes.
func (r *NodeRegistry) Snapshot() []StorageNodeInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	now := time.Now()
	result := make([]StorageNodeInfo, 0, len(r.nodes))

	for _, node := range r.nodes {
		info := *node

		missed := int(now.Sub(info.LastHeartbeat) / HeartbeatInterval)

		if missed < 0 {
			missed = 0
		}

		info.MissedBeats = missed

		switch {
		case missed >= FailureThreshold:
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

// RunMonitor periodically prints the health of registered nodes.
func (r *NodeRegistry) RunMonitor(ctx context.Context) {
	ticker := time.NewTicker(MonitorInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			nodes := r.Snapshot()

			for _, node := range nodes {
				switch node.State {
				case NodeActive:
					log.Printf("Storage Node %s: ACTIVE", node.NodeID)

				case NodeMissed:
					log.Printf(
						"Storage Node %s: MISSED (%d/%d)",
						node.NodeID,
						node.MissedBeats,
						FailureThreshold,
					)

				case NodeDead:
					log.Printf("Storage Node %s: DEAD", node.NodeID)
				}
			}
		}
	}
}
