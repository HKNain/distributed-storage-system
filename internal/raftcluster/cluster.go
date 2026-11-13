package raftcluster

import (
	"fmt"

	bolt "go.etcd.io/bbolt"
)

type Cluster struct {
	Node     *Node
	FSM      *MetadataFSM
	Metadata *MetadataService
}

func StartCluster(db *bolt.DB, cfg Config) (*Cluster, error) {
	if db == nil {
		return nil, fmt.Errorf("metadata database is required")
	}

	fsm, err := NewMetadataFSM(db)
	if err != nil {
		return nil, fmt.Errorf("initialize metadata FSM: %w", err)
	}

	node, err := Start(cfg, fsm)
	if err != nil {
		return nil, fmt.Errorf("start raft node: %w", err)
	}

	metadata, err := NewMetadataService(node.Raft)
	if err != nil {
		node.Shutdown()
		return nil, fmt.Errorf("initialize metadata service: %w", err)
	}

	return &Cluster{
		Node:     node,
		FSM:      fsm,
		Metadata: metadata,
	}, nil
}

func (c *Cluster) Shutdown() {
	if c != nil && c.Node != nil {
		c.Node.Shutdown()
	}
}
