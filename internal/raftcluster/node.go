package raftcluster

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/HKNain/distributed-storage-system/internal/utils"
	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb/v2"
)

type Peer struct {
	ID      string
	Address string
}

type Config struct {
	NodeID           string
	BindAddress      string
	AdvertiseAddress string
	DataDir          string

	// Only one designated node should have Bootstrap = true.
	Bootstrap bool
	Peers     []Peer
}

type Node struct {
	Raft      *raft.Raft
	transport *raft.NetworkTransport
}

func Start(cfg Config, fsm raft.FSM) (*Node, error) {
	logger.LogRaft(fmt.Sprintf("event=start node=%q bind=%q advertise=%q bootstrap=%t", cfg.NodeID, cfg.BindAddress, cfg.AdvertiseAddress, cfg.Bootstrap))
	if cfg.NodeID == "" {
		return nil, fmt.Errorf("raft node ID is required")
	}

	if cfg.BindAddress == "" || cfg.AdvertiseAddress == "" {
		return nil, fmt.Errorf("raft bind and advertise addresses are required")
	}

	if cfg.DataDir == "" {
		return nil, fmt.Errorf("raft data directory is required")
	}

	if fsm == nil {
		return nil, fmt.Errorf("raft FSM is required")
	}

	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("create raft data directory: %w", err)
	}

	logStore, err := boltdb.NewBoltStore(
		filepath.Join(cfg.DataDir, "raft.db"),
	)
	if err != nil {
		return nil, fmt.Errorf("create raft log store: %w", err)
	}

	stableStore, err := boltdb.NewBoltStore(
		filepath.Join(cfg.DataDir, "stable.db"),
	)
	if err != nil {
		logStore.Close()
		return nil, fmt.Errorf("create raft stable store: %w", err)
	}

	snapshotStore, err := raft.NewFileSnapshotStore(
		cfg.DataDir,
		3,
		os.Stderr,
	)
	if err != nil {
		logStore.Close()
		stableStore.Close()
		return nil, fmt.Errorf("create snapshot store: %w", err)
	}

	hasState, err := raft.HasExistingState(
		logStore,
		stableStore,
		snapshotStore,
	)
	if err != nil {
		logStore.Close()
		stableStore.Close()
		return nil, fmt.Errorf("check existing raft state: %w", err)
	}

	advertiseAddr, err := net.ResolveTCPAddr(
		"tcp",
		cfg.AdvertiseAddress,
	)
	if err != nil {
		logStore.Close()
		stableStore.Close()
		return nil, fmt.Errorf("resolve advertise address: %w", err)
	}

	transport, err := raft.NewTCPTransport(
		cfg.BindAddress,
		advertiseAddr,
		3,
		10*time.Second,
		os.Stderr,
	)
	if err != nil {
		logStore.Close()
		stableStore.Close()
		return nil, fmt.Errorf("create raft transport: %w", err)
	}

	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(cfg.NodeID)

	raftNode, err := raft.NewRaft(
		raftConfig,
		fsm,
		logStore,
		stableStore,
		snapshotStore,
		transport,
	)
	if err != nil {
		transport.Close()
		logStore.Close()
		stableStore.Close()
		return nil, fmt.Errorf("initialize raft: %w", err)
	}

	node := &Node{
		Raft:      raftNode,
		transport: transport,
	}

	if cfg.Bootstrap && !hasState {
		servers := make([]raft.Server, 0, len(cfg.Peers))

		for _, peer := range cfg.Peers {
			servers = append(servers, raft.Server{
				ID:       raft.ServerID(peer.ID),
				Address:  raft.ServerAddress(peer.Address),
				Suffrage: raft.Voter,
			})
		}

		if len(servers) < 3 {
			node.Shutdown()
			return nil, fmt.Errorf("initial raft cluster requires at least 3 peers")
		}

		future := raftNode.BootstrapCluster(raft.Configuration{
			Servers: servers,
		})

		if err := future.Error(); err != nil {
			logger.LogError(fmt.Errorf("bootstrap raft cluster: %w", err))
			node.Shutdown()
			return nil, fmt.Errorf("bootstrap raft cluster: %w", err)
		}
		logger.LogRaft(fmt.Sprintf("event=bootstrap node=%q peers=%d result=success", cfg.NodeID, len(servers)))
	}

	logger.LogRaft(fmt.Sprintf("event=ready node=%q existing_state=%t", cfg.NodeID, hasState))
	return node, nil
}

func (n *Node) IsLeader() bool {
	return n.Raft.State() == raft.Leader
}

func (n *Node) LeaderAddress() string {
	return string(n.Raft.Leader())
}

func (n *Node) Shutdown() {
	if n == nil {
		return
	}

	if n.Raft != nil {
		logger.LogRaft("event=shutdown")
		_ = n.Raft.Shutdown().Error()
	}

	if n.transport != nil {
		_ = n.transport.Close()
	}
}
