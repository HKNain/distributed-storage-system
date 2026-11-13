package raftcluster

import (
	"fmt"
	"os"
	"strings"

	"github.com/joho/godotenv"
)

func LoadConfig() (Config, error) {
	// Load .env without overwriting existing environment variables.
	if err := godotenv.Load(); err != nil && !os.IsNotExist(err) {
		return Config{}, fmt.Errorf("load .env: %w", err)
	}

	nodeID := strings.TrimSpace(os.Getenv("MASTER_NODE_ID"))
	bindAddress := strings.TrimSpace(os.Getenv("MASTER_RAFT_ADDRESS"))
	advertiseAddress := strings.TrimSpace(os.Getenv("MASTER_RAFT_ADVERTISE_ADDRESS"))
	dataDir := strings.TrimSpace(os.Getenv("MASTER_DATA_DIR"))

	if nodeID == "" || bindAddress == "" ||
		advertiseAddress == "" || dataDir == "" {
		return Config{}, fmt.Errorf("missing required master Raft configuration")
	}

	peerEntries := strings.Split(os.Getenv("MASTER_RAFT_PEERS"), ",")

	var peers []Peer

	for _, entry := range peerEntries {
		parts := strings.SplitN(strings.TrimSpace(entry), "=", 2)
		if len(parts) != 2 ||
			strings.TrimSpace(parts[0]) == "" ||
			strings.TrimSpace(parts[1]) == "" {
			return Config{}, fmt.Errorf("invalid Raft peer entry: %q", entry)
		}

		peers = append(peers, Peer{
			ID:      strings.TrimSpace(parts[0]),
			Address: strings.TrimSpace(parts[1]),
		})
	}

	bootstrap := strings.EqualFold(
		strings.TrimSpace(os.Getenv("MASTER_RAFT_BOOTSTRAP")),
		"true",
	)

	return Config{
		NodeID:           nodeID,
		BindAddress:      bindAddress,
		AdvertiseAddress: advertiseAddress,
		DataDir:           dataDir,
		Bootstrap:        bootstrap,
		Peers:             peers,
	}, nil
}
