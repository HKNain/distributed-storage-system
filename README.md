# Distributed Storage System

A distributed, fault-tolerant object storage system built using **Go, gRPC, HashiCorp Raft, and bbolt**. It splits files into smaller chunks, distributes them across multiple storage nodes, and maintains replicated metadata to support reliable storage and retrieval.

## Features

- **Chunk-based Storage:** Splits files into fixed-size 4 MiB chunks.
- **Data Replication:** Maintains a target replication factor of 3 across distinct storage nodes.
- **Quorum-based Writes:** Acknowledges writes after the primary and one replica successfully persist the chunk; the third replica is handled asynchronously.
- **Raft Consensus:** Replicates file, chunk, and user metadata across multiple master nodes.
- **Multi-Master Architecture:** Supports multiple HTTP master instances with leader-based write forwarding.
- **Failure Detection:** Uses periodic heartbeats to monitor storage-node availability.
- **Data Integrity:** Uses SHA-256 checksums to verify individual chunks and reconstructed files.
- **Crash Recovery:** Uses a Write-Ahead Log (WAL) to track upload operations and recover interrupted uploads.
- **Replica Repair:** Supports restoring missing or corrupted replicas.
- **Authentication:** JWT-based authentication with bcrypt password hashing.

## Architecture

The system consists of two main components:

**1. Master Nodes**
- Handle client requests through REST APIs.
- Manage file and chunk metadata.
- Coordinate storage-node selection and replication.
- Maintain metadata consistency using Raft.
- Monitor storage-node health.

**2. Storage Nodes**
- Store chunks on local disk.
- Expose storage operations through gRPC.
- Handle chunk upload, retrieval, and deletion.
- Send periodic heartbeats to master nodes.

### High-level Design

```text
                 Client
                   |
             REST API (HTTP)
                   |
        +----------+----------+
        |          |          |
     Master 1   Master 2   Master 3
        |          |          |
        +------ Raft Consensus ------+
                   |
            Metadata Replication
                   
        +----------+----------+
        |          |          |
     Storage 1  Storage 2  Storage 3
                   |
               Storage 4
```

## How It Works

**Upload**
1. Client uploads a file through a master node.
2. Requests received by followers are forwarded to the Raft leader.
3. The file is divided into 4 MiB chunks.
4. Each chunk is assigned to a primary storage node and two replicas.
5. The primary and one replica persist the chunk synchronously.
6. The third replica is written asynchronously.
7. Metadata and upload progress are recorded through Raft and WAL.

**Download**
1. Client requests a file using its ID.
2. The master retrieves file and chunk metadata.
3. Chunks are fetched from available storage nodes.
4. SHA-256 checksums verify chunk integrity.
5. Verified chunks are reassembled in their original order.
6. The complete file checksum is verified before returning the file.

## Technology Stack

| Technology | Purpose |
|---|---|
| Go | Core implementation |
| gRPC & Protocol Buffers | Internal node communication |
| HashiCorp Raft | Distributed metadata consensus |
| bbolt | Persistent metadata storage |
| REST API | Client communication |
| JWT & bcrypt | Authentication |
| SHA-256 | Data integrity |
| WAL | Crash recovery |

## Project Structure

```text
distributed-storage-system/
├── api/proto/          # Protobuf service definitions
├── cmd/
│   ├── master/         # Master node entry point
│   └── storage-node/   # Storage node entry point
├── internal/
│   ├── controller/     # HTTP controllers
│   ├── database/       # Database initialization
│   ├── middleware/     # Authentication and leader forwarding
│   ├── models/         # Data models
│   ├── repository/     # Metadata repositories
│   ├── routes/         # API routes
│   ├── rpc/            # gRPC, registry, replication and repair
│   ├── raftcluster/    # Raft consensus and metadata FSM
│   ├── storage/        # Disk-based chunk storage
│   ├── chunker/        # File chunking
│   └── wal/            # Write-Ahead Logging and recovery
├── data/               # Persistent node data
├── logs/               # Application logs
├── go.mod
└── README.md
```

## Running Locally

Clone the repository:

```bash
git clone https://github.com/HKNain/distributed-storage-system.git
cd distributed-storage-system
```

Install dependencies:

```bash
go mod download
```

Configure the required environment variables in `.env`, including master addresses, Raft peers, storage-node addresses, and data directories.

Start the master cluster and storage nodes using the provided launch scripts or their respective Go entry points.

```bash
go run ./cmd/master
```

```bash
go run ./cmd/storage-node
```

## Future Improvements

- Dynamic storage-node discovery and load-aware chunk placement.
- Leadership-aware background workers.
- Replicated upload-session state and cross-master crash recovery.
- Stronger failure handling and automated rebalancing.
- Monitoring, benchmarking, and performance evaluation.

---

**Built to explore distributed systems concepts through a practical, multi-node storage implementation.**