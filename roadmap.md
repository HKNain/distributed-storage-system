# Distributed Storage System — 5-Week Backend Roadmap

## Project Overview

Build a Go-based distributed object storage system with:

- Configurable file chunking
- SHA-256 integrity verification
- Three-way chunk replication
- WAL-based crash recovery
- Centralized metadata management
- gRPC + Protocol Buffers communication
- Quorum-based writes (2 of 3 replicas)
- Automatic replica failover
- Raft-based metadata consensus
- Storage node health monitoring

**Duration:** 5 weeks  
**Focus:** Backend and distributed systems  
**Approach:** Learn → Design → Implement → Test

---

## Phase 1 — Foundations, Chunking and Storage Engine (Week 1)

**Goal:** Build a single-node storage engine.

### Tasks

- [ ] Design overall system architecture.
- [ ] Define repository structure.
- [ ] Implement configuration management.
- [ ] Implement configurable file chunking.
- [ ] Generate SHA-256 checksum for each chunk.
- [ ] Generate content-addressed chunk IDs.
- [ ] Implement filesystem-based chunk storage.
- [ ] Implement hash-prefix directory sharding.
- [ ] Implement atomic file writes using temporary files, fsync and rename.
- [ ] Implement chunk retrieval and checksum verification.
- [ ] Implement file reassembly.
- [ ] Implement centralized logging.

**Completion criteria:**
- Upload and store a large file.
- Retrieve and reconstruct the original file.
- Verify the original and reconstructed file hashes.
- Detect manually corrupted chunks.

---

## Phase 2 — WAL and Crash Recovery (Week 2)

**Goal:** Make storage operations durable and recoverable.

### Tasks

- [ ] Design WAL record structure.
- [ ] Implement append-only WAL files.
- [ ] Implement checksummed WAL records.
- [ ] Implement operation IDs and sequence numbers.
- [ ] Log chunk state transitions.
- [ ] Implement durable log writes using fsync.
- [ ] Implement WAL replay during startup.
- [ ] Implement recovery of incomplete operations.
- [ ] Implement idempotent recovery.
- [ ] Implement safe WAL compaction.
- [ ] Handle partially written WAL records.

**Completion criteria:**
- Kill the storage process during an upload.
- Restart the process.
- Recover or discard incomplete operations correctly.
- Verify that committed chunks remain intact.

---

## Phase 3 — Metadata Management and RPC Communication (Week 3)

**Goal:** Introduce multiple storage nodes and centralized metadata.

### Tasks

**Metadata Management**
- [ ] Design metadata schemas for files, chunks and storage nodes.
- [ ] Implement metadata database.
- [ ] Implement file repository.
- [ ] Implement chunk repository.
- [ ] Implement storage node repository.
- [ ] Implement chunk-to-node replica mapping.
- [ ] Implement upload session metadata.
- [ ] Define global chunk lifecycle states.
- [ ] Implement metadata service interfaces.

**RPC Communication**
- [ ] Design Protobuf service contracts.
- [ ] Define ChunkService.
- [ ] Define MetadataService.
- [ ] Generate Go code using protoc.
- [ ] Implement gRPC servers and clients.
- [ ] Implement storage node registration.
- [ ] Implement chunk allocation.
- [ ] Implement chunk location lookup.
- [ ] Implement chunk storage and retrieval RPCs.

**Completion criteria:**
- Start three storage node processes.
- Register nodes with the metadata service.
- Upload chunks through the master.
- Allocate storage locations.
- Retrieve chunk locations through metadata.
- Communicate between nodes using gRPC.

---

## Phase 4 — Replication, Quorum and Automatic Failover (Week 4)

**Goal:** Maintain three copies of chunks and tolerate storage node failures.

### Tasks

- [ ] Implement primary-replica architecture.
- [ ] Implement replication factor of three.
- [ ] Implement failure-domain-aware replica placement.
- [ ] Implement ReplicateChunk RPC.
- [ ] Implement synchronous replication to two replicas.
- [ ] Implement asynchronous replication to the third replica.
- [ ] Implement quorum-based write acknowledgement.
- [ ] Implement committed replica tracking.
- [ ] Implement reads from any healthy COMMITTED replica.
- [ ] Implement checksum verification during reads.
- [ ] Implement automatic replica fallback.
- [ ] Implement node heartbeats.
- [ ] Implement node health tracking.
- [ ] Implement under-replicated chunk tracking.
- [ ] Implement basic automatic re-replication.

**Completion criteria:**
- Upload a file and verify three replicas.
- Stop one storage node and verify downloads still work.
- Corrupt a replica and verify fallback.
- Restart a failed node.
- Detect and restore under-replicated chunks.

---

## Phase 5 — Raft Consensus and Metadata Failover (Week 5)

**Goal:** Remove metadata management as a single point of failure.

### Tasks

- [ ] Understand Raft consensus and leader election.
- [ ] Integrate HashiCorp Raft.
- [ ] Implement a three-node metadata cluster.
- [ ] Implement Raft-backed metadata state machine.
- [ ] Route metadata writes through the leader.
- [ ] Implement follower redirection or leader discovery.
- [ ] Implement metadata log replication.
- [ ] Implement commit index handling.
- [ ] Implement snapshotting.
- [ ] Implement metadata recovery.
- [ ] Implement leader failure detection and election.
- [ ] Implement retries after leader changes.

**Completion criteria:**
- Start three metadata nodes.
- Verify leader election.
- Perform metadata operations.
- Kill the current leader.
- Verify a new leader is elected.
- Continue metadata operations.
- Restart the failed leader.
- Verify metadata consistency.

---

## Future Enhancements (Out of Scope)

- Garbage collection and orphan cleanup.
- Soft delete and restore.
- Background integrity scrubbing.
- Dynamic rebalancing.
- Deduplication.
- Erasure coding.
- Advanced node membership management.
- mTLS.
- Automated chaos testing.

---

## Final Project Verification

- [ ] Large file upload and download.
- [ ] Chunking and file reassembly.
- [ ] SHA-256 integrity verification.
- [ ] WAL crash recovery.
- [ ] Three-way replication.
- [ ] Quorum-based writes.
- [ ] Replica fallback.
- [ ] Storage node health monitoring.
- [ ] Automatic replica repair.
- [ ] Raft leader election.
- [ ] Metadata consistency after restart.
- [ ] Logging of important state transitions.

**Development principle:** Understand the concept, design the component, implement it, test failure scenarios and document architectural decisions before moving to the next phase.