# ReplicatedFS — Replicated Distributed File System

A distributed file system built on top of the [Membership Protocol](../MembershipProtocol) that uses **consistent hashing** for file placement, **3-way replication** for fault tolerance, and a **block-based append-only** storage model inspired by GFS. Supports concurrent appends with **merge operations** to guarantee eventual consistency and **read-my-writes** semantics.

## Architecture

```
              ┌──────────────────────────────┐
              │       Controller CLI         │
              │       (controller.py)        │
              └──────────┬───────────────────┘
                         │ TCP commands (control_port)
         ┌───────────────┼───────────────┐
         ▼               ▼               ▼
   ┌──────────┐    ┌──────────┐    ┌──────────┐
   │  Node 1  │◄──►│  Node 2  │◄──►│  Node 3  │  ...  Node N
   │          │    │          │    │          │
   │ ┌──────┐ │    │ ┌──────┐ │    │ ┌──────┐ │
   │ │Ring  │ │    │ │Ring  │ │    │ │Ring  │ │
   │ │Store │ │    │ │Store │ │    │ │Store │ │
   │ │Repl. │ │    │ │Repl. │ │    │ │Repl. │ │
   │ └──────┘ │    │ └──────┘ │    │ └──────┘ │
   └─────┬────┘    └─────┬────┘    └─────┬────┘
         │               │               │
         └───────┬───────┴───────┬───────┘
                 │               │
          TCP (rfs_port)     UDP (membership_port)
          File operations    Failure detection
```

**Each node runs:**
- **Consistent Hash Ring** — maps files to their replica set
- **Block Storage** — append-only file storage with per-block persistence
- **Replication Manager** — maintains 3 replicas, handles re-replication on failures
- **Consistency Manager** — tracks per-client write ordering, coordinates merges
- **Network Manager** — TCP server for file operation messages
- **Membership Service** — UDP-based failure detection (from MembershipProtocol)

## Concepts Demonstrated

### Consistent Hashing

Files and nodes are mapped onto a SHA-1 hash ring. Each file is stored on its 3 successor nodes in the ring. When nodes join or leave, only files near the affected ring positions are redistributed.

```
           0
           │
    Node-3 ●─────────● Node-1
           │  file.txt│
           │    ↓     │
           │  Replicas: Node-1, Node-4, Node-2
           │         │
    Node-5 ●─────────● Node-4
           │
    Node-2 ●
```

### Block-Based Append Semantics

Files are stored as ordered sequences of blocks. Each `append` creates a new block with:
- **block_id** — unique identifier
- **client_id** — which client wrote this block
- **sequence_num** — monotonically increasing per client
- **timestamp** — wall-clock time of the write
- **data** — the appended bytes

This structure supports concurrent appends from multiple clients while preserving per-client ordering.

### Merge Operation

When replicas diverge after concurrent appends, a `merge` collects blocks from all replicas, deduplicates by `block_id`, and sorts by `(client_id, sequence_num, timestamp)`. The merged result is written back to all replicas, achieving eventual consistency.

```
Replica 1:  [create] [A:0] [B:0] [A:1]
Replica 2:  [create] [B:0] [A:0]
Replica 3:  [create] [A:0] [A:1] [B:0]

After merge (all replicas):
            [create] [A:0] [A:1] [B:0]
            ^^^^^^^^ ^^^^^ ^^^^^ ^^^^^
            initial  client A     client B
                     in order     in order
```

### Read-My-Writes Consistency

A client that appends to a file cannot read stale data — the system tracks pending writes per client and blocks reads until the client's own writes are complete.

### Automatic Re-Replication

A background monitor checks every 5 seconds for under-replicated files. When a node fails, its files are automatically re-replicated to maintain the target of 3 copies.

## File Structure

```
ReplicatedFS/
├── node.py                  Core node — integrates all components
├── main.py                  CLI interface and control server
├── ring.py                  Consistent hashing ring (SHA-1)
├── storage.py               Block-based file storage with disk persistence
├── consistency.py           Write tracking, merge algorithm, conflict detection
├── network.py               TCP message server with handler registration
├── replication.py           Replica monitoring and re-replication
├── utils.py                 Hashing, serialization, logging utilities
├── controller.py            Remote control CLI for multi-node management
├── start_nodes.py           Helper script to launch/stop nodes locally
├── test_replicatedfs.py     Unit tests for ring, storage, and consistency
├── config.json              Node addresses and protocol parameters
└── __init__.py              Package marker
```

## Configuration

`config.json` defines node addresses and protocol parameters:

```json
{
    "nodes": [
        {"id": 1, "ip": "127.0.0.1", "membership_port": 8081, "control_port": 9091, "rfs_port": 7001},
        {"id": 2, "ip": "127.0.0.1", "membership_port": 8082, "control_port": 9092, "rfs_port": 7002},
        ...
    ],
    "introducer_id": 1,
    "replication_factor": 3,
    "gossip_interval": 0.5,
    "failure_timeout": 15.0
}
```

Each node uses three ports:
| Port | Protocol | Purpose |
|------|----------|---------|
| `membership_port` | UDP | Failure detection heartbeats |
| `rfs_port` | TCP | File operations (create, get, append, merge) |
| `control_port` | TCP | Commands from controller CLI |

For distributed deployment, replace `127.0.0.1` with real IPs and assign each node to a separate machine.

## Quick Start

### Using the helper script

```bash
cd ReplicatedFS

# Start 3 nodes in the background
python start_nodes.py --count 3

# Check status
python start_nodes.py --status

# Stop all nodes
python start_nodes.py --stop
```

### Manual setup

Start each node in a separate terminal:

```bash
# Terminal 1 (introducer)
python -m ReplicatedFS.main --node-id 1

# Terminal 2
python -m ReplicatedFS.main --node-id 2

# Terminal 3
python -m ReplicatedFS.main --node-id 3
```

### Using the controller

```bash
python controller.py
```

```
rfs-ctrl> node 1
Target set to: Node-1 (127.0.0.1:9091)

rfs-ctrl> create sample.txt rfs_sample.txt
  Created on localhost

rfs-ctrl> node all
Target set to: all nodes

rfs-ctrl> liststore
[Node-1] Files stored (1): rfs_sample.txt
[Node-2] Files stored (1): rfs_sample.txt
[Node-3] Files stored (1): rfs_sample.txt
```

## Usage Examples

### Interactive mode

```bash
python -m ReplicatedFS.main --node-id 1
```

```
rfs> create mydata.txt rfs_data.txt
  Created on localhost
  Create completed: 3/3 replicas

rfs> append more_data.txt rfs_data.txt
  Appended to localhost
  Append completed: 3/3 replicas

rfs> get rfs_data.txt output.txt
  Retrieved 2048 bytes from localhost
  Saved to output.txt

rfs> ls rfs_data.txt
  File ID: a1b2c3d4e5f6g7h8
  Stored on 3 machines:
    - localhost (Ring: 12345678...)

rfs> liststore
  Files stored (1): rfs_data.txt

rfs> merge rfs_data.txt
  Merge completed in 0.042 seconds

rfs> list_mem_ids
  Total members: 3
  Sorted by Ring Position:
    Ring 12345: node-1 (ACTIVE)
    Ring 45678: node-2 (ACTIVE)
    Ring 78901: node-3 (ACTIVE)
```

## Protocol Details

### Message Types

| Message | Direction | Description |
|---------|-----------|-------------|
| `CREATE` | Client → Replicas | Create file with initial data on all replicas |
| `GET` | Client → Replica | Read file data (checks read-my-writes) |
| `APPEND` | Client → Replicas | Append a new block to all replicas |
| `MERGE` | Client → Coordinator | Coordinator collects, merges, and distributes blocks |
| `CHECK_FILE` | Node → Node | Check if a replica has a file |
| `GET_FILE_BLOCKS` | Node → Node | Fetch all blocks for re-replication or merge |
| `REPLICATE_FILE` | Node → Node | Send file blocks to a new replica |
| `LS` | Client → Node | List which nodes store a file |
| `LISTSTORE` | Client → Node | List all files on a node |
| `LIST_MEM_IDS` | Client → Node | List membership with ring positions |

All messages use JSON over TCP with a 10-byte size header.

## Testing

```bash
# Run unit tests
python -m pytest ReplicatedFS/test_replicatedfs.py -v

# Or with unittest
python -m unittest ReplicatedFS.test_replicatedfs -v
```

Tests cover:
- Consistent hash ring (placement, determinism, minimal disruption)
- Block storage (create, append, persistence, replace)
- Consistency manager (merge deduplication, ordering, conflict detection)
- Merge coordinator (concurrent merge prevention)

## Requirements

- Python 3.6+
- No external dependencies (uses only the standard library)
- Requires the sibling [MembershipProtocol](../MembershipProtocol) directory

## Credits

- Consistent hashing: Karger et al., *Consistent Hashing and Random Trees* (1997)
- Append semantics inspired by the Google File System (Ghemawat et al., 2003)
- SWIM failure detection: Das et al., *SWIM: Scalable Weakly-consistent Infection-style Process Group Membership Protocol* (2002)
- Network programming patterns from [Beej's Guide to Network Programming](https://beej.us/guide/bgnet/)
