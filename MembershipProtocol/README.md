# Membership Protocol

A group membership service implementing two selectable failure detection strategies — **Gossip** (epidemic) and **Ping-Ack** (direct probing) — with an optional suspicion mechanism to reduce false positives. Nodes communicate over UDP and maintain eventually-consistent membership lists using heartbeat counters and incarnation-based versioning.

## Architecture

```
                ┌────────────────────────────┐
                │       Controller CLI       │
                │      (controller.py)       │
                └────────┬───────────────────┘
                         │ TCP commands
         ┌───────────────┼───────────────┐
         ▼               ▼               ▼
   ┌──────────┐    ┌──────────┐    ┌──────────┐
   │  Node 1  │◄──►│  Node 2  │◄──►│  Node 3  │ ...  Node N
   │(introducer)   │          │    │          │
   └──────────┘    └──────────┘    └──────────┘
         ▲               ▲               ▲
         └───────────────┴───────────────┘
              UDP heartbeats / gossip
```

**Nodes (`membership.py`):**
- Maintain a local membership list with heartbeat counters
- Run a failure detection protocol (Gossip or Ping-Ack)
- Detect failed nodes via heartbeat timeouts
- Accept commands from the controller over TCP

**Controller (`controller.py`):**
- Interactive CLI for managing nodes
- Sends commands to individual nodes or all nodes at once
- Can trigger joins, leaves, protocol switches, and status queries

## Concepts Demonstrated

### Failure Detection

The default protocol is **Ping-Ack** with suspicion disabled. Both can be switched at runtime via the controller.

**Gossip Protocol:**
Each node periodically increments its heartbeat counter and sends the full membership list to 1-3 random peers. Receiving nodes merge the remote list. If a node's heartbeat hasn't been updated within `failure_timeout` (15s), it is marked as failed.

```
Node A: heartbeat=42 ──gossip──► Node B
Node B: heartbeat=38 ──gossip──► Node C
Node C: heartbeat=50 ──gossip──► Node A
                   (epidemic convergence)
```

**Ping-Ack Protocol:**
Each node periodically sends a PING to a random active peer and waits for an ACK. If no ACK arrives within `failure_timeout`, the peer is marked as failed.

```
Node A ──PING──► Node B
Node A ◄──ACK── Node B    (healthy)

Node A ──PING──► Node C
Node A    ...    (timeout) → mark Node C FAILED
```

### Suspicion Mechanism

When enabled, suspected nodes get a grace period (`suspicion_timeout` = 10s) before being marked as failed. This reduces false positives from transient network issues or temporary load spikes.

```
ACTIVE ↔ SUSPECTED → FAILED → (removed after cleanup_timeout)
  ↕
LEFT (voluntary)
```

### Incarnation Numbers

Each node tracks an incarnation counter. When a node receives a gossip message claiming it is SUSPECTED or FAILED, it bumps its own incarnation number and marks itself ACTIVE. The higher incarnation propagates through subsequent gossip rounds, overriding the stale failure report. Other nodes only accept membership updates with a higher incarnation (or equal incarnation with a higher heartbeat), so the recovered node's corrected status eventually replaces the false report across the cluster.

### Introducer Pattern

Node 1 acts as the fixed introducer. New nodes send a JOIN to the introducer, which broadcasts the join to all existing members and replies with the full membership list.

## File Structure

```
MembershipProtocol/
├── membership.py         # Core membership service (all protocol logic)
├── controller.py         # Interactive CLI for sending commands to nodes
├── config.json           # Configuration for nodes (ports, timeouts)
├── start_nodes.py        # Helper script to start/stop all nodes
├── test_membership.py    # Unit tests for membership list logic
├── .gitignore            # Excludes logs, PID files, __pycache__
├── logs/                 # Per-node log files (auto-created)
│   ├── machine.1.log
│   └── ...
└── README.md
```

## Configuration

The `config.json` file defines nodes with unique ports for local testing:

```json
{
    "nodes": [
        {"id": 1, "ip": "127.0.0.1", "membership_port": 8081, "control_port": 9091},
        {"id": 2, "ip": "127.0.0.1", "membership_port": 8082, "control_port": 9092},
        ...
    ],
    "introducer_id": 1,
    "gossip_interval": 0.5,
    "pingack_interval": 0.5,
    "failure_timeout": 15.0,
    "suspicion_timeout": 10.0,
    "cleanup_timeout": 10.0
}
```

Each node has two ports:
- `membership_port` — UDP port for peer-to-peer heartbeats
- `control_port` — TCP port for receiving commands from the controller

Timeouts are configurable in the JSON. Add or remove entries from the `nodes` array to change the cluster size.

## Quick Start

### Option 1: Using Helper Scripts

```bash
# 1. Start all nodes (runs in background)
python start_nodes.py

# 2. Open the controller
python controller.py

# 3. In the controller, join nodes to the group:
>>> node 1
>>> join
>>> node all
>>> join

# 4. Check membership
>>> node 1
>>> list_mem

# 5. Stop all nodes when done
python start_nodes.py --stop
```

### Option 2: Manual Setup

Start nodes in separate terminals:

```bash
# Terminal 1 (introducer)
python membership.py --node-id 1

# Terminal 2
python membership.py --node-id 2

# Terminal 3
python membership.py --node-id 3
```

Then use the controller to issue commands:

```bash
# Terminal 4
python controller.py
>>> node all
>>> join
>>> list_mem
```

## Controller Commands

```
node <id>                               Select a target node
node all                                Select all nodes

join                                    Join the membership group
leave                                   Voluntarily leave the group
list_mem                                Show full membership list (JSON)
list_self                               Show this node's member ID
display_protocol                        Show current (protocol, suspicion) mode
display_suspects                        Show nodes currently marked SUSPECTED
switch gossip|ping suspect|nosuspect    Switch failure detection protocol
drop_rate <0.0-1.0>                     Simulate UDP packet loss
metrics                                 Show bytes sent/received and suspicion count
quit                                    Exit controller
```

## Example: Failure Detection Demo

```bash
# Start 3 nodes
python start_nodes.py --count 3

# In the controller:
>>> node all
>>> join
>>> node 1
>>> list_mem          # All 3 nodes should be ACTIVE

# Kill node 3 (Ctrl+C in its terminal, or stop via PID)
# Wait ~15 seconds for failure detection

>>> node 1
>>> list_mem          # Node 3 should now be FAILED or removed
```

## Example: Protocol Switching

```bash
>>> node 1
>>> display_protocol
# (pingack, nosuspect)

>>> node all
>>> switch gossip suspect
# All nodes switch to gossip protocol with suspicion enabled

>>> node 1
>>> display_protocol
# (gossip, suspect)
```

## Example: Simulating Network Partitions

```bash
# Set 50% packet loss on node 2
>>> node 2
>>> drop_rate 0.5

# Watch for SUSPECTED/FAILED events in other nodes' logs

# Reset packet loss
>>> node 2
>>> drop_rate 0.0
```

## Protocol Details

### Message Types (JSON over UDP)

| Message | Direction | Purpose |
|---------|-----------|---------|
| `PING` | Node → Node | Direct health check |
| `ACK` | Node → Node | Response to PING |
| `GOSSIP` | Node → Node | Full membership list propagation |
| `JOIN` | Node → Introducer | Request to join the group |
| `JOIN_REPLY` | Introducer → Node | Full membership list for new joiner |
| `LEAVE` | Node → All | Voluntary departure notification |
| `SWITCH` | Node → All | Protocol/suspicion mode change |

### Timeout Configuration

| Parameter | Default | Purpose |
|-----------|---------|---------|
| `gossip_interval` | 0.5s | Heartbeat send frequency |
| `pingack_interval` | 0.5s | Ping send frequency |
| `failure_timeout` | 15.0s | Time before marking a node SUSPECTED or FAILED |
| `suspicion_timeout` | 10.0s | Grace period before escalating SUSPECTED → FAILED |
| `cleanup_timeout` | 10.0s | Time before removing FAILED/LEFT nodes from the list |

### Thread Architecture

Each node runs four daemon threads:

```
Main Thread (sleep loop, Ctrl+C handler)
├── receive_loop    UDP listener, dispatches to protocol handlers
├── monitor_loop    Periodic cleanup of FAILED/LEFT members
├── control_loop    TCP server for controller commands
└── protocol_loop   Gossip heartbeats or Ping-Ack probes
```

## Distributed Deployment

For deployment across multiple machines, update `config.json` with real IPs and use a single port per node:

```json
{
    "nodes": [
        {"id": 1, "ip": "192.168.1.101", "membership_port": 8080, "control_port": 9090},
        {"id": 2, "ip": "192.168.1.102", "membership_port": 8080, "control_port": 9090}
    ],
    "introducer_id": 1
}
```

Then start one node per machine:
```bash
python membership.py --node-id 1   # On server1
python membership.py --node-id 2   # On server2
```

The controller reads IPs from `config.json`, so it can reach remote nodes without code changes.

## Testing

Unit tests cover the core membership list merge logic, incarnation mechanism, and state transitions:

```bash
python -m pytest test_membership.py -v
# or
python test_membership.py
```

## Requirements

- Python 3.6+
- No external dependencies (uses only standard library)

## Credits

Failure detection design inspired by the [SWIM protocol](https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf) (Das et al., 2002), particularly its suspicion mechanism and protocol-switching concepts. Network programming concepts adapted from [Beej's Guide to Network Programming](https://beej.us/guide/).
