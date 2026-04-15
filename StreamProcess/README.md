# StreamProcess — Distributed Stream Processing

A distributed stream processing framework built on top of [ReplicatedFS](../ReplicatedFS) and the [Membership Protocol](../MembershipProtocol). Implements **exactly-once semantics**, **fault tolerance** with automatic task recovery, and **autoscaling** based on processing rates. Operators are simple stdin/stdout programs chained into multi-stage pipelines.

## Architecture

```
              ┌──────────────────────────────┐
              │          CLI (cli.py)        │
              └──────────┬───────────────────┘
                         │ TCP commands
                         ▼
              ┌──────────────────────────────┐
              │     Leader (Node 1)          │
              │  - Job scheduling            │
              │  - Source partitioning        │
              │  - Task monitoring            │
              │  - Autoscaling               │
              │  - Output collection          │
              └──────┬───────────┬───────────┘
                     │           │
         ┌───────────┘           └───────────┐
         ▼                                   ▼
   ┌──────────────┐                   ┌──────────────┐
   │  Worker (N2) │                   │  Worker (N3) │  ...
   │              │                   │              │
   │ ┌──────────┐ │                   │ ┌──────────┐ │
   │ │ Task 0:0 │─┼───── tuples ────►│ │ Task 1:0 │ │
   │ │(filter)  │ │                   │ │(count)   │ │
   │ └──────────┘ │                   │ └──────────┘ │
   │ ┌──────────┐ │                   │ ┌──────────┐ │
   │ │ Task 0:1 │─┼───── tuples ────►│ │ Task 1:1 │ │
   │ │(filter)  │ │                   │ │(count)   │ │
   │ └──────────┘ │                   │ └──────────┘ │
   └──────────────┘                   └──────────────┘
```

**Leader** (Node 1): Schedules tasks across workers, reads source data from ReplicatedFS, partitions input to stage 0, monitors rates and failures, triggers autoscaling, collects final output.

**Workers** (Node 2+): Manage local task processes, proxy ReplicatedFS writes for exactly-once persistence.

**Tasks**: Individual processes running an operator. Receive tuples via TCP, process through the operator (stdin/stdout), and forward results to the next stage.

## Concepts Demonstrated

### Exactly-Once Semantics

Each tuple carries a unique `tuple_id`. Tasks track processed IDs and output IDs to prevent duplicates:

```
Tuple arrives → Check if duplicate (ACK + skip)
             → Mark as started
             → Run operator
             → Forward outputs with deterministic output IDs
             → Wait for ACKs from next stage
             → Mark as processed
             → Persist state to ReplicatedFS for crash recovery
```

### Fault Tolerance

- **Failure detection**: Leader monitors task PIDs every 10 seconds
- **Automatic restart**: Failed tasks are restarted (on original or any available node)
- **State recovery**: Task ID is preserved, so the restarted task loads its exactly-once log from ReplicatedFS
- **Pending retry**: Unacknowledged outputs are retried with exponential backoff

### Autoscaling

The leader monitors per-stage processing rates and adjusts task count:
- **Scale up**: When rate exceeds high watermark, add a task to that stage
- **Scale down**: When rate drops below low watermark, drain and remove a task
- Cooldown period prevents oscillation

### Operator Interface

Operators are simple programs that read `key\tvalue` from stdin and write `key\tvalue` to stdout:

```
stdin:  data:0    field1,field2,field3,field4
stdout: field3    field1,field2,field3
```

## File Structure

```
StreamProcess/
├── streamprocess.py     Leader/worker node — job scheduling, monitoring, autoscaling
├── task.py              Task process — tuple processing, exactly-once, rate reporting
├── cli.py               Management CLI — list tasks, kill tasks, view output
├── ops.py               All operators in one file (filter, count, transform, identity)
├── filter_op.py         Standalone filter operator
├── count_op.py          Standalone aggregate-by-key count operator
├── transform_op.py      Standalone CSV field extraction operator
├── identity_op.py       Standalone pass-through operator
├── cleanup.sh           Cleanup script for logs, temp files, storage between runs
├── config.json          Node addresses and port assignments
├── .gitignore           Excludes logs, temp files, storage
└── __init__.py          Package marker
```

## Configuration

`config.json` extends the ReplicatedFS config with StreamProcess-specific ports:

```json
{
    "nodes": [
        {"id": 1, "ip": "127.0.0.1", "membership_port": 8081, "control_port": 9091, "rfs_port": 7001, "stream_port": 8001, "task_base_port": 8101},
        ...
    ],
    "leader_id": 1,
    "max_tasks_per_stage": 10
}
```

Each node uses five ports:
| Port | Protocol | Purpose |
|------|----------|---------|
| `membership_port` | UDP | Failure detection heartbeats |
| `rfs_port` | TCP | ReplicatedFS file operations |
| `control_port` | TCP | ReplicatedFS controller commands |
| `stream_port` | TCP | Leader/worker coordination |
| `task_base_port` | TCP | Task-to-task tuple forwarding (base + stage*10 + task_idx) |

## Quick Start

### 1. Start the leader (Node 1)

```bash
python3 streamprocess.py --vm-id 1 --leader
```

### 2. Start workers (Node 2+)

```bash
python3 streamprocess.py --vm-id 2
python3 streamprocess.py --vm-id 3
```

### 3. Submit a job

```bash
python3 streamprocess.py --submit \
    --nstages 2 \
    --ntasks 3 \
    --op1 ./filter_op.py --args1 "Pattern" \
    --op2 ./count_op.py --args2 "" \
    --src input.csv \
    --dest output.txt
```

### 4. Monitor and manage

```bash
# List running tasks
python3 cli.py list_tasks

# View output
python3 cli.py cat_output output.txt

# Download output locally
python3 cli.py get_output output.txt local_output.txt

# Kill a task
python3 cli.py kill_task 2 12345
```

### 5. Clean up between runs

```bash
bash cleanup.sh
```

## Built-in Operators

| Operator | Usage | Description |
|----------|-------|-------------|
| `filter_op.py` | `./filter_op.py <pattern> [col]` | Keep lines matching pattern; optionally rekey by column |
| `count_op.py` | `./count_op.py <task_id>` | Count occurrences per key (stateful) |
| `transform_op.py` | `./transform_op.py` | Extract first 3 CSV fields |
| `identity_op.py` | `./identity_op.py` | Pass-through (for testing) |

### Writing custom operators

Any program that reads `key\tvalue` lines from stdin and writes `key\tvalue` lines to stdout:

```python
#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    parts = line.split('\t', 1)
    key, value = (parts[0], parts[1]) if len(parts) == 2 else ("", line)

    # Your logic here
    if some_condition(value):
        print(f"{key}\t{transform(value)}")
```

## Protocol Details

### Leader/Worker Messages (stream_port)

| Message | Direction | Description |
|---------|-----------|-------------|
| `SUBMIT_JOB` | CLI → Leader | Submit a new processing job |
| `START_TASK` | Leader → Worker | Start a task process on a worker |
| `KILL_PROCESS` | Leader → Worker | Kill a task process |
| `TASK_STARTED` | Worker → Leader | Notify task is running |
| `TASK_COMPLETED` | Worker → Leader | Notify task finished |
| `RATE_UPDATE` | Task → Leader | Report processing rate |
| `LIST_TASKS` | CLI → Leader | Query running tasks |
| `WRITE_RFS` | Task → Worker | Write output to ReplicatedFS |
| `WRITE_RFS_EO` | Task → Worker | Write exactly-once log to ReplicatedFS |
| `READ_RFS_EO` | Task → Worker | Read exactly-once log for recovery |

### Task-to-Task Messages (task ports)

| Message | Direction | Description |
|---------|-----------|-------------|
| `TUPLE` | Task → Task | Forward a data tuple |
| `ACK` | Task → Task | Acknowledge tuple receipt |
| `EOF` | Source → Task | Signal end of input |
| `UPDATE_SUCCESSORS` | Leader → Task | Update downstream task list (autoscaling) |

## Requirements

- Python 3.6+
- No external dependencies (uses only the standard library)
- Requires sibling [ReplicatedFS](../ReplicatedFS) and [MembershipProtocol](../MembershipProtocol) directories

## Credits

- Stream processing model inspired by Apache Storm (Toshniwal et al., 2014)
- Exactly-once semantics based on tuple ID deduplication with persistent tracking
- SWIM failure detection: Das et al., *SWIM: Scalable Weakly-consistent Infection-style Process Group Membership Protocol* (2002)
