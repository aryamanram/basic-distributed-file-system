#!/usr/bin/env python3
"""
StreamProcess Task - Individual task process that runs an operator
Handles tuple processing, exactly-once semantics, and rate reporting

FIXED VERSION with proper exactly-once semantics:
1. ACK duplicates (don't just drop them)
2. Track output tuples with their data for retry after restart
3. ACK only after processing + forwarding succeeds
4. Proper ReplicatedFS sync (only append new entries)
"""
import argparse
import json
import os
import socket
import subprocess
import sys
import threading
import time
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple

# Add parent directory to path for sibling project imports
_parent_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')
sys.path.insert(0, _parent_dir)


def _load_config() -> dict:
    """Load configuration from config.json."""
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.json')
    with open(config_path, 'r') as f:
        return json.load(f)

_CONFIG = _load_config()

# Build per-node port lookups
_NODE_STREAM_PORTS = {n['id']: n.get('stream_port', 8001) for n in _CONFIG['nodes']}
_NODE_TASK_BASE_PORTS = {n['id']: n.get('task_base_port', 8101) for n in _CONFIG['nodes']}


def get_stream_port(node_id: int) -> int:
    """Get the stream processing port for a given node."""
    return _NODE_STREAM_PORTS.get(node_id, 8001)


def get_task_base_port(node_id: int) -> int:
    """Get the task base port for a given node."""
    return _NODE_TASK_BASE_PORTS.get(node_id, 8101)


class ReplicatedFSClientProxy:
    """
    Lightweight ReplicatedFS client proxy for tasks.

    Instead of creating a full ReplicatedFSNode (which would bind port 7000 and conflict
    with the worker/leader's ReplicatedFS node), this proxy sends requests to the local
    worker's WRITE_RFS_EO endpoint, which handles the actual ReplicatedFS operations.

    This enables exactly-once semantics to persist EO logs to ReplicatedFS for recovery
    after task failures, even when tasks restart on different nodes.
    """

    def __init__(self, task_id: str, logger, vm_id: int = 1):
        self.task_id = task_id
        self.logger = logger
        self.vm_id = vm_id
        self.stream_port = get_stream_port(vm_id)
        self.client_id = f"task_{task_id}_{os.getpid()}_{time.time()}"

    def create(self, local_filename: str, rfs_filename: str) -> bool:
        """Create a file in ReplicatedFS via the local worker."""
        try:
            with open(local_filename, 'r') as f:
                data = f.read()
            return self._send_rfs_request('create', rfs_filename, data)
        except Exception as e:
            self.logger.log(f"ReplicatedFS PROXY CREATE ERROR: {e}")
            return False

    def append(self, local_filename: str, rfs_filename: str) -> bool:
        """Append to a file in ReplicatedFS via the local worker."""
        try:
            with open(local_filename, 'r') as f:
                data = f.read()
            return self._send_rfs_request('append', rfs_filename, data)
        except Exception as e:
            self.logger.log(f"ReplicatedFS PROXY APPEND ERROR: {e}")
            return False

    def get(self, rfs_filename: str, local_filename: str) -> bool:
        """Get a file from ReplicatedFS via the local worker."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(30.0)
            sock.connect(('localhost', self.stream_port))

            msg = {
                'type': 'READ_RFS_EO',
                'filename': rfs_filename,
                'task_id': self.task_id
            }

            sock.sendall(json.dumps(msg).encode('utf-8'))

            # Read response - may be large
            chunks = []
            while True:
                try:
                    chunk = sock.recv(65536)
                    if not chunk:
                        break
                    chunks.append(chunk)
                    # Try to parse - if successful, we have complete response
                    try:
                        full_response = b''.join(chunks).decode('utf-8')
                        result = json.loads(full_response)
                        break
                    except json.JSONDecodeError:
                        continue
                except socket.timeout:
                    break

            sock.close()

            if not chunks:
                return False

            full_response = b''.join(chunks).decode('utf-8')
            result = json.loads(full_response)

            if result.get('status') == 'success':
                data = result.get('data', '')
                with open(local_filename, 'w') as f:
                    f.write(data)
                return True
            return False

        except Exception as e:
            self.logger.log(f"ReplicatedFS PROXY GET ERROR: {e}")
            return False

    def _send_rfs_request(self, operation: str, rfs_filename: str, data: str) -> bool:
        """Send a ReplicatedFS write request to the local worker."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(30.0)
            sock.connect(('localhost', self.stream_port))

            msg = {
                'type': 'WRITE_RFS_EO',
                'filename': rfs_filename,
                'data': data,
                'operation': operation,
                'task_id': self.task_id
            }

            sock.sendall(json.dumps(msg).encode('utf-8'))
            response = sock.recv(65536).decode('utf-8')
            sock.close()

            result = json.loads(response)
            return result.get('status') == 'success'

        except Exception as e:
            self.logger.log(f"ReplicatedFS PROXY {operation.upper()} ERROR: {e}")
            return False


def get_timestamp() -> str:
    """Get formatted timestamp."""
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]


class TaskLogger:
    """Logger for task operations."""
    
    def __init__(self, task_id: str, run_id: str, vm_id: int):
        self.task_id = task_id
        self.log_file = f"task_{task_id}_{run_id}.log"
        self.vm_id = vm_id
        self.lock = threading.Lock()
    
    def log(self, msg: str):
        """Log a message with timestamp."""
        line = f"[{get_timestamp()}] [{self.task_id}] {msg}"
        with self.lock:
            print(line)
            with open(self.log_file, 'a') as f:
                f.write(line + "\n")


class ExactlyOnceTracker:
    """
    Tracks processed tuples for exactly-once semantics.
    Uses ReplicatedFS for persistence to survive task failures.

    FIXED:
    - Stores output tuple DATA for retry after restart
    - Only appends NEW entries to ReplicatedFS (not entire file)
    - Properly separates local buffer from synced state
    - Tracks output sequence numbers for deterministic output IDs
    """

    def __init__(self, task_id: str, rfs_client, logger: TaskLogger):
        self.task_id = task_id
        self.rfs_client = rfs_client
        self.logger = logger

        # Processed input tuples (by tuple_id) - fully completed
        self.processed_ids: Set[str] = set()

        # Started input tuples (by tuple_id) - started but may not be complete
        self.started_ids: Set[str] = set()

        # Acked output tuples (by output_id) - these were confirmed by next stage
        self.acked_outputs: Set[str] = set()

        # FIXED: Pending output tuples with their DATA for retry
        # output_id -> {tuple_msg, target}
        self.pending_outputs: Dict[str, dict] = {}

        # FIXED: Track max output sequence number per input tuple for recovery
        # This ensures deterministic output IDs after restart
        self.output_seq_max: Dict[str, int] = {}

        self.lock = threading.Lock()

        # Local log file (write buffer)
        task_dir = os.path.dirname(os.path.abspath(__file__))
        self.local_log_file = os.path.join(task_dir, f"eo_log_{task_id}.log")

        # ReplicatedFS log file name (persistent storage)
        self.rfs_log_file = f"eo_log_{task_id}.log"

        # Track entries that need to be synced to ReplicatedFS
        self.pending_sync_entries: List[str] = []
        self.entries_since_rfs_sync = 0
        # Sync periodically for crash resilience (balance between safety and performance)
        # Higher threshold = better performance, but more data loss on crash
        self.rfs_sync_threshold = 50

        # Load existing state (for recovery after failure)
        self._load_state()

    def _load_state(self):
        """Load state from ReplicatedFS log file (for recovery after failure)."""
        loaded_from_rfs = False

        def parse_output_id_seq(output_id: str) -> Tuple[Optional[str], int]:
            """
            Extract input tuple_id and sequence number from output_id.
            Format: {input_tuple_id}_{task_id}_out{seq_num}
            """
            if '_out' in output_id:
                parts = output_id.rsplit('_out', 1)
                if len(parts) == 2:
                    try:
                        seq = int(parts[1])
                        # parts[0] is {input_tuple_id}_{task_id}, extract input_tuple_id
                        prefix = parts[0]
                        # Find last underscore before task_id
                        if f'_{self.task_id}' in prefix:
                            input_tid = prefix.rsplit(f'_{self.task_id}', 1)[0]
                            return input_tid, seq
                    except ValueError:
                        pass
            return None, -1

        def process_line(line: str):
            """Process a single log line."""
            line = line.strip()
            if line.startswith('PROCESSED:'):
                tuple_id = line[10:]
                self.processed_ids.add(tuple_id)
                self.started_ids.add(tuple_id)  # Processed implies started
            elif line.startswith('STARTED:'):
                tuple_id = line[8:]
                self.started_ids.add(tuple_id)
            elif line.startswith('ACK:'):
                output_id = line[4:]
                self.acked_outputs.add(output_id)
                # FIXED: Track max sequence number for deterministic output IDs
                input_tid, seq = parse_output_id_seq(output_id)
                if input_tid and seq >= 0:
                    current_max = self.output_seq_max.get(input_tid, -1)
                    self.output_seq_max[input_tid] = max(current_max, seq)
            elif line.startswith('PENDING:'):
                # FIXED: Load pending output data for retry
                # Format: PENDING:output_id:json_data
                parts = line[8:].split(':', 1)
                if len(parts) == 2:
                    output_id, json_data = parts
                    try:
                        data = json.loads(json_data)
                        # Only add if not already acked
                        if output_id not in self.acked_outputs:
                            self.pending_outputs[output_id] = data
                        # Track sequence number
                        input_tid, seq = parse_output_id_seq(output_id)
                        if input_tid and seq >= 0:
                            current_max = self.output_seq_max.get(input_tid, -1)
                            self.output_seq_max[input_tid] = max(current_max, seq)
                    except Exception:
                        pass

        # Try to load from ReplicatedFS first (for recovery)
        if self.rfs_client:
            try:
                temp_file = f"/tmp/eo_recovery_{self.task_id}.log"
                self.rfs_client.get(self.rfs_log_file, temp_file)

                if os.path.exists(temp_file):
                    with open(temp_file, 'r') as f:
                        for line in f:
                            process_line(line)

                    self.logger.log(f"EO: Recovered from ReplicatedFS - {len(self.processed_ids)} processed, "
                                  f"{len(self.acked_outputs)} acked, {len(self.pending_outputs)} pending")
                    loaded_from_rfs = True

                    # Copy ReplicatedFS state to local file for continued operation
                    import shutil
                    shutil.copy(temp_file, self.local_log_file)
                    os.remove(temp_file)
            except Exception as e:
                self.logger.log(f"EO: No ReplicatedFS log found (new task or first run): {e}")

        # Fall back to local file if ReplicatedFS load failed
        if not loaded_from_rfs:
            try:
                if os.path.exists(self.local_log_file):
                    with open(self.local_log_file, 'r') as f:
                        for line in f:
                            process_line(line)

                    self.logger.log(f"EO: Loaded from local - {len(self.processed_ids)} processed, "
                                  f"{len(self.acked_outputs)} acked, {len(self.pending_outputs)} pending")
            except Exception as e:
                self.logger.log(f"EO: No existing log (new task): {e}")

    def is_duplicate(self, tuple_id: str) -> bool:
        """Check if a tuple has already been fully processed."""
        with self.lock:
            return tuple_id in self.processed_ids

    def was_started(self, tuple_id: str) -> bool:
        """Check if a tuple was started but not completed (partial processing before crash)."""
        with self.lock:
            return tuple_id in self.started_ids and tuple_id not in self.processed_ids

    def mark_started(self, tuple_id: str):
        """Mark a tuple as started processing."""
        with self.lock:
            self.started_ids.add(tuple_id)
            entry = f"STARTED:{tuple_id}"
            self._append_entry(entry)

    def check_and_mark_in_progress(self, tuple_id: str) -> bool:
        """
        Atomically check if tuple is duplicate and mark as processed if not.
        Returns True if this is a NEW tuple (not duplicate), False if duplicate.
        Also persists to log for recovery after crash.
        """
        with self.lock:
            if tuple_id in self.processed_ids:
                return False  # Duplicate
            self.processed_ids.add(tuple_id)
            # Persist to log for crash recovery
            entry = f"PROCESSED:{tuple_id}"
            self._append_entry(entry)
            return True  # New tuple, now marked as processed

    def unmark_in_progress(self, tuple_id: str):
        """Remove tuple from in-progress set (used when forwarding fails)."""
        with self.lock:
            self.processed_ids.discard(tuple_id)

    def check_and_mark_output(self, output_id: str) -> bool:
        """
        Atomically check if output was already written and mark if not.
        Returns True if this is a NEW output (not duplicate), False if duplicate.
        """
        with self.lock:
            if output_id in self.acked_outputs:
                return False  # Duplicate
            self.acked_outputs.add(output_id)
            # Also persist to log
            entry = f"ACK:{output_id}"
            self._append_entry(entry)
            return True  # New output

    def get_next_output_seq(self, input_tuple_id: str) -> int:
        """
        Get the next output sequence number for an input tuple.
        Used after recovery to continue from where we left off.
        """
        with self.lock:
            # Return max_seen + 1, or 0 if never seen
            return self.output_seq_max.get(input_tuple_id, -1) + 1

    def mark_processed(self, tuple_id: str):
        """Mark a tuple as processed."""
        with self.lock:
            self.processed_ids.add(tuple_id)
            entry = f"PROCESSED:{tuple_id}"
            self._append_entry(entry)

    def add_pending_output(self, output_id: str, tuple_msg: dict, target: dict):
        """
        FIXED: Record a pending output tuple with its data.
        This allows retry after task restart.
        """
        with self.lock:
            self.pending_outputs[output_id] = {
                'tuple': tuple_msg,
                'target': target,
                'time': time.time(),
                'retries': 0
            }
            # Persist to log for recovery
            json_data = json.dumps({'tuple': tuple_msg, 'target': target})
            entry = f"PENDING:{output_id}:{json_data}"
            self._append_entry(entry)

    def mark_output_acked(self, output_id: str):
        """Mark an output as acknowledged by next stage."""
        with self.lock:
            self.acked_outputs.add(output_id)
            if output_id in self.pending_outputs:
                del self.pending_outputs[output_id]
            entry = f"ACK:{output_id}"
            self._append_entry(entry)

    def is_output_acked(self, output_id: str) -> bool:
        """Check if an output has been acknowledged."""
        with self.lock:
            return output_id in self.acked_outputs

    def get_pending_outputs(self) -> List[Tuple[str, dict]]:
        """
        FIXED: Get list of pending outputs that need retry.
        Returns [(output_id, {tuple, target, time, retries}), ...]
        """
        with self.lock:
            return list(self.pending_outputs.items())

    def increment_retry(self, output_id: str):
        """Increment retry count for a pending output."""
        with self.lock:
            if output_id in self.pending_outputs:
                self.pending_outputs[output_id]['retries'] += 1
                self.pending_outputs[output_id]['time'] = time.time()

    def _append_entry(self, entry: str):
        """Append an entry to the local log and track for ReplicatedFS sync."""
        # Append to local file immediately with durable write
        try:
            with open(self.local_log_file, 'a') as f:
                f.write(entry + "\n")
                f.flush()  # Force write to OS buffer
                os.fsync(f.fileno())  # Force OS buffer to disk
        except Exception as e:
            self.logger.log(f"EO: Local write error: {e}")
        
        # Track for ReplicatedFS sync
        self.pending_sync_entries.append(entry)
        self.entries_since_rfs_sync += 1
        
        # Sync to ReplicatedFS periodically
        if self.entries_since_rfs_sync >= self.rfs_sync_threshold:
            self._sync_to_rfs()

    def _sync_to_rfs(self):
        """
        FIXED: Sync only NEW entries to ReplicatedFS (append-only).
        Previous version was appending the entire local file, causing duplicates.

        This method now works with ReplicatedFSClientProxy which sends requests to the
        local worker/leader for actual ReplicatedFS operations. This enables cross-node
        recovery when tasks restart on different machines.
        """
        if not self.rfs_client or not self.pending_sync_entries:
            return

        try:
            # Write only new entries to a temp file
            temp_sync_file = f"/tmp/eo_sync_{self.task_id}_{time.time()}.log"
            with open(temp_sync_file, 'w') as f:
                for entry in self.pending_sync_entries:
                    f.write(entry + "\n")

            # Try append first (file might already exist)
            success = self.rfs_client.append(temp_sync_file, self.rfs_log_file)
            if not success:
                # If append fails (file doesn't exist), create it
                success = self.rfs_client.create(temp_sync_file, self.rfs_log_file)

            if success:
                # Clear pending entries after successful sync
                synced_count = len(self.pending_sync_entries)
                self.pending_sync_entries.clear()
                self.entries_since_rfs_sync = 0
                self.logger.log(f"EO: Synced {synced_count} entries to ReplicatedFS")
            else:
                self.logger.log(f"EO: ReplicatedFS sync failed (will retry)")

            # Cleanup temp file
            if os.path.exists(temp_sync_file):
                os.remove(temp_sync_file)
        except Exception as e:
            self.logger.log(f"EO: ReplicatedFS sync failed: {e}")

    def flush(self):
        """Force flush pending entries to ReplicatedFS."""
        with self.lock:
            self._sync_to_rfs()


class StreamProcessTask:
    """
    Individual StreamProcess task process.
    Receives tuples, processes them with operator, and forwards to next stage.

    FIXED for exactly-once:
    - ACKs duplicates (spec requirement)
    - Tracks output tuples with data for retry after restart
    - Proper ACK timing (after processing succeeds)
    - DETERMINISTIC output IDs (critical for duplicate detection across retries)
    """

    def __init__(self, task_id: str, stage: int, task_idx: int, port: int,
                 op_exe: str, op_args: str, leader_host: str, run_id: str,
                 vm_id: int, exactly_once: bool):
        self.task_id = task_id
        self.stage = stage
        self.task_idx = task_idx
        self.port = port
        self.op_exe = op_exe
        self.op_args = op_args
        self.leader_host = leader_host
        self.run_id = run_id
        self.vm_id = vm_id
        self.exactly_once = exactly_once
        self.stream_port = get_stream_port(vm_id)

        self.logger = TaskLogger(task_id, run_id, vm_id)

        self.running = False
        self.server_socket: Optional[socket.socket] = None

        # Exactly-once tracker
        self.eo_tracker: Optional[ExactlyOnceTracker] = None

        # Task configuration from leader
        self.config: Optional[dict] = None
        self.successor_tasks: List[dict] = []
        self.successor_lock = threading.Lock()
        self.rfs_dest: str = ''
        self.num_stages: int = 0

        # Rate tracking
        self.tuples_processed = 0
        self.last_rate_time = time.time()
        self.last_tuple_count = 0

        # EOF tracking
        self.eof_received = 0
        self.expected_eof = 0

        # FIXED: Output sequence counter for deterministic output IDs
        # Maps input tuple_id -> next output sequence number
        self.output_seq_counter: Dict[str, int] = {}

        # Lock for processing tuples (prevents race conditions in exactly-once)
        self.processing_lock = threading.Lock()
        # Track tuples currently being processed (for concurrent request handling)
        self.in_progress_tuples: Set[str] = set()

        # ReplicatedFS client for output writing (initialized in start() for final stage)
        self.rfs_client = None
        self.output_write_lock = threading.Lock()

        # Batched output writing for performance
        self.output_buffer: List[Tuple[str, str]] = []  # [(line, output_id), ...]
        self.output_buffer_lock = threading.Lock()
        self.output_buffer_size = 200  # Flush after this many tuples (increased for better throughput)
        self.last_flush_time = time.time()
        self.flush_interval = 5.0  # Flush at least every 5 seconds (increased for better throughput)
    
    def start(self):
        """Start the task."""
        self.running = True

        self.logger.log(f"TASK START: stage={self.stage} idx={self.task_idx} "
                       f"op={self.op_exe} args={self.op_args}")

        # CRITICAL: Bind socket FIRST so we're ready to receive connections ASAP
        # This prevents timeout waiting for tasks to be ready
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(('', self.port))
        self.server_socket.listen(20)
        self.logger.log(f"Socket bound to port {self.port}")

        # Get configuration from leader (need to know if we're final stage)
        self._get_config_from_leader()

        # Create ReplicatedFS client proxy for exactly-once log syncing
        # This proxy sends requests to the local worker/leader which has the actual ReplicatedFS node
        # This enables cross-node recovery: when a task restarts on a different node,
        # it can load its EO log from ReplicatedFS (not just local file)
        self.rfs_node = None
        if self.exactly_once:
            self.rfs_client = ReplicatedFSClientProxy(self.task_id, self.logger, self.vm_id)
            self.logger.log(f"Created ReplicatedFS proxy client for exactly-once semantics")
        else:
            self.rfs_client = None

        # Initialize exactly-once tracker
        if self.exactly_once:
            self.eo_tracker = ExactlyOnceTracker(self.task_id, self.rfs_client, self.logger)
            # Retry pending outputs on startup for crash recovery
            # (task_id is preserved on restart, so pending outputs are still valid)
            self._retry_pending_from_recovery()

        self.logger.log(f"Task initialized (exactly_once={self.exactly_once})")

        # Notify leader
        self._notify_leader_started()
        
        # Start rate reporting thread
        threading.Thread(target=self._rate_report_loop, daemon=True).start()

        # Start retry thread for exactly-once
        if self.exactly_once:
            threading.Thread(target=self._retry_loop, daemon=True).start()
            # Start background ReplicatedFS sync thread for crash resilience
            threading.Thread(target=self._rfs_sync_loop, daemon=True).start()

        # Start output buffer flush thread (for final stage)
        is_final_stage = (self.stage == self.num_stages - 1)
        if is_final_stage and self.rfs_dest:
            threading.Thread(target=self._output_flush_loop, daemon=True).start()

        # Main loop
        self._server_loop()
    
    def _retry_pending_from_recovery(self):
        """FIXED: Retry pending outputs that weren't acked before crash."""
        if not self.eo_tracker:
            return

        pending = self.eo_tracker.get_pending_outputs()
        if pending:
            self.logger.log(f"EO RECOVERY: Retrying {len(pending)} pending outputs")

            # Get fresh successor info from leader (stored targets may be stale)
            fresh_successors = self._get_fresh_successors()

            for output_id, info in pending:
                tuple_msg = info.get('tuple')
                target = info.get('target')
                if tuple_msg and target:
                    # Use fresh target if available, fall back to stored
                    actual_target = target
                    if fresh_successors:
                        key = tuple_msg.get('key', '')
                        task_idx = hash(key) % len(fresh_successors)
                        actual_target = fresh_successors[task_idx]

                    success = self._send_tuple_to_task(actual_target, tuple_msg)
                    if success:
                        self.eo_tracker.mark_output_acked(output_id)
                        self.logger.log(f"EO RECOVERY: Resent and acked {output_id}")

    def _get_fresh_successors(self) -> List[dict]:
        """Get fresh successor task info from leader."""
        try:
            msg = {'type': 'GET_TASK_CONFIG', 'task_id': self.task_id}
            response = self._send_to_leader(msg)
            if response and response.get('status') == 'success':
                return response.get('successor_tasks', [])
        except Exception as e:
            self.logger.log(f"EO RECOVERY: Could not get fresh successors: {e}")
        return []
    
    def stop(self):
        """Stop the task."""
        self.running = False

        if self.eo_tracker:
            self.eo_tracker.flush()

        if hasattr(self, 'rfs_node') and self.rfs_node:
            try:
                self.rfs_node.membership.leave_group()
                self.rfs_node.stop()
            except Exception:
                pass

        if self.server_socket:
            self.server_socket.close()

        self._notify_leader_completed()

        self.logger.log("TASK END")
    
    def _get_config_from_leader(self):
        """Get task configuration from leader."""
        try:
            msg = {'type': 'GET_TASK_CONFIG', 'task_id': self.task_id}
            response = self._send_to_leader(msg)
            
            if response and response.get('status') == 'success':
                self.config = response
                with self.successor_lock:
                    self.successor_tasks = response.get('successor_tasks', [])
                self.rfs_dest = response.get('rfs_dest', '')
                self.num_stages = response.get('num_stages', 1)
                self.expected_eof = response.get('num_predecessors', 1)

                self.logger.log(f"CONFIG: successors={len(self.successor_tasks)} "
                              f"dest={self.rfs_dest} expected_eof={self.expected_eof}")
        except Exception as e:
            self.logger.log(f"CONFIG ERROR: {e}")
    
    def _notify_leader_started(self):
        """Notify leader that task has started."""
        msg = {
            'type': 'TASK_STARTED',
            'task_id': self.task_id,
            'pid': os.getpid(),
            'log_file': self.logger.log_file
        }
        self._send_to_leader(msg)
    
    def _notify_leader_completed(self):
        """Notify leader that task has completed."""
        msg = {'type': 'TASK_COMPLETED', 'task_id': self.task_id}
        self._send_to_leader(msg)
    
    def _send_to_leader(self, msg: dict) -> Optional[dict]:
        """Send message to leader."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10.0)
            leader_port = get_stream_port(_CONFIG.get('leader_id', 1))
            sock.connect((self.leader_host, leader_port))
            sock.sendall(json.dumps(msg).encode('utf-8'))
            response = sock.recv(65536).decode('utf-8')
            sock.close()
            return json.loads(response)
        except Exception as e:
            self.logger.log(f"LEADER COMM ERROR: {e}")
            return None
    
    def _server_loop(self):
        """Main server loop to receive tuples."""
        while self.running:
            try:
                self.server_socket.settimeout(1.0)
                conn, addr = self.server_socket.accept()
                threading.Thread(target=self._handle_tuple,
                               args=(conn, addr), daemon=True).start()
            except socket.timeout:
                continue
            except Exception as e:
                if self.running:
                    self.logger.log(f"SERVER ERROR: {e}")
    
    def _handle_tuple(self, conn: socket.socket, addr: tuple):
        """Handle incoming tuple."""
        try:
            data = conn.recv(65536).decode('utf-8')
            if not data:
                return
            
            msg = json.loads(data)
            msg_type = msg.get('type')
            
            if msg_type == 'TUPLE':
                # Process tuple - returns True on success (including duplicates)
                success = self._process_tuple(msg)
                # Only ACK if processing succeeded (forwarding completed or duplicate)
                # This ensures source will retry if we crash before forwarding
                if success:
                    conn.sendall(json.dumps({'status': 'ack'}).encode('utf-8'))
                else:
                    conn.sendall(json.dumps({'status': 'nack'}).encode('utf-8'))
            elif msg_type == 'ACK':
                self._handle_ack(msg)
            elif msg_type == 'EOF':
                self._handle_eof(msg)
                conn.sendall(json.dumps({'status': 'ack'}).encode('utf-8'))
            elif msg_type == 'UPDATE_SUCCESSORS':
                self._handle_update_successors(msg)
                conn.sendall(json.dumps({'status': 'ack'}).encode('utf-8'))
        except Exception as e:
            self.logger.log(f"TUPLE ERROR: {e}")
        finally:
            conn.close()
    
    def _process_tuple(self, msg: dict) -> bool:
        """
        Process an incoming tuple.

        FIXED exactly-once approach:
        - Check if tuple_id already seen -> discard but ACK
        - Check if tuple_id was started but not completed -> resend pending outputs only
        - Check if tuple_id is currently being processed -> NACK (let sender retry)
        - Else -> mark started, process, forward, mark completed, ACK

        Key insight: We must NOT mark as processed until forwarding succeeds.
        Otherwise, a retry after failed forward would be discarded as duplicate.
        """
        tuple_id = msg.get('tuple_id')
        key = msg.get('key')
        value = msg.get('value')

        # Step 1: Atomic check and claim the tuple for processing
        if self.exactly_once and self.eo_tracker:
            with self.processing_lock:
                # Check if already fully processed
                if self.eo_tracker.is_duplicate(tuple_id):
                    # Already processed and forwarded successfully - ACK the duplicate
                    self.logger.log(f"DUPLICATE REJECTED: {tuple_id}")
                    return True

                # Check if was started but not completed (crash recovery case)
                if self.eo_tracker.was_started(tuple_id):
                    # Don't re-run operator - just resend pending outputs
                    self.logger.log(f"EO RECOVERY: Tuple {tuple_id} was started, resending pending outputs")
                    self._resend_pending_outputs_for_tuple(tuple_id)
                    # Mark as fully processed now
                    self.eo_tracker.mark_processed(tuple_id)
                    return True

                # Check if currently being processed by another thread
                if tuple_id in self.in_progress_tuples:
                    # Another thread is processing this - NACK to trigger retry later
                    return False

                # Claim this tuple for processing
                self.in_progress_tuples.add(tuple_id)

        try:
            # Step 2: Mark as started (before running operator)
            if self.exactly_once and self.eo_tracker:
                self.eo_tracker.mark_started(tuple_id)

            # Step 3: Run operator
            output_tuples = self._run_operator(key, value)

            self.tuples_processed += 1

            if self.tuples_processed % 100 == 1:
                self.logger.log(f"PROCESSED: tuple #{self.tuples_processed}, produced {len(output_tuples)} outputs")

            # Step 4: Forward to next stage or output
            is_last_stage = (self.stage == self.num_stages - 1)
            all_forwarded = True

            if is_last_stage:
                self._write_output(output_tuples, tuple_id)
            else:
                # Initialize output sequence counter for deterministic output IDs
                if self.exactly_once and self.eo_tracker:
                    start_seq = self.eo_tracker.get_next_output_seq(tuple_id)
                else:
                    start_seq = 0
                self.output_seq_counter[tuple_id] = start_seq

                for out_key, out_value in output_tuples:
                    success = self._forward_tuple(tuple_id, out_key, out_value)
                    if not success:
                        all_forwarded = False

            # Clean up sequence counter
            if tuple_id in self.output_seq_counter:
                del self.output_seq_counter[tuple_id]

            # Step 5: ONLY mark as processed AFTER forwarding succeeds
            # This ensures retries will re-process if forwarding failed
            if all_forwarded and self.exactly_once and self.eo_tracker:
                self.eo_tracker.mark_processed(tuple_id)

            # Return True to ACK only if forwarding succeeded
            return all_forwarded

        finally:
            # Always release the tuple from in_progress set
            if self.exactly_once:
                with self.processing_lock:
                    self.in_progress_tuples.discard(tuple_id)
    
    def _run_operator(self, key: str, value: str) -> List[tuple]:
        """Run the operator on input tuple."""
        try:
            cmd = [self.op_exe]
            if self.op_args:
                cmd.extend(self.op_args.split())
            # Always append task_id WITH run_id for stateful operators (like count_op)
            # This ensures unique state files per task per run
            cmd.append(f"{self.task_id}_{self.run_id}")

            input_data = f"{key}\t{value}\n"
            
            task_dir = os.path.dirname(os.path.abspath(__file__))
            result = subprocess.run(
                cmd,
                input=input_data,
                capture_output=True,
                text=True,
                timeout=5.0,
                cwd=task_dir
            )

            if result.stderr:
                self.logger.log(f"OPERATOR STDERR: {result.stderr[:200]}")

            output_tuples = []
            for line in result.stdout.strip().split('\n'):
                if line:
                    parts = line.split('\t', 1)
                    if len(parts) == 2:
                        output_tuples.append((parts[0], parts[1]))
                    elif len(parts) == 1:
                        output_tuples.append((key, parts[0]))
            
            return output_tuples
            
        except subprocess.TimeoutExpired:
            self.logger.log(f"OPERATOR TIMEOUT: {self.op_exe}")
            return []
        except FileNotFoundError:
            self.logger.log(f"OPERATOR NOT FOUND: {self.op_exe}")
            return []
        except Exception as e:
            self.logger.log(f"OPERATOR ERROR: {e}")
            return []
    
    def _forward_tuple(self, source_tuple_id: str, key: str, value: str) -> bool:
        """
        Forward tuple to next stage.

        FIXED: Uses DETERMINISTIC output_id based on:
        - source_tuple_id (the input tuple)
        - task_id (this task)
        - sequence number (for multiple outputs from same input)

        This ensures that retries produce the SAME output_id, allowing
        downstream tasks to correctly detect duplicates.

        Returns True if forwarding succeeded, False otherwise.
        """
        with self.successor_lock:
            if not self.successor_tasks:
                self.logger.log(f"FORWARD SKIP: No successor tasks configured")
                return True  # No successors = nothing to do = success
            successors_snapshot = list(self.successor_tasks)

        # Hash partition
        target_idx = hash(key) % len(successors_snapshot)
        target = successors_snapshot[target_idx]

        # FIXED: Create DETERMINISTIC output ID
        # This ID will be the same across retries, allowing duplicate detection
        seq_num = self.output_seq_counter.get(source_tuple_id, 0)
        self.output_seq_counter[source_tuple_id] = seq_num + 1
        output_id = f"{source_tuple_id}_{self.task_id}_out{seq_num}"

        tuple_msg = {
            'type': 'TUPLE',
            'tuple_id': output_id,
            'key': key,
            'value': value,
            'source_task': self.task_id
        }

        # Track pending output with full data for retry after restart
        if self.exactly_once and self.eo_tracker:
            # Only add if not already acked (handles restart case)
            if not self.eo_tracker.is_output_acked(output_id):
                self.eo_tracker.add_pending_output(output_id, tuple_msg, target)
            else:
                # Already acked from before crash, skip sending
                self.logger.log(f"EO: Output {output_id} already acked, skipping")
                return True

        success = self._send_tuple_to_task(target, tuple_msg)

        if success:
            # Mark as acked
            if self.exactly_once and self.eo_tracker:
                self.eo_tracker.mark_output_acked(output_id)
            return True
        else:
            # Retry with backoff
            for retry in range(3):
                time.sleep(0.1 * (retry + 1))
                success = self._send_tuple_to_task(target, tuple_msg)
                if success:
                    if self.exactly_once and self.eo_tracker:
                        self.eo_tracker.mark_output_acked(output_id)
                    return True
            self.logger.log(f"FORWARD FAILED: to {target['task_id']} after retries")
            return False
    
    def _send_tuple_to_task(self, target: dict, msg: dict) -> bool:
        """Send tuple to a task and wait for ACK."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)
            sock.connect((target['hostname'], target['port']))
            sock.sendall(json.dumps(msg).encode('utf-8'))

            # Wait for ACK
            response = sock.recv(1024).decode('utf-8')
            sock.close()

            if 'ack' in response:
                return True
            return False
        except Exception as e:
            self.logger.log(f"SEND ERROR to {target.get('hostname')}:{target.get('port')}: {e}")
            return False
    
    def _resend_pending_outputs_for_tuple(self, tuple_id: str):
        """
        Resend pending outputs for a tuple that was started but not completed.
        Used during crash recovery to avoid re-running the operator.
        """
        if not self.eo_tracker:
            return

        pending = self.eo_tracker.get_pending_outputs()
        fresh_successors = self._get_fresh_successors()

        for output_id, info in pending:
            # Check if this output belongs to the given tuple
            if not output_id.startswith(f"{tuple_id}_"):
                continue

            tuple_msg = info.get('tuple')
            target = info.get('target')

            if not tuple_msg or not target:
                continue

            # Use fresh target if available
            actual_target = target
            if fresh_successors:
                key = tuple_msg.get('key', '')
                task_idx = hash(key) % len(fresh_successors)
                actual_target = fresh_successors[task_idx]

            success = self._send_tuple_to_task(actual_target, tuple_msg)
            if success:
                self.eo_tracker.mark_output_acked(output_id)
                self.logger.log(f"EO RECOVERY: Resent pending output {output_id}")

    def _handle_ack(self, msg: dict):
        """Handle ACK from next stage."""
        output_id = msg.get('output_id')

        if self.exactly_once and self.eo_tracker:
            self.eo_tracker.mark_output_acked(output_id)
    
    def _handle_eof(self, msg: dict):
        """Handle EOF signal."""
        self.eof_received += 1
        self.logger.log(f"EOF received ({self.eof_received}/{self.expected_eof})")

        if self.eof_received >= self.expected_eof:
            self.logger.log("All EOFs received, finishing up...")

            # Flush any remaining output buffer to ReplicatedFS
            if self.rfs_dest:
                with self.output_buffer_lock:
                    if self.output_buffer:
                        self.logger.log(f"EOF: Flushing {len(self.output_buffer)} remaining outputs to ReplicatedFS")
                        self._flush_output_buffer()

            # Log final stats
            if self.eo_tracker:
                self.logger.log(f"TASK STATS: processed={self.tuples_processed}, "
                              f"tracked_processed={len(self.eo_tracker.processed_ids)}, "
                              f"acked_outputs={len(self.eo_tracker.acked_outputs)}, "
                              f"pending_outputs={len(self.eo_tracker.pending_outputs)}")

            # Forward EOF to all successor tasks
            with self.successor_lock:
                successors_snapshot = list(self.successor_tasks)
            if successors_snapshot:
                for target in successors_snapshot:
                    eof_msg = {'type': 'EOF', 'source_task': self.task_id}
                    self._send_tuple_to_task(target, eof_msg)
                self.logger.log(f"Forwarded EOF to {len(successors_snapshot)} successors")

            # Flush exactly-once tracker
            if self.eo_tracker:
                self.eo_tracker.flush()

            time.sleep(0.5)

            self.logger.log("Task complete, shutting down")
            self.running = False

    def _handle_update_successors(self, msg: dict):
        """Handle autoscaling update to successor task list."""
        new_successors = msg.get('successor_tasks', [])
        with self.successor_lock:
            old_count = len(self.successor_tasks)
            self.successor_tasks = new_successors
            self.logger.log(f"AUTOSCALE: Updated successors {old_count} -> {len(self.successor_tasks)}")
    
    def _write_output(self, tuples: List[tuple], source_tuple_id: str = None):
        """
        Buffer output tuples for batched ReplicatedFS writes.
        Also writes to local file immediately as backup.
        Uses output tracking to prevent duplicates after crash recovery.
        """
        if not tuples:
            return

        task_dir = os.path.dirname(os.path.abspath(__file__))
        local_output_file = os.path.join(task_dir, f"output_{self.task_id}.txt")

        # Collect lines to write
        lines_to_write = []
        for idx, (key, value) in enumerate(tuples):
            # For exactly-once, create unique output ID and check if already written
            if self.exactly_once and self.eo_tracker and source_tuple_id:
                output_id = f"{source_tuple_id}_{self.task_id}_write{idx}"
                # Check if this output was already written (handles crash recovery)
                if self.eo_tracker.is_output_acked(output_id):
                    continue  # Skip - already written before crash
                lines_to_write.append((f"{key}\t{value}\n", output_id))
            else:
                lines_to_write.append((f"{key}\t{value}\n", None))

        if not lines_to_write:
            return

        # Write to local file immediately (backup)
        with open(local_output_file, 'a') as f:
            for line, _ in lines_to_write:
                f.write(line)

        # Buffer for batched ReplicatedFS write
        if self.rfs_dest:
            with self.output_buffer_lock:
                self.output_buffer.extend(lines_to_write)

                # Check if we should flush now
                should_flush = (
                    len(self.output_buffer) >= self.output_buffer_size or
                    (time.time() - self.last_flush_time) >= self.flush_interval
                )

                if should_flush:
                    self._flush_output_buffer()

        if self.tuples_processed == 1:
            self.logger.log(f"OUTPUT: local={local_output_file} rfs={self.rfs_dest}")

    def _flush_output_buffer(self):
        """
        Flush buffered output to ReplicatedFS.
        Must be called with output_buffer_lock held.
        """
        if not self.output_buffer:
            return

        # Copy and clear buffer
        to_flush = self.output_buffer[:]
        self.output_buffer.clear()
        self.last_flush_time = time.time()

        # Combine all lines into single string
        output_data = ''.join(line for line, _ in to_flush)

        # Send to local worker for ReplicatedFS write
        try:
            success = self._send_to_worker_rfs(self.rfs_dest, output_data)

            if success:
                self.logger.log(f"OUTPUT FLUSH: {len(to_flush)} lines to ReplicatedFS")
            else:
                self.logger.log(f"ReplicatedFS OUTPUT ERROR: Worker write failed for {len(to_flush)} lines (not re-buffering to avoid duplicates)")

        except Exception as e:
            self.logger.log(f"ReplicatedFS OUTPUT ERROR: {e} for {len(to_flush)} lines (not re-buffering to avoid duplicates)")

        # ALWAYS mark outputs as acked to prevent re-processing
        # Even if ReplicatedFS write failed, the data is in the local file as backup
        # Re-buffering causes exponential duplication
        if self.exactly_once and self.eo_tracker:
            for _, output_id in to_flush:
                if output_id:
                    self.eo_tracker.mark_output_acked(output_id)

    def _output_flush_loop(self):
        """Periodically flush output buffer to ReplicatedFS."""
        while self.running:
            try:
                time.sleep(self.flush_interval)

                with self.output_buffer_lock:
                    if self.output_buffer and (time.time() - self.last_flush_time) >= self.flush_interval:
                        self._flush_output_buffer()

            except Exception as e:
                self.logger.log(f"OUTPUT FLUSH ERROR: {e}")

    def _send_to_worker_rfs(self, rfs_filename: str, data: str) -> bool:
        """Send data to local worker for ReplicatedFS write."""
        try:
            # Worker is on localhost at the node's stream port
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10.0)
            sock.connect(('localhost', self.stream_port))

            msg = {
                'type': 'WRITE_RFS',
                'filename': rfs_filename,
                'data': data,
                'operation': 'append',
                'task_id': self.task_id
            }

            sock.sendall(json.dumps(msg).encode('utf-8'))
            response = sock.recv(65536).decode('utf-8')
            sock.close()

            result = json.loads(response)
            return result.get('status') == 'success'

        except Exception as e:
            self.logger.log(f"WORKER RFS ERROR: {e}")
            return False
    
    def _rate_report_loop(self):
        """Report rate to leader every second."""
        while self.running:
            try:
                time.sleep(1.0)

                now = time.time()
                elapsed = now - self.last_rate_time

                if elapsed > 0:
                    rate = (self.tuples_processed - self.last_tuple_count) / elapsed

                    msg = {
                        'type': 'RATE_UPDATE',
                        'task_id': self.task_id,
                        'rate': rate,
                        'tuples_processed': self.tuples_processed
                    }
                    self._send_to_leader(msg)

                    self.last_rate_time = now
                    self.last_tuple_count = self.tuples_processed

            except Exception as e:
                pass

    def _rfs_sync_loop(self):
        """Periodically sync state to ReplicatedFS for crash resilience."""
        while self.running:
            try:
                time.sleep(1.0)  # Sync every 1 second

                if self.eo_tracker:
                    self.eo_tracker.flush()

            except Exception as e:
                self.logger.log(f"RFS SYNC ERROR: {e}")
    
    def _retry_loop(self):
        """FIXED: Retry pending outputs for exactly-once."""
        while self.running:
            try:
                time.sleep(2.0)

                if not self.eo_tracker:
                    continue

                # Get pending outputs
                pending = self.eo_tracker.get_pending_outputs()
                if not pending:
                    continue

                now = time.time()

                # Get fresh successor info (stored targets may be stale after failures)
                fresh_successors = self._get_fresh_successors()

                for output_id, info in pending:
                    # Retry if older than 5 seconds and under max retries
                    if now - info.get('time', 0) > 5.0 and info.get('retries', 0) < 5:
                        tuple_msg = info.get('tuple')
                        target = info.get('target')

                        if tuple_msg and target:
                            self.eo_tracker.increment_retry(output_id)

                            # Use fresh target if available
                            actual_target = target
                            if fresh_successors:
                                key = tuple_msg.get('key', '')
                                task_idx = hash(key) % len(fresh_successors)
                                actual_target = fresh_successors[task_idx]

                            success = self._send_tuple_to_task(actual_target, tuple_msg)
                            if success:
                                self.eo_tracker.mark_output_acked(output_id)
                                self.logger.log(f"EO RETRY: Successfully resent {output_id}")

            except Exception as e:
                self.logger.log(f"RETRY ERROR: {e}")


def main():
    parser = argparse.ArgumentParser(description='StreamProcess Task')
    parser.add_argument('--task-id', required=True)
    parser.add_argument('--stage', type=int, required=True)
    parser.add_argument('--task-idx', type=int, required=True)
    parser.add_argument('--port', type=int, required=True)
    parser.add_argument('--op-exe', required=True)
    parser.add_argument('--op-args', default='')
    parser.add_argument('--leader-host', required=True)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--vm-id', type=int, required=True)
    parser.add_argument('--exactly-once', action='store_true')
    
    args = parser.parse_args()
    
    task = StreamProcessTask(
        task_id=args.task_id,
        stage=args.stage,
        task_idx=args.task_idx,
        port=args.port,
        op_exe=args.op_exe,
        op_args=args.op_args,
        leader_host=args.leader_host,
        run_id=args.run_id,
        vm_id=args.vm_id,
        exactly_once=args.exactly_once
    )
    
    try:
        task.start()
    except KeyboardInterrupt:
        pass
    finally:
        task.stop()


if __name__ == '__main__':
    main()