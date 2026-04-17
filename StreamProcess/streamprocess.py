#!/usr/bin/env python3
"""
StreamProcess - Stream Processing Framework
Main entry point and Leader coordination

Usage:
  Terminal 1 (Node 1): python3 streamprocess.py --vm-id 1 --leader
  Terminal 2+:        python3 streamprocess.py --vm-id N
  Submit job:         python3 streamprocess.py --submit --nstages 2 --ntasks 3 ...
"""
import argparse
import json
import os
import socket
import sys
import threading
import time
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple

# Add parent directory to path for sibling project imports
_parent_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')
sys.path.insert(0, _parent_dir)

# MembershipProtocol uses flat imports (no relative), so add its directory directly
_membership_dir = os.path.join(_parent_dir, 'MembershipProtocol')
sys.path.insert(0, _membership_dir)

from membership import MembershipService, MemberStatus
from ReplicatedFS.node import ReplicatedFSNode
from ReplicatedFS.main import ReplicatedFSClient


def _load_config() -> dict:
    """Load configuration from config.json."""
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.json')
    with open(config_path, 'r') as f:
        return json.load(f)

_CONFIG = _load_config()

# Build node host and port mappings from config
NODE_HOSTS = {}
NODE_STREAM_PORTS = {}
NODE_TASK_BASE_PORTS = {}
for _node in _CONFIG['nodes']:
    NODE_HOSTS[_node['id']] = _node['ip']
    NODE_STREAM_PORTS[_node['id']] = _node.get('stream_port', 8001)
    NODE_TASK_BASE_PORTS[_node['id']] = _node.get('task_base_port', 8101)

MAX_TASKS_PER_STAGE = _CONFIG.get('max_tasks_per_stage', 10)


def get_stream_port(node_id: int) -> int:
    """Get the stream processing port for a given node."""
    return NODE_STREAM_PORTS.get(node_id, 8001)


def get_stream_port_by_host(hostname: str) -> int:
    """Get the stream processing port for a node identified by hostname/IP."""
    for nid, host in NODE_HOSTS.items():
        if host == hostname:
            return NODE_STREAM_PORTS.get(nid, 8001)
    return 8001


def get_node_id_from_hostname(hostname: str) -> int:
    """Get node ID from hostname."""
    for node_id, host in NODE_HOSTS.items():
        if host == hostname:
            return node_id
    return 1

def get_task_port(stage: int, task_idx: int, node_id: int = 1) -> int:
    """Calculate the port for a task based on stage, task index, and node.

    This ensures tasks in different stages don't collide on the same port.
    Port = task_base_port + (stage * MAX_TASKS_PER_STAGE) + task_idx

    Examples (node 1, base 8101):
      Stage 0, Task 0 -> 8101
      Stage 0, Task 1 -> 8102
      Stage 1, Task 0 -> 8111
      Stage 1, Task 1 -> 8112
    """
    base = NODE_TASK_BASE_PORTS.get(node_id, 8101)
    return base + (stage * MAX_TASKS_PER_STAGE) + task_idx

def get_timestamp() -> str:
    """Get formatted timestamp."""
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]


class StreamProcessLogger:
    """Logger for StreamProcess operations."""
    
    def __init__(self, vm_id: int, run_id: str, is_leader: bool = False):
        self.vm_id = vm_id
        self.run_id = run_id
        prefix = "leader" if is_leader else "streamprocess"
        self.log_file = f"{prefix}_{vm_id}_{run_id}.log"
        self.lock = threading.Lock()
    
    def log(self, msg: str):
        """Log a message with timestamp."""
        line = f"[{get_timestamp()}] {msg}"
        with self.lock:
            print(line)
            with open(self.log_file, 'a') as f:
                f.write(line + "\n")


class TaskInfo:
    """Information about a running task."""
    
    def __init__(self, task_id: str, stage: int, task_idx: int, vm_id: int, 
                 pid: int, op_exe: str, op_args: str, log_file: str):
        self.task_id = task_id
        self.stage = stage
        self.task_idx = task_idx
        self.vm_id = vm_id
        self.pid = pid
        self.op_exe = op_exe
        self.op_args = op_args
        self.log_file = log_file
        self.start_time = time.time()
        self.tuples_processed = 0
        self.last_rate_time = time.time()
        self.last_tuple_count = 0
    
    def to_dict(self) -> dict:
        return {
            'task_id': self.task_id,
            'stage': self.stage,
            'task_idx': self.task_idx,
            'vm_id': self.vm_id,
            'pid': self.pid,
            'op_exe': self.op_exe,
            'op_args': self.op_args,
            'log_file': self.log_file
        }


class StreamProcessLeader:
    """
    StreamProcess Leader - coordinates stream processing jobs.
    Handles scheduling, monitoring, autoscaling, and failure recovery.
    """

    def __init__(self, vm_id: int, rfs_node: ReplicatedFSNode):
        self.vm_id = vm_id
        self.hostname = socket.gethostname()
        self.rfs_node = rfs_node
        self.rfs_client = ReplicatedFSClient(rfs_node)
        self.rfs_write_lock = threading.Lock()  # Lock for ReplicatedFS writes from tasks

        # Track which ReplicatedFS files have been created (per-instance, not class-level)
        self._rfs_files_created: set = set()
        self._eo_rfs_files_created: set = set()

        self.run_id = f"{int(time.time())}"
        self.logger = StreamProcessLogger(vm_id, self.run_id, is_leader=True)

        # Job state
        self.running = False
        self.job_running = False  # True while a job is actively processing
        self.job_config: Optional[dict] = None
        self.tasks: Dict[str, TaskInfo] = {}  # task_id -> TaskInfo
        self.tasks_lock = threading.Lock()
        
        # Stage configuration
        self.stages: List[dict] = []  # [{op_exe, op_args, num_tasks}]
        self.stage_tasks: Dict[int, List[str]] = {}  # stage -> [task_ids]
        self.final_stage_tasks: List[dict] = []  # Track final stage task info for output collection
        
        # Rate monitoring for autoscaling
        self.task_rates: Dict[str, float] = {}  # task_id -> tuples/sec
        self.rates_lock = threading.Lock()
        
        # Autoscaling config
        self.autoscale_enabled = False
        self.input_rate = 0
        self.lw = 0  # Low watermark
        self.hw = 0  # High watermark
        
        # Exactly-once
        self.exactly_once = False
        
        # Network
        self.server_socket: Optional[socket.socket] = None
        self.server_thread: Optional[threading.Thread] = None
        
        # Monitoring threads
        self.monitor_thread: Optional[threading.Thread] = None
        self.rate_log_thread: Optional[threading.Thread] = None
    
    def start(self):
        """Start the leader server."""
        self.running = True
        
        # Start server for receiving messages
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.stream_port = get_stream_port(self.vm_id)
        self.server_socket.bind(('', self.stream_port))
        self.server_socket.listen(20)

        self.server_thread = threading.Thread(target=self._server_loop, daemon=True)
        self.server_thread.start()

        self.logger.log(f"LEADER: Started on port {self.stream_port}")
    
    def stop(self):
        """Stop the leader."""
        self.running = False
        if self.server_socket:
            self.server_socket.close()
        self.logger.log("LEADER: Stopped")
    
    def _server_loop(self):
        """Main server loop to accept connections."""
        while self.running:
            try:
                self.server_socket.settimeout(1.0)
                conn, addr = self.server_socket.accept()
                threading.Thread(target=self._handle_connection, 
                               args=(conn, addr), daemon=True).start()
            except socket.timeout:
                continue
            except Exception as e:
                if self.running:
                    self.logger.log(f"LEADER ERROR: {e}")
    
    def _handle_connection(self, conn: socket.socket, addr: tuple):
        """Handle incoming connection."""
        try:
            data = conn.recv(65536).decode('utf-8')
            if not data:
                return
            
            msg = json.loads(data)
            msg_type = msg.get('type')
            
            response = {'status': 'error', 'message': 'Unknown message type'}
            
            if msg_type == 'RATE_UPDATE':
                response = self._handle_rate_update(msg)
            elif msg_type == 'TASK_STARTED':
                response = self._handle_task_started(msg)
            elif msg_type == 'TASK_COMPLETED':
                response = self._handle_task_completed(msg)
            elif msg_type == 'LIST_TASKS':
                response = self._handle_list_tasks()
            elif msg_type == 'KILL_TASK':
                response = self._handle_kill_task(msg)
            elif msg_type == 'GET_TASK_CONFIG':
                response = self._handle_get_task_config(msg)
            elif msg_type == 'SUBMIT_JOB':
                response = self._handle_submit_job(msg)
            elif msg_type == 'GET_WORKERS':
                response = self._handle_get_workers()
            elif msg_type == 'CHECK_TASK':
                # Leader also needs to handle CHECK_TASK for local tasks
                response = self._handle_check_task_local(msg)
            elif msg_type == 'KILL_PROCESS':
                # Leader also needs to handle KILL_PROCESS for local tasks
                response = self._handle_kill_process_local(msg)
            elif msg_type == 'GET_OUTPUT':
                # Leader also needs to handle GET_OUTPUT for local tasks
                response = self._handle_get_output_local(msg)
            elif msg_type == 'WRITE_RFS':
                # Leader also needs to handle WRITE_RFS for local tasks
                response = self._handle_write_rfs(msg)
            elif msg_type == 'WRITE_RFS_EO':
                # Handle exactly-once log writes from tasks
                response = self._handle_write_rfs_eo(msg)
            elif msg_type == 'READ_RFS_EO':
                # Handle exactly-once log reads from tasks (for recovery)
                response = self._handle_read_rfs_eo(msg)

            conn.sendall(json.dumps(response).encode('utf-8'))
        except Exception as e:
            self.logger.log(f"LEADER ERROR handling connection: {e}")
        finally:
            conn.close()
    
    def _handle_submit_job(self, msg: dict) -> dict:
        """Handle job submission from CLI."""
        try:
            self.start_job(
                nstages=msg.get('nstages'),
                ntasks_per_stage=msg.get('ntasks'),
                operators=msg.get('operators'),
                rfs_src=msg.get('src'),
                rfs_dest=msg.get('dest'),
                exactly_once=msg.get('exactly_once', False),
                autoscale_enabled=msg.get('autoscale', False),
                input_rate=msg.get('input_rate', 100),
                lw=msg.get('lw', 50),
                hw=msg.get('hw', 150)
            )
            return {'status': 'success', 'message': 'Job submitted'}
        except Exception as e:
            return {'status': 'error', 'message': str(e)}
    
    def _handle_get_workers(self) -> dict:
        """Return list of available workers."""
        workers = self._get_available_workers()
        return {'status': 'success', 'workers': workers}
    
    def _handle_rate_update(self, msg: dict) -> dict:
        """Handle rate update from a task."""
        task_id = msg.get('task_id')
        rate = msg.get('rate', 0)
        tuples_processed = msg.get('tuples_processed', 0)

        # Only accept rate updates from tasks belonging to current job
        # Task IDs include run_id: stage0_task0_<run_id>
        if self.run_id and f"_{self.run_id}" not in task_id:
            # This is a rate update from an old job's task - ignore it
            return {'status': 'ignored', 'reason': 'old_job'}

        with self.rates_lock:
            self.task_rates[task_id] = rate

        with self.tasks_lock:
            if task_id in self.tasks:
                self.tasks[task_id].tuples_processed = tuples_processed

        return {'status': 'success'}
    
    def _handle_task_started(self, msg: dict) -> dict:
        """Handle task started notification."""
        task_id = msg.get('task_id')
        pid = msg.get('pid')
        log_file = msg.get('log_file')
        
        with self.tasks_lock:
            if task_id in self.tasks:
                self.tasks[task_id].pid = pid
                self.tasks[task_id].log_file = log_file
                task = self.tasks[task_id]
                self.logger.log(f"TASK START: {task_id} on Node-{task.vm_id} PID={pid} "
                              f"op={task.op_exe} log={log_file}")
        
        return {'status': 'success'}
    
    def _handle_task_completed(self, msg: dict) -> dict:
        """Handle task completion notification."""
        task_id = msg.get('task_id')
        self.logger.log(f"TASK END: {task_id}")

        # Remove completed task from tracking
        with self.tasks_lock:
            if task_id in self.tasks:
                del self.tasks[task_id]

            # Check if all tasks are done
            if len(self.tasks) == 0 and self.job_running:
                self.job_running = False
                self.logger.log("JOB COMPLETE: All tasks finished")
                rfs_dest = self.job_config.get('rfs_dest', '') if self.job_config else ''
                self.logger.log(f"RUN END: Output written to ReplicatedFS:{rfs_dest}")

        return {'status': 'success'}
    
    def _handle_list_tasks(self) -> dict:
        """Return list of all tasks."""
        with self.tasks_lock:
            tasks = [t.to_dict() for t in self.tasks.values()]
        return {'status': 'success', 'tasks': tasks}
    
    def _handle_kill_task(self, msg: dict) -> dict:
        """Kill a specific task."""
        vm_id = msg.get('vm_id')
        pid = msg.get('pid')
        
        # Send kill command to the worker
        hostname = NODE_HOSTS.get(vm_id)
        if not hostname:
            return {'status': 'error', 'message': 'Invalid node ID'}
        
        try:
            kill_msg = {'type': 'KILL_PROCESS', 'pid': pid}
            response = self._send_to_worker(hostname, kill_msg)
            
            if response and response.get('status') == 'success':
                self.logger.log(f"TASK KILLED: Node-{vm_id} PID={pid}")
                return {'status': 'success'}
            return {'status': 'error', 'message': 'Kill failed'}
        except Exception as e:
            return {'status': 'error', 'message': str(e)}
    
    def _handle_get_task_config(self, msg: dict) -> dict:
        """Return configuration for a task."""
        task_id = msg.get('task_id')
        
        with self.tasks_lock:
            if task_id not in self.tasks:
                return {'status': 'error', 'message': 'Task not found'}
            
            task = self.tasks[task_id]
            stage = task.stage
        
        # Calculate number of predecessor tasks (for EOF tracking)
        if stage == 0:
            # Stage 0 receives from source only
            num_predecessors = 1
        else:
            # Other stages receive from all tasks in previous stage
            prev_stage = stage - 1
            with self.tasks_lock:
                num_predecessors = len(self.stage_tasks.get(prev_stage, []))

        # Build task configuration
        config = {
            'status': 'success',
            'task_id': task_id,
            'stage': stage,
            'task_idx': task.task_idx,
            'op_exe': task.op_exe,
            'op_args': task.op_args,
            'exactly_once': self.exactly_once,
            'rfs_dest': self.job_config.get('rfs_dest') if self.job_config else '',
            'num_stages': len(self.stages),
            'input_rate': self.input_rate,
            'num_predecessors': num_predecessors
        }

        # Add successor task info
        if stage < len(self.stages) - 1:
            next_stage = stage + 1
            config['successor_tasks'] = []
            with self.tasks_lock:
                for tid in self.stage_tasks.get(next_stage, []):
                    if tid in self.tasks:
                        t = self.tasks[tid]
                        config['successor_tasks'].append({
                            'task_id': tid,
                            'vm_id': t.vm_id,
                            'hostname': NODE_HOSTS[t.vm_id],
                            'port': get_task_port(t.stage, t.task_idx)
                        })

        return config

    def _handle_check_task_local(self, msg: dict) -> dict:
        """Check if a local task is still running (leader handles this for tasks on its node)."""
        pid = msg.get('pid')
        try:
            os.kill(pid, 0)
            # Also check if process is a zombie (terminated but not reaped)
            try:
                with open(f'/proc/{pid}/status', 'r') as f:
                    status = f.read()
                    if 'State:\tZ' in status:  # Zombie state
                        return {'status': 'success', 'running': False}
            except FileNotFoundError:
                return {'status': 'success', 'running': False}
            except Exception:
                pass
            return {'status': 'success', 'running': True}
        except OSError:
            return {'status': 'success', 'running': False}

    def _handle_kill_process_local(self, msg: dict) -> dict:
        """Kill a local process (leader handles this for tasks on its node)."""
        pid = msg.get('pid')
        try:
            os.kill(pid, 9)
            self.logger.log(f"KILLED LOCAL: PID={pid}")
            return {'status': 'success'}
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    def _handle_get_output_local(self, msg: dict) -> dict:
        """Get output file contents for a local task."""
        task_id = msg.get('task_id')
        task_dir = os.path.dirname(os.path.abspath(__file__))
        output_file = os.path.join(task_dir, f"output_{task_id}.txt")

        try:
            with open(output_file, 'r') as f:
                lines = f.read().splitlines()
            self.logger.log(f"GET_OUTPUT LOCAL: {task_id} - {len(lines)} lines")
            return {'status': 'success', 'output': lines}
        except FileNotFoundError:
            return {'status': 'success', 'output': []}
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    def _check_rfs_file_exists(self, rfs_filename: str) -> bool:
        """Check if a file exists in ReplicatedFS by attempting a GET."""
        try:
            temp_check = f"/tmp/rfs_check_{time.time()}.tmp"
            # Try to get the file - if it succeeds, file exists
            self.rfs_client.get(rfs_filename, temp_check)
            exists = os.path.exists(temp_check) and os.path.getsize(temp_check) > 0
            # Cleanup
            if os.path.exists(temp_check):
                os.remove(temp_check)
            return exists
        except Exception:
            return False

    def _handle_write_rfs(self, msg: dict) -> dict:
        """
        Write data to ReplicatedFS on behalf of a local task.
        Tasks send output data here, and leader uses its ReplicatedFS client.

        Strategy:
        1. If we've created the file locally before, just APPEND
        2. Otherwise, check if file exists in ReplicatedFS
           - If exists: APPEND
           - If not exists: CREATE
        """
        rfs_filename = msg.get('filename')
        data = msg.get('data')  # String data
        task_id = msg.get('task_id', 'unknown')

        if not rfs_filename or data is None:
            return {'status': 'error', 'message': 'Missing filename or data'}

        with self.rfs_write_lock:
            temp_file = None
            try:
                # Write data to temp file
                temp_file = f"/tmp/rfs_write_{task_id}_{time.time()}.tmp"

                with open(temp_file, 'w') as f:
                    f.write(data)

                # Determine whether to CREATE or APPEND
                if rfs_filename in self._rfs_files_created:
                    # We've written to this file before - just append
                    self.rfs_client.append(temp_file, rfs_filename)
                    self.logger.log(f"RFS APPEND: {rfs_filename} from task {task_id}")
                else:
                    # First time we're writing to this file - check if it exists
                    if self._check_rfs_file_exists(rfs_filename):
                        # File exists (created by another worker) - append
                        self.rfs_client.append(temp_file, rfs_filename)
                        self.logger.log(f"RFS APPEND (file existed): {rfs_filename} from task {task_id}")
                    else:
                        # File doesn't exist - create it
                        self.rfs_client.create(temp_file, rfs_filename)
                        self.logger.log(f"RFS CREATE: {rfs_filename} from task {task_id}")

                    # Mark as created locally for future writes
                    self._rfs_files_created.add(rfs_filename)

                # Cleanup temp file
                os.remove(temp_file)

                return {'status': 'success'}

            except Exception as e:
                self.logger.log(f"RFS WRITE ERROR: {rfs_filename} - {e}")
                # Try to cleanup temp file
                if temp_file:
                    try:
                        os.remove(temp_file)
                    except Exception:
                        pass
                return {'status': 'error', 'message': str(e)}

    def _handle_write_rfs_eo(self, msg: dict) -> dict:
        """
        Write exactly-once log data to ReplicatedFS on behalf of a task.

        This enables cross-node recovery: when a task restarts on a different node,
        it can load its EO log from ReplicatedFS (not just local file).

        Strategy:
        1. If we've created the file before, just APPEND
        2. Otherwise, check if file exists in ReplicatedFS
           - If exists: APPEND
           - If not exists: CREATE
        """
        rfs_filename = msg.get('filename')
        data = msg.get('data')  # String data
        operation = msg.get('operation', 'append')  # 'create' or 'append'
        task_id = msg.get('task_id', 'unknown')

        if not rfs_filename or data is None:
            return {'status': 'error', 'message': 'Missing filename or data'}

        with self.rfs_write_lock:
            temp_file = None
            try:
                # Write data to temp file
                temp_file = f"/tmp/rfs_eo_{task_id}_{time.time()}.tmp"

                with open(temp_file, 'w') as f:
                    f.write(data)

                if operation == 'create':
                    # Explicit create request
                    self.rfs_client.create(temp_file, rfs_filename)
                    self._eo_rfs_files_created.add(rfs_filename)
                    self.logger.log(f"RFS EO CREATE: {rfs_filename} from task {task_id}")
                else:
                    # Append operation - determine if file exists
                    if rfs_filename in self._eo_rfs_files_created:
                        # We've written to this file before - just append
                        self.rfs_client.append(temp_file, rfs_filename)
                        self.logger.log(f"RFS EO APPEND: {rfs_filename} from task {task_id}")
                    else:
                        # First time - check if file exists
                        if self._check_rfs_file_exists(rfs_filename):
                            self.rfs_client.append(temp_file, rfs_filename)
                            self.logger.log(f"RFS EO APPEND (existed): {rfs_filename} from task {task_id}")
                        else:
                            self.rfs_client.create(temp_file, rfs_filename)
                            self.logger.log(f"RFS EO CREATE (new): {rfs_filename} from task {task_id}")
                        self._eo_rfs_files_created.add(rfs_filename)

                # Cleanup temp file
                os.remove(temp_file)

                return {'status': 'success'}

            except Exception as e:
                self.logger.log(f"RFS EO WRITE ERROR: {rfs_filename} - {e}")
                if temp_file:
                    try:
                        os.remove(temp_file)
                    except Exception:
                        pass
                return {'status': 'error', 'message': str(e)}

    def _handle_read_rfs_eo(self, msg: dict) -> dict:
        """
        Read exactly-once log data from ReplicatedFS for task recovery.

        This is called when a task starts up and needs to recover its EO state
        from ReplicatedFS (for cross-node recovery scenarios).
        """
        rfs_filename = msg.get('filename')
        task_id = msg.get('task_id', 'unknown')

        if not rfs_filename:
            return {'status': 'error', 'message': 'Missing filename'}

        try:
            # Try to get the file from ReplicatedFS
            temp_file = f"/tmp/rfs_eo_read_{task_id}_{time.time()}.tmp"

            self.rfs_client.get(rfs_filename, temp_file)

            if os.path.exists(temp_file) and os.path.getsize(temp_file) > 0:
                with open(temp_file, 'r') as f:
                    data = f.read()
                os.remove(temp_file)
                self.logger.log(f"RFS EO READ: {rfs_filename} for task {task_id} ({len(data)} bytes)")
                return {'status': 'success', 'data': data}
            else:
                if os.path.exists(temp_file):
                    os.remove(temp_file)
                return {'status': 'error', 'message': 'File not found or empty'}

        except Exception as e:
            self.logger.log(f"RFS EO READ ERROR: {rfs_filename} - {e}")
            try:
                if os.path.exists(temp_file):
                    os.remove(temp_file)
            except Exception:
                pass
            return {'status': 'error', 'message': str(e)}

    def _send_to_worker(self, hostname: str, msg: dict, timeout: float = 10.0) -> Optional[dict]:
        """Send message to a worker node."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.settimeout(timeout)
            sock.connect((hostname, get_stream_port_by_host(hostname)))
            sock.sendall(json.dumps(msg).encode('utf-8'))

            # Read response in chunks for large responses
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
                        return result
                    except json.JSONDecodeError:
                        # Incomplete JSON, keep reading
                        continue
                except socket.timeout:
                    break

            if chunks:
                full_response = b''.join(chunks).decode('utf-8')
                return json.loads(full_response)
            return None
        except Exception as e:
            self.logger.log(f"LEADER ERROR sending to {hostname}: {e}")
            return None
        finally:
            sock.close()

    def _cleanup_existing_tasks(self):
        """Kill all existing tasks from previous jobs to free up ports."""
        self.logger.log("CLEANUP: Starting cleanup for new job")

        # Stop monitor threads from previous job to prevent interference
        # We do this by setting job_running=False which the monitor loop checks
        self.job_running = False

        # Wait a bit for monitor threads to notice and stop their task checking
        time.sleep(0.5)

        with self.tasks_lock:
            if self.tasks:
                self.logger.log(f"CLEANUP: Killing {len(self.tasks)} existing tasks from previous job")

                # Group tasks by node for efficient cleanup
                tasks_by_vm: Dict[int, List[TaskInfo]] = {}
                for task_id, task in self.tasks.items():
                    if task.vm_id not in tasks_by_vm:
                        tasks_by_vm[task.vm_id] = []
                    tasks_by_vm[task.vm_id].append(task)

                # Kill tasks on each node
                for vm_id, vm_tasks in tasks_by_vm.items():
                    for task in vm_tasks:
                        if task.pid > 0:
                            try:
                                if vm_id == self.vm_id:
                                    # Local task
                                    os.kill(task.pid, 9)
                                    self.logger.log(f"CLEANUP: Killed local task {task.task_id} PID={task.pid}")
                                else:
                                    # Remote task - send kill command
                                    hostname = NODE_HOSTS[vm_id]
                                    kill_msg = {'type': 'KILL_PROCESS', 'pid': task.pid}
                                    self._send_to_worker(hostname, kill_msg)
                                    self.logger.log(f"CLEANUP: Killed remote task {task.task_id} on Node-{vm_id} PID={task.pid}")
                            except Exception as e:
                                self.logger.log(f"CLEANUP: Failed to kill {task.task_id}: {e}")

            # ALWAYS clear task tracking structures (even if tasks dict was empty)
            # This ensures stale rates from completed jobs are cleared
            self.tasks.clear()
            self.stage_tasks.clear()
            self.final_stage_tasks.clear()

        # Clear rates outside of tasks_lock to avoid potential deadlock with rates_lock
        with self.rates_lock:
            old_rate_count = len(self.task_rates)
            self.task_rates.clear()
            if old_rate_count > 0:
                self.logger.log(f"CLEANUP: Cleared {old_rate_count} stale task rate entries")

        # Clear ReplicatedFS files created tracking - CRITICAL for new jobs to CREATE not APPEND
        self._rfs_files_created.clear()
        self._eo_rfs_files_created.clear()

        # Give processes time to terminate and release ports
        time.sleep(1.0)
        self.logger.log("CLEANUP: Cleanup complete")

    def start_job(self, nstages: int, ntasks_per_stage: int,
                  operators: List[Tuple[str, str]], rfs_src: str,
                  rfs_dest: str, exactly_once: bool, autoscale_enabled: bool,
                  input_rate: int = 100, lw: int = 50, hw: int = 150):
        """Start a StreamProcess job."""
        # IMPORTANT: Kill any existing tasks from previous jobs first
        # This ensures ports are free for new tasks
        self._cleanup_existing_tasks()

        # Generate new run_id for each job to ensure fresh state
        self.run_id = f"{int(time.time())}"

        self.job_running = True
        self.logger.log(f"RUN START: stages={nstages} tasks_per_stage={ntasks_per_stage} run_id={self.run_id}")
        self.logger.log(f"  src={rfs_src} dest={rfs_dest}")
        self.logger.log(f"  exactly_once={exactly_once} autoscale={autoscale_enabled}")
        
        self.job_config = {
            'nstages': nstages,
            'ntasks_per_stage': ntasks_per_stage,
            'operators': operators,
            'rfs_src': rfs_src,
            'rfs_dest': rfs_dest
        }
        
        self.exactly_once = exactly_once
        self.autoscale_enabled = autoscale_enabled
        self.input_rate = input_rate
        self.lw = lw
        self.hw = hw
        
        # Setup stages
        self.stages = []
        for i, (op_exe, op_args) in enumerate(operators):
            self.stages.append({
                'op_exe': op_exe,
                'op_args': op_args,
                'num_tasks': ntasks_per_stage
            })
            self.stage_tasks[i] = []
        
        # Get available workers (should already be joined)
        workers = self._get_available_workers()
        
        if len(workers) < 1:
            self.logger.log("ERROR: No workers available. Make sure workers have joined.")
            return
        
        self.logger.log(f"Available workers: {workers}")
        
        # Schedule tasks across workers
        self._schedule_tasks(workers)

        # Wait for all tasks to be ready before starting source
        self._wait_for_tasks_ready()

        # Start monitoring threads
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()

        self.rate_log_thread = threading.Thread(target=self._rate_log_loop, daemon=True)
        self.rate_log_thread.start()

        # Start the source on the leader
        self._start_source(rfs_src)
    
    def _get_available_workers(self, include_leader: bool = True) -> List[int]:
        """Get list of available worker node IDs."""
        workers = []
        members = self.rfs_node.membership.membership.get_members()
        
        for hostname, info in members.items():
            if info['status'] == MemberStatus.ACTIVE.value:
                vm_id = get_node_id_from_hostname(hostname)
                workers.append(vm_id)
        
        # If no other workers found, at least include the leader
        if not workers:
            workers = [self.vm_id]
        
        return sorted(workers)
    
    def _schedule_tasks(self, workers: List[int]):
        """Schedule tasks across available workers."""
        worker_idx = 0
        num_workers = len(workers)
        
        for stage_idx, stage_config in enumerate(self.stages):
            num_tasks = stage_config['num_tasks']
            op_exe = stage_config['op_exe']
            op_args = stage_config['op_args']
            
            for task_idx in range(num_tasks):
                vm_id = workers[worker_idx % num_workers]
                worker_idx += 1

                # Include run_id in task_id for unique state files per job
                task_id = f"stage{stage_idx}_task{task_idx}_{self.run_id}"
                
                task_info = TaskInfo(
                    task_id=task_id,
                    stage=stage_idx,
                    task_idx=task_idx,
                    vm_id=vm_id,
                    pid=0,
                    op_exe=op_exe,
                    op_args=op_args,
                    log_file=""
                )
                
                with self.tasks_lock:
                    self.tasks[task_id] = task_info
                    self.stage_tasks[stage_idx].append(task_id)

                # Track final stage tasks for output collection
                if stage_idx == len(self.stages) - 1:
                    self.final_stage_tasks.append({
                        'task_id': task_id,
                        'vm_id': vm_id,
                        'hostname': NODE_HOSTS[vm_id]
                    })

                self.logger.log(f"SCHEDULED: {task_id} on Node-{vm_id}")

        # Send start commands to workers
        for task_id, task in self.tasks.items():
            self._start_task_on_worker(task)

    def _wait_for_tasks_ready(self):
        """Wait for all tasks to be ready (listening on their ports)."""
        self.logger.log("Waiting for tasks to be ready...")

        max_wait = 30.0  # Maximum wait time in seconds (increased for ReplicatedFS initialization)
        start_time = time.time()

        while time.time() - start_time < max_wait:
            all_ready = True
            with self.tasks_lock:
                for task_id, task in self.tasks.items():
                    if task.pid == 0:
                        all_ready = False
                        break
                    # Try to connect to task port to verify it's listening
                    hostname = NODE_HOSTS[task.vm_id]
                    port = get_task_port(task.stage, task.task_idx)
                    try:
                        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sock.settimeout(0.5)
                        sock.connect((hostname, port))
                        sock.close()
                    except Exception:
                        all_ready = False
                        break

            if all_ready:
                self.logger.log("All tasks ready")
                return

            time.sleep(0.2)

        self.logger.log("WARNING: Timeout waiting for tasks, proceeding anyway")

    def _start_task_on_worker(self, task: TaskInfo):
        """Start a task on its assigned worker."""
        hostname = NODE_HOSTS[task.vm_id]
        
        # If task is on leader, start it locally
        if task.vm_id == self.vm_id:
            self._start_task_locally(task)
            return
        
        msg = {
            'type': 'START_TASK',
            'task_id': task.task_id,
            'stage': task.stage,
            'task_idx': task.task_idx,
            'op_exe': task.op_exe,
            'op_args': task.op_args,
            'exactly_once': self.exactly_once,
            'leader_host': self.hostname,
            'run_id': self.run_id
        }
        
        response = self._send_to_worker(hostname, msg)
        if response and response.get('status') == 'success':
            # Update task info with PID and log_file from worker
            task.pid = response.get('pid', 0)
            task.log_file = response.get('log_file', f"task_{task.task_id}_{self.run_id}.log")
            self.logger.log(f"TASK STARTED: {task.task_id} on Node-{task.vm_id} PID={task.pid}")
        else:
            self.logger.log(f"TASK START FAILED: {task.task_id} - {response}")
    
    def _start_task_locally(self, task: TaskInfo):
        """Start a task locally on the leader."""
        import subprocess

        task_port = get_task_port(task.stage, task.task_idx)
        log_file = f"task_{task.task_id}_{self.run_id}.log"
        
        cmd = [
            sys.executable, 'task.py',
            '--task-id', task.task_id,
            '--stage', str(task.stage),
            '--task-idx', str(task.task_idx),
            '--port', str(task_port),
            '--op-exe', task.op_exe,
            '--op-args', task.op_args,
            '--leader-host', self.hostname,
            '--run-id', self.run_id,
            '--vm-id', str(self.vm_id)
        ]
        
        if self.exactly_once:
            cmd.append('--exactly-once')
        
        process = subprocess.Popen(
            cmd,
            stdout=open(log_file, 'w'),
            stderr=subprocess.STDOUT,
            cwd=os.path.dirname(os.path.abspath(__file__))
        )
        
        task.pid = process.pid
        task.log_file = log_file
        
        self.logger.log(f"TASK STARTED LOCAL: {task.task_id} PID={process.pid}")
    
    def _start_source(self, rfs_src: str):
        """Start the source process to read from ReplicatedFS."""
        self.logger.log(f"SOURCE START: Reading from {rfs_src}")
        
        # Start source in a separate thread
        source_thread = threading.Thread(
            target=self._source_loop,
            args=(rfs_src,),
            daemon=True
        )
        source_thread.start()
    
    def _get_stage0_tasks(self) -> List[dict]:
        """Get current stage 0 task info (refreshes on each call)."""
        stage0_tasks = []
        with self.tasks_lock:
            for task_id in self.stage_tasks.get(0, []):
                if task_id in self.tasks:
                    t = self.tasks[task_id]
                    stage0_tasks.append({
                        'task_id': task_id,
                        'hostname': NODE_HOSTS[t.vm_id],
                        'port': get_task_port(t.stage, t.task_idx)
                    })
        return stage0_tasks

    def _source_loop(self, rfs_src: str):
        """
        FIXED: Source loop with ACK tracking for exactly-once semantics.

        Key improvements:
        1. Refreshes target task info on each send (handles task restarts)
        2. Retries pending tuples periodically during sending, not just at the end
        3. Uses fresh target info when retrying
        """
        try:
            # Try to read from local file first
            local_path = rfs_src
            if not os.path.exists(local_path):
                temp_file = f"/tmp/streamprocess_src_{self.run_id}.csv"
                self.rfs_client.get(rfs_src, temp_file)
                local_path = temp_file

            if not os.path.exists(local_path):
                self.logger.log(f"SOURCE ERROR: Cannot find {rfs_src}")
                return

            with open(local_path, 'r') as f:
                lines = f.readlines()

            self.logger.log(f"SOURCE: Loaded {len(lines)} lines")

            # Verify we have stage 0 tasks
            stage0_tasks = self._get_stage0_tasks()
            if not stage0_tasks:
                self.logger.log("SOURCE ERROR: No stage 0 tasks available")
                return

            # Track pending and acked tuples for exactly-once
            pending_tuples: Dict[str, dict] = {}  # tuple_id -> {msg, key, time, retries}
            acked_tuples: Set[str] = set()

            # Send tuples at input_rate
            interval = 1.0 / self.input_rate if self.input_rate > 0 else 0.01
            last_retry_time = time.time()
            retry_interval = 2.0  # Retry pending tuples every 2 seconds

            # Store current run_id to detect if a new job started
            my_run_id = self.run_id

            for line_num, line in enumerate(lines):
                # Stop if server is shutting down OR if a new job started
                if not self.running or self.run_id != my_run_id:
                    self.logger.log(f"SOURCE: Stopping early (server running={self.running}, run_id changed={self.run_id != my_run_id})")
                    return  # Exit entirely, don't send EOF for old job

                # Create tuple
                key = f"{rfs_src}:{line_num}"
                value = line.strip()
                tuple_id = f"{my_run_id}_{line_num}"

                tuple_msg = {
                    'type': 'TUPLE',
                    'tuple_id': tuple_id,
                    'key': key,
                    'value': value,
                    'source_task': 'source'
                }

                # FIXED: Get fresh target info each time (handles task restarts)
                stage0_tasks = self._get_stage0_tasks()
                if not stage0_tasks:
                    self.logger.log("SOURCE ERROR: No stage 0 tasks available mid-stream")
                    pending_tuples[tuple_id] = {
                        'msg': tuple_msg,
                        'key': key,
                        'time': time.time(),
                        'retries': 0
                    }
                    continue

                # Hash partition to determine target task
                task_idx = hash(key) % len(stage0_tasks)
                target = stage0_tasks[task_idx]

                # Send with ACK for exactly-once
                if self.exactly_once:
                    success = self._send_tuple_with_ack(target, tuple_msg)
                    if success:
                        acked_tuples.add(tuple_id)
                    else:
                        # Track for retry (store key for re-hashing, not stale target)
                        pending_tuples[tuple_id] = {
                            'msg': tuple_msg,
                            'key': key,
                            'time': time.time(),
                            'retries': 0
                        }
                        self.logger.log(f"SOURCE: Tuple {tuple_id} pending (no ACK)")
                else:
                    # Fire-and-forget for non-exactly-once
                    self._send_tuple(target['hostname'], target['port'], tuple_msg)

                # FIXED: Periodically retry pending tuples DURING sending
                if self.exactly_once and pending_tuples and (time.time() - last_retry_time) > retry_interval:
                    self._retry_pending_tuples(pending_tuples, acked_tuples)
                    last_retry_time = time.time()

                time.sleep(interval)

            # Check if new job started before doing final retries
            if self.run_id != my_run_id:
                self.logger.log(f"SOURCE: New job started, abandoning retries for run {my_run_id}")
                return

            # Final retry of any remaining pending tuples
            if self.exactly_once and pending_tuples:
                self.logger.log(f"SOURCE: Final retry of {len(pending_tuples)} pending tuples")
                # FIXED: Increased max retry rounds to handle task restart delays
                max_retry_rounds = 30
                retry_round = 0

                while pending_tuples and retry_round < max_retry_rounds:
                    # Check if new job started
                    if self.run_id != my_run_id:
                        self.logger.log(f"SOURCE: New job started during retries, stopping")
                        return

                    retry_round += 1
                    self.logger.log(f"SOURCE: Retry round {retry_round}/{max_retry_rounds}, {len(pending_tuples)} pending")
                    self._retry_pending_tuples(pending_tuples, acked_tuples)

                    if pending_tuples:
                        time.sleep(2.0)  # Wait 2 seconds before next retry round

                if pending_tuples:
                    self.logger.log(f"SOURCE WARNING: {len(pending_tuples)} tuples never acked after {max_retry_rounds} rounds")
                    for tid in list(pending_tuples.keys())[:10]:  # Log first 10 for debugging
                        self.logger.log(f"  LOST: {tid} (retries={pending_tuples[tid]['retries']})")

            # Check again before sending EOF
            if self.run_id != my_run_id:
                self.logger.log(f"SOURCE: New job started, not sending EOF for old job")
                return

            # Send EOF to all stage 0 tasks (with fresh task info)
            stage0_tasks = self._get_stage0_tasks()
            for target in stage0_tasks:
                eof_msg = {'type': 'EOF', 'source_task': 'source'}
                # Retry EOF until it succeeds
                for attempt in range(5):
                    if self.run_id != my_run_id:
                        return  # New job started
                    if self.exactly_once:
                        success = self._send_tuple_with_ack(target, eof_msg)
                        if success:
                            break
                        time.sleep(0.5)
                    else:
                        self._send_tuple(target['hostname'], target['port'], eof_msg)
                        break

            self.logger.log(f"SOURCE: Finished sending all tuples")
            self.logger.log(f"SOURCE STATS: Total lines={len(lines)}, Acked={len(acked_tuples)}, Still pending={len(pending_tuples)}")

        except Exception as e:
            self.logger.log(f"SOURCE ERROR: {e}")
            import traceback
            self.logger.log(f"SOURCE TRACEBACK: {traceback.format_exc()}")

    def _retry_pending_tuples(self, pending_tuples: Dict[str, dict], acked_tuples: Set[str]):
        """Retry pending tuples with fresh target info."""
        stage0_tasks = self._get_stage0_tasks()
        if not stage0_tasks:
            return

        for tuple_id in list(pending_tuples.keys()):
            info = pending_tuples[tuple_id]
            # FIXED: Increased retry limit to handle task restart delays
            # Task restart can take 5-10 seconds, and we retry every 2 seconds
            if info['retries'] >= 30:
                self.logger.log(f"SOURCE: Giving up on {tuple_id} after {info['retries']} retries")
                continue  # Give up after 30 retries

            info['retries'] += 1
            info['time'] = time.time()

            # FIXED: Re-hash to get fresh target (handles task restarts/moves)
            key = info['key']
            task_idx = hash(key) % len(stage0_tasks)
            target = stage0_tasks[task_idx]

            success = self._send_tuple_with_ack(target, info['msg'])
            if success:
                acked_tuples.add(tuple_id)
                del pending_tuples[tuple_id]
                self.logger.log(f"SOURCE: Retry succeeded for {tuple_id}")


    def _send_tuple_with_ack(self, target: dict, msg: dict) -> bool:
        """
        FIXED: Send a tuple to a task and wait for ACK.

        Returns True if ACK received, False otherwise.
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.settimeout(5.0)
            sock.connect((target['hostname'], target['port']))
            sock.sendall(json.dumps(msg).encode('utf-8'))

            # Wait for ACK
            response = sock.recv(1024).decode('utf-8')

            if 'ack' in response.lower():
                return True
            return False
        except socket.timeout:
            self.logger.log(f"SOURCE: Timeout waiting for ACK from {target.get('task_id', target['hostname'])}")
            return False
        except Exception as e:
            self.logger.log(f"SOURCE: Error sending to {target.get('task_id', target['hostname'])}: {e}")
            return False
        finally:
            sock.close()
    
    def _send_tuple(self, hostname: str, port: int, msg: dict):
        """Send a tuple to a task (fire-and-forget, for non-exactly-once mode)."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.settimeout(5.0)
            sock.connect((hostname, port))
            sock.sendall(json.dumps(msg).encode('utf-8'))
        except Exception:
            pass  # Silently ignore in fire-and-forget mode
        finally:
            sock.close()
    
    def _monitor_loop(self):
        """Monitor tasks for failures and autoscaling."""
        self.logger.log("MONITOR: Starting monitor loop")
        check_count = 0
        my_run_id = self.run_id  # Capture run_id at start
        while self.running and self.job_running:
            try:
                time.sleep(1.0)

                # Exit if job changed (new job started)
                if self.run_id != my_run_id:
                    self.logger.log(f"MONITOR: Exiting (run_id changed from {my_run_id} to {self.run_id})")
                    break

                check_count += 1

                # Check for failed tasks (log every 10th check to avoid spam)
                if check_count % 10 == 0:
                    self.logger.log(f"MONITOR: Checking tasks (check #{check_count})")
                self._check_task_failures()

                # Autoscaling
                if self.autoscale_enabled:
                    self._check_autoscaling()

            except Exception as e:
                self.logger.log(f"MONITOR ERROR: {e}")
                import traceback
                self.logger.log(f"MONITOR TRACEBACK: {traceback.format_exc()}")

        self.logger.log("MONITOR: Monitor loop exited")
    
    def _check_task_failures(self):
        """Check for failed tasks and restart them."""
        failed_tasks = []

        tasks_to_check = []
        with self.tasks_lock:
            tasks_to_check = [(task_id, task) for task_id, task in self.tasks.items() if task.pid > 0]

        for task_id, task in tasks_to_check:
            # Check if task is still running
            hostname = NODE_HOSTS[task.vm_id]
            msg = {'type': 'CHECK_TASK', 'pid': task.pid}
            response = self._send_to_worker(hostname, msg, timeout=2.0)

            # Debug: log every check response
            self.logger.log(f"CHECK_TASK: {task_id} pid={task.pid} on {hostname} -> {response}")

            # Task failed if: no response (worker unreachable) OR task not running
            if response is None:
                self.logger.log(f"TASK FAILURE DETECTED: {task_id} on Node-{task.vm_id} (no response from worker)")
                failed_tasks.append(task)
            elif not response.get('running', True):
                self.logger.log(f"TASK FAILURE DETECTED: {task_id} on Node-{task.vm_id} (process not running)")
                failed_tasks.append(task)

        # Restart failed tasks outside the lock
        for task in failed_tasks:
            self._restart_task(task)

    def _restart_task(self, task: TaskInfo):
        """Restart a failed task, possibly on a different node if original is down."""
        original_vm = task.vm_id

        # Check if original node is still available
        workers = self._get_available_workers()
        if original_vm not in workers:
            # Original VM is down, pick a new one
            if workers:
                new_vm = workers[0]  # Pick first available
                self.logger.log(f"TASK RESTART: {task.task_id} moving from Node-{original_vm} to Node-{new_vm}")
                task.vm_id = new_vm
            else:
                self.logger.log(f"TASK RESTART FAILED: No available workers for {task.task_id}")
                return
        else:
            self.logger.log(f"TASK RESTART: {task.task_id} on Node-{task.vm_id}")

        # Re-start the task (preserves task_id for exactly-once recovery)
        self._start_task_on_worker(task)
    
    def _check_autoscaling(self):
        """Check if autoscaling is needed."""
        # Cooldown: don't scale if we recently scaled
        current_time = time.time()
        if not hasattr(self, '_last_scale_time'):
            self._last_scale_time = 0
        if current_time - self._last_scale_time < 5.0:  # 5 second cooldown
            return

        # Check each non-stateful stage (skip stage 0 - source handles it separately)
        for stage_idx in range(1, len(self.stages)):  # Start from stage 1
            stage_config = self.stages[stage_idx]

            # Skip stateful stages (aggregation)
            if 'aggregate' in stage_config['op_exe'].lower() or 'count' in stage_config['op_exe'].lower():
                continue

            # Calculate average rate for this stage
            task_ids = self.stage_tasks.get(stage_idx, [])
            if not task_ids:
                continue

            with self.rates_lock:
                rates = [self.task_rates.get(tid, 0) for tid in task_ids]

            if not rates:
                continue

            # Filter out zero rates (task might be starting up)
            non_zero_rates = [r for r in rates if r > 0]
            if not non_zero_rates:
                continue

            avg_rate = sum(non_zero_rates) / len(non_zero_rates)

            # Check watermarks
            if avg_rate > self.hw and len(task_ids) < 10:
                self.logger.log(f"AUTOSCALE CHECK: stage={stage_idx} avg_rate={avg_rate:.2f} > hw={self.hw}, scaling UP")
                self._scale_up(stage_idx, avg_rate)
                self._last_scale_time = current_time
            elif avg_rate < self.lw and len(task_ids) > 1:
                self.logger.log(f"AUTOSCALE CHECK: stage={stage_idx} avg_rate={avg_rate:.2f} < lw={self.lw}, scaling DOWN")
                self._scale_down(stage_idx, avg_rate)
                self._last_scale_time = current_time
    
    def _scale_up(self, stage_idx: int, current_rate: float):
        """Add a task to a stage."""
        self.logger.log(f"AUTOSCALE UP: stage={stage_idx} rate={current_rate:.2f} tuples/sec")

        workers = self._get_available_workers()
        if not workers:
            return

        # Find least loaded worker (simple round-robin for now)
        with self.tasks_lock:
            # Count tasks per worker
            worker_task_counts = {w: 0 for w in workers}
            for task in self.tasks.values():
                if task.vm_id in worker_task_counts:
                    worker_task_counts[task.vm_id] += 1
            # Pick worker with fewest tasks
            vm_id = min(worker_task_counts, key=worker_task_counts.get)

        stage_config = self.stages[stage_idx]
        task_idx = len(self.stage_tasks[stage_idx])
        # Include run_id in task_id for unique state files per job
        task_id = f"stage{stage_idx}_task{task_idx}_{self.run_id}"

        task_info = TaskInfo(
            task_id=task_id,
            stage=stage_idx,
            task_idx=task_idx,
            vm_id=vm_id,
            pid=0,
            op_exe=stage_config['op_exe'],
            op_args=stage_config['op_args'],
            log_file=""
        )

        with self.tasks_lock:
            self.tasks[task_id] = task_info
            self.stage_tasks[stage_idx].append(task_id)

        # Track final stage tasks for output collection
        if stage_idx == len(self.stages) - 1:
            self.final_stage_tasks.append({
                'task_id': task_id,
                'vm_id': vm_id,
                'hostname': NODE_HOSTS[vm_id]
            })

        self._start_task_on_worker(task_info)

        # Wait for new task to be ready before notifying predecessors
        task_port = get_task_port(stage_idx, task_idx)
        hostname = NODE_HOSTS[vm_id]
        max_wait = 5.0
        start_time = time.time()
        task_ready = False

        while time.time() - start_time < max_wait:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(0.5)
                sock.connect((hostname, task_port))
                sock.close()
                task_ready = True
                break
            except Exception:
                time.sleep(0.2)

        if not task_ready:
            self.logger.log(f"AUTOSCALE WARNING: New task {task_id} not ready, proceeding anyway")

        # Notify predecessor tasks to update their successor list
        self._notify_predecessor_tasks_of_scale(stage_idx)

        self.logger.log(f"AUTOSCALE UP COMPLETE: stage={stage_idx} now has {len(self.stage_tasks[stage_idx])} tasks")

    def _scale_down(self, stage_idx: int, current_rate: float):
        """Remove a task from a stage."""
        self.logger.log(f"AUTOSCALE DOWN: stage={stage_idx} rate={current_rate:.2f} tuples/sec")

        task_ids = self.stage_tasks[stage_idx]
        if len(task_ids) <= 1:
            return

        # Remove last task
        task_id = task_ids[-1]

        with self.tasks_lock:
            if task_id not in self.tasks:
                return
            task = self.tasks[task_id]
            hostname = NODE_HOSTS[task.vm_id]
            task_port = get_task_port(task.stage, task.task_idx)

            # Step 1: Remove from stage_tasks FIRST so predecessors stop sending to it
            self.stage_tasks[stage_idx].remove(task_id)

        # Remove from final_stage_tasks if this is the final stage
        if stage_idx == len(self.stages) - 1:
            self.final_stage_tasks = [t for t in self.final_stage_tasks if t['task_id'] != task_id]

        # Step 2: Notify predecessors to update their successor list (excludes removed task)
        self._notify_predecessor_tasks_of_scale(stage_idx)

        # Step 3: Give time for in-flight tuples to be processed
        self.logger.log(f"AUTOSCALE DOWN: Draining task {task_id}...")
        time.sleep(2.0)  # Allow in-flight tuples to complete

        # Step 4: Now kill the task
        with self.tasks_lock:
            if task_id in self.tasks:
                kill_msg = {'type': 'KILL_PROCESS', 'pid': task.pid}
                self._send_to_worker(hostname, kill_msg)
                del self.tasks[task_id]

        self.logger.log(f"AUTOSCALE DOWN COMPLETE: stage={stage_idx} now has {len(self.stage_tasks[stage_idx])} tasks")

    def _notify_predecessor_tasks_of_scale(self, stage_idx: int):
        """Notify predecessor stage tasks that successor list has changed."""
        if stage_idx == 0:
            # Stage 0 predecessors are source, handled separately
            return

        pred_stage = stage_idx - 1
        pred_task_ids = self.stage_tasks.get(pred_stage, [])

        for pred_task_id in pred_task_ids:
            with self.tasks_lock:
                if pred_task_id not in self.tasks:
                    continue
                pred_task = self.tasks[pred_task_id]
                hostname = NODE_HOSTS[pred_task.vm_id]

            # Send UPDATE_SUCCESSORS message to predecessor task
            successor_tasks = []
            with self.tasks_lock:
                for tid in self.stage_tasks.get(stage_idx, []):
                    if tid in self.tasks:
                        t = self.tasks[tid]
                        successor_tasks.append({
                            'task_id': tid,
                            'vm_id': t.vm_id,
                            'hostname': NODE_HOSTS[t.vm_id],
                            'port': get_task_port(t.stage, t.task_idx)
                        })

            msg = {
                'type': 'UPDATE_SUCCESSORS',
                'successor_tasks': successor_tasks
            }

            # Send directly to task port
            try:
                port = get_task_port(pred_task.stage, pred_task.task_idx)
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(5.0)
                sock.connect((hostname, port))
                sock.sendall(json.dumps(msg).encode('utf-8'))
                sock.close()
                self.logger.log(f"AUTOSCALE: Notified {pred_task_id} of successor change")
            except Exception as e:
                self.logger.log(f"AUTOSCALE: Failed to notify {pred_task_id}: {e}")
    
    def _rate_log_loop(self):
        """Log rates every second while job is running."""
        my_run_id = self.run_id  # Capture run_id at start
        while self.running and self.job_running:
            try:
                time.sleep(1.0)

                # Exit if job changed (new job started)
                if self.run_id != my_run_id:
                    break

                # Only log rates while job is actively running
                if not self.job_running:
                    continue

                with self.rates_lock:
                    for task_id, rate in self.task_rates.items():
                        self.logger.log(f"RATE: {task_id} {rate:.2f} tuples/sec")

            except Exception as e:
                pass

    def _collect_outputs_to_rfs(self):
        """Collect output files from final stage tasks and store in ReplicatedFS."""
        if not self.job_config:
            return

        rfs_dest = self.job_config.get('rfs_dest', '')
        if not rfs_dest:
            self.logger.log("No ReplicatedFS destination specified, skipping output collection")
            return

        self.logger.log(f"Collecting outputs to ReplicatedFS: {rfs_dest}")

        # Give tasks a moment to finish writing
        time.sleep(1.0)

        # Collect output files from each final stage task
        combined_output = []

        # Use absolute path in StreamProcess directory
        task_dir = os.path.dirname(os.path.abspath(__file__))

        for task_info in self.final_stage_tasks:
            task_id = task_info['task_id']
            vm_id = task_info['vm_id']
            hostname = task_info['hostname']
            output_file = os.path.join(task_dir, f"output_{task_id}.txt")

            self.logger.log(f"Fetching output from {task_id} on Node-{vm_id}")

            if vm_id == self.vm_id:
                # Local task - read directly
                try:
                    with open(output_file, 'r') as f:
                        lines = f.readlines()
                        combined_output.extend(lines)
                        self.logger.log(f"  Read {len(lines)} lines from local {output_file}")
                except FileNotFoundError:
                    self.logger.log(f"  No output file found: {output_file}")
                except Exception as e:
                    self.logger.log(f"  Error reading {output_file}: {e}")
            else:
                # Remote task - request output via worker
                try:
                    msg = {'type': 'GET_OUTPUT', 'task_id': task_id}
                    response = self._send_to_worker(hostname, msg, timeout=30.0)

                    if response and response.get('status') == 'success':
                        lines = response.get('output', [])
                        combined_output.extend([line + '\n' if not line.endswith('\n') else line for line in lines])
                        self.logger.log(f"  Received {len(lines)} lines from Node-{vm_id}")
                    else:
                        self.logger.log(f"  Failed to get output from Node-{vm_id}: {response}")
                except Exception as e:
                    self.logger.log(f"  Error fetching from Node-{vm_id}: {e}")

        # Write combined output to temp file
        if combined_output:
            temp_file = f"/tmp/streamprocess_combined_{self.run_id}.txt"
            try:
                with open(temp_file, 'w') as f:
                    f.writelines(combined_output)

                # Create in ReplicatedFS
                self.rfs_client.create(temp_file, rfs_dest)
                self.logger.log(f"OUTPUT SAVED: {len(combined_output)} lines to ReplicatedFS:{rfs_dest}")

                # Cleanup
                os.remove(temp_file)
            except Exception as e:
                self.logger.log(f"ERROR saving to ReplicatedFS: {e}")
        else:
            self.logger.log("No output to save (0 tuples produced)")

        # Clear final stage tasks for next job
        self.final_stage_tasks.clear()


class StreamProcessWorker:
    """
    StreamProcess Worker - runs on non-leader nodes.
    Receives commands from leader and manages local tasks.
    """

    def __init__(self, vm_id: int, rfs_node: ReplicatedFSNode):
        self.vm_id = vm_id
        self.hostname = socket.gethostname()
        self.rfs_node = rfs_node
        self.rfs_client = ReplicatedFSClient(rfs_node)  # Create ReplicatedFS client for output writing

        self.run_id = f"{int(time.time())}"
        self.logger = StreamProcessLogger(vm_id, self.run_id)

        self.running = False
        self.server_socket: Optional[socket.socket] = None
        self.task_processes: Dict[str, int] = {}  # task_id -> pid
        self.rfs_write_lock = threading.Lock()  # Lock for ReplicatedFS writes

        # Track which ReplicatedFS files have been created (per-instance, not class-level)
        self._rfs_files_created: set = set()
        self._eo_rfs_files_created: set = set()
    
    def start(self):
        """Start the worker server."""
        self.running = True
        
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.stream_port = get_stream_port(self.vm_id)
        self.server_socket.bind(('', self.stream_port))
        self.server_socket.listen(20)

        threading.Thread(target=self._server_loop, daemon=True).start()

        self.logger.log(f"WORKER: Started on port {self.stream_port}")
    
    def stop(self):
        """Stop the worker."""
        self.running = False
        if self.server_socket:
            self.server_socket.close()
    
    def _server_loop(self):
        """Main server loop."""
        while self.running:
            try:
                self.server_socket.settimeout(1.0)
                conn, addr = self.server_socket.accept()
                threading.Thread(target=self._handle_connection,
                               args=(conn, addr), daemon=True).start()
            except socket.timeout:
                continue
            except Exception as e:
                if self.running:
                    self.logger.log(f"WORKER ERROR: {e}")
    
    def _handle_connection(self, conn: socket.socket, addr: tuple):
        """Handle incoming connection."""
        try:
            data = conn.recv(65536).decode('utf-8')
            if not data:
                return

            msg = json.loads(data)
            msg_type = msg.get('type')

            response = {'status': 'error', 'message': 'Unknown message type'}

            if msg_type == 'START_TASK':
                response = self._handle_start_task(msg)
            elif msg_type == 'KILL_PROCESS':
                response = self._handle_kill_process(msg)
            elif msg_type == 'CHECK_TASK':
                response = self._handle_check_task(msg)
            elif msg_type == 'GET_OUTPUT':
                response = self._handle_get_output(msg)
            elif msg_type == 'WRITE_RFS':
                response = self._handle_write_rfs(msg)
            elif msg_type == 'WRITE_RFS_EO':
                # Handle exactly-once log writes from tasks
                response = self._handle_write_rfs_eo(msg)
            elif msg_type == 'READ_RFS_EO':
                # Handle exactly-once log reads from tasks (for recovery)
                response = self._handle_read_rfs_eo(msg)

            # Send full response, handling large payloads
            response_bytes = json.dumps(response).encode('utf-8')
            total_sent = 0
            while total_sent < len(response_bytes):
                sent = conn.send(response_bytes[total_sent:])
                if sent == 0:
                    break
                total_sent += sent
        except Exception as e:
            self.logger.log(f"WORKER ERROR: {e}")
        finally:
            conn.close()
    
    def _handle_start_task(self, msg: dict) -> dict:
        """Start a task process."""
        task_id = msg.get('task_id')
        stage = msg.get('stage')
        task_idx = msg.get('task_idx')
        op_exe = msg.get('op_exe')
        op_args = msg.get('op_args')
        exactly_once = msg.get('exactly_once', False)
        leader_host = msg.get('leader_host')
        run_id = msg.get('run_id')

        # Start task process
        import subprocess

        task_port = get_task_port(stage, task_idx)
        log_file = f"task_{task_id}_{run_id}.log"
        
        cmd = [
            sys.executable, 'task.py',
            '--task-id', task_id,
            '--stage', str(stage),
            '--task-idx', str(task_idx),
            '--port', str(task_port),
            '--op-exe', op_exe,
            '--op-args', op_args,
            '--leader-host', leader_host,
            '--run-id', run_id,
            '--vm-id', str(self.vm_id)
        ]
        
        if exactly_once:
            cmd.append('--exactly-once')
        
        process = subprocess.Popen(
            cmd,
            stdout=open(log_file, 'w'),
            stderr=subprocess.STDOUT,
            cwd=os.path.dirname(os.path.abspath(__file__))
        )
        
        self.task_processes[task_id] = process.pid
        
        self.logger.log(f"TASK STARTED: {task_id} PID={process.pid}")
        
        return {'status': 'success', 'pid': process.pid, 'log_file': log_file}
    
    def _handle_kill_process(self, msg: dict) -> dict:
        """Kill a process by PID."""
        pid = msg.get('pid')
        
        try:
            os.kill(pid, 9)
            self.logger.log(f"KILLED: PID={pid}")
            return {'status': 'success'}
        except Exception as e:
            return {'status': 'error', 'message': str(e)}
    
    def _handle_check_task(self, msg: dict) -> dict:
        """Check if a task is still running."""
        pid = msg.get('pid')

        try:
            os.kill(pid, 0)
            # Also check if process is a zombie (terminated but not reaped)
            # On Linux, we can check /proc/<pid>/status
            try:
                with open(f'/proc/{pid}/status', 'r') as f:
                    status = f.read()
                    if 'State:\tZ' in status:  # Zombie state
                        return {'status': 'success', 'running': False}
            except FileNotFoundError:
                # Process doesn't exist or /proc not available
                return {'status': 'success', 'running': False}
            except Exception:
                pass  # Fall through to return running=True
            return {'status': 'success', 'running': True}
        except OSError:
            return {'status': 'success', 'running': False}

    def _handle_get_output(self, msg: dict) -> dict:
        """Get output file contents for a task."""
        task_id = msg.get('task_id')
        # Use absolute path in StreamProcess directory
        task_dir = os.path.dirname(os.path.abspath(__file__))
        output_file = os.path.join(task_dir, f"output_{task_id}.txt")

        try:
            with open(output_file, 'r') as f:
                lines = f.read().splitlines()
            self.logger.log(f"GET_OUTPUT: {task_id} - {len(lines)} lines from {output_file}")
            return {'status': 'success', 'output': lines}
        except FileNotFoundError:
            self.logger.log(f"GET_OUTPUT: {task_id} - file not found: {output_file}")
            return {'status': 'success', 'output': []}
        except Exception as e:
            self.logger.log(f"GET_OUTPUT ERROR: {task_id} - {e}")
            return {'status': 'error', 'message': str(e)}

    def _check_rfs_file_exists(self, rfs_filename: str) -> bool:
        """Check if a file exists in ReplicatedFS by attempting a GET."""
        try:
            temp_check = f"/tmp/rfs_check_{time.time()}.tmp"
            # Try to get the file - if it succeeds, file exists
            self.rfs_client.get(rfs_filename, temp_check)
            exists = os.path.exists(temp_check) and os.path.getsize(temp_check) > 0
            # Cleanup
            if os.path.exists(temp_check):
                os.remove(temp_check)
            return exists
        except Exception:
            return False

    def _handle_write_rfs(self, msg: dict) -> dict:
        """
        Write data to ReplicatedFS on behalf of a task.
        Tasks send output data here, and worker uses its ReplicatedFS client.

        Strategy:
        1. If we've created the file locally before, just APPEND
        2. Otherwise, check if file exists in ReplicatedFS
           - If exists: APPEND
           - If not exists: CREATE
        """
        rfs_filename = msg.get('filename')
        data = msg.get('data')  # List of bytes or string
        task_id = msg.get('task_id', 'unknown')

        if not rfs_filename or data is None:
            return {'status': 'error', 'message': 'Missing filename or data'}

        with self.rfs_write_lock:
            temp_file = None
            try:
                # Write data to temp file
                temp_file = f"/tmp/rfs_write_{task_id}_{time.time()}.tmp"

                # Handle data as string or bytes
                if isinstance(data, str):
                    with open(temp_file, 'w') as f:
                        f.write(data)
                else:
                    with open(temp_file, 'wb') as f:
                        f.write(bytes(data))

                # Determine whether to CREATE or APPEND
                if rfs_filename in self._rfs_files_created:
                    # We've written to this file before - just append
                    self.rfs_client.append(temp_file, rfs_filename)
                    self.logger.log(f"RFS APPEND: {rfs_filename} from task {task_id}")
                else:
                    # First time we're writing to this file - check if it exists
                    if self._check_rfs_file_exists(rfs_filename):
                        # File exists (created by another worker) - append
                        self.rfs_client.append(temp_file, rfs_filename)
                        self.logger.log(f"RFS APPEND (file existed): {rfs_filename} from task {task_id}")
                    else:
                        # File doesn't exist - create it
                        self.rfs_client.create(temp_file, rfs_filename)
                        self.logger.log(f"RFS CREATE: {rfs_filename} from task {task_id}")

                    # Mark as created locally for future writes
                    self._rfs_files_created.add(rfs_filename)

                # Cleanup temp file
                os.remove(temp_file)

                return {'status': 'success'}

            except Exception as e:
                self.logger.log(f"RFS WRITE ERROR: {rfs_filename} - {e}")
                if temp_file:
                    try:
                        os.remove(temp_file)
                    except Exception:
                        pass
                return {'status': 'error', 'message': str(e)}

    def _handle_write_rfs_eo(self, msg: dict) -> dict:
        """
        Write exactly-once log data to ReplicatedFS on behalf of a task.

        This enables cross-node recovery: when a task restarts on a different node,
        it can load its EO log from ReplicatedFS (not just local file).

        Strategy:
        1. If we've created the file before, just APPEND
        2. Otherwise, check if file exists in ReplicatedFS
           - If exists: APPEND
           - If not exists: CREATE
        """
        rfs_filename = msg.get('filename')
        data = msg.get('data')  # String data
        operation = msg.get('operation', 'append')  # 'create' or 'append'
        task_id = msg.get('task_id', 'unknown')

        if not rfs_filename or data is None:
            return {'status': 'error', 'message': 'Missing filename or data'}

        with self.rfs_write_lock:
            temp_file = None
            try:
                # Write data to temp file
                temp_file = f"/tmp/rfs_eo_{task_id}_{time.time()}.tmp"

                with open(temp_file, 'w') as f:
                    f.write(data)

                if operation == 'create':
                    # Explicit create request
                    self.rfs_client.create(temp_file, rfs_filename)
                    self._eo_rfs_files_created.add(rfs_filename)
                    self.logger.log(f"RFS EO CREATE: {rfs_filename} from task {task_id}")
                else:
                    # Append operation - determine if file exists
                    if rfs_filename in self._eo_rfs_files_created:
                        # We've written to this file before - just append
                        self.rfs_client.append(temp_file, rfs_filename)
                        self.logger.log(f"RFS EO APPEND: {rfs_filename} from task {task_id}")
                    else:
                        # First time - check if file exists
                        if self._check_rfs_file_exists(rfs_filename):
                            self.rfs_client.append(temp_file, rfs_filename)
                            self.logger.log(f"RFS EO APPEND (existed): {rfs_filename} from task {task_id}")
                        else:
                            self.rfs_client.create(temp_file, rfs_filename)
                            self.logger.log(f"RFS EO CREATE (new): {rfs_filename} from task {task_id}")
                        self._eo_rfs_files_created.add(rfs_filename)

                # Cleanup temp file
                os.remove(temp_file)

                return {'status': 'success'}

            except Exception as e:
                self.logger.log(f"RFS EO WRITE ERROR: {rfs_filename} - {e}")
                if temp_file:
                    try:
                        os.remove(temp_file)
                    except Exception:
                        pass
                return {'status': 'error', 'message': str(e)}

    def _handle_read_rfs_eo(self, msg: dict) -> dict:
        """
        Read exactly-once log data from ReplicatedFS for task recovery.

        This is called when a task starts up and needs to recover its EO state
        from ReplicatedFS (for cross-node recovery scenarios).
        """
        rfs_filename = msg.get('filename')
        task_id = msg.get('task_id', 'unknown')

        if not rfs_filename:
            return {'status': 'error', 'message': 'Missing filename'}

        try:
            # Try to get the file from ReplicatedFS
            temp_file = f"/tmp/rfs_eo_read_{task_id}_{time.time()}.tmp"

            self.rfs_client.get(rfs_filename, temp_file)

            if os.path.exists(temp_file) and os.path.getsize(temp_file) > 0:
                with open(temp_file, 'r') as f:
                    data = f.read()
                os.remove(temp_file)
                self.logger.log(f"RFS EO READ: {rfs_filename} for task {task_id} ({len(data)} bytes)")
                return {'status': 'success', 'data': data}
            else:
                if os.path.exists(temp_file):
                    os.remove(temp_file)
                return {'status': 'error', 'message': 'File not found or empty'}

        except Exception as e:
            self.logger.log(f"RFS EO READ ERROR: {rfs_filename} - {e}")
            try:
                if os.path.exists(temp_file):
                    os.remove(temp_file)
            except Exception:
                pass
            return {'status': 'error', 'message': str(e)}


def submit_job(args):
    """Submit a job to the leader from a separate terminal."""
    # Parse operators
    operators = []
    for op in args.operators:
        parts = op.split(':')
        op_exe = parts[0]
        op_args = parts[1] if len(parts) > 1 else ''
        operators.append((op_exe, op_args))

    msg = {
        'type': 'SUBMIT_JOB',
        'nstages': args.nstages,
        'ntasks': args.ntasks or 3,
        'operators': operators,
        'src': args.src or '',
        'dest': args.dest or '',
        'exactly_once': args.exactly_once,
        'autoscale': args.autoscale,
        'input_rate': args.input_rate,
        'lw': args.lw,
        'hw': args.hw
    }

    leader_host = NODE_HOSTS.get(args.leader_vm, NODE_HOSTS[1])

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10.0)
        leader_port = get_stream_port(args.leader_vm)
        sock.connect((leader_host, leader_port))
        sock.sendall(json.dumps(msg).encode('utf-8'))
        response = sock.recv(65536).decode('utf-8')
        sock.close()

        result = json.loads(response)
        if result.get('status') == 'success':
            print(f"Job submitted successfully to {leader_host}")
        else:
            print(f"Job submission failed: {result.get('message')}")
    except Exception as e:
        print(f"Error submitting job: {e}")


def get_output(args):
    """Get output file from ReplicatedFS."""
    rfs_file = args.rfs_file
    local_file = args.local_file or f"local_{rfs_file}"

    # Create a temporary ReplicatedFS node to fetch the file
    vm_id = get_node_id_from_hostname(socket.gethostname())
    rfs_node = ReplicatedFSNode(vm_id)
    rfs_node.start()
    rfs_node.membership.join_group()
    time.sleep(2)

    rfs_client = ReplicatedFSClient(rfs_node)

    try:
        rfs_client.get(rfs_file, local_file)
        print(f"Downloaded {rfs_file} to {local_file}")

        # Print first 20 lines
        with open(local_file, 'r') as f:
            lines = f.readlines()
            print(f"\n=== Output ({len(lines)} total lines) ===")
            for line in lines[:20]:
                print(line.rstrip())
            if len(lines) > 20:
                print(f"... ({len(lines) - 20} more lines)")
    except Exception as e:
        print(f"Error fetching output: {e}")
    finally:
        rfs_node.membership.leave_group()
        rfs_node.stop()


def main():
    parser = argparse.ArgumentParser(description='StreamProcess Stream Processing')
    parser.add_argument('--vm-id', type=int, help='VM ID (1-10)')
    parser.add_argument('--leader', action='store_true', help='Run as leader')
    parser.add_argument('--submit', action='store_true', help='Submit job to leader')
    parser.add_argument('--get-output', action='store_true', help='Fetch output from ReplicatedFS')
    parser.add_argument('--leader-vm', type=int, default=1, help='Leader node ID for job submission')

    # Job parameters
    parser.add_argument('--nstages', type=int, help='Number of stages')
    parser.add_argument('--ntasks', type=int, help='Number of tasks per stage')
    parser.add_argument('--operators', nargs='+', help='op_exe:op_args pairs')
    parser.add_argument('--src', type=str, help='Source file')
    parser.add_argument('--dest', type=str, help='Destination file')
    parser.add_argument('--exactly-once', action='store_true')
    parser.add_argument('--autoscale', action='store_true')
    parser.add_argument('--input-rate', type=int, default=100)
    parser.add_argument('--lw', type=int, default=50)
    parser.add_argument('--hw', type=int, default=150)

    # Get output parameters
    parser.add_argument('--rfs-file', type=str, help='ReplicatedFS file to fetch')
    parser.add_argument('--local-file', type=str, help='Local file to save to')

    args = parser.parse_args()

    # Get output mode - fetch from ReplicatedFS
    if args.get_output:
        if not args.rfs_file:
            print("Error: --get-output requires --rfs-file")
            sys.exit(1)
        get_output(args)
        return

    # Submit mode - just send job to leader and exit
    if args.submit:
        if not args.nstages or not args.operators:
            print("Error: --submit requires --nstages and --operators")
            sys.exit(1)
        # Enforce mutual exclusivity of exactly-once and autoscale
        if args.exactly_once and args.autoscale:
            print("Error: --exactly-once and --autoscale cannot be used together")
            print("  exactly-once requires stable task configuration")
            print("  autoscale provides at-least-once semantics only")
            sys.exit(1)
        submit_job(args)
        return
    
    # Node mode - requires vm-id
    if args.vm_id is None:
        print("Error: --vm-id required for leader/worker mode")
        sys.exit(1)
    
    # Start ReplicatedFS node
    rfs_node = ReplicatedFSNode(args.vm_id)
    rfs_node.start()
    
    # Join membership
    rfs_node.membership.join_group()
    time.sleep(2)
    
    if args.leader:
        leader = StreamProcessLeader(args.vm_id, rfs_node)
        leader.start()
        
        print("Leader running. Waiting for job submissions...")
        print("Submit jobs from another terminal using:")
        print("  python3 streamprocess.py --submit --nstages N --ntasks M --operators ... --src FILE --dest FILE")
        print("Press Ctrl+C to stop.")
        
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        
        leader.stop()
    else:
        worker = StreamProcessWorker(args.vm_id, rfs_node)
        worker.start()
        
        print("Worker running. Press Ctrl+C to stop.")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        
        worker.stop()
    
    rfs_node.membership.leave_group()
    rfs_node.stop()


if __name__ == '__main__':
    main()