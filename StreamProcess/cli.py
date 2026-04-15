#!/usr/bin/env python3
"""
StreamProcess CLI - Command line interface for interacting with StreamProcess
Supports: list_tasks, kill_task, cat_output, get_output
"""
import argparse
import json
import os
import socket
import sys
import time


def _load_config() -> dict:
    """Load configuration from config.json."""
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.json')
    with open(config_path, 'r') as f:
        return json.load(f)

_CONFIG = _load_config()

# Build node host mapping from config
NODE_HOSTS = {}
NODE_RFS_PORTS = {}
NODE_STREAM_PORTS = {}
for _node in _CONFIG['nodes']:
    NODE_HOSTS[_node['id']] = _node['ip']
    NODE_RFS_PORTS[_node['id']] = _node['rfs_port']
    NODE_STREAM_PORTS[_node['id']] = _node['stream_port']

STREAM_PORT = _CONFIG['nodes'][0].get('stream_port', 8001)


def send_to_leader(leader_node: int, msg: dict) -> dict:
    """Send message to leader and get response."""
    hostname = NODE_HOSTS.get(leader_node)
    port = NODE_STREAM_PORTS.get(leader_node, STREAM_PORT)
    if not hostname:
        return {'status': 'error', 'message': 'Invalid node ID'}

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10.0)
        sock.connect((hostname, port))
        sock.sendall(json.dumps(msg).encode('utf-8'))
        response = sock.recv(65536).decode('utf-8')
        sock.close()
        return json.loads(response)
    except Exception as e:
        return {'status': 'error', 'message': str(e)}


def list_tasks(leader_node: int):
    """List all running tasks."""
    msg = {'type': 'LIST_TASKS'}
    response = send_to_leader(leader_node, msg)

    if response.get('status') == 'success':
        tasks = response.get('tasks', [])

        if not tasks:
            print("No tasks running.")
            return

        print(f"\n{'Task ID':<20} {'Node':<6} {'PID':<10} {'Op':<20} {'Log File':<30}")
        print("-" * 86)

        for task in tasks:
            print(f"{task['task_id']:<20} "
                  f"N{task['vm_id']:<4} "
                  f"{task['pid']:<10} "
                  f"{task['op_exe']:<20} "
                  f"{task['log_file']:<30}")

        print(f"\nTotal: {len(tasks)} tasks")
    else:
        print(f"Error: {response.get('message')}")


def kill_task(leader_node: int, node_id: int, pid: int):
    """Kill a specific task."""
    msg = {
        'type': 'KILL_TASK',
        'vm_id': node_id,
        'pid': pid
    }
    response = send_to_leader(leader_node, msg)

    if response.get('status') == 'success':
        print(f"Task killed: Node-{node_id} PID={pid}")
    else:
        print(f"Error: {response.get('message')}")


def send_rfs_request(hostname: str, port: int, message: dict, timeout: float = 30.0) -> dict:
    """Send a request to a ReplicatedFS node and get response.

    Uses the same protocol as ReplicatedFS/utils.py:
    - 10-byte ASCII size header
    - JSON message body
    """
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        sock.connect((hostname, port))

        # Send message with 10-byte ASCII size header
        data = json.dumps(message).encode('utf-8')
        size_header = f"{len(data):010d}".encode('utf-8')
        sock.sendall(size_header + data)

        # Receive response with 10-byte ASCII size header
        size_header = sock.recv(10)
        if not size_header:
            return {'status': 'error', 'message': 'No response'}
        response_length = int(size_header.decode('utf-8'))

        # Receive response data
        response_data = b''
        while len(response_data) < response_length:
            chunk = sock.recv(min(65536, response_length - len(response_data)))
            if not chunk:
                break
            response_data += chunk

        sock.close()
        return json.loads(response_data.decode('utf-8'))
    except Exception as e:
        return {'status': 'error', 'message': str(e)}


def fetch_from_rfs(rfs_file: str, verbose: bool = False) -> bytes:
    """
    Fetch a file from ReplicatedFS by trying all nodes.
    Returns file content as bytes.
    """
    client_id = f"cli_{socket.gethostname()}_{os.getpid()}_{time.time()}"

    errors = []
    for node_id in sorted(NODE_HOSTS.keys()):
        hostname = NODE_HOSTS[node_id]
        rfs_port = NODE_RFS_PORTS[node_id]
        try:
            message = {
                'type': 'GET',
                'filename': rfs_file,
                'client_id': client_id
            }

            if verbose:
                print(f"Trying Node-{node_id} ({hostname}:{rfs_port})...")

            response = send_rfs_request(hostname, rfs_port, message)

            if response.get('status') == 'success':
                data = bytes(response.get('data', []))
                print(f"Retrieved {len(data)} bytes from Node-{node_id}")
                return data
            else:
                err_msg = response.get('message', 'Unknown error')
                errors.append(f"Node-{node_id}: {err_msg}")
                if verbose:
                    print(f"  Node-{node_id} failed: {err_msg}")
        except Exception as e:
            errors.append(f"Node-{node_id}: {str(e)}")
            if verbose:
                print(f"  Node-{node_id} error: {e}")
            continue

    # Print all errors for debugging
    print("Failed to fetch from all nodes:")
    for err in errors:
        print(f"  {err}")
    raise Exception(f"Could not fetch {rfs_file} from any node")


def cat_output(rfs_file: str, head: int = None, tail: int = None,
               show_all: bool = False, count_only: bool = False, verbose: bool = False):
    """
    View output file from ReplicatedFS.

    Args:
        rfs_file: ReplicatedFS filename to view
        head: Show first N lines (default 20 if no other option)
        tail: Show last N lines
        show_all: Show all lines
        count_only: Only print line count
        verbose: Show debug output
    """
    try:
        data = fetch_from_rfs(rfs_file, verbose=verbose)
        lines = data.decode('utf-8').splitlines(keepends=True)

        total_lines = len(lines)

        # Count only mode
        if count_only:
            print(f"{total_lines}")
            return

        print(f"=== {rfs_file} ({total_lines} lines) ===\n")

        # Determine which lines to show
        if show_all:
            display_lines = lines
        elif tail:
            n = min(tail, total_lines)
            display_lines = lines[-n:]
            if n < total_lines:
                print(f"... (showing last {n} of {total_lines} lines)\n")
        else:
            # Default: head mode
            n = head if head else 20
            n = min(n, total_lines)
            display_lines = lines[:n]

        # Print lines
        for line in display_lines:
            print(line.rstrip())

        # Show truncation message for head mode
        if not show_all and not tail and total_lines > n:
            print(f"\n... ({total_lines - n} more lines, use --all to see all or --tail N for last N)")

    except Exception as e:
        print(f"Error fetching output: {e}")


def get_output(rfs_file: str, local_file: str, verbose: bool = False):
    """
    Download output file from ReplicatedFS and save locally.

    Args:
        rfs_file: ReplicatedFS filename to download
        local_file: Local filename to save to
        verbose: Show debug output
    """
    try:
        data = fetch_from_rfs(rfs_file, verbose=verbose)

        # Save to local file
        with open(local_file, 'wb') as f:
            f.write(data)

        print(f"Downloaded {rfs_file} to {local_file}")

        # Print first 20 lines
        lines = data.decode('utf-8').splitlines()
        print(f"\n=== Output ({len(lines)} total lines) ===")
        for line in lines[:20]:
            print(line.rstrip())
        if len(lines) > 20:
            print(f"... ({len(lines) - 20} more lines)")

    except Exception as e:
        print(f"Error fetching output: {e}")


def main():
    parser = argparse.ArgumentParser(description='StreamProcess CLI')
    parser.add_argument('--leader', type=int, default=1, help='Leader node ID')

    subparsers = parser.add_subparsers(dest='command', help='Command')

    # list_tasks
    subparsers.add_parser('list_tasks', help='List all running tasks')

    # kill_task
    kill_parser = subparsers.add_parser('kill_task', help='Kill a task')
    kill_parser.add_argument('node_id', type=int, help='Node ID')
    kill_parser.add_argument('pid', type=int, help='Process ID')

    # cat_output - view ReplicatedFS file
    cat_parser = subparsers.add_parser('cat_output', help='View output file from ReplicatedFS')
    cat_parser.add_argument('rfs_file', type=str, help='ReplicatedFS filename to view')
    cat_parser.add_argument('--head', type=int, help='Show first N lines (default 20)')
    cat_parser.add_argument('--tail', type=int, help='Show last N lines')
    cat_parser.add_argument('--all', action='store_true', help='Show all lines')
    cat_parser.add_argument('--count', action='store_true', help='Only show line count')
    cat_parser.add_argument('--verbose', '-v', action='store_true', help='Show debug output')

    # get_output - download ReplicatedFS file
    get_parser = subparsers.add_parser('get_output', help='Download output file from ReplicatedFS')
    get_parser.add_argument('rfs_file', type=str, help='ReplicatedFS filename to download')
    get_parser.add_argument('local_file', type=str, help='Local filename to save to')
    get_parser.add_argument('--verbose', '-v', action='store_true', help='Show debug output')

    args = parser.parse_args()

    if args.command == 'list_tasks':
        list_tasks(args.leader)
    elif args.command == 'kill_task':
        kill_task(args.leader, args.node_id, args.pid)
    elif args.command == 'cat_output':
        cat_output(args.rfs_file, args.head, args.tail, args.all, args.count, args.verbose)
    elif args.command == 'get_output':
        get_output(args.rfs_file, args.local_file, args.verbose)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
