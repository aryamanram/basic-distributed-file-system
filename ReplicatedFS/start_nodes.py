"""
Start/Stop All ReplicatedFS Nodes

Helper script to manage all ReplicatedFS node instances for local testing.
Launches each node as a background process running main.py in non-interactive mode.
"""

import subprocess
import sys
import os
import json
import signal
import argparse
from typing import Optional

PID_FILE = ".node_pids.json"


def load_config() -> dict:
    """Load node configuration."""
    config_path = os.path.join(os.path.dirname(__file__), "config.json")
    with open(config_path, "r") as f:
        return json.load(f)


def start_nodes(count: Optional[int] = None) -> None:
    """Start ReplicatedFS nodes as background processes."""
    config = load_config()
    nodes = config["nodes"]
    if count:
        nodes = nodes[:count]

    pids = {}
    script_dir = os.path.dirname(os.path.abspath(__file__))
    print("Starting ReplicatedFS nodes...")

    for node in nodes:
        node_id = node["id"]

        cmd = [
            sys.executable, "-m", "ReplicatedFS.main",
            "--node-id", str(node_id),
            "--no-interactive"
        ]

        if sys.platform == "win32":
            process = subprocess.Popen(
                cmd,
                cwd=os.path.dirname(script_dir),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
            )
        else:
            process = subprocess.Popen(
                cmd,
                cwd=os.path.dirname(script_dir),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )

        pids[node_id] = process.pid
        print(f"  node-{node_id} started (PID: {process.pid}, "
              f"rfs={node['rfs_port']}, control={node['control_port']}, "
              f"membership={node['membership_port']})")

    pid_path = os.path.join(script_dir, PID_FILE)
    with open(pid_path, "w") as f:
        json.dump(pids, f)

    print(f"\n{len(pids)} nodes started.")
    print(f"PIDs saved to {PID_FILE}")
    print("\nNext steps:")
    print("  python controller.py          # Open the controller CLI")
    print("  python start_nodes.py --stop  # Stop all nodes")


def stop_nodes() -> None:
    """Stop all running nodes."""
    pid_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), PID_FILE)
    if not os.path.exists(pid_path):
        print("No PID file found. Nodes may not be running.")
        return

    with open(pid_path, "r") as f:
        pids = json.load(f)

    print("Stopping ReplicatedFS nodes...")

    for node_id, pid in pids.items():
        try:
            if sys.platform == "win32":
                subprocess.run(["taskkill", "/F", "/PID", str(pid)], capture_output=True)
            else:
                os.kill(pid, signal.SIGTERM)
            print(f"  node-{node_id} stopped (PID: {pid})")
        except (ProcessLookupError, OSError):
            print(f"  node-{node_id} was not running (PID: {pid})")

    os.remove(pid_path)
    print("\nAll nodes stopped.")


def check_status() -> None:
    """Check if nodes are running."""
    pid_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), PID_FILE)
    if not os.path.exists(pid_path):
        print("No nodes are being tracked (no PID file).")
        return

    with open(pid_path, "r") as f:
        pids = json.load(f)

    print("Node status:")

    for node_id, pid in pids.items():
        try:
            if sys.platform == "win32":
                result = subprocess.run(
                    ["tasklist", "/FI", f"PID eq {pid}"],
                    capture_output=True, text=True,
                )
                running = str(pid) in result.stdout
            else:
                os.kill(pid, 0)
                running = True
        except (ProcessLookupError, OSError):
            running = False

        status = "RUNNING" if running else "STOPPED"
        print(f"  node-{node_id}: {status} (PID: {pid})")


def main() -> None:
    parser = argparse.ArgumentParser(description="Manage ReplicatedFS Nodes")
    parser.add_argument("--stop", action="store_true", help="Stop all nodes")
    parser.add_argument("--status", action="store_true", help="Check node status")
    parser.add_argument("--count", type=int, default=None,
                        help="Number of nodes to start (default: all)")
    args = parser.parse_args()

    if args.stop:
        stop_nodes()
    elif args.status:
        check_status()
    else:
        start_nodes(args.count)


if __name__ == "__main__":
    main()
