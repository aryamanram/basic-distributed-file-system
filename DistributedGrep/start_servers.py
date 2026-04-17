"""
Start/Stop All Distributed Grep Servers

Helper script to manage all 10 server instances for local testing.
"""

import subprocess
import sys
import os
import json
import signal
import time
import argparse

PID_FILE = '.server_pids.json'


def load_config():
    """Load server configuration."""
    with open('config.json', 'r') as f:
        return json.load(f)


def start_servers():
    """Start all servers as background processes."""
    config = load_config()
    pids = {}

    print("Starting servers...")

    for vm in config['vms']:
        vm_id = vm['id']
        port = vm['port']

        # Start server as background process
        if sys.platform == 'win32':
            # Windows: use CREATE_NEW_PROCESS_GROUP
            process = subprocess.Popen(
                [sys.executable, 'server.py', '--vm-id', str(vm_id)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP
            )
        else:
            # Unix: use standard approach
            process = subprocess.Popen(
                [sys.executable, 'server.py', '--vm-id', str(vm_id)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True
            )

        pids[vm_id] = process.pid
        print(f"  Server VM{vm_id} started (PID: {process.pid}, port: {port})")

    # Save PIDs for later cleanup
    with open(PID_FILE, 'w') as f:
        json.dump(pids, f)

    print(f"\nAll {len(pids)} servers started.")
    print(f"PIDs saved to {PID_FILE}")
    print("\nTo stop all servers, run: python start_servers.py --stop")


def stop_servers():
    """Stop all running servers."""
    if not os.path.exists(PID_FILE):
        print("No PID file found. Servers may not be running.")
        return

    with open(PID_FILE, 'r') as f:
        pids = json.load(f)

    print("Stopping servers...")

    for vm_id, pid in pids.items():
        try:
            if sys.platform == 'win32':
                # Windows: use taskkill
                subprocess.run(['taskkill', '/F', '/PID', str(pid)],
                             capture_output=True)
            else:
                # Unix: send SIGTERM
                os.kill(pid, signal.SIGTERM)
            print(f"  Server VM{vm_id} stopped (PID: {pid})")
        except (ProcessLookupError, OSError):
            print(f"  Server VM{vm_id} was not running (PID: {pid})")

    # Remove PID file
    os.remove(PID_FILE)
    print("\nAll servers stopped.")


def check_status():
    """Check if servers are running."""
    if not os.path.exists(PID_FILE):
        print("No servers are being tracked (no PID file).")
        return

    with open(PID_FILE, 'r') as f:
        pids = json.load(f)

    print("Server status:")

    for vm_id, pid in pids.items():
        try:
            if sys.platform == 'win32':
                # Windows: check if process exists
                result = subprocess.run(
                    ['tasklist', '/FI', f'PID eq {pid}'],
                    capture_output=True,
                    text=True
                )
                running = str(pid) in result.stdout
            else:
                # Unix: send signal 0 to check if process exists
                os.kill(pid, 0)
                running = True
        except (ProcessLookupError, OSError):
            running = False

        status = "RUNNING" if running else "STOPPED"
        print(f"  Server VM{vm_id}: {status} (PID: {pid})")


def main():
    parser = argparse.ArgumentParser(description='Manage Distributed Grep Servers')
    parser.add_argument('--stop', action='store_true', help='Stop all servers')
    parser.add_argument('--status', action='store_true', help='Check server status')
    args = parser.parse_args()

    if args.stop:
        stop_servers()
    elif args.status:
        check_status()
    else:
        start_servers()


if __name__ == "__main__":
    main()
