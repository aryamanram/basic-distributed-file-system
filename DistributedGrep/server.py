"""
Distributed Grep Server

Runs on each node and responds to grep queries from the client.
Adapted from Beej's Guide to Network Programming.
"""

import socket
import subprocess
import json
import threading
import sys
import signal
import argparse

DEFAULT_PORT = 3490


def load_config(vm_id):
    """Load configuration for a specific VM ID."""
    with open('config.json', 'r') as f:
        config = json.load(f)

    for vm in config['vms']:
        if vm['id'] == vm_id:
            return vm

    print(f"Error: Could not find VM with id {vm_id} in config.json")
    sys.exit(1)


def handle_client(conn, addr, vm_config):
    """Handle a grep request from a client."""
    try:
        data = conn.recv(4096).decode('utf-8')
        if not data:
            return

        request = json.loads(data)
        grep_pattern = request.get('pattern', '')
        grep_options = request.get('options', '')

        vm_id = vm_config['id']
        log_file = vm_config['log_file']

        try:
            if grep_options:
                cmd = f"grep {grep_options} '{grep_pattern}' {log_file}"
            else:
                cmd = f"grep '{grep_pattern}' {log_file}"

            result = subprocess.run(
                cmd,
                shell=True,
                capture_output=True,
                text=True,
                timeout=30
            )

            lines = []
            if result.stdout:
                lines = result.stdout.strip().split('\n')
                lines = [f"{log_file}:{line}" for line in lines if line]

            response = {
                'vm_id': vm_id,
                'count': len(lines),
                'lines': lines[:1000],
                'error': None
            }
        except Exception as e:
            response = {
                'vm_id': vm_id,
                'count': 0,
                'lines': [],
                'error': str(e)
            }

        response_data = json.dumps(response).encode('utf-8')
        size_header = f"{len(response_data):010d}".encode('utf-8')
        conn.send(size_header)
        conn.send(response_data)

    except Exception as e:
        print(f"Error handling client: {e}")
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description='Distributed Grep Server')
    parser.add_argument('--vm-id', type=int, required=True,
                       help='VM ID for this server instance (must match config.json)')
    args = parser.parse_args()

    vm_config = load_config(args.vm_id)
    port = vm_config.get('port', DEFAULT_PORT)
    vm_id = vm_config['id']
    log_file = vm_config['log_file']

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    sock.bind(('0.0.0.0', port))
    sock.listen(10)

    print(f"Server VM{vm_id} listening on port {port}")
    print(f"Log file: {log_file}")

    def signal_handler(sig, frame):
        print("\nShutting down server...")
        sock.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)

    while True:
        try:
            conn, addr = sock.accept()
            thread = threading.Thread(
                target=handle_client,
                args=(conn, addr, vm_config)
            )
            thread.daemon = True
            thread.start()
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Error accepting connection: {e}")

    sock.close()


if __name__ == "__main__":
    main()
