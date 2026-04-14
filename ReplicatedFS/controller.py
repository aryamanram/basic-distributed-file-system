"""
ReplicatedFS Controller - Control interface for managing ReplicatedFS operations across nodes

Sends commands to running ReplicatedFS nodes via their control server ports.
Node addresses and ports are loaded from config.json.
"""
import json
import os
import socket
import sys


def load_config():
    """Load node configuration from config.json."""
    config_path = os.path.join(os.path.dirname(__file__), 'config.json')
    with open(config_path, 'r') as f:
        return json.load(f)


def get_node_hosts(config):
    """Build a node_id -> (ip, control_port) mapping from config."""
    hosts = {}
    for node in config['nodes']:
        hosts[node['id']] = (node['ip'], node['control_port'])
    return hosts


def send_command(node_id, command, node_hosts):
    """
    Send a command to a specific node and receive response.
    """
    ip, control_port = node_hosts[node_id]
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(60.0)  # Longer timeout for file operations
        sock.connect((ip, control_port))

        # Send command
        sock.sendall(command.encode('utf-8'))

        # Receive response
        response = sock.recv(65536).decode('utf-8')
        sock.close()

        print(f"\n[Node-{node_id} ({ip}:{control_port})]")
        print(response)
        return response
    except socket.timeout:
        print(f"\n[Node-{node_id}] Timeout - operation may still be in progress")
        return None
    except ConnectionRefusedError:
        print(f"\n[Node-{node_id}] Not running")
        return None
    except Exception as e:
        print(f"\n[Node-{node_id}] Error: {e}")
        return None


def print_help(max_node_id):
    """
    Print available commands.
    """
    print("\n" + "="*70)
    print("ReplicatedFS Controller Commands")
    print("="*70)
    print("\nNode Selection:")
    print(f"  node <id>              - Select single node (1-{max_node_id})")
    print(f"  node <id1,id2,...>     - Select multiple nodes (e.g., node 1,2,5)")
    print("  node all               - Select all nodes")
    print("\nFile Operations:")
    print("  create <local> <rfs>")
    print("  get <rfs> <local>")
    print("  append <local> <rfs>")
    print("  merge <rfs>")
    print("\nQuery Operations:")
    print("  ls <rfs>")
    print("  liststore")
    print("  getfromreplica <node_address> <rfs> <local>")
    print("  list_mem_ids")
    print("\nAdvanced:")
    print("  multiappend <rfs> <node1,node2,...> <file1,file2,...>")
    print("\nControl:")
    print("  help             - Show this help")
    print("  status           - Check node connectivity")
    print("  quit/exit        - Exit controller")
    print("="*70)
    print("\nNote: File paths are relative to each node's working directory")
    print("="*70 + "\n")


def check_node_status(node_hosts):
    """
    Check connectivity to all nodes.
    """
    print("\nChecking node status...")
    print("-" * 60)

    for node_id in sorted(node_hosts.keys()):
        ip, control_port = node_hosts[node_id]
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2.0)
            sock.connect((ip, control_port))

            sock.sendall("status".encode('utf-8'))
            response = sock.recv(1024).decode('utf-8')
            sock.close()

            print(f"  Node-{node_id:2d} ({ip}:{control_port}): ONLINE - {response.strip()}")
        except socket.timeout:
            print(f"  Node-{node_id:2d} ({ip}:{control_port}): TIMEOUT")
        except ConnectionRefusedError:
            print(f"  Node-{node_id:2d} ({ip}:{control_port}): NOT RUNNING")
        except Exception as e:
            print(f"  Node-{node_id:2d} ({ip}:{control_port}): ERROR - {e}")

    print("-" * 60)


def parse_command(cmd_str):
    """
    Parse command string and validate.
    Returns (command_type, args) or (None, None) if invalid.
    """
    parts = cmd_str.strip().split()
    if not parts:
        return None, None

    cmd = parts[0].lower()

    # Commands with no args
    if cmd in ['liststore', 'list_mem_ids', 'status', 'help', 'quit', 'exit']:
        return cmd, []

    # Commands with specific arg counts
    cmd_args = {
        'create': 2,
        'get': 2,
        'append': 2,
        'merge': 1,
        'ls': 1,
        'getfromreplica': 3,
        'multiappend': 3
    }

    if cmd in cmd_args:
        expected = cmd_args[cmd]
        if len(parts) - 1 != expected:
            print(f"Error: '{cmd}' expects {expected} arguments, got {len(parts) - 1}")
            return None, None
        return cmd, parts[1:]

    return None, None


def main():
    config = load_config()
    node_hosts = get_node_hosts(config)
    node_ids = sorted(node_hosts.keys())
    max_node_id = max(node_ids)

    print("\n" + "="*70)
    print("ReplicatedFS Controller")
    print("="*70)
    print(f"\nConfigured nodes: {', '.join(str(n) for n in node_ids)}")
    print("\nType 'help' for available commands")
    print("Type 'status' to check node connectivity")
    print("\nNode Selection Examples:")
    print("  node 1         - Select node 1")
    print("  node 1,2,3     - Select nodes 1, 2, and 3")
    print("  node all       - Select all nodes")
    print("="*70 + "\n")

    target = None

    while True:
        try:
            cmd_str = input("rfs-ctrl> ").strip()
            if not cmd_str:
                continue

            # Handle node selection
            if cmd_str.startswith("node "):
                arg = cmd_str.split()[1] if len(cmd_str.split()) > 1 else ""
                if arg == "all":
                    target = "all"
                    print(f"Target set to: all nodes")
                elif ',' in arg:
                    try:
                        ids = [int(x.strip()) for x in arg.split(',')]
                        invalid = [nid for nid in ids if nid not in node_hosts]
                        if invalid:
                            print(f"Error: Invalid node IDs: {invalid}. Available: {node_ids}")
                        else:
                            target = ids
                            print(f"Target set to: {', '.join(f'Node-{nid}' for nid in ids)}")
                    except ValueError:
                        print("Error: Invalid node ID format. Use comma-separated numbers (e.g., 1,2,3)")
                else:
                    try:
                        nid = int(arg)
                        if nid in node_hosts:
                            target = nid
                            ip, port = node_hosts[nid]
                            print(f"Target set to: Node-{nid} ({ip}:{port})")
                        else:
                            print(f"Error: Node ID {nid} not in config. Available: {node_ids}")
                    except ValueError:
                        print("Error: Invalid node ID")
                continue

            # Handle special commands
            if cmd_str.lower() in ('quit', 'exit'):
                print("Exiting controller...")
                break

            if cmd_str.lower() == 'help':
                print_help(max_node_id)
                continue

            if cmd_str.lower() == 'status':
                check_node_status(node_hosts)
                continue

            # Validate target is set
            if target is None:
                print("Error: No target node selected. Use 'node <id>', 'node <id1,id2,...>', or 'node all'")
                continue

            # Parse and validate command
            cmd_type, args = parse_command(cmd_str)
            if cmd_type is None:
                print("Error: Invalid command. Type 'help' for available commands")
                continue

            # Execute command on target node(s)
            if target == "all":
                print(f"\nExecuting '{cmd_str}' on all nodes...")
                print("=" * 60)
                for nid in node_ids:
                    send_command(nid, cmd_str, node_hosts)
                print("=" * 60)
            elif isinstance(target, list):
                print(f"\nExecuting '{cmd_str}' on nodes: {', '.join(map(str, target))}...")
                print("=" * 60)
                for nid in target:
                    send_command(nid, cmd_str, node_hosts)
                print("=" * 60)
            else:
                send_command(target, cmd_str, node_hosts)

        except KeyboardInterrupt:
            print("\n\nExiting controller...")
            break
        except Exception as e:
            print(f"Error: {e}")


if __name__ == "__main__":
    main()
