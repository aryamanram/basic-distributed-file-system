"""
Membership Protocol Controller

Interactive CLI for sending commands to running membership nodes.
Connects to nodes over TCP to issue join, leave, switch, and status commands.
"""

import json
import socket
from typing import Dict


def load_config() -> dict:
    """Load node configuration from config.json."""
    with open("config.json", "r") as f:
        return json.load(f)


def recv_all(sock: socket.socket, size: int) -> bytes:
    """Receive exactly `size` bytes from a socket."""
    data = b""
    while len(data) < size:
        chunk = sock.recv(min(4096, size - len(data)))
        if not chunk:
            break
        data += chunk
    return data


def send_command(node_id: int, ip: str, control_port: int, command: str) -> None:
    """Send a command to a specific node and print the response.

    Uses a 10-byte length-prefix protocol to receive the full response.
    """
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        sock.connect((ip, control_port))
        sock.sendall(command.encode())

        header = recv_all(sock, 10)
        if not header:
            print(f"[node-{node_id}] No response")
            sock.close()
            return
        resp_size = int(header.decode("utf-8"))
        response = recv_all(sock, resp_size).decode("utf-8")

        sock.close()
        print(f"[node-{node_id}] {response}")
    except Exception as e:
        print(f"[node-{node_id}] Error: {e}")


def main() -> None:
    config = load_config()

    node_info: Dict[int, dict] = {}
    for node in config["nodes"]:
        node_info[node["id"]] = node

    node_ids = sorted(node_info.keys())

    print("\nMembership Protocol Controller")
    print("=" * 40)
    print("\nTarget selection:")
    print(f"  node <id>   Select a single node ({node_ids[0]}-{node_ids[-1]})")
    print("  node all    Select all nodes")
    print("\nCommands:")
    print("  join                                  Join the membership group")
    print("  leave                                 Leave the membership group")
    print("  list_mem                              Show membership list")
    print("  list_self                             Show this node's ID")
    print("  display_protocol                      Show current protocol")
    print("  display_suspects                      Show suspected nodes")
    print("  switch gossip|ping suspect|nosuspect  Switch protocol")
    print("  drop_rate <0.0-1.0>                   Simulate packet loss")
    print("  metrics                               Show network statistics")
    print("  quit                                  Exit controller")
    print()

    target = None

    while True:
        try:
            cmd = input(">>> ").strip()
            if not cmd:
                continue

            if cmd.startswith("node "):
                arg = cmd.split()[1]
                if arg == "all":
                    target = "all"
                    print(f"Target: all nodes ({node_ids[0]}-{node_ids[-1]})")
                else:
                    try:
                        nid = int(arg)
                        if nid not in node_info:
                            print(f"Unknown node ID: {nid}. Valid IDs: {node_ids}")
                            continue
                        target = nid
                        print(f"Target: node-{nid}")
                    except ValueError:
                        print("Usage: node <id> or node all")
                continue

            if cmd in ("quit", "exit"):
                break

            if target is None:
                print("Select a target first: node <id> or node all")
                continue

            if target == "all":
                for nid in node_ids:
                    n = node_info[nid]
                    send_command(nid, n["ip"], n["control_port"], cmd)
            else:
                n = node_info[target]
                send_command(target, n["ip"], n["control_port"], cmd)

        except KeyboardInterrupt:
            print("\nExiting controller.")
            break
        except Exception as e:
            print(f"Error: {e}")


if __name__ == "__main__":
    main()
