"""
Membership Protocol Service

Implements a group membership protocol with two selectable failure detection
strategies: Gossip (epidemic) and Ping-Ack (direct probing). Supports an
optional suspicion mechanism to reduce false positives.

Each node maintains a membership list with heartbeat counters and incarnation
numbers. Nodes detect failures via timeouts and propagate membership updates
to peers over UDP.
"""

import argparse
import copy
import json
import os
import random
import socket
import threading
import time
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Tuple

MAX_JOIN_RETRIES = 3
JOIN_RETRY_INTERVAL = 2.0


class MemberStatus(Enum):
    ACTIVE = "ACTIVE"
    SUSPECTED = "SUSPECTED"
    FAILED = "FAILED"
    LEFT = "LEFT"


class ProtocolType(Enum):
    GOSSIP = "GOSSIP"
    PINGACK = "PINGACK"


class Logger:
    """Thread-safe logger that writes to both stdout and a per-node log file."""

    def __init__(self, node_id: int):
        self.node_id = node_id
        self.lock = threading.Lock()
        os.makedirs("logs", exist_ok=True)
        self._file = open(f"logs/machine.{node_id}.log", "a")

    def log(self, msg: str) -> None:
        line = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] {msg}"
        with self.lock:
            print(line)
            self._file.write(line + "\n")
            self._file.flush()

    def close(self) -> None:
        self._file.close()


class MembershipList:
    """Thread-safe membership list tracking all known nodes and their status.

    Each member entry contains:
        id          - Unique identifier (timestamp_nodename_port)
        hostname    - Node's hostname identifier
        heartbeat   - Monotonically increasing counter
        timestamp   - Last time this entry was updated locally
        status      - Current status (ACTIVE/SUSPECTED/FAILED/LEFT)
        incarnation - Generation counter to override stale failure reports
    """

    def __init__(self, hostname: str, member_id: str, logger: Logger,
                 on_suspicion: Optional["callable"] = None):
        self.lock = threading.RLock()
        self.members: Dict[str, dict] = {}
        self.hostname = hostname
        self.member_id = member_id
        self.logger = logger
        self._on_suspicion = on_suspicion

        self.members[hostname] = {
            "id": member_id,
            "hostname": hostname,
            "heartbeat": 0,
            "timestamp": time.time(),
            "status": MemberStatus.ACTIVE.value,
            "incarnation": 0,
        }

    def get_members(self) -> Dict[str, dict]:
        with self.lock:
            return copy.deepcopy(self.members)

    def get_self(self) -> str:
        return self.member_id

    def increment_self_heartbeat(self) -> None:
        with self.lock:
            m = self.members[self.hostname]
            m["heartbeat"] += 1
            m["timestamp"] = time.time()

    def increment_self_incarnation(self) -> None:
        """Bump incarnation to override a false failure report about this node."""
        with self.lock:
            m = self.members[self.hostname]
            m["incarnation"] += 1
            m["timestamp"] = time.time()
            m["status"] = MemberStatus.ACTIVE.value
            self.logger.log(f"INCARNATION: Bumped to {m['incarnation']} for {self.hostname}")

    def update_member_from_remote(self, hostname: str, data: dict) -> bool:
        """Merge a remote member entry using incarnation-based versioning.

        A remote entry is accepted only if it has a higher incarnation number,
        or the same incarnation with a higher heartbeat. This prevents stale
        information from incorrectly overriding current state.
        """
        with self.lock:
            now = time.time()
            remote_inc = data.get("incarnation", 0)
            remote_hb = data.get("heartbeat", 0)
            remote_status = data.get("status", MemberStatus.ACTIVE.value)

            # If this remote update claims we are SUSPECTED or FAILED, bump incarnation
            if hostname == self.hostname:
                if remote_status in (MemberStatus.SUSPECTED.value, MemberStatus.FAILED.value):
                    if remote_inc >= self.members[self.hostname]["incarnation"]:
                        self.logger.log(f"INCARNATION: Remote claims I am {remote_status}, bumping incarnation")
                        self.members[self.hostname]["incarnation"] += 1
                        self.members[self.hostname]["timestamp"] = now
                        self.members[self.hostname]["status"] = MemberStatus.ACTIVE.value
                return False

            if hostname not in self.members:
                self.members[hostname] = {
                    "id": data.get("id", f"unknown_{hostname}"),
                    "hostname": hostname,
                    "heartbeat": remote_hb,
                    "timestamp": now,
                    "status": remote_status,
                    "incarnation": remote_inc,
                }
                return True

            local = self.members[hostname]
            updated = False

            if remote_inc > local["incarnation"]:
                local.update({
                    "heartbeat": remote_hb,
                    "status": remote_status,
                    "timestamp": now,
                    "incarnation": remote_inc,
                })
                updated = True
            elif remote_inc == local["incarnation"] and remote_hb > local["heartbeat"]:
                local["heartbeat"] = remote_hb
                local["status"] = remote_status
                local["timestamp"] = now
                updated = True

            return updated

    def mark_active(self, hostname: str) -> None:
        with self.lock:
            if hostname in self.members:
                m = self.members[hostname]
                m["timestamp"] = time.time()
                if m["status"] != MemberStatus.LEFT.value:
                    m["status"] = MemberStatus.ACTIVE.value

    def mark_suspected(self, hostname: str, source: str) -> None:
        with self.lock:
            if hostname in self.members:
                m = self.members[hostname]
                if m["status"] not in (MemberStatus.FAILED.value, MemberStatus.LEFT.value):
                    m["status"] = MemberStatus.SUSPECTED.value
                    m["timestamp"] = time.time()
                    self.logger.log(f"SUSPECTED: {hostname} (by {source})")
        # Callback outside the lock to avoid nested lock acquisition
        if self._on_suspicion:
            self._on_suspicion()

    def mark_failed(self, hostname: str) -> None:
        with self.lock:
            if hostname in self.members:
                m = self.members[hostname]
                elapsed = time.time() - m["timestamp"]
                m["status"] = MemberStatus.FAILED.value
                m["timestamp"] = time.time()
                self.logger.log(f"DETECTED FAILURE {hostname} after {elapsed:.3f}s since last heartbeat")

    def mark_left(self, hostname: str) -> None:
        with self.lock:
            if hostname in self.members:
                m = self.members[hostname]
                m["status"] = MemberStatus.LEFT.value
                m["timestamp"] = time.time()
                self.logger.log(f"LEFT: {hostname} voluntarily left")

    def remove_member(self, hostname: str) -> None:
        with self.lock:
            if hostname in self.members:
                del self.members[hostname]


class PingAckProtocol:
    """Direct probing failure detection.

    Periodically selects a random active peer and sends a PING. If no ACK
    is received within failure_timeout, the peer is marked SUSPECTED (if
    suspicion is enabled) or FAILED (if not).
    """

    def __init__(self, service: "MembershipService"):
        self.service = service
        self.running = False
        self.thread: Optional[threading.Thread] = None
        self.ack_lock = threading.Lock()
        self.waiting_acks: Dict[str, float] = {}
        self.suspicion_timers: Dict[str, threading.Timer] = {}

    def start(self) -> None:
        if self.running:
            return
        self.running = True
        self.thread = threading.Thread(target=self.ping_loop, daemon=True)
        self.thread.start()
        self.service.logger.log("PROTOCOL: Started PingAck")

    def stop(self) -> None:
        if not self.running:
            return
        self.running = False
        # Cancel all pending suspicion timers
        for timer in self.suspicion_timers.values():
            timer.cancel()
        self.suspicion_timers.clear()
        if self.thread:
            self.thread.join(timeout=1)
        self.service.logger.log("PROTOCOL: Stopped PingAck")

    def ping_loop(self) -> None:
        while self.running:
            try:
                self.send_ping()
                self.check_timeouts()
                time.sleep(self.service.pingack_interval)
            except Exception as e:
                self.service.logger.log(f"ERROR: PingAck loop: {e}")

    def send_ping(self) -> None:
        members = self.service.membership.get_members()
        actives = [
            h for h, v in members.items()
            if h != self.service.node_name and v["status"] == MemberStatus.ACTIVE.value
        ]
        if not actives:
            return
        target = random.choice(actives)
        msg = {"type": "PING", "sender": self.service.node_name}
        try:
            target_addr = self.service.get_member_addr(target)
            self.service.sendto(json.dumps(msg).encode("utf-8"), target_addr)
            with self.ack_lock:
                if target not in self.waiting_acks:
                    self.waiting_acks[target] = time.time()
            self.service.logger.log(f"PINGACK: Sent PING to {target}")
        except Exception as e:
            self.service.logger.log(f"ERROR: send PING to {target}: {e}")

    def handle_ping(self, msg: dict, addr: Tuple[str, int]) -> None:
        reply = {"type": "ACK", "sender": self.service.node_name}
        try:
            self.service.sendto(json.dumps(reply).encode("utf-8"), addr)
            self.service.logger.log(f"PINGACK: Received PING from {msg.get('sender')}, sent ACK")
        except Exception as e:
            self.service.logger.log(f"ERROR: send ACK: {e}")

    def handle_ack(self, msg: dict) -> None:
        sender = msg.get("sender")
        with self.ack_lock:
            self.waiting_acks.pop(sender, None)
        self.service.membership.mark_active(sender)
        self.service.logger.log(f"PINGACK: Received ACK from {sender}")

    def check_timeouts(self) -> None:
        now = time.time()
        with self.ack_lock:
            expired = [h for h, t0 in self.waiting_acks.items() if now - t0 > self.service.failure_timeout]
            for host in expired:
                del self.waiting_acks[host]
        for host in expired:
            if self.service.suspicion_enabled:
                self.service.membership.mark_suspected(host, source=f"{self.service.node_name}/ping")
                timer = threading.Timer(self.service.suspicion_timeout, self.suspect_to_fail, args=[host])
                self.suspicion_timers[host] = timer
                timer.start()
            else:
                self.service.membership.mark_failed(host)

    def suspect_to_fail(self, host: str) -> None:
        self.suspicion_timers.pop(host, None)
        mem = self.service.membership.get_members()
        if host in mem and mem[host]["status"] == MemberStatus.SUSPECTED.value:
            self.service.membership.mark_failed(host)


class GossipProtocol:
    """Epidemic-style failure detection.

    Periodically increments a local heartbeat counter and sends the full
    membership list to 1-3 random peers. Receiving nodes merge the remote
    list using incarnation-based versioning. Nodes whose heartbeats haven't
    been updated within failure_timeout are marked as failed.
    """

    def __init__(self, service: "MembershipService"):
        self.service = service
        self.running = False
        self.thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if self.running:
            return
        self.running = True
        self.thread = threading.Thread(target=self.gossip_loop, daemon=True)
        self.thread.start()
        self.service.logger.log("PROTOCOL: Started Gossip")

    def stop(self) -> None:
        if not self.running:
            return
        self.running = False
        if self.thread:
            self.thread.join(timeout=1)
        self.service.logger.log("PROTOCOL: Stopped Gossip")

    def gossip_loop(self) -> None:
        while self.running:
            try:
                self.service.membership.increment_self_heartbeat()
                self.send_gossip()
                self.check_suspected()
                time.sleep(self.service.gossip_interval)
            except Exception as e:
                self.service.logger.log(f"ERROR: Gossip loop: {e}")

    def send_gossip(self) -> None:
        members = {
            h: m for h, m in self.service.membership.get_members().items()
            if not (m["status"] in (MemberStatus.FAILED.value, MemberStatus.LEFT.value)
                    and (time.time() - m["timestamp"]) > self.service.cleanup_timeout)
        }
        peers = [
            h for h, v in members.items()
            if h != self.service.node_name and v["status"] != MemberStatus.LEFT.value
        ]
        if not peers:
            return
        k = min(len(peers), random.randint(1, 3))
        targets = random.sample(peers, k)

        msg = {
            "type": "GOSSIP",
            "sender": self.service.node_name,
            "members": members,
            "protocol": self.service.current_protocol.value,
            "suspicion": self.service.suspicion_enabled,
        }
        data = json.dumps(msg).encode("utf-8")
        for t in targets:
            try:
                target_addr = self.service.get_member_addr(t)
                self.service.sendto(data, target_addr)
            except Exception as e:
                self.service.logger.log(f"ERROR: Gossip send to {t}: {e}")

    def handle_gossip(self, msg: dict) -> None:
        remote_members = msg.get("members", {})
        for host, m in remote_members.items():
            self.service.membership.update_member_from_remote(host, m)

        try:
            r_proto = ProtocolType(msg.get("protocol", self.service.current_protocol.value))
            r_susp = bool(msg.get("suspicion", self.service.suspicion_enabled))
            if (r_proto != self.service.current_protocol) or (r_susp != self.service.suspicion_enabled):
                self.service.switch(r_proto, r_susp, rebroadcast=False)
                self.service.logger.log(
                    f"GOSSIP: Adopted protocol ({r_proto.value.lower()}, "
                    f"{'suspect' if r_susp else 'nosuspect'})"
                )
        except ValueError:
            self.service.logger.log("ERROR: Invalid protocol value in gossip message")

    def check_suspected(self) -> None:
        now = time.time()
        members = self.service.membership.get_members()
        for host, m in members.items():
            if host == self.service.node_name:
                continue
            if m["status"] == MemberStatus.LEFT.value:
                continue
            age = now - m["timestamp"]
            if m["status"] == MemberStatus.ACTIVE.value and age > self.service.failure_timeout:
                if self.service.suspicion_enabled:
                    self.service.membership.mark_suspected(host, source=f"{self.service.node_name}/gossip")
                else:
                    self.service.membership.mark_failed(host)
                    self.service.logger.log(f"GOSSIP: FAILED {host} (timeout, suspicion OFF)")
            elif m["status"] == MemberStatus.SUSPECTED.value and age > self.service.cleanup_timeout:
                self.service.membership.mark_failed(host)
                self.service.logger.log(f"GOSSIP: Suspected {host} marked as FAILED")


class MembershipService:
    """Main orchestrator for the membership protocol.

    Manages the membership list, failure detection protocol, and communication.
    Runs four daemon threads:
        1. receive_loop  - UDP listener for peer messages
        2. monitor_loop  - Periodic cleanup of failed members
        3. control_loop  - TCP server for external commands
        4. Protocol thread (gossip_loop or ping_loop)
    """

    def __init__(self, node_id: int):
        with open("config.json", "r") as f:
            config = json.load(f)

        self.node_id = node_id
        self.config = config
        self.node_config: Optional[dict] = None
        self.peer_addrs: Dict[str, Tuple[str, int]] = {}

        for node in config["nodes"]:
            name = f"node-{node['id']}"
            self.peer_addrs[name] = (node["ip"], node["membership_port"])
            if node["id"] == node_id:
                self.node_config = node

        if not self.node_config:
            raise ValueError(f"Node ID {node_id} not found in config.json")

        self.node_name = f"node-{node_id}"
        self.membership_port: int = self.node_config["membership_port"]
        self.control_port: int = self.node_config["control_port"]

        introducer_id = config.get("introducer_id", 1)
        self.introducer_name = f"node-{introducer_id}"

        self.member_id = f"{int(time.time())}_{self.node_name}_{self.membership_port}"

        self.gossip_interval: float = config.get("gossip_interval", 0.5)
        self.pingack_interval: float = config.get("pingack_interval", 0.5)
        self.failure_timeout: float = config.get("failure_timeout", 15.0)
        self.suspicion_timeout: float = config.get("suspicion_timeout", 10.0)
        self.cleanup_timeout: float = config.get("cleanup_timeout", 10.0)

        self.logger = Logger(node_id)
        self.membership = MembershipList(
            self.node_name, self.member_id, self.logger,
            on_suspicion=self._on_suspicion,
        )

        self.suspicion_enabled = False
        self.current_protocol = ProtocolType.PINGACK

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("", self.membership_port))
        self.sock.settimeout(0.5)

        self.gossip = GossipProtocol(self)
        self.pingack = PingAckProtocol(self)

        self.running = False

        self.metrics_lock = threading.Lock()
        self.drop_rate: float = 0.0
        self.bytes_sent: int = 0
        self.bytes_recv: int = 0
        self.total_suspicions: int = 0

        self.logger.log(f"INIT: {self.node_name} (ID {self.member_id}) "
                        f"membership={self.membership_port} control={self.control_port}")

    def _on_suspicion(self) -> None:
        """Callback invoked by MembershipList when a node is suspected."""
        with self.metrics_lock:
            self.total_suspicions += 1

    def get_member_addr(self, node_name: str) -> Tuple[str, int]:
        """Resolve a node name to its (ip, membership_port) address."""
        if node_name in self.peer_addrs:
            return self.peer_addrs[node_name]
        raise ValueError(f"Unknown node: {node_name}")

    def sendto(self, data: bytes, addr: Tuple[str, int]) -> int:
        """Send UDP data and track bytes sent."""
        try:
            sent = self.sock.sendto(data, addr)
            with self.metrics_lock:
                self.bytes_sent += sent
            return sent
        except Exception as e:
            self.logger.log(f"ERROR: sendto failed: {e}")
            return 0

    def start(self) -> None:
        self.running = True
        for target in [self.receive_loop, self.monitor_loop, self.control_loop]:
            t = threading.Thread(target=target, daemon=True)
            t.start()
        self._start_protocol()
        self.logger.log("SERVICE: Started")

    def stop(self) -> None:
        self.running = False
        self.gossip.stop()
        self.pingack.stop()
        self.sock.close()
        self.logger.log("SERVICE: Stopped")
        self.logger.close()

    def _start_protocol(self) -> None:
        """Start the currently selected protocol."""
        if self.current_protocol == ProtocolType.GOSSIP:
            self.gossip.start()
        else:
            self.pingack.start()

    def switch(self, new_proto: ProtocolType, new_suspect_enabled: bool,
               rebroadcast: bool = True) -> None:
        """Switch failure detection protocol and/or suspicion mode at runtime.

        When rebroadcast=True, sends a one-hop SWITCH message to all peers.
        Peers apply the switch locally without re-broadcasting, preventing
        message storms.
        """
        if (self.current_protocol == new_proto and
                self.suspicion_enabled == new_suspect_enabled):
            return

        if self.current_protocol != new_proto:
            if self.current_protocol == ProtocolType.GOSSIP:
                self.gossip.stop()
            else:
                self.pingack.stop()

            self.current_protocol = new_proto

            if self.current_protocol == ProtocolType.GOSSIP:
                self.gossip.start()
            else:
                self.pingack.start()

        self.suspicion_enabled = new_suspect_enabled
        self.logger.log(f"PROTOCOL: Now ({self.current_protocol.value.lower()}, "
                        f"{'suspect' if self.suspicion_enabled else 'nosuspect'})")

        if rebroadcast:
            self.broadcast_switch(self.current_protocol, self.suspicion_enabled)

    def broadcast_switch(self, proto: ProtocolType, susp: bool) -> None:
        msg = {
            "type": "SWITCH",
            "sender": self.node_name,
            "protocol": proto.value,
            "suspicion": susp,
        }
        members = self.membership.get_members()
        targets = [h for h, v in members.items()
                   if h != self.node_name and v["status"] != MemberStatus.LEFT.value]
        data = json.dumps(msg).encode("utf-8")
        for t in targets:
            try:
                self.sendto(data, self.get_member_addr(t))
            except Exception as e:
                self.logger.log(f"ERROR: broadcast SWITCH to {t}: {e}")

    def join_group(self) -> str:
        """Join the membership group via the introducer node.

        Non-introducer nodes send a JOIN request and retry up to MAX_JOIN_RETRIES
        times if no JOIN_REPLY is received (UDP is unreliable).
        """
        self.membership.mark_active(self.node_name)

        # Restart protocol if it was stopped by a previous leave
        if not self.gossip.running and not self.pingack.running:
            self._start_protocol()

        if self.node_name == self.introducer_name:
            self.logger.log("JOIN: This node is the introducer")
            return "Joined as introducer"

        msg = {
            "type": "JOIN",
            "sender": self.node_name,
            "member_data": self.membership.get_members()[self.node_name],
        }
        introducer_addr = self.get_member_addr(self.introducer_name)
        data = json.dumps(msg).encode("utf-8")

        for attempt in range(1, MAX_JOIN_RETRIES + 1):
            try:
                self.sendto(data, introducer_addr)
                self.logger.log(f"JOIN: Sent join to {self.introducer_name} (attempt {attempt}/{MAX_JOIN_RETRIES})")
            except Exception as e:
                self.logger.log(f"ERROR: join send: {e}")
                return f"Join error: {e}"

            # Wait for JOIN_REPLY — check if we learned about other members
            time.sleep(JOIN_RETRY_INTERVAL)
            members = self.membership.get_members()
            if len(members) > 1:
                self.logger.log("JOIN: Received membership from introducer")
                return "Joined group"

        self.logger.log("JOIN: No reply from introducer after retries (will discover peers via protocol)")
        return "Join sent (no reply, will discover via protocol)"

    def handle_join(self, message: dict) -> None:
        sender = message.get("sender")
        data = message.get("member_data", {})
        self.membership.update_member_from_remote(sender, data)
        self.membership.mark_active(sender)
        self.logger.log(f"JOIN: {sender} joined")

        if self.node_name == self.introducer_name:
            members = self.membership.get_members()
            for h, v in members.items():
                if h in (self.node_name, sender):
                    continue
                if v["status"] == MemberStatus.LEFT.value:
                    continue
                fwd = {"type": "JOIN", "sender": sender, "member_data": data}
                try:
                    self.sendto(json.dumps(fwd).encode("utf-8"), self.get_member_addr(h))
                except Exception as e:
                    self.logger.log(f"ERROR: forward JOIN to {h}: {e}")
            reply = {"type": "JOIN_REPLY", "members": members}
            try:
                self.sendto(json.dumps(reply).encode("utf-8"), self.get_member_addr(sender))
                self.logger.log(f"JOIN: Sent JOIN_REPLY to {sender}")
            except Exception as e:
                self.logger.log(f"ERROR: JOIN_REPLY: {e}")

    def handle_join_reply(self, message: dict) -> None:
        remote = message.get("members", {})
        for host, m in remote.items():
            self.membership.update_member_from_remote(host, m)
        self.logger.log("JOIN: Applied JOIN_REPLY membership")

    def leave_group(self) -> str:
        """Voluntarily leave the group and notify all peers."""
        if self.current_protocol == ProtocolType.GOSSIP:
            self.gossip.stop()
        elif self.current_protocol == ProtocolType.PINGACK:
            self.pingack.stop()

        self.membership.mark_left(self.node_name)

        members = self.membership.get_members()
        targets = [h for h, v in members.items()
                   if h != self.node_name and v["status"] != MemberStatus.LEFT.value]
        msg = {"type": "LEAVE", "sender": self.node_name}
        data = json.dumps(msg).encode("utf-8")
        for t in targets:
            try:
                self.sendto(data, self.get_member_addr(t))
            except Exception as e:
                self.logger.log(f"ERROR: send LEAVE to {t}: {e}")

        self.logger.log("LEAVE: Voluntarily left the group")
        return "Left group"

    def handle_leave(self, message: dict) -> None:
        sender = message.get("sender")
        self.membership.mark_left(sender)

    def receive_loop(self) -> None:
        """Listen for incoming UDP messages and dispatch to handlers."""
        while self.running:
            try:
                data, addr = self.sock.recvfrom(65535)
                with self.metrics_lock:
                    self.bytes_recv += len(data)
                message = json.loads(data.decode("utf-8"))

                t = message.get("type")

                if t in ("PING", "ACK", "GOSSIP"):
                    if random.random() < self.drop_rate:
                        self.logger.log(f"DROP: Simulated drop of {t} from {message.get('sender')}")
                        continue

                if t == "PING":
                    self.pingack.handle_ping(message, addr)
                elif t == "ACK":
                    self.pingack.handle_ack(message)
                elif t == "GOSSIP":
                    self.gossip.handle_gossip(message)
                elif t == "JOIN":
                    self.handle_join(message)
                elif t == "JOIN_REPLY":
                    self.handle_join_reply(message)
                elif t == "LEAVE":
                    self.handle_leave(message)
                elif t == "SWITCH":
                    proto = ProtocolType(message["protocol"])
                    susp = bool(message["suspicion"])
                    self.switch(proto, susp, rebroadcast=False)
            except socket.timeout:
                continue
            except OSError:
                break
            except Exception as e:
                self.logger.log(f"ERROR: recv: {e}")

    def monitor_loop(self) -> None:
        """Periodically remove members that have been FAILED or LEFT beyond the cleanup timeout."""
        while self.running:
            try:
                now = time.time()
                members = self.membership.get_members()
                for host, m in list(members.items()):
                    if host == self.node_name:
                        continue
                    status = m["status"]
                    age = now - m["timestamp"]
                    if status in (MemberStatus.FAILED.value, MemberStatus.LEFT.value) and age > self.cleanup_timeout:
                        self.membership.remove_member(host)
                        self.logger.log(f"MONITOR: Removed {host}")
                time.sleep(1.0)
            except Exception as e:
                self.logger.log(f"ERROR: monitor: {e}")

    def control_loop(self) -> None:
        """TCP server accepting commands from the controller CLI.

        Uses a 10-byte length-prefix protocol: the response is preceded by
        a zero-padded integer indicating its size in bytes.
        """
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.settimeout(1.0)
        srv.bind(("", self.control_port))
        srv.listen(8)
        self.logger.log(f"CONTROL: listening on {self.control_port}")
        while self.running:
            try:
                conn, _ = srv.accept()
                try:
                    cmd = conn.recv(8192).decode().strip()
                    resp = self.handle_command(cmd)
                    resp_data = resp.encode("utf-8")
                    header = f"{len(resp_data):010d}".encode("utf-8")
                    conn.sendall(header + resp_data)
                except Exception as e:
                    self.logger.log(f"ERROR: control: {e}")
                finally:
                    conn.close()
            except socket.timeout:
                continue
            except Exception as e:
                self.logger.log(f"ERROR: control accept: {e}")
        srv.close()

    def handle_command(self, cmd: str) -> str:
        """Process a command string and return a response."""
        try:
            if cmd == "list_mem":
                return json.dumps(self.membership.get_members(), indent=2)

            if cmd == "list_self":
                return self.membership.get_self()

            if cmd == "display_suspects":
                mem = self.membership.get_members()
                suspects = [h for h, v in mem.items() if v["status"] == MemberStatus.SUSPECTED.value]
                return json.dumps(suspects)

            if cmd == "display_protocol":
                return (f"({self.current_protocol.value.lower()}, "
                        f"{'suspect' if self.suspicion_enabled else 'nosuspect'})")

            if cmd == "join":
                return self.join_group()

            if cmd == "leave":
                return self.leave_group()

            if cmd.startswith("switch"):
                parts = cmd.split()
                if len(parts) != 3:
                    return "Usage: switch (gossip|ping) (suspect|nosuspect)"
                proto_str = parts[1].strip().lower()
                susp_str = parts[2].strip().lower()
                if proto_str not in ("gossip", "ping"):
                    return "Usage: switch (gossip|ping) (suspect|nosuspect)"
                if susp_str not in ("suspect", "nosuspect"):
                    return "Usage: switch (gossip|ping) (suspect|nosuspect)"

                new_proto = ProtocolType.GOSSIP if proto_str == "gossip" else ProtocolType.PINGACK
                new_susp = (susp_str == "suspect")
                self.switch(new_proto, new_susp, rebroadcast=True)
                return (f"Switched -> ({new_proto.value.lower()}, "
                        f"{'suspect' if new_susp else 'nosuspect'}) (broadcasted)")

            if cmd.startswith("drop_rate"):
                parts = cmd.split()
                if len(parts) != 2:
                    return "Usage: drop_rate <0.0-1.0>"
                try:
                    rate = float(parts[1])
                    if not (0.0 <= rate <= 1.0):
                        return "Rate must be between 0.0 and 1.0"
                    self.drop_rate = rate
                    return f"Drop rate set to {rate * 100:.1f}%"
                except ValueError:
                    return "Invalid number"

            if cmd == "metrics":
                with self.metrics_lock:
                    return json.dumps({
                        "bytes_sent": self.bytes_sent,
                        "bytes_recv": self.bytes_recv,
                        "total_suspicions": self.total_suspicions,
                    }, indent=2)

            return f"Unknown command: {cmd}"
        except Exception as e:
            return f"Error: {e}"


def main():
    parser = argparse.ArgumentParser(description="Membership Protocol Service")
    parser.add_argument("--node-id", type=int, required=True,
                        help="Node ID (must match an entry in config.json)")
    args = parser.parse_args()

    service = MembershipService(args.node_id)
    service.start()

    print(f"\nNode {args.node_id} running. Press Ctrl+C to stop.\n")

    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        service.stop()


if __name__ == "__main__":
    main()
