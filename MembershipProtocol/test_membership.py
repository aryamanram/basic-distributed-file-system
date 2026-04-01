"""
Unit tests for the membership protocol.

Tests the core MembershipList merge logic, incarnation mechanism,
and state transitions without requiring network I/O.
"""

import threading
import time
import unittest

from membership import Logger, MembershipList, MemberStatus


class MockLogger:
    """Captures log messages for test assertions."""

    def __init__(self):
        self.messages = []
        self.lock = threading.Lock()

    def log(self, msg):
        with self.lock:
            self.messages.append(msg)

    def close(self):
        pass


class TestMembershipList(unittest.TestCase):
    """Tests for MembershipList merge logic and state transitions."""

    def setUp(self):
        self.logger = MockLogger()
        self.suspicion_count = 0

        def on_suspicion():
            self.suspicion_count += 1

        self.ml = MembershipList("node-1", "123_node-1_8081", self.logger, on_suspicion)

    def test_initial_state_is_active(self):
        members = self.ml.get_members()
        self.assertEqual(members["node-1"]["status"], "ACTIVE")
        self.assertEqual(members["node-1"]["heartbeat"], 0)
        self.assertEqual(members["node-1"]["incarnation"], 0)

    def test_increment_heartbeat(self):
        self.ml.increment_self_heartbeat()
        self.ml.increment_self_heartbeat()
        members = self.ml.get_members()
        self.assertEqual(members["node-1"]["heartbeat"], 2)

    def test_add_new_member_from_remote(self):
        data = {
            "id": "456_node-2_8082",
            "hostname": "node-2",
            "heartbeat": 5,
            "status": "ACTIVE",
            "incarnation": 0,
        }
        updated = self.ml.update_member_from_remote("node-2", data)
        self.assertTrue(updated)

        members = self.ml.get_members()
        self.assertIn("node-2", members)
        self.assertEqual(members["node-2"]["heartbeat"], 5)

    def test_higher_heartbeat_accepted(self):
        data = {"id": "456_node-2_8082", "hostname": "node-2",
                "heartbeat": 5, "status": "ACTIVE", "incarnation": 0}
        self.ml.update_member_from_remote("node-2", data)

        data2 = {"id": "456_node-2_8082", "hostname": "node-2",
                 "heartbeat": 10, "status": "ACTIVE", "incarnation": 0}
        updated = self.ml.update_member_from_remote("node-2", data2)
        self.assertTrue(updated)
        self.assertEqual(self.ml.get_members()["node-2"]["heartbeat"], 10)

    def test_lower_heartbeat_rejected(self):
        data = {"id": "456_node-2_8082", "hostname": "node-2",
                "heartbeat": 10, "status": "ACTIVE", "incarnation": 0}
        self.ml.update_member_from_remote("node-2", data)

        data2 = {"id": "456_node-2_8082", "hostname": "node-2",
                 "heartbeat": 3, "status": "ACTIVE", "incarnation": 0}
        updated = self.ml.update_member_from_remote("node-2", data2)
        self.assertFalse(updated)
        self.assertEqual(self.ml.get_members()["node-2"]["heartbeat"], 10)

    def test_higher_incarnation_overrides(self):
        data = {"id": "456_node-2_8082", "hostname": "node-2",
                "heartbeat": 10, "status": "ACTIVE", "incarnation": 0}
        self.ml.update_member_from_remote("node-2", data)

        data2 = {"id": "456_node-2_8082", "hostname": "node-2",
                 "heartbeat": 1, "status": "ACTIVE", "incarnation": 1}
        updated = self.ml.update_member_from_remote("node-2", data2)
        self.assertTrue(updated)
        members = self.ml.get_members()
        self.assertEqual(members["node-2"]["incarnation"], 1)
        self.assertEqual(members["node-2"]["heartbeat"], 1)

    def test_incarnation_bump_on_false_suspicion(self):
        """When a remote claims this node is SUSPECTED, incarnation should bump."""
        initial_inc = self.ml.get_members()["node-1"]["incarnation"]

        data = {"id": "123_node-1_8081", "hostname": "node-1",
                "heartbeat": 0, "status": "SUSPECTED", "incarnation": 0}
        self.ml.update_member_from_remote("node-1", data)

        members = self.ml.get_members()
        self.assertEqual(members["node-1"]["incarnation"], initial_inc + 1)
        self.assertEqual(members["node-1"]["status"], "ACTIVE")

    def test_incarnation_bump_on_false_failure(self):
        """When a remote claims this node is FAILED, incarnation should bump."""
        data = {"id": "123_node-1_8081", "hostname": "node-1",
                "heartbeat": 0, "status": "FAILED", "incarnation": 0}
        self.ml.update_member_from_remote("node-1", data)

        members = self.ml.get_members()
        self.assertEqual(members["node-1"]["incarnation"], 1)
        self.assertEqual(members["node-1"]["status"], "ACTIVE")

    def test_mark_suspected_then_failed(self):
        data = {"id": "456_node-2_8082", "hostname": "node-2",
                "heartbeat": 5, "status": "ACTIVE", "incarnation": 0}
        self.ml.update_member_from_remote("node-2", data)

        self.ml.mark_suspected("node-2", "test")
        self.assertEqual(self.ml.get_members()["node-2"]["status"], "SUSPECTED")
        self.assertEqual(self.suspicion_count, 1)

        self.ml.mark_failed("node-2")
        self.assertEqual(self.ml.get_members()["node-2"]["status"], "FAILED")

    def test_mark_left(self):
        data = {"id": "456_node-2_8082", "hostname": "node-2",
                "heartbeat": 5, "status": "ACTIVE", "incarnation": 0}
        self.ml.update_member_from_remote("node-2", data)

        self.ml.mark_left("node-2")
        self.assertEqual(self.ml.get_members()["node-2"]["status"], "LEFT")

    def test_mark_active_does_not_override_left(self):
        data = {"id": "456_node-2_8082", "hostname": "node-2",
                "heartbeat": 5, "status": "ACTIVE", "incarnation": 0}
        self.ml.update_member_from_remote("node-2", data)

        self.ml.mark_left("node-2")
        self.ml.mark_active("node-2")
        self.assertEqual(self.ml.get_members()["node-2"]["status"], "LEFT")

    def test_remove_member(self):
        data = {"id": "456_node-2_8082", "hostname": "node-2",
                "heartbeat": 5, "status": "ACTIVE", "incarnation": 0}
        self.ml.update_member_from_remote("node-2", data)

        self.ml.remove_member("node-2")
        self.assertNotIn("node-2", self.ml.get_members())

    def test_get_members_returns_deep_copy(self):
        members = self.ml.get_members()
        members["node-1"]["heartbeat"] = 999
        self.assertEqual(self.ml.get_members()["node-1"]["heartbeat"], 0)

    def test_suspected_does_not_override_failed(self):
        data = {"id": "456_node-2_8082", "hostname": "node-2",
                "heartbeat": 5, "status": "ACTIVE", "incarnation": 0}
        self.ml.update_member_from_remote("node-2", data)

        self.ml.mark_failed("node-2")
        self.ml.mark_suspected("node-2", "test")
        self.assertEqual(self.ml.get_members()["node-2"]["status"], "FAILED")


if __name__ == "__main__":
    unittest.main()
