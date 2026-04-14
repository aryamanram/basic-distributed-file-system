"""
Unit tests for ReplicatedFS core components.

Tests the consistent hashing ring, block-based storage, and consistency
manager without requiring network I/O or running nodes.
"""

import os
import shutil
import threading
import time
import unittest

from .ring import ConsistentHashRing
from .storage import FileStorage, FileBlock
from .consistency import ConsistencyManager, MergeCoordinator, ClientSequenceTracker
from .utils import hash_to_ring, get_file_id


class MockLogger:
    """Captures log messages for test assertions."""

    def __init__(self):
        self.messages = []
        self.lock = threading.Lock()

    def log(self, msg):
        with self.lock:
            self.messages.append(msg)


# ========================= Ring Tests =========================

class TestConsistentHashRing(unittest.TestCase):
    """Tests for the consistent hashing ring."""

    def setUp(self):
        self.ring = ConsistentHashRing()

    def test_add_node(self):
        pos = self.ring.add_node("node-1:7001:12345")
        self.assertIsInstance(pos, int)
        self.assertEqual(self.ring.get_node_count(), 1)

    def test_remove_node(self):
        self.ring.add_node("node-1:7001:12345")
        self.assertTrue(self.ring.remove_node("node-1:7001:12345"))
        self.assertEqual(self.ring.get_node_count(), 0)

    def test_remove_nonexistent_node(self):
        self.assertFalse(self.ring.remove_node("node-99:7001:12345"))

    def test_get_successors_single_node(self):
        self.ring.add_node("node-1:7001:12345")
        successors = self.ring.get_successors("testfile.txt", 3)
        self.assertEqual(len(successors), 1)
        self.assertEqual(successors[0], "node-1:7001:12345")

    def test_get_successors_multiple_nodes(self):
        for i in range(5):
            self.ring.add_node(f"node-{i}:7001:{i}")
        successors = self.ring.get_successors("testfile.txt", 3)
        self.assertEqual(len(successors), 3)
        # All successors should be unique
        self.assertEqual(len(set(successors)), 3)

    def test_get_successors_empty_ring(self):
        successors = self.ring.get_successors("testfile.txt", 3)
        self.assertEqual(successors, [])

    def test_get_node_position(self):
        self.ring.add_node("node-1:7001:12345")
        pos = self.ring.get_node_position("node-1:7001:12345")
        self.assertIsNotNone(pos)

    def test_get_all_nodes(self):
        for i in range(3):
            self.ring.add_node(f"node-{i}:7001:{i}")
        nodes = self.ring.get_all_nodes()
        self.assertEqual(len(nodes), 3)
        # Should be sorted by position
        positions = [pos for pos, _ in nodes]
        self.assertEqual(positions, sorted(positions))

    def test_deterministic_placement(self):
        """Same file should always map to the same successors."""
        for i in range(5):
            self.ring.add_node(f"node-{i}:7001:{i}")
        s1 = self.ring.get_successors("testfile.txt", 3)
        s2 = self.ring.get_successors("testfile.txt", 3)
        self.assertEqual(s1, s2)

    def test_minimal_disruption(self):
        """Adding a node should change assignments minimally."""
        for i in range(5):
            self.ring.add_node(f"node-{i}:7001:{i}")
        before = self.ring.get_successors("testfile.txt", 3)

        self.ring.add_node("node-new:7001:999")
        after = self.ring.get_successors("testfile.txt", 3)

        # At least some successors should remain the same
        overlap = set(before) & set(after)
        self.assertGreater(len(overlap), 0)


# ========================= Storage Tests =========================

class TestFileStorage(unittest.TestCase):
    """Tests for block-based file storage."""

    def setUp(self):
        self.storage_dir = "test_rfs_storage"
        self.storage = FileStorage(self.storage_dir)

    def tearDown(self):
        if os.path.exists(self.storage_dir):
            shutil.rmtree(self.storage_dir)

    def test_create_file(self):
        success, msg = self.storage.create_file("test.txt", b"hello world")
        self.assertTrue(success)
        self.assertTrue(self.storage.file_exists("test.txt"))

    def test_create_duplicate_file(self):
        self.storage.create_file("test.txt", b"hello")
        success, msg = self.storage.create_file("test.txt", b"world")
        self.assertFalse(success)

    def test_get_file_data(self):
        self.storage.create_file("test.txt", b"hello world")
        data = self.storage.get_file_data("test.txt")
        self.assertEqual(data, b"hello world")

    def test_get_nonexistent_file(self):
        data = self.storage.get_file_data("nonexistent.txt")
        self.assertIsNone(data)

    def test_append_block(self):
        self.storage.create_file("test.txt", b"hello")
        block = FileBlock(
            block_id="block_1",
            client_id="client_1",
            sequence_num=0,
            timestamp=time.time(),
            data=b" world",
            size=6
        )
        success, msg = self.storage.append_block("test.txt", block)
        self.assertTrue(success)
        data = self.storage.get_file_data("test.txt")
        self.assertEqual(data, b"hello world")

    def test_list_files(self):
        self.storage.create_file("a.txt", b"aaa")
        self.storage.create_file("b.txt", b"bbb")
        files = self.storage.list_files()
        self.assertEqual(len(files), 2)
        filenames = {f[0] for f in files}
        self.assertIn("a.txt", filenames)
        self.assertIn("b.txt", filenames)

    def test_delete_file(self):
        self.storage.create_file("test.txt", b"hello")
        self.assertTrue(self.storage.delete_file("test.txt"))
        self.assertFalse(self.storage.file_exists("test.txt"))

    def test_replace_file_blocks(self):
        self.storage.create_file("test.txt", b"original")
        new_blocks = [
            FileBlock("b1", "c1", 0, 1.0, b"replaced", 8)
        ]
        self.assertTrue(self.storage.replace_file_blocks("test.txt", new_blocks))
        data = self.storage.get_file_data("test.txt")
        self.assertEqual(data, b"replaced")

    def test_persistence(self):
        """Files should survive a storage reload."""
        self.storage.create_file("test.txt", b"persistent data")
        # Create a new storage instance pointing to the same dir
        storage2 = FileStorage(self.storage_dir)
        data = storage2.get_file_data("test.txt")
        self.assertEqual(data, b"persistent data")


# ========================= Consistency Tests =========================

class TestClientSequenceTracker(unittest.TestCase):
    """Tests for per-client sequence tracking."""

    def test_monotonic_sequences(self):
        tracker = ClientSequenceTracker()
        for expected in range(5):
            self.assertEqual(tracker.get_next_sequence("client-1"), expected)

    def test_independent_clients(self):
        tracker = ClientSequenceTracker()
        self.assertEqual(tracker.get_next_sequence("client-1"), 0)
        self.assertEqual(tracker.get_next_sequence("client-2"), 0)
        self.assertEqual(tracker.get_next_sequence("client-1"), 1)
        self.assertEqual(tracker.get_next_sequence("client-2"), 1)

    def test_reset_client(self):
        tracker = ClientSequenceTracker()
        tracker.get_next_sequence("client-1")
        tracker.get_next_sequence("client-1")
        tracker.reset_client("client-1")
        self.assertEqual(tracker.get_next_sequence("client-1"), 0)


class TestConsistencyManager(unittest.TestCase):
    """Tests for consistency management and merge operations."""

    def setUp(self):
        self.logger = MockLogger()
        self.cm = ConsistencyManager(self.logger)

    def test_pending_writes(self):
        self.cm.mark_write_pending("file.txt", "client-1")
        self.assertTrue(self.cm.has_pending_writes("file.txt", "client-1"))
        self.assertFalse(self.cm.has_pending_writes("file.txt", "client-2"))

    def test_complete_writes(self):
        self.cm.mark_write_pending("file.txt", "client-1")
        self.cm.mark_write_complete("file.txt", "client-1")
        self.assertFalse(self.cm.has_pending_writes("file.txt", "client-1"))

    def test_merge_deduplication(self):
        """Merge should deduplicate blocks by block_id."""
        block = FileBlock("b1", "c1", 0, 1.0, b"data", 4)
        # Same block appears in two replica lists
        merged = self.cm.merge_blocks([[block], [block]])
        self.assertEqual(len(merged), 1)

    def test_merge_ordering(self):
        """Merge should sort by (client_id, sequence_num, timestamp)."""
        b1 = FileBlock("b1", "client-a", 0, 1.0, b"a0", 2)
        b2 = FileBlock("b2", "client-a", 1, 2.0, b"a1", 2)
        b3 = FileBlock("b3", "client-b", 0, 1.5, b"b0", 2)
        merged = self.cm.merge_blocks([[b2, b3], [b1]])
        ids = [b.block_id for b in merged]
        self.assertEqual(ids, ["b1", "b2", "b3"])

    def test_validate_ordering_valid(self):
        blocks = [
            FileBlock("b1", "c1", 0, 1.0, b"", 0),
            FileBlock("b2", "c1", 1, 2.0, b"", 0),
            FileBlock("b3", "c2", 0, 1.5, b"", 0),
        ]
        self.assertTrue(self.cm.validate_block_ordering(blocks))

    def test_validate_ordering_invalid(self):
        blocks = [
            FileBlock("b1", "c1", 1, 1.0, b"", 0),
            FileBlock("b2", "c1", 0, 2.0, b"", 0),  # Out of order
        ]
        self.assertFalse(self.cm.validate_block_ordering(blocks))

    def test_detect_conflicts_gap(self):
        blocks = [
            FileBlock("b1", "c1", 0, 1.0, b"", 0),
            FileBlock("b2", "c1", 3, 2.0, b"", 0),  # Gap: 0 -> 3
        ]
        conflicts = self.cm.detect_conflicts(blocks)
        self.assertGreater(len(conflicts), 0)

    def test_detect_conflicts_clean(self):
        blocks = [
            FileBlock("b1", "c1", 0, 1.0, b"", 0),
            FileBlock("b2", "c1", 1, 2.0, b"", 0),
        ]
        conflicts = self.cm.detect_conflicts(blocks)
        self.assertEqual(len(conflicts), 0)


class TestMergeCoordinator(unittest.TestCase):
    """Tests for merge coordination."""

    def setUp(self):
        self.logger = MockLogger()
        self.mc = MergeCoordinator(self.logger)

    def test_start_merge(self):
        self.assertTrue(self.mc.start_merge("file.txt"))
        self.assertTrue(self.mc.is_merge_in_progress("file.txt"))

    def test_concurrent_merge_blocked(self):
        self.assertTrue(self.mc.start_merge("file.txt"))
        self.assertFalse(self.mc.start_merge("file.txt"))

    def test_finish_merge(self):
        self.mc.start_merge("file.txt")
        self.mc.finish_merge("file.txt")
        self.assertFalse(self.mc.is_merge_in_progress("file.txt"))


# ========================= Utility Tests =========================

class TestUtils(unittest.TestCase):
    """Tests for utility functions."""

    def test_hash_deterministic(self):
        h1 = hash_to_ring("test")
        h2 = hash_to_ring("test")
        self.assertEqual(h1, h2)

    def test_hash_different_keys(self):
        h1 = hash_to_ring("key1")
        h2 = hash_to_ring("key2")
        self.assertNotEqual(h1, h2)

    def test_file_id_deterministic(self):
        id1 = get_file_id("test.txt")
        id2 = get_file_id("test.txt")
        self.assertEqual(id1, id2)

    def test_file_id_length(self):
        fid = get_file_id("test.txt")
        self.assertEqual(len(fid), 16)


if __name__ == '__main__':
    unittest.main()
