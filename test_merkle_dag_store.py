"""Tests for Merkle DAG Store"""
import os
import shutil
import tempfile
import unittest

from merkle_dag_store import MerkleDAGStore, HashAlgorithm


class TestMerkleDAGStore(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.store = MerkleDAGStore(
            os.path.join(self.test_dir, "dag"),
            hash_algo=HashAlgorithm.SHA256
        )

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_put_and_get(self):
        data = b"Hello, Merkle!"
        cid = self.store.put(data)
        retrieved = self.store.get(str(cid))
        self.assertEqual(data, retrieved)

    def test_deduplication(self):
        data = b"dedup test"
        cid1 = self.store.put(data)
        cid2 = self.store.put(data)
        self.assertEqual(str(cid1), str(cid2))

    def test_get_nonexistent(self):
        result = self.store.get("nonexistent_cid")
        self.assertIsNone(result)

    def test_linked_blocks(self):
        cid1 = self.store.put(b"child 1")
        cid2 = self.store.put(b"child 2")
        parent_cid = self.store.put(b"parent", links=[str(cid1), str(cid2)])

        links = self.store.get_links(str(parent_cid))
        self.assertEqual(len(links), 2)
        self.assertIn(str(cid1), links)
        self.assertIn(str(cid2), links)

    def test_link_method(self):
        cid1 = self.store.put(b"block 1")
        cid2 = self.store.put(b"block 2")
        self.assertTrue(self.store.link(str(cid1), str(cid2)))

        links = self.store.get_links(str(cid1))
        self.assertIn(str(cid2), links)

    def test_link_nonexistent_parent(self):
        cid = self.store.put(b"block")
        with self.assertRaises(ValueError):
            self.store.link("nonexistent", str(cid))

    def test_block_info(self):
        cid = self.store.put(b"info test")
        info = self.store.get_block_info(str(cid))
        self.assertIsNotNone(info)
        self.assertEqual(info['size'], len(b"info test"))

    def test_snapshot(self):
        cid = self.store.put(b"snapshot root")
        snap_id = self.store.snapshot(str(cid), metadata={"v": "1.0"}, tag="v1.0")
        self.assertIsNotNone(snap_id)

        snap = self.store.get_snapshot("v1.0")
        self.assertIsNotNone(snap)
        self.assertEqual(snap.root_cid, str(cid))

    def test_list_snapshots(self):
        cid = self.store.put(b"snap test")
        self.store.snapshot(str(cid), tag="snap1")
        self.store.snapshot(str(cid), tag="snap2")
        snaps = self.store.list_snapshots()
        self.assertEqual(len(snaps), 2)

    def test_verify(self):
        cid = self.store.put(b"verify test")
        self.assertTrue(self.store.verify(str(cid)))

    def test_verify_recursive(self):
        cid1 = self.store.put(b"child")
        cid2 = self.store.put(b"parent", links=[str(cid1)])
        self.assertTrue(self.store.verify(str(cid2), recursive=True))

    def test_traverse_dag(self):
        cid1 = self.store.put(b"leaf 1")
        cid2 = self.store.put(b"leaf 2")
        root = self.store.put(b"root", links=[str(cid1), str(cid2)])

        reachable = self.store.traverse_dag(str(root))
        self.assertEqual(len(reachable), 3)

    def test_gc(self):
        cid1 = self.store.put(b"keep")
        cid2 = self.store.put(b"garbage")
        self.store.snapshot(str(cid1), tag="keeper")

        removed = self.store.gc(keep_snapshots=10)
        self.assertGreaterEqual(removed, 0)

    def test_stats(self):
        self.store.put(b"stats test")
        stats = self.store.stats()
        self.assertEqual(stats['block_count'], 1)
        self.assertGreater(stats['total_size_bytes'], 0)

    def test_type_error(self):
        with self.assertRaises(TypeError):
            self.store.put("not bytes")

    def test_different_hash_algorithms(self):
        for algo in HashAlgorithm:
            store = MerkleDAGStore(
                os.path.join(self.test_dir, f"dag_{algo.value}"),
                hash_algo=algo
            )
            cid = store.put(b"test")
            data = store.get(str(cid))
            self.assertEqual(data, b"test")


if __name__ == "__main__":
    unittest.main()
