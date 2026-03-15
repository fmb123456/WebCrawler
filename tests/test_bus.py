"""Tests for IPC bus abstraction and filesystem backend."""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from libs.ipc.bus import MessageConsumer, MessageProducer, create_producer, create_consumer
from libs.ipc.bus_fs import FsConsumer, FsProducer


class TestInterface(unittest.TestCase):
    def test_fs_implements_interface(self):
        self.assertTrue(issubclass(FsProducer, MessageProducer))
        self.assertTrue(issubclass(FsConsumer, MessageConsumer))

    def test_factory_filesystem(self):
        with tempfile.TemporaryDirectory() as d:
            p = create_producer({"backend": "filesystem", "base_dir": d})
            c = create_consumer({"backend": "filesystem", "base_dir": d}, "g", "c")
            self.assertIsInstance(p, FsProducer)
            self.assertIsInstance(c, FsConsumer)

    def test_factory_default_is_filesystem(self):
        with tempfile.TemporaryDirectory() as d:
            p = create_producer({"base_dir": d})
            self.assertIsInstance(p, FsProducer)

    def test_factory_unknown_backend(self):
        with self.assertRaises(ValueError):
            create_producer({"backend": "unknown"})


class TestFsRoundTrip(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.producer = FsProducer(base_dir=self.tmpdir)
        self.consumer = FsConsumer(base_dir=self.tmpdir)

    def test_send_and_poll(self):
        self.producer.send("t", 0, {"key": "value"})
        msgs = self.consumer.poll("t", 0, max_messages=10)
        self.assertEqual(len(msgs), 1)
        self.assertEqual(msgs[0]["key"], "value")

    def test_poll_empty(self):
        msgs = self.consumer.poll("t", 0, max_messages=10)
        self.assertEqual(msgs, [])

    def test_ordering(self):
        for i in range(5):
            self.producer.send("t", 0, {"seq": i})
        msgs = self.consumer.poll("t", 0, max_messages=10)
        self.assertEqual([m["seq"] for m in msgs], [0, 1, 2, 3, 4])

    def test_poll_deletes_files(self):
        self.producer.send("t", 0, {"a": 1})
        self.consumer.poll("t", 0, max_messages=10)
        self.assertEqual(self.consumer.poll("t", 0, max_messages=10), [])

    def test_max_messages(self):
        for i in range(10):
            self.producer.send("t", 0, {"i": i})
        msgs = self.consumer.poll("t", 0, max_messages=3)
        self.assertEqual(len(msgs), 3)
        remaining = self.consumer.poll("t", 0, max_messages=10)
        self.assertEqual(len(remaining), 7)

    def test_partitions_isolated(self):
        self.producer.send("t", 0, {"p": 0})
        self.producer.send("t", 1, {"p": 1})
        self.assertEqual(self.consumer.poll("t", 0, max_messages=10)[0]["p"], 0)
        self.assertEqual(self.consumer.poll("t", 1, max_messages=10)[0]["p"], 1)

    def test_topics_isolated(self):
        self.producer.send("a", 0, {"t": "a"})
        self.producer.send("b", 0, {"t": "b"})
        self.assertEqual(self.consumer.poll("a", 0, max_messages=10)[0]["t"], "a")
        self.assertEqual(self.consumer.poll("b", 0, max_messages=10)[0]["t"], "b")

    def test_send_batch(self):
        self.producer.send_batch("t", 0, [{"i": i} for i in range(5)])
        msgs = self.consumer.poll("t", 0, max_messages=10)
        self.assertEqual(len(msgs), 5)

    def test_no_tmp_files(self):
        self.producer.send("t", 0, {"k": "v"})
        d = Path(self.tmpdir) / "t" / "00"
        self.assertEqual(list(d.glob("*.tmp")), [])

    def test_large_payload(self):
        payload = {"urls": [f"https://example.com/{i}" for i in range(512)]}
        self.producer.send("q", 0, payload)
        msgs = self.consumer.poll("q", 0, max_messages=1)
        self.assertEqual(len(msgs[0]["urls"]), 512)

    def test_unicode(self):
        self.producer.send("q", 0, {"title": "Hello world"})
        msgs = self.consumer.poll("q", 0, max_messages=1)
        self.assertEqual(msgs[0]["title"], "Hello world")


if __name__ == "__main__":
    unittest.main()
