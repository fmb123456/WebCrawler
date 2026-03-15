"""Filesystem-backed IPC backend. Wraps existing jsonio utilities."""
from __future__ import annotations

import json
import os
import threading
import time
import uuid
from pathlib import Path

from .bus import MessageConsumer, MessageProducer

_counter_lock = threading.Lock()
_counter = 0


def _next_counter() -> int:
    global _counter
    with _counter_lock:
        _counter += 1
        return _counter


class FsProducer(MessageProducer):
    """
    Writes each message as an atomic JSON file:
      {base_dir}/{topic}/{partition:02d}/{ts}_{seq}_{uuid}.json
    """

    def __init__(self, base_dir: str):
        self.base_dir = Path(base_dir)

    def _topic_dir(self, topic: str, partition: int) -> Path:
        return self.base_dir / topic / f"{partition:02d}"

    def send(self, topic: str, partition: int, payload: dict) -> None:
        d = self._topic_dir(topic, partition)
        d.mkdir(parents=True, exist_ok=True)

        seq = _next_counter()
        name = f"{int(time.time())}_{seq:010d}_{uuid.uuid4()}.json"
        final = d / name
        tmp = final.with_suffix(".json.tmp")

        data = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        with open(tmp, "wb") as f:
            f.write(data)
            f.flush()
        os.replace(tmp, final)

    def send_batch(self, topic: str, partition: int, payloads: list[dict]) -> None:
        for p in payloads:
            self.send(topic, partition, p)

    def close(self) -> None:
        pass


class FsConsumer(MessageConsumer):
    """
    Reads JSON files in FIFO order (sorted by filename), deletes after read.
    """

    def __init__(self, base_dir: str):
        self.base_dir = Path(base_dir)

    def _topic_dir(self, topic: str, partition: int) -> Path:
        return self.base_dir / topic / f"{partition:02d}"

    def poll(self, topic: str, partition: int, max_messages: int) -> list[dict]:
        d = self._topic_dir(topic, partition)
        if not d.exists():
            return []

        files = sorted(d.glob("*.json"))[:max_messages]
        results: list[dict] = []

        for f in files:
            try:
                with open(f, "rb") as fh:
                    results.append(json.loads(fh.read().decode("utf-8")))
                f.unlink()
            except (FileNotFoundError, json.JSONDecodeError):
                continue

        return results

    def commit(self, topic: str, partition: int) -> None:
        pass

    def close(self) -> None:
        pass
