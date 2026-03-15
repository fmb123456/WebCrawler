"""
IPC message bus abstraction.

Provides a backend-agnostic interface for producing and consuming messages.
Backend is selected via config:

    ipc:
      backend: "filesystem"   # default, uses existing JSON/JSONL files
      # backend: "redis"      # optional, requires `pip install webcrawler[redis]`
"""
from __future__ import annotations

from abc import ABC, abstractmethod


class MessageProducer(ABC):
    @abstractmethod
    def send(self, topic: str, partition: int, payload: dict) -> None:
        raise NotImplementedError

    @abstractmethod
    def send_batch(self, topic: str, partition: int, payloads: list[dict]) -> None:
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError


class MessageConsumer(ABC):
    @abstractmethod
    def poll(self, topic: str, partition: int, max_messages: int) -> list[dict]:
        raise NotImplementedError

    @abstractmethod
    def commit(self, topic: str, partition: int) -> None:
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError


def create_producer(config: dict) -> MessageProducer:
    """Factory: create a producer from config dict.

    config = {"backend": "filesystem", "base_dir": "/data/ipc"}
    config = {"backend": "redis", "url": "redis://redis:6379/0"}
    """
    backend = config.get("backend", "filesystem")

    if backend == "filesystem":
        from .bus_fs import FsProducer
        return FsProducer(base_dir=config.get("base_dir", "/data/ipc"))

    if backend == "redis":
        from .bus_redis import RedisProducer, make_redis_client
        client = make_redis_client(config.get("url", "redis://redis:6379/0"))
        return RedisProducer(client)

    raise ValueError(f"Unknown IPC backend: {backend}")


def create_consumer(config: dict, group: str, consumer_name: str) -> MessageConsumer:
    """Factory: create a consumer from config dict."""
    backend = config.get("backend", "filesystem")

    if backend == "filesystem":
        from .bus_fs import FsConsumer
        return FsConsumer(base_dir=config.get("base_dir", "/data/ipc"))

    if backend == "redis":
        from .bus_redis import RedisConsumer, make_redis_client
        client = make_redis_client(config.get("url", "redis://redis:6379/0"))
        return RedisConsumer(client, group=group, consumer_name=consumer_name)

    raise ValueError(f"Unknown IPC backend: {backend}")
