"""Redis Stream-backed IPC backend. Requires `pip install webcrawler[redis]`."""
from __future__ import annotations

import json

try:
    import redis
except ImportError:
    raise ImportError(
        "Redis backend requires the redis package. "
        "Install with: pip install webcrawler[redis]"
    )

from .bus import MessageConsumer, MessageProducer


def make_redis_client(url: str = "redis://redis:6379/0") -> redis.Redis:
    return redis.Redis.from_url(url, decode_responses=False)


class RedisProducer(MessageProducer):
    """Produces messages to a Redis Stream."""

    def __init__(self, client: redis.Redis):
        self.client = client

    def _key(self, topic: str, partition: int) -> str:
        return f"{topic}:{partition:02d}"

    def send(self, topic: str, partition: int, payload: dict) -> None:
        data = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        self.client.xadd(self._key(topic, partition), {"data": data})

    def send_batch(self, topic: str, partition: int, payloads: list[dict]) -> None:
        pipe = self.client.pipeline()
        key = self._key(topic, partition)
        for p in payloads:
            data = json.dumps(p, ensure_ascii=False, separators=(",", ":"))
            pipe.xadd(key, {"data": data})
        pipe.execute()

    def close(self) -> None:
        pass


class RedisConsumer(MessageConsumer):
    """Consumes messages from a Redis Stream using consumer groups."""

    def __init__(self, client: redis.Redis, group: str, consumer_name: str):
        self.client = client
        self.group = group
        self.consumer_name = consumer_name
        self._ensured: set[str] = set()

    def _key(self, topic: str, partition: int) -> str:
        return f"{topic}:{partition:02d}"

    def _ensure_group(self, key: str) -> None:
        if key in self._ensured:
            return
        try:
            self.client.xgroup_create(key, self.group, id="0", mkstream=True)
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise
        self._ensured.add(key)

    def poll(self, topic: str, partition: int, max_messages: int, block_ms: int = 1000) -> list[dict]:
        key = self._key(topic, partition)
        self._ensure_group(key)

        results_raw = self.client.xreadgroup(
            self.group, self.consumer_name,
            {key: ">"}, count=max_messages, block=block_ms,
        )
        if not results_raw:
            return []

        messages = []
        for _, entries in results_raw:
            for msg_id, fields in entries:
                try:
                    payload = json.loads(fields[b"data"].decode("utf-8"))
                    messages.append(payload)
                except (json.JSONDecodeError, KeyError, UnicodeDecodeError):
                    self.client.xack(key, self.group, msg_id)
        return messages

    def commit(self, topic: str, partition: int) -> None:
        pass

    def close(self) -> None:
        pass
