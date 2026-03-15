"""Benchmark: filesystem vs redis IPC backend."""
from __future__ import annotations

import tempfile
import time
import threading
import statistics

from libs.ipc.bus import create_producer, create_consumer


def make_payload(batch_size: int = 512) -> dict:
    return {"urls": [f"https://example.com/{i}" for i in range(batch_size)]}


def bench_produce(config: dict, count: int, payload: dict) -> float:
    p = create_producer(config)
    start = time.perf_counter()
    for _ in range(count):
        p.send("bench", 0, payload)
    elapsed = time.perf_counter() - start
    p.close()
    return count / elapsed


def bench_consume(config: dict, count: int) -> float:
    c = create_consumer(config, "bench_group", "bench_consumer")
    consumed = 0
    start = time.perf_counter()
    while consumed < count:
        msgs = c.poll("bench", 0, max_messages=100)
        if not msgs:
            break
        consumed += len(msgs)
    elapsed = time.perf_counter() - start
    c.close()
    return consumed / elapsed if elapsed > 0 else 0


def bench_e2e(config: dict, count: int, payload: dict) -> dict:
    latencies = []

    def producer():
        p = create_producer(config)
        for i in range(count):
            p.send("bench_e2e", 0, {**payload, "_ts": time.perf_counter()})
            time.sleep(0.001)
        p.close()

    def consumer():
        c = create_consumer(config, "bench_e2e_group", "bench_e2e_consumer")
        consumed = 0
        empty = 0
        while consumed < count:
            msgs = c.poll("bench_e2e", 0, max_messages=10)
            if not msgs:
                empty += 1
                if empty > 500:
                    break
                time.sleep(0.005)
                continue
            empty = 0
            now = time.perf_counter()
            for m in msgs:
                ts = m.get("_ts")
                if ts:
                    latencies.append(now - ts)
                consumed += 1
        c.close()

    t_con = threading.Thread(target=consumer)
    t_pro = threading.Thread(target=producer)
    t_con.start()
    time.sleep(0.2)
    t_pro.start()
    t_pro.join()
    t_con.join()

    if not latencies:
        return {"p50": 0, "p99": 0, "max": 0}

    s = sorted(latencies)
    return {
        "p50": s[len(s) // 2] * 1000,
        "p99": s[int(len(s) * 0.99)] * 1000,
        "max": s[-1] * 1000,
    }


def run_all():
    count = 5000
    e2e_count = 500
    payload = make_payload(512)

    configs = {
        "filesystem": {"backend": "filesystem", "base_dir": tempfile.mkdtemp()},
        "redis": {"backend": "redis", "url": "redis://localhost:6379/0"},
    }

    # Clear redis
    import redis as r
    r.Redis.from_url("redis://localhost:6379/0").flushall()

    print(f"=== Produce {count} msgs (512 URLs each) ===")
    for name, cfg in configs.items():
        rate = bench_produce(cfg, count, payload)
        print(f"  {name:12s}: {rate:,.0f} msgs/sec")

    print(f"\n=== Consume {count} msgs ===")
    # Re-produce for filesystem (consumed during produce bench above? no, different topic)
    # Actually produce bench wrote to "bench" topic, consume reads from same
    # For filesystem, produce again since we need messages
    fs_cfg = {"backend": "filesystem", "base_dir": tempfile.mkdtemp()}
    for _ in range(count):
        create_producer(fs_cfg).send("bench", 0, payload)

    for name, cfg in [("filesystem", fs_cfg), ("redis", configs["redis"])]:
        rate = bench_consume(cfg, count)
        print(f"  {name:12s}: {rate:,.0f} msgs/sec")

    # Clear redis for e2e
    r.Redis.from_url("redis://localhost:6379/0").flushall()

    print(f"\n=== E2E latency {e2e_count} msgs ===")
    for name, cfg in configs.items():
        if name == "filesystem":
            cfg = {"backend": "filesystem", "base_dir": tempfile.mkdtemp()}
        lat = bench_e2e(cfg, e2e_count, payload)
        print(f"  {name:12s}: p50={lat['p50']:.2f}ms  p99={lat['p99']:.2f}ms  max={lat['max']:.2f}ms")


if __name__ == "__main__":
    run_all()
