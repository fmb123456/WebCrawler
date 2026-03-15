"""
Seed URLs into Redis Stream for initial crawl.

Usage:
  python -m scripts.seed_urls --urls-file urls.txt --redis-url redis://localhost:6379/0 --num-crawlers 16 --batch-size 512
"""
from __future__ import annotations

import argparse
from collections import defaultdict

from libs.ipc.redis_stream import RedisProducer, make_redis_client


def main() -> None:
    ap = argparse.ArgumentParser(description="Seed URLs into Redis queue")
    ap.add_argument("--urls-file", required=True)
    ap.add_argument("--redis-url", default="redis://localhost:6379/0")
    ap.add_argument("--num-crawlers", type=int, default=16)
    ap.add_argument("--batch-size", type=int, default=512)
    args = ap.parse_args()

    with open(args.urls_file) as f:
        urls = [line.strip() for line in f if line.strip()]

    print(f"Loaded {len(urls)} URLs from {args.urls_file}")

    client = make_redis_client(args.redis_url)
    producer = RedisProducer(client)

    buckets: dict[int, list[str]] = defaultdict(list)
    for i, url in enumerate(urls):
        crawler_id = i % args.num_crawlers
        buckets[crawler_id].append(url)

    total_msgs = 0
    for crawler_id, crawler_urls in sorted(buckets.items()):
        for i in range(0, len(crawler_urls), args.batch_size):
            batch = crawler_urls[i:i + args.batch_size]
            producer.send("url_queue", crawler_id, {"urls": batch})
            total_msgs += 1

        print(f"  crawler_{crawler_id:02d}: {len(crawler_urls)} URLs")

    print(f"Total: {len(urls)} URLs, {total_msgs} messages")


if __name__ == "__main__":
    main()
