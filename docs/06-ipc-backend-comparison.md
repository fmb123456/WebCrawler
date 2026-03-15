# 06. IPC Backend Comparison

## Before: Filesystem IPC

```mermaid
flowchart TD
    subgraph scheduler_control["scheduler_control"]
        Offerer["Offerer"]
    end

    subgraph crawler_container["crawler"]
        QueueConsumer["QueueConsumer"]
        Scrapy["Scrapy Spider"]
        Pipeline["JsonPipeline"]
        QueueConsumer --> Scrapy --> Pipeline
    end

    subgraph scheduler_ingest["scheduler_ingest"]
        Router["Router"]
        Ingestor["Ingestor"]
        Extractor["Extractor"]
        Stats["Stats"]
    end

    DB[("PostgreSQL")]
    IPC_Queue["/data/ipc/url_queue/"]
    IPC_Crawl["/data/ipc/crawl_result/crawler_XX/"]
    IPC_Ingest["/data/ipc/crawl_result/ingestor_XX/"]
    IPC_Stats["/data/ipc/stats/"]

    DB --> Offerer --> IPC_Queue --> QueueConsumer
    Pipeline --> IPC_Crawl --> Router --> IPC_Ingest
    IPC_Ingest --> Ingestor --> DB
    IPC_Ingest --> Extractor --> DB

    Offerer & Pipeline & Router & Ingestor -.->|"delta"| IPC_Stats -.-> Stats -.-> DB
```

## After: Redis Stream IPC (feature flag)

```mermaid
flowchart TD
    subgraph scheduler_control["scheduler_control"]
        Offerer["Offerer"]
    end

    subgraph crawler_container["crawler"]
        Scrapy["Scrapy Spider"]
    end

    subgraph scheduler_ingest["scheduler_ingest"]
        Router["Router"]
        Ingestor["Ingestor"]
        Extractor["Extractor"]
        Stats["Stats"]
    end

    DB[("PostgreSQL")]
    Redis[("Redis Streams")]

    DB --> Offerer -->|"url_queue"| Redis -->|"url_queue"| Scrapy
    Scrapy -->|"crawl_result"| Redis -->|"crawl_result"| Router
    Router -->|"ingest_input"| Redis
    Redis -->|"group: ingestor"| Ingestor --> DB
    Redis -->|"group: extractor"| Extractor --> DB

    Offerer & Scrapy & Router & Ingestor -.->|"stats_delta"| Redis -.-> Stats -.-> DB
```

## Comparison

| | Filesystem | Redis Stream |
|---|---|---|
| Produce | 3,002 msg/sec | 3,716 msg/sec (+24%) |
| Consume | 3,847 msg/sec | 18,692 msg/sec (+386%) |
| E2E latency p50 | 4.64 ms | 1.03 ms (4.5x) |
| Pipeline delay | 20+ min (folder wait) | < 1 sec |
| Delivery guarantee | Read-and-delete, crash loses data | ACK-based, crash safe |
| Consumer groups | N/A, Ingestor and Extractor read same files sequentially | Independent groups on same stream |
| Monitoring | None | XINFO GROUPS for consumer lag |
| Cleanup | Manual, ingestor_XX never deleted | Automatic retention |
| Extra dependency | None | Redis container |

## How to Switch

Config-based feature flag in `libs/ipc/bus.py`:

```yaml
# Filesystem (default, no extra deps)
ipc:
  backend: "filesystem"
  base_dir: "/data/ipc"

# Redis (requires pip install webcrawler[redis])
ipc:
  backend: "redis"
  url: "redis://redis:6379/0"
```

```python
from libs.ipc.bus import create_producer, create_consumer

producer = create_producer(config["ipc"])
consumer = create_consumer(config["ipc"], group="router", consumer_name="router_00")
```
