"""
Initialize database schema: create all sharded and non-sharded tables.

Usage:
  python -m scripts.init_db --dsn "postgresql+psycopg2://crawler:crawler@localhost:5432/crawlerdb"
"""
from __future__ import annotations

import argparse

from sqlalchemy import create_engine, text

from libs.db.base import Base
from libs.db.models.domain.state import DomainState
from libs.db.models.domain.stats_daily import DomainStatsDaily
from libs.db.models.summary.daily import SummaryDaily
from libs.db.models.link.url_link import UrlLink
from libs.db.sharding.table_factory import (
    url_state_current_table,
    url_state_history_table,
    url_event_counter_table,
    content_feature_current_table,
    content_feature_history_table,
)

NUM_SHARDS = 256


def main() -> None:
    ap = argparse.ArgumentParser(description="Initialize database schema")
    ap.add_argument("--dsn", required=True)
    args = ap.parse_args()

    engine = create_engine(args.dsn)

    # Register all sharded tables so Base.metadata knows about them.
    for shard_id in range(NUM_SHARDS):
        url_state_current_table(shard_id)
        url_state_history_table(shard_id)
        url_event_counter_table(shard_id)
        content_feature_current_table(shard_id)
        content_feature_history_table(shard_id)

    Base.metadata.create_all(engine)
    print(f"Created {len(Base.metadata.tables)} tables.")

    # Add sequence for history snapshot_id if not exists.
    with engine.connect() as conn:
        conn.execute(text(
            "CREATE SEQUENCE IF NOT EXISTS url_state_history_snapshot_id_seq"
        ))
        conn.execute(text(
            "CREATE SEQUENCE IF NOT EXISTS content_feature_history_snapshot_id_seq"
        ))
        conn.commit()

    print("Done.")


if __name__ == "__main__":
    main()
