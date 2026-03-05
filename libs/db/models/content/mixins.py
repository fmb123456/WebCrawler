from __future__ import annotations

from sqlalchemy import BigInteger, Column, Integer, String
from sqlalchemy.sql import func
from sqlalchemy import DateTime


class ContentFeatureMixin:
    """
    Shared columns for content_feature_current_XXX and content_feature_history_XXX.

    Schema (current/history):
      - url: varchar (PK only in current; non-null in history)
      - domain_id: bigint NOT NULL
      - fetched_at: timestamptz default now()
      - content_length: int default 0
      - content_hash: varchar nullable
      - num_links: int default 0
    """
    domain_id = Column(BigInteger, nullable=False)

    fetched_at = Column(DateTime(timezone=True), server_default=func.now())

    content_length = Column(Integer, default=0)
    content_hash = Column(String)
    num_links = Column(Integer, default=0)
