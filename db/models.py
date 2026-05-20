"""
ORM model cho bảng `data_sources` — scraper staging table.

Schema phải đồng bộ với `data_source.sql`. Khi sửa cột ở DB phải sửa song song
ở đây (không có Alembic auto-detect).
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    Index,
    Numeric,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from .session import Base


class DataSource(Base):
    """1 row = 1 listing đã crawl từ portal ngoài (nhatot / muaban)."""

    __tablename__ = "data_sources"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=func.gen_random_uuid(),
    )

    source: Mapped[str] = mapped_column(String(20), nullable=False)
    source_id: Mapped[str] = mapped_column(String(100), nullable=False)
    source_url: Mapped[Optional[str]] = mapped_column(Text)

    title: Mapped[Optional[str]] = mapped_column(String(500))
    description: Mapped[Optional[str]] = mapped_column(Text)
    raw_data: Mapped[Optional[dict]] = mapped_column(JSONB)

    property_type: Mapped[Optional[str]] = mapped_column(String(50))
    transaction_type: Mapped[Optional[str]] = mapped_column(String(20), default="ban")

    poster_type: Mapped[Optional[str]] = mapped_column(String(20))
    poster_confidence_score: Mapped[Optional[int]] = mapped_column(SmallInteger)
    poster_classification_reason: Mapped[Optional[str]] = mapped_column(Text)

    price: Mapped[Optional[int]] = mapped_column(BigInteger)
    area: Mapped[Optional[float]] = mapped_column(Numeric(10, 2))

    province: Mapped[Optional[str]] = mapped_column(String(100))
    ward: Mapped[Optional[str]] = mapped_column(String(100))

    phone_masked: Mapped[Optional[str]] = mapped_column(String(20))
    phone_full: Mapped[Optional[str]] = mapped_column(String(20))
    contact_name: Mapped[Optional[str]] = mapped_column(String(255))

    posted_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    scraped_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    last_seen_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    __table_args__ = (
        UniqueConstraint("source", "source_id", name="uq_data_sources_source_id"),
        CheckConstraint("source IN ('nhatot','muaban')", name="ck_data_sources_source"),
        CheckConstraint(
            "transaction_type IN ('ban','cho-thue')",
            name="ck_data_sources_transaction_type",
        ),
        CheckConstraint(
            "poster_type IN ('moi_gioi','chu_nha','khong_xac_dinh')",
            name="ck_data_sources_poster_type",
        ),
        CheckConstraint(
            "poster_confidence_score IS NULL OR poster_confidence_score BETWEEN 0 AND 100",
            name="ck_data_sources_poster_confidence",
        ),
        CheckConstraint("price IS NULL OR price >= 0", name="ck_data_sources_price"),
        Index("idx_data_sources_posted_at", "posted_at"),
        Index("idx_data_sources_source_posted", "source", "posted_at"),
        Index("idx_data_sources_province_type", "province", "property_type"),
        Index("idx_data_sources_poster_type", "poster_type"),
        Index("idx_data_sources_phone_full", "phone_full"),
        Index("idx_data_sources_last_seen_at", "last_seen_at"),
    )

    def __repr__(self) -> str:
        return f"<DataSource {self.source}:{self.source_id}>"
