"""
SQLAlchemy engine + Session factory cho V-Nexus scraper.

Dùng psycopg2 (sync) cho flow ghi DB sau scrape: upsert + classify trong scheduler
(xem scheduler/jobs.py). Mỗi cycle mở 1 session qua `session_scope()`.

Env vars (load từ `.env`): DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD.
"""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from typing import Iterator

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

load_dotenv()

log = logging.getLogger("db.session")


class Base(DeclarativeBase):
    """Declarative base cho mọi ORM model trong project."""


def _build_db_url() -> str:
    host = os.getenv("DB_HOST", "localhost").strip()
    port = os.getenv("DB_PORT", "5432").strip()
    name = os.getenv("DB_NAME", "vnexus").strip()
    user = os.getenv("DB_USER", "vnexus").strip()
    password = os.getenv("DB_PASSWORD", "").strip()
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{name}"


_engine: Engine | None = None
_SessionFactory: sessionmaker[Session] | None = None


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        url = _build_db_url()
        _engine = create_engine(
            url,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
            future=True,
            connect_args={"connect_timeout": 10},  # chặn hang khi DB down
        )
        log.info("DB engine created host=%s db=%s", os.getenv("DB_HOST"), os.getenv("DB_NAME"))
    return _engine


def get_session_factory() -> sessionmaker[Session]:
    global _SessionFactory
    if _SessionFactory is None:
        _SessionFactory = sessionmaker(bind=get_engine(), expire_on_commit=False, future=True)
    return _SessionFactory


@contextmanager
def session_scope() -> Iterator[Session]:
    """Context manager dùng cho scraper/scheduler — commit/rollback tự động."""
    session = get_session_factory()()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
