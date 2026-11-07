"""Bounded retention and disk/WAL admission; never deletes execution/risk history."""

import shutil
from datetime import UTC, datetime, timedelta
from pathlib import Path

from sqlalchemy import text

from tradeforge.persistence.db import Database, EntryHalt


def storage_ready(db: Database, *, volume_path: Path = Path("/postgres-volume")) -> bool:
    try:
        free = shutil.disk_usage(volume_path).free
    except OSError:
        return False
    if free < 1024**3:
        return False
    with db.transaction() as c:
        size = c.scalar(text("SELECT pg_database_size(current_database())"))
        wal = c.scalar(text("SELECT coalesce(sum(size),0) FROM pg_ls_waldir()"))
    # PostgreSQL reuses vacuumed pages without shrinking relation files. Admission
    # uses actual volume free space; a fixed pg_database_size ceiling would latch
    # forever even after safe retention had made those pages reusable.
    return bool(size >= 0 and wal < 1024**3)


def observe_storage(db: Database) -> bool:
    from tradeforge.runtime.storage import save_runtime

    at = datetime.now(UTC)
    ready = storage_ready(db)
    save_runtime(
        db,
        "execution_guardian",
        at,
        at + timedelta(seconds=30),
        {
            "storage_ready": ready,
            "storage_checked_at": at.isoformat(),
            "live": False,
            "reason": "STORAGE_READY" if ready else "STORAGE_UNAVAILABLE",
        },
    )
    return ready


def observation_admission(db: Database) -> bool:
    with db.transaction() as c:
        row = c.execute(
            text(
                "SELECT observed_at,payload FROM runtime_observations "
                "WHERE service='execution_guardian'"
            )
        ).first()
    try:
        checked_at = datetime.fromisoformat(row.payload["storage_checked_at"]) if row else None
    except (KeyError, TypeError, ValueError):
        return False
    now = datetime.now(UTC)
    return bool(
        row
        and checked_at is not None
        and checked_at.utcoffset() == timedelta(0)
        and row.payload.get("storage_ready") is True
        and checked_at <= row.observed_at
        and 0 <= (now - checked_at).total_seconds() < 30
        and 0 <= (now - row.observed_at).total_seconds() < 30
    )


def retain(db: Database, *, volume_path: Path = Path("/postgres-volume")) -> None:
    with db.transaction() as c:
        c.execute(text("SELECT retain_runtime_history()"))
    if not storage_ready(db, volume_path=volume_path):
        raise EntryHalt("STORAGE_BUDGET_EXHAUSTED")
