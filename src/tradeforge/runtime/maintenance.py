"""Bounded retention and disk/WAL admission; never deletes execution/risk history."""

import os
import shutil
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from sqlalchemy import text

from tradeforge.persistence.db import Database, EntryHalt

FREE_LIMIT = 1024**3
WAL_LIMIT = 1024**3
DATABASE_LIMIT = 4 * 1024**3


@dataclass(frozen=True)
class StorageBudget:
    """Measured PostgreSQL volume budget used for admission and optional work shedding."""

    free_bytes: int
    database_bytes: int
    wal_bytes: int

    @property
    def database_pressure(self) -> float:
        return min(1.0, self.database_bytes / DATABASE_LIMIT)

    @property
    def wal_pressure(self) -> float:
        return min(1.0, self.wal_bytes / WAL_LIMIT)

    @property
    def free_pressure(self) -> float:
        return 1.0 if self.free_bytes < FREE_LIMIT else 0.0

    @property
    def pressure(self) -> float:
        return max(self.database_pressure, self.wal_pressure, self.free_pressure)

    @property
    def ready(self) -> bool:
        # PostgreSQL reuses vacuumed pages without shrinking relation files.
        # Admission uses actual volume free space; a fixed pg_database_size
        # ceiling would latch forever even after retention made pages reusable.
        return self.free_bytes >= FREE_LIMIT and self.wal_bytes < WAL_LIMIT


def database_volume() -> Path:
    """The measured disk that admission trusts.

    Compose mounts the PostgreSQL state volume at a fixed path. A co-located
    local database has no such mount; its operator names the data directory
    explicitly instead. An unset or missing volume never borrows another
    disk's free space.
    """
    override = os.environ.get("TF_POSTGRES_VOLUME", "").strip()
    return Path(override) if override else Path("/postgres-volume")


def storage_budget(db: Database, *, volume_path: Path | None = None) -> StorageBudget:
    if volume_path is None:
        volume_path = database_volume()
    try:
        free = shutil.disk_usage(volume_path).free
    except OSError:
        return StorageBudget(0, DATABASE_LIMIT, WAL_LIMIT)
    with db.transaction() as c:
        # This gauge is approximate. Walking every relation file for an
        # exact size can time out and prevent the separate WAL/free-space
        # checks that actually determine admission.
        size = c.scalar(text("""
            SELECT coalesce(sum(relpages::bigint),0)*current_setting('block_size')::bigint
            FROM pg_catalog.pg_class
            WHERE relkind IN ('r','i','t','m','S') AND NOT relisshared
        """))
        wal = c.scalar(text("SELECT coalesce(sum(size),0) FROM pg_ls_waldir()"))
    return StorageBudget(int(free), int(size or 0), int(wal or 0))


def storage_ready(db: Database, *, volume_path: Path | None = None) -> bool:
    return storage_budget(db, volume_path=volume_path).ready


def observe_storage(db: Database) -> bool:
    from tradeforge.runtime.storage import save_runtime

    at = datetime.now(UTC)
    budget = storage_budget(db)
    ready = budget.ready
    save_runtime(
        db,
        "execution_core",
        at,
        at + timedelta(seconds=30),
        {
            "storage_ready": ready,
            "storage_checked_at": at.isoformat(),
            "storage_free_bytes": budget.free_bytes,
            "storage_database_bytes": budget.database_bytes,
            "storage_wal_bytes": budget.wal_bytes,
            "resource_pressure": budget.pressure,
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
                "WHERE service='execution_core'"
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


def retain(db: Database, *, volume_path: Path | None = None) -> None:
    with db.transaction() as c:
        c.execute(text("SELECT retain_runtime_history()"))
    if not storage_ready(db, volume_path=volume_path):
        raise EntryHalt("STORAGE_BUDGET_EXHAUSTED")
