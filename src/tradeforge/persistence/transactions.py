"""Transactional schema changes. Database URL is explicit, never old env fallback."""

import argparse
import os
from importlib.resources import files

from alembic import command
from alembic.config import Config

from tradeforge.persistence.db import Database


def migrate(db: Database, target: str) -> None:
    config = Config()
    config.set_main_option("script_location", str(files("migrations")))
    with db.transaction() as connection:
        # Serialize migration runners; rollback releases the lock automatically.
        from sqlalchemy import text

        connection.execute(text("SELECT pg_advisory_xact_lock(74202002)"))
        config.attributes["connection"] = connection
        if target == "base":
            command.downgrade(config, "base")
        else:
            command.upgrade(config, target)


def main() -> int:
    parser = argparse.ArgumentParser(description="Explicit local PostgreSQL migrations")
    parser.add_argument("target", choices=["head", "base"])
    args = parser.parse_args()
    url = os.environ.get("TRADEFORGE_DATABASE_URL")
    if not url:
        parser.error("TRADEFORGE_DATABASE_URL_REQUIRED")
    db = Database(url)
    try:
        migrate(db, args.target)
    finally:
        db.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
