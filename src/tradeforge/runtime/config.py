"""Local configuration files are explicit, bounded and never echoed."""

from pathlib import Path

from sqlalchemy.engine import make_url

from tradeforge.config.settings import load_settings
from tradeforge.persistence.db import Database


def local_database(path: Path) -> Database:
    try:
        settings = load_settings()
        if settings.mode.value != "paper" or settings.live or path.stat().st_size > 4096:
            raise ValueError
        url = make_url(path.read_text(encoding="utf-8").strip())
        if url.host not in {"127.0.0.1", "localhost", "::1"} or url.query:
            raise ValueError
        return Database(url)
    except Exception:
        raise ValueError("LOCAL_DATABASE_CONFIG_INVALID") from None
