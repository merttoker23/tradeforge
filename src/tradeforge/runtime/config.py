"""Local configuration files are explicit, bounded and never echoed."""

from pathlib import Path

from sqlalchemy.engine import make_url

from tradeforge.config.policy import RuntimeMode
from tradeforge.config.settings import load_settings
from tradeforge.persistence.db import Database


def local_database(path: Path) -> Database:
    try:
        settings = load_settings()
        # Testnet is the only mode in which the embedded local execution may
        # run. The loopback-only URL and the locked live switch still hold.
        if (
            settings.mode not in {RuntimeMode.PAPER, RuntimeMode.TESTNET}
            or settings.live
            or path.stat().st_size > 4096
        ):
            raise ValueError
        url = make_url(path.read_text(encoding="utf-8").strip())
        if url.host not in {"127.0.0.1", "localhost", "::1"} or url.query:
            raise ValueError
        return Database(url)
    except Exception:
        raise ValueError("LOCAL_DATABASE_CONFIG_INVALID") from None


def local_token_file(path: Path) -> str:
    """Bounded local token file. The strict private-file rules live in
    tradeforge.runtime.secrets and are Linux-only; the co-located stack
    must also run on Windows."""
    try:
        if path.stat().st_size > 8192:
            raise ValueError
        value = path.read_text(encoding="utf-8").strip()
        if not 43 <= len(value) <= 4096 or any(not 33 <= ord(c) <= 126 for c in value):
            raise ValueError
        return value
    except Exception:
        raise ValueError("LOCAL_TOKEN_FILE_INVALID") from None
