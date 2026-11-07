"""Private demo keys and sanitized diagnostics, isolated from dashboard bearer auth."""

import subprocess
from pathlib import Path

from pydantic import SecretStr

from tradeforge.execution.binance_client import Credentials
from tradeforge.runtime.secrets import read_secret


def private_ignored_file(path: Path, root: Path, *, maximum: int = 512) -> str:
    try:
        root = root.resolve(strict=True)
        if any(p.is_symlink() for p in (path, *path.parents)):
            raise ValueError
        resolved = path.resolve(strict=True)
        if resolved.is_relative_to(root):
            relative = resolved.relative_to(root).as_posix()
            tracked = subprocess.run(
                ["git", "ls-files", "--error-unmatch", "--", relative],
                cwd=root,
                capture_output=True,
                timeout=5,
            )
            ignored = subprocess.run(
                ["git", "check-ignore", "-q", "--", relative],
                cwd=root,
                capture_output=True,
                timeout=5,
            )
            if tracked.returncode != 1 or ignored.returncode != 0:
                raise ValueError
        elif not resolved.is_relative_to(Path("/run/credentials")):
            # Outside-repository inputs are exclusively systemd credential mounts.
            raise ValueError
        return read_secret(path, maximum=maximum)
    except Exception:
        raise ValueError("DEMO_SECRET_FILE_UNSAFE") from None


def load_demo_credentials(
    home: Path, *, root: Path | None = None, observer_home: Path | None = None
) -> Credentials:
    root = root or Path(__file__).resolve().parents[3]
    # Linux systemd service reads dedicated mode-0600 key files; no DPAPI/bundle.
    key = private_ignored_file(home / "binance-demo.key", root)
    secret = private_ignored_file(home / "binance-demo.secret", root)
    tokens = [home / "dashboard.token", root / "var/local-runtime/dashboard.token"]
    if observer_home is not None:
        tokens.append(observer_home / "dashboard.token")
    if key == secret or any(
        p.is_file() and p.read_text(encoding="utf-8").strip() in {key, secret} for p in tokens
    ):
        raise ValueError("DEMO_SECRET_FILE_UNSAFE")
    return Credentials(api_key=SecretStr(key), api_secret=SecretStr(secret))
