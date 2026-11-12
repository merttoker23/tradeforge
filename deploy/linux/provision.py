"""Offline, root-only Linux Compose secret provisioning; never starts containers."""

import argparse
import os
import secrets
from pathlib import Path


def provision(directory: Path) -> None:
    if os.name != "posix" or getattr(os, "geteuid", lambda: -1)() != 0:
        raise ValueError("LINUX_ROOT_REQUIRED")
    directory = directory.absolute()
    if directory.exists() or any(p.is_symlink() for p in (directory, *directory.parents)):
        raise ValueError("FRESH_PRIVATE_DIRECTORY_REQUIRED")
    os.umask(0o077)
    directory.mkdir(parents=True, mode=0o700)
    for name in (
        "postgres_password",
        "market_password",
        "quant_password",
        "dashboard_password",
        "gateway_token",
        "dashboard_login",
        "session_key",
        "binance_demo_key",
        "binance_demo_secret",
    ):
        value = "" if name.startswith("binance_demo_") else secrets.token_hex(32)
        path = directory / name
        fd = os.open(
            path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o400,
        )
        with os.fdopen(fd, "w", encoding="ascii") as stream:
            stream.write(value)
        getattr(os, "chown", lambda *args, **kwargs: None)(path, 10001, 10001)


def main() -> int:
    parser = argparse.ArgumentParser(description="Create fresh private Linux Compose secrets")
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    try:
        provision(args.directory)
    except Exception:
        parser.error("PROVISION_FAILED_PRESERVE_PARTIAL_STATE")
    print("PROVISIONED: paper default; Demo credentials empty; no services started")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
