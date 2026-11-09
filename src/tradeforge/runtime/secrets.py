"""Linux-only private file helpers; fail closed on ownership/permission problems."""

import os
import stat
from pathlib import Path


def restrict(path: Path, *, directory: bool = False) -> None:
    if os.name != "posix":
        raise ValueError("LINUX_PERMISSIONS_REQUIRED")
    path.chmod(0o700 if directory else 0o600)


def private_directory(path: Path) -> None:
    try:
        if any(part.is_symlink() for part in (path, *path.parents)):
            raise ValueError
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
        restrict(path, directory=True)
    except Exception:
        raise ValueError("PRIVATE_DIRECTORY_UNAVAILABLE") from None


def write_secret(path: Path, value: str) -> None:
    try:
        if path.exists() or path.is_symlink():
            raise ValueError
        fd = os.open(
            path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600
        )
        with os.fdopen(fd, "w", encoding="utf-8") as output:
            output.write(value)
        restrict(path)
    except Exception:
        raise ValueError("PRIVATE_FILE_CREATE_FAILED") from None


def read_secret(path: Path, *, maximum: int = 512) -> str:
    """Read one owned private regular file without following symlinks."""
    try:
        if os.name != "posix" or any(p.is_symlink() for p in (path, *path.parents)):
            raise ValueError
        fd = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0),
        )
        with os.fdopen(fd, "r", encoding="utf-8") as stream:
            info = os.fstat(stream.fileno())
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_mode & 0o077
                or info.st_uid != getattr(os, "geteuid", lambda: -1)()
                or info.st_nlink != 1
                or not 0 < info.st_size <= 8192
            ):
                raise ValueError
            value = stream.read(8193).strip()
        if not value or len(value) > maximum or any(not 33 <= ord(c) <= 126 for c in value):
            raise ValueError
        return value
    except Exception:
        raise ValueError("DEMO_SECRET_FILE_UNSAFE") from None
