"""Linux-only config and private file safety invariants."""

import os
from pathlib import Path

import pytest


@pytest.mark.skipif(os.name != "posix", reason="Linux file permission contract")
def test_linux_secret_files_are_private_and_not_overwritten(tmp_path: Path) -> None:
    from tradeforge.runtime.secrets import private_directory, write_secret

    parent = tmp_path / "private"
    private_directory(parent)
    token = parent / "demo.key"
    write_secret(token, "SYNTHETIC_TEST_KEY")
    assert token.stat().st_mode & 0o077 == 0
    assert parent.stat().st_mode & 0o077 == 0
    with pytest.raises(ValueError, match="PRIVATE_FILE_CREATE_FAILED"):
        write_secret(token, "replacement")
    assert token.read_text() == "SYNTHETIC_TEST_KEY"


def test_database_remote_host_and_override_rejected(tmp_path: Path) -> None:
    from tradeforge.runtime.config import local_database

    url = tmp_path / "database.url"
    for text in (
        "postgresql+psycopg://x:SENTINEL@remote.invalid/tradeforge",
        "postgresql+psycopg://x:SENTINEL@127.0.0.1/tradeforge?host=remote.invalid",
    ):
        url.write_text(text)
        with pytest.raises(ValueError, match="LOCAL_DATABASE_CONFIG_INVALID") as error:
            local_database(url)
        assert "SENTINEL" not in str(error.value)


@pytest.mark.skipif(os.name != "posix", reason="Linux private file ownership and permissions")
@pytest.mark.parametrize("unsafe", ["public", "symlink", "hardlink", "oversized", "fifo"])
def test_linux_private_reader_rejects_unsafe_inputs(tmp_path, unsafe):
    from tradeforge.runtime.secrets import read_secret, write_secret

    target = tmp_path / "secret"
    write_secret(target, "SYNTHETIC_TEST_VALUE")
    assert read_secret(target) == "SYNTHETIC_TEST_VALUE"
    if unsafe == "public":
        target.chmod(0o644)
    elif unsafe == "symlink":
        link = tmp_path / "symlink"
        link.symlink_to(target)
        target = link
    elif unsafe == "hardlink":
        os.link(target, tmp_path / "hardlink")
    elif unsafe == "oversized":
        target.write_text("x" * 513)
    else:
        target.unlink()
        os.mkfifo(target, mode=0o600)
    with pytest.raises(ValueError, match="DEMO_SECRET_FILE_UNSAFE"):
        read_secret(target)
