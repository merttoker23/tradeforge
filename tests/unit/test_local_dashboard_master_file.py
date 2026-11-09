"""Private loopback dashboard key handling (no exchange network)."""

import pytest

from apps.dashboard.runtime import select_binance_master


def test_inline_and_file_master_cannot_be_combined(tmp_path):
    path = tmp_path / "master"
    path.write_text("x" * 64)
    with pytest.raises(ValueError, match="BINANCE_MASTER_AMBIGUOUS"):
        select_binance_master("y" * 64, path)


def test_missing_master_is_allowed_only_for_no_api_settings():
    assert select_binance_master(None, None) is None
    with pytest.raises(ValueError, match="BINANCE_ENCRYPTION_SECRET_INVALID"):
        select_binance_master("x" * 31, None)


def test_private_master_file_requires_safe_permissions(tmp_path):
    path = tmp_path / "master"
    path.write_text("s" * 64)
    path.chmod(0o600)
    assert select_binance_master(None, path) == "s" * 64
    path.chmod(0o644)
    with pytest.raises(ValueError, match="DEMO_SECRET_FILE_UNSAFE"):
        select_binance_master(None, path)
