"""Mode vocabulary. LIVE activation is unavailable until the dedicated live phase."""

from enum import StrEnum


class RuntimeMode(StrEnum):
    DISABLED = "disabled"
    PAPER = "paper"
    TESTNET = "testnet"
    LIVE = "live"


def require_safe_mode(mode: RuntimeMode) -> None:
    if mode is RuntimeMode.LIVE:
        raise ValueError("LIVE_LOCKED")


def require_demo_trial(mode: RuntimeMode, *, orders: str, trial: str) -> bool:
    if trial not in {"true", "false"} or orders not in {"true", "false"}:
        raise ValueError("EXPLICIT_DEMO_TRIAL_REQUIRED")
    if trial == "true" and (mode is not RuntimeMode.TESTNET or orders != "true"):
        raise ValueError("TESTNET_TRIAL_REQUIRES_DEMO_ORDERS")
    return trial == "true"
