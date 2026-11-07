"""No dotenv, credential fields, legacy aliases, implicit mode promotion or raw logs."""

import logging
import os
from collections.abc import Mapping
from typing import Annotated, Literal, Self

from pydantic import BeforeValidator, ValidationError, model_validator

from tradeforge.config.policy import RuntimeMode, require_safe_mode
from tradeforge.domain.common import StrictModel, exact_bool

LOGGER = logging.getLogger("tradeforge.config")
ENV_FIELDS = {"TRADEFORGE_MODE", "TRADEFORGE_LIVE"}


class ConfigError(ValueError):
    """Public errors deliberately contain no rejected values or validation payload."""


class Settings(StrictModel):
    mode: RuntimeMode = RuntimeMode.PAPER
    live: Annotated[Literal[False], BeforeValidator(exact_bool)] = False

    @model_validator(mode="after")
    def mode_policy(self) -> Self:
        require_safe_mode(self.mode)
        return self


def load_settings(environment: Mapping[str, str] | None = None) -> Settings:
    source = os.environ if environment is None else environment
    try:
        if any(name.startswith("TRADEFORGE_") and name not in ENV_FIELDS for name in source):
            raise ValueError("UNKNOWN_SETTING")
        mode = RuntimeMode(source.get("TRADEFORGE_MODE", "paper"))
        if source.get("TRADEFORGE_LIVE", "false") != "false":
            raise ValueError("LIVE_LOCKED_OR_INVALID")
        return Settings(mode=mode, live=False)
    except (ValueError, TypeError):
        LOGGER.warning("config_rejected")
        raise ConfigError("RUNTIME_CONFIG_INVALID") from None


def update_settings(current: Settings, changes: Mapping[str, object]) -> Settings:
    """Validate before replacing; failed changes leave the original immutable object intact."""
    try:
        replacement = Settings.model_validate({**current.model_dump(), **changes})
    except (ValidationError, TypeError):
        LOGGER.warning("config_change_rejected")
        raise ConfigError("RUNTIME_CONFIG_INVALID") from None
    changed = sorted(
        name
        for name in Settings.model_fields
        if getattr(current, name) != getattr(replacement, name)
    )
    if changed:
        LOGGER.info("config_changed fields=%s", ",".join(changed))
    return replacement
