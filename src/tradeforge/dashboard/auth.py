"""High-entropy bearer tokens, offline digest provisioning and bounded admission."""

import hashlib
import hmac
import json
import re
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from threading import Lock


class AuthError(ValueError):
    def __init__(self, status: int) -> None:
        self.status = status
        super().__init__({401: "AUTH_REQUIRED", 403: "SCOPE_REQUIRED", 429: "RATE_LIMITED"}[status])


@dataclass(frozen=True)
class Principal:
    subject: str
    scopes: frozenset[str]

    def require(self, scope: str) -> None:
        if scope not in self.scopes:
            raise AuthError(403)


@dataclass(frozen=True)
class Token:
    principal: Principal
    sha256: str = field(repr=False)


class Authenticator:
    def __init__(self, tokens: tuple[Token, ...], *, clock: Callable[[], float] = time.monotonic):
        self._tokens = tokens
        self.clock = clock
        self._start = clock()
        self._calls = 0
        self._failures = 0
        self._lock = Lock()

    @classmethod
    def from_json(
        cls, body: str, *, clock: Callable[[], float] = time.monotonic
    ) -> "Authenticator":
        try:
            if len(body) > 65536:
                raise ValueError
            config = json.loads(body)
            if not isinstance(config, dict) or set(config) != {"tokens"}:
                raise ValueError
            rows = config["tokens"]
            if not isinstance(rows, list) or not 1 <= len(rows) <= 32:
                raise ValueError
            tokens = []
            for row in rows:
                if not isinstance(row, dict) or set(row) != {"subject", "sha256", "scopes"}:
                    raise ValueError
                subject, digest, scopes = row["subject"], row["sha256"], row["scopes"]
                if (
                    not isinstance(subject, str)
                    or re.fullmatch(r"[A-Za-z0-9_-]{1,64}", subject) is None
                    or not isinstance(digest, str)
                    or re.fullmatch(r"[a-f0-9]{64}", digest) is None
                    or not isinstance(scopes, list)
                    or scopes not in (["read"], ["read", "control"])
                ):
                    raise ValueError
                tokens.append(Token(Principal(subject, frozenset(scopes)), digest))
            if len({x.sha256 for x in tokens}) != len(tokens):
                raise ValueError
            return cls(tuple(tokens), clock=clock)
        except (ValueError, TypeError, KeyError):
            raise ValueError("DASHBOARD_AUTH_INVALID") from None

    def authenticate(self, header: str | None, scope: str) -> Principal:
        with self._lock:
            now = self.clock()
            if now - self._start >= 60:
                self._start, self._calls, self._failures = now, 0, 0
            if self._calls >= 120 or self._failures >= 8:
                raise AuthError(429)
            self._calls += 1
            value = header[7:] if header is not None and header.startswith("Bearer ") else ""
            valid = re.fullmatch(r"[A-Za-z0-9_-]{43,128}", value) is not None
            digest = hashlib.sha256(value.encode()).hexdigest()
            principal = None
            for token in self._tokens:
                if hmac.compare_digest(digest, token.sha256) and valid:
                    principal = token.principal
            if principal is None:
                self._failures += 1
                raise AuthError(401)
            principal.require(scope)
            return principal
