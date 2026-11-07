"""Stateless HTTPS sessions signed by a mounted, persistent secret.

The login token is high entropy and provisioned offline. Changing either the login
digest or the signing key revokes every existing session, including after restart.
"""

import hashlib
import hmac
import re
import secrets
import time
from collections.abc import Callable

from tradeforge.dashboard.auth import Authenticator, AuthError, Principal

COOKIE = "__Host-tradeforge"


class Sessions:
    lifetime = 30 * 86400

    def __init__(
        self, auth: Authenticator, key: bytes, *, clock: Callable[[], float] = time.time
    ) -> None:
        if len(key) < 32:
            raise ValueError("SESSION_KEY_TOO_SHORT")
        self.auth, self.key, self.clock = auth, key, clock

    def _signature(self, value: str) -> str:
        return hmac.new(self.key, value.encode(), hashlib.sha256).hexdigest()

    def login(self, token: str) -> str:
        principal = self.auth.authenticate("Bearer " + token, "read")
        # The digest is not an authorization credential; it binds revocation to
        # the configured login credential without placing that credential in a cookie.
        binding = hashlib.sha256(token.encode()).hexdigest()
        scopes = ",".join(sorted(principal.scopes))
        value = ".".join(
            (principal.subject, scopes, str(int(self.clock())), secrets.token_hex(16), binding)
        )
        return value + "." + self._signature(value)

    def authenticate(self, cookie: str | None, scope: str) -> Principal:
        try:
            if cookie is None or len(cookie) > 512:
                raise ValueError
            subject, scopes, issued, nonce, binding, signature = cookie.split(".")
            value = cookie.rsplit(".", 1)[0]
            if (
                not hmac.compare_digest(self._signature(value), signature)
                or re.fullmatch(r"[a-f0-9]{32}", nonce) is None
                or not 0 <= self.clock() - int(issued) < self.lifetime
            ):
                raise ValueError
            person = Principal(subject, frozenset(scopes.split(",")))
            if not any(
                t.principal == person and hmac.compare_digest(t.sha256, binding)
                for t in self.auth._tokens
            ):
                raise ValueError
            person.require(scope)
            return person
        except (ValueError, TypeError):
            raise AuthError(401) from None
