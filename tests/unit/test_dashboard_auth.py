"""Bearer credentials are scoped, bounded and never reflected in errors."""

import hashlib
import json

import pytest


def auth(clock=lambda: 0):
    from tradeforge.dashboard.auth import Authenticator

    return Authenticator.from_json(
        json.dumps(
            {
                "tokens": [
                    {
                        "subject": "viewer",
                        "sha256": hashlib.sha256(b"v" * 48).hexdigest(),
                        "scopes": ["read"],
                    },
                    {
                        "subject": "operator",
                        "sha256": hashlib.sha256(b"o" * 48).hexdigest(),
                        "scopes": ["read", "control"],
                    },
                ]
            }
        ),
        clock=clock,
    )


def test_bearer_scopes_and_safe_repr():
    from tradeforge.dashboard.auth import AuthError

    a = auth()
    assert a.authenticate("Bearer " + "v" * 48, "read").subject == "viewer"
    assert a.authenticate("Bearer " + "o" * 48, "control").subject == "operator"
    with pytest.raises(AuthError) as error:
        a.authenticate("Bearer " + "v" * 48, "control")
    assert error.value.status == 403
    assert "v" * 48 not in repr(a)


@pytest.mark.parametrize("header", [None, "", "Basic x", "Bearer short", "Bearer " + "x" * 48])
def test_invalid_credentials(header):
    from tradeforge.dashboard.auth import AuthError

    with pytest.raises(AuthError) as error:
        auth().authenticate(header, "read")
    assert error.value.status == 401
    assert "x" * 48 not in str(error.value)


def test_brute_force_and_request_budgets_expire():
    from tradeforge.dashboard.auth import AuthError

    now = [0.0]
    a = auth(lambda: now[0])
    for _ in range(8):
        with pytest.raises(AuthError):
            a.authenticate("Bearer " + "x" * 48, "read")
    with pytest.raises(AuthError) as error:
        a.authenticate("Bearer " + "o" * 48, "read")
    assert error.value.status == 429
    now[0] = 61
    for _ in range(120):
        a.authenticate("Bearer " + "o" * 48, "read")
    with pytest.raises(AuthError) as error:
        a.authenticate("Bearer " + "o" * 48, "read")
    assert error.value.status == 429


@pytest.mark.parametrize(
    "config",
    [
        "{}",
        '{"tokens":[]}',
        '{"tokens":[],"secret":"sentinel"}',
        '{"tokens":[{"subject":"x","sha256":"short","scopes":["live"]}]}',
    ],
)
def test_invalid_config_is_redacted(config):
    from tradeforge.dashboard.auth import Authenticator

    with pytest.raises(ValueError, match="DASHBOARD_AUTH_INVALID") as error:
        Authenticator.from_json(config)
    assert "sentinel" not in str(error.value)
