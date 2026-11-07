"""Verify real RSA signatures rather than trusting decoded JWT claim fixtures."""

import base64
import json
import subprocess
import time

import pytest

from deploy.automation import auth
from deploy.automation.policy import AUDIENCE, REPOSITORY, WORKFLOW


def encoded(raw):
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


@pytest.fixture
def signer(tmp_path, monkeypatch):
    private = tmp_path / "private-test-key"
    subprocess.run(
        [
            "openssl",
            "genpkey",
            "-algorithm",
            "RSA",
            "-pkeyopt",
            "rsa_keygen_bits:2048",
            "-out",
            str(private),
        ],
        check=True,
        capture_output=True,
    )
    modulus = (
        subprocess.run(
            ["openssl", "rsa", "-in", str(private), "-modulus", "-noout"],
            capture_output=True,
            text=True,
            check=True,
        )
        .stdout.strip()
        .split("=")[1]
    )
    monkeypatch.setattr(
        auth,
        "fetch_json",
        lambda _: {
            "keys": [
                {
                    "kid": "test",
                    "kty": "RSA",
                    "n": encoded(bytes.fromhex(modulus)),
                    "e": encoded((65537).to_bytes(3, "big")),
                }
            ]
        },
    )

    def sign(overrides=None, header=None):
        now = int(time.time())
        claims = {
            "iss": "https://token.actions.githubusercontent.com",
            "aud": AUDIENCE,
            "iat": now,
            "nbf": now - 1,
            "exp": now + 300,
            "jti": "test-unique",
            "repository": REPOSITORY,
            "repository_id": "1305289470",
            "repository_owner_id": "61198883",
            "ref": "refs/heads/main",
            "event_name": "push",
            "workflow_ref": WORKFLOW,
            "workflow_sha": "a" * 40,
            "sha": "a" * 40,
            "run_id": "123",
            "run_attempt": "1",
            "check_run_id": "456",
            "runner_environment": "github-hosted",
        }
        claims.update(overrides or {})
        parts = [
            encoded(json.dumps(header or {"alg": "RS256", "kid": "test", "typ": "JWT"}).encode()),
            encoded(json.dumps(claims).encode()),
        ]
        signing_input = ".".join(parts).encode()
        signature = subprocess.run(
            ["openssl", "dgst", "-sha256", "-sign", str(private)],
            input=signing_input,
            capture_output=True,
            check=True,
        ).stdout
        return signing_input.decode() + "." + encoded(signature)

    return sign


def test_oidc_real_signature(signer, tmp_path):
    result = auth.verify_oidc(signer(), "a" * 40, tmp_path)
    assert result["check_run_id"] == "456"
    assert not list(tmp_path.glob("oidc-*"))


@pytest.mark.parametrize(
    "claims",
    [
        {"iss": "https://attacker.invalid"},
        {"aud": "other"},
        {"exp": 1},
        {"iat": 1},
        {"nbf": 9999999999},
        {"exp": True},
        {"sha": "b" * 40},
    ],
)
def test_signed_but_unauthorized_identity_is_rejected(signer, tmp_path, claims):
    with pytest.raises(ValueError):
        auth.verify_oidc(signer(claims), "a" * 40, tmp_path)


def test_forged_signature_and_algorithm_confusion_are_rejected(signer, tmp_path):
    token = signer()
    parts = token.split(".")
    parts[1] = encoded(json.dumps({"sha": "a" * 40}).encode())
    with pytest.raises(ValueError, match="SIGNATURE_REJECTED"):
        auth.verify_oidc(".".join(parts), "a" * 40, tmp_path)
    with pytest.raises(ValueError, match="ALGORITHM_REJECTED"):
        auth.verify_oidc(
            signer(header={"alg": "none", "kid": "test", "typ": "JWT"}), "a" * 40, tmp_path
        )
