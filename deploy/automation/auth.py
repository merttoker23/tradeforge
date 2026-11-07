"""Verify GitHub RS256 OIDC and successful jobs from the same workflow attempt."""

import base64
import json
import subprocess
import tempfile
import time
import urllib.request
from pathlib import Path

from deploy.automation.policy import AUDIENCE, REPOSITORY, validate_claims


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ValueError("REDIRECT_REJECTED")


def fetch_json(url, token=None):
    headers = {"Accept": "application/vnd.github+json", "User-Agent": "Tradeforge-deploy"}
    if token:
        headers["Authorization"] = "Bearer " + token
    request = urllib.request.Request(url, headers=headers)
    with urllib.request.build_opener(NoRedirect()).open(request, timeout=20) as response:
        return json.loads(response.read(4 * 1024 * 1024 + 1))


def api(path, token):
    return fetch_json("https://api.github.com/repos/" + REPOSITORY + path, token)


def b64(value):
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def der(tag, body):
    size = len(body)
    length = (
        bytes([size])
        if size < 128
        else (
            bytes([128 + (size.bit_length() + 7) // 8])
            + size.to_bytes((size.bit_length() + 7) // 8, "big")
        )
    )
    return bytes([tag]) + length + body


def integer(value):
    raw = b64(value).lstrip(b"\0") or b"\0"
    if raw[0] >= 128:
        raw = b"\0" + raw
    return der(2, raw)


def verify_oidc(token, sha, workdir):
    if not isinstance(token, str) or len(token) > 12000:
        raise ValueError("OIDC_TOKEN_REJECTED")
    pieces = token.split(".")
    if len(pieces) != 3:
        raise ValueError("OIDC_TOKEN_REJECTED")
    header = json.loads(b64(pieces[0]))
    if header.get("alg") != "RS256" or header.get("typ") != "JWT":
        raise ValueError("OIDC_ALGORITHM_REJECTED")
    keys = fetch_json("https://token.actions.githubusercontent.com/.well-known/jwks")["keys"]
    matches = [k for k in keys if k.get("kid") == header.get("kid") and k.get("kty") == "RSA"]
    if len(matches) != 1:
        raise ValueError("OIDC_KEY_REJECTED")
    key = matches[0]
    rsa = der(48, integer(key["n"]) + integer(key["e"]))
    pem = (
        b"-----BEGIN RSA PUBLIC KEY-----\n"
        + base64.encodebytes(rsa)
        + b"-----END RSA PUBLIC KEY-----\n"
    )
    with tempfile.TemporaryDirectory(prefix="oidc-", dir=workdir) as temp:
        path = Path(temp)
        (path / "key.pem").write_bytes(pem)
        (path / "signature").write_bytes(b64(pieces[2]))
        result = subprocess.run(
            [
                "/usr/bin/openssl",
                "dgst",
                "-sha256",
                "-verify",
                str(path / "key.pem"),
                "-signature",
                str(path / "signature"),
            ],
            input=(pieces[0] + "." + pieces[1]).encode(),
            capture_output=True,
            timeout=10,
        )
        if result.returncode:
            raise ValueError("OIDC_SIGNATURE_REJECTED")
    claims = json.loads(b64(pieces[1]))
    if (
        claims.get("iss") != "https://token.actions.githubusercontent.com"
        or claims.get("aud") != AUDIENCE
    ):
        raise ValueError("OIDC_ISSUER_AUDIENCE_REJECTED")
    now = time.time()
    if any(type(claims.get(k)) is not int for k in ("exp", "iat", "nbf")):
        raise ValueError("OIDC_TIME_REJECTED")
    if not (
        claims["nbf"] <= now + 5
        and now < claims["exp"]
        and now - 300 <= claims["iat"] <= now + 5
        and 0 < claims["exp"] - claims["iat"] <= 600
    ):
        raise ValueError("OIDC_EXPIRED")
    validate_claims(claims, sha)
    return claims


def check_jobs(claims, run, jobs):
    if (
        str(run.get("id")) != claims["run_id"]
        or str(run.get("run_attempt")) != claims["run_attempt"]
        or run.get("head_sha") != claims["sha"]
        or run.get("head_branch") != "main"
        or run.get("event") != "push"
        or run.get("path") != ".github/workflows/ci.yml"
        or run.get("status") != "in_progress"
        or run.get("repository", {}).get("id") != 1305289470
    ):
        raise ValueError("CI_RUN_REJECTED")
    for name in ("foundation", "docker", "publish"):
        matches = [j for j in jobs if j["name"] == name]
        if (
            len(matches) != 1
            or matches[0].get("conclusion") != "success"
            or matches[0].get("status") != "completed"
        ):
            raise ValueError("CI_GATE_FAILED")
    deploy = [j for j in jobs if j["name"] == "deploy"]
    expected = (
        "https://api.github.com/repos/" + REPOSITORY + "/check-runs/" + claims["check_run_id"]
    )
    if (
        len(deploy) != 1
        or deploy[0].get("check_run_url") != expected
        or deploy[0].get("status") != "in_progress"
    ):
        raise ValueError("CI_DEPLOY_JOB_REJECTED")


def verify_ci(claims, token):
    run_id, attempt = claims["run_id"], claims["run_attempt"]
    run = api("/actions/runs/" + run_id, token)
    jobs = api("/actions/runs/" + run_id + "/attempts/" + attempt + "/jobs?per_page=100", token)
    if jobs["total_count"] > 100:
        raise ValueError("CI_JOB_LIMIT")
    check_jobs(claims, run, jobs["jobs"])
    if api("/git/ref/heads/main", token)["object"]["sha"] != claims["sha"]:
        raise ValueError("SUPERSEDED_MAIN_COMMIT")
