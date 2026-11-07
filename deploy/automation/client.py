"""GitHub-hosted client: ephemeral OIDC and read-only package token over HTTPS."""

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

ENDPOINT = "https://forgetrade.tech/_tradeforge/deploy"


def request_oidc():
    url = (
        os.environ["ACTIONS_ID_TOKEN_REQUEST_URL"]
        + "&audience="
        + urllib.parse.quote(ENDPOINT, safe="")
    )
    request = urllib.request.Request(
        url, headers={"Authorization": "Bearer " + os.environ["ACTIONS_ID_TOKEN_REQUEST_TOKEN"]}
    )
    with urllib.request.urlopen(request, timeout=20) as response:
        return json.load(response)["value"]


def summary(result):
    print(json.dumps(result, sort_keys=True))
    target = os.environ.get("GITHUB_STEP_SUMMARY")
    if target:
        lines = ["### Tradeforge deployment", "", "| Field | Result |", "|---|---|"]
        for name, value in result.items():
            safe = str(value).replace("|", " ").replace("\n", " ").replace("`", "")
            lines.append(f"| {name} | `{safe}` |")
        with Path(target).open("a") as handle:
            handle.write("\n".join(lines) + "\n")


def main():
    base = {"sha": os.environ["GITHUB_SHA"], "digest": os.environ["DEPLOY_DIGEST"]}
    result = {**base, "outcome": "failed", "migration": "not_run", "health": "not_run"}
    deadline = time.monotonic() + 1800
    try:
        while time.monotonic() < deadline:
            payload = {**base, "oidc": request_oidc(), "registry_token": os.environ["GITHUB_TOKEN"]}
            request = urllib.request.Request(
                ENDPOINT,
                data=json.dumps(payload).encode(),
                method="POST",
                headers={"Content-Type": "application/json"},
            )
            try:
                with urllib.request.urlopen(request, timeout=1100) as response:
                    result = json.load(response)
                break
            except urllib.error.HTTPError as error:
                if error.code == 409:
                    time.sleep(15)
                    continue
                failure = json.loads(error.read(4000))
                result["error"] = failure.get("error", "DEPLOYMENT_REQUEST_REJECTED")
                break
        else:
            result["error"] = "DEPLOYMENT_QUEUE_TIMEOUT"
    except Exception:
        result["error"] = "DEPLOYMENT_TRANSPORT_FAILED_CHECK_SERVER_JOURNAL"
    summary(result)
    return 0 if result["outcome"] == "success" and result.get("cleanup") == "success" else 1


if __name__ == "__main__":
    raise SystemExit(main())
