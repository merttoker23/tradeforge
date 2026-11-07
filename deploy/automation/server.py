"""Loopback-only HTTPS gateway backend; fixed root policy, no remote shell."""

import fcntl
import hashlib
import json
import os
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from deploy.automation.auth import verify_ci, verify_oidc
from deploy.automation.controller import perform
from deploy.automation.policy import image_ref
from deploy.automation.runtime import ROOT, Operations, atomic, contract, revision

LOCK = threading.Lock()


def dispatch(body):
    if not isinstance(body, dict) or set(body) != {"sha", "digest", "oidc", "registry_token"}:
        raise ValueError("REQUEST_REJECTED")
    sha = body["sha"]
    image_ref(body["digest"])
    token = body["registry_token"]
    if not isinstance(token, str) or not 20 <= len(token) <= 4096 or "\n" in token:
        raise ValueError("REGISTRY_TOKEN_REJECTED")
    with tempfile.TemporaryDirectory(prefix="request-", dir="/run/tradeforge-deploy") as temporary:
        claims = verify_oidc(body["oidc"], sha, temporary)
        verify_ci(claims, token)
        replay = hashlib.sha256(claims["jti"].encode()).hexdigest()
        state = json.loads((ROOT / "state.json").read_text())
        if replay in state.get("used_tokens", []):
            raise ValueError("OIDC_REPLAY_REJECTED")
        state["used_tokens"] = [*state.get("used_tokens", [])[-99:], replay]
        atomic(ROOT / "state.json", state)
        ops = Operations(sha, body["digest"], token, claims, temporary)
        outcome = perform(ops)
        # Safe rejection before application replacement can be retried by a later commit.
        # A crash, unknown DB revision or changed contract leaves the journal locked.
        if outcome["outcome"] == "failed" and ops.state.get("inflight"):
            stage = ops.state["inflight"]["stage"]
            if stage in {"pull", "prepared", "migrating"}:
                if revision() == ops.state["head"] and ops.before == contract():
                    ops.state["inflight"] = None
                    atomic(ROOT / "state.json", ops.state)
        if outcome["outcome"] != "success" and not ops.state.get("inflight"):
            try:
                ops.discard_candidate()
                outcome["cleanup"] = "success"
            except Exception:
                outcome["cleanup"] = "failed"
        outcome.update(
            sha=sha,
            digest=body["digest"],
            run_id=claims["run_id"],
            run_attempt=claims["run_attempt"],
            schema_revision=ops.head,
            migration_count=len(getattr(ops, "pending", [])),
            timestamp=int(time.time()),
        )
        atomic(ROOT / "last-result.json", outcome)
        with (ROOT / "events.jsonl").open("a") as handle:
            handle.write(json.dumps(outcome, sort_keys=True) + "\n")
        print(json.dumps(outcome, sort_keys=True), flush=True)
        return outcome


class Handler(BaseHTTPRequestHandler):
    server_version = "Tradeforge"
    sys_version = ""

    def log_message(self, *_):
        pass

    def reply(self, status, value):
        data = json.dumps(value).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        try:
            self.wfile.write(data)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_POST(self):
        if self.path != "/_tradeforge/deploy":
            self.reply(404, {"error": "NOT_FOUND"})
            return
        try:
            size = int(self.headers.get("Content-Length", "0"))
            if not 0 < size <= 24000 or self.headers.get("Transfer-Encoding"):
                raise ValueError("REQUEST_SIZE_REJECTED")
            self.connection.settimeout(20)
            body = json.loads(self.rfile.read(size))
        except Exception:
            self.reply(400, {"error": "REQUEST_REJECTED"})
            return
        if not LOCK.acquire(blocking=False):
            self.reply(409, {"error": "DEPLOYMENT_BUSY"})
            return
        try:
            with (ROOT / "deploy.lock").open("a") as lock:
                try:
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    self.reply(409, {"error": "DEPLOYMENT_BUSY"})
                    return
                try:
                    result = dispatch(body)
                except Exception as error:
                    code = str(error) if isinstance(error, ValueError) else "REQUEST_FAILED_CLOSED"
                    if not code.replace("_", "").isalnum() or len(code) > 100:
                        code = "REQUEST_FAILED_CLOSED"
                    self.reply(403, {"error": code})
                    return
                self.reply(200, result)
        finally:
            LOCK.release()


def main():
    os.umask(0o077)
    server = ThreadingHTTPServer(("127.0.0.1", 18105), Handler)
    server.daemon_threads = False
    server.serve_forever()


if __name__ == "__main__":
    main()
