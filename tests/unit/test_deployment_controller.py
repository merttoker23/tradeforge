"""Exercise failure transitions without a production database or Docker socket."""

import importlib
import json
import threading
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest


class Operations:
    def __init__(self, fail=None, schema_ok=True):
        self.events = []
        self.fail = fail
        self.schema_ok = schema_ok

    def step(self, name):
        self.events.append(name)
        if self.fail == name:
            raise RuntimeError("injected failure")

    def prepare(self):
        self.step("prepare")

    def migrate(self):
        self.step("migration")

    def schema_compatible(self):
        self.step("compatibility")
        return self.schema_ok

    def start(self):
        self.step("start")

    def healthy(self):
        self.step("health")

    def stop(self):
        self.step("stop")

    def rollback(self):
        self.step("rollback")

    def rollback_healthy(self):
        self.step("rollback_health")

    def finish(self):
        self.step("finish")

    def cleanup(self):
        self.step("cleanup")


def perform(ops):
    module = importlib.import_module("deploy.automation.controller")
    return module.perform(ops)


def test_migration_failure_never_starts_new_apps():
    ops = Operations(fail="migration")
    result = perform(ops)
    assert result["migration"] == "failed"
    assert result["health"] == "not_run"
    assert ops.events == ["prepare", "migration"]


def test_success_checks_schema_before_start_and_cleans_only_after_health():
    ops = Operations()
    result = perform(ops)
    assert result["outcome"] == "success"
    assert ops.events == [
        "prepare",
        "migration",
        "compatibility",
        "start",
        "health",
        "finish",
        "cleanup",
    ]


@pytest.mark.parametrize("failure", ["start", "health"])
def test_failed_release_rolls_back_only_with_compatible_schema(failure):
    ops = Operations(fail=failure)
    result = perform(ops)
    assert result["outcome"] == "rolled_back"
    assert result["rollback"] == "healthy"
    assert ops.events.index("stop") < ops.events.index("rollback")
    assert ops.events[-4:] == ["stop", "compatibility", "rollback", "rollback_health"]


def test_incompatible_schema_blocks_start_and_rollback():
    ops = Operations(schema_ok=False)
    result = perform(ops)
    assert result["outcome"] == "manual_review"
    assert "start" not in ops.events
    assert "rollback" not in ops.events


def test_cleanup_error_preserves_successful_application():
    ops = Operations(fail="cleanup")
    result = perform(ops)
    assert result["health"] == "healthy"
    assert result["cleanup"] == "failed"
    assert "rollback" not in ops.events


def test_ci_job_binding_rejects_failed_skipped_or_different_job():
    module = importlib.import_module("deploy.automation.auth")
    claims = {"run_id": "12", "run_attempt": "1", "sha": "a" * 40, "check_run_id": "99"}
    run = {
        "id": 12,
        "run_attempt": 1,
        "head_sha": "a" * 40,
        "head_branch": "main",
        "event": "push",
        "path": ".github/workflows/ci.yml",
        "status": "in_progress",
        "repository": {"id": 1305289470},
    }
    jobs = [
        {"name": n, "conclusion": "success", "status": "completed"}
        for n in ("foundation", "docker", "publish")
    ]
    jobs.append(
        {
            "name": "deploy",
            "check_run_url": "https://api.github.com/repos/merttoker23/tradeforge/check-runs/99",
            "status": "in_progress",
        }
    )
    module.check_jobs(claims, run, jobs)
    for conclusion in ("failure", "skipped", None):
        jobs[0]["conclusion"] = conclusion
        with pytest.raises(ValueError):
            module.check_jobs(claims, run, jobs)
    jobs[0]["conclusion"] = "success"
    claims["check_run_id"] = "100"
    with pytest.raises(ValueError):
        module.check_jobs(claims, run, jobs)


def test_dashboard_health_respects_existing_host_allowlist(monkeypatch):
    runtime = importlib.import_module("deploy.automation.runtime")

    class Dashboard(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200 if self.headers.get("Host") == "forgetrade.tech" else 403)
            self.end_headers()

        def log_message(self, *_):
            pass

    server = HTTPServer(("127.0.0.1", 0), Dashboard)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    actual_open = urllib.request.urlopen

    def mapped_open(request, timeout):
        headers = (
            dict(request.header_items()) if isinstance(request, urllib.request.Request) else {}
        )
        target = urllib.request.Request(
            f"http://127.0.0.1:{server.server_port}/health", headers=headers
        )
        return actual_open(target, timeout=timeout)

    monkeypatch.setattr(runtime.urllib.request, "urlopen", mapped_open)
    monkeypatch.setattr(
        runtime,
        "run",
        lambda _: json.dumps(
            [
                {
                    "Image": "expected",
                    "State": {"Status": "running", "Health": {"Status": "healthy"}},
                }
                for _ in range(4)
            ]
        ),
    )
    try:
        assert runtime.health("expected") is None
    finally:
        server.shutdown()
        server.server_close()
        worker.join(timeout=2)
