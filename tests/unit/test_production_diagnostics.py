"""Execute the administrator helper against hostile controlled process boundaries."""

import http.client
import importlib.util
import json
import os
import signal
import stat
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
HELPER = ROOT / "deploy/diagnostics/production_diagnostics.py"
CID = "a" * 64
IMAGE = "sha256:" + "b" * 64
SECRET = "apiKey=DO_NOT_DISCLOSE signature=secret_token account_id=123456"


@pytest.fixture
def diagnostic():
    assert HELPER.is_file(), "bounded production diagnostic helper has not been implemented"
    spec = importlib.util.spec_from_file_location("production_diagnostics", HELPER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def container(service="market_gateway", **updates):
    result = {
        "Id": CID,
        "Image": IMAGE,
        "Config": {
            "Labels": {
                "com.docker.compose.project": "forgetrade",
                "com.docker.compose.project.working_dir": "/opt/forgetrade",
                "com.docker.compose.service": service,
                "org.opencontainers.image.revision": "c" * 40,
            },
            "Env": [SECRET],
        },
        "State": {
            "Running": True,
            "Status": "running",
            "OOMKilled": False,
            "StartedAt": "2026-11-04T12:00:00.123456789Z",
            "Health": {"Status": "healthy", "Log": [{"Output": SECRET}]},
        },
        "RestartCount": 3,
    }
    result.update(updates)
    return result


class ControlledRunner:
    def __init__(self, item=None, probe=None, logs=None):
        self.item = container() if item is None else item
        self.probe = (
            {
                "readiness": {
                    "http_status": 503,
                    "data": {
                        "data_ready": False,
                        "analysis_ready": False,
                        "reason": "LOW_COVERAGE",
                        "last_progress_at": "2026-11-04T12:00:01Z",
                        "last_error": SECRET,
                        "input_rejections": [{"symbol": SECRET, "reason": "FLOW_UNAVAILABLE"}],
                        "protection": {"ready": False, "account": SECRET},
                    },
                },
                "analysis": {
                    "http_status": 200,
                    "data": {
                        "status": {
                            "analysis_market_covered": 411,
                            "analysis_coverage_ready": False,
                            "arbitrary": SECRET,
                        }
                    },
                },
            }
            if probe is None
            else probe
        )
        self.logs = (
            json.dumps(
                {"level": "ERROR", "reason": "CLOCK_UNSAFE", "message": SECRET, "token": SECRET}
            )
            + "\n"
            + SECRET
            if logs is None
            else logs
        )
        self.calls = []

    def __call__(self, argv, *, timeout, limit):
        self.calls.append((tuple(argv), timeout, limit))
        assert argv[0] == "/usr/bin/docker"
        if argv[1] == "ps":
            return CID.encode()
        if argv[1] == "inspect":
            return json.dumps([self.item]).encode()
        if argv[1:3] == ["image", "inspect"]:
            return json.dumps(
                [
                    {
                        "Id": IMAGE,
                        "RepoDigests": [
                            "ghcr.io/merttoker23/tradeforge@sha256:" + "b" * 64,
                        ],
                        "Config": {"Labels": {"org.opencontainers.image.revision": "d" * 40}},
                    }
                ]
            ).encode()
        if argv[1] == "logs":
            return self.logs.encode()
        if argv[1] == "exec":
            return json.dumps(self.probe).encode()
        raise AssertionError("unexpected executable operation")


def test_snapshot_projects_only_safe_fields_and_preserves_real_times(diagnostic):
    runner = ControlledRunner()
    result = diagnostic.snapshot(runner)
    serialized = json.dumps(result)
    assert SECRET not in serialized
    assert "DO_NOT_DISCLOSE" not in serialized
    gateway = result["services"]["market_gateway"]
    assert gateway["started_at"] == "2026-11-04T12:00:00.123456789Z"
    assert gateway["restart_count"] == 3
    assert gateway["readiness"]["data"]["reason"] == "LOW_COVERAGE"
    assert gateway["readiness"]["data"]["last_error"] == "UNKNOWN"
    assert gateway["analysis"]["data"]["analysis_market_covered"] == 411
    assert gateway["logs"]["records"] == [{"level": "ERROR", "reason": "CLOCK_UNSAFE"}]
    assert gateway["logs"]["discarded"] == 1
    assert result["services"]["quant_core"]["status"] == "UNKNOWN"
    assert result["database"]["status"] == "UNKNOWN"


@pytest.mark.parametrize(
    "change",
    [
        {"Id": "--privileged"},
        {"Image": "$(touch /tmp/should-not-exist)"},
        {
            "Config": {
                "Labels": {
                    "com.docker.compose.project": "other",
                    "com.docker.compose.service": "market_gateway",
                }
            }
        },
        {
            "Config": {
                "Labels": {
                    "com.docker.compose.project": "forgetrade",
                    "com.docker.compose.project.working_dir": "/tmp/hostile",
                    "com.docker.compose.service": "market_gateway",
                }
            }
        },
    ],
)
def test_untrusted_inspect_cannot_select_targets_or_commands(diagnostic, change):
    runner = ControlledRunner(container(**change))
    result = diagnostic.snapshot(runner)
    assert result["services"]["market_gateway"]["status"] == "UNKNOWN"
    assert all(call[0][1] in {"ps", "inspect"} for call in runner.calls)


def test_fixed_exec_is_isolated_with_no_app_imports_or_caller_environment(diagnostic, monkeypatch):
    monkeypatch.setenv("DOCKER_HOST", "tcp://attacker:2375")
    monkeypatch.setenv("PYTHONPATH", "/tmp/hostile")
    monkeypatch.setenv("PGOPTIONS", "-c role=tradeforge_owner")
    runner = ControlledRunner()
    diagnostic.snapshot(runner)
    command = next(c[0] for c in runner.calls if c[0][1] == "exec")
    assert command[2:6] == ("--user", "10001:10001", "--workdir", "/")
    python = command.index("/usr/local/bin/python3")
    assert command[python + 1 : python + 5] == ("-I", "-S", "-B", "-c")
    assert command[python + 5] == diagnostic.PROBE_GATEWAY
    assert all(0 < c[1] <= 12 and 0 < c[2] <= 2_000_000 for c in runner.calls)


def test_unknown_json_values_are_not_echoed_as_reasons_or_metrics(diagnostic):
    result = diagnostic.project_telemetry(
        {
            "reason": "SECRET_TOKEN_SHAPED_LIKE_A_REASON",
            "stage": SECRET,
            "ready": "true",
            "eligible": True,
            "covered": float("nan"),
            "last_progress_at": SECRET,
            "analysis_coverage_ready": False,
            "reason_counts": {"LOW_COVERAGE": 4, SECRET: 100},
            "private_stream": {
                "connected": True,
                "last_failure": SECRET,
                "last_close_code": 1006,
                "listenKey": SECRET,
            },
            "input_rejections": [{"symbol": SECRET, "reason": "FLOW_UNAVAILABLE"}] * 1000,
        }
    )
    assert SECRET not in json.dumps(result)
    assert result["reason"] == result["stage"] == "UNKNOWN"
    assert result["ready"] is None and result["eligible"] is None
    assert result["covered"] is None
    assert result["analysis_coverage_ready"] is False
    assert result["reason_counts"] == {"LOW_COVERAGE": 4}
    assert result["private_stream"]["connected"] is True
    assert result["private_stream"]["last_close_code"] == 1006
    assert result["input_rejections"] == {"FLOW_UNAVAILABLE": 64}


def test_malformed_large_or_failed_boundary_is_unknown_without_error_leak(diagnostic):
    for payload in (b"{" + SECRET.encode(), b"x" * 2_000_001):

        def runner(argv, payload=payload, **kwargs):
            return payload

        result = diagnostic.snapshot(runner)
        assert result["services"]["market_gateway"]["status"] == "UNKNOWN"
        assert SECRET not in json.dumps(result)

    def failed(argv, **kwargs):
        raise RuntimeError(SECRET)

    result = diagnostic.snapshot(failed)
    assert SECRET not in json.dumps(result)
    assert result["database"]["status"] == "UNKNOWN"


def test_real_runner_caps_output_and_time_and_clears_environment(diagnostic, monkeypatch):
    monkeypatch.setenv("TF_HOSTILE_TOKEN", SECRET)
    output = diagnostic.run_bounded(
        [
            sys.executable,
            "-I",
            "-S",
            "-c",
            "import os;print(os.environ.get('TF_HOSTILE_TOKEN','ABSENT'));print(os.getcwd())",
        ],
        timeout=2,
        limit=100,
    )
    assert output == b"ABSENT\n/\n"
    with pytest.raises(diagnostic.ReadUnavailable):
        diagnostic.run_bounded(
            [sys.executable, "-I", "-S", "-c", "print('x'*1000000)"],
            timeout=2,
            limit=100,
        )
    with pytest.raises(diagnostic.ReadUnavailable):
        diagnostic.run_bounded(
            [sys.executable, "-I", "-S", "-c", "import time;time.sleep(2)"],
            timeout=0.05,
            limit=100,
        )


def test_cli_rejects_arguments_before_privileged_operations():
    assert HELPER.is_file(), "argument-free production diagnostic helper is missing"
    process = subprocess.run(
        [sys.executable, "-I", "-S", str(HELPER), "--sql", SECRET],
        capture_output=True,
        check=False,
        timeout=2,
        env={**os.environ, "PYTHONPATH": "/tmp/hostile", "DOCKER_HOST": "attacker"},
    )
    assert process.returncode == 2
    assert process.stderr == b""
    assert json.loads(process.stdout) == {"status": "ARGUMENT_FREE_ROOT_REQUIRED"}
    assert SECRET.encode() not in process.stdout


def test_final_renderer_emits_valid_bounded_json(diagnostic):
    rendered = diagnostic.render({"secret": SECRET, "too_large": ["x" * 1000] * 1000})
    assert len(rendered.encode()) <= 96_000
    assert json.loads(rendered) == {"status": "OUTPUT_CAPACITY", "database": {"status": "UNKNOWN"}}


def test_fixed_gateway_probe_uses_token_only_for_fixed_local_read(
    diagnostic,
    monkeypatch,
    tmp_path,
    capsys,
):
    token = "secret_" + "x" * 58
    token_file = tmp_path / "token"
    token_file.write_text(token)
    real_open = os.open
    requests = []

    def open_token(path, flags):
        assert path == "/run/secrets/gateway_token"
        assert flags & os.O_NOFOLLOW and flags & os.O_NONBLOCK
        return real_open(token_file, flags)

    class Response:
        status = 200

        def __init__(self, path):
            self.path = path

        def read(self, limit):
            assert limit == 4_000_001
            data = {"analysis_market_covered": 411, "analysis_coverage_ready": False}
            return json.dumps({"status": data, "markets": [{"secret": SECRET}]}).encode()

    class Connection:
        def __init__(self, host, port, timeout):
            assert (host, port, timeout) == ("127.0.0.1", 8101, 3)

        def request(self, method, path, headers):
            assert method == "GET"
            requests.append((path, headers))
            self.path = path

        def getresponse(self):
            return Response(self.path)

        def close(self):
            pass

    monkeypatch.setattr(http.client, "HTTPConnection", Connection)
    monkeypatch.setattr(signal, "alarm", lambda timeout: None)
    monkeypatch.setattr(os, "open", open_token)
    monkeypatch.setattr(os, "lstat", lambda path: os.stat_result((stat.S_IFDIR,) + (0,) * 9))
    exec(diagnostic.PROBE_GATEWAY, {})
    output = capsys.readouterr().out
    assert token not in output and SECRET not in output
    assert requests == [("/readiness", {}), ("/market-state", {"Authorization": "Bearer " + token})]
    assert json.loads(output)["analysis"]["data"]["status"]["analysis_market_covered"] == 411


def test_image_inspect_failure_does_not_hide_independent_readiness(diagnostic):
    controlled = ControlledRunner()

    def runner(argv, **kwargs):
        if argv[1:3] == ["image", "inspect"]:
            raise diagnostic.ReadUnavailable()
        return controlled(argv, **kwargs)

    output = diagnostic.snapshot(runner)
    assert output["services"]["market_gateway"]["readiness"]["http_status"] == 503


def test_database_command_cannot_inherit_env_or_fall_back_to_owner(diagnostic, monkeypatch):
    monkeypatch.setenv("PGUSER", "tradeforge_owner")
    monkeypatch.setenv("PGPASSWORD", SECRET)
    runner = ControlledRunner(container("postgres"))
    result = diagnostic.snapshot(runner)
    assert result["database"]["status"] == "UNKNOWN"
    command = next(c[0] for c in runner.calls if c[0][1] == "exec")
    assert command[7:9] == ("/usr/bin/env", "-i")
    assert "--username=tf_diagnostics" in command
    assert "--host=/var/run/postgresql" in command and "--dbname=tradeforge" in command
    assert "PGPASSFILE=/dev/null" in command and "PGSERVICEFILE=/dev/null" in command
    assert "--username=tradeforge_owner" not in command and SECRET not in str(command)


def test_database_success_and_bad_protocol_are_sanitized(diagnostic):
    def runner(argv, **kwargs):
        row = {
            "section": "runtime_current",
            "row": {
                "service": "quant_core",
                "fresh": True,
                "data": {"analysis_ready": False, "reason": SECRET, "token": SECRET},
            },
        }
        lines = [b"TF_DIAGNOSTICS_SECTION:role_check", b"1"]
        for name in diagnostic.QUERIES:
            lines.append(("TF_DIAGNOSTICS_SECTION:" + name).encode())
            if name == "runtime_current":
                lines.append(json.dumps(row).encode())
        return b"\n".join(lines) + b"\n"

    result = diagnostic.database_snapshot(runner, CID)
    assert result["status"] == "OK"
    assert SECRET not in json.dumps(result)
    assert result["sections"]["runtime_current"]["rows"][0]["data"]["reason"] == "UNKNOWN"

    def unsafe(argv, **kwargs):
        return b"0\n" + SECRET.encode()

    assert diagnostic.database_snapshot(unsafe, CID)["status"] == "UNKNOWN"


@pytest.mark.parametrize(
    "reason",
    [
        "BROAD_ANALYSIS_NOT_READY",
        "REGIME_UNKNOWN",
        "REGIME_TRANSITION",
        "DEMO_ENTRIES_DISABLED",
        "ACCOUNT_RECONCILIATION_STALE",
        "WAITING_CLOSED_BAR",
        "STALE_OR_INVALID_INPUT",
        "CHANGE_POINT",
        "INSUFFICIENT_HISTORY",
        "HISTORY_SYNCED",
        "HISTORY_PARTIAL",
        "HISTORY_STARTING",
        "DIRECTIONAL_BREADTH",
        "RANGE_EVIDENCE",
        "HYSTERESIS",
        "OVERLAP_REVISION",
        "HISTORY_RESTORED",
        "DATA_GAP",
        "ANALYZED",
        "KEY_PERMISSIONS_UNVERIFIED",
        "EXPLICIT_TESTNET_EXECUTION_REQUIRED",
        "CANONICAL_5M_GAP",
        "CANONICAL_15M_STALE",
        "CANONICAL_REPAIR_PENDING",
    ],
)
def test_actual_runtime_reason_survives_both_projection_boundaries(diagnostic, reason):
    namespace = {}
    exec(diagnostic.PROBE_RULES, namespace)
    sanitized = namespace["sanitize"]({"reason": reason, "blockers": [reason, SECRET]})
    projected = diagnostic.project_telemetry(sanitized)
    assert projected["reason"] == reason
    assert projected["blockers"] == sorted([reason, "UNKNOWN"])
    assert SECRET not in json.dumps(projected)


@pytest.mark.parametrize(
    ("section", "stderr", "failure"),
    [
        ("role_check", "ERROR:  22012\n" + SECRET, "DB_ROLE_POLICY_REJECTED"),
        ("pipeline_72h", "ERROR:  42501\n" + SECRET, "DB_PERMISSION_DENIED"),
        ("history_72h", "ERROR:  57014\n" + SECRET, "DB_QUERY_CANCELED"),
        ("runtime_current", "ERROR:  42703\n" + SECRET, "DB_SCHEMA_MISMATCH"),
        (None, 'psql: error: FATAL: role "tf_diagnostics" does not exist\n', "DB_ROLE_MISSING"),
        (
            None,
            'psql: error: FATAL: Peer authentication failed for user "tf_diagnostics"\n',
            "DB_AUTHENTICATION_FAILED",
        ),
        (None, SECRET, "DB_READ_UNAVAILABLE"),
    ],
)
def test_database_failure_reports_only_fixed_stage_and_code(
    diagnostic,
    section,
    stderr,
    failure,
):
    marker = b"" if section is None else ("TF_DIAGNOSTICS_SECTION:" + section + "\n").encode()
    error = diagnostic.database_failure(marker, stderr.encode())

    def runner(*args, **kwargs):
        raise error

    result = diagnostic.database_snapshot(runner, CID)
    assert result["status"] == "UNKNOWN"
    assert result["failure"] == failure
    assert result["failed_section"] == section
    assert result["sections"]["pipeline_72h"]["rows"] is None
    assert SECRET not in json.dumps(result)
    assert SECRET not in str(error)


def test_real_process_error_keeps_database_failure_boundary_without_raw_output(diagnostic):
    code = (
        "import sys;print('TF_DIAGNOSTICS_SECTION:pipeline_72h',flush=True);"
        "sys.stderr.write('ERROR:  42501\\n'+'" + SECRET + "');sys.exit(1)"
    )
    with pytest.raises(diagnostic.ReadUnavailable) as failure:
        diagnostic.run_bounded(
            [sys.executable, "-I", "-S", "-c", code, "--username=tf_diagnostics"],
            timeout=2,
            limit=4096,
        )
    assert failure.value.failure == "DB_PERMISSION_DENIED"
    assert failure.value.failed_section == "pipeline_72h"
    assert SECRET not in str(failure.value)


def test_canceled_aggregate_is_isolated_without_losing_other_fixed_counts(diagnostic):
    calls = []

    def runner(argv, **kwargs):
        sql = argv[-1]
        calls.append(sql)
        if sql == diagnostic.SQL:
            raise diagnostic.ReadUnavailable("DB_QUERY_CANCELED", "pipeline_72h")
        section = next(
            name for name in diagnostic.QUERIES if ("TF_DIAGNOSTICS_SECTION:" + name + "';") in sql
        )
        rows = []
        if section == "pipeline_72h":
            stage = next(
                name for name, _, _ in diagnostic.PIPELINE_READS if "'stage','" + name + "'" in sql
            )
            if stage == "feature_snapshots":
                raise diagnostic.ReadUnavailable("DB_QUERY_CANCELED", "pipeline_72h")
            rows = [{"stage": stage, "count": 0}]
        if section == "runtime_current":
            rows = [
                {
                    "service": "execution_core",
                    "data": {"blockers": ["BROAD_ANALYSIS_NOT_READY", SECRET]},
                }
            ]
        lines = [
            b"TF_DIAGNOSTICS_SECTION:role_check",
            b"1",
            ("TF_DIAGNOSTICS_SECTION:" + section).encode(),
        ]
        lines += [json.dumps({"section": section, "row": row}).encode() for row in rows]
        return b"\n".join(lines) + b"\n"

    result = diagnostic.database_snapshot(runner, CID)
    assert result["status"] == "PARTIAL"
    pipeline = result["sections"]["pipeline_72h"]
    assert pipeline["status"] == "PARTIAL"
    counts = {row["stage"]: row["count"] for row in pipeline["rows"]}
    assert counts["feature_snapshots"] is None
    assert counts["execution_intents"] == counts["fills_observed"] == 0
    assert result["sections"]["runtime_current"]["status"] == "OK"
    assert result["sections"]["candidate_transitions_72h"] == {"status": "OK", "rows": []}
    assert SECRET not in json.dumps(result)
    assert all("READ ONLY" in sql and "1200ms" in sql and "ROLLBACK" in sql for sql in calls)


@pytest.mark.parametrize("section", [None, "role_check"])
def test_cancellation_without_proven_role_gate_does_not_start_separate_reads(diagnostic, section):
    calls = []

    def runner(argv, **kwargs):
        calls.append(argv)
        raise diagnostic.ReadUnavailable("DB_QUERY_CANCELED", section)

    result = diagnostic.database_snapshot(runner, CID)
    assert result["status"] == "UNKNOWN"
    assert len(calls) == 1


def test_only_four_measured_slow_reads_get_bounded_longer_query_budget(diagnostic):
    longer = []
    for section, stage, statement in diagnostic.isolated_reads():
        assert statement.startswith(diagnostic.SQL_PREFIX)
        if "SET LOCAL statement_timeout='10000ms';" in statement:
            longer.append((section, stage))
            assert statement.index(diagnostic.ROLE_CHECK) < statement.index("'10000ms'")
    assert set(longer) == {
        ("pipeline_72h", "opportunities"),
        ("regimes_72h", None),
        ("history_72h", None),
        ("history_revisions_72h", None),
    }


def test_real_runner_counts_stderr_toward_read_cap(diagnostic):
    with pytest.raises(diagnostic.ReadUnavailable):
        diagnostic.run_bounded(
            [
                sys.executable,
                "-I",
                "-S",
                "-c",
                "import sys;sys.stderr.write('s'*1000000)",
            ],
            timeout=2,
            limit=100,
        )


def test_global_deadline_cannot_be_swallowed_by_boundary_exception_handlers(
    diagnostic,
    monkeypatch,
    capsys,
):
    callbacks = {}
    reached_after_deadline = []

    def snapshot():
        try:
            callbacks[signal.SIGALRM](signal.SIGALRM, None)
        except Exception:
            pass
        reached_after_deadline.append(True)
        return {"status": "TOO_LATE"}

    monkeypatch.setattr(diagnostic, "snapshot", snapshot)
    monkeypatch.setattr(sys, "argv", [str(HELPER)])
    monkeypatch.setattr(os, "geteuid", lambda: 0)
    monkeypatch.setattr(os, "environ", {})
    monkeypatch.setattr(os, "chdir", lambda path: None)
    monkeypatch.setattr(signal, "signal", lambda sig, callback: callbacks.update({sig: callback}))
    monkeypatch.setattr(signal, "alarm", lambda seconds: None)
    assert diagnostic.main() == 1
    assert reached_after_deadline == []
    assert json.loads(capsys.readouterr().out)["status"] == "UNKNOWN"


def test_transport_and_bootstrap_projection_retains_fixed_typed_evidence(diagnostic):
    output = diagnostic.project_telemetry(
        {
            "protection": {
                "account_requests": [
                    {
                        "method": "GET",
                        "path": "/fapi/v3/account",
                        "reason": "EXCHANGE_TRANSPORT_UNKNOWN",
                        "duration_ms": 5000.1,
                        "phase": "TLS",
                        "failure": "TIMEOUT",
                        "params": SECRET,
                    }
                ]
                * 100,
                "failed_stage": "/fapi/v3/account",
            },
            "autonomous_decisions": {
                "candidate_count": 2,
                "approvals": 0,
                "last_reason": "UNPROVEN",
                "reason_counts": {"UNPROVEN": 2, SECRET: 999},
                "statistical_evidence": {
                    "evidence_environment": "testnet",
                    "proof_state": "UNPROVEN",
                    "train_effective_samples": 0,
                    "oos_effective_samples": 0,
                    "train_required": 150,
                    "oos_required": 100,
                    "maximum_trial_entries": 20,
                    "trial_can_self_validate": False,
                    "execution_authorized": False,
                    "bootstrap_blockers": ["TRIAL_CANNOT_MEET_SAMPLE_MINIMUM"],
                    "raw": SECRET,
                },
            },
        }
    )
    assert SECRET not in json.dumps(output)
    requests = output["protection"]["account_requests"]
    assert len(requests) == 32
    assert requests[0] == {
        "method": "GET",
        "path": "/fapi/v3/account",
        "reason": "EXCHANGE_TRANSPORT_UNKNOWN",
        "duration_ms": 5000.1,
        "phase": "TLS",
        "failure": "TIMEOUT",
    }
    evidence = output["autonomous_decisions"]["statistical_evidence"]
    assert evidence["maximum_trial_entries"] == 20
    assert evidence["trial_can_self_validate"] is False
    assert evidence["execution_authorized"] is False


def test_docker_logs_captures_stderr_then_projects_both_streams(diagnostic, monkeypatch):
    real_popen = subprocess.Popen

    def controlled(argv, **kwargs):
        assert argv == ["/usr/bin/docker", "logs", "--tail", "100", CID]
        source = (
            "import json,sys;"
            "print(json.dumps({'reason':'CLOCK_UNSAFE','message':'secret_stdout'}));"
            "sys.stderr.write(json.dumps({'level':'ERROR','reason':'LOW_COVERAGE',"
            "'message':'secret_stderr'})+'\\nraw_secret_stderr\\n')"
        )
        return real_popen([sys.executable, "-I", "-S", "-c", source], **kwargs)

    monkeypatch.setattr(subprocess, "Popen", controlled)
    output = diagnostic.run_bounded(
        ["/usr/bin/docker", "logs", "--tail", "100", CID],
        timeout=2,
        limit=8192,
    )
    projected = diagnostic.project_logs(output)
    assert {row["reason"] for row in projected["records"]} == {"CLOCK_UNSAFE", "LOW_COVERAGE"}
    assert projected["discarded"] == 1
    assert "secret" not in json.dumps(projected)


@pytest.mark.parametrize("label", ["DISORDER", "VOL_COMPRESSION"])
def test_real_regime_labels_round_trip_child_parent_and_database(diagnostic, label):
    namespace = {}
    exec(diagnostic.PROBE_RULES, namespace)
    child = namespace["sanitize"]({"regime_label": label})
    assert diagnostic.project_telemetry(child)["regime_label"] == label
    assert diagnostic.project_database_row("regimes_72h", {"state": label})["state"] == label


def test_real_decision_and_bootstrap_status_round_trip_both_sanitizers(diagnostic):
    from tests.decision_fixtures import change
    from tests.edge_fixtures import analyze, trades
    from tradeforge.decision.bootstrap import bootstrap_diagnostics
    from tradeforge.decision.trial import TestnetTrialPolicy as TrialPolicy
    from tradeforge.runtime.demo_decisions import DemoDecisions

    decisions = DemoDecisions(None, None)
    reasons = (
        "NO_ARMED_CANDIDATES",
        "CANDIDATE_EXPIRED",
        "INSUFFICIENT_STATISTICAL_EVIDENCE",
        "TESTNET_TRIAL_BUDGET_EXHAUSTED",
        "TESTNET_TRIAL_SETTLEMENT_REQUIRED",
        "TESTNET_TRIAL_LOSS_LIMIT",
        "OOS_EMBARGO_OR_UNSETTLED_TRAIN",
        "VOLUME_UNAVAILABLE",
    )
    for reason in reasons:
        decisions._note(reason)
    report = bootstrap_diagnostics(
        analyze(tuple(change(r, forecast_p_win=None) for r in trades(5, 5))),
        TrialPolicy(),
    )
    output = decisions.status()
    output["autonomous_decisions"]["statistical_evidence"] = report
    namespace = {}
    exec(diagnostic.PROBE_RULES, namespace)
    projected = diagnostic.project_telemetry(namespace["sanitize"](output))
    actual = projected["autonomous_decisions"]
    assert actual["last_reason"] == "VOLUME_UNAVAILABLE"
    assert actual["reason_counts"] == {reason: 1 for reason in reasons}
    for key, value in report.items():
        sanitized = actual["statistical_evidence"][key]
        assert set(sanitized) == set(value) if isinstance(value, list) else sanitized == value
