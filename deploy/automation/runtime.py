"""Fixed production operations. No caller-controlled commands, paths or Compose input."""

import base64
import copy
import hashlib
import json
import os
import re
import subprocess
import time
import urllib.request
from pathlib import Path

from deploy.automation.auth import api
from deploy.automation.migrations import render
from deploy.automation.policy import (
    APPLICATIONS,
    REPOSITORY,
    compatible,
    image_ref,
    migration_plan,
    removable_images,
)

ROOT = Path("/var/lib/tradeforge-deploy")
COMPOSE = Path("/opt/forgetrade/compose.json")
DOCKER = "/usr/bin/docker"
DC = [DOCKER, "compose", "-p", "forgetrade", "-f", str(COMPOSE)]

# The production controller never runs the app's migrate/bootstrap entrypoint.
# Grant only the roles needed for this reviewed, additive 0007 revision.
# These statements execute in the same transaction as its Alembic SQL.
BINANCE_0007_GRANTS = """
REVOKE ALL ON public.binance_runtime, public.binance_credentials FROM PUBLIC;
REVOKE ALL ON public.binance_credentials FROM tf_market, tf_quant, tf_dashboard;
GRANT SELECT ON public.binance_runtime TO tf_market, tf_quant, tf_dashboard;
GRANT UPDATE (environment, revision, changed_at) ON public.binance_runtime TO tf_dashboard;
GRANT SELECT ON public.binance_credentials TO tf_dashboard, tf_quant;
GRANT INSERT, UPDATE ON public.binance_credentials TO tf_dashboard;
"""


def run(args, data=None, timeout=120, environment=None):
    completed = subprocess.run(
        args, input=data, text=True, capture_output=True, timeout=timeout, env=environment
    )
    if completed.returncode:
        raise RuntimeError("COMMAND_FAILED_" + args[0].rsplit("/", 1)[-1])
    return completed.stdout


def atomic(path, value):
    temporary = path.with_name(path.name + ".new")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "w") as handle:
        json.dump(value, handle, sort_keys=True, indent=2)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    directory = os.open(path.parent, os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def psql(sql):
    return run(
        [
            DOCKER,
            "exec",
            "-i",
            "forgetrade-postgres-1",
            "psql",
            "-X",
            "-q",
            "-A",
            "-t",
            "-U",
            "tradeforge_owner",
            "-d",
            "tradeforge",
            "-v",
            "ON_ERROR_STOP=1",
            "--single-transaction",
        ],
        data=sql,
        timeout=180,
    )


def revision():
    value = psql("SELECT version_num FROM public.schema_migrations;").strip()
    if not re.fullmatch(r"[a-zA-Z0-9_]{1,32}", value):
        raise ValueError("MANUAL_REVIEW_DATABASE_REVISION")
    return value


def contract():
    # Catalog-only compatibility query; no table data is read or persisted.
    raw = psql("""
SELECT json_object_agg(k,v) FROM (
 SELECT 'column:'||c.relname||'.'||a.attname AS k,
        json_build_array(format_type(a.atttypid,a.atttypmod),a.attnotnull,
                         pg_get_expr(d.adbin,d.adrelid),a.attidentity,a.attgenerated)::text AS v
 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
 JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped
 LEFT JOIN pg_attrdef d ON d.adrelid=c.oid AND d.adnum=a.attnum
 WHERE n.nspname='public' AND c.relkind IN ('r','p')
 UNION ALL
 SELECT 'constraint:'||c.relname||'.'||x.conname,pg_get_constraintdef(x.oid)
 FROM pg_constraint x JOIN pg_class c ON c.oid=x.conrelid
 JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public'
 UNION ALL
 SELECT 'trigger:'||c.relname||'.'||t.tgname,pg_get_triggerdef(t.oid)
 FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid
 JOIN pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname='public' AND NOT t.tgisinternal
 UNION ALL
 SELECT 'function:'||p.oid::regprocedure::text,pg_get_functiondef(p.oid)
 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
 WHERE n.nspname='public' AND p.prokind='f'
) contracts;
""")
    return json.loads(raw)


def infrastructure():
    ids = run([DOCKER, "ps", "-aq"]).split()
    objects = json.loads(run([DOCKER, "inspect", *ids])) if ids else []
    protected = {}
    for item in objects:
        labels = item["Config"].get("Labels") or {}
        project, service = (
            labels.get("com.docker.compose.project"),
            labels.get("com.docker.compose.service"),
        )
        if project != "forgetrade" or service == "postgres":
            protected[item["Id"]] = {
                k: item[k]
                for k in (
                    "Id",
                    "Image",
                    "Created",
                    "RestartCount",
                    "State",
                    "Mounts",
                    "NetworkSettings",
                )
            }
            # Health samples are dynamic; process identity/restart state must stay fixed.
            protected[item["Id"]]["State"].pop("Health", None)
    volumes = json.loads(run([DOCKER, "volume", "inspect", "forgetrade_postgres_state"]))
    return {"containers": protected, "volume": volumes}


def normalized(compose):
    result = copy.deepcopy(compose)
    for name in (*APPLICATIONS, "migrate"):
        service = result["services"][name]
        service.pop("build", None)
        service["image"] = "APPLICATION_IMAGE"
    return result


def health(expected_image):
    deadline = time.monotonic() + 240
    while time.monotonic() < deadline:
        items = json.loads(
            run([DOCKER, "inspect", *["forgetrade-" + n + "-1" for n in APPLICATIONS]])
        )
        good = all(
            i["Image"] == expected_image
            and i["State"]["Status"] == "running"
            and i["State"].get("Health", {}).get("Status") == "healthy"
            and not i["State"].get("OOMKilled")
            for i in items
        )
        if good:
            request = urllib.request.Request(
                "http://127.0.0.1:18104/health", headers={"Host": "forgetrade.tech"}
            )
            with urllib.request.urlopen(request, timeout=5) as response:
                if response.status == 200:
                    return
        time.sleep(3)
    raise RuntimeError("HEALTH_CHECK_FAILED")


class Operations:
    def __init__(self, sha, digest, token, claims, temporary):
        self.sha, self.ref, self.token = sha, image_ref(digest), token
        self.claims, self.temporary = claims, Path(temporary)
        self.state = json.loads((ROOT / "state.json").read_text())
        self.old_compose = json.loads(COMPOSE.read_text())
        self.before = None
        self.protected = None
        self.image_id = None
        self.head = None
        self.sources = {}

    def save(self, stage):
        self.state["inflight"]["stage"] = stage
        atomic(ROOT / "state.json", self.state)

    def prepare(self):
        if self.state.get("inflight"):
            raise ValueError("MANUAL_REVIEW_INTERRUPTED_DEPLOYMENT")
        baseline = json.loads((ROOT / "production-policy.json").read_text())
        if normalized(self.old_compose) != baseline:
            raise ValueError("MANUAL_REVIEW_PRODUCTION_CONFIG_DRIFT")
        for name in (*APPLICATIONS, "migrate"):
            env = self.old_compose["services"][name].get("environment", {})
            if any(
                env.get(k) != v
                for k, v in {
                    "TRADEFORGE_LIVE": "false",
                    "TRADEFORGE_MODE": "paper",
                    "TF_DEMO_ORDERS": "false",
                }.items()
            ):
                raise ValueError("PAPER_MODE_REQUIRED")
        self.protected = infrastructure()
        health(self.state["active"]["id"])
        current = revision()
        if current != self.state["head"]:
            raise ValueError("MANUAL_REVIEW_SCHEMA_DRIFT")
        self.before = contract()
        self.state["inflight"] = {
            "sha": self.sha,
            "image": self.ref,
            "stage": "pull",
            "run_id": self.claims["run_id"],
        }
        self.save("pull")
        config = self.temporary / "docker"
        config.mkdir(mode=0o700)
        run(
            [
                DOCKER,
                "--config",
                str(config),
                "login",
                "ghcr.io",
                "-u",
                "merttoker23",
                "--password-stdin",
            ],
            data=self.token,
        )
        run([DOCKER, "--config", str(config), "pull", self.ref], timeout=480)
        info = json.loads(run([DOCKER, "image", "inspect", self.ref]))[0]
        labels = info["Config"].get("Labels") or {}
        if (
            labels.get("org.opencontainers.image.revision") != self.sha
            or labels.get("org.opencontainers.image.source") != "https://github.com/" + REPOSITORY
            or labels.get("io.tradeforge.managed") != "true"
            or self.ref not in (info.get("RepoDigests") or [])
        ):
            raise ValueError("IMAGE_IDENTITY_REJECTED")
        self.image_id = info["Id"]
        entries = api("/contents/migrations/versions?ref=" + self.sha, self.token)
        if not isinstance(entries, list) or len(entries) > 100:
            raise ValueError("MIGRATION_FILE_LIMIT")
        for item in entries:
            name = item["name"]
            if item["type"] != "file" or not re.fullmatch(r"[a-zA-Z0-9_]+\.(?:py|sql)", name):
                raise ValueError("MANUAL_REVIEW_MIGRATION_FILENAME")
            blob = api("/git/blobs/" + item["sha"], self.token)
            body = base64.b64decode(blob["content"])
            if len(body) > 262144:
                raise ValueError("MIGRATION_FILE_LIMIT")
            self.sources[name] = body.decode()
        self.head, self.pending = migration_plan(
            self.sources, self.state["migration_hashes"], current
        )
        self.sql = render(self.sources, current, self.head, self.temporary) if self.pending else ""
        self.candidate = copy.deepcopy(self.old_compose)
        for name in (*APPLICATIONS, "migrate"):
            self.candidate["services"][name].pop("build", None)
            self.candidate["services"][name]["image"] = self.ref
        path = self.temporary / "candidate.json"
        atomic(path, self.candidate)
        run([DOCKER, "compose", "-p", "forgetrade", "-f", str(path), "config", "--quiet"])
        self.save("prepared")

    def migrate(self):
        self.save("migrating")
        if self.sql:
            sql = self.sql
            if any(head == "0007" for head, _, _ in self.pending):
                sql += BINANCE_0007_GRANTS
            psql(
                "SET LOCAL lock_timeout='10s'; SET LOCAL statement_timeout='120s';\n"
                "SELECT pg_advisory_xact_lock(74202002);\n" + sql
            )
        if revision() != self.head:
            raise RuntimeError("MIGRATION_REVISION_MISMATCH")
        self.state["head"] = self.head
        self.state["migration_hashes"] = {
            k: hashlib.sha256(v.encode()).hexdigest() for k, v in self.sources.items()
        }
        self.save("migrated")

    def schema_compatible(self):
        return revision() == self.head and compatible(self.before, contract())

    def start(self):
        # Admission already authorized this main commit. Finish the serialized operation
        # even if another commit arrives while the image is being pulled.
        self.save("starting")
        atomic(COMPOSE, self.candidate)
        run(
            [*DC, "up", "-d", "--no-deps", "--no-build", "--pull", "never", *APPLICATIONS],
            timeout=180,
        )

    def healthy(self):
        health(self.image_id)
        if infrastructure() != self.protected:
            raise RuntimeError("PROTECTED_INFRASTRUCTURE_CHANGED")
        if not self.schema_compatible():
            raise RuntimeError("POST_START_SCHEMA_CHANGED")

    def stop(self):
        run([*DC, "stop", *APPLICATIONS], timeout=180)

    def rollback(self):
        self.save("rolling_back")
        atomic(COMPOSE, self.old_compose)
        run(
            [*DC, "up", "-d", "--no-deps", "--no-build", "--pull", "never", *APPLICATIONS],
            timeout=180,
        )

    def rollback_healthy(self):
        health(self.state["active"]["id"])
        if infrastructure() != self.protected:
            raise RuntimeError("PROTECTED_INFRASTRUCTURE_CHANGED")
        self.state["inflight"] = None
        atomic(ROOT / "state.json", self.state)

    def finish(self):
        if self.state["active"]["id"] != self.image_id:
            self.state["previous"] = self.state["active"]
        self.state["active"] = {"id": self.image_id, "sha": self.sha, "image": self.ref}
        self.state["inflight"] = None
        atomic(ROOT / "state.json", self.state)

    def cleanup(self):
        # Only the stopped Compose migration container may reference a superseded app image.
        run([*DC, "rm", "-f", "migrate"])
        ids = run([DOCKER, "image", "ls", "-aq", "--no-trunc"]).split()
        images = json.loads(run([DOCKER, "image", "inspect", *sorted(set(ids))])) if ids else []
        containers = run([DOCKER, "ps", "-aq"]).split()
        used = (
            {i["Image"] for i in json.loads(run([DOCKER, "inspect", *containers]))}
            if containers
            else set()
        )
        retained = {self.state[k]["id"] for k in ("active", "previous") if self.state.get(k)}
        for identity in removable_images(images, retained, used):
            # No force: Docker refuses to remove any image still referenced elsewhere.
            info = next(i for i in images if i["Id"] == identity)
            for ref in info.get("RepoTags") or []:
                run([DOCKER, "image", "rm", ref])
            if not info.get("RepoTags"):
                run([DOCKER, "image", "rm", identity])
        for identity in self.state.get("legacy_cache_ids", []):
            if not re.fullmatch(r"[a-z0-9]{20,32}", identity):
                raise ValueError("CACHE_ID_REJECTED")
            run(
                [
                    DOCKER,
                    "buildx",
                    "--builder",
                    "default",
                    "prune",
                    "--force",
                    "--filter",
                    "id=" + identity,
                    "--filter",
                    "inuse=false",
                ],
                timeout=60,
            )
        if infrastructure() != self.protected:
            raise RuntimeError("PROTECTED_INFRASTRUCTURE_CHANGED")

    def discard_candidate(self):
        if not self.image_id:
            return
        retained = {self.state[k]["id"] for k in ("active", "previous") if self.state.get(k)}
        container_ids = run([DOCKER, "ps", "-aq"]).split()
        used = (
            {i["Image"] for i in json.loads(run([DOCKER, "inspect", *container_ids]))}
            if container_ids
            else set()
        )
        info = json.loads(run([DOCKER, "image", "inspect", self.ref]))
        if self.image_id in removable_images(info, retained, used):
            run([DOCKER, "image", "rm", self.ref])
