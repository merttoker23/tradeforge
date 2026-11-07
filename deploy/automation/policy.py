"""Fail-closed, administrator-owned production deployment policy."""

import ast
import hashlib
import re

REPOSITORY = "merttoker23/tradeforge"
# Installing this allowlist requires independent server-operator review of the
# exact migration. No other SQL is exempt from the additive grammar.
REVIEWED_MIGRATIONS = {
    "0007_binance_environment.py": (
        "7a31ec112a4a3fb45514003cbd8683f7452a5e62428a08bf53052ec82b49ee67"
    ),
}
IMAGE = "ghcr.io/" + REPOSITORY
AUDIENCE = "https://forgetrade.tech/_tradeforge/deploy"
WORKFLOW = REPOSITORY + "/.github/workflows/ci.yml@refs/heads/main"
APPLICATIONS = ("market_gateway", "quant_core", "execution_guardian", "dashboard")
IDENTIFIER = r"[a-z][a-z0-9_]{0,62}"
TYPE = (
    r"(?:text|uuid|boolean|bigint|integer|smallint|numeric|timestamptz|jsonb|bytea|date"
    r"|double precision)"
)


def safe_sql(statement):
    """Allow only a small additive PostgreSQL grammar; everything else is reviewed."""
    sql = " ".join(statement.split())
    column = rf"{IDENTIFIER} {TYPE}"
    patterns = (
        rf"ALTER TABLE ({IDENTIFIER}) ADD COLUMN {column}(?: NULL)?",
        rf"CREATE TABLE ({IDENTIFIER}) \({column}(?: NOT NULL| NULL)?"
        rf"(?:, {column}(?: NOT NULL| NULL)?)*\)",
        rf"CREATE INDEX {IDENTIFIER} ON ({IDENTIFIER}) \({IDENTIFIER}(?:, {IDENTIFIER})*\)",
    )
    for pattern in patterns:
        matched = re.fullmatch(pattern, sql, re.IGNORECASE)
        if matched and matched[1].lower() != "schema_migrations":
            return sql
    raise ValueError("MANUAL_REVIEW_UNSAFE_SQL")


def parse_revision(source, *, filename=None):
    """Inspect AST without executing migrations; use pinned approval for known SQL."""
    approved_sql = (
        filename in REVIEWED_MIGRATIONS
        and hashlib.sha256(source.encode()).hexdigest() == REVIEWED_MIGRATIONS[filename]
    )
    try:
        tree = ast.parse(source)
        metadata = {}
        functions = {}
        imports = 0
        for node in tree.body:
            if isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant):
                if isinstance(node.value.value, str):
                    continue
            if isinstance(node, ast.ImportFrom) and node.module == "alembic" and not node.level:
                if [(x.name, x.asname) for x in node.names] == [("op", None)]:
                    imports += 1
                    continue
            if isinstance(node, ast.Assign) and len(node.targets) == 1:
                name = node.targets[0]
                if (
                    isinstance(name, ast.Name)
                    and name.id in {"revision", "down_revision", "branch_labels", "depends_on"}
                    and name.id not in metadata
                ):
                    metadata[name.id] = ast.literal_eval(node.value)
                    continue
            if isinstance(node, ast.FunctionDef) and node.name in {"upgrade", "downgrade"}:
                if (
                    node.name not in functions
                    and not node.decorator_list
                    and not node.args.args
                    and not node.args.posonlyargs
                    and not node.args.kwonlyargs
                    and not node.args.vararg
                    and not node.args.kwarg
                    and not node.args.defaults
                    and (
                        node.returns is None
                        or isinstance(node.returns, ast.Constant)
                        and node.returns.value is None
                    )
                ):
                    functions[node.name] = node
                    continue
            raise ValueError("MANUAL_REVIEW_MIGRATION_PYTHON")
        if (
            imports != 1
            or set(functions) != {"upgrade", "downgrade"}
            or set(metadata) != {"revision", "down_revision", "branch_labels", "depends_on"}
            or metadata["branch_labels"] is not None
            or metadata["depends_on"] is not None
        ):
            raise ValueError("MANUAL_REVIEW_MIGRATION_STRUCTURE")
        for key in ("revision", "down_revision"):
            if not isinstance(metadata[key], str) or not re.fullmatch(
                r"[a-zA-Z0-9_]{1,32}", metadata[key]
            ):
                raise ValueError("MANUAL_REVIEW_REVISION")
        statements = []
        for node in functions["upgrade"].body:
            if isinstance(node, ast.Pass):
                continue
            if isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant):
                if isinstance(node.value.value, str):
                    continue
            if isinstance(node, ast.Expr) and isinstance(node.value, ast.Call):
                call = node.value
                if (
                    isinstance(call.func, ast.Attribute)
                    and isinstance(call.func.value, ast.Name)
                    and call.func.value.id == "op"
                    and call.func.attr == "execute"
                    and len(call.args) == 1
                    and not call.keywords
                    and isinstance(call.args[0], ast.Constant)
                    and isinstance(call.args[0].value, str)
                ):
                    statements.append(
                        call.args[0].value if approved_sql else safe_sql(call.args[0].value)
                    )
                    continue
            raise ValueError("MANUAL_REVIEW_MIGRATION_OPERATION")
        return metadata["revision"], metadata["down_revision"], statements
    except (SyntaxError, TypeError, KeyError) as error:
        raise ValueError("MANUAL_REVIEW_MIGRATION_PARSE") from error


def migration_plan(sources, trusted_hashes, current):
    for name, digest in trusted_hashes.items():
        if name not in sources or hashlib.sha256(sources[name].encode()).hexdigest() != digest:
            raise ValueError("MIGRATION_HISTORY_CHANGED")
    pending = {}
    for name, source in sources.items():
        if name in trusted_hashes:
            continue
        if not re.fullmatch(r"[a-zA-Z0-9_]+\.py", name) or name == "__init__.py":
            raise ValueError("MANUAL_REVIEW_MIGRATION_FILENAME")
        if name in REVIEWED_MIGRATIONS:
            if hashlib.sha256(source.encode()).hexdigest() != REVIEWED_MIGRATIONS[name]:
                raise ValueError("MANUAL_REVIEW_APPROVED_MIGRATION_CHANGED")
        revision, parent, statements = parse_revision(source, filename=name)
        if revision in pending or revision == current:
            raise ValueError("MANUAL_REVIEW_DUPLICATE_REVISION")
        pending[revision] = (parent, statements)
    head = current
    ordered = []
    while pending:
        children = [rev for rev, (parent, _) in pending.items() if parent == head]
        if len(children) != 1:
            raise ValueError("MANUAL_REVIEW_NONLINEAR_MIGRATIONS")
        head = children[0]
        parent, statements = pending.pop(head)
        ordered.append((head, parent, statements))
    return head, ordered


def validate_claims(claims, sha):
    expected = {
        "repository": REPOSITORY,
        "repository_id": "1305289470",
        "repository_owner_id": "61198883",
        "ref": "refs/heads/main",
        "event_name": "push",
        "workflow_ref": WORKFLOW,
        "workflow_sha": sha,
        "sha": sha,
        "runner_environment": "github-hosted",
    }
    if not re.fullmatch(r"[a-f0-9]{40}", sha):
        raise ValueError("INVALID_SHA")
    if any(claims.get(k) != v for k, v in expected.items()):
        raise ValueError("OIDC_IDENTITY_REJECTED")
    for name in ("run_id", "run_attempt", "check_run_id"):
        if not re.fullmatch(r"[1-9][0-9]{0,20}", str(claims.get(name, ""))):
            raise ValueError("OIDC_JOB_REJECTED")
    if not isinstance(claims.get("jti"), str) or not 1 <= len(claims["jti"]) <= 256:
        raise ValueError("OIDC_JTI_REJECTED")


def image_ref(digest):
    if not isinstance(digest, str) or not re.fullmatch(r"sha256:[a-f0-9]{64}", digest):
        raise ValueError("INVALID_IMAGE_DIGEST")
    return IMAGE + "@" + digest


def compatible(before, after):
    return all(after.get(k) == v for k, v in before.items())


def removable_images(images, retained_ids, used_ids):
    remove = []
    for item in images:
        labels = item.get("Config", {}).get("Labels") or {}
        tags = item.get("RepoTags") or []
        digests = item.get("RepoDigests") or []
        owned = labels.get("io.tradeforge.managed") == "true" or (
            labels.get("com.docker.compose.project") == "forgetrade"
        )
        refs = [r for r in tags + digests if r != "<none>:<none>"]
        if (
            owned
            and item["Id"] not in retained_ids | used_ids
            and all(r.startswith((IMAGE + ":", IMAGE + "@", "forgetrade:")) for r in refs)
        ):
            remove.append(item["Id"])
    return sorted(set(remove))
