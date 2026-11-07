"""Fail CI on tracked legacy paths, runtime residues, or recognizable secrets."""

import ast
import re
import subprocess
from pathlib import Path, PurePosixPath

OLD_ROOTS = {"core", "engine", "exchange", "services", "webapp", "data", "scripts"}
OLD_MODULES = OLD_ROOTS | {"nats", "redis", "influxdb", "influxdb_client"}
OLD_IDENTIFIER = re.compile(
    r"RAVEN_NOT_READY|PocketOption|binary_options|KAIROS|NATS_URL|REDIS_URL|INFLUXDB_\w+"
)
OLD_FILES = {
    "Tradeforge.cmd",
    "Tradeforge.vbs",
    "tools/windows_launcher.py",
    "tools/local_runtime.py",
    "tools/setup_local_runtime.py",
    "tools/start_local.py",
    "src/tradeforge/runtime/credential_store.py",
    "src/tradeforge/runtime/process_group.py",
    "Dockerfile",
    "docker-compose.yml",
    "requirements.txt",
    "requirements-dev.txt",
    ".env.production-execution.example",
    ".env.testnet-execution.example",
    ".cursorignore",
}
RUNTIME_RESIDUE = re.compile(
    r"\b(?:engine|services|core|exchange|webapp)\s*\."
    r"|\b(?:from|import)\s+(?:engine|services|core|exchange|webapp|nats|redis|influxdb)\b"
    r"|\b(?:RAVEN_NOT_READY|PocketOption|binary_options|KAIROS|NATS_URL|REDIS_URL)\b"
    r"|\bINFLUXDB_\w*|legacy strategy",
    re.IGNORECASE,
)
SECRET = re.compile(
    r"-----BEGIN (?:[A-Z0-9]+ )?PRIVATE KEY-----"
    r"|\bgh[pousr]_[A-Za-z0-9]{30,}\b"
    r"|\bgithub_pat_[A-Za-z0-9_]{30,}\b"
    r"|\bAKIA[A-Z0-9]{16}\b"
)


def python_has_legacy(content: str) -> bool:
    """Inspect every Python location; quoted negative fixtures are data, not imports."""
    try:
        tree = ast.parse(content)
    except SyntaxError:
        return True  # Unparseable tracked Python cannot pass the gate.
    for node in ast.walk(tree):
        modules: list[str] = []
        if isinstance(node, ast.Import):
            modules = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            modules = [node.module or ""]
        elif isinstance(node, ast.Name) and OLD_IDENTIFIER.fullmatch(node.id):
            return True
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            if OLD_IDENTIFIER.fullmatch(node.value):
                return True
        elif isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
            if node.value.id in OLD_ROOTS:
                return True
        elif isinstance(node, ast.Call) and node.args:
            function = node.func
            name = function.id if isinstance(function, ast.Name) else ""
            if isinstance(function, ast.Attribute):
                name = function.attr
            argument = node.args[0]
            if name in {"import_module", "__import__"} and isinstance(argument, ast.Constant):
                if isinstance(argument.value, str):
                    modules = [argument.value]
        if any(module.split(".")[0] in OLD_MODULES for module in modules):
            return True
    return False


def inspect_files(root: Path, paths: list[str]) -> list[str]:
    """Return path-only diagnostics; never print a secret's contents."""
    problems: list[str] = []
    for name in paths:
        path = PurePosixPath(name)
        if path.is_absolute() or ".." in path.parts:
            problems.append(f"{name}: invalid repository path")
            continue
        if (path.suffix.lower() == ".md" and name not in {"README.md", "AGENTS.md"}) or path.parts[
            0
        ] == "docs":
            problems.append(f"{name}: local-only documentation must not be tracked")
        if path.parts[0] in OLD_ROOTS or name in OLD_FILES:
            problems.append(f"{name}: forbidden legacy path")
        if (path.name == ".env" or path.name.startswith(".env.")) and name != ".env.example":
            problems.append(f"{name}: forbidden environment file")
        if path.suffix.lower() in {".pem", ".key", ".p12", ".pfx"}:
            problems.append(f"{name}: forbidden key file")
        file = root / name
        if file.is_symlink():
            problems.append(f"{name}: symlinks are not allowed")
            continue
        try:
            content = file.read_text(encoding="utf-8")
        except (OSError, UnicodeError):
            problems.append(f"{name}: cannot inspect tracked file")
            continue
        if SECRET.search(content):
            problems.append(f"{name}: possible secret")
        if path.suffix == ".py" and python_has_legacy(content):
            problems.append(f"{name}: forbidden or unparseable Python runtime content")
        runtime = path.parts[0] in {"src", "apps", "deploy"} or name in {
            ".env.example",
            "pyproject.toml",
            ".github/workflows/ci.yml",
        }
        if runtime and RUNTIME_RESIDUE.search(content):
            problems.append(f"{name}: forbidden legacy runtime content")
    return problems


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run(["git", "ls-files", "-z"], cwd=root, check=True, capture_output=True)
    paths = result.stdout.decode("utf-8").rstrip("\0").split("\0") if result.stdout else []
    if not paths:
        print("FAIL: no tracked files to inspect")
        return 1
    problems = inspect_files(root, paths)
    for problem in problems:
        print(problem)
    if problems:
        print(f"FAIL: {len(problems)} repository violations")
        return 1
    print(f"PASS: legacy/secret scan ({len(paths)} tracked files)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
