"""Render only approved Alembic upgrades with an administrator-owned environment."""

import io
from pathlib import Path

from alembic import command
from alembic.config import Config

ENVIRONMENT = """from alembic import context
context.configure(url="postgresql://", literal_binds=True, transactional_ddl=False,
                  version_table="schema_migrations")
context.run_migrations()
"""


def render(sources, current, head, directory):
    root = Path(directory) / "alembic"
    versions = root / "versions"
    versions.mkdir(parents=True)
    for name, source in sources.items():
        (versions / name).write_text(source)
    (root / "env.py").write_text(ENVIRONMENT)
    output = io.StringIO()
    config = Config(output_buffer=output)
    config.set_main_option("script_location", str(root))
    command.upgrade(config, current + ":" + head, sql=True)
    return output.getvalue()
