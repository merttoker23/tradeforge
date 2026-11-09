"""Regression guard for the bounded 0007 -> 0008 historical backfill window."""

from importlib import import_module


def test_0008_backfill_temporarily_suspends_only_immutable_observation_trigger(monkeypatch):
    migration = import_module("migrations.versions.0008_bounded_memory")
    commands: list[str] = []
    monkeypatch.setattr(migration.op, "execute", commands.append)

    migration.upgrade()

    add = "ALTER TABLE closed_bar_observations ADD COLUMN content_hash text"
    disable = "ALTER TABLE closed_bar_observations DISABLE TRIGGER observation_immutable"
    backfill = (
        "UPDATE closed_bar_observations SET content_hash=input_hash WHERE content_hash IS NULL"
    )
    enable = "ALTER TABLE closed_bar_observations ENABLE TRIGGER observation_immutable"
    not_null = "ALTER TABLE closed_bar_observations ALTER COLUMN content_hash SET NOT NULL"

    assert commands[:5] == [add, disable, backfill, enable, not_null]
    assert commands.count(disable) == commands.count(enable) == 1
    assert not any("DISABLE TRIGGER" in command for command in commands[4:])
    assert not any("DROP TRIGGER" in command for command in commands)
    assert any("RAISE EXCEPTION 'APPEND_ONLY'" in command for command in commands)


def test_0008_downgrade_does_not_suppress_immutable_observation_trigger(monkeypatch):
    migration = import_module("migrations.versions.0008_bounded_memory")
    commands: list[str] = []
    monkeypatch.setattr(migration.op, "execute", commands.append)

    migration.downgrade()

    assert not any("DISABLE TRIGGER" in command for command in commands)
    assert not any("DROP TRIGGER observation_immutable" in command for command in commands)
    assert commands[-1] == "ALTER TABLE closed_bar_observations DROP COLUMN content_hash"
