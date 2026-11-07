"""A missing PostgreSQL-volume mount cannot be mistaken for free application disk."""


def test_storage_requires_actual_database_volume(tmp_path):
    from tradeforge.runtime.maintenance import storage_ready

    class NoQuery:
        def transaction(self):
            raise AssertionError("Missing volume must fail before querying SQL")

    assert storage_ready(NoQuery(), volume_path=tmp_path / "not-mounted") is False
