"""Tests for the public HiveServer2 read API."""

import sys
from types import SimpleNamespace
from typing import Any, Callable, cast

import pyarrow as pa
import pytest

from ray.data.read_api import read_hive


@pytest.mark.parametrize("max_errored_blocks", [0, 3, -1])
@pytest.mark.parametrize("block_count", [None, 1, 4])
def test_public_api_passes_task_options_without_repartition(
    monkeypatch, max_errored_blocks, block_count
):
    from ray.data import read_api

    class FakeDataset:
        context = SimpleNamespace(max_errored_blocks=max_errored_blocks)

    dataset = FakeDataset()
    calls = []
    warnings = []
    monkeypatch.setattr(read_api.logger, "warning", warnings.append)
    monkeypatch.setattr(
        read_api,
        "_read_datasource_v2",
        lambda datasource, **kwargs: calls.append((datasource, kwargs)) or dataset,
    )
    schema = pa.schema([("id", pa.int64())])
    result = read_hive(
        connection_factory=lambda: pytest.fail(
            "Explicit query schema must not connect"
        ),
        user="session-user",
        query="SELECT id FROM events",
        schema=schema,
        num_cpus=2,
        resources={"custom": 1},
        override_num_blocks=block_count,
    )
    assert result is dataset
    assert calls[0][0].infer_schema(None) == schema
    assert calls[0][0]._spec.user == "session-user"
    assert calls[0][1]["parallelism"] == 1
    assert calls[0][1]["ray_remote_args"] == {"max_retries": 0}
    assert calls[0][1]["num_cpus"] == 2
    assert calls[0][1]["resources"] == {"custom": 1}
    assert dataset.context.max_errored_blocks == max_errored_blocks
    if block_count is None:
        assert warnings == []
    else:
        assert len(warnings) == 1
        assert "override_num_blocks" in warnings[0]
        assert "ignored" in warnings[0]
        assert "preserve streaming" in warnings[0]


@pytest.mark.parametrize("block_count", [0, -1, True, 1.5])
def test_public_api_rejects_invalid_block_counts(monkeypatch, block_count):
    from ray.data import read_api

    warnings = []
    monkeypatch.setattr(read_api.logger, "warning", warnings.append)
    with pytest.raises(ValueError, match="override_num_blocks"):
        read_hive(
            connection_factory=lambda: pytest.fail("Invalid hints must not connect"),
            query="SELECT id FROM events",
            schema=pa.schema([("id", pa.int64())]),
            override_num_blocks=block_count,
        )
    assert warnings == []


def test_public_api_requires_connection_factory():
    with pytest.raises(TypeError, match="connection_factory"):
        cast(Callable[..., Any], read_hive)(
            query="SELECT id FROM events",
            schema=pa.schema([("id", pa.int64())]),
        )


@pytest.mark.parametrize("keyword", ["host", "password", "auth_mechanism"])
def test_public_api_rejects_replaced_connection_keywords(keyword):
    with pytest.raises(TypeError, match=keyword):
        cast(Callable[..., Any], read_hive)(
            connection_factory=lambda: object(),
            query="SELECT id FROM events",
            schema=pa.schema([("id", pa.int64())]),
            **{keyword: "unused"},
        )


def test_read_hive_driver_validation_fails_before_task():
    def failing_factory():
        import ssl

        raise ssl.SSLCertVerificationError(
            1,
            "[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: "
            "self-signed certificate in certificate chain",
        )

    with pytest.raises(
        RuntimeError,
        match="TLS certificate verification failed.*self-signed certificate",
    ):
        read_hive("analytics.events", connection_factory=failing_factory)


def test_read_hive_worker_validation_fails_in_ray_task(
    shutdown_only, restore_data_context
):
    import ray

    def failing_worker_factory():
        import ssl

        raise ssl.CertificateError(
            "hostname 'hive.internal' doesn't match 'hive.external.com'"
        )

    if not ray.is_initialized():
        ray.init(num_cpus=2, include_dashboard=False)
    schema = pa.schema([("id", pa.int64()), ("name", pa.string())])
    ds = read_hive(
        query="SELECT id, name FROM events",
        schema=schema,
        connection_factory=failing_worker_factory,
    )
    with pytest.raises(
        Exception,
        match="TLS certificate verification failed due to hostname mismatch",
    ):
        ds.take_all()


def test_read_hive_driver_succeeds_worker_fails_kerberos(
    shutdown_only, restore_data_context, tmp_path
):
    import ray

    marker_file = tmp_path / "driver_done.txt"

    def hybrid_factory():
        if not marker_file.exists():
            marker_file.write_text("done")

            class DriverCursor:
                def get_table_schema(self, table, database):
                    return [("id", "BIGINT"), ("name", "STRING")]

                def close(self):
                    pass

            class DriverConnection:
                def cursor(self, user=None):
                    return DriverCursor()

                def close(self):
                    pass

            return DriverConnection()

        raise RuntimeError(
            "Major (851968): Miscellaneous failure, "
            "Minor (2529639053): No Kerberos credentials available (default cache: /tmp/krb5cc_1000)"
        )

    if not ray.is_initialized():
        ray.init(num_cpus=2, include_dashboard=False)
    ds = read_hive("analytics.events", connection_factory=hybrid_factory)
    assert len(ds.schema().names) == 2
    assert marker_file.exists()

    with pytest.raises(Exception, match="Kerberos/GSSAPI authentication failed"):
        ds.take_all()


def test_read_hive_worker_successful_read(shutdown_only):
    import ray

    def success_factory():
        class MockCursor:
            def __init__(self):
                self.rows = [(1, "item1")]
                self.description = [
                    ("id", "BIGINT", None, None, None, None, None),
                    ("name", "STRING", None, None, None, None, None),
                ]

            def execute(self, statement):
                pass

            def fetchmany(self, size):
                r = self.rows
                self.rows = []
                return r

            def cancel_operation(self):
                pass

            def close(self):
                pass

        class MockConnection:
            def cursor(self, user=None):
                return MockCursor()

            def close(self):
                pass

        return MockConnection()

    if not ray.is_initialized():
        ray.init(num_cpus=2, include_dashboard=False)
    schema = pa.schema([("id", pa.int64()), ("name", pa.string())])
    ds = read_hive(
        query="SELECT id, name FROM events",
        schema=schema,
        connection_factory=success_factory,
    )
    rows = ds.take_all()
    assert rows == [{"id": 1, "name": "item1"}]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
