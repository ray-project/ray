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


@pytest.mark.parametrize(
    "options,error",
    [
        ({"read_mode": "auto"}, ValueError),
        ({"merge_schema": "true"}, TypeError),
        ({"merge_schema": True}, ValueError),
        ({"filesystem": object()}, ValueError),
    ],
)
def test_public_api_rejects_unsupported_native_options_without_connecting(
    options, error
):
    with pytest.raises(error):
        read_hive(
            "events",
            connection_factory=lambda: pytest.fail("Invalid options must not connect"),
            **options,
        )


def test_direct_query_rejected_before_connection():
    with pytest.raises(ValueError, match="table reads"):
        read_hive(
            query="SELECT id FROM events",
            schema=pa.schema([("id", pa.int64())]),
            read_mode="direct",
            connection_factory=lambda: pytest.fail("Direct queries must not connect"),
        )


def test_direct_api_uses_native_parallelism_and_global_limit(monkeypatch, tmp_path):
    from ray.data import read_api
    from ray.data._internal.datasource_v2.formats.hive import hive_direct

    class FakeDataset:
        def limit(self, value):
            self.source_limit = value
            return self

    dataset = FakeDataset()
    calls = []
    monkeypatch.setattr(
        hive_direct,
        "read_file_table_metadata",
        lambda spec: hive_direct.HiveFileTable(
            str(tmp_path), "parquet", pa.schema([("id", pa.int64())])
        ),
    )
    monkeypatch.setattr(
        read_api,
        "_read_datasource_v2",
        lambda datasource, **kwargs: calls.append((datasource, kwargs)) or dataset,
    )
    result = read_hive(
        "events",
        connection_factory=lambda: object(),
        read_mode="direct",
        limit=3,
        override_num_blocks=4,
    )
    assert result is dataset
    assert dataset.source_limit == 3
    assert calls[0][1]["parallelism"] == 4
    assert "ray_remote_args" not in calls[0][1]


def test_direct_api_reuses_project_for_column_name_case(monkeypatch, tmp_path):
    import pyarrow.parquet as pq

    from ray.data import read_api
    from ray.data._internal.datasource_v2.formats.hive import hive_direct

    pq.write_table(pa.table({"ID": [1]}), tmp_path / "000000_0")
    calls = []

    class FakeDataset:
        def rename_columns(self, mapping):
            calls.append(mapping)
            return self

    dataset = FakeDataset()
    monkeypatch.setattr(
        hive_direct,
        "read_file_table_metadata",
        lambda spec: hive_direct.HiveFileTable(
            str(tmp_path), "parquet", pa.schema([("id", pa.int64())])
        ),
    )
    monkeypatch.setattr(
        read_api, "_read_datasource_v2", lambda *args, **kwargs: dataset
    )
    result = read_hive(
        "events", connection_factory=lambda: object(), read_mode="direct"
    )
    assert result is dataset
    assert calls == [{"ID": "id"}]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
