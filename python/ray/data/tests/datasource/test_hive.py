"""Tests for the public HiveServer2 read API."""

import json
import sys
from dataclasses import fields
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


def test_direct_parquet_converts_values_before_filter_projection_and_limit(
    ray_start_regular_shared, tmp_path, monkeypatch
):
    from datetime import datetime, timedelta

    import pyarrow.parquet as pq

    from ray.data._internal.datasource_v2.formats.hive import hive_direct
    from ray.data.expressions import col

    pq.write_table(
        pa.table(
            {
                "id": [0, 1, 2, 3],
                "code": [None, "ab", "ab", "ab"],
                "tag": [None, "abcdef", "abcdef", "abcdef"],
                "ts": pa.array([None, 1001, 1001, 1001], type=pa.timestamp("ns")),
            }
        ),
        tmp_path / "000000_0",
    )
    hive_schema = pa.schema(
        [
            ("id", pa.int64()),
            pa.field("code", pa.string(), metadata={b"hive_type": b"char(4)"}),
            pa.field("tag", pa.string(), metadata={b"hive_type": b"varchar(3)"}),
            ("ts", pa.timestamp("us")),
        ]
    )
    monkeypatch.setattr(
        hive_direct,
        "read_file_table_metadata",
        lambda spec: hive_direct.HiveFileTable(str(tmp_path), "parquet", hive_schema),
    )
    converted_time = datetime(1970, 1, 1) + timedelta(microseconds=1)
    dataset = (
        read_hive(
            "events",
            connection_factory=lambda: pytest.fail(
                "Native workers must not connect to HS2"
            ),
            read_mode="direct",
        )
        .filter(
            expr=(col("code") == "ab  ")
            & (col("tag") == "abc")
            & (col("ts") == converted_time)
        )
        .select_columns(["id", "tag", "ts"])
        .limit(2)
    )
    assert dataset.schema().base_schema == pa.schema(
        [("id", pa.int64()), ("tag", pa.string()), ("ts", pa.timestamp("us"))]
    )
    rows = dataset.take_all()
    assert len(rows) == 2
    ids = {row["id"] for row in rows}
    assert len(ids) == 2 and ids <= {1, 2, 3}
    assert all(row["tag"] == "abc" and row["ts"] == converted_time for row in rows)


@pytest.mark.parametrize("positional", [False, True])
@pytest.mark.parametrize("unsampled", [False, True])
@pytest.mark.parametrize("operation", ["projection", "filter_only"])
def test_direct_orc_scan_requests_prune_unused_columns(
    ray_start_regular_shared, tmp_path, monkeypatch, positional, unsampled, operation
):
    import pyarrow.dataset as pds
    from pyarrow import orc

    from ray.data._internal.datasource_v2.formats.hive import hive_direct
    from ray.data.expressions import col

    directory = tmp_path / "data"
    directory.mkdir()
    file_count = 17 if unsampled else 1
    physical_names = {}
    for index in range(file_count):
        late = index == 16
        if positional:
            prefix = "renamed" if late else "old"
            names = [
                prefix + suffix for suffix in ("_unused", "_id", "_value", "_payload")
            ]
            data = dict(
                zip(
                    names,
                    [
                        [-1, -1],
                        [2 * index, 2 * index + 1],
                        [10 + 2 * index, 11 + 2 * index],
                        ["wide" * 1024] * 2,
                    ],
                )
            )
        else:
            data = {
                "ID" if late else "id": [2 * index, 2 * index + 1],
                "value": [10 + 2 * index, 11 + 2 * index],
                "payload": ["wide" * 1024] * 2,
                "unused": [-1, -1],
            }
            names = (
                ["ID", "payload", "value", "unused"]
                if late
                else ["payload", "value", "id", "unused"]
            )
            data = {name: data[name] for name in names}
        path = directory / f"{index:06d}_0"
        orc.write_table(pa.table(data), path)
        physical_names[str(path)] = names

    hive_schema = pa.schema(
        [
            ("unused", pa.int64()),
            ("id", pa.int64()),
            ("value", pa.int64()),
            ("payload", pa.string()),
        ]
    )
    monkeypatch.setattr(
        hive_direct,
        "read_file_table_metadata",
        lambda spec: hive_direct.HiveFileTable(
            str(directory), "orc", hive_schema, force_positional_evolution=positional
        ),
    )
    sampled_paths = []
    original_infer = hive_direct._infer_orc_read_schema

    def infer_schema(table, sample, filesystem):
        sampled_paths.extend(str(path) for path in sample.paths)
        return original_infer(table, sample, filesystem)

    monkeypatch.setattr(hive_direct, "_infer_orc_read_schema", infer_schema)
    trace_path = str(tmp_path / "scan-requests.jsonl")
    original_create_scanner = hive_direct.HiveDirectDatasource.create_scanner

    class TracingScanner(hive_direct._HiveOrcScanner):
        def create_reader(self):
            reader = super().create_reader()
            original_iter = reader._iter_fragment_tables

            class Fragment:
                def __init__(self, fragment):
                    self._fragment = fragment

                def __getattr__(self, name):
                    return getattr(self._fragment, name)

                def scanner(self, **kwargs):
                    request = {
                        "path": self._fragment.path,
                        "columns": {
                            name: str(expr) for name, expr in kwargs["columns"].items()
                        },
                        "filter": str(kwargs["filter"]),
                    }
                    with open(trace_path, "a") as trace:
                        trace.write(json.dumps(request) + "\n")
                    return self._fragment.scanner(**kwargs)

            def traced_iter(fragment, scanner_kwargs):
                yield from original_iter(Fragment(fragment), scanner_kwargs)

            reader._iter_fragment_tables = traced_iter
            return reader

    def create_scanner(datasource, *args, **kwargs):
        scanner = original_create_scanner(datasource, *args, **kwargs)
        return TracingScanner(
            **{
                field.name: getattr(scanner, field.name)
                for field in fields(scanner)
                if field.init
            }
        )

    monkeypatch.setattr(
        hive_direct.HiveDirectDatasource, "create_scanner", create_scanner
    )
    dataset = read_hive(
        "events",
        connection_factory=lambda: pytest.fail(
            "Native workers must not connect to HS2"
        ),
        read_mode="direct",
        override_num_blocks=2,
    )
    assert sampled_paths == list(physical_names)[:16]
    if operation == "filter_only":
        dataset = dataset.filter(expr=col("id") > 0)
    dataset = dataset.select_columns(["value"])
    assert dataset.schema().base_schema == pa.schema([("value", pa.int64())])
    expected = range(11 if operation == "filter_only" else 10, 10 + 2 * file_count)
    assert sorted(row["value"] for row in dataset.take_all()) == list(expected)

    with open(trace_path) as trace:
        requests = [json.loads(line) for line in trace]
    assert len(requests) == file_count
    assert {request["path"] for request in requests} == set(physical_names)
    required = ["id", "value"] if operation == "filter_only" else ["value"]
    for request in requests:
        names = physical_names[request["path"]]
        indices = (
            {"id": 1, "value": 2}
            if positional
            else {
                "id": names.index("ID" if "ID" in names else "id"),
                "value": names.index("value"),
            }
        )
        assert request["columns"] == {
            name: str(pds.field(indices[name])) for name in required
        }
        assert request["filter"] == "None"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
