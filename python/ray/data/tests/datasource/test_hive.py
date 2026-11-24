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
        (
            {"read_mode": "direct", "schema": pa.schema([("id", pa.int64())])},
            ValueError,
        ),
        (
            {
                "read_mode": "direct",
                "query": "SELECT id FROM events",
                "schema": pa.schema([("id", pa.int64())]),
            },
            ValueError,
        ),
    ],
)
def test_public_api_rejects_native_options_before_connecting(options, error):
    with pytest.raises(error):
        read_hive(
            None if "query" in options else "events",
            connection_factory=lambda: pytest.fail("Invalid options must not connect"),
            **options,
        )


@pytest.fixture
def direct_table(tmp_path, monkeypatch):
    from ray.data._internal.datasource_v2.formats.hive import hive_direct

    def read(file_format, schema, *, positional=False, **options):
        monkeypatch.setattr(
            hive_direct,
            "read_file_table_metadata",
            lambda spec: hive_direct.HiveFileTable(
                str(tmp_path),
                file_format,
                schema,
                force_positional_evolution=positional,
            ),
        )
        return read_hive(
            "events",
            connection_factory=lambda: pytest.fail("HS2 replay is forbidden"),
            read_mode="direct",
            **options,
        )

    return read


def test_direct_api_forwards_native_execution_options(monkeypatch, direct_table):
    from unittest.mock import MagicMock

    from ray.data import read_api

    read = MagicMock(return_value=object())
    monkeypatch.setattr(read_api, "_read_datasource_v2", read)
    assert (
        direct_table("parquet", pa.schema([("id", pa.int64())]), override_num_blocks=4)
        is read.return_value
    )
    assert read.call_args.kwargs["parallelism"] == 4
    assert "ray_remote_args" not in read.call_args.kwargs


def test_direct_parquet_converts_values_before_filter_projection_and_limit(
    ray_start_regular_shared, tmp_path, direct_table
):
    from datetime import datetime, timedelta

    import pyarrow.parquet as pq

    from ray.data.expressions import col

    pq.write_table(
        pa.table(
            {
                "ID": range(5),
                " code": [None, "ab", "ab", "中文😀abc", ""],
                "tag ": [None, "abcdef", "abcdef", "中文😀abc", ""],
                "ts": pa.array([None, 1001, 1001, -1, 0], type=pa.timestamp("ns")),
            }
        ),
        tmp_path / "part",
    )
    declared = pa.schema(
        [
            ("id", pa.int64()),
            pa.field(" code", pa.string(), metadata={b"hive_type": b"char(4)"}),
            pa.field("tag ", pa.string(), metadata={b"hive_type": b"varchar(3)"}),
            ("ts", pa.timestamp("us")),
        ]
    )
    dataset = direct_table("parquet", declared)
    epoch = datetime(1970, 1, 1)
    result = sorted(dataset.take_all(), key=lambda row: row["id"])
    assert [(row[" code"], row["tag "], row["ts"]) for row in result] == [
        (None, None, None),
        *[("ab  ", "abc", epoch + timedelta(microseconds=1))] * 2,
        ("中文😀a", "中文😀", epoch - timedelta(microseconds=1)),
        ("    ", "", epoch),
    ]
    selected = (
        dataset.filter(
            expr=(col(" code") == "ab  ")
            & (col("tag ") == "abc")
            & (col("ts") == epoch + timedelta(microseconds=1))
        )
        .select_columns(["id", "tag ", "ts"])
        .limit(1)
    )
    assert selected.schema().base_schema == pa.schema(
        [("id", pa.int64()), ("tag ", pa.string()), ("ts", pa.timestamp("us"))]
    )
    assert selected.take_all() == [
        {"id": 1, "tag ": "abc", "ts": epoch + timedelta(microseconds=1)}
    ]


@pytest.mark.parametrize(
    "file_format,positional", [("parquet", False), ("orc", False), ("orc", True)]
)
def test_direct_full_schema_retains_candidates_and_lineage(
    ray_start_regular_shared, tmp_path, direct_table, file_format, positional
):
    from datetime import datetime, timedelta

    import pyarrow.parquet as pq
    from pyarrow import orc

    import ray
    from ray.data.expressions import col

    writer = pq.write_table if file_format == "parquet" else orc.write_table
    expected = []
    for index in range(17):
        prefix = ("new_" if index == 16 else "old_") if positional else ""
        values = {
            prefix + "id": pa.array([index], type=pa.int32()),
            prefix
            + "payload": pa.array(
                [None if index == 0 else {"ts": -1 if index % 2 else 1001}],
                type=pa.struct([("ts", pa.timestamp("ns"))]),
            ),
        }
        if index == 16:
            values[prefix + "late"] = ["present"]
        writer(pa.table(values), tmp_path / f"{index:06d}_0")
        expected.append(
            {
                "id": index,
                "payload": None
                if index == 0
                else {
                    "ts": datetime(1970, 1, 1)
                    + timedelta(microseconds=-1 if index % 2 else 1)
                },
                "late": "present" if index == 16 else None,
            }
        )
    declared = pa.schema(
        [
            ("id", pa.int64()),
            ("payload", pa.struct([("ts", pa.timestamp("us"))])),
            ("late", pa.string()),
        ]
    )
    dataset = direct_table(
        file_format, declared, merge_schema=True, positional=positional
    )
    writer(
        pa.table({"id": [99], "late": ["not planned"]}), tmp_path / "published_later"
    )
    assert dataset.schema().base_schema == declared
    assert sorted(dataset.take_all(), key=lambda row: row["id"]) == expected
    assert dataset.filter(expr=col("id") < 2).select_columns(["late"]).take_all() == [
        {"late": None},
        {"late": None},
    ]
    assert dataset.select_columns([]).count() == 17
    restored = ray.data.Dataset.deserialize_lineage(dataset.serialize_lineage())
    selected = (
        restored.filter(expr=col("id") >= 16)
        .select_columns(["payload", "late"])
        .limit(2)
    )
    assert selected.schema().base_schema == pa.schema(
        [declared.field("payload"), declared.field("late")]
    )
    assert selected.take_all() == [
        {"payload": expected[-1]["payload"], "late": "present"}
    ]


@pytest.mark.parametrize(
    "file_format,kind,error",
    [
        ("parquet", "type", "type mismatch"),
        ("orc", "type", "type mismatch"),
        ("parquet", "case", "column name changes"),
        ("parquet", "ambiguous", "ambiguous"),
        ("orc", "ambiguous", "ambiguous"),
    ],
)
def test_direct_late_file_failure_does_not_replay_hs2(
    ray_start_regular_shared, tmp_path, direct_table, file_format, kind, error
):
    import pyarrow.parquet as pq
    from pyarrow import orc

    writer = pq.write_table if file_format == "parquet" else orc.write_table
    for index in range(17):
        data = (
            {"id": [index]}
            if index < 16
            else {
                "type": {"id": ["bad"]},
                "case": {"ID": [42]},
                "ambiguous": {"id": [42], "ID": [43]},
            }[kind]
        )
        writer(pa.table(data), tmp_path / f"{index:06d}_0")

    dataset = direct_table(file_format, pa.schema([("id", pa.int64())]))
    with pytest.raises(Exception, match=error) as failure:
        dataset.take_all()
    assert "HS2 replay is forbidden" not in str(failure.value)
    if file_format == "parquet" and kind == "type":
        with pytest.raises((pa.ArrowInvalid, pa.ArrowTypeError)):
            direct_table(
                file_format, pa.schema([("id", pa.int64())]), merge_schema=True
            )


@pytest.mark.parametrize(
    "file_format,merge_schema", [("parquet", True), ("orc", False), ("orc", True)]
)
def test_direct_empty_table_preserves_schema(
    ray_start_regular_shared, direct_table, file_format, merge_schema
):

    declared = pa.schema([("id", pa.int64())])
    dataset = direct_table(file_format, declared, merge_schema=merge_schema)
    assert dataset.take_all() == []
    assert dataset.schema().base_schema == declared


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
