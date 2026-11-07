import json
import os
from dataclasses import fields
from decimal import Decimal
from pathlib import Path
from typing import Iterator

import pandas as pd
import pyarrow as pa
import pytest
from pyarrow import orc

import ray
from ray.data._internal.arrow_block import _BATCH_SIZE_PRESERVING_STUB_COL_NAME
from ray.data._internal.datasource.orc_datasink import ORCDatasink
from ray.data._internal.datasource.orc_datasource import ORCDatasource
from ray.data._internal.object_extensions.arrow import ArrowPythonObjectType
from ray.data._internal.util import rows_same
from ray.data.block import BlockAccessor
from ray.data.datasource.partitioning import Partitioning, PartitionStyle
from ray.data.expressions import col


@pytest.fixture(params=[False, True], ids=["v1", "v2"])
def orc_reader_version(request, monkeypatch):
    from ray.data.context import DataContext

    monkeypatch.setattr(DataContext.get_current(), "use_datasource_v2", request.param)
    return request.param


def _write_orc(path, table):
    with pa.OSFile(path, "wb") as sink:
        orc.write_table(table, sink)


def _list_visible_files(directory):
    return sorted(
        filename for filename in os.listdir(directory) if not filename.startswith(".")
    )


def _read_orc_dir(directory):
    return pa.concat_tables(
        orc.read_table(os.path.join(directory, filename))
        for filename in _list_visible_files(directory)
    )


def _write_typed_partition_orc_files(root, style, arrow_type=pa.int64()):
    from ray.data.datasource.partitioning import PartitionStyle

    paths = []
    for year, ids in [(2023, [0, 1, 2, 3]), (2024, [4, 5, 6, 7])]:
        directory = root / (
            f"year={year}" if style == PartitionStyle.HIVE else str(year)
        )
        directory.mkdir(parents=True)
        path = directory / "data.orc"
        _write_orc(
            str(path),
            pa.table({"id": ids, "year": pa.array([year] * 4, type=arrow_type)}),
        )
        paths.append(str(path))
    return paths


@pytest.mark.parametrize("style", ["hive", "dir"])
@pytest.mark.parametrize("arrow_type", [pa.int32(), pa.int64()])
@pytest.mark.parametrize("expr", [col("year") == 2024, col("year") > 2023])
def test_read_orc_v2_partition_predicate_matches_memory_logical_schema(
    ray_start_regular_shared, tmp_path, monkeypatch, style, arrow_type, expr
):
    from ray.data.context import DataContext
    from ray.data.datasource.partitioning import Partitioning, PartitionStyle

    monkeypatch.setattr(DataContext.get_current(), "use_datasource_v2", True)
    partition_style = PartitionStyle(style)
    paths = _write_typed_partition_orc_files(tmp_path, partition_style, arrow_type)
    partitioning = Partitioning(
        partition_style, base_dir=str(tmp_path), field_names=["year"]
    )

    def read():
        return ray.data.read_orc(
            paths, partitioning=partitioning, override_num_blocks=2
        )

    source = read()
    schema = source.schema().base_schema
    assert isinstance(schema, pa.Schema)
    assert schema.field("year").type == arrow_type
    memory = ray.data.from_arrow(pa.Table.from_pylist(source.take_all(), schema=schema))
    assert memory.schema().base_schema.equals(schema, check_metadata=True)
    expected = [{"id": i, "year": 2024} for i in [4, 5, 6, 7]]
    assert (
        sorted(memory.filter(expr=expr).take_all(), key=lambda row: row["id"])
        == expected
    )
    assert (
        sorted(read().filter(expr=expr).take_all(), key=lambda row: row["id"])
        == expected
    )
    assert partitioning.field_types == {}


def test_read_orc_v2_partition_predicate_uses_float32_logical_type(
    ray_start_regular_shared, tmp_path, monkeypatch
):
    from ray.data.context import DataContext
    from ray.data.datasource.partitioning import Partitioning, PartitionStyle

    monkeypatch.setattr(DataContext.get_current(), "use_datasource_v2", True)
    directory = tmp_path / "year=16777217"
    directory.mkdir()
    path = directory / "data.orc"
    _write_orc(
        str(path),
        pa.table(
            {
                "id": [1],
                "year": pa.array([16777217.0], type=pa.float32()),
            }
        ),
    )

    source = ray.data.read_orc(
        str(tmp_path),
        partitioning=Partitioning(PartitionStyle.HIVE, field_names=["year"]),
        override_num_blocks=1,
    )
    schema = source.schema().base_schema
    assert isinstance(schema, pa.Schema)
    assert schema.field("year").type == pa.float32()

    rows = source.filter(expr=col("year") == 16777216.0).take_all()
    assert rows == [{"id": 1, "year": 16777216.0}]


def test_read_orc_v2_partition_predicate_does_not_cast_unused_invalid_field(
    ray_start_regular_shared, tmp_path, monkeypatch
):
    from ray.data.context import DataContext
    from ray.data.datasource.partitioning import Partitioning, PartitionStyle

    monkeypatch.setattr(DataContext.get_current(), "use_datasource_v2", True)
    directory = tmp_path / "year=invalid" / "country=US"
    directory.mkdir(parents=True)
    path = directory / "data.orc"
    _write_orc(
        str(path),
        pa.table(
            {
                "id": [1],
                "year": pa.array([2024], type=pa.int32()),
            }
        ),
    )

    source = ray.data.read_orc(
        str(tmp_path),
        partitioning=Partitioning(PartitionStyle.HIVE, field_names=["year", "country"]),
        override_num_blocks=1,
    )
    schema = source.schema().base_schema
    assert isinstance(schema, pa.Schema)
    assert schema.field("year").type == pa.int32()

    rows = source.filter(expr=col("country") == "US").select_columns(["id"]).take_all()
    assert rows == [{"id": 1}]


@pytest.mark.parametrize("reverse_paths", [False, True])
@pytest.mark.parametrize(
    "operation", ["and", "or", "limit", "empty", "count", "empty_count"]
)
def test_read_orc_v2_typed_partition_filter_combinations(
    ray_start_regular_shared, tmp_path, monkeypatch, reverse_paths, operation
):
    from ray.data.context import DataContext
    from ray.data.datasource.partitioning import Partitioning, PartitionStyle

    monkeypatch.setattr(DataContext.get_current(), "use_datasource_v2", True)
    paths = _write_typed_partition_orc_files(tmp_path, PartitionStyle.HIVE)
    if reverse_paths:
        paths.reverse()
    ds = ray.data.read_orc(
        paths,
        partitioning=Partitioning(PartitionStyle.HIVE, field_names=["year"]),
        override_num_blocks=2,
    )
    predicate = col("year") == 2024
    if operation == "and":
        predicate = predicate & (col("id") > 5)
    elif operation == "or":
        predicate = predicate | (col("id") == 0)
    elif operation == "empty":
        predicate = col("year") == 2025
    filtered = ds.filter(expr=predicate)
    if operation in {"count", "empty_count"}:
        if operation == "empty_count":
            filtered = filtered.select_columns([])
        assert filtered.count() == 4
        return
    filtered = filtered.select_columns(["id"])
    if operation == "limit":
        rows = filtered.limit(2).take_all()
        assert len(rows) == 2
        assert all(row["id"] in [4, 5, 6, 7] for row in rows)
    else:
        expected = {"and": [6, 7], "or": [0, 4, 5, 6, 7], "empty": []}[operation]
        assert sorted(row["id"] for row in filtered.take_all()) == expected


def test_read_orc_v2_typed_partition_prunes_before_worker_read(
    ray_start_regular_shared, tmp_path, monkeypatch
):
    from ray.data._internal.datasource_v2.formats.orc.orc_datasource_v2 import (
        OrcDatasourceV2,
    )
    from ray.data._internal.datasource_v2.formats.orc.orc_file_reader import (
        OrcFileReader,
    )
    from ray.data._internal.datasource_v2.formats.orc.orc_scanner import OrcScanner
    from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
    from ray.data.context import DataContext
    from ray.data.datasource.partitioning import Partitioning, PartitionStyle

    monkeypatch.setattr(DataContext.get_current(), "use_datasource_v2", True)
    paths = _write_typed_partition_orc_files(tmp_path / "input", PartitionStyle.HIVE)
    trace_path = str(tmp_path / "reader-requests.jsonl")
    original_create_scanner = OrcDatasourceV2.create_scanner

    class RecordingScanner(OrcScanner):
        def create_reader(self):
            reader = super().create_reader()
            original_read = reader.read

            def traced_read(input_split: FileManifest) -> Iterator[pa.Table]:
                import inspect

                with open(trace_path, "a") as trace:
                    trace.write(
                        json.dumps(
                            {
                                "paths": input_split.paths.tolist(),
                                "pid": os.getpid(),
                                "source": str(
                                    Path(inspect.getfile(OrcFileReader)).resolve()
                                ),
                            }
                        )
                        + "\n"
                    )
                yield from original_read(input_split)

            reader.read = traced_read
            return reader

    def create_scanner(datasource, schema, filesystem=None, **options):
        scanner = original_create_scanner(datasource, schema, filesystem, **options)
        return RecordingScanner(
            **{field.name: getattr(scanner, field.name) for field in fields(scanner)}
        )

    monkeypatch.setattr(OrcDatasourceV2, "create_scanner", create_scanner)
    rows = (
        ray.data.read_orc(
            paths,
            partitioning=Partitioning(PartitionStyle.HIVE, field_names=["year"]),
            override_num_blocks=2,
        )
        .filter(expr=col("year") == 2024)
        .select_columns(["id"])
        .take_all()
    )
    assert sorted(row["id"] for row in rows) == [4, 5, 6, 7]
    records = [json.loads(line) for line in Path(trace_path).read_text().splitlines()]
    assert records
    assert {path for record in records for path in record["paths"]} == {paths[1]}
    assert all(record["pid"] != os.getpid() for record in records)
    assert all(
        record["source"].endswith(
            "python/ray/data/_internal/datasource_v2/formats/orc/orc_file_reader.py"
        )
        for record in records
    )


def test_read_orc_skips_empty_stripes(monkeypatch):
    record_batches = [
        pa.record_batch([pa.array([], type=pa.int64())], names=["id"]),
        pa.record_batch([pa.array([1], type=pa.int64())], names=["id"]),
    ]

    class FakeORCFile:
        nstripes = len(record_batches)

        def read_stripe(self, stripe_index):
            return record_batches[stripe_index]

    monkeypatch.setattr(orc, "ORCFile", lambda _: FakeORCFile())

    datasource = ORCDatasource.__new__(ORCDatasource)
    tables = list(datasource._read_stream(None, "unused"))

    assert tables == [pa.table({"id": [1]})]


def test_read_orc_rejects_pickle_object_columns(monkeypatch):
    storage = pa.array([b"payload"], type=pa.large_binary())
    extension_array = pa.ExtensionArray.from_storage(ArrowPythonObjectType(), storage)
    record_batch = pa.record_batch([extension_array], names=["col"])

    class FakeORCFile:
        nstripes = 1

        def read_stripe(self, stripe_index):
            assert stripe_index == 0
            return record_batch

    monkeypatch.setattr(orc, "ORCFile", lambda _: FakeORCFile())

    datasource = ORCDatasource.__new__(ORCDatasource)
    with pytest.raises(ValueError, match="arrow_pickled_object"):
        list(datasource._read_stream(None, "unused"))


def test_read_orc_basic(ray_start_regular_shared, tmp_path, orc_reader_version):
    path = os.path.join(tmp_path, "data.orc")
    table = pa.table({"id": [0, 1, 2], "name": ["a", "b", "c"]})
    _write_orc(path, table)

    ds = ray.data.read_orc(path)

    assert ds.count() == 3
    assert set(ds.schema().names) == {"id", "name"}
    assert sorted(row["id"] for row in ds.take_all()) == [0, 1, 2]


def test_read_orc_multiple_files(
    ray_start_regular_shared, tmp_path, orc_reader_version
):
    for i in range(3):
        _write_orc(os.path.join(tmp_path, f"part_{i}.orc"), pa.table({"id": [i]}))

    ds = ray.data.read_orc(str(tmp_path))

    assert ds.count() == 3
    assert sorted(row["id"] for row in ds.take_all()) == [0, 1, 2]


def test_read_orc_include_paths(ray_start_regular_shared, tmp_path, orc_reader_version):
    path = os.path.join(tmp_path, "data.orc")
    _write_orc(path, pa.table({"id": [0]}))

    ds = ray.data.read_orc(path, include_paths=True)

    rows = ds.take_all()
    assert all("path" in row for row in rows)
    assert all(row["path"].endswith("data.orc") for row in rows)


@pytest.mark.parametrize(
    "partitioning", [None, Partitioning(PartitionStyle.HIVE, field_names=["path"])]
)
@pytest.mark.parametrize(
    "operation", ["path", "directory", "and", "or", "not", "limit_count"]
)
def test_read_orc_v2_synthesized_path_filters(
    ray_start_regular_shared, tmp_path, monkeypatch, partitioning, operation
):
    from ray.data.context import DataContext

    monkeypatch.setattr(DataContext.get_current(), "use_datasource_v2", True)
    directory = tmp_path / "path=foo"
    directory.mkdir()
    first = directory / "first.orc"
    matching = directory / "matching with spaces.orc"
    _write_orc(str(first), pa.table({"id": [1]}))
    _write_orc(str(matching), pa.table({"id": [2]}))
    ds = ray.data.read_orc(
        [str(first), str(matching)],
        include_paths=True,
        partitioning=partitioning,
        override_num_blocks=1,
    )
    predicates = {
        "path": col("path") == str(matching),
        "directory": col("path") == "foo",
        "and": (col("path") == str(matching)) & (col("id") == 2),
        "or": (col("path") == "foo") | (col("id") == 2),
        "not": ~(col("path") == "foo"),
        "limit_count": col("path") == str(matching),
    }
    selected = ds.filter(expr=predicates[operation])
    if operation == "limit_count":
        selected = selected.select_columns(["id"])
        assert selected.limit(1).take_all() == [{"id": 2}]
        assert selected.count() == 1
        assert ds.filter(expr=col("path") == "foo").select_columns(["id"]).count() == 0
    else:
        rows = selected.take_all()
        expected = (
            [] if operation == "directory" else [1, 2] if operation == "not" else [2]
        )
        assert sorted(row["id"] for row in rows) == expected
        assert all(
            row["path"] == str(first if row["id"] == 1 else matching) for row in rows
        )


def test_read_orc_ignore_missing_paths(
    ray_start_regular_shared, tmp_path, orc_reader_version
):
    existing = os.path.join(tmp_path, "data.orc")
    _write_orc(existing, pa.table({"id": [0, 1]}))
    missing = os.path.join(tmp_path, "does_not_exist.orc")

    ds = ray.data.read_orc([existing, missing], ignore_missing_paths=True)
    assert ds.count() == 2

    with pytest.raises(FileNotFoundError):
        ray.data.read_orc([existing, missing], ignore_missing_paths=False).materialize()


def test_read_orc_file_extensions_filtering(
    ray_start_regular_shared, tmp_path, orc_reader_version
):
    _write_orc(os.path.join(tmp_path, "data.orc"), pa.table({"id": [0, 1]}))
    # A non-ORC file in the same directory should be filtered out by default.
    with open(os.path.join(tmp_path, "_SUCCESS"), "w") as f:
        f.write("")

    ds = ray.data.read_orc(str(tmp_path))
    assert ds.count() == 2

    # A directory with no matching files raises a clear error.
    empty_dir = os.path.join(tmp_path, "empty")
    os.makedirs(empty_dir)
    with open(os.path.join(empty_dir, "_SUCCESS"), "w") as f:
        f.write("")
    with pytest.raises(ValueError):
        ray.data.read_orc(empty_dir)


def test_read_orc_override_num_blocks(
    ray_start_regular_shared, tmp_path, orc_reader_version
):
    path = os.path.join(tmp_path, "data.orc")
    _write_orc(path, pa.table({"id": list(range(100))}))

    ds = ray.data.read_orc(path, override_num_blocks=1)

    assert ds.count() == 100
    assert ds.materialize().num_blocks() == 1


def test_read_orc_partitioned(ray_start_regular_shared, tmp_path, orc_reader_version):
    from ray.data.datasource.partitioning import Partitioning, PartitionStyle

    os.makedirs(os.path.join(tmp_path, "year=2024"))
    _write_orc(
        os.path.join(tmp_path, "year=2024", "data.orc"),
        pa.table({"data": [0, 1]}),
    )

    ds = ray.data.read_orc(
        str(tmp_path), partitioning=Partitioning(PartitionStyle.HIVE)
    )

    rows = ds.take_all()
    assert sorted(row["data"] for row in rows) == [0, 1]
    assert all(row["year"] == "2024" for row in rows)


def test_read_orc_v2_unifies_schema_and_hive_partitions(
    ray_start_regular_shared, tmp_path, monkeypatch
):
    from ray.data.context import DataContext
    from ray.data.datasource.partitioning import Partitioning, PartitionStyle

    monkeypatch.setattr(DataContext.get_current(), "use_datasource_v2", True)

    for year in ("2023", "2024"):
        partition_dir = tmp_path / f"year={year}"
        partition_dir.mkdir()
    _write_orc(str(tmp_path / "year=2023" / "first.orc"), pa.table({"id": [1]}))
    _write_orc(
        str(tmp_path / "year=2024" / "second.orc"),
        pa.table({"id": [2], "name": ["two"]}),
    )

    ds = ray.data.read_orc(
        str(tmp_path),
        partitioning=Partitioning(PartitionStyle.HIVE, base_dir=str(tmp_path)),
        include_paths=True,
        override_num_blocks=1,
    )

    materialized = ds.materialize()
    rows = sorted(materialized.take_all(), key=lambda row: row["id"])
    assert ds.schema().names == ["id", "name", "year", "path"]
    assert rows[0]["id"] == 1
    assert rows[0]["name"] is None
    assert rows[0]["year"] == "2023"
    assert rows[0]["path"].endswith("first.orc")
    assert rows[1]["id"] == 2
    assert rows[1]["name"] == "two"
    assert rows[1]["year"] == "2024"
    assert rows[1]["path"].endswith("second.orc")
    assert materialized.num_blocks() == 1


def test_read_orc_v2_list_files_keeps_caller_source_paths(
    ray_start_regular_shared, tmp_path, monkeypatch
):
    from ray.data._internal.logical.operators import ListFiles
    from ray.data.context import DataContext

    # Lineage tracking reads ``ListFiles.source_paths``. ``paths`` holds the
    # resolved paths, which lose the URI scheme.
    monkeypatch.setattr(DataContext.get_current(), "use_datasource_v2", True)
    _write_orc(str(tmp_path / "data.orc"), pa.table({"id": [1, 2]}))
    uri = f"file://{tmp_path}"

    ds = ray.data.read_orc(uri)

    list_files_op = ds._logical_plan.dag.input_dependencies[0]
    assert isinstance(list_files_op, ListFiles)
    assert list_files_op.paths == [str(tmp_path)]
    assert list_files_op.source_paths == [uri]


def test_read_orc_v1_fallback(ray_start_regular_shared, tmp_path, monkeypatch):
    from ray.data.context import DataContext

    monkeypatch.setattr(DataContext.get_current(), "use_datasource_v2", False)
    path = os.path.join(tmp_path, "data.orc")
    _write_orc(path, pa.table({"id": [1, 2]}))

    ds = ray.data.read_orc(path)

    assert sorted(row["id"] for row in ds.take_all()) == [1, 2]


def test_read_orc_partitioned_with_partition_filter(
    ray_start_regular_shared, tmp_path, orc_reader_version
):
    from ray.data.datasource.partitioning import (
        Partitioning,
        PartitionStyle,
        PathPartitionFilter,
    )

    for year in ("2023", "2024"):
        os.makedirs(os.path.join(tmp_path, f"year={year}"))
        _write_orc(
            os.path.join(tmp_path, f"year={year}", "data.orc"),
            pa.table({"data": [0, 1]}),
        )

    partition_filter = PathPartitionFilter.of(
        filter_fn=lambda partitions: partitions["year"] == "2024",
        style=PartitionStyle.HIVE,
    )
    ds = ray.data.read_orc(
        str(tmp_path),
        partitioning=Partitioning(PartitionStyle.HIVE),
        partition_filter=partition_filter,
    )

    rows = ds.take_all()
    assert len(rows) == 2
    assert all(row["year"] == "2024" for row in rows)


def test_read_orc_multiple_stripes(
    ray_start_regular_shared, tmp_path, orc_reader_version
):

    path = os.path.join(tmp_path, "multi.orc")
    table = pa.table({"id": list(range(10000))})
    with pa.OSFile(path, "wb") as sink:
        orc.write_table(table, sink, stripe_size=64 * 1024)

    ds = ray.data.read_orc(path)

    assert ds.count() == 10000
    assert sorted(row["id"] for row in ds.take_all()) == list(range(10000))


def test_read_orc_empty_file(ray_start_regular_shared, tmp_path, orc_reader_version):
    path = os.path.join(tmp_path, "empty.orc")
    table = pa.table(
        {
            "id": pa.array([], type=pa.int64()),
            "name": pa.array([], type=pa.string()),
        }
    )
    with pa.OSFile(path, "wb") as sink:
        orc.write_table(table, sink)

    ds = ray.data.read_orc(path)

    assert ds.count() == 0
    assert ds.take_all() == []


def test_orc_write(ray_start_regular_shared, tmp_path):
    input_df = pd.DataFrame({"id": [0, 1, 2], "name": ["a", "b", "c"]})
    ds = ray.data.from_blocks([input_df])

    ds.write_orc(tmp_path)

    output_df = _read_orc_dir(tmp_path).to_pandas()
    assert rows_same(input_df, output_df)


@pytest.mark.parametrize("override_num_blocks", [None, 2])
def test_orc_roundtrip(
    ray_start_regular_shared, tmp_path, override_num_blocks, orc_reader_version
):
    df = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})

    ds = ray.data.from_pandas([df], override_num_blocks=override_num_blocks)
    ds.write_orc(tmp_path)

    ds2 = ray.data.read_orc(str(tmp_path))
    ds2df = ds2.to_pandas()
    assert rows_same(ds2df, df)
    for entry in ds2._execute().blocks:
        assert (
            # pyrefly: ignore[no-matching-overload]
            BlockAccessor.for_block(ray.get(entry.ref)).size_bytes()
            == entry.metadata.size_bytes
        )


def test_orc_write_rejects_stream_compression(tmp_path):
    with pytest.raises(
        ValueError,
        match=(
            "Pass the compression parameter straight to write_orc "
            "instead of via open_stream_args"
        ),
    ):
        ORCDatasink(str(tmp_path), open_stream_args={"compression": "gzip"})


def test_orc_write_rejects_zero_user_columns(tmp_path):
    block = BlockAccessor.for_block(
        pa.table({_BATCH_SIZE_PRESERVING_STUB_COL_NAME: pa.nulls(3)})
    )
    datasink = ORCDatasink(str(tmp_path))

    with pa.OSFile(os.path.join(tmp_path, "data.orc"), "wb") as file:
        with pytest.raises(ValueError, match="at least one column"):
            datasink.write_block_to_file(block, file)


def test_orc_write_strips_internal_columns(tmp_path):
    block = BlockAccessor.for_block(
        pa.table(
            {
                _BATCH_SIZE_PRESERVING_STUB_COL_NAME: pa.nulls(3),
                "id": [1, 2, 3],
            }
        )
    )
    output_path = os.path.join(tmp_path, "data.orc")
    datasink = ORCDatasink(str(tmp_path))

    with pa.OSFile(output_path, "wb") as file:
        datasink.write_block_to_file(block, file)

    output = orc.read_table(output_path)
    assert output.schema.names == ["id"]
    assert output.column("id").to_pylist() == [1, 2, 3]


def test_orc_write_compression(ray_start_regular_shared, tmp_path):
    input_df = pd.DataFrame({"id": [0, 1, 2]})
    ds = ray.data.from_blocks([input_df])

    ds.write_orc(tmp_path, compression="zstd")

    filenames = _list_visible_files(tmp_path)
    assert len(filenames) == 1
    output_file = os.path.join(tmp_path, filenames[0])
    assert orc.ORCFile(output_file).compression == "ZSTD"
    output_df = orc.read_table(output_file).to_pandas()
    assert rows_same(input_df, output_df)


def test_orc_write_args_fn_overrides_args(ray_start_regular_shared, tmp_path):
    ds = ray.data.range(3)

    ds.write_orc(
        tmp_path,
        arrow_orc_args_fn=lambda: {"compression": "zstd"},
        compression="uncompressed",
    )

    filenames = _list_visible_files(tmp_path)
    assert filenames
    assert all(
        orc.ORCFile(os.path.join(tmp_path, filename)).compression == "ZSTD"
        for filename in filenames
    )


def test_orc_write_empty(ray_start_regular_shared, tmp_path):
    df = pd.DataFrame({"id": pd.Series([], dtype="int64")})
    ds = ray.data.from_pandas(df)

    ds.write_orc(tmp_path)

    assert _list_visible_files(tmp_path) == []


@pytest.mark.parametrize("min_rows_per_file", [5, 10, 50])
def test_orc_write_min_rows_per_file(
    tmp_path, ray_start_regular_shared, min_rows_per_file
):
    ray.data.range(100, override_num_blocks=20).write_orc(
        tmp_path, min_rows_per_file=min_rows_per_file
    )

    filenames = _list_visible_files(tmp_path)
    assert len(filenames) == 100 // min_rows_per_file
    for filename in filenames:
        num_rows_written = orc.read_table(os.path.join(tmp_path, filename)).num_rows
        assert num_rows_written == min_rows_per_file


@pytest.mark.parametrize("min_rows_per_file", [0, -1])
def test_orc_write_rejects_non_positive_min_rows_per_file(
    ray_start_regular_shared, tmp_path, min_rows_per_file
):
    with pytest.raises(
        ValueError, match="min_rows_per_file must be a positive integer"
    ):
        ray.data.range(1).write_orc(tmp_path, min_rows_per_file=min_rows_per_file)


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("project", [False, True])
def test_read_orc_mixed_partition_projection(
    ray_start_regular_shared, tmp_path, orc_reader_version, reverse, project
):
    from ray.data.datasource.partitioning import Partitioning, PartitionStyle

    root = tmp_path / "root.orc"
    partition_dir = tmp_path / "year=2024"
    partition_dir.mkdir()
    partition = partition_dir / "data.orc"
    _write_orc(str(root), pa.table({"id": [1]}))
    _write_orc(str(partition), pa.table({"id": [3]}))
    paths = [str(partition), str(root)] if reverse else str(tmp_path)
    ds = ray.data.read_orc(
        paths, partitioning=Partitioning(PartitionStyle.HIVE), override_num_blocks=1
    )
    if project:
        ds = ds.select_columns(["year", "id"])
    assert sorted(ds.take_all(), key=lambda row: row["id"]) == [
        {"id": 1, "year": None},
        {"id": 3, "year": "2024"},
    ]


@pytest.mark.parametrize("field_names", [None, ["year"]])
def test_read_orc_partition_conflict_follows_reader_version(
    ray_start_regular_shared, tmp_path, orc_reader_version, field_names
):
    from ray.data.datasource.partitioning import Partitioning, PartitionStyle
    from ray.data.expressions import col
    from ray.exceptions import RayTaskError

    root = tmp_path / "root.orc"
    partition_dir = tmp_path / "year=2024"
    partition_dir.mkdir()
    partition = partition_dir / "data.orc"
    _write_orc(str(root), pa.table({"id": [1], "year": ["from-file"]}))
    _write_orc(str(partition), pa.table({"id": [2], "year": ["from-file"]}))
    ds = ray.data.read_orc(
        [str(root), str(partition)],
        partitioning=Partitioning(PartitionStyle.HIVE, field_names=field_names),
        override_num_blocks=1,
    )
    expected_exceptions: tuple[type[Exception], ...] = (
        ValueError,
        RayTaskError,
    )
    filtered = ds.filter(expr=col("year") == "from-file").select_columns(["id"])
    if not orc_reader_version:
        with pytest.raises(expected_exceptions, match="Partition column year"):
            filtered.take_all()
    else:
        # Declaring partition keys must not change a root file's stored value.
        assert filtered.take_all() == [{"id": 1}]


@pytest.mark.parametrize("field_names", [None, ["year"]])
@pytest.mark.parametrize("root_has_year", [False, True])
@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize(
    "predicate,with_root_year,without_root_year",
    [
        (col("year") == "2024", [3], [3]),
        (col("year") == "from-file", [1], []),
        (col("year").is_null(), [], [1]),
        (~(col("year") == "2024"), [1], []),
        ((col("year") == "2024") | (col("id") == 1), [1, 3], [1, 3]),
        ((col("year").is_null()) & (col("id") > 0), [], [1]),
    ],
)
def test_read_orc_v2_partition_filters_preserve_root_rows(
    ray_start_regular_shared,
    tmp_path,
    monkeypatch,
    field_names,
    root_has_year,
    reverse,
    predicate,
    with_root_year,
    without_root_year,
):
    from ray.data.context import DataContext
    from ray.data.datasource.partitioning import Partitioning, PartitionStyle

    monkeypatch.setattr(DataContext.get_current(), "use_datasource_v2", True)
    root = tmp_path / "root.orc"
    directory = tmp_path / "year=2024"
    directory.mkdir()
    partitioned = directory / "data.orc"
    data: dict[str, list[int] | list[str]] = {"id": [1]}
    if root_has_year:
        data["year"] = ["from-file"]
    _write_orc(str(root), pa.table(data))
    _write_orc(str(partitioned), pa.table({"id": [3]}))
    paths = [str(partitioned), str(root)] if reverse else [str(root), str(partitioned)]
    ds = ray.data.read_orc(
        paths,
        partitioning=Partitioning(PartitionStyle.HIVE, field_names=field_names),
        override_num_blocks=1,
    )
    rows = ds.filter(expr=predicate).select_columns(["id"]).limit(2).take_all()
    expected = with_root_year if root_has_year else without_root_year
    assert sorted(row["id"] for row in rows) == expected


@pytest.mark.parametrize("operation", ["projection", "partition_filter", "data_filter"])
@pytest.mark.parametrize("stored_partition", [False, True])
def test_read_orc_v2_partitioned_scan_optimizations(
    ray_start_regular_shared, tmp_path, monkeypatch, operation, stored_partition
):
    from ray.data._internal.datasource_v2.formats.orc.orc_datasource_v2 import (
        OrcDatasourceV2,
    )
    from ray.data._internal.datasource_v2.formats.orc.orc_scanner import OrcScanner
    from ray.data.context import DataContext
    from ray.data.datasource.partitioning import Partitioning, PartitionStyle
    from ray.data.expressions import col

    monkeypatch.setattr(DataContext.get_current(), "use_datasource_v2", True)
    trace_path = str(tmp_path / "scan-requests.jsonl")
    original_create_scanner = OrcDatasourceV2.create_scanner

    class TracingScanner(OrcScanner):
        def create_reader(self):
            import fsspec
            from pyarrow.fs import FSSpecHandler, PyFileSystem

            reader = super().create_reader()
            original_iter = reader._iter_fragment_tables
            original_read = reader.read

            def record(kind, **kwargs):
                with open(trace_path, "a") as trace:
                    trace.write(json.dumps({"kind": kind, **kwargs}) + "\n")

            class TrackingHandler(FSSpecHandler):
                def open_input_file(self, path):
                    record("open", path=path)
                    return super().open_input_file(path)

                def open_input_stream(self, path):
                    record("open", path=path)
                    return super().open_input_stream(path)

            reader._filesystem = PyFileSystem(
                TrackingHandler(fsspec.filesystem("file"))
            )

            class Fragment:
                def __init__(self, fragment):
                    self._fragment = fragment

                def __getattr__(self, name):
                    return getattr(self._fragment, name)

                def scanner(self, **kwargs):
                    import inspect

                    from ray.data._internal.datasource_v2.formats.orc.orc_file_reader import (
                        OrcFileReader,
                    )

                    record(
                        "scan",
                        path=self._fragment.path,
                        columns=kwargs["columns"],
                        filter=str(kwargs["filter"]),
                        source=inspect.getfile(OrcFileReader),
                    )
                    return self._fragment.scanner(**kwargs)

            def traced_iter(fragment, scanner_kwargs):
                yield from original_iter(Fragment(fragment), scanner_kwargs)

            def traced_read(input_split):
                record("manifest", paths=input_split.paths.tolist())
                yield from original_read(input_split)

            reader._iter_fragment_tables = traced_iter
            reader.read = traced_read
            return reader

    def create_scanner(datasource, schema, filesystem=None, **options):
        scanner = original_create_scanner(datasource, schema, filesystem, **options)
        return TracingScanner(
            **{field.name: getattr(scanner, field.name) for field in fields(scanner)}
        )

    monkeypatch.setattr(OrcDatasourceV2, "create_scanner", create_scanner)
    for year, ids in [("2023", [1, 2]), ("2024", [3, 4])]:
        directory = tmp_path / f"year={year}"
        directory.mkdir()
        data = {"id": ids, "payload": ["wide" * 1024] * 2}
        if stored_partition:
            data["year"] = ["wrong"] * 2
        _write_orc(str(directory / "data.orc"), pa.table(data))
    ds = ray.data.read_orc(
        str(tmp_path),
        partitioning=Partitioning(PartitionStyle.HIVE),
        override_num_blocks=2,
    )
    if operation == "partition_filter":
        ds = ds.filter(expr=col("year") == "2024")
    elif operation == "data_filter":
        ds = ds.filter(expr=col("id") > 2)
    ds = ds.select_columns(["id"])
    assert sorted(row["id"] for row in ds.take_all()) == (
        [1, 2, 3, 4] if operation == "projection" else [3, 4]
    )
    with open(trace_path) as trace:
        events = [json.loads(line) for line in trace]
    requests = [event for event in events if event["kind"] == "scan"]
    assert len(requests) == (1 if operation == "partition_filter" else 2)
    assert all(request["columns"] == ["id"] for request in requests)
    assert all(
        request["source"].endswith(
            "python/ray/data/_internal/datasource_v2/formats/orc/orc_file_reader.py"
        )
        for request in requests
    )
    if operation == "partition_filter":
        manifests = [event for event in events if event["kind"] == "manifest"]
        opened = [event for event in events if event["kind"] == "open"]
        assert manifests and opened
        assert {path for event in manifests for path in event["paths"]} == {
            str(tmp_path / "year=2024" / "data.orc")
        }
        assert all("year=2024" in event["path"] for event in opened + requests)
    assert all(
        (request["filter"] != "None") == (operation == "data_filter")
        for request in requests
    )


@pytest.mark.parametrize("stored_partition", [False, True])
@pytest.mark.parametrize(
    "predicate,expected_ids",
    [
        ((col("year") == "2024") & (col("id") > 2), [3, 4]),
        ((col("year") == "2024") | (col("id") == 1), [1, 3, 4]),
        (~(col("year") == "2024"), [1, 2]),
        ("id > 2", [3, 4]),
        ("id == 2", [2]),
    ],
)
def test_read_orc_v2_partitioned_predicates_and_limit(
    ray_start_regular_shared,
    tmp_path,
    monkeypatch,
    stored_partition,
    predicate,
    expected_ids,
):
    from ray.data.context import DataContext
    from ray.data.datasource.partitioning import Partitioning, PartitionStyle

    monkeypatch.setattr(DataContext.get_current(), "use_datasource_v2", True)
    for year, ids in [("2023", [1, 2]), ("2024", [3, 4])]:
        directory = tmp_path / f"year={year}"
        directory.mkdir()
        data: dict[str, list[int] | list[str] | pa.Array] = {
            "id": ids,
            "payload": ["unused"] * 2,
        }
        if stored_partition:
            data["year"] = pa.array([year, None], type=pa.string())
        _write_orc(str(directory / "data.orc"), pa.table(data))
    ds = ray.data.read_orc(
        str(tmp_path),
        partitioning=Partitioning(PartitionStyle.HIVE),
        override_num_blocks=2,
    ).filter(expr=predicate)
    assert (
        sorted(row["id"] for row in ds.select_columns(["id"]).take_all())
        == expected_ids
    )
    limited = ds.select_columns(["year"]).limit(1).take_all()
    assert len(limited) == 1
    assert limited[0]["year"] in {
        "2023" if value < 3 else "2024" for value in expected_ids
    }


@pytest.mark.parametrize(
    "predicate,expected",
    [
        ("id > 100", []),
        ("year == '2025'", []),
        ("year == '2024'", [{"id": 1}]),
        ("year == 'wrong' or id < 0", []),
    ],
)
def test_read_orc_v2_filters_use_path_partition_values(
    ray_start_regular_shared, tmp_path, monkeypatch, predicate, expected
):
    from ray.data.context import DataContext
    from ray.data.datasource.partitioning import Partitioning, PartitionStyle

    monkeypatch.setattr(DataContext.get_current(), "use_datasource_v2", True)
    directory = tmp_path / "year=2024"
    directory.mkdir()
    _write_orc(str(directory / "data.orc"), pa.table({"id": [1], "year": ["wrong"]}))
    ds = (
        ray.data.read_orc(
            str(tmp_path),
            partitioning=Partitioning(PartitionStyle.HIVE),
            override_num_blocks=1,
        )
        .filter(expr=predicate)
        .select_columns(["id"])
    )
    assert ds.take_all() == expected


@pytest.mark.parametrize("file_year", ["wrong", None, "2024"])
@pytest.mark.parametrize("operation", ["full", "project", "filter", "filter_only"])
def test_read_orc_v2_partition_values_match_parquet(
    ray_start_regular_shared, tmp_path, monkeypatch, file_year, operation
):
    import pyarrow.parquet as pq

    from ray.data.context import DataContext
    from ray.data.datasource.partitioning import Partitioning, PartitionStyle

    monkeypatch.setattr(DataContext.get_current(), "use_datasource_v2", True)
    directory = tmp_path / "year=2024"
    directory.mkdir()
    table = pa.table(
        {"id": [1, 2], "year": pa.array([file_year, None], type=pa.string())}
    )
    _write_orc(str(directory / "data.orc"), table)
    pq.write_table(table, directory / "data.parquet")
    rows_by_format = []
    for read in [ray.data.read_orc, ray.data.read_parquet]:
        ds = read(
            str(tmp_path),
            partitioning=Partitioning(PartitionStyle.HIVE),
            override_num_blocks=1,
        )
        if operation in {"filter", "filter_only"}:
            ds = ds.filter(expr=(col("year") == "2024") & (col("id") > 1))
        if operation in {"project", "filter"}:
            ds = ds.select_columns(["year", "id"])
        elif operation == "filter_only":
            ds = ds.select_columns(["id"])
        rows_by_format.append(ds.take_all())
    assert rows_by_format[0] == rows_by_format[1]
    assert rows_by_format[0] == (
        [{"id": 2}]
        if operation == "filter_only"
        else [
            {"id": value, "year": "2024"}
            for value in ([2] if operation == "filter" else [1, 2])
        ]
    )


@pytest.mark.parametrize("operation", ["full", "project", "filter", "filter_only"])
@pytest.mark.parametrize(
    "physical_type,logical_type,path_value,stored_value,root_value,expected",
    [
        pytest.param(
            pa.float32(),
            pa.float64(),
            "0.1",
            0.1,
            0.2,
            0.1,
            id="inferred-float-widening",
        ),
        pytest.param(
            pa.int32(),
            pa.int64(),
            "2147483647",
            2147483647,
            2147483648,
            2147483647,
            id="inferred-integer-widening",
        ),
        pytest.param(
            pa.decimal128(6, 2),
            pa.decimal128(10, 4),
            "1.2300",
            Decimal("1.23"),
            Decimal("2.3400"),
            Decimal("1.2300"),
            id="inferred-decimal-scale",
        ),
        pytest.param(
            pa.bool_(),
            pa.bool_(),
            "true",
            True,
            False,
            True,
            id="bool",
        ),
    ],
)
def test_read_orc_v2_stored_partition_logical_types(
    ray_start_regular_shared,
    tmp_path,
    monkeypatch,
    operation,
    physical_type,
    logical_type,
    path_value,
    stored_value,
    root_value,
    expected,
):
    from ray.data.context import DataContext
    from ray.data.datasource.partitioning import Partitioning, PartitionStyle

    monkeypatch.setattr(DataContext.get_current(), "use_datasource_v2", True)
    root = tmp_path / "root.orc"
    directory = tmp_path / f"key={path_value}"
    directory.mkdir()
    partition = directory / "data.orc"
    _write_orc(
        str(root),
        pa.table({"id": [0], "key": pa.array([root_value], type=logical_type)}),
    )
    _write_orc(
        str(partition),
        pa.table(
            {
                "id": [1, 2, 3],
                "key": pa.array([stored_value, None, stored_value], type=physical_type),
            }
        ),
    )
    # Sampling the root first leaves field_names unresolved. Projection must
    # still return the same logical path values as an unprojected read.
    ds = ray.data.read_orc(
        [str(root), str(partition)],
        partitioning=Partitioning(PartitionStyle.HIVE),
        override_num_blocks=1,
    )
    assert ds.schema().base_schema.field("key").type == logical_type
    if operation in {"filter", "filter_only"}:
        ds = ds.filter(expr=(col("key") == expected) | (col("id") < 0)).limit(2)
    if operation == "project":
        ds = ds.select_columns(["key", "id"])
    elif operation == "filter_only":
        ds = ds.select_columns(["id"])
    rows = sorted(ds.take_all(), key=lambda row: row["id"])
    if operation == "filter_only":
        assert rows == [{"id": 1}, {"id": 2}]
    elif operation == "filter":
        assert rows == [{"id": 1, "key": expected}, {"id": 2, "key": expected}]
    else:
        assert rows == [{"id": 0, "key": root_value}] + [
            {"id": value, "key": expected} for value in [1, 2, 3]
        ]


def _trace_orc_batch_sizing(monkeypatch, trace_path):
    from ray.data._internal.datasource_v2.formats.orc.orc_datasource_v2 import (
        OrcDatasourceV2,
    )
    from ray.data._internal.datasource_v2.formats.orc.orc_scanner import OrcScanner

    original_create_scanner = OrcDatasourceV2.create_scanner

    class TracingScanner(OrcScanner):
        def create_reader(self):
            import uuid

            reader = super().create_reader()
            reader_id = uuid.uuid4().hex
            read_count = 0
            original_read = reader.read
            original_iter = reader._iter_fragment_tables

            def traced_read(input_split):
                nonlocal read_count
                read_count += 1
                yield from original_read(input_split)

            def traced_iter(fragment, scanner_kwargs):
                with open(trace_path, "a") as trace:
                    trace.write(
                        json.dumps(
                            {
                                "event": "scan",
                                "reader": reader_id,
                                "read_count": read_count,
                                "path": fragment.path,
                                "target": reader._target_block_size,
                                "batch_size": scanner_kwargs["batch_size"],
                            }
                        )
                        + "\n"
                    )
                for table in original_iter(fragment, scanner_kwargs):
                    with open(trace_path, "a") as trace:
                        trace.write(
                            json.dumps(
                                {
                                    "event": "batch",
                                    "rows": table.num_rows,
                                    "nbytes": table.nbytes,
                                }
                            )
                            + "\n"
                        )
                    yield table

            reader.read = traced_read
            reader._iter_fragment_tables = traced_iter
            return reader

    def create_scanner(datasource, schema, filesystem=None, **options):
        scanner = original_create_scanner(datasource, schema, filesystem, **options)
        return TracingScanner(
            **{field.name: getattr(scanner, field.name) for field in fields(scanner)}
        )

    monkeypatch.setattr(OrcDatasourceV2, "create_scanner", create_scanner)


@pytest.mark.parametrize("file_count", [1, 2])
def test_read_orc_v2_batch_sizing_fallback(
    ray_start_regular_shared, tmp_path, monkeypatch, file_count
):
    from ray.data._internal.datasource_v2.common.file_reader import (
        _ARROW_DEFAULT_BATCH_SIZE,
    )
    from ray.data.context import DataContext

    monkeypatch.setattr(DataContext.get_current(), "use_datasource_v2", True)
    monkeypatch.setattr(DataContext.get_current(), "target_max_block_size", 128)
    trace_path = str(tmp_path / "batch-sizing.jsonl")
    _trace_orc_batch_sizing(monkeypatch, trace_path)
    for index in range(file_count):
        _write_orc(
            str(tmp_path / f"part-{index}.orc"),
            pa.table({"id": [index], "payload": [[None, "x" * 512]]}),
        )
    rows = ray.data.read_orc(str(tmp_path), override_num_blocks=file_count).take_all()
    assert sorted(rows, key=lambda row: row["id"]) == [
        {"id": index, "payload": [None, "x" * 512]} for index in range(file_count)
    ]
    with open(trace_path) as trace:
        records = [json.loads(line) for line in trace]
    requests = [record for record in records if record["event"] == "scan"]
    assert len(requests) == file_count
    assert all(request["read_count"] == 1 for request in requests)
    assert all(request["target"] == 128 for request in requests)
    assert all(
        request["batch_size"] == _ARROW_DEFAULT_BATCH_SIZE for request in requests
    )


@pytest.mark.parametrize("target", [65536, 131072])
@pytest.mark.parametrize("project_narrow", [False, True])
def test_read_orc_v2_initial_batch_sizing(
    ray_start_regular_shared, tmp_path, monkeypatch, target, project_narrow
):
    from ray.data.context import DataContext

    monkeypatch.setattr(DataContext.get_current(), "use_datasource_v2", True)
    monkeypatch.setattr(DataContext.get_current(), "target_max_block_size", target)
    trace_path = str(tmp_path / "initial-batch-sizing.jsonl")
    _trace_orc_batch_sizing(monkeypatch, trace_path)
    table = pa.table({"id": range(256), "payload": ["x" * 4096] * 256})
    path = str(tmp_path / "data.orc")
    _write_orc(path, table)
    dataset = ray.data.read_orc(path, override_num_blocks=1)
    if project_narrow:
        dataset = dataset.select_columns(["id"])
    rows = sorted(dataset.take_all(), key=lambda row: row["id"])
    assert rows == (table.select(["id"]) if project_narrow else table).to_pylist()
    with open(trace_path) as trace:
        records = [json.loads(line) for line in trace]
    scans = [record for record in records if record["event"] == "scan"]
    batches = [record for record in records if record["event"] == "batch"]
    expected_size = 256 if project_narrow else target // 4096
    assert len(scans) == 1
    assert scans[0]["read_count"] == 1
    assert scans[0]["target"] == target
    assert scans[0]["batch_size"] == expected_size
    assert batches[0]["rows"] == expected_size
    assert sum(batch["rows"] for batch in batches) == 256
    assert all(batch["rows"] <= expected_size for batch in batches)
    if not project_narrow:
        assert batches[0]["nbytes"] < table.nbytes


def test_read_orc_v1_fallback_preserves_columns_outside_v2_sample(
    ray_start_regular_shared, tmp_path, monkeypatch
):
    from ray.data.context import DataContext

    # V1 preserves columns that are absent from the V2 schema sample.
    monkeypatch.setattr(DataContext.get_current(), "use_datasource_v2", False)
    for index in range(20):
        table: dict[str, list[int] | list[str]] = {"id": [index]}
        if index == 19:
            table["extra"] = ["late"]
        _write_orc(str(tmp_path / f"part-{index:02d}.orc"), pa.table(table))
    rows = sorted(
        ray.data.read_orc(str(tmp_path), override_num_blocks=1).take_all(),
        key=lambda row: row["id"],
    )
    assert len(rows) == 20
    assert rows[-1] == {"id": 19, "extra": "late"}


@pytest.fixture
def full_schema_context(ray_start_regular_shared, monkeypatch):
    from ray.data.context import DataContext

    monkeypatch.setattr(DataContext.get_current(), "use_datasource_v2", True)


@pytest.mark.parametrize("file_count", [1, 3, 17])
def test_read_orc_merge_schema_discovers_and_reads_all_columns(
    tmp_path, full_schema_context, file_count
):
    for i in range(file_count):
        table = (
            pa.table({"id": [i], "late": ["value"]})
            if i == file_count - 1
            else pa.table({"id": [i]})
        )
        _write_orc(str(tmp_path / f"f{i:02d}.orc"), table)

    bounded = ray.data.read_orc(str(tmp_path))
    merged = ray.data.read_orc(str(tmp_path), merge_schema=True, include_paths=True)

    assert ("late" in bounded.schema().names) is (file_count <= 16)
    assert merged.schema().names == ["id", "late", "path"]
    rows = sorted(merged.take_all(), key=lambda row: row["id"])
    assert len(rows) == file_count
    assert all(row["late"] is None for row in rows[:-1])
    assert rows[-1]["late"] == "value"
    assert all(row["path"].endswith(f"f{row['id']:02d}.orc") for row in rows)
    assert merged.filter(expr=col("late") == "value").select_columns(
        ["id", "late"]
    ).take_all() == [{"id": file_count - 1, "late": "value"}]


def test_read_orc_merge_schema_fails_on_conflict_outside_sample(
    tmp_path, full_schema_context
):
    for i in range(17):
        values = ["incompatible"] if i == 16 else [i]
        _write_orc(str(tmp_path / f"f{i:02d}.orc"), pa.table({"id": values}))

    assert ray.data.read_orc(str(tmp_path)).schema().names == ["id"]
    with pytest.raises((pa.ArrowTypeError, pa.ArrowInvalid), match="f16.orc"):
        ray.data.read_orc(str(tmp_path), merge_schema=True)


def test_read_orc_merge_schema_reuses_candidates_across_actions_and_lineage(
    tmp_path, full_schema_context, monkeypatch
):
    from ray.data._internal.datasource_v2.common import listing_utils
    from ray.data._internal.datasource_v2.common.non_sampling_file_indexer import (
        NonSamplingFileIndexer,
    )
    from ray.data._internal.datasource_v2.formats.orc.orc_datasource_v2 import (
        OrcDatasourceV2,
    )
    from ray.data._internal.logical.operators import ListFiles
    from ray.data.datasource.file_based_datasource import FileShuffleConfig

    class NoRelistingIndexer(NonSamplingFileIndexer):
        listed = False

        def list_file_infos(self, *args, **kwargs):
            assert not self.listed, "Execution must reuse planning-time candidates"
            self.listed = True
            yield from super().list_file_infos(*args, **kwargs)

    monkeypatch.setattr(listing_utils, "DEFAULT_SCHEMA_FILE_INFO_BLOCK_SIZE", 1)
    monkeypatch.setattr(
        OrcDatasourceV2,
        "_get_file_indexer",
        lambda _: NoRelistingIndexer(ignore_missing_paths=False),
    )
    for i in range(4):
        _write_orc(str(tmp_path / f"f{i}.orc"), pa.table({"id": [i]}))
    ds = ray.data.read_orc(
        str(tmp_path), merge_schema=True, shuffle=FileShuffleConfig(seed=13)
    )
    listing = ds._logical_plan.dag.input_dependencies[0]
    assert isinstance(listing, ListFiles)
    assert listing.prelisted_file_infos is not None
    assert len(listing.prelisted_file_infos) == 4
    assert all(
        block.column_names == ["__path", "__file_size"]
        for block in listing.prelisted_file_infos
    )
    restored = ray.data.Dataset.deserialize_lineage(ds.serialize_lineage())
    _write_orc(str(tmp_path / "new.orc"), pa.table({"id": [99], "new": ["excluded"]}))

    assert ds.schema().names == ["id"]
    assert sorted(row["id"] for row in ds.take_all()) == [0, 1, 2, 3]
    assert ds.count() == 4
    assert sorted(row["id"] for row in ds.take_all()) == [0, 1, 2, 3]
    assert sorted(row["id"] for row in restored.take_all()) == [0, 1, 2, 3]


def test_read_orc_merge_schema_applies_initial_filters(tmp_path, full_schema_context):
    from ray.data.datasource.partitioning import PathPartitionFilter

    keep = tmp_path / "group=keep"
    skip = tmp_path / "group=skip"
    keep.mkdir()
    skip.mkdir()
    _write_orc(str(keep / "data.orc"), pa.table({"id": [1]}))
    _write_orc(str(skip / "data.orc"), pa.table({"id": [2], "excluded": [3]}))
    _write_orc(str(keep / "ignored.other"), pa.table({"excluded": [4]}))
    _write_orc(str(tmp_path / ".hidden.orc"), pa.table({"excluded": [5]}))
    (keep / "empty.orc").touch()
    partition_filter = PathPartitionFilter.of(
        lambda parts: parts["group"] == "keep", base_dir=str(tmp_path)
    )
    ds = ray.data.read_orc(
        [str(tmp_path), str(tmp_path / "missing.orc")],
        merge_schema=True,
        partition_filter=partition_filter,
        ignore_missing_paths=True,
    )

    assert ds.schema().names == ["id"]
    assert ds.take_all() == [{"id": 1}]


def test_read_orc_merge_schema_partition_filter_and_count(
    tmp_path, full_schema_context
):
    from ray.data.datasource.partitioning import Partitioning, PartitionStyle

    for year, count in (("2024", 2), ("2025", 3)):
        directory = tmp_path / f"year={year}"
        directory.mkdir()
        _write_orc(str(directory / "data.orc"), pa.table({"id": list(range(count))}))
    ds = ray.data.read_orc(
        str(tmp_path),
        merge_schema=True,
        partitioning=Partitioning(PartitionStyle.HIVE, base_dir=str(tmp_path)),
    )

    assert ds.count() == 5
    assert ds.filter(expr=col("year") == "2025").count() == 3


def test_read_orc_merge_schema_warns_for_large_candidate_set(
    tmp_path, full_schema_context, monkeypatch, caplog
):
    import logging

    import ray.data.read_api as read_api

    for i in range(2):
        _write_orc(str(tmp_path / f"f{i}.orc"), pa.table({"id": [i]}))
    monkeypatch.setattr(read_api, "_FULL_SCHEMA_INFERENCE_WARNING_FILE_COUNT", 1)
    monkeypatch.setattr(read_api.logger, "handlers", [caplog.handler])
    monkeypatch.setattr(read_api.logger, "propagate", False)

    with caplog.at_level(logging.WARNING, logger="ray.data.read_api"):
        ray.data.read_orc(str(tmp_path), merge_schema=True)

    messages = [
        record.getMessage()
        for record in caplog.records
        if "candidate files" in record.getMessage()
    ]
    assert len(messages) == 1
    assert "2 candidate files" in messages[0]
    assert "read_parquet" not in messages[0]


def test_read_orc_merge_schema_rejects_empty_candidates(tmp_path, full_schema_context):
    _write_orc(str(tmp_path / "ignored.other"), pa.table({"id": [1]}))
    with pytest.raises(ValueError, match="no files found"):
        ray.data.read_orc(str(tmp_path), merge_schema=True)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
