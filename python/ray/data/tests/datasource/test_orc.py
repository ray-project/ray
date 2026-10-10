import json
import os
from dataclasses import fields
from decimal import Decimal

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
        # With unresolved keys the root's physical value remains a data value;
        # with explicit keys the shared path pruner excludes unpartitioned files.
        assert filtered.take_all() == ([{"id": 1}] if field_names is None else [])


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

            def traced_read(manifest):
                record("manifest", paths=manifest.paths.tolist())
                yield from original_read(manifest)

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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
