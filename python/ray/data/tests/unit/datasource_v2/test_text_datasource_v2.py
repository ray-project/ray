"""Unit tests for the plain-text DataSourceV2: reader, scanner and datasource."""

import bz2
import gzip
from pathlib import Path
from unittest.mock import Mock

import pyarrow as pa
import pytest
from pyarrow.fs import LocalFileSystem

import ray.data.read_api as read_api
from ray.data._internal.arrow_block import _BATCH_SIZE_PRESERVING_STUB_COL_NAME
from ray.data._internal.datasource_v2.common.non_sampling_file_indexer import (
    NonSamplingFileIndexer,
)
from ray.data._internal.datasource_v2.common.round_robin_partitioner import (
    RoundRobinPartitioner,
)
from ray.data._internal.datasource_v2.common.size_estimators import (
    SamplingInMemorySizeEstimator,
)
from ray.data._internal.datasource_v2.common.synthesized_columns import PathColumn
from ray.data._internal.datasource_v2.formats.text import text_reader as text_reader_mod
from ray.data._internal.datasource_v2.formats.text.text_datasource_v2 import (
    TextDatasourceV2,
)
from ray.data._internal.datasource_v2.formats.text.text_reader import (
    TEXT_COLUMN_NAME,
    TextReader,
)
from ray.data._internal.datasource_v2.formats.text.text_scanner import TextScanner
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.datasource_v2.interfaces.file_partitioner import PartitionHints
from ray.data._internal.util import RetryingPyFileSystem
from ray.data.context import DataContext
from ray.data.datasource.partitioning import Partitioning, PartitionStyle
from ray.data.expressions import col

TEXT_SCHEMA = pa.schema([(TEXT_COLUMN_NAME, pa.string())])


def _write(path: Path, content: str, encoding: str = "utf-8") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content.encode(encoding))
    return path


def _manifest(*paths: Path) -> FileManifest:
    return FileManifest.construct_manifest(
        paths=[str(path) for path in paths],
        sizes=[path.stat().st_size for path in paths],
        chunk_metadatas=[None] * len(paths),
    )


def _read(reader: TextReader, *paths: Path) -> pa.Table:
    tables = list(reader.read(_manifest(*paths)))
    if not tables:
        return TEXT_SCHEMA.empty_table()
    return pa.concat_tables(tables)


def _lines(reader: TextReader, *paths: Path) -> list:
    return _read(reader, *paths).column(TEXT_COLUMN_NAME).to_pylist()


# -- reader --------------------------------------------------------------------


def test_reader_yields_one_table_per_file_in_manifest_order(tmp_path):
    first = _write(tmp_path / "a.txt", "hello\nworld\n")
    second = _write(tmp_path / "b.txt", "goodbye")

    tables = list(TextReader().read(_manifest(first, second)))

    assert [table.column(TEXT_COLUMN_NAME).to_pylist() for table in tables] == [
        ["hello", "world"],
        ["goodbye"],
    ]
    assert all(table.schema == TEXT_SCHEMA for table in tables)


@pytest.mark.parametrize(
    "content, expected",
    [
        ("a\nb", ["a", "b"]),
        ("a\nb\n", ["a", "b"]),
        ("a\r\nb\r\n", ["a", "b"]),
        ("a\rb", ["a", "b"]),
        ("\n", [""]),
        ("", []),
    ],
    ids=["no_trailing_newline", "trailing_newline", "crlf", "cr", "one_blank", "empty"],
)
def test_reader_splits_lines_like_v1(tmp_path, content, expected):
    path = _write(tmp_path / "data.txt", content)
    assert _lines(TextReader(drop_empty_lines=False), path) == expected


@pytest.mark.parametrize("drop_empty_lines", [True, False])
def test_reader_drop_empty_lines_also_drops_whitespace_only(tmp_path, drop_empty_lines):
    path = _write(tmp_path / "data.txt", "a\n\n  \t\nb\n")

    lines = _lines(TextReader(drop_empty_lines=drop_empty_lines), path)

    assert lines == (["a", "b"] if drop_empty_lines else ["a", "", "  \t", "b"])


def test_reader_decodes_with_the_requested_encoding(tmp_path):
    path = _write(tmp_path / "data.txt", "café\n", encoding="latin-1")

    assert _lines(TextReader(encoding="latin-1"), path) == ["café"]
    with pytest.raises(UnicodeDecodeError):
        _lines(TextReader(), path)


def test_reader_empty_file_yields_no_tables(tmp_path):
    path = _write(tmp_path / "empty.txt", "")
    assert list(TextReader().read(_manifest(path))) == []


def test_reader_empty_manifest_yields_nothing():
    manifest = FileManifest.construct_manifest(paths=[], sizes=[], chunk_metadatas=[])
    assert list(TextReader().read(manifest)) == []


def test_reader_reads_duplicated_path_once_per_row(tmp_path):
    path = _write(tmp_path / "data.txt", "x\ny\n")
    assert _lines(TextReader(), path, path) == ["x", "y", "x", "y"]


def test_reader_limit_stops_within_and_across_files(tmp_path):
    first = _write(tmp_path / "a.txt", "1\n2\n3\n")
    second = _write(tmp_path / "b.txt", "4\n5\n")

    assert _lines(TextReader(limit=2), first, second) == ["1", "2"]
    assert _lines(TextReader(limit=4), first, second) == ["1", "2", "3", "4"]
    assert _lines(TextReader(limit=10), first, second) == ["1", "2", "3", "4", "5"]


def test_reader_limit_does_not_open_later_files(tmp_path):
    first = _write(tmp_path / "a.txt", "1\n2\n")
    missing = tmp_path / "missing.txt"
    manifest = FileManifest.construct_manifest(
        paths=[str(first), str(missing)], sizes=[4, 1], chunk_metadatas=[None, None]
    )

    tables = list(TextReader(limit=2).read(manifest))

    assert pa.concat_tables(tables).column(TEXT_COLUMN_NAME).to_pylist() == ["1", "2"]


def test_reader_empty_projection_preserves_rows(tmp_path):
    path = _write(tmp_path / "data.txt", "a\nb\nc\n")

    scanner = TextScanner(schema=TEXT_SCHEMA).prune_columns([])
    tables = list(scanner.create_reader().read(_manifest(path)))

    assert scanner.read_schema().names == []
    assert sum(table.num_rows for table in tables) == 3
    assert all(
        table.column_names == [_BATCH_SIZE_PRESERVING_STUB_COL_NAME] for table in tables
    )


@pytest.mark.parametrize(
    "columns",
    [None, ["path", "year", TEXT_COLUMN_NAME], ["year"]],
    ids=["no_projection", "reordered", "partition_only"],
)
def test_reader_synthesizes_partition_and_path_columns(tmp_path, columns):
    path = _write(tmp_path / "year=2024" / "data.txt", "a\nb\n")
    partitioning = Partitioning(
        PartitionStyle.HIVE, base_dir=str(tmp_path), field_types={"year": int}
    )
    schema = pa.schema(
        [(TEXT_COLUMN_NAME, pa.string()), ("year", pa.int64()), ("path", pa.string())]
    )
    reader = TextReader(
        columns=columns,
        partitioning=partitioning,
        synthesized_columns=(PathColumn(),),
        schema=schema,
    )

    table = _read(reader, path)

    expected_names = columns or [TEXT_COLUMN_NAME, "year", "path"]
    assert table.column_names == expected_names
    assert table.num_rows == 2
    if "year" in expected_names:
        assert table.column("year").type == pa.int64()
        assert table.column("year").to_pylist() == [2024, 2024]
    if "path" in expected_names:
        assert table.column("path").to_pylist() == [str(path)] * 2
    if TEXT_COLUMN_NAME in expected_names:
        assert table.column(TEXT_COLUMN_NAME).to_pylist() == ["a", "b"]


def test_reader_null_fills_partition_columns_of_root_files(tmp_path):
    root = _write(tmp_path / "root.txt", "r\n")
    nested = _write(tmp_path / "year=2024" / "data.txt", "n\n")
    schema = pa.schema(
        [(TEXT_COLUMN_NAME, pa.string()), ("year", pa.string()), ("path", pa.string())]
    )
    reader = TextReader(
        partitioning=Partitioning(PartitionStyle.HIVE, base_dir=str(tmp_path)),
        synthesized_columns=(PathColumn(),),
        schema=schema,
    )

    tables = list(reader.read(_manifest(root, nested)))

    assert [table.schema for table in tables] == [schema, schema]
    assert pa.concat_tables(tables).to_pylist() == [
        {TEXT_COLUMN_NAME: "r", "year": None, "path": str(root)},
        {TEXT_COLUMN_NAME: "n", "year": "2024", "path": str(nested)},
    ]


def test_reader_rejects_partition_field_named_text(tmp_path):
    path = _write(tmp_path / "text=a" / "data.txt", "line\n")
    reader = TextReader(
        partitioning=Partitioning(PartitionStyle.HIVE, base_dir=str(tmp_path)),
        schema=TEXT_SCHEMA,
    )

    with pytest.raises(ValueError, match="Partition field 'text'"):
        list(reader.read(_manifest(path)))


def test_reader_rejects_root_file_when_partition_type_is_declared(tmp_path):
    root = _write(tmp_path / "root.txt", "r\n")
    reader = TextReader(
        partitioning=Partitioning(
            PartitionStyle.HIVE, base_dir=str(tmp_path), field_types={"year": int}
        ),
    )

    with pytest.raises(ValueError, match="Expected partition field 'year'"):
        list(reader.read(_manifest(root)))


@pytest.mark.parametrize(
    "predicate, expected",
    [
        (col("year") == "2024", ["n24"]),
        (col("year") != "2024", ["n23"]),
        (col("year").is_null(), ["r"]),
        (col("year").is_not_null(), ["n23", "n24"]),
    ],
    ids=["eq", "ne", "is_null", "is_not_null"],
)
def test_reader_applies_partition_predicate_before_opening(
    tmp_path, predicate, expected
):
    root = _write(tmp_path / "root.txt", "r\n")
    n23 = _write(tmp_path / "year=2023" / "data.txt", "n23\n")
    n24 = _write(tmp_path / "year=2024" / "data.txt", "n24\n")
    schema = pa.schema([(TEXT_COLUMN_NAME, pa.string()), ("year", pa.string())])
    opened = []

    class CountingReader(TextReader):
        def _read_bytes(self, path):
            opened.append(path)
            return super()._read_bytes(path)

    reader = CountingReader(
        partitioning=Partitioning(PartitionStyle.HIVE, base_dir=str(tmp_path)),
        partition_predicate=predicate,
        schema=schema,
    )

    assert sorted(_lines(reader, root, n23, n24)) == expected
    assert len(opened) == len(expected)


@pytest.mark.parametrize(
    "suffix, compress",
    [(".txt.gz", gzip.compress), (".txt.bz2", bz2.compress)],
    ids=["gzip", "bz2"],
)
def test_reader_decompresses_by_extension(tmp_path, suffix, compress):
    path = tmp_path / f"data{suffix}"
    path.write_bytes(compress("a\nb\n".encode("utf-8")))

    assert _lines(TextReader(), path) == ["a", "b"]


def test_reader_reports_missing_file_path(tmp_path):
    missing = tmp_path / "missing.txt"
    manifest = FileManifest.construct_manifest(
        paths=[str(missing)], sizes=[1], chunk_metadatas=[None]
    )
    with pytest.raises(FileNotFoundError, match=str(missing)):
        list(TextReader().read(manifest))


def test_reader_retries_transient_errors(tmp_path, monkeypatch):
    path = _write(tmp_path / "data.txt", "a\n")
    monkeypatch.setattr(
        DataContext.get_current(), "retried_io_errors", ["flaky storage"]
    )
    attempts = []
    original = TextReader._read_file

    def flaky(self, file_path, limit):
        attempts.append(file_path)
        if len(attempts) == 1:
            raise OSError("flaky storage")
        return original(self, file_path, limit)

    monkeypatch.setattr(TextReader, "_read_file", flaky)
    # ``tests/unit/conftest.py`` makes ``time.sleep`` raise.
    monkeypatch.setattr("ray.data._internal.util.time.sleep", lambda _: None)

    assert _lines(TextReader(), path) == ["a"]
    assert len(attempts) == 2


def test_reader_runs_pickle_object_gate(tmp_path, monkeypatch):
    path = _write(tmp_path / "data.txt", "a\n")
    gate = Mock(side_effect=ValueError("arrow_pickled_object"))
    monkeypatch.setattr(text_reader_mod, "raise_on_pickle_object_columns", gate)

    with pytest.raises(ValueError, match="arrow_pickled_object"):
        list(TextReader().read(_manifest(path)))
    gate.assert_called_once()


# -- scanner -------------------------------------------------------------------


def test_scanner_read_schema_projects_planned_columns():
    schema = pa.schema(
        [(TEXT_COLUMN_NAME, pa.string()), ("year", pa.int64()), ("path", pa.string())]
    )
    scanner = TextScanner(schema=schema)

    assert scanner.read_schema() == schema
    assert scanner.prune_columns(["path", TEXT_COLUMN_NAME]).read_schema() == pa.schema(
        [("path", pa.string()), (TEXT_COLUMN_NAME, pa.string())]
    )


def test_scanner_pushdowns_survive_and_reach_the_reader():
    scanner = (
        TextScanner(schema=TEXT_SCHEMA, encoding="latin-1", drop_empty_lines=False)
        .prune_columns([TEXT_COLUMN_NAME])
        .push_limit(7)
        .push_limit(9)
    )

    assert scanner.pruned_column_names() == (TEXT_COLUMN_NAME,)
    assert scanner.pushed_limit() == 7
    reader = scanner.create_reader()
    assert reader._encoding == "latin-1"
    assert reader._drop_empty_lines is False
    assert reader._columns == [TEXT_COLUMN_NAME]
    assert reader._limit == 7


def test_scanner_does_not_claim_filter_pushdown():
    from ray.data._internal.datasource_v2.interfaces.pushdown import (
        SupportsFilterPushdown,
    )

    assert not isinstance(TextScanner(schema=TEXT_SCHEMA), SupportsFilterPushdown)


def test_scanner_prunes_partitions_from_typed_paths_and_keeps_root_files(tmp_path):
    scanner = TextScanner(
        schema=pa.schema([(TEXT_COLUMN_NAME, pa.string()), ("year", pa.int64())]),
        partitioning=Partitioning(
            PartitionStyle.HIVE, base_dir=str(tmp_path), field_names=["year"]
        ),
    ).prune_partitions(col("year") == 2024)
    paths = [
        str(tmp_path / "year=2024" / "a.txt"),
        str(tmp_path / "year=2023" / "b.txt"),
        str(tmp_path / "root.txt"),
    ]
    manifest = FileManifest.construct_manifest(
        paths=paths, sizes=[1, 1, 1], chunk_metadatas=[None, None, None]
    )

    assert scanner.partition_columns == {"year"}
    assert scanner.prune_input_split(manifest).paths.tolist() == [paths[0], paths[2]]
    pruner = scanner.pushed_partition_pruner()
    assert pruner is not None
    assert [pruner.should_include(path) for path in paths] == [True, False, True]
    reader = scanner.create_reader()
    assert isinstance(reader, TextReader)
    assert reader._partition_predicate is scanner.partition_predicate


# -- datasource ----------------------------------------------------------------


def test_datasource_infer_schema_is_fixed_and_opens_no_files(tmp_path):
    paths = [str(tmp_path / "missing.txt")]
    sample = FileManifest.construct_manifest(
        paths=paths, sizes=[1], chunk_metadatas=[None]
    )

    datasource = TextDatasourceV2([str(tmp_path)])

    assert datasource.schema_needs_file_sample
    assert datasource.infer_schema(sample) == TEXT_SCHEMA
    assert datasource.infer_schema(None) == TEXT_SCHEMA


def test_datasource_infer_schema_adds_partition_and_path_columns(tmp_path):
    first = _write(tmp_path / "year=2023" / "a.txt", "a\n")
    second = _write(tmp_path / "year=2024" / "b.txt", "b\n")
    datasource = TextDatasourceV2(
        [str(tmp_path)],
        partitioning=Partitioning(
            PartitionStyle.HIVE, base_dir=str(tmp_path), field_types={"year": int}
        ),
        include_paths=True,
    )

    schema = datasource.infer_schema(_manifest(first, second))

    assert schema == pa.schema(
        [(TEXT_COLUMN_NAME, pa.string()), ("year", pa.int64()), ("path", pa.string())]
    )


def test_datasource_infer_schema_with_untyped_root_file(tmp_path):
    root = _write(tmp_path / "root.txt", "a\n")
    nested = _write(tmp_path / "year=2024" / "b.txt", "b\n")
    datasource = TextDatasourceV2(
        [str(tmp_path)],
        partitioning=Partitioning(PartitionStyle.HIVE, base_dir=str(tmp_path)),
    )

    schema = datasource.infer_schema(_manifest(root, nested))

    assert schema == pa.schema([(TEXT_COLUMN_NAME, pa.string()), ("year", pa.string())])


def test_datasource_infer_schema_rejects_partition_field_named_text(tmp_path):
    path = _write(tmp_path / "text=a" / "data.txt", "line\n")
    datasource = TextDatasourceV2(
        [str(tmp_path)],
        partitioning=Partitioning(PartitionStyle.HIVE, base_dir=str(tmp_path)),
    )

    with pytest.raises(ValueError, match="Partition field 'text'"):
        datasource.infer_schema(_manifest(path))


def test_datasource_filesystem_retries_listing_errors(tmp_path):
    datasource = TextDatasourceV2([str(tmp_path)])

    assert isinstance(datasource.filesystem, RetryingPyFileSystem)
    assert isinstance(datasource.filesystem.unwrap(), LocalFileSystem)


@pytest.mark.parametrize("preserve_order", [False, True])
def test_datasource_uses_one_bucket_when_order_is_preserved(
    tmp_path, monkeypatch, preserve_order
):
    monkeypatch.setattr(
        DataContext.get_current().execution_options, "preserve_order", preserve_order
    )
    paths = [_write(tmp_path / f"{i}.txt", f"{i}\n") for i in range(5)]
    datasource = TextDatasourceV2([str(tmp_path)])

    partitioner = datasource.get_file_partitioner(
        hints=PartitionHints(min_bucket_size=0, max_bucket_size=1024, num_buckets=2)
    )

    assert isinstance(partitioner, RoundRobinPartitioner)
    assert partitioner.num_buckets == (1 if preserve_order else 2)
    for path in paths:
        partitioner.add_input(_manifest(path))
    partitioner.finalize()
    partitions = []
    while partitioner.has_partition():
        partitions.append(partitioner.next_partition().paths.tolist())
    if preserve_order:
        assert partitions == [[str(path) for path in paths]]
    else:
        assert sorted(sum(partitions, [])) == [str(path) for path in paths]


def test_datasource_reuses_generic_file_components(tmp_path):
    path = _write(tmp_path / "data.txt", "a\nb\n")
    datasource = TextDatasourceV2([str(tmp_path)], file_extensions=["txt"])
    sample = _manifest(path)

    assert isinstance(datasource._get_file_indexer(), NonSamplingFileIndexer)
    assert datasource.get_file_partitioner(hints=None) is None
    partitioner = datasource.get_file_partitioner(
        hints=PartitionHints(min_bucket_size=0, max_bucket_size=1024, num_buckets=1)
    )
    assert isinstance(partitioner, RoundRobinPartitioner)
    estimator = partitioner._in_memory_size_estimator
    assert not isinstance(estimator, SamplingInMemorySizeEstimator)
    assert estimator.estimate_in_memory_sizes(sample).tolist() == [4]
    partitioner.add_input(sample)
    partitioner.finalize()
    assert partitioner.has_partition()
    assert partitioner.next_partition().paths.tolist() == sample.paths.tolist()


def test_datasource_keeps_caller_paths_for_lineage(tmp_path):
    uri = f"file://{tmp_path}"

    datasource = TextDatasourceV2([uri])

    assert datasource.source_paths == [uri]
    assert datasource.paths == [str(tmp_path)]
    assert datasource.supports_distributed_reads


def test_list_and_read_text_files_with_partition_columns(tmp_path):
    _write(tmp_path / "year=2023" / "a.txt", "one\n")
    _write(tmp_path / "year=2024" / "b.txt", "two\n\nthree\n")
    datasource = TextDatasourceV2(
        [str(tmp_path)],
        partitioning=Partitioning(PartitionStyle.HIVE, base_dir=str(tmp_path)),
        file_extensions=["txt"],
        include_paths=True,
    )

    manifests = list(
        datasource._get_file_indexer().list_files(
            pa.array(datasource.paths, type=pa.string()),
            filesystem=datasource.filesystem,
            preserve_order=True,
        )
    )
    manifest = FileManifest.concat(manifests)
    schema = datasource.infer_schema(manifest)
    partitioning = datasource.resolve_partitioning(manifest)
    scanner = datasource.create_scanner(
        schema, filesystem=datasource.filesystem, partitioning=partitioning
    )

    assert isinstance(scanner, TextScanner)
    assert scanner.read_schema() == schema
    result = pa.concat_tables(list(scanner.create_reader().read(manifest)))
    rows = sorted(result.to_pylist(), key=lambda row: row[TEXT_COLUMN_NAME])
    assert [(row[TEXT_COLUMN_NAME], row["year"]) for row in rows] == [
        ("one", "2023"),
        ("three", "2024"),
        ("two", "2024"),
    ]
    assert rows[0]["path"].endswith("year=2023/a.txt")
    assert rows[1]["path"].endswith("year=2024/b.txt")


# -- read_api routing ----------------------------------------------------------


def test_read_text_routes_to_v2_and_forwards_read_options(monkeypatch, tmp_path):
    result = object()
    captured = {}

    def _read_datasource_v2(datasource, **kwargs):
        captured["datasource"] = datasource
        captured["kwargs"] = kwargs
        return result

    monkeypatch.setattr(read_api, "_read_datasource_v2", _read_datasource_v2)
    monkeypatch.setattr(read_api.DataContext.get_current(), "use_datasource_v2", True)
    partition_filter = Mock()

    actual = read_api.read_text(
        str(tmp_path),
        encoding="latin-1",
        drop_empty_lines=False,
        partition_filter=partition_filter,
        include_paths=True,
        file_extensions=["txt"],
        concurrency=4,
        parallelism=2,
        override_num_blocks=3,
    )

    datasource = captured["datasource"]
    assert actual is result
    assert isinstance(datasource, TextDatasourceV2)
    assert datasource.paths == [str(tmp_path)]
    assert datasource.file_extensions == ["txt"]
    assert datasource._encoding == "latin-1"
    assert datasource._drop_empty_lines is False
    assert datasource._synthesized_columns == (PathColumn(),)
    assert captured["kwargs"]["parallelism"] == 2
    assert captured["kwargs"]["concurrency"] == 4
    assert captured["kwargs"]["partition_filter"] is partition_filter


@pytest.mark.parametrize(
    "use_v2, open_stream_args, path",
    [
        (False, None, None),
        (True, {"compression": "gzip"}, None),
        (True, None, "http://example.com/data.txt"),
    ],
    ids=["v2_disabled", "open_stream_args", "http"],
)
def test_read_text_routes_to_v1(monkeypatch, tmp_path, use_v2, open_stream_args, path):
    result = object()
    captured = {}
    path = path or str(tmp_path)

    class FakeTextDatasource:
        def __init__(self, *args, **kwargs):
            captured["v1_init_args"] = args
            captured["v1_init_kwargs"] = kwargs

    def _read_datasource(datasource, **kwargs):
        captured["datasource"] = datasource
        captured["kwargs"] = kwargs
        return result

    def _unexpected_v2(*args, **kwargs):
        raise AssertionError("read_text must not route to V2 here")

    monkeypatch.setattr(read_api, "TextDatasource", FakeTextDatasource)
    monkeypatch.setattr(read_api, "read_datasource", _read_datasource)
    monkeypatch.setattr(read_api, "_read_datasource_v2", _unexpected_v2)
    monkeypatch.setattr(read_api.DataContext.get_current(), "use_datasource_v2", use_v2)

    actual = read_api.read_text(
        path,
        include_paths=True,
        parallelism=2,
        override_num_blocks=3,
        arrow_open_stream_args=open_stream_args,
    )

    assert actual is result
    assert isinstance(captured["datasource"], FakeTextDatasource)
    assert captured["v1_init_args"] == (path,)
    assert captured["v1_init_kwargs"]["include_paths"] is True
    assert captured["v1_init_kwargs"]["open_stream_args"] == open_stream_args
    assert captured["kwargs"]["parallelism"] == 2
    assert captured["kwargs"]["override_num_blocks"] == 3


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
