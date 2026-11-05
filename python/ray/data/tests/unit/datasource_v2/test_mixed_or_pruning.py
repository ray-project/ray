"""Mixed OR pruning regressions using real Arrow fragments without a Ray cluster."""
import inspect
import operator
from dataclasses import dataclass, fields, replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.dataset as pads
import pyarrow.parquet as pq
import pytest

from ray.data._internal.datasource_v2.common.synthesized_columns import PathColumn
from ray.data._internal.datasource_v2.formats.parquet.mixed_or_pruning import (
    specialize_fragment,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_file_reader import (
    ParquetFileReader,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_scanner import (
    ParquetScanner,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data.context import DataContext
from ray.data.datasource.partitioning import Partitioning, PartitionStyle
from ray.data.expressions import col


@pytest.fixture(params=["base", "inherited", "extended"])
def scanner_type(request):
    if request.param == "base":
        return ParquetScanner
    if request.param == "inherited":

        class InheritedScanner(ParquetScanner):
            pass

        return InheritedScanner

    @dataclass(frozen=True)
    class ExtendedScanner(ParquetScanner):
        source_tag: str = "default"

    return ExtendedScanner


def test_scanner_preserves_existing_positional_arguments(scanner_type):
    # Preserve the positional signature predating the mixed OR hint, including
    # fields added by downstream dataclass subclasses.
    values: dict[str, Any] = {
        "schema": pa.schema([("id", pa.int64()), ("year", pa.string())]),
        "batch_size": 128,
        "columns": ("id",),
        "predicate": col("id") > 0,
        "limit": 7,
        "filesystem": pa.fs.LocalFileSystem(),
        "ignore_prefixes": ["_"],
        "target_block_size": 4096,
        "synthesized_columns": (PathColumn(),),
        "parquet_format_kwargs": {"pre_buffer": False},
    }
    if "source_tag" in scanner_type.__dataclass_fields__:
        values["source_tag"] = "external-source"
    partitioning = Partitioning(PartitionStyle.HIVE, field_names=["year"])
    partition_predicate = col("year") == "2023"
    scanner = scanner_type(
        *values.values(),
        partitioning=partitioning,
        partition_predicate=partition_predicate,
        shuffle="files",
    )
    for name, value in values.items():
        assert getattr(scanner, name) is value
    assert scanner.partitioning is partitioning
    assert scanner.partition_predicate is partition_predicate
    assert scanner.shuffle == "files"
    assert scanner.mixed_pruning_predicate is None
    positional = [
        name
        for name, parameter in inspect.signature(scanner_type).parameters.items()
        if parameter.kind == inspect.Parameter.POSITIONAL_OR_KEYWORD
    ]
    assert positional == list(values)


def test_scanner_accepts_mixed_hint_by_keyword(scanner_type):
    expr = (col("year") == "2023") | (col("id") < 0)
    scanner = scanner_type(
        schema=pa.schema([("id", pa.int64()), ("year", pa.string())]),
        mixed_pruning_predicate=expr,
    )
    assert scanner.mixed_pruning_predicate is expr
    assert not scanner.metadata_row_count_is_exact()
    assert replace(scanner, limit=3).mixed_pruning_predicate is expr
    expected = expr if scanner_type is ParquetScanner else None
    assert scanner.create_reader()._mixed_pruning_predicate is expected


def test_scanner_mixed_hint_is_keyword_only():
    parameter = inspect.signature(ParquetScanner).parameters["mixed_pruning_predicate"]
    assert parameter.kind == inspect.Parameter.KEYWORD_ONLY
    assert next(f for f in fields(ParquetScanner) if f.name == parameter.name).kw_only


@pytest.fixture
def fixture(tmp_path, monkeypatch):
    monkeypatch.setattr(
        DataContext.get_current().execution_options, "preserve_order", True
    )
    schema = pa.schema(
        [
            ("id", pa.int64()),
            ("x", pa.int64()),
            ("payload", pa.string()),
            ("year", pa.string()),
        ]
    )
    files, rows = [], []
    for year, offset in [("2020", 0), ("2021", 100), ("2023", 200)]:
        path = tmp_path / f"year={year}" / "part.parquet"
        path.parent.mkdir()
        table = pa.table(
            {
                "id": list(range(offset, offset + 20)),
                "x": [None if i % 3 == 0 else offset + i for i in range(20)],
                "payload": [f"row-{offset+i}" for i in range(20)],
            }
        )
        pq.write_table(table, path, row_group_size=5)
        files.append(str(path))
        rows.extend(dict(r, year=year) for r in table.to_pylist())
    manifest = FileManifest.construct_manifest(
        paths=files,
        sizes=[Path(p).stat().st_size for p in files],
        chunk_metadatas=[None] * len(files),
    )
    return (
        schema,
        files,
        rows,
        manifest,
        Partitioning(PartitionStyle.HIVE, base_dir=str(tmp_path), field_names=["year"]),
    )


class ObservedReader(ParquetFileReader):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.groups = []

    def _iter_fragment_tables_without_pickle_check(self, fragment, scanner_kwargs):
        self.groups.extend((fragment.path, r.id) for r in fragment.row_groups)
        yield from super()._iter_fragment_tables_without_pickle_check(
            fragment, scanner_kwargs
        )


@pytest.mark.parametrize(
    "comparison", [operator.lt, operator.le, operator.eq, operator.ge, operator.gt]
)
@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("typed", [False, True])
def test_full_rows_match_with_residual(fixture, comparison, reverse, typed):
    schema, files, rows, manifest, partitioning = fixture
    if typed:
        schema = schema.set(3, pa.field("year", pa.int64()))
    path = col("year") == (2023 if typed else "2023")
    data = comparison(col("id"), 105)
    expr = (data | path) if reverse else (path | data)
    oracle = pa.Table.from_pylist(rows).cast(schema).filter(expr.to_pyarrow())
    counts = []
    for hint in [None, expr]:
        reader = ObservedReader(
            schema=schema,
            partitioning=partitioning,
            mixed_pruning_predicate=hint,
            batch_size=5,
        )
        tables = list(reader.read(manifest))
        actual = pa.concat_tables(tables).filter(expr.to_pyarrow())
        assert actual.to_pylist() == oracle.to_pylist()
        assert len(reader.groups) == len(set(reader.groups))
        counts.append(len(reader.groups))
    assert counts[0] == 12
    expected = {
        operator.lt: 9,
        operator.le: 10,
        operator.eq: 5,
        operator.ge: 7,
        operator.gt: 7,
    }
    assert counts[1] == expected[comparison]


@pytest.mark.parametrize("kind", ["physical_cast", "arithmetic", "nullable"])
def test_reader_fallback_or_nullable_negative(fixture, kind):
    schema, files, rows, manifest, partitioning = fixture
    if kind == "physical_cast":
        schema = schema.set(0, pa.field("id", pa.int32()))
    data = (
        (col("id") + 1 < 5)
        if kind == "arithmetic"
        else (col("x") < 5)
        if kind == "nullable"
        else (col("id") < 5)
    )
    expr = (col("year") == "2023") | data
    reader = ObservedReader(
        schema=schema,
        partitioning=partitioning,
        mixed_pruning_predicate=expr,
        batch_size=5,
    )
    actual = pa.concat_tables(list(reader.read(manifest))).filter(expr.to_pyarrow())
    oracle = pa.Table.from_pylist(rows).cast(schema).filter(expr.to_pyarrow())
    assert actual.to_pylist() == oracle.to_pylist()
    if kind != "nullable":
        assert not reader._mixed_subset_enabled
        assert len(reader.groups) == 12
    # Nullable pruning depends on Arrow version; do not enshrine its current bug.


@pytest.mark.parametrize(
    "kind",
    [
        "missing",
        "null",
        "physical_collision",
        "projected_partition",
        "no_schema",
        "bool_literal",
        "overflow",
        "type_mismatch",
    ],
)
def test_unsafe_fragment_declines_hint(fixture, kind, tmp_path):
    schema, files, rows, manifest, partitioning = fixture
    reader = ParquetFileReader(schema=schema, partitioning=partitioning)
    fragment = next(pads.dataset(files[0]).get_fragments())
    expr = (col("year") == "2023") | (col("id") < 5)
    parser, columns = reader._partition_parser, None
    if kind in ("missing", "null"):

        parser = MagicMock()
        parser.scheme = SimpleNamespace(field_names=["year"], style="hive")
        parser.return_value = {} if kind == "missing" else {"year": None}
    elif kind == "physical_collision":
        path = tmp_path / "year=2020" / "collision.parquet"
        pq.write_table(pa.table({"id": [1], "year": ["2023"]}), path)
        fragment = next(pads.dataset(str(path)).get_fragments())
    elif kind == "projected_partition":
        columns = ["id"]
    elif kind == "no_schema":
        schema = None
    elif kind == "bool_literal":
        expr = (col("year") == "2023") | (col("id") < True)
    elif kind == "overflow":
        expr = (col("year") == "2023") | (col("id") < 2**80)
    elif kind == "type_mismatch":
        expr = (col("year") == 2023) | (col("id") < 5)
    safe, hint = specialize_fragment(expr, parser, schema, fragment, columns)
    assert not safe and hint is None


def test_missing_statistics_keeps_groups(fixture):
    schema, files, rows, manifest, partitioning = fixture
    for filename in files:
        table = pq.ParquetFile(filename).read()
        pq.write_table(table, filename, row_group_size=5, write_statistics=False)
    manifest = FileManifest.construct_manifest(
        paths=files,
        sizes=[Path(p).stat().st_size for p in files],
        chunk_metadatas=[None] * len(files),
    )
    expr = (col("year") == "2023") | (col("id") < 5)
    reader = ObservedReader(
        schema=schema,
        partitioning=partitioning,
        mixed_pruning_predicate=expr,
        batch_size=5,
    )
    actual = pa.concat_tables(list(reader.read(manifest))).filter(expr.to_pyarrow())
    assert (
        actual.to_pylist()
        == pa.Table.from_pylist(rows).cast(schema).filter(expr.to_pyarrow()).to_pylist()
    )
    assert len(reader.groups) == 12


def test_subset_does_not_reintroduce_excluded_groups(fixture):
    schema, files, rows, manifest, partitioning = fixture
    reader = ParquetFileReader(schema=schema, partitioning=partitioning)
    original = next(pads.dataset(files[0]).get_fragments()).subset(row_group_ids=[1, 3])
    expr = (col("year") == "2023") | (col("id") < 15)
    safe, hint = specialize_fragment(
        expr, reader._partition_parser, schema, original, None
    )
    assert safe and hint is not None
    result = original.subset(filter=hint)
    assert [g.id for g in result.row_groups] == [1]


@pytest.mark.parametrize("root_first", [True, False])
def test_mixed_root_disables_hint_for_entire_manifest(fixture, tmp_path, root_first):
    schema, files, rows, manifest, partitioning = fixture
    root = tmp_path / "root.parquet"
    pq.write_table(
        pa.table({"id": [999], "x": [0], "payload": ["root"], "year": ["2023"]}), root
    )
    paths = [str(root), *files] if root_first else [*files, str(root)]
    manifest = FileManifest.construct_manifest(
        paths=paths,
        sizes=[Path(p).stat().st_size for p in paths],
        chunk_metadatas=[None] * len(paths),
    )
    expr = (col("year") == "2023") | (col("id") < 5)
    reader = ObservedReader(
        schema=schema, partitioning=partitioning, mixed_pruning_predicate=expr
    )
    list(reader.read(manifest))
    # This checks safe fallback only. Mixed-root row semantics have a separate
    # upstream fix; matching the old reader is not evidence of correct rows.
    assert not reader._mixed_subset_enabled
    assert len(reader.groups) == 13


@pytest.mark.parametrize("signed_type", [pa.int8(), pa.int16(), pa.int32(), pa.int64()])
@pytest.mark.parametrize("typed_parser", [False, True])
def test_multiple_partitions_and_existing_filter(tmp_path, signed_type, typed_parser):
    schema = pa.schema(
        [("id", signed_type), ("year", pa.int64()), ("region", pa.string())]
    )
    paths, rows = [], []
    for year in [2020, 2023]:
        for region in ["east", "west"]:
            folder = tmp_path / f"year={year}" / f"region={region}"
            folder.mkdir(parents=True)
            path = folder / "part.parquet"
            values = [None, -5, -1, 0, 2, 5, 9, 12]
            pq.write_table(
                pa.table({"id": pa.array(values, type=signed_type)}),
                path,
                row_group_size=2,
            )
            paths.append(str(path))
            rows.extend(
                {"id": value, "year": year, "region": region} for value in values
            )
    partitioning = Partitioning(
        PartitionStyle.HIVE,
        base_dir=str(tmp_path),
        field_names=["year", "region"],
        field_types={"year": int} if typed_parser else {},
    )
    manifest = FileManifest.construct_manifest(
        paths=paths,
        sizes=[Path(p).stat().st_size for p in paths],
        chunk_metadatas=[None] * len(paths),
    )
    expr = (col("year") == 2023) | (col("id") < 0)
    existing = col("id") > -3
    reader = ObservedReader(
        schema=schema,
        partitioning=partitioning,
        predicate=existing,
        mixed_pruning_predicate=expr,
    )
    actual = pa.concat_tables(list(reader.read(manifest))).filter(expr.to_pyarrow())
    expected = pa.Table.from_pylist(rows, schema=schema).filter(
        (expr & existing).to_pyarrow()
    )
    assert sorted(
        actual.to_pylist(), key=lambda row: (row["year"], row["region"], row["id"])
    ) == sorted(
        expected.to_pylist(), key=lambda row: (row["year"], row["region"], row["id"])
    )
    # Preserve evaluation of the separately pushed data predicate.
    assert not reader._mixed_subset_enabled


@pytest.mark.parametrize(
    "dtype,value,literal",
    [
        (pa.int64(), "2023", 2**80),
        (pa.int64(), str(2**63 - 1), 2**63),
        (pa.int64(), str(-(2**63)), -(2**63) - 1),
        (pa.int8(), "127", 128),
        (pa.string(), "2023", "\ud800"),
    ],
)
def test_unrepresentable_partition_literal_declines_hint(
    tmp_path, dtype, value, literal
):
    folder = tmp_path / f"year={value}"
    folder.mkdir()
    path = folder / "part.parquet"
    pq.write_table(pa.table({"id": [10, 11]}), path)
    schema = pa.schema([("id", pa.int64()), ("year", dtype)])
    reader = ParquetFileReader(
        schema=schema,
        partitioning=Partitioning(
            PartitionStyle.HIVE, base_dir=str(tmp_path), field_names=["year"]
        ),
    )
    fragment = next(pads.dataset(str(path)).get_fragments())
    expr = (col("year") == literal) | (col("id") < 0)
    eligible, hint = specialize_fragment(
        expr, reader._partition_parser, schema, fragment, None
    )
    assert not eligible and hint is None


@pytest.mark.parametrize(
    "kind",
    [
        "payload_bad_int",
        "payload_valid_int",
        "payload_bad_utf8",
        "other_partition_bad_int",
        "other_partition_valid_int",
    ],
)
def test_all_materialization_types_are_checked_before_pruning(tmp_path, kind):
    folder = tmp_path / "year=2020"
    fields = [("id", pa.int64()), ("year", pa.string())]
    names = ["year"]
    table = pa.table({"id": [10, 11, 12, 13]})
    if kind.startswith("other_partition"):
        folder = folder / ("month=abc" if kind.endswith("bad_int") else "month=7")
        names.append("month")
        fields.append(("month", pa.int64()))
    else:
        values = (
            [b"\xff"] * 4
            if kind.endswith("bad_utf8")
            else (["123"] * 4 if kind.endswith("valid_int") else ["abc"] * 4)
        )
        table = table.append_column("payload", pa.array(values))
        fields.append(
            ("payload", pa.string() if kind.endswith("bad_utf8") else pa.int64())
        )
    folder.mkdir(parents=True)
    path = folder / "part.parquet"
    pq.write_table(table, path, row_group_size=2)
    schema = pa.schema(fields)
    partitioning = Partitioning(
        PartitionStyle.HIVE, base_dir=str(tmp_path), field_names=names
    )
    manifest = FileManifest.construct_manifest(
        paths=[str(path)], sizes=[path.stat().st_size], chunk_metadatas=[None]
    )
    expr = (col("year") == "2023") | (col("id") < 0)
    reader = ObservedReader(
        schema=schema, partitioning=partitioning, mixed_pruning_predicate=expr
    )
    if "bad" in kind:
        with pytest.raises(pa.ArrowInvalid):
            list(reader.read(manifest))
        assert not reader._mixed_subset_enabled
    elif kind == "payload_valid_int":
        rows = pa.concat_tables(list(reader.read(manifest))).filter(expr.to_pyarrow())
        assert rows.num_rows == 0
        assert not reader._mixed_subset_enabled
        assert len(reader.groups) == 2
    else:
        assert sum(table.num_rows for table in reader.read(manifest)) == 0
        assert reader._mixed_subset_enabled


def test_existing_reader_limit_disables_hint(fixture):
    schema, files, rows, manifest, partitioning = fixture
    expr = (col("year") == "2023") | (col("id") < 0)
    reader = ObservedReader(
        schema=schema, partitioning=partitioning, limit=3, mixed_pruning_predicate=expr
    )
    actual = pa.concat_tables(list(reader.read(manifest)))
    assert actual.to_pylist() == rows[:3]
    assert not reader._mixed_subset_enabled
    assert actual.filter(expr.to_pyarrow()).num_rows == 0


@pytest.mark.parametrize("duplicate_name", ["id", "payload", "year"])
def test_duplicate_physical_names_still_decline_hint(duplicate_name):
    schema = pa.schema(
        [("id", pa.int64()), ("payload", pa.int64()), ("year", pa.string())]
    )
    physical = pa.schema(
        [("id", pa.int64()), ("payload", pa.int64())]
        + [(duplicate_name, schema.field(duplicate_name).type)] * 2
    )
    parser = MagicMock()
    parser.scheme.style = "hive"
    parser.scheme.field_names = ["year"]
    parser.return_value = {"year": "2020"}
    fragment = MagicMock(spec=pads.ParquetFileFragment)
    fragment.path = "year=2020/part.parquet"
    fragment.physical_schema = physical
    eligible, hint = specialize_fragment(
        (col("year") == "2023") | (col("id") < 0),
        parser,
        schema,
        fragment,
        None,
    )
    assert not eligible and hint is None


@pytest.mark.parametrize("hint_first", [False, True])
@pytest.mark.parametrize("partial", [False, True])
def test_existing_predicate_errors_are_not_pruned(tmp_path, hint_first, partial):
    path = tmp_path / "year=2020" / "part.parquet"
    path.parent.mkdir()
    pq.write_table(pa.table({"id": [10, 11, 12, 13]}), path, row_group_size=2)
    scanner = ParquetScanner(
        schema=pa.schema([("id", pa.int64()), ("year", pa.string())]),
        partitioning=Partitioning(
            PartitionStyle.HIVE, base_dir=str(tmp_path), field_names=["year"]
        ),
    )
    existing = (col("id") / (col("id") - 10)) > 0
    hint = (col("year") == "2023") | ((col("id") >= 12) if partial else (col("id") < 0))
    if hint_first:
        scanner = replace(scanner, mixed_pruning_predicate=hint)
        scanner, _ = scanner.push_filters(existing)
        assert isinstance(scanner, ParquetScanner)
    else:
        scanner, _ = scanner.push_filters(existing)
        assert isinstance(scanner, ParquetScanner)
        scanner = replace(scanner, mixed_pruning_predicate=hint)
    manifest = FileManifest.construct_manifest(
        paths=[str(path)], sizes=[path.stat().st_size], chunk_metadatas=[None]
    )
    reader = scanner.create_reader()
    assert isinstance(reader, ParquetFileReader)
    with pytest.raises(pa.ArrowInvalid, match="divide by zero"):
        list(reader.read(manifest))
    assert not reader._mixed_subset_enabled


@pytest.mark.parametrize("chunked", [False, True])
@pytest.mark.parametrize("failure", ["once", "persistent"])
@pytest.mark.parametrize("location", ["schema", "subset"])
def test_hint_io_failure_uses_original_read_path(
    tmp_path, monkeypatch, chunked, failure, location
):
    from ray.data._internal.datasource_v2.common import file_reader
    from ray.data._internal.datasource_v2.interfaces.file_manifest import FileChunk
    from ray.data._internal.util import iterate_with_retry

    path = tmp_path / "year=2020" / "part.parquet"
    path.parent.mkdir()
    pq.write_table(pa.table({"id": [10, 11, 12, 13]}), path, row_group_size=2)
    dataset = pads.dataset(str(path), format="parquet")
    fragment = next(dataset.get_fragments())
    calls = {"failures": 0, "attempts": 0, "backoffs": 0}

    def fail_if_needed():
        if failure == "persistent" or calls["failures"] == 0:
            calls["failures"] += 1
            raise OSError("AWS Error NETWORK_CONNECTION")

    class ObservedFragment:
        def __init__(self, wrapped):
            self.wrapped = wrapped

        def __getattr__(self, name):
            return getattr(self.wrapped, name)

        @property
        def physical_schema(self):
            if location == "schema":
                fail_if_needed()
            return self.wrapped.physical_schema

        @property
        def format(self):
            def make_fragment(*args, **kwargs):
                return ObservedFragment(
                    self.wrapped.format.make_fragment(*args, **kwargs)
                )

            return SimpleNamespace(make_fragment=make_fragment)

        def subset(self, **kwargs):
            if location == "subset" and "filter" in kwargs:
                fail_if_needed()
            return ObservedFragment(self.wrapped.subset(**kwargs))

    wrapped = ObservedFragment(fragment)
    monkeypatch.setattr(
        file_reader.pds,
        "dataset",
        lambda **kwargs: SimpleNamespace(
            schema=dataset.schema, get_fragments=lambda: iter([wrapped])
        ),
    )

    def bounded_retry(factory, description, *, match):
        def observed_factory():
            calls["attempts"] += 1
            yield from factory()

        return iterate_with_retry(
            observed_factory, description, match=match, max_attempts=2, max_backoff_s=0
        )

    monkeypatch.setattr(file_reader, "iterate_with_retry", bounded_retry)

    def record_backoff(seconds):
        assert seconds == 0
        calls["backoffs"] += 1

    monkeypatch.setattr("ray.data._internal.util.time.sleep", record_backoff)
    monkeypatch.setattr(
        DataContext.get_current(), "retried_io_errors", ["NETWORK_CONNECTION"]
    )
    chunk = (
        FileChunk(
            unit_ids=(0, 1), num_rows=4, size_bytes=path.stat().st_size
        ).to_metadata()
        if chunked
        else None
    )
    manifest = FileManifest.construct_manifest(
        paths=[str(path)], sizes=[path.stat().st_size], chunk_metadatas=[chunk]
    )
    hint = (col("year") == "2023") | (col("id") >= 12)
    reader = ParquetFileReader(
        schema=pa.schema([("id", pa.int64()), ("year", pa.string())]),
        partitioning=Partitioning(
            PartitionStyle.HIVE, base_dir=str(tmp_path), field_names=["year"]
        ),
        mixed_pruning_predicate=hint,
    )
    if location == "schema" and failure == "persistent":
        with pytest.raises(OSError, match="NETWORK_CONNECTION"):
            list(reader.read(manifest))
        assert calls["attempts"] == 2
        assert calls["backoffs"] == 1
        assert not reader._mixed_subset_enabled
    else:
        rows = pa.concat_tables(list(reader.read(manifest)))
        # The failed optimization must keep the original groups; the complete
        # residual remains responsible for the final two rows.
        assert rows["id"].to_pylist() == [10, 11, 12, 13]
        assert rows.filter(hint.to_pyarrow())["id"].to_pylist() == [12, 13]
        assert calls["attempts"] == 1
        assert calls["backoffs"] == 0
    assert calls["failures"] >= 1


@pytest.mark.parametrize("dtype", [pa.int8(), pa.int16(), pa.int32(), pa.int64()])
@pytest.mark.parametrize(
    "comparison", [operator.lt, operator.le, operator.ge, operator.gt]
)
@pytest.mark.parametrize("reverse", [False, True])
def test_integer_partition_ranges_full_rows(
    tmp_path, dtype, comparison, reverse, monkeypatch
):
    """Exercise true/false/equal partitions, extreme values and nullable data."""
    monkeypatch.setattr(
        DataContext.get_current().execution_options, "preserve_order", True
    )
    lo, hi = -(2 ** (dtype.bit_width - 1)), 2 ** (dtype.bit_width - 1) - 1
    schema = pa.schema([("id", pa.int64()), ("year", dtype)])
    paths, rows = [], []
    for year in [lo, -1, 0, 1, hi]:
        path = tmp_path / f"year={year}" / "data.parquet"
        path.parent.mkdir()
        table = pa.table({"id": [None, -2, 10, 11]})
        pq.write_table(table, path, row_group_size=2)
        paths.append(str(path))
        rows.extend(dict(r, year=year) for r in table.to_pylist())
    manifest = FileManifest.construct_manifest(
        paths=paths,
        sizes=[Path(p).stat().st_size for p in paths],
        chunk_metadatas=[None] * len(paths),
    )
    partitioning = Partitioning(
        PartitionStyle.HIVE, base_dir=str(tmp_path), field_names=["year"]
    )
    for literal in [lo, 0, hi]:
        path_expr, data_expr = comparison(col("year"), literal), col("id") < 0
        expr = data_expr | path_expr if reverse else path_expr | data_expr
        reader = ObservedReader(
            schema=schema,
            partitioning=partitioning,
            mixed_pruning_predicate=expr,
        )
        actual = pa.concat_tables(list(reader.read(manifest))).filter(expr.to_pyarrow())
        expected = pa.Table.from_pylist(rows, schema=schema).filter(expr.to_pyarrow())
        assert actual.to_pylist() == expected.to_pylist()
        assert reader._mixed_subset_enabled
        # True partitions retain both groups; false partitions drop [10, 11].
        assert len(reader.groups) == sum(
            2 if comparison(year, literal) else 1 for year in [lo, -1, 0, 1, hi]
        )


@pytest.mark.parametrize(
    "comparison", [operator.lt, operator.le, operator.ge, operator.gt]
)
@pytest.mark.parametrize("dtype", [pa.int8(), pa.int16(), pa.int32(), pa.int64()])
def test_integer_partition_range_literals(tmp_path, comparison, dtype):
    path = tmp_path / "year=0" / "data.parquet"
    path.parent.mkdir()
    pq.write_table(pa.table({"id": [10, 11]}), path)
    fragment = next(pads.dataset(str(path)).get_fragments())
    schema = pa.schema([("id", pa.int64()), ("year", dtype)])
    reader = ParquetFileReader(
        schema=schema,
        partitioning=Partitioning(
            PartitionStyle.HIVE, base_dir=str(tmp_path), field_names=["year"]
        ),
    )
    lo, hi = -(2 ** (dtype.bit_width - 1)), 2 ** (dtype.bit_width - 1) - 1
    for literal in [lo, hi, lo - 1, hi + 1, True, 1.0, None, "0"]:
        expr = comparison(col("year"), literal) | (col("id") < 0)
        safe, hint = specialize_fragment(
            expr, reader._partition_parser, schema, fragment, None
        )
        assert safe == (type(literal) is int and lo <= literal <= hi)
        if not safe:
            assert hint is None


@pytest.mark.parametrize(
    "kind",
    [
        "string",
        "string_numeric_literal",
        "uint",
        "float",
        "date",
        "dictionary",
        "dictionary_payload_cast",
        "bad_text",
        "overflow_text",
        "null",
        "missing",
        "collision",
        "physical_cast",
        "projected",
        "constant_left",
        "data_constant_left",
        "arithmetic",
        "not_equal",
        "nested",
    ],
)
def test_integer_partition_ranges_decline_unsafe(tmp_path, kind):
    from ray.data.expressions import BinaryExpr, Operation, lit

    path = tmp_path / "year=0" / "data.parquet"
    path.parent.mkdir()
    table = pa.table({"id": [10, 11]})
    schema = pa.schema([("id", pa.int64()), ("year", pa.int8())])
    if kind == "collision":
        table = table.append_column("year", pa.array([0, 0], type=pa.int8()))
    if kind == "dictionary_payload_cast":
        table = table.append_column("payload", pa.array(["a", "b"]))
        schema = schema.append(
            pa.field("payload", pa.dictionary(pa.int8(), pa.string()))
        )
    pq.write_table(table, path)
    fragment = next(pads.dataset(str(path)).get_fragments())
    types = {
        "string": pa.string(),
        "string_numeric_literal": pa.string(),
        "uint": pa.uint8(),
        "float": pa.float64(),
        "date": pa.date32(),
        "dictionary": pa.dictionary(pa.int8(), pa.int64()),
    }
    if kind in types:
        schema = schema.set(1, pa.field("year", types[kind]))
    if kind == "physical_cast":
        schema = schema.set(0, pa.field("id", pa.int32()))
    reader = ParquetFileReader(
        schema=schema,
        partitioning=Partitioning(
            PartitionStyle.HIVE, base_dir=str(tmp_path), field_names=["year"]
        ),
    )
    parser = reader._partition_parser
    if kind in ["null", "missing", "bad_text", "overflow_text"]:
        parser = MagicMock()
        parser.scheme = SimpleNamespace(field_names=["year"], style="hive")
        parser.return_value = {
            "null": {"year": None},
            "missing": {},
            "bad_text": {"year": "bad"},
            "overflow_text": {"year": "128"},
        }[kind]
    path_expr = col("year") > ("0" if kind == "string" else 0)
    data_expr = col("id") < 0
    if kind == "constant_left":
        path_expr = BinaryExpr(Operation.LT, lit(0), col("year"))
    if kind == "data_constant_left":
        data_expr = BinaryExpr(Operation.GT, lit(0), col("id"))
    if kind == "arithmetic":
        path_expr = col("year") + 1 > 0
    if kind == "not_equal":
        path_expr = col("year") != 0
    if kind == "nested":
        path_expr = path_expr & (col("year") < 2)
    expr = path_expr | data_expr
    assert specialize_fragment(
        expr, parser, schema, fragment, ["id"] if kind == "projected" else None
    ) == (False, None)


@pytest.mark.parametrize("hint_first", [False, True])
@pytest.mark.parametrize(
    "comparison", [operator.lt, operator.le, operator.ge, operator.gt]
)
def test_integer_partition_ranges_keep_existing_predicate_gate(
    fixture, hint_first, comparison
):
    schema, files, rows, manifest, partitioning = fixture
    schema = schema.set(3, pa.field("year", pa.int64()))
    hint = comparison(col("year"), 2021) | (col("id") < 5)
    existing = col("id") >= 2
    scanner = ParquetScanner(schema=schema, partitioning=partitioning)
    if hint_first:
        scanner = replace(scanner, mixed_pruning_predicate=hint)
        scanner, _ = scanner.push_filters(existing)
        assert isinstance(scanner, ParquetScanner)
    else:
        scanner, _ = scanner.push_filters(existing)
        assert isinstance(scanner, ParquetScanner)
        scanner = replace(scanner, mixed_pruning_predicate=hint)
    reader = scanner.create_reader()
    assert isinstance(reader, ParquetFileReader)
    actual = pa.concat_tables(list(reader.read(manifest))).filter(hint.to_pyarrow())
    expected = (
        pa.Table.from_pylist(rows).cast(schema).filter((hint & existing).to_pyarrow())
    )
    assert actual.to_pylist() == expected.to_pylist()
    assert not reader._mixed_subset_enabled


@pytest.mark.parametrize("kind", ["cast", "synthesized", "limit"])
def test_integer_partition_ranges_other_gates(fixture, kind):
    from ray.data.datatype import DataType

    schema, files, rows, manifest, partitioning = fixture
    schema = schema.set(3, pa.field("year", pa.int64()))
    path_expr = col("year")
    if kind == "cast":
        path_expr = path_expr.cast(DataType.from_arrow(pa.int64()))
    expr = (path_expr > 2021) | (col("id") < 5)
    kwargs: dict[str, Any] = {}
    if kind == "synthesized":
        kwargs["synthesized_columns"] = (PathColumn(),)
    if kind == "limit":
        kwargs["limit"] = 1
    reader = ObservedReader(
        schema=schema, partitioning=partitioning, mixed_pruning_predicate=expr, **kwargs
    )
    list(reader.read(manifest))
    assert not reader._mixed_subset_enabled
