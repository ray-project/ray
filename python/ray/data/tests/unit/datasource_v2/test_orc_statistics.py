"""File statistics pruning and conservative predicate tests."""

import copy
import itertools
import pickle
import random
from dataclasses import replace
from pathlib import Path
from typing import Any, Generator, cast

import pyarrow as pa
import pyarrow.dataset as pds
import pyarrow.orc as orc
import pytest

from ray.data._internal.datasource_v2.formats.orc import (
    orc_file_reader as reader_module,
    orc_metadata as metadata_module,
)
from ray.data._internal.datasource_v2.formats.orc.orc_batch_size_estimator import (
    _file_tail_message_type,
)
from ray.data._internal.datasource_v2.formats.orc.orc_metadata import (
    ColumnStatistics,
    MetadataUnavailable,
    _statistics,
    read_metadata,
)
from ray.data._internal.datasource_v2.formats.orc.orc_predicate import OrcPredicate
from ray.data._internal.datasource_v2.formats.orc.orc_scanner import OrcScanner
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data.context import DataContext
from ray.data.datasource.partitioning import Partitioning, PartitionStyle
from ray.data.expressions import col, lit

CODECS = ["uncompressed", "zlib", "snappy", "lz4", "zstd"]


def Footer():
    return cast(Any, _file_tail_message_type()()).footer


def _manifest(path):
    return FileManifest.construct_manifest(
        paths=[str(path)], sizes=[path.stat().st_size], chunk_metadatas=[None]
    )


def _write(path, table, codec="uncompressed"):
    orc.write_table(
        table,
        str(path),
        compression=codec,
        compression_strategy="compression",
        stripe_size=8192,
        batch_size=256,
        compression_block_size=65536,
    )


def _rows(scanner, path):
    return [
        row
        for batch in scanner.create_reader().read(_manifest(path))
        for row in batch.to_pylist()
    ]


def _integer_stats(values):
    valid = [value for value in values if value is not None]
    return {
        "x": ColumnStatistics(
            4, len(valid), min(valid) if valid else None, max(valid) if valid else None
        )
    }


def test_possible_values_against_arrow_oracle():
    schema = pa.schema([("x", pa.int64())])
    leaves = [col("x").is_null(), col("x").is_not_null()]
    for value in (-3, 0, 3, None):
        leaves.extend(
            [
                col("x") == value,
                col("x") != value,
                col("x") < value,
                col("x") <= value,
                col("x") > value,
                col("x") >= value,
                lit(value) < col("x"),
            ]
        )
    leaves.extend(col("x").is_in(values) for values in ([], [None], [0], [-3, 3, None]))
    expressions = leaves + [~leaf for leaf in leaves]
    expressions += [a & b for a, b in itertools.product(leaves[::3], leaves[::5])]
    expressions += [~(a | b) for a, b in itertools.product(leaves[::5], leaves[::3])]
    # An unsupported arithmetic leaf remains unknown even under NOT.
    expressions += [~((col("x") + 1) > 5), ((col("x") + 1) > 5) | (col("x") == 0)]
    rng = random.Random(66995)
    cases = [[None], [0], [-3, None, 3], []]
    cases += [
        [rng.choice([None, -3, -1, 0, 2, 3]) for _ in range(rng.randrange(1, 16))]
        for _ in range(40)
    ]
    for values, expr in itertools.product(cases, expressions):
        table = pa.table({"x": pa.array(values, type=pa.int64())})
        bound = OrcPredicate(expr, schema, schema)
        actual = pds.dataset(table).to_table(filter=expr.to_pyarrow()).num_rows
        assert not actual or bound.can_match(_integer_stats(values), len(values)), repr(
            expr
        )
    for expr in [col("x") > 3, col("x") == 3, ~col("x").is_in([0, None])]:
        assert not OrcPredicate(expr, schema, schema).can_match(
            _integer_stats([0, None]), 2
        )


def test_mode_capture_validation_and_serialization(monkeypatch):
    context = DataContext.get_current()
    monkeypatch.setattr(context, "orc_statistics_pruning", "file")
    scanner = OrcScanner(schema=pa.schema([("x", pa.int64())]))
    monkeypatch.setattr(context, "orc_statistics_pruning", "off")
    for copy_scanner in (
        copy.deepcopy(scanner),
        pickle.loads(pickle.dumps(scanner)),
        scanner.push_limit(3),
    ):
        assert isinstance(copy_scanner, OrcScanner)
        assert copy_scanner.statistics_pruning == "file"
        assert copy_scanner.create_reader()._statistics_pruning == "file"
    with pytest.raises(ValueError):
        context.orc_statistics_pruning = "invalid"
    with pytest.raises(ValueError):
        replace(scanner, statistics_pruning="invalid")


@pytest.mark.parametrize("mode", ["off", "file"])
def test_statistics_and_batch_sizing_config_survive_pushdowns(
    tmp_path, monkeypatch, mode
):
    from ray.data._internal.datasource_v2.formats.orc.orc_datasource_v2 import (
        OrcDatasourceV2,
    )

    table = pa.table({"x": range(16), "payload": ["value"] * 16})
    path = tmp_path / "data.orc"
    _write(path, table)
    context = DataContext.get_current()
    monkeypatch.setattr(context, "orc_statistics_pruning", mode)
    monkeypatch.setattr(context, "target_max_block_size", 4096)
    scanner = OrcDatasourceV2([str(path)]).create_scanner(table.schema)
    monkeypatch.setattr(context, "orc_statistics_pruning", "off")
    monkeypatch.setattr(context, "target_max_block_size", 1)
    pushed, residual = scanner.push_filters(col("x") >= 8)
    assert residual is None
    pushed = pushed.prune_columns(["payload"]).push_limit(3)
    for copied in (
        pushed,
        copy.deepcopy(pushed),
        pickle.loads(pickle.dumps(pushed)),
    ):
        assert isinstance(copied, OrcScanner)
        reader = copied.create_reader()
        assert copied.statistics_pruning == reader._statistics_pruning == mode
        assert copied.target_block_size == reader._target_block_size == 4096
        assert reader._explicit_batch_size is None
        assert pa.concat_tables(list(reader.read(_manifest(path)))).equals(
            table.slice(8, 3).select(["payload"])
        )


@pytest.mark.parametrize("mode", ["off", "file"])
def test_no_eligible_predicate_has_no_extra_statistics(tmp_path, monkeypatch, mode):
    path = tmp_path / "data.orc"
    table = pa.table({"x": [1, 2, 3]})
    _write(path, table)

    def fail(*args):
        pytest.fail("Unexpected statistics I/O")

    monkeypatch.setattr(reader_module, "read_metadata", fail)
    if mode == "off":
        monkeypatch.setattr(reader_module, "OrcPredicate", fail)
    predicate = (col("x") > 10) if mode == "off" else ((col("x") + 1) > 2)
    scanner = OrcScanner(
        schema=table.schema, predicate=predicate, statistics_pruning=mode
    )
    assert _rows(scanner, path) == table.filter(predicate.to_pyarrow()).to_pylist()


@pytest.mark.parametrize("mode", ["file"])
def test_unavailable_metadata_falls_back_but_io_errors_propagate(
    tmp_path, monkeypatch, mode
):
    path = tmp_path / "data.orc"
    table = pa.table({"x": [1, 2, 3]})
    _write(path, table)
    scanner = OrcScanner(
        schema=table.schema, predicate=col("x") > 10, statistics_pruning=mode
    )

    def unavailable(source):
        raise MetadataUnavailable("unknown writer")

    monkeypatch.setattr(reader_module, "read_metadata", unavailable)
    assert _rows(scanner, path) == []

    def broken(source):
        raise OSError("object store connection failed")

    monkeypatch.setattr(reader_module, "read_metadata", broken)
    with pytest.raises(OSError, match="object store connection failed"):
        _rows(scanner, path)


@pytest.mark.parametrize("mode", ["off", "file"])
@pytest.mark.parametrize("style", [PartitionStyle.HIVE, PartitionStyle.DIRECTORY])
def test_logical_partition_pruning_and_statistics(tmp_path, monkeypatch, mode, style):
    monkeypatch.setattr(
        DataContext.get_current().execution_options, "preserve_order", True
    )
    rng = random.Random(66995)
    table = pa.table(
        {
            "x": range(4096),
            "payload": [rng.randbytes(128) for _ in range(4096)],
            "year": pa.array([1900] * 4096, type=pa.int32()),
        }
    )
    paths = []
    for year, name, values in (
        (2, "partition-excluded", range(4096)),
        (10, "statistics-excluded", range(-4096, 0)),
        (10, "candidate", range(4096)),
    ):
        parts = (
            [f"year={year}", "unused=not-an-int"]
            if style == PartitionStyle.HIVE
            else [str(year), "not-an-int"]
        )
        directory = tmp_path.joinpath(*parts)
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"{name}.orc"
        _write(path, table.set_column(0, "x", pa.array(values)), "zlib")
        paths.append(str(path))
    schema = table.schema.append(pa.field("unused", pa.int32()))
    partitioning = Partitioning(
        style, base_dir=str(tmp_path), field_names=["year", "unused"]
    )
    scanner = OrcScanner(
        schema=schema,
        partitioning=partitioning,
        statistics_pruning=mode,
        batch_size=37,
    ).prune_partitions(col("year") > 2)
    scanner, residual = scanner.push_filters(col("x") >= 3500)
    assert residual is None
    scanner = scanner.prune_columns(["payload"]).push_limit(73)
    assert isinstance(scanner, OrcScanner)
    manifest = FileManifest.construct_manifest(
        paths=paths,
        sizes=[Path(path).stat().st_size for path in paths],
        chunk_metadatas=[None] * len(paths),
    )
    pruner = scanner.pushed_partition_pruner()
    assert pruner is not None
    assert [path for path in paths if pruner.should_include(path)] == paths[1:]
    pruned = scanner.prune_input_split(manifest)
    assert pruned.paths.tolist() == paths[1:]
    assert partitioning.field_types == {}

    streamed = []
    native_scan = reader_module.OrcFileReader._scan_with_statistics

    def scan(self, fragment, *args, **kwargs):
        class Fragment:
            def __getattr__(self, name):
                return getattr(fragment, name)

            def scanner(self, **scan_kwargs):
                native = fragment.scanner(**scan_kwargs)

                class Scanner:
                    def scan_batches(self):
                        streamed.append(fragment.path)
                        yield from native.scan_batches()

                return Scanner()

        yield from native_scan(self, Fragment(), *args, **kwargs)

    monkeypatch.setattr(reader_module.OrcFileReader, "_scan_with_statistics", scan)
    batches = list(scanner.create_reader().read(pruned))
    assert pa.concat_tables(batches).equals(table.slice(3500, 73).select(["payload"]))
    assert all(batch.num_rows <= 37 for batch in batches)
    assert sorted(streamed) == sorted(paths[1:] if mode == "off" else paths[2:])


def test_unsafe_output_cast_is_not_hidden_by_pruning(tmp_path):
    path = tmp_path / "data.orc"
    table = pa.table({"x": [0], "text": ["not-an-integer"]})
    _write(path, table)
    schema = pa.schema([("x", pa.int64()), ("text", pa.int64())])
    scanner = OrcScanner(schema=schema, predicate=col("x") > 10)

    # Arrow may filter the bad row before its projection cast. In either case,
    # the optimization must agree with the unmodified streaming path.
    def outcome(mode):
        try:
            return _rows(replace(scanner, statistics_pruning=mode), path)
        except pa.ArrowException as exc:
            return type(exc), str(exc)

    assert outcome("file") == outcome("off")


@pytest.mark.parametrize(
    "count,counts",
    [(0, [1]), (1, [2]), (2, [3]), (0, [1, 0]), (0, [0, 1])],
    ids=["zero", "one", "two", "multi-first", "multi-second"],
)
def test_contradictory_boolean_bucket_invalidates_count(count, counts):
    footer = Footer()
    raw = footer.statistics.add(number_of_values=count, has_null=count < 2)
    raw.bucket_statistics.count.extend(counts)
    assert _statistics(footer.statistics, 2, {"flag": (0, 0)}, False) == {}
    valid = footer.statistics.add(number_of_values=2, has_null=False)
    valid.int_statistics.minimum, valid.int_statistics.maximum = 0, 1
    stats = _statistics(footer.statistics, 2, {"flag": (0, 0), "x": (1, 4)}, False)
    assert stats == {"x": ColumnStatistics(4, 2, 0, 1)}


@pytest.mark.parametrize(
    "values,predicate",
    [
        ([False, False], col("x") == lit(True)),
        ([True, True], col("x") != lit(True)),
        ([1, 2], col("x").is_null()),
        ([None, None], col("x").is_not_null()),
        ([1, 2], ~(col("x") >= 0)),
        ([1, 2], col("x").is_in([3, None])),
        ([1, 1, None], ~col("x").is_in([1, None])),
    ],
)
def test_extended_predicates_exclude_whole_file(tmp_path, values, predicate):
    path = tmp_path / "data.orc"
    value_type = (
        pa.bool_() if any(type(value) is bool for value in values) else pa.int64()
    )
    table = pa.table({"x": pa.array(values, type=value_type)})
    _write(path, table, "zlib")
    fragment = next(pds.dataset(str(path), format="orc").get_fragments())

    class Fragment:
        def __getattr__(self, name):
            return getattr(fragment, name)

        def scanner(self, **kwargs):
            fragment.scanner(**kwargs)

            class Scanner:
                def scan_batches(self):
                    pytest.fail("Statistics exclusion decoded data")

            return Scanner()

    assert table.filter(predicate.to_pyarrow()).num_rows == 0
    for mode in ("file",):
        reader = OrcScanner(
            schema=table.schema, predicate=predicate, statistics_pruning=mode
        ).create_reader()
        assert (
            list(
                reader._iter_fragment_tables(
                    Fragment(),
                    {
                        "columns": None,
                        "filter": predicate.to_pyarrow(),
                        "batch_size": 2,
                    },
                )
            )
            == []
        )


@pytest.mark.parametrize(
    "data_type", [pa.float64(), pa.date32(), pa.timestamp("ns"), pa.decimal128(12, 2)]
)
def test_null_counts_do_not_require_range_support(tmp_path, data_type):
    path = tmp_path / "nulls.orc"
    table = pa.table({"x": pa.nulls(3, type=data_type)})
    _write(path, table, "zlib")
    with pa.OSFile(str(path), "rb") as source:
        metadata = read_metadata(source)
    predicate = OrcPredicate(col("x").is_not_null(), table.schema, table.schema)
    assert not predicate.can_match(metadata.statistics, 3)


@pytest.mark.parametrize(
    "values", [[1, True], [1.5], ["1"], [1 << 64], list(range(1025))]
)
def test_unsafe_or_over_budget_integer_sets_are_unknown(values):
    schema = pa.schema([("x", pa.int64())])
    for expr in (col("x").is_in(values), ~col("x").is_in(values)):
        bound = OrcPredicate(expr, schema, schema)
        assert not bound.eligible
        assert bound.can_match(_integer_stats([1, None]), 2)


def test_independent_no_null_flag_and_contradictions():
    footer = Footer()
    raw = footer.statistics.add(has_null=False)
    schema = pa.schema([("x", pa.int64())])
    stats = _statistics(footer.statistics, 5, {"x": (0, 4)}, False)
    assert stats["x"].values == 5
    assert not OrcPredicate(col("x").is_null(), schema, schema).can_match(stats, 5)
    raw.has_null = True
    assert _statistics(footer.statistics, 5, {"x": (0, 4)}, False) == {}
    raw.number_of_values = 5
    assert _statistics(footer.statistics, 5, {"x": (0, 4)}, False) == {}


@pytest.mark.parametrize("kind", [16, 17])
def test_char_varchar_null_proof_does_not_enable_ranges(kind):
    schema = pa.schema([("x", pa.string())])
    stats = {"x": ColumnStatistics(kind, 0)}
    assert not OrcPredicate(col("x").is_not_null(), schema, schema).can_match(stats, 5)
    assert OrcPredicate(col("x") < "z", schema, schema).can_match(stats, 5)


def test_nan_is_not_null_and_float_ranges_stay_unknown(tmp_path):
    path = tmp_path / "float.orc"
    table = pa.table({"x": [float("nan"), float("inf")]})
    _write(path, table, "zlib")
    with pa.OSFile(str(path), "rb") as source:
        stats = read_metadata(source).statistics
    assert not OrcPredicate(col("x").is_null(), table.schema, table.schema).can_match(
        stats, 2
    )
    assert not OrcPredicate(col("x") < 0.0, table.schema, table.schema).eligible


def test_string_comparisons_and_integer_limits_against_arrow():
    for values, data_type, kind, literals in [
        (
            ["", "a\x00z", "é", "中", "😀", None],
            pa.string(),
            7,
            ["", "a\x00b", "é", "😀", "😁"],
        ),
        (
            [-(1 << 63), -1, (1 << 63) - 1, None],
            pa.int64(),
            4,
            [-(1 << 63), 0, (1 << 63) - 1],
        ),
    ]:
        table = pa.table({"x": pa.array(values, type=data_type)})
        valid = [value for value in values if value is not None]
        stats = {"x": ColumnStatistics(kind, len(valid), min(valid), max(valid))}
        for value in literals:
            expressions = [
                col("x") == value,
                col("x") != value,
                col("x") < value,
                col("x") <= value,
                col("x") > value,
                col("x") >= value,
                lit(value) < col("x"),
                lit(value) >= col("x"),
            ]
            for expr in expressions + [~expr for expr in expressions]:
                actual = table.filter(expr.to_pyarrow()).num_rows
                assert not actual or OrcPredicate(
                    expr, table.schema, table.schema
                ).can_match(stats, len(values))


@pytest.mark.parametrize("codec", CODECS)
def test_serialized_footer_statistics_and_file_exclusion(tmp_path, codec):
    path = tmp_path / "values.orc"
    table = pa.table({"x": [1, 2, 3], "flag": [True, False, None]})
    _write(path, table, codec)
    with pa.OSFile(str(path), "rb") as source:
        metadata = read_metadata(source)
    assert metadata.rows == 3
    assert (metadata.statistics["x"].minimum, metadata.statistics["x"].maximum) == (
        1,
        3,
    )
    assert metadata.statistics["flag"].true_count == 1
    predicate = col("x") > 10
    fragment = next(pds.dataset(str(path), format="orc").get_fragments())

    class GuardFragment:
        def __getattr__(self, name):
            return getattr(fragment, name)

        def scanner(self, **kwargs):
            fragment.scanner(**kwargs)

            class Scanner:
                def scan_batches(self):
                    pytest.fail("Excluded file entered the streaming decoder")

            return Scanner()

    reader = OrcScanner(
        schema=table.schema, predicate=predicate, statistics_pruning="file"
    ).create_reader()
    assert (
        list(
            reader._iter_fragment_tables(
                GuardFragment(),
                {"columns": None, "filter": predicate.to_pyarrow(), "batch_size": 2},
            )
        )
        == []
    )


def test_qualified_java_and_unqualified_cpp_string_statistics(tmp_path):
    path = Path(__file__).parent / "fixtures" / "orc_java_strings.orc"
    table = orc.ORCFile(str(path)).read()
    with pa.OSFile(str(path), "rb") as source:
        stats = read_metadata(source).statistics
    assert stats["s"].minimum == "a"
    assert stats["s"].maximum == "😁"
    assert not OrcPredicate(col("s") < "a", table.schema, table.schema).can_match(
        stats, table.num_rows
    )
    for predicate in (col("s") < "a", col("s") == "a\x00z", ~(col("s") < "b")):
        scanner = OrcScanner(
            schema=table.schema, predicate=predicate, statistics_pruning="file"
        )
        assert _rows(scanner, path) == table.filter(predicate.to_pyarrow()).to_pylist()
    cpp = tmp_path / "cpp.orc"
    cpp_table = pa.table({"s": ["a\x00z", "a\x00b", None]})
    _write(cpp, cpp_table, "zlib")
    with pa.OSFile(str(cpp), "rb") as source:
        stats = read_metadata(source).statistics
    assert stats["s"].minimum is None
    assert stats["s"].values == 2


@pytest.mark.parametrize("change", ["missing", "broken", "writer", "count", "boolean"])
def test_serialized_footer_unavailable_or_contradictory_retains_rows(
    tmp_path, monkeypatch, change
):
    path = tmp_path / "values.orc"
    table = pa.table({"flag": [True, None], "x": [1, 2]})
    _write(path, table)
    original = orc.ORCFile

    class TailReader:
        def __init__(self, native):
            self.native = native

        def serialized_file_tail(self):
            if change == "missing":
                raise NotImplementedError
            if change == "broken":
                return b"\xff"
            tail = cast(
                Any,
                _file_tail_message_type().FromString(
                    self.native.serialized_file_tail()
                ),
            )
            if change == "writer":
                tail.footer.writer = 42
            elif change == "count":
                tail.footer.number_of_rows += 1
            else:
                tail.footer.statistics[1].number_of_values = 0
            return tail.SerializeToString()

    class WrappedFile:
        def __init__(self, source):
            self.native = original(source)
            self.reader = TailReader(self.native.reader)
            self.nrows = self.native.nrows
            self.nstripes = self.native.nstripes

    monkeypatch.setattr(metadata_module.orc, "ORCFile", WrappedFile)
    predicate = col("flag") == lit(True)
    scanner = OrcScanner(
        schema=table.schema, predicate=predicate, statistics_pruning="file"
    )
    assert _rows(scanner, path) == [{"flag": True, "x": 1}]


def test_default_and_rejected_stripe_mode(monkeypatch):
    context = DataContext.get_current()
    assert type(context).__dataclass_fields__["orc_statistics_pruning"].default == "off"
    monkeypatch.setattr(context, "orc_statistics_pruning", "off")
    with pytest.raises(ValueError):
        context.orc_statistics_pruning = "stripe"
    with pytest.raises(ValueError):
        OrcScanner(schema=pa.schema([("x", pa.int64())]), statistics_pruning="stripe")


def test_presence_counts_bounds_and_writer():
    footer = Footer()
    raw = footer.statistics.add()
    columns = {"x": (0, 4)}
    assert _statistics(footer.statistics, 5, columns, False) == {}
    raw.number_of_values = 0
    assert _statistics(footer.statistics, 5, columns, False)["x"].values == 0
    raw.has_null = True
    assert _statistics(footer.statistics, 5, columns, False)["x"].values == 0
    raw.number_of_values = 6
    assert _statistics(footer.statistics, 5, columns, False) == {}
    raw.number_of_values, raw.has_null = 5, False
    raw.int_statistics.minimum = 0
    assert _statistics(footer.statistics, 5, columns, False)["x"].minimum is None
    raw.int_statistics.maximum = -1
    assert _statistics(footer.statistics, 5, columns, False)["x"].minimum is None
    raw.string_statistics.minimum, raw.string_statistics.maximum = "a", "z"
    assert _statistics(footer.statistics, 5, {"x": (0, 7)}, False)["x"].minimum is None
    assert _statistics(footer.statistics, 5, {"x": (0, 7)}, True)["x"].minimum == "a"
    raw.string_statistics.lower_bound = "a"
    assert _statistics(footer.statistics, 5, {"x": (0, 7)}, True)["x"].minimum is None
    # Presence survives protobuf serialization, including explicitly zero counts.
    parsed = cast(Any, _file_tail_message_type()()).footer
    parsed.ParseFromString(footer.SerializeToString())
    assert parsed.statistics[0].HasField("number_of_values")
    assert not parsed.statistics[0].HasField("bucket_statistics")


@pytest.mark.parametrize(
    "operation", ["success", "fallback", "init_failure", "early_close", "excluded"]
)
def test_statistics_handle_lifetime(tmp_path, monkeypatch, operation):
    path = tmp_path / "data.orc"
    table = pa.table({"x": range(4096), "payload": [b"p" * 128] * 4096})
    _write(path, table)
    fragment = next(pds.dataset(str(path), format="orc").get_fragments())
    opened = []

    class Filesystem:
        def open_input_file(self, path):
            source = fragment.filesystem.open_input_file(path)
            opened.append(source)
            return source

    class Fragment:
        filesystem = Filesystem()

        def __getattr__(self, name):
            return getattr(fragment, name)

    predicate = col("x") > (5000 if operation == "excluded" else 3500)
    reader = OrcScanner(
        schema=table.schema,
        predicate=predicate,
        statistics_pruning="file",
        batch_size=17,
    ).create_reader()
    if operation == "fallback":

        def unavailable(source):
            raise MetadataUnavailable("test fallback")

        monkeypatch.setattr(reader_module, "read_metadata", unavailable)
    elif operation == "init_failure":

        def failed(source):
            raise OSError("test decoder initialization")

        monkeypatch.setattr(orc, "ORCFile", failed)
    iterator = reader._iter_fragment_tables(
        Fragment(),
        {"columns": None, "filter": predicate.to_pyarrow(), "batch_size": 17},
    )
    if operation == "early_close":
        next(iterator)
        cast(Generator[pa.Table, None, None], iterator).close()
    elif operation == "init_failure":
        with pytest.raises(OSError, match="test decoder initialization"):
            list(iterator)
    else:
        list(iterator)
    assert opened and all(source.closed for source in opened)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
