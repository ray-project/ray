"""ORC file statistics and reader integration tests."""

import itertools
import random
from dataclasses import replace
from functools import partial
from pathlib import Path
from typing import Any, cast

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
    read_metadata,
)
from ray.data._internal.datasource_v2.formats.orc.orc_predicate import OrcPredicate
from ray.data._internal.datasource_v2.formats.orc.orc_scanner import OrcScanner
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data.expressions import col, lit

CODECS = ["uncompressed", "zlib", "snappy", "lz4", "zstd"]


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
    expressions += [
        col("x").not_in(values) for values in ([], [None], [0], [-3, 3, None])
    ]
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
    for expr in [
        col("x") > 3,
        col("x") == 3,
        ~col("x").is_in([0, None]),
        col("x").not_in([0, None]),
    ]:
        assert not OrcPredicate(expr, schema, schema).can_match(
            _integer_stats([0, None]), 2
        )


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


def _assert_statistics_exclude_file(path, schema, predicate):
    with pa.OSFile(str(path), "rb") as source:
        metadata = read_metadata(source)
    assert not OrcPredicate(predicate, schema, schema).can_match(
        metadata.statistics, metadata.rows
    )


@pytest.mark.parametrize(
    "mode,predicate,expected",
    [
        pytest.param("off", col("x") > 10, [], id="disabled"),
        pytest.param("file", None, [{"x": 1}, {"x": 2}, {"x": 3}], id="no-filter"),
        pytest.param(
            "file", (col("x") + 1) > 2, [{"x": 2}, {"x": 3}], id="unsupported"
        ),
    ],
)
def test_no_pruning_has_no_extra_statistics(
    tmp_path, monkeypatch, mode, predicate, expected
):
    path = tmp_path / "data.orc"
    table = pa.table({"x": [1, 2, 3]})
    _write(path, table)

    def fail(*args):
        pytest.fail("Unexpected statistics work")

    monkeypatch.setattr(reader_module, "read_metadata", fail)
    scanner = OrcScanner(
        schema=table.schema, predicate=predicate, statistics_pruning=mode
    )
    assert _rows(scanner, path) == expected


def test_unavailable_metadata_falls_back_but_io_errors_propagate(tmp_path, monkeypatch):
    path = tmp_path / "data.orc"
    table = pa.table({"x": [1, 2, 3]})
    _write(path, table)
    scanner = OrcScanner(
        schema=table.schema, predicate=col("x") > 1, statistics_pruning="file"
    )

    sources = []

    def unavailable(source):
        sources.append(source)
        raise MetadataUnavailable("unknown writer")

    monkeypatch.setattr(reader_module, "read_metadata", unavailable)
    assert _rows(scanner, path) == [{"x": 2}, {"x": 3}]
    assert sources[-1].closed

    def broken_orc_file(source):
        raise OSError("object store connection failed")

    def read_metadata_and_record(source):
        sources.append(source)
        return read_metadata(source)

    monkeypatch.setattr(reader_module, "read_metadata", read_metadata_and_record)
    monkeypatch.setattr(metadata_module.orc, "ORCFile", broken_orc_file)
    with pytest.raises(OSError, match="object store connection failed"):
        _rows(scanner, path)
    assert sources[-1].closed


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
    "values,data_type,predicate",
    [
        pytest.param(
            [False, False], pa.bool_(), col("x") == lit(True), id="boolean-eq"
        ),
        pytest.param([True, True], pa.bool_(), col("x") != lit(True), id="boolean-ne"),
        pytest.param([1, 2], pa.int64(), col("x").is_null(), id="is-null"),
        pytest.param([1, 2], pa.int64(), col("x").is_in([3, None]), id="in"),
        pytest.param([None] * 3, pa.float64(), col("x").is_not_null(), id="float"),
        pytest.param([None] * 3, pa.date32(), col("x").is_not_null(), id="date"),
        pytest.param(
            [None] * 3, pa.timestamp("ns"), col("x").is_not_null(), id="timestamp"
        ),
        pytest.param(
            [None] * 3, pa.decimal128(12, 2), col("x").is_not_null(), id="decimal"
        ),
    ],
)
def test_extended_predicates_exclude_file_by_statistics(
    tmp_path, values, data_type, predicate
):
    path = tmp_path / "data.orc"
    table = pa.table({"x": pa.array(values, type=data_type)})
    _write(path, table, "zlib")
    _assert_statistics_exclude_file(path, table.schema, predicate)


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


@pytest.mark.parametrize("codec", [pytest.param(codec, id=codec) for codec in CODECS])
def test_serialized_footer_statistics_and_file_pruning(tmp_path, monkeypatch, codec):
    path = tmp_path / "values.orc"
    table = pa.table({"x": [1, 2, 3], "flag": [True, False, None]})
    _write(path, table, codec)
    native_orc_file = orc.ORCFile

    class MetadataOnlyFile:
        def __init__(self, source):
            self._file = native_orc_file(source)

        def __getattr__(self, name):
            return getattr(self._file, name)

        def read(self, *args, **kwargs):
            pytest.fail("Metadata inspection read data")

        def read_stripe(self, *args, **kwargs):
            pytest.fail("Metadata inspection read a stripe")

    monkeypatch.setattr(metadata_module.orc, "ORCFile", MetadataOnlyFile)
    with pa.OSFile(str(path), "rb") as source:
        metadata = read_metadata(source)
    assert metadata.rows == 3
    assert (metadata.statistics["x"].minimum, metadata.statistics["x"].maximum) == (
        1,
        3,
    )
    assert metadata.statistics["flag"].true_count == 1
    assert not OrcPredicate(col("x") > 10, table.schema, table.schema).can_match(
        metadata.statistics, metadata.rows
    )


def test_qualified_java_and_unqualified_cpp_string_statistics(tmp_path):
    path = (
        Path(__file__).parent.parent
        / "unit"
        / "datasource_v2"
        / "fixtures"
        / "orc_java_strings.orc"
    )
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


def _missing_tail(native):
    raise NotImplementedError


def _broken_tail(native):
    return b"\xff"


def _invalid_writer(footer):
    footer.writer = 42
    footer.statistics[1].bucket_statistics.count[0] = 0


def _invalid_count(footer):
    footer.number_of_rows += 1
    footer.statistics[1].bucket_statistics.count[0] = 0


def _invalid_boolean(footer):
    # The original true count now contradicts the zero non-NULL count.
    pass


def _altered_tail(native, mutate):
    tail = cast(
        Any,
        _file_tail_message_type().FromString(native.serialized_file_tail()),
    )
    # The physical file contains a True row, but these statistics say otherwise.
    tail.footer.statistics[1].number_of_values = 0
    mutate(tail.footer)
    return tail.SerializeToString()


@pytest.mark.parametrize(
    "serialized_tail",
    [
        pytest.param(_missing_tail, id="missing"),
        pytest.param(_broken_tail, id="broken"),
        pytest.param(partial(_altered_tail, mutate=_invalid_writer), id="writer"),
        pytest.param(partial(_altered_tail, mutate=_invalid_count), id="count"),
        pytest.param(partial(_altered_tail, mutate=_invalid_boolean), id="boolean"),
    ],
)
def test_serialized_footer_unavailable_or_contradictory_retains_rows(
    tmp_path, monkeypatch, serialized_tail
):
    path = tmp_path / "values.orc"
    table = pa.table({"flag": [True, None], "x": [1, 2]})
    _write(path, table)
    original = orc.ORCFile

    class TailReader:
        def __init__(self, native):
            self.native = native

        def serialized_file_tail(self):
            return serialized_tail(self.native)

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


def test_statistics_batch_sizing_and_handle_lifetime(tmp_path, monkeypatch):
    path = tmp_path / "data.orc"
    table = pa.table({"x": [1, 2, 3], "payload": ["wide" * 64] * 3})
    _write(path, table)
    sources = []

    def read_metadata_and_record(source):
        sources.append(source)
        return read_metadata(source)

    monkeypatch.setattr(reader_module, "read_metadata", read_metadata_and_record)
    scanner = OrcScanner(
        schema=table.schema, statistics_pruning="file", target_block_size=128
    )
    scanner, residual = scanner.push_filters(col("x") > 1)
    assert residual is None
    scanner = cast(OrcScanner, scanner.prune_columns(["x", "payload"]).push_limit(2))
    batches = list(scanner.create_reader().read(_manifest(path)))
    assert pa.concat_tables(batches).to_pylist() == table.slice(1).to_pylist()
    assert len(batches) > 1
    assert _rows(replace(scanner, predicate=col("x") > 3), path) == []
    assert len(sources) == 2 and all(source.closed for source in sources)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
