"""Conservative ORC statistics and predicate unit tests."""

from typing import Any, cast
from unittest.mock import Mock

import pyarrow as pa
import pyarrow.orc as orc
import pytest

from ray.data._internal.datasource_v2.formats.orc.orc_batch_size_estimator import (
    _file_tail_message_type,
)
from ray.data._internal.datasource_v2.formats.orc.orc_metadata import (
    ColumnStatistics,
    _statistics,
)
from ray.data._internal.datasource_v2.formats.orc.orc_predicate import OrcPredicate
from ray.data._internal.datasource_v2.formats.orc.orc_scanner import OrcScanner
from ray.data._internal.object_extensions.arrow import ArrowPythonObjectType
from ray.data.context import DataContext
from ray.data.expressions import col, lit


def Footer():
    return cast(Any, _file_tail_message_type()()).footer


def test_contradictory_boolean_bucket_invalidates_count():
    footer = Footer()
    raw = footer.statistics.add(number_of_values=2, has_null=False)
    raw.bucket_statistics.count.extend([0, 3])
    valid = footer.statistics.add(number_of_values=2, has_null=False)
    valid.int_statistics.minimum, valid.int_statistics.maximum = 0, 1
    stats = _statistics(footer.statistics, 2, {"flag": (0, 0), "x": (1, 4)}, False)
    assert stats == {"x": ColumnStatistics(4, 2, 0, 1)}


@pytest.mark.parametrize(
    "values",
    [
        pytest.param([1, True], id="boolean"),
        pytest.param([1 << 64], id="out-of-range"),
        pytest.param(list(range(1025)), id="over-budget"),
    ],
)
def test_unsafe_or_over_budget_integer_sets_are_unknown(values):
    schema = pa.schema([("x", pa.int64())])
    for expr in (col("x").is_in(values), ~col("x").is_in(values)):
        bound = OrcPredicate(expr, schema, schema)
        assert not bound.eligible
        assert bound.can_match({"x": ColumnStatistics(4, 1, 1, 1)}, 2)


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


@pytest.mark.parametrize(
    "kind", [pytest.param(16, id="varchar"), pytest.param(17, id="char")]
)
def test_char_varchar_null_proof_does_not_enable_ranges(kind):
    schema = pa.schema([("x", pa.string())])
    stats = {"x": ColumnStatistics(kind, 0)}
    assert not OrcPredicate(col("x").is_not_null(), schema, schema).can_match(stats, 5)
    assert OrcPredicate(col("x") < "z", schema, schema).can_match(stats, 5)


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


def test_statistics_pruning_configuration(restore_data_context):
    context = restore_data_context
    assert DataContext().orc_statistics_pruning == "off"
    context.orc_statistics_pruning = "off"
    with pytest.raises(ValueError, match="orc_statistics_pruning"):
        context.orc_statistics_pruning = "stripe"
    with pytest.raises(ValueError, match="orc_statistics_pruning"):
        OrcScanner(schema=pa.schema([("x", pa.int64())]), statistics_pruning="stripe")


def test_statistics_pruning_checks_pickle_columns_before_skipping():
    schema = pa.schema([("x", pa.int64()), ("object", ArrowPythonObjectType())])
    sink = pa.BufferOutputStream()
    orc.write_table(pa.table({"x": [1], "object": [b"payload"]}), sink)
    data = sink.getvalue()

    class Filesystem:
        def open_input_file(self, path):
            return pa.BufferReader(data)

    class Fragment:
        physical_schema = schema
        path = "data.orc"
        filesystem = Filesystem()

        def scanner(self, **kwargs):
            return object()

    predicate = col("x") > 10
    reader = OrcScanner(
        schema=schema, predicate=predicate, statistics_pruning="file"
    ).create_reader()
    with pytest.raises(ValueError, match="arrow_pickled_object"):
        list(
            reader._scan_with_statistics(
                Fragment(), {"columns": None}, schema, predicate
            )
        )


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


def test_invalid_utf8_string_bounds_are_unknown():
    bounds = Mock(minimum=b"\xff", maximum="z")
    bounds.HasField.side_effect = lambda name: name in ("minimum", "maximum")
    stat = Mock(number_of_values=1, has_null=False, string_statistics=bounds)
    stat.HasField.side_effect = lambda name: name in (
        "number_of_values",
        "has_null",
        "string_statistics",
    )
    assert _statistics([stat], 1, {"x": (0, 7)}, True) == {"x": ColumnStatistics(7, 1)}


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
