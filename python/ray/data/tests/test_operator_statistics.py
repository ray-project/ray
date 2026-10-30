import datetime
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ray.data._internal.cbo_stats.operator_statistics import (
    CONFIDENCE_DECAY_PER_OP,
    DEFAULT_EQ_SELECTIVITY,
    DEFAULT_RANGE_SELECTIVITY,
    DEFAULT_UNKNOWN_SELECTIVITY,
    ColumnStatistics,
    ConfidenceLevel,
    OperatorStatistics,
    estimate_selectivity_from_column_stats,
)
from ray.data._internal.cbo_stats.selectivity import estimate_predicate_selectivity
from ray.data._internal.datasource.parquet_datasource import ParquetDatasource
from ray.data._internal.datasource.parquet_stat_extractor import (
    extract_column_stats_from_parquet_metadata,
)
from ray.data._internal.datasource.range_datasource import RangeDatasource
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.logical.operators.all_to_all_operator import (
    Aggregate,
    RandomShuffle,
    Repartition,
)
from ray.data._internal.logical.operators.map_operator import (
    Filter,
    FlatMap,
    MapBatches,
    Project,
)
from ray.data._internal.logical.operators.n_ary_operator import Union
from ray.data._internal.logical.operators.one_to_one_operator import Limit
from ray.data._internal.logical.operators.read_operator import (
    Read,
    _parquet_footer_statistics,
)
from ray.data.expressions import col, lit, star

HIGH = ConfidenceLevel.HIGH.value
LOW = ConfidenceLevel.LOW.value


class _StatsSource(LogicalOperator):
    """Source operator returning fixed statistics."""

    def __init__(self, stats):
        # ``LogicalOperator`` is a frozen dataclass whose generated ``__init__``
        # takes no arguments, so the base fields are set directly.
        object.__setattr__(self, "_input_dependencies", [])
        object.__setattr__(self, "_name", "StatsSource")
        self._stats = stats

    @property
    def num_outputs(self):
        return None

    def infer_statistics(self):
        return self._stats


def _source(**kwargs):
    kwargs.setdefault("confidence", HIGH)
    return _StatsSource(OperatorStatistics(**kwargs))


def _int_column(name, lo=0, hi=99, **kwargs):
    return ColumnStatistics(name=name, min_value=lo, max_value=hi, **kwargs)


class TestColumnStatistics:
    def test_negative_counts_are_sanitized(self):
        stat = ColumnStatistics(name="a", null_count=-1, distinct_count=0)
        assert stat.null_count == 0
        assert stat.distinct_count is None

    def test_merge_widens_bounds(self):
        merged = _int_column("a", 0, 10, null_count=1, distinct_count=5).merge(
            _int_column("a", 5, 20, null_count=2, distinct_count=8)
        )
        assert (merged.min_value, merged.max_value) == (0, 20)
        assert merged.null_count == 3
        # The max of the parts is a lower bound of the combined NDV.
        assert merged.distinct_count == 8

    def test_merge_with_unknown_side_drops_bounds(self):
        merged = _int_column("a", 0, 10, null_count=1).merge(ColumnStatistics("a"))
        assert merged.min_value is None and merged.max_value is None
        assert merged.null_count is None

    def test_merge_incomparable_bounds(self):
        merged = _int_column("a", 0, 10).merge(ColumnStatistics("a", "x", "y"))
        assert merged.min_value is None and merged.max_value is None

    def test_dict_roundtrip(self):
        stat = _int_column("a", null_count=3, confidence=LOW)
        assert ColumnStatistics.from_dict(stat.to_dict()) == stat


class TestOperatorStatistics:
    def test_post_init(self):
        stats = OperatorStatistics(
            num_rows=-5, size_bytes=-1, selectivity=2.0, confidence=1.5
        )
        assert stats.num_rows == 0
        assert stats.size_bytes == 0
        assert stats.selectivity == 1.0
        assert stats.confidence == 1.0
        assert OperatorStatistics(num_rows=10, size_bytes=100).avg_row_bytes == 10
        assert OperatorStatistics(num_rows=0, size_bytes=0).avg_row_bytes is None

    def test_scale(self):
        stats = OperatorStatistics(
            num_rows=1000,
            size_bytes=10_000,
            num_blocks=4,
            column_stats={"a": _int_column("a", null_count=100, distinct_count=500)},
            confidence=HIGH,
        )
        scaled = stats.scale(0.1)
        assert scaled.num_rows == 100
        assert scaled.size_bytes == 1000
        assert scaled.num_blocks == 4
        assert scaled.selectivity == 0.1
        assert scaled.amplification_ratio is None
        assert scaled.confidence == pytest.approx(HIGH * CONFIDENCE_DECAY_PER_OP)
        column = scaled.column_stats["a"]
        assert column.null_count == 10
        assert column.distinct_count == 100
        assert (column.min_value, column.max_value) == (0, 99)
        # The input isn't mutated.
        assert stats.column_stats["a"].null_count == 100

    def test_scale_amplification_and_zero(self):
        amplified = OperatorStatistics(num_rows=10, size_bytes=0).scale(3.0)
        assert amplified.num_rows == 30
        assert amplified.size_bytes == 0
        assert amplified.selectivity is None
        assert amplified.amplification_ratio == 3.0

        assert OperatorStatistics(num_rows=10).scale(-1).num_rows == 0
        assert OperatorStatistics().scale(0.5).num_rows is None

        zero_rows = OperatorStatistics(
            num_rows=10,
            size_bytes=0,
            column_stats={"a": _int_column("a", distinct_count=5)},
        ).scale(0.0)
        assert zero_rows.num_rows == 0
        # A 0-row dataset cannot have a distinct value.
        assert zero_rows.column_stats["a"].distinct_count is None

    def test_passthrough(self):
        stats = OperatorStatistics(num_rows=10, selectivity=0.5, confidence=HIGH)
        passed = stats.passthrough(num_blocks=3)
        assert passed.num_rows == 10
        assert passed.num_blocks == 3
        assert passed.selectivity is None
        assert passed.confidence == pytest.approx(HIGH * CONFIDENCE_DECAY_PER_OP)

    def test_projected_size_bytes(self):
        stats = OperatorStatistics(
            num_rows=10,
            size_bytes=1000,
            column_stats={
                "a": ColumnStatistics("a", avg_size_bytes=8),
                "b": ColumnStatistics("b", avg_size_bytes=72),
            },
        )
        assert stats.projected_size_bytes(["a"]) == 100
        assert stats.projected_size_bytes([]) == 0
        # Duplicated columns never exceed the input size.
        assert stats.projected_size_bytes(["b", "b"]) == 1000
        assert stats.projected_size_bytes(["c"]) is None

        stats.column_stats["c"] = ColumnStatistics("c")
        assert stats.projected_size_bytes(["a"]) is None
        assert OperatorStatistics(num_rows=10).projected_size_bytes(["a"]) is None

    def test_projected_size_upper_bytes(self):
        stats = OperatorStatistics(
            num_rows=10,
            size_bytes=1000,
            size_bytes_upper=4000,
            column_stats={
                "a": ColumnStatistics("a", avg_size_bytes=8),
                "b": ColumnStatistics("b", avg_size_bytes=72),
            },
        )
        assert stats.projected_size_upper_bytes(["a"]) == 400
        # The bound is rounded up: truncation would understate it.
        assert (
            OperatorStatistics(
                num_rows=10,
                size_bytes=1000,
                size_bytes_upper=1001,
                column_stats=stats.column_stats,
            ).projected_size_upper_bytes(["a"])
            == 101
        )
        # No proven bound on the input -> no bound on the projection.
        assert (
            OperatorStatistics(num_rows=10, size_bytes=1000).projected_size_upper_bytes(
                ["a"]
            )
            is None
        )

    def test_upper_bound_is_never_below_point_estimate(self):
        stats = OperatorStatistics(size_bytes=100, size_bytes_upper=10)
        assert stats.size_bytes_upper == 100

    def test_scale_keeps_upper_bound(self):
        stats = OperatorStatistics(num_rows=100, size_bytes=1000, size_bytes_upper=2000)
        assert stats.scale(0.5).size_bytes_upper == 2000

    def test_merge(self):
        left = OperatorStatistics(
            num_rows=10,
            size_bytes=100,
            num_blocks=1,
            column_stats={"a": _int_column("a", 0, 5), "b": _int_column("b")},
            confidence=HIGH,
        )
        right = OperatorStatistics(
            num_rows=20,
            size_bytes=None,
            num_blocks=2,
            column_stats={"a": _int_column("a", 3, 9)},
            confidence=LOW,
        )
        merged = left.merge(right)
        assert merged.num_rows == 30
        assert merged.size_bytes is None
        assert merged.size_bytes_upper is None
        assert merged.num_blocks == 3
        assert merged.confidence == LOW
        # Only columns known on both sides are valid for the union.
        assert set(merged.column_stats) == {"a"}
        assert merged.column_stats["a"].max_value == 9
        assert left.merge(None) is left

    def test_to_log_string(self):
        log = OperatorStatistics(
            num_rows=10, size_bytes=2 * 1024**2, confidence=HIGH
        ).to_log_string()
        assert "rows=10" in log
        assert "size=2.00MB" in log
        assert "confidence=0.95" in log

    def test_zero_and_unknown(self):
        assert OperatorStatistics.zero().num_rows == 0
        assert not OperatorStatistics.unknown().is_available()


class TestSelectivityEstimation:
    @pytest.mark.parametrize(
        "operator,value,expected",
        [
            ("GT", 75, 0.25),
            ("GE", 75, 0.25),
            ("GTE", 75, 0.25),
            ("LT", 25, 0.25),
            ("LE", 25, 0.25),
            ("LTE", 25, 0.25),
            ("GT", 200, 0.0),
            ("LT", -5, 0.0),
            ("GT", -5, 1.0),
        ],
    )
    def test_range(self, operator, value, expected):
        stat = _int_column("a", 0, 100)
        assert estimate_selectivity_from_column_stats(
            "a", operator, value, stat
        ) == pytest.approx(expected)

    def test_range_single_value_domain(self):
        stat = _int_column("a", 5, 5)
        assert estimate_selectivity_from_column_stats("a", "GE", 5, stat) == 1.0
        assert estimate_selectivity_from_column_stats("a", "GT", 5, stat) == 0.0

    def test_eq(self):
        assert estimate_selectivity_from_column_stats(
            "a", "EQ", 5, _int_column("a", distinct_count=20)
        ) == pytest.approx(0.05)
        # Dense integer domain without NDV.
        assert estimate_selectivity_from_column_stats(
            "a", "EQ", 5, _int_column("a", 0, 99)
        ) == pytest.approx(0.01)
        assert estimate_selectivity_from_column_stats(
            "a", "NE", 5, _int_column("a", 0, 99)
        ) == pytest.approx(0.99)

    def test_eq_out_of_range_requires_reliable_bounds(self):
        stat = _int_column("a", 0, 99, distinct_count=10, confidence=HIGH)
        assert estimate_selectivity_from_column_stats("a", "EQ", 500, stat) == 0.0
        # Bounds from a sample of files aren't global bounds.
        sampled = _int_column(
            "a", 0, 99, distinct_count=10, confidence=ConfidenceLevel.MEDIUM.value
        )
        assert estimate_selectivity_from_column_stats(
            "a", "EQ", 500, sampled
        ) == pytest.approx(0.1)

    def test_in(self):
        stat = _int_column("a", distinct_count=10)
        assert estimate_selectivity_from_column_stats(
            "a", "IN", [1, 2, 2, 3], stat
        ) == pytest.approx(0.3)
        assert estimate_selectivity_from_column_stats(
            "a", "NOT_IN", [1, 2, 3], stat
        ) == pytest.approx(0.7)
        assert (
            estimate_selectivity_from_column_stats("a", "IN", list(range(50)), stat)
            == 1.0
        )

    def test_nulls(self):
        stat = _int_column("a", 0, 100, null_count=20)
        assert estimate_selectivity_from_column_stats(
            "a", "IS_NULL", None, stat, num_rows=100
        ) == pytest.approx(0.2)
        assert estimate_selectivity_from_column_stats(
            "a", "IS_NOT_NULL", None, stat, num_rows=100
        ) == pytest.approx(0.8)
        # Nulls never satisfy comparisons.
        assert estimate_selectivity_from_column_stats(
            "a", "GT", 50, stat, num_rows=100
        ) == pytest.approx(0.4)

    def test_defaults_without_stats(self):
        assert (
            estimate_selectivity_from_column_stats("a", "EQ", 1, None)
            == DEFAULT_EQ_SELECTIVITY
        )
        assert (
            estimate_selectivity_from_column_stats("a", "GT", 1, None)
            == DEFAULT_RANGE_SELECTIVITY
        )
        assert (
            estimate_selectivity_from_column_stats("a", "LIKE", 1, None)
            == DEFAULT_UNKNOWN_SELECTIVITY
        )
        # Strings have no numeric interpolation.
        assert (
            estimate_selectivity_from_column_stats(
                "a", "GT", "m", ColumnStatistics("a", "a", "z")
            )
            == DEFAULT_RANGE_SELECTIVITY
        )

    def test_temporal_values(self):
        stat = ColumnStatistics(
            "d", datetime.date(2024, 1, 1), datetime.date(2024, 1, 11)
        )
        assert estimate_selectivity_from_column_stats(
            "d", "GE", datetime.date(2024, 1, 9), stat
        ) == pytest.approx(0.2)


class TestPredicateSelectivity:
    @pytest.fixture
    def stats(self):
        return OperatorStatistics(
            num_rows=100,
            column_stats={
                "a": _int_column("a", 0, 100),
                "b": _int_column("b", 0, 100, null_count=10),
            },
        )

    def test_comparison(self, stats):
        assert estimate_predicate_selectivity(col("a") > 75, stats) == (
            pytest.approx(0.25),
            True,
        )

    def test_literal_on_the_left_is_mirrored(self, stats):
        # 75 < a  <=>  a > 75
        selectivity, from_stats = estimate_predicate_selectivity(
            lit(75) < col("a"), stats
        )
        assert selectivity == pytest.approx(0.25)
        assert from_stats

    def test_boolean_combinations(self, stats):
        a_gt, _ = estimate_predicate_selectivity(col("a") > 50, stats)
        a_lt, _ = estimate_predicate_selectivity(col("a") < 20, stats)
        both, _ = estimate_predicate_selectivity(
            (col("a") > 50) & (col("a") < 20), stats
        )
        either, _ = estimate_predicate_selectivity(
            (col("a") > 50) | (col("a") < 20), stats
        )
        negated, _ = estimate_predicate_selectivity(~(col("a") > 50), stats)
        assert both == pytest.approx(a_gt * a_lt)
        assert either == pytest.approx(1 - (1 - a_gt) * (1 - a_lt))
        assert negated == pytest.approx(1 - a_gt)

    def test_null_and_membership_predicates(self, stats):
        assert estimate_predicate_selectivity(col("b").is_null(), stats) == (
            pytest.approx(0.1),
            True,
        )
        selectivity, from_stats = estimate_predicate_selectivity(
            col("a").is_in([1, 2]), stats
        )
        assert selectivity == pytest.approx(2 / 101)
        assert from_stats

    def test_unsupported_or_unknown_columns(self, stats):
        assert estimate_predicate_selectivity(col("a") > col("b"), stats) == (
            DEFAULT_UNKNOWN_SELECTIVITY,
            False,
        )
        selectivity, from_stats = estimate_predicate_selectivity(
            col("missing") > 1, stats
        )
        assert selectivity == DEFAULT_RANGE_SELECTIVITY
        assert not from_stats
        # A single heuristic leaf makes the whole estimate heuristic.
        _, from_stats = estimate_predicate_selectivity(
            (col("a") > 1) & (col("missing") > 1), stats
        )
        assert not from_stats


class TestStatisticsPropagation:
    def test_expression_filter(self):
        source = _source(
            num_rows=1000, size_bytes=8000, column_stats={"a": _int_column("a", 0, 100)}
        )
        stats = Filter(
            predicate_expr=col("a") > 75, input_dependencies=[source]
        ).infer_statistics()
        assert stats.num_rows == 250
        assert stats.size_bytes == 2000
        assert stats.confidence == pytest.approx(HIGH * CONFIDENCE_DECAY_PER_OP)

    def test_heuristic_filters_have_low_confidence(self):
        source = _source(num_rows=1000, size_bytes=8000)
        udf_filter = Filter(
            fn=lambda row: True, input_dependencies=[source]
        ).infer_statistics()
        assert udf_filter.num_rows == 500
        assert udf_filter.confidence <= LOW
        no_stats = Filter(
            predicate_expr=col("a") > 1, input_dependencies=[source]
        ).infer_statistics()
        assert no_stats.confidence <= LOW

    def test_limit(self):
        source = _source(num_rows=1000, size_bytes=8000)
        assert Limit(10, input_dependencies=[source]).infer_statistics().num_rows == 10
        assert (
            Limit(10_000, input_dependencies=[source]).infer_statistics().num_rows
            == 1000
        )
        unknown_rows = Limit(
            10, input_dependencies=[_source(size_bytes=8000)]
        ).infer_statistics()
        assert unknown_rows.size_bytes == 8000

    def test_udf_maps(self):
        source = _source(
            num_rows=1000,
            size_bytes=8000,
            size_bytes_upper=8000,
            column_stats={"a": _int_column("a")},
        )
        # Row-preserving by contract: rows are kept, the size is a prior.
        mapped = MapBatches(
            lambda batch: batch, input_dependencies=[source]
        ).infer_statistics()
        assert mapped.num_rows == 1000
        assert mapped.size_bytes == 8000
        # A UDF can widen rows, so it never carries a proven bound.
        assert mapped.size_bytes_upper is None
        assert mapped.column_stats == {}
        assert mapped.confidence <= LOW

        # Rows may change: the row count is unknown, the size is still a prior
        # so downstream operators keep a demand estimate.
        modifying = MapBatches(
            lambda batch: batch,
            input_dependencies=[source],
            can_modify_num_rows=True,
        ).infer_statistics()
        assert modifying.num_rows is None
        assert modifying.size_bytes == 8000
        assert modifying.size_bytes_upper is None
        assert modifying.confidence <= LOW

        assert (
            FlatMap(lambda row: [row], input_dependencies=[source]).infer_statistics()
            is not None
        )

    def test_project(self):
        source = _source(
            num_rows=10,
            size_bytes=100,
            column_stats={"a": _int_column("a"), "b": _int_column("b")},
        )
        selected = Project(
            exprs=[col("a")], input_dependencies=[source]
        ).infer_statistics()
        assert set(selected.column_stats) == {"a"}
        assert selected.confidence == pytest.approx(HIGH * CONFIDENCE_DECAY_PER_OP)

        renamed = Project(
            exprs=[star(), col("a")._rename("c")], input_dependencies=[source]
        ).infer_statistics()
        assert set(renamed.column_stats) == {"b", "c"}
        assert renamed.column_stats["c"].name == "c"

        computed = Project(
            exprs=[star(), (col("a") + 1).alias("a")], input_dependencies=[source]
        ).infer_statistics()
        assert set(computed.column_stats) == {"b"}
        assert computed.confidence <= LOW

    def test_project_size(self):
        source = _source(
            num_rows=10,
            size_bytes=1000,
            column_stats={
                "a": _int_column("a", avg_size_bytes=8),
                "b": _int_column("b", avg_size_bytes=92),
            },
        )
        selected = Project(
            exprs=[col("a")._rename("c")], input_dependencies=[source]
        ).infer_statistics()
        assert selected.size_bytes == 80
        assert selected.avg_row_bytes == 8
        assert selected.column_stats["c"].avg_size_bytes == 8
        # Without column sizes, the input size is kept as an upper bound.
        unsized = _source(
            num_rows=10,
            size_bytes=1000,
            column_stats={"a": _int_column("a"), "b": _int_column("b")},
        )
        assert (
            Project(exprs=[col("a")], input_dependencies=[unsized])
            .infer_statistics()
            .size_bytes
            == 1000
        )
        computed = Project(
            exprs=[col("a"), (col("a") + 1).alias("d")],
            input_dependencies=[source],
        )
        assert computed.infer_statistics().size_bytes == 1000

    def test_all_to_all(self):
        source = _source(num_rows=10, size_bytes=100, num_blocks=2)
        assert (
            RandomShuffle(input_dependencies=[source]).infer_statistics().num_rows == 10
        )
        repartitioned = Repartition(
            shuffle=False, num_outputs=8, input_dependencies=[source]
        )
        assert repartitioned.infer_statistics().num_blocks == 8
        assert (
            Aggregate(key="a", aggs=[], input_dependencies=[source]).infer_statistics()
            is None
        )

    def test_union(self):
        left = _source(num_rows=10, size_bytes=100)
        right = _source(num_rows=5, size_bytes=50)
        assert Union([left, right]).infer_statistics().num_rows == 15
        assert Union([left, _StatsSource(None)]).infer_statistics() is None

    def test_read_without_pushdown_support(self):
        # Datasources without projection pushdown have no projection map.
        datasource = RangeDatasource(n=1000)
        stats = Read(datasource, datasource, parallelism=-1).infer_statistics()
        assert stats.num_rows == 1000
        assert stats.size_bytes == 8000
        assert stats.confidence == HIGH

    def test_read_parquet_with_pushdown(self, ray_start_regular_shared, tmp_path):
        pq.write_table(
            pa.table({"id": list(range(100)), "x": ["x" * 100] * 100}),
            tmp_path / "data.parquet",
        )
        datasource = ParquetDatasource(str(tmp_path))
        full = Read(datasource, datasource, parallelism=-1).infer_statistics()
        assert full.column_stats["x"].avg_size_bytes > 100

        datasource = datasource.apply_projection({"id": "key"}).apply_predicate(
            col("id") < 25
        )
        stats = Read(datasource, datasource, parallelism=-1).infer_statistics()
        assert stats.num_rows == 25
        # The read stage never renames: the projection map is identity, and the
        # ``key`` alias is a ``Project`` above the read instead.
        assert set(stats.column_stats) == {"id"}
        # Only the 8-byte `id` column is read.
        assert stats.avg_row_bytes == pytest.approx(8, rel=0.1)
        assert stats.size_bytes < full.size_bytes * 0.25 * 0.1


class TestParquetFooterStatistics:
    @pytest.fixture
    def footers(self, tmp_path):
        tables = [
            pa.table({"a": [1, 2, None], "s": ["x", "y", "z"]}),
            pa.table({"a": [10, 20, 30], "s": ["a", "b", "c"]}),
        ]
        metadata = []
        for i, table in enumerate(tables):
            path = tmp_path / f"{i}.parquet"
            pq.write_table(table, path, row_group_size=2)
            metadata.append(pq.ParquetFile(path).metadata)
        return metadata

    def test_extract_single_file(self, footers):
        stats = extract_column_stats_from_parquet_metadata(footers[0])
        assert stats["a"]["min"] == 1 and stats["a"]["max"] == 2
        assert stats["a"]["null_count"] == 1
        assert stats["s"]["min"] == "x"

    def test_missing_statistics(self, tmp_path):
        path = tmp_path / "no_stats.parquet"
        pq.write_table(pa.table({"a": [1, 2]}), path, write_statistics=False)
        stats = extract_column_stats_from_parquet_metadata(
            pq.ParquetFile(path).metadata
        )
        assert stats["a"] == {
            "min": None,
            "max": None,
            "null_count": None,
            "distinct_count": None,
        }

    def test_nested_columns_are_skipped(self, tmp_path):
        path = tmp_path / "nested.parquet"
        pq.write_table(pa.table({"a": [1], "n": [{"x": 1}]}), path)
        stats = extract_column_stats_from_parquet_metadata(
            pq.ParquetFile(path).metadata
        )
        assert set(stats) == {"a"}

    def _datasource(self, footers, num_fragments, column_sizes=None):
        fragments = [object() for _ in range(num_fragments)]
        column_sizes = column_sizes or [None] * len(footers)
        return SimpleNamespace(
            _pq_fragments=fragments,
            _sampled_fragments=fragments[: len(footers)],
            _sampled_file_infos=[
                SimpleNamespace(metadata=m, avg_column_in_mem_bytes=sizes)
                for m, sizes in zip(footers, column_sizes)
            ],
        )

    def test_all_files_sampled(self, footers):
        column_stats, num_rows, complete = _parquet_footer_statistics(
            self._datasource(footers, num_fragments=2)
        )
        assert complete
        assert num_rows == 6
        assert (column_stats["a"].min_value, column_stats["a"].max_value) == (1, 30)
        assert column_stats["a"].null_count == 1
        assert column_stats["a"].confidence == HIGH

    def test_sampled_subset_is_extrapolated(self, footers):
        column_stats, num_rows, complete = _parquet_footer_statistics(
            self._datasource(footers, num_fragments=4)
        )
        assert not complete
        assert num_rows == 12
        assert column_stats["a"].null_count == 2
        assert column_stats["a"].confidence == ConfidenceLevel.MEDIUM.value

    def test_pruned_files_are_ignored(self, footers):
        datasource = self._datasource(footers, num_fragments=2)
        datasource._pq_fragments = datasource._pq_fragments[1:]
        column_stats, num_rows, complete = _parquet_footer_statistics(datasource)
        assert complete
        assert num_rows == 3
        assert column_stats["a"].min_value == 10

    def test_column_sizes(self, footers):
        column_stats, _, _ = _parquet_footer_statistics(
            self._datasource(
                footers,
                num_fragments=2,
                column_sizes=[
                    {"a": 8, "s": 10, "n": 4},
                    {"a": 8, "s": 40, "n": 4},
                ],
            )
        )
        assert column_stats["a"].avg_size_bytes == 8
        # Weighted by the row counts of the files (3 rows each).
        assert column_stats["s"].avg_size_bytes == 25
        # Columns without footer statistics still get a size.
        assert not column_stats["n"].is_available()
        assert column_stats["n"].avg_size_bytes == 4

        # A file without sizes makes all sizes unknown.
        column_stats, _, _ = _parquet_footer_statistics(
            self._datasource(
                footers, num_fragments=2, column_sizes=[{"a": 8, "s": 10}, None]
            )
        )
        assert column_stats["a"].avg_size_bytes is None
        assert "n" not in column_stats

    def test_not_parquet(self):
        assert _parquet_footer_statistics(SimpleNamespace()) == ({}, None, False)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
