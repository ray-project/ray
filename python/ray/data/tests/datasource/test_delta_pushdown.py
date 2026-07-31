"""End-to-end tests for Delta Lake predicate pushdown.

Two things have to hold, and they are tested separately on purpose:

* **Correctness** -- a filtered ``read_delta`` returns exactly what the V1
  path returns. Log-level pruning is an optimization, so it must not be able
  to change an answer.
* **Effectiveness** -- files that cannot match are never listed. Asserting
  only on rows would let pruning silently stop working, since reading every
  file also produces the right answer.
"""

import os
from typing import List, Optional

import pyarrow as pa
import pytest
from packaging.version import parse as parse_version

import ray
from ray.data._internal.logical.operators.read_operator import ListFiles
from ray.data._internal.logical.optimizers import LogicalOptimizer
from ray.data._internal.utils.arrow_utils import get_pyarrow_version
from ray.data.context import DataContext
from ray.data.expressions import Expr, col, lit
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa

_pa_version = get_pyarrow_version()
assert _pa_version is not None, "pyarrow must be installed to run these tests"

pytestmark = pytest.mark.skipif(
    _pa_version < parse_version("15.0.0"),
    reason="deltalake write_deltalake requires pyarrow >= 15.0",
)


@pytest.fixture
def use_datasource_v2(request):
    """Toggle the V2 read path for the duration of a test."""
    ctx = DataContext.get_current()
    previous = ctx.use_datasource_v2
    ctx.use_datasource_v2 = request.param
    yield request.param
    ctx.use_datasource_v2 = previous


@pytest.fixture
def zordered_table(tmp_path) -> str:
    """Unpartitioned table whose files hold disjoint ``val`` ranges.

    This is the shape the issue is about: without partition directories,
    only the log's min/max statistics can rule a file out.

        file 0: val 0-9     file 1: val 100-109
        file 2: val 200-209 file 3: val 300-309
    """
    from deltalake import write_deltalake

    path = os.path.join(tmp_path, "zordered")
    for i, low in enumerate((0, 100, 200, 300)):
        write_deltalake(
            path,
            pa.table({"val": list(range(low, low + 10)), "name": ["x"] * 10}),
            mode="error" if i == 0 else "append",
        )
    return path


@pytest.fixture
def partitioned_table(tmp_path) -> str:
    from deltalake import write_deltalake

    path = os.path.join(tmp_path, "partitioned")
    write_deltalake(
        path,
        pa.table(
            {
                "region": ["US", "US", "EU", "EU"],
                "val": [1, 2, 100, 200],
                "name": ["a", "b", "c", "d"],
            }
        ),
        partition_by=["region"],
    )
    return path


def _files_listed(ds: "ray.data.Dataset", path: str) -> int:
    """Number of files the optimized plan's listing stage would emit.

    Drives the post-optimization ``ListFiles`` indexer directly, which is
    what the execution layer does. Reaching into the plan is the only way to
    observe pruning: it changes how much work happens, not the answer.
    """
    import pyarrow.fs

    optimized = LogicalOptimizer().optimize(ds._logical_plan)

    def find_indexer(op):
        if isinstance(op, ListFiles):
            return op.file_indexer
        for dependency in op.input_dependencies:
            found = find_indexer(dependency)
            if found is not None:
                return found
        return None

    indexer = find_indexer(optimized.dag)
    assert indexer is not None, "plan has no ListFiles operator"
    return sum(
        len(manifest)
        for manifest in indexer.list_files(
            pa.array([path]), filesystem=pyarrow.fs.LocalFileSystem()
        )
    )


def _read(path: str, v2: bool, predicate: Optional[Expr] = None, **kwargs):
    DataContext.get_current().use_datasource_v2 = v2
    ds = ray.data.read_delta(path, **kwargs)
    if predicate is not None:
        ds = ds.filter(expr=predicate)
    return ds


def _sorted_rows(ds) -> List[dict]:
    return sorted(ds.take_all(), key=lambda row: sorted(row.items()))


# ---------------------------------------------------------------------------
# Parity with the V1 path
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("table", ["zordered_table", "partitioned_table"])
def test_schema_matches_v1_including_column_order(table, request, restore_data_context):
    """Column order is part of the contract, not an implementation detail.

    The V2 reader appends path-derived partition columns after the file's own
    columns, so a partitioned table would come back reordered unless the
    scanner pins a projection.
    """
    path = request.getfixturevalue(table)
    v1_schema = _read(path, v2=False).schema()
    v2_schema = _read(path, v2=True).schema()

    assert v2_schema.names == v1_schema.names
    assert v2_schema.types == v1_schema.types


@pytest.mark.parametrize("table", ["zordered_table", "partitioned_table"])
@pytest.mark.parametrize(
    "predicate",
    [
        None,
        col("val") > lit(150),
        col("val") < lit(5),
        col("val") > lit(100_000),
        col("val") != lit(3),
        (col("val") > lit(0)) & (col("val") < lit(105)),
        (col("val") < lit(5)) | (col("val") > lit(250)),
    ],
    ids=["none", "gt", "lt", "matches-nothing", "ne", "and", "or"],
)
def test_rows_match_v1(table, predicate, request, restore_data_context):
    path = request.getfixturevalue(table)
    assert _sorted_rows(_read(path, v2=True, predicate=predicate)) == _sorted_rows(
        _read(path, v2=False, predicate=predicate)
    )


def test_partition_values_match_v1_when_they_need_encoding(
    tmp_path, restore_data_context
):
    """Partition values round-trip through percent-encoded directory names."""
    from deltalake import write_deltalake

    path = os.path.join(tmp_path, "encoded")
    write_deltalake(
        path,
        pa.table({"grp": ["a b", "c/d", "e=f", "plain"], "val": [1, 2, 3, 4]}),
        partition_by=["grp"],
    )

    assert _sorted_rows(_read(path, v2=True)) == _sorted_rows(_read(path, v2=False))


def test_null_partition_values_match_v1(tmp_path, restore_data_context):
    """A null partition value is a directory name, not a null.

    Delta writes it as ``year=__HIVE_DEFAULT_PARTITION__``. Reading the
    Delta-declared schema makes ``year`` typed, so the sentinel has to be
    recognized before the cast rather than fed to it.
    """
    from deltalake import write_deltalake

    path = os.path.join(tmp_path, "null_partition")
    write_deltalake(
        path,
        pa.table(
            {
                "year": pa.array([2024, None], type=pa.int64()),
                "val": [1, 2],
            }
        ),
        partition_by=["year"],
    )

    v2 = _read(path, v2=True)
    assert _sorted_rows(v2) == _sorted_rows(_read(path, v2=False))
    assert sorted(row["year"] for row in v2.take_all() if row["year"] is None) == [None]


def test_columns_argument_matches_v1(zordered_table, restore_data_context):
    v1 = _read(zordered_table, v2=False, columns=["val"])
    v2 = _read(zordered_table, v2=True, columns=["val"])
    assert v2.schema().names == v1.schema().names == ["val"]
    assert _sorted_rows(v2) == _sorted_rows(v1)


def test_pinned_version_matches_v1(zordered_table, restore_data_context):
    v1 = _read(zordered_table, v2=False, version=0)
    v2 = _read(zordered_table, v2=True, version=0)
    assert _sorted_rows(v2) == _sorted_rows(v1)
    assert v2.count() == 10


# ---------------------------------------------------------------------------
# Pruning actually happens
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "predicate,expected_files,description",
    [
        (col("val") > lit(250), 1, "only the top range can match"),
        (col("val") < lit(5), 1, "only the bottom range can match"),
        (col("val") > lit(100_000), 0, "no file can match, so none are listed"),
        (col("val") >= lit(0), 4, "every file can match"),
        (col("val") != lit(3), 4, "an unprovable predicate prunes nothing"),
        (None, 4, "no predicate prunes nothing"),
    ],
)
def test_statistics_skip_files_without_partitioning(
    zordered_table,
    predicate: Optional[Expr],
    expected_files: int,
    description: str,
    restore_data_context,
):
    ds = _read(zordered_table, v2=True, predicate=predicate)
    assert _files_listed(ds, zordered_table) == expected_files, description


def test_partition_predicate_skips_files(partitioned_table, restore_data_context):
    ds = _read(partitioned_table, v2=True, predicate=col("region") == lit("US"))
    assert _files_listed(ds, partitioned_table) == 1


def test_partition_and_data_predicates_skip_together(
    partitioned_table, restore_data_context
):
    ds = _read(
        partitioned_table,
        v2=True,
        predicate=(col("region") == lit("EU")) & (col("val") > lit(150)),
    )
    assert _files_listed(ds, partitioned_table) == 1
    assert _sorted_rows(ds) == [{"region": "EU", "val": 200, "name": "d"}]


def test_pruning_is_optional_for_correctness(zordered_table, restore_data_context):
    """A plan the pruning rule doesn't recognize must still be correct.

    The rule reads predicates off the scanner; a predicate it cannot lower
    (here, a Python callable) never reaches the scanner at all. The read then
    lists every file, and the ``Filter`` above it produces the right answer.
    """
    DataContext.get_current().use_datasource_v2 = True
    ds = ray.data.read_delta(zordered_table).filter(lambda row: row["val"] > 250)

    assert _files_listed(ds, zordered_table) == 4
    assert sorted(row["val"] for row in ds.take_all()) == list(range(300, 310))


# ---------------------------------------------------------------------------
# Deletion Vectors
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("use_datasource_v2", [True, False], indirect=True)
def test_deletion_vectors_are_rejected(
    tmp_path, use_datasource_v2, restore_data_context
):
    """Reading such a table would resurrect rows it marks as deleted.

    Rejected on both paths: the V1 reader has never supported these tables,
    and the V2 reader opens the Parquet files directly, where the deleted
    rows are still physically present.
    """
    from deltalake import write_deltalake

    path = os.path.join(tmp_path, "deletion_vectors")
    write_deltalake(
        path,
        pa.table({"val": list(range(10))}),
        configuration={"delta.enableDeletionVectors": "true"},
    )

    with pytest.raises(ValueError, match="Deletion Vectors"):
        ray.data.read_delta(path)


# ---------------------------------------------------------------------------
# Things the V2 path must not change
# ---------------------------------------------------------------------------


def test_count_matches_v1(zordered_table, restore_data_context):
    """``count()`` takes a different plan shape than a normal read.

    ``PushdownCountFiles`` rewrites `Count -> ReadFiles` into a metadata-only
    pass over ``ListFiles``, so it exercises the indexer through a path a
    plain ``take_all()`` never reaches. Call it on a fresh dataset -- reading
    first would populate the cached row count and answer from there.
    """
    assert _read(zordered_table, v2=True).count() == 40
    assert _read(zordered_table, v2=False).count() == 40


def test_arrow_parquet_args_are_honored(zordered_table, restore_data_context):
    """``**arrow_parquet_args`` reaches ``iter_batches`` only on the V1 path.

    Dropping them silently would change which rows come back, so a read that
    passes them stays on V1 instead.
    """
    import pyarrow.compute as pc

    v2 = _read(zordered_table, v2=True, filter=pc.field("val") > 250)
    v1 = _read(zordered_table, v2=False, filter=pc.field("val") > 250)
    # The table holds 0-9, 100-109, 200-209 and 300-309.
    assert sorted(row["val"] for row in v2.take_all()) == list(range(300, 310))
    assert _sorted_rows(v2) == _sorted_rows(v1)


def test_include_paths_matches_v1(partitioned_table, restore_data_context):
    v1 = _read(partitioned_table, v2=False, include_paths=True)
    v2 = _read(partitioned_table, v2=True, include_paths=True)
    assert v2.schema().names == v1.schema().names
    assert all(row["path"] for row in v2.take_all())


def test_include_paths_survives_a_column_projection(
    partitioned_table, restore_data_context
):
    """``path`` is synthesized post-read, so it isn't in the caller's columns.

    A projection built only from ``columns`` would drop the very column
    ``include_paths`` was asked to add.
    """
    v1 = _read(partitioned_table, v2=False, columns=["val"], include_paths=True)
    v2 = _read(partitioned_table, v2=True, columns=["val"], include_paths=True)
    assert v2.schema().names == v1.schema().names


def test_snapshot_is_pinned_at_call_time(tmp_path, restore_data_context):
    """Listing is deferred, so the version must not be re-resolved later.

    Without pinning, a commit landing between construction and execution
    would be read with a schema inferred before it existed -- and a retried
    task could disagree with its first attempt.
    """
    from deltalake import write_deltalake

    path = os.path.join(tmp_path, "concurrent")
    write_deltalake(path, pa.table({"val": [1, 2, 3]}))

    DataContext.get_current().use_datasource_v2 = True
    ds = ray.data.read_delta(path)
    write_deltalake(path, pa.table({"val": [4, 5]}), mode="append")

    assert sorted(row["val"] for row in ds.take_all()) == [1, 2, 3]


@pytest.mark.parametrize(
    "kwargs,expected_columns",
    [
        ({}, ["a", "b"]),
        ({"columns": ["a"]}, ["a"]),
        ({"include_paths": True}, ["a", "b", "path"]),
    ],
)
def test_empty_table_honors_read_options(
    tmp_path, kwargs, expected_columns, restore_data_context
):
    """An empty table still has to produce the schema a full read would.

    Otherwise unioning it with a populated table fails on mismatched schemas.
    """
    from deltalake import write_deltalake

    path = os.path.join(tmp_path, "empty_opts")
    write_deltalake(
        path,
        pa.table(
            {"a": pa.array([], type=pa.int64()), "b": pa.array([], type=pa.string())}
        ),
    )

    ds = _read(path, v2=True, **kwargs)
    assert ds.schema().names == expected_columns
    assert ds.count() == 0


@pytest.mark.parametrize(
    "kwargs",
    [
        {},
        {"columns": ["a"]},
        {"include_paths": True},
        {"columns": ["a"], "include_paths": True},
    ],
    ids=["plain", "columns", "include-paths", "columns+include-paths"],
)
def test_empty_and_populated_tables_agree_on_schema(
    tmp_path, kwargs, restore_data_context
):
    """The two are built by completely different code paths.

    An empty table is answered from the log while a populated one goes
    through the read pipeline, so every combination of options has to be
    checked -- ``columns`` together with ``include_paths`` diverged because
    the projection dropped the synthesized ``path`` on one path but not the
    other. A mismatch here fails any union of the two.
    """
    from deltalake import write_deltalake

    empty = os.path.join(tmp_path, "empty")
    write_deltalake(
        empty,
        pa.table(
            {"a": pa.array([], type=pa.int64()), "b": pa.array([], type=pa.string())}
        ),
    )
    populated = os.path.join(tmp_path, "populated")
    write_deltalake(populated, pa.table({"a": [1, 2], "b": ["x", "y"]}))

    assert (
        _read(empty, v2=True, **kwargs).schema().names
        == _read(populated, v2=True, **kwargs).schema().names
    )


def test_empty_table_rejects_unknown_columns(tmp_path, restore_data_context):
    from deltalake import write_deltalake

    path = os.path.join(tmp_path, "empty_bad_col")
    write_deltalake(path, pa.table({"a": pa.array([], type=pa.int64())}))

    with pytest.raises(ValueError, match="don't exist"):
        _read(path, v2=True, columns=["nope"])


# ---------------------------------------------------------------------------
# Pruning is an optimization, not the thing enforcing the predicate
# ---------------------------------------------------------------------------


@pytest.fixture
def int_partitioned_table(tmp_path) -> str:
    """Partitioned by an ``int64`` column, which only the Delta log knows.

    ``read_parquet`` types partition columns as strings, so this case only
    arises once the schema comes from the Delta log.
    """
    from deltalake import write_deltalake

    path = os.path.join(tmp_path, "int_partitioned")
    write_deltalake(
        path,
        pa.table(
            {
                "year": pa.array([2023, 2023, 2024], type=pa.int64()),
                "val": [1, 2, 3],
            }
        ),
        partition_by=["year"],
    )
    return path


def test_delta_scanner_declines_to_enforce_partition_predicates():
    """Path parsing is a lossy view of what the log records.

    ``PathPartitionParser.evaluate_predicate_on_partition`` swallows any
    evaluation error and keeps the file, which is only sound while something
    else applies the predicate. Declining enforcement is what makes the
    optimizer keep that ``Filter``.
    """
    from ray.data._internal.datasource_v2.scanners.delta_scanner import DeltaScanner
    from ray.data._internal.datasource_v2.scanners.parquet_scanner import ParquetScanner

    schema = pa.schema([pa.field("p", pa.string())])
    assert DeltaScanner(schema=schema).enforces_partition_predicate is False
    assert ParquetScanner(schema=schema).enforces_partition_predicate is True


def test_partition_predicate_stays_in_a_filter(partitioned_table, restore_data_context):
    """The optimized plan must still contain a Filter for the predicate."""
    from ray.data._internal.logical.operators.map_operator import Filter

    ds = _read(partitioned_table, v2=True, predicate=col("region") == lit("US"))
    optimized = LogicalOptimizer().optimize(ds._logical_plan)

    def has_filter(op):
        return isinstance(op, Filter) or any(
            has_filter(d) for d in op.input_dependencies
        )

    assert has_filter(optimized.dag)
    # ...and pruning still happened, so correctness didn't cost the optimization.
    assert _files_listed(ds, partitioned_table) == 1


@pytest.mark.parametrize(
    "predicate",
    [
        col("p") == lit("a"),
        col("p") != lit("a"),
        col("p").is_in(["a"]),
        col("p").is_in(["a"]) | (col("p") == lit("b")),
        col("p").is_null(),
        col("p").is_not_null(),
    ],
    ids=["eq", "ne", "in", "in-or-eq", "is-null", "is-not-null"],
)
def test_partition_predicates_over_a_null_partition_match_v1(
    tmp_path, predicate: Expr, restore_data_context
):
    """A NULL partition is a directory name, and several kernels reject it.

    ``pc.is_in`` over the null-typed single-row table the path parser builds
    raises, which the scanner turns into "keep the file". These are the cases
    that returned extra rows while the predicate had only one enforcement
    point.
    """
    from deltalake import write_deltalake

    path = os.path.join(tmp_path, "null_part")
    write_deltalake(
        path,
        pa.table({"p": pa.array(["a", "b", None], type=pa.string()), "val": [1, 2, 3]}),
        partition_by=["p"],
    )

    assert _sorted_rows(_read(path, v2=True, predicate=predicate)) == _sorted_rows(
        _read(path, v2=False, predicate=predicate)
    )


def test_typed_partition_predicate_is_correct_without_the_pruning_rule(
    int_partitioned_table, restore_data_context, monkeypatch
):
    """Remove the rule entirely; the answer must not change."""
    from ray.data._internal.logical import optimizers
    from ray.data._internal.logical.interfaces.optimizer import Rule
    from ray.data._internal.logical.rules import PushdownDeltaFilePruning

    without_rule = [
        rule
        for rule in optimizers._LOGICAL_RULESET
        if rule is not PushdownDeltaFilePruning
    ]
    monkeypatch.setattr(
        optimizers, "_LOGICAL_RULESET", type(optimizers._LOGICAL_RULESET)(without_rule)
    )
    assert issubclass(PushdownDeltaFilePruning, Rule)

    DataContext.get_current().use_datasource_v2 = True
    rows = (
        ray.data.read_delta(int_partitioned_table)
        .filter(expr=col("year") == lit(2024))
        .take_all()
    )
    assert sorted(row["val"] for row in rows) == [3]


def test_pruning_rule_declares_its_ordering_dependency():
    """List position is only a tiebreaker; the edge has to be declared.

    The rule reads predicates that ``PredicatePushdown`` settles onto the
    scanner. If another rule later declares ``PredicatePushdown`` as a
    dependent, topological order flips and this rule would see a bare plan.
    """
    from ray.data._internal.logical.rules import (
        PredicatePushdown,
        PushdownDeltaFilePruning,
    )

    assert PredicatePushdown in PushdownDeltaFilePruning.dependencies()


@pytest.mark.parametrize(
    "partition_type,expected",
    [
        (pa.int64(), int),
        (pa.float64(), float),
        (pa.string(), str),
        (pa.bool_(), bool),
        # Not expressible as a `Partitioning` field type. Left unmapped, so
        # the value stays a string and prunes less -- it does not affect the
        # answer, which a retained `Filter` is responsible for.
        (pa.date32(), None),
    ],
)
def test_partition_field_types_from_the_log(
    partition_type: pa.DataType, expected: Optional[type]
):
    from ray.data.read_api import _delta_partition_field_types

    class _FakeMetadata:
        partition_columns = ["p"]

    class _FakeSchema:
        @staticmethod
        def to_arrow():
            return pa.schema(
                [pa.field("p", partition_type), pa.field("val", pa.int64())]
            )

    class _FakeTable:
        def metadata(self):
            return _FakeMetadata()

        def schema(self):
            return _FakeSchema()

    assert _delta_partition_field_types(_FakeTable()).get("p") is expected


@pytest.mark.parametrize(
    "mode,eligible",
    [(None, True), ("none", True), ("name", False), ("id", False)],
)
def test_column_mapping_tables_stay_on_the_v1_path(mode: Optional[str], eligible: bool):
    """Column mapping renames columns between the schema and the Parquet.

    The V2 reader opens the data files directly with the logical schema, so
    it would ask for names the files don't have and return empty columns.
    """
    from ray.data.read_api import _delta_table_supports_datasource_v2

    class _FakeMetadata:
        partition_columns = []
        configuration = {} if mode is None else {"delta.columnMapping.mode": mode}

    class _FakeSchema:
        @staticmethod
        def to_arrow():
            return pa.schema([pa.field("val", pa.int64())])

    class _FakeTable:
        def metadata(self):
            return _FakeMetadata()

        def schema(self):
            return _FakeSchema()

    assert _delta_table_supports_datasource_v2(_FakeTable(), {}) is eligible


@pytest.mark.parametrize("scheme", ["", "file://"])
def test_uri_addressed_tables_read_correctly(tmp_path, scheme, restore_data_context):
    """``deltalake`` wants the scheme; PyArrow rejects it.

    The log is opened with the URI as given, while the paths handed to the
    reader are filesystem-native. ``read_delta``'s documented examples are
    ``s3://`` and ``az://`` URIs, so this is the common case, not an edge one.
    """
    from deltalake import write_deltalake

    path = os.path.join(tmp_path, "uri_table")
    write_deltalake(path, pa.table({"v": [1, 2, 3]}))

    assert _sorted_rows(_read(scheme + path, v2=True)) == _sorted_rows(
        _read(scheme + path, v2=False)
    )


def test_table_with_a_path_column_stays_on_v1(tmp_path, restore_data_context):
    """``path`` is how the reader labels the synthesized file path.

    A real column of that name would be shadowed rather than returned.
    """
    from deltalake import write_deltalake

    path = os.path.join(tmp_path, "path_column")
    write_deltalake(path, pa.table({"path": ["x", "y"], "v": [1, 2]}))

    v2 = _read(path, v2=True)
    assert v2.schema().names == ["path", "v"]
    assert _sorted_rows(v2) == _sorted_rows(_read(path, v2=False))


def test_v2_eligibility_fails_closed():
    """A table that can't be inspected goes to V1 rather than guessing."""
    from ray.data.read_api import _delta_table_supports_datasource_v2

    class _BrokenTable:
        def metadata(self):
            raise RuntimeError("log unreadable")

    assert _delta_table_supports_datasource_v2(_BrokenTable(), {}) is False


def test_null_partition_value_with_field_types(tmp_path):
    """``null_fallback`` resolves to ``None``, which must skip coercion.

    Delta sets both: a typed partition column and the Hive null sentinel.
    Casting the sentinel-derived ``None`` to ``int`` would raise.
    """
    from ray.data.datasource.partitioning import (
        HIVE_DEFAULT_PARTITION,
        Partitioning,
        PartitionStyle,
        PathPartitionParser,
    )

    parser = PathPartitionParser(
        Partitioning(
            style=PartitionStyle.HIVE,
            field_names=["year"],
            field_types={"year": int},
            null_fallback=HIVE_DEFAULT_PARTITION,
        )
    )
    assert parser(f"/base/year={HIVE_DEFAULT_PARTITION}/f.parquet") == {"year": None}
    assert parser("/base/year=2024/f.parquet") == {"year": 2024}


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
