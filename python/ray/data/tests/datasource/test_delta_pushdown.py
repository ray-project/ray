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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
