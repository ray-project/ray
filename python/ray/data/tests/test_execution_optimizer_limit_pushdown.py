import sys

import pandas as pd
import pytest

import ray
from ray.data.block import BlockMetadata
from ray.data.datasource import Datasource
from ray.data.datasource.datasource import ReadTask
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa


def _check_valid_plan_and_result(
    ds,
    expected_plan,
    expected_result,
    expected_physical_plan_ops=None,
):
    assert ds.take_all() == expected_result
    assert ds._plan._logical_plan.dag.dag_str == expected_plan

    expected_physical_plan_ops = expected_physical_plan_ops or []
    for op in expected_physical_plan_ops:
        assert op in ds.stats(), f"Operator {op} not found: {ds.stats()}"


def test_limit_pushdown_conservative(ray_start_regular_shared_2_cpus):
    """Test limit pushdown behavior - pushes through safe operations."""

    def f1(x):
        return x

    def f2(x):
        return x

    # Test 1: Basic Limit -> Limit fusion (should still work)
    ds = ray.data.range(100).limit(5).limit(100)
    _check_valid_plan_and_result(
        ds, "Read[ReadRange] -> Limit[limit=5]", [{"id": i} for i in range(5)]
    )

    ds = ray.data.range(100).limit(100).limit(5)
    _check_valid_plan_and_result(
        ds, "Read[ReadRange] -> Limit[limit=5]", [{"id": i} for i in range(5)]
    )

    ds = ray.data.range(100).limit(50).limit(80).limit(5).limit(20)
    _check_valid_plan_and_result(
        ds, "Read[ReadRange] -> Limit[limit=5]", [{"id": i} for i in range(5)]
    )

    # Test 2: Limit should push through MapRows operations (safe)
    ds = ray.data.range(100, override_num_blocks=100).map(f1).limit(1)
    _check_valid_plan_and_result(
        ds, "Read[ReadRange] -> Limit[limit=1] -> MapRows[Map(f1)]", [{"id": 0}]
    )

    # Test 3: Limit should push through MapBatches operations
    ds = ray.data.range(100, override_num_blocks=100).map_batches(f2).limit(1)
    _check_valid_plan_and_result(
        ds,
        "Read[ReadRange] -> Limit[limit=1] -> MapBatches[MapBatches(f2)]",
        [{"id": 0}],
    )

    # Test 4: Limit should NOT push through Filter operations (conservative)
    ds = (
        ray.data.range(100, override_num_blocks=100)
        .filter(lambda x: x["id"] < 50)
        .limit(1)
    )
    _check_valid_plan_and_result(
        ds, "Read[ReadRange] -> Filter[Filter(<lambda>)] -> Limit[limit=1]", [{"id": 0}]
    )

    # Test 5: Limit should push through Project operations (safe)
    ds = ray.data.range(100, override_num_blocks=100).select_columns(["id"]).limit(5)
    _check_valid_plan_and_result(
        ds,
        "Read[ReadRange] -> Limit[limit=5] -> Project[Project]",
        [{"id": i} for i in range(5)],
    )

    # Test 6: Limit should stop at Sort operations (AllToAll)
    ds = ray.data.range(100).sort("id").limit(5)
    _check_valid_plan_and_result(
        ds,
        "Read[ReadRange] -> Sort[Sort] -> Limit[limit=5]",
        [{"id": i} for i in range(5)],
    )

    # Test 7: More complex interweaved case.
    ds = ray.data.range(100).sort("id").map(f1).limit(20).sort("id").map(f2).limit(5)
    _check_valid_plan_and_result(
        ds,
        "Read[ReadRange] -> Sort[Sort] -> Limit[limit=20] -> MapRows[Map(f1)] -> "
        "Sort[Sort] -> Limit[limit=5] -> MapRows[Map(f2)]",
        [{"id": i} for i in range(5)],
    )

    # Test 8: Test limit pushdown between two Map operators.
    ds = ray.data.range(100, override_num_blocks=100).map(f1).limit(1).map(f2)
    _check_valid_plan_and_result(
        ds,
        "Read[ReadRange] -> Limit[limit=1] -> MapRows[Map(f1)] -> MapRows[Map(f2)]",
        [{"id": 0}],
    )


def test_limit_pushdown_correctness(ray_start_regular_shared_2_cpus):
    """Test that limit pushdown produces correct results in various scenarios."""

    # Test 1: Simple project + limit
    ds = ray.data.range(100).select_columns(["id"]).limit(10)
    result = ds.take_all()
    expected = [{"id": i} for i in range(10)]
    assert result == expected

    # Test 2: Multiple operations + limit (with MapRows pushdown)
    ds = (
        ray.data.range(100)
        .map(lambda x: {"id": x["id"], "squared": x["id"] ** 2})
        .select_columns(["id"])
        .limit(5)
    )
    result = ds.take_all()
    expected = [{"id": i} for i in range(5)]
    assert result == expected

    # Test 3: MapRows operations should get limit pushed (safe)
    ds = ray.data.range(100).map(lambda x: {"id": x["id"] * 2}).limit(5)
    result = ds.take_all()
    expected = [{"id": i * 2} for i in range(5)]
    assert result == expected

    # Test 4: MapBatches operations should not get limit pushed
    ds = ray.data.range(100).map_batches(lambda batch: {"id": batch["id"] * 2}).limit(5)
    result = ds.take_all()
    expected = [{"id": i * 2} for i in range(5)]
    assert result == expected

    # Test 5: Filter operations should not get limit pushed (conservative)
    ds = ray.data.range(100).filter(lambda x: x["id"] % 2 == 0).limit(3)
    result = ds.take_all()
    expected = [{"id": i} for i in [0, 2, 4]]
    assert result == expected

    # Test 6: Complex chain with both safe operations (should all get limit pushed)
    ds = (
        ray.data.range(100)
        .select_columns(["id"])  # Project - could be safe if it was the immediate input
        .map(lambda x: {"id": x["id"] + 1})  # MapRows - NOT safe, stops pushdown
        .limit(3)
    )
    result = ds.take_all()
    expected = [{"id": i + 1} for i in range(3)]
    assert result == expected

    # The plan should show all operations after the limit
    plan_str = ds._plan._logical_plan.dag.dag_str
    assert (
        "Read[ReadRange] -> Limit[limit=3] -> Project[Project] -> MapRows[Map(<lambda>)]"
        == plan_str
    )


def test_limit_pushdown_scan_efficiency(ray_start_regular_shared_2_cpus):
    """Test that limit pushdown scans fewer rows from the data source."""

    @ray.remote
    class Counter:
        def __init__(self):
            self.value = 0

        def increment(self, amount=1):
            self.value += amount
            return self.value

        def get(self):
            return self.value

        def reset(self):
            self.value = 0

    # Create a custom datasource that tracks how many rows it produces
    class CountingDatasource(Datasource):
        def __init__(self):
            self.counter = Counter.remote()

        def prepare_read(self, parallelism, n_per_block=10):
            def read_fn(block_idx):
                # Each block produces n_per_block rows
                ray.get(self.counter.increment.remote(n_per_block))
                return [
                    pd.DataFrame(
                        {
                            "id": range(
                                block_idx * n_per_block, (block_idx + 1) * n_per_block
                            )
                        }
                    )
                ]

            return [
                ReadTask(
                    lambda i=i: read_fn(i),
                    BlockMetadata(
                        num_rows=n_per_block,
                        size_bytes=n_per_block * 8,  # rough estimate
                        input_files=None,
                        exec_stats=None,
                    ),
                )
                for i in range(parallelism)
            ]

        def get_rows_produced(self):
            return ray.get(self.counter.get.remote())

    # Test 1: Project + Limit should scan fewer rows due to pushdown
    source = CountingDatasource()
    ds = ray.data.read_datasource(source, override_num_blocks=20, n_per_block=10)
    ds = ds.select_columns(["id"]).limit(5)
    result = ds.take_all()

    # Should get correct results
    assert len(result) == 5
    assert result == [{"id": i} for i in range(5)]

    # Should have scanned significantly fewer than all 200 rows (20 blocks * 10 rows)
    # Due to pushdown, we should scan much less
    rows_produced_1 = source.get_rows_produced()
    assert rows_produced_1 < 200  # Should be much less than total

    # Test 2: MapRows + Limit should also scan fewer rows due to pushdown
    source2 = CountingDatasource()
    ds2 = ray.data.read_datasource(source2, override_num_blocks=20, n_per_block=10)
    ds2 = ds2.map(lambda x: x).limit(5)
    result2 = ds2.take_all()

    # Should get correct results
    assert len(result2) == 5
    assert result2 == [{"id": i} for i in range(5)]

    # Should also scan fewer than total due to pushdown
    rows_produced_2 = source2.get_rows_produced()
    assert rows_produced_2 < 200

    # Both should be efficient with pushdown
    assert rows_produced_1 < 100  # Should be much less than total
    assert rows_produced_2 < 100  # Should be much less than total

    # Test 3: Filter + Limit should scan fewer due to early termination, but not pushdown
    source3 = CountingDatasource()
    ds3 = ray.data.read_datasource(source3, override_num_blocks=20, n_per_block=10)
    ds3 = ds3.filter(lambda x: x["id"] % 2 == 0).limit(3)
    result3 = ds3.take_all()

    # Should get correct results
    assert len(result3) == 3
    assert result3 == [{"id": i} for i in [0, 2, 4]]

    # Should still scan fewer than total due to early termination
    rows_produced_3 = source3.get_rows_produced()
    assert rows_produced_3 < 200


def test_limit_pushdown_union(ray_start_regular_shared_2_cpus):
    """Test limit pushdown behavior with Union operations."""

    # Create two datasets and union with limit
    ds1 = ray.data.range(100, override_num_blocks=10)
    ds2 = ray.data.range(200, override_num_blocks=10)
    ds = ds1.union(ds2).limit(5)

    expected_plan = "Read[ReadRange] -> Limit[limit=5], Read[ReadRange] -> Limit[limit=5] -> Union[Union] -> Limit[limit=5]"
    _check_valid_plan_and_result(ds, expected_plan, [{"id": i} for i in range(5)])


def test_limit_pushdown_union_with_maprows(ray_start_regular_shared_2_cpus):
    """Limit after Union + MapRows: limit should be pushed before the MapRows
    and inside each Union branch."""
    ds1 = ray.data.range(100, override_num_blocks=10)
    ds2 = ray.data.range(200, override_num_blocks=10)
    ds = ds1.union(ds2).map(lambda x: x).limit(5)

    expected_plan = (
        "Read[ReadRange] -> Limit[limit=5], "
        "Read[ReadRange] -> Limit[limit=5] -> Union[Union] -> "
        "Limit[limit=5] -> MapRows[Map(<lambda>)]"
    )
    _check_valid_plan_and_result(ds, expected_plan, [{"id": i} for i in range(5)])


def test_limit_pushdown_union_with_sort(ray_start_regular_shared_2_cpus):
    """Limit after Union + Sort: limit must NOT push through the Sort."""
    ds1 = ray.data.range(100, override_num_blocks=4)
    ds2 = ray.data.range(50, override_num_blocks=4).map(
        lambda x: {"id": x["id"] + 1000}
    )
    ds = ds1.union(ds2).sort("id").limit(5)

    expected_plan = (
        "Read[ReadRange], "
        "Read[ReadRange] -> MapRows[Map(<lambda>)] -> "
        "Union[Union] -> Sort[Sort] -> Limit[limit=5]"
    )
    _check_valid_plan_and_result(ds, expected_plan, [{"id": i} for i in range(5)])


def test_limit_pushdown_multiple_unions(ray_start_regular_shared_2_cpus):
    """Outer limit over nested unions should create a branch-local limit
    for every leaf plus the global one."""
    ds = (
        ray.data.range(100)
        .union(ray.data.range(100, override_num_blocks=5))
        .union(ray.data.range(50))
        .limit(5)
    )

    expected_plan = (
        "Read[ReadRange] -> Limit[limit=5], "
        "Read[ReadRange] -> Limit[limit=5] -> Union[Union] -> Limit[limit=5], "
        "Read[ReadRange] -> Limit[limit=5] -> Union[Union] -> Limit[limit=5]"
    )
    _check_valid_plan_and_result(ds, expected_plan, [{"id": i} for i in range(5)])


def test_limit_pushdown_union_with_groupby(ray_start_regular_shared_2_cpus):
    """Limit after Union + Aggregate: limit should stay after Aggregate."""
    ds1 = ray.data.range(100)
    ds2 = ray.data.range(100).map(lambda x: {"id": x["id"] + 1000})
    ds = ds1.union(ds2).groupby("id").count().limit(5)
    # Result should contain 5 distinct ids with count == 1.
    res = ds.take_all()
    # Plan suffix check (no branch limits past Aggregate).
    assert ds._plan._logical_plan.dag.dag_str.endswith(
        "Union[Union] -> Aggregate[Aggregate] -> Limit[limit=5]"
    )
    assert len(res) == 5 and all(r["count()"] == 1 for r in res)


def test_limit_pushdown_complex_chain(ray_start_regular_shared_2_cpus):
    """
    Complex end-to-end case:
      1. Two branches each with a branch-local Limit pushed to Read.
         • left  : Project
         • right : MapRows
      2. Union of the two branches.
      3. Global Aggregate (groupby/count).
      4. Sort (descending id) – pushes stop here.
      5. Final Limit.
    Verifies both plan rewrite and result correctness.
    """
    # ── left branch ────────────────────────────────────────────────
    left = ray.data.range(50).select_columns(["id"]).limit(10)

    # ── right branch ───────────────────────────────────────────────
    right = ray.data.range(50).map(lambda x: {"id": x["id"] + 1000}).limit(10)

    # ── union → aggregate → sort → limit ──────────────────────────
    ds = left.union(right).groupby("id").count().sort("id", descending=True).limit(3)

    # Expected logical-plan string.
    expected_plan = (
        "Read[ReadRange] -> Limit[limit=10] -> Project[Project], "
        "Read[ReadRange] -> Limit[limit=10] -> MapRows[Map(<lambda>)] "
        "-> Union[Union] -> Aggregate[Aggregate] -> Sort[Sort] -> Limit[limit=3]"
    )

    # Top-3 ids are the three largest (1009, 1008, 1007) with count()==1.
    expected_result = [
        {"id": 1009, "count()": 1},
        {"id": 1008, "count()": 1},
        {"id": 1007, "count()": 1},
    ]

    _check_valid_plan_and_result(ds, expected_plan, expected_result)


def test_limit_pushdown_union_maps_projects(ray_start_regular_shared_2_cpus):
    r"""
    Read -> MapBatches -> MapRows -> Project
         \                               /
          --------   Union   -------------   → Limit
    The limit should be pushed in front of each branch
    (past MapRows, Project) while the original
    global Limit is preserved after the Union.
    """
    # Left branch.
    left = (
        ray.data.range(30)
        .map_batches(lambda b: b)
        .map(lambda r: {"id": r["id"]})
        .select_columns(["id"])
    )

    # Right branch with shifted ids.
    right = (
        ray.data.range(30)
        .map_batches(lambda b: b)
        .map(lambda r: {"id": r["id"] + 100})
        .select_columns(["id"])
    )

    ds = left.union(right).limit(3)

    expected_plan = (
        "Read[ReadRange] -> "
        "Limit[limit=3] -> MapBatches[MapBatches(<lambda>)] -> MapRows[Map(<lambda>)] -> "
        "Project[Project], "
        "Read[ReadRange] -> "
        "Limit[limit=3] -> MapBatches[MapBatches(<lambda>)] -> MapRows[Map(<lambda>)] -> "
        "Project[Project] -> Union[Union] -> Limit[limit=3]"
    )

    expected_result = [{"id": i} for i in range(3)]  # First 3 rows from left branch.

    _check_valid_plan_and_result(ds, expected_plan, expected_result)


def test_limit_pushdown_map_per_block_limit_applied(ray_start_regular_shared_2_cpus):
    """Test that per-block limits are actually applied during map execution."""

    # Create a global counter using Ray
    @ray.remote
    class Counter:
        def __init__(self):
            self.value = 0

        def increment(self):
            self.value += 1
            return self.value

        def get(self):
            return self.value

    counter = Counter.remote()

    def track_processing(row):
        # Record that this row was processed
        ray.get(counter.increment.remote())
        return row

    # Create dataset with limit pushed through map
    ds = ray.data.range(1000, override_num_blocks=10).map(track_processing).limit(50)

    # Execute and get results
    result = ds.take_all()

    # Verify correct results
    expected = [{"id": i} for i in range(50)]
    assert result == expected

    # Check how many rows were actually processed
    processed_count = ray.get(counter.get.remote())

    # With per-block limits, we should process fewer rows than the total dataset
    # but at least the number we need for the final result
    assert (
        processed_count >= 50
    ), f"Expected at least 50 rows processed, got {processed_count}"
    assert (
        processed_count < 1000
    ), f"Expected fewer than 1000 rows processed, got {processed_count}"

    print(f"Processed {processed_count} rows to get {len(result)} results")


def test_limit_pushdown_preserves_map_behavior(ray_start_regular_shared_2_cpus):
    """Test that adding per-block limits doesn't change the logical result."""

    def add_one(row):
        row["id"] += 1
        return row

    # Compare with and without limit pushdown
    ds_with_limit = ray.data.range(100).map(add_one).limit(10)
    ds_without_limit = ray.data.range(100).limit(10).map(add_one)

    result_with = ds_with_limit.take_all()
    result_without = ds_without_limit.take_all()

    # Results should be identical
    assert result_with == result_without

    # Both should have the expected transformation applied
    expected = [{"id": i + 1} for i in range(10)]
    assert result_with == expected


@pytest.mark.parametrize(
    "udf_modifying_row_count,expected_plan",
    [
        (
            False,
            "Read[ReadRange] -> Limit[limit=10] -> MapBatches[MapBatches(<lambda>)]",
        ),
        (
            True,
            "Read[ReadRange] -> MapBatches[MapBatches(<lambda>)] -> Limit[limit=10]",
        ),
    ],
)
def test_limit_pushdown_udf_modifying_row_count_with_map_batches(
    ray_start_regular_shared_2_cpus,
    udf_modifying_row_count,
    expected_plan,
):
    """Test that limit pushdown preserves the row count with map batches."""
    ds = (
        ray.data.range(100)
        .map_batches(lambda x: x, udf_modifying_row_count=udf_modifying_row_count)
        .limit(10)
    )
    _check_valid_plan_and_result(
        ds,
        expected_plan,
        [{"id": i} for i in range(10)],
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
