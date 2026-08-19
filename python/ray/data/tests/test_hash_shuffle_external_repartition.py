"""``ds.repartition()`` through the external (file-transport) hash-shuffle
variant. External-only tests (flag-off default, chained ops, on-disk
cleanup) follow the shared correctness block.
"""

import gc
import glob
import os
import tempfile
import time
from typing import cast

import pyarrow as pa
import pytest

import ray
from ray.data.context import DataContext, ShuffleStrategy
from ray.data.tests.conftest import *  # noqa: F401, F403
from ray.data.tests.test_hash_shuffle_v2 import (
    _assert_keys_colocated,
    _keys_per_block,
)
from ray.tests.conftest import *  # noqa: F401, F403


@pytest.fixture(autouse=True)
def _assert_no_leftover_shuffle_dirs():
    """After every test, assert no ``$TMPDIR/ray_shuffle_external_*`` dir
    leaked. The map op's ``_teardown_shuffle`` fires ``_cleanup_shuffle_dir``
    tasks eagerly and waits up to 5s, so by the time pytest teardown runs
    all external shuffle output should be gone.
    """
    pattern = os.path.join(tempfile.gettempdir(), "ray_shuffle_external_*")
    pre_existing = set(glob.glob(pattern))
    yield
    gc.collect()
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        leftover = set(glob.glob(pattern)) - pre_existing
        if not leftover:
            return
        time.sleep(0.1)
    raise AssertionError(f"leftover shuffle dirs after test: {leftover}")


# --- Correctness -------------------------------------------------------------


def test_external_sort_reduce_uses_higher_multiplier(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    disable_fallback_to_object_extension,
):
    """Sorted external reduces request 3x, matching object-store ShuffleReduceOp."""
    from ray.data._internal.execution.operators.shuffle_operators.shuffle_tasks import (
        SHUFFLE_PEAK_MEMORY_MULTIPLIER,
    )
    from ray.data._internal.logical.optimizers import get_execution_plan

    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.SHUFFLE_V2
    ctx.use_external_hash_shuffle = True

    sorted_dag = get_execution_plan(
        ray.data.range(10).repartition(2, keys=["id"], sort=True)._logical_plan
    )[0].dag
    assert sorted_dag._peak_memory_multiplier == 3
    sorted_dag.input_dependencies[0]._partition_bytes[0] = 100
    assert sorted_dag.incremental_resource_usage().memory == 300

    plain_dag = get_execution_plan(
        ray.data.range(10).repartition(2, keys=["id"])._logical_plan
    )[0].dag
    assert plain_dag._peak_memory_multiplier == SHUFFLE_PEAK_MEMORY_MULTIPLIER


@pytest.mark.parametrize("num_partitions", [1, 4, 8])
def test_external_repartition_keys_preserves_rows(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    disable_fallback_to_object_extension,
    num_partitions,
):
    """No rows are lost or duplicated; key totals are preserved."""
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.SHUFFLE_V2
    ctx.use_external_hash_shuffle = True

    ds = ray.data.range(1000, override_num_blocks=10)
    out = ds.repartition(num_partitions, keys=["id"])
    assert out.count() == 1000
    assert out.sum("id") == sum(range(1000))


def test_external_repartition_block_number_matched(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    disable_fallback_to_object_extension,
):
    """All-non-empty partitions => exactly num_partitions output blocks."""
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.SHUFFLE_V2
    ctx.use_external_hash_shuffle = True

    # 1000 distinct keys over 8 buckets => all 8 partitions are non-empty.
    ds = ray.data.range(1000, override_num_blocks=20)
    out = ds.repartition(8, keys=["id"]).materialize()
    assert out.num_blocks() == 8


def test_external_same_key_lands_in_same_block(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    disable_fallback_to_object_extension,
):
    """All rows sharing a key should end up in one block."""
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.SHUFFLE_V2
    ctx.use_external_hash_shuffle = True

    ds = ray.data.range(500, override_num_blocks=10).map(
        lambda row: {"k": row["id"] % 25, "v": row["id"]}
    )
    out = ds.repartition(5, keys=["k"])

    _assert_keys_colocated(_keys_per_block(out, ["k"]))
    assert out.count() == 500


def test_external_multi_column_keys(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    disable_fallback_to_object_extension,
):
    """Composite keys hash on all columns: every distinct (a, b) tuple lands in
    exactly one block."""
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.SHUFFLE_V2
    ctx.use_external_hash_shuffle = True

    ds = ray.data.range(500, override_num_blocks=10).map(
        lambda row: {"a": row["id"] % 5, "b": row["id"] % 7, "v": row["id"]}
    )
    out = ds.repartition(4, keys=["a", "b"])

    _assert_keys_colocated(_keys_per_block(out, ["a", "b"]))
    assert out.count() == 500


def test_external_more_partitions_than_keys_emits_empty_blocks(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    disable_fallback_to_object_extension,
):
    """Requesting more partitions than there are distinct keys emits the extra
    partitions as empty (0-row) blocks that still carry the dataset schema."""
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.SHUFFLE_V2
    ctx.use_external_hash_shuffle = True

    # 3 distinct keys into 50 partitions => at most 3 non-empty, >=47 empty.
    ds = ray.data.range(600, override_num_blocks=10).map(
        lambda row: {"k": row["id"] % 3, "v": row["id"]}
    )
    out = ds.repartition(50, keys=["k"]).materialize()

    assert out.count() == 600
    assert out.num_blocks() == 50

    rows_per_block = []
    schemas = []
    for ref_bundle in out.iter_internal_ref_bundles():
        for block_ref in ref_bundle.block_refs:
            block = cast(pa.Table, ray.get(block_ref))
            rows_per_block.append(block.num_rows)
            schemas.append(block.schema)

    assert rows_per_block.count(0) >= 47
    assert all(schema.equals(schemas[0]) for schema in schemas)

    _assert_keys_colocated(_keys_per_block(out, ["k"]))


def test_external_repartition_empty_dataset(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    disable_fallback_to_object_extension,
):
    """Empty dataset should still output N blocks"""
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.SHUFFLE_V2
    ctx.use_external_hash_shuffle = True

    ds = ray.data.range(100, override_num_blocks=4).filter(lambda row: False)
    out = ds.repartition(4, keys=["id"]).materialize()
    assert out.count() == 0
    assert out.num_blocks() == 4
    rows_per_block = [
        cast(pa.Table, ray.get(block_ref)).num_rows
        for ref_bundle in out.iter_internal_ref_bundles()
        for block_ref in ref_bundle.block_refs
    ]
    assert rows_per_block == [0, 0, 0, 0]


def test_external_repartition_preserves_null_typed_rows(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    disable_fallback_to_object_extension,
):
    """A null-typed column has rows with ``tbl.nbytes == 0``. Empty-gating
    by bytes would drop the partition; gating by ``num_rows`` must keep them.
    """
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.SHUFFLE_V2
    ctx.use_external_hash_shuffle = True

    table = pa.table({"k": pa.nulls(10)})
    assert table.num_rows == 10
    assert table.nbytes == 0

    out = ray.data.from_arrow(table).repartition(4, keys=["k"]).materialize()
    assert out.count() == 10
    assert out.num_blocks() == 4


def test_external_repartition_with_sort_produces_sorted_partitions(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    disable_fallback_to_object_extension,
):
    """Check that rows are sorted in every partition."""
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.SHUFFLE_V2
    ctx.use_external_hash_shuffle = True

    ds = ray.data.range(200, override_num_blocks=4)
    out = ds.repartition(4, keys=["id"], sort=True)

    for ref_bundle in out.iter_internal_ref_bundles():
        for block_ref in ref_bundle.block_refs:
            ids = cast(pa.Table, ray.get(block_ref))["id"].to_pylist()
            assert ids == sorted(ids)


def test_external_flag_off_keeps_object_store_path(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    disable_fallback_to_object_extension,
):
    """With flag=False the planner must NOT dispatch to the external variant.

    Row count alone can't distinguish the two paths, and the on-disk shuffle
    dir gets cleaned up eagerly by ``_do_shutdown`` so a post-hoc filesystem
    check races with cleanup. Patch ``ExternalHashShuffleMapOp.__init__`` to
    raise so any construction of the external op fails the test immediately.
    """
    from unittest import mock

    from ray.data._internal.execution.operators.shuffle_operators.external_shuffle_map_operator import (  # noqa: E501
        ExternalHashShuffleMapOp,
    )

    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.SHUFFLE_V2
    ctx.use_external_hash_shuffle = False

    with mock.patch.object(
        ExternalHashShuffleMapOp,
        "__init__",
        side_effect=AssertionError("external op was constructed with flag=False"),
    ):
        ds = ray.data.range(200).repartition(4, keys=["id"])
        assert ds.count() == 200


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
