import pandas as pd
import ray

from ray.data._internal.execution.interfaces.execution_options import (
    ExecutionOptions,
)
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.map_operator import (
    MapOperator,
)
from ray.data._internal.execution.operators.base_physical_operator import (
    AllToAllOperator,
)
from ray.data._internal.execution.operators.map_transformer import (
    create_map_transformer_from_block_fn,
)
from ray.data._internal.execution.streaming_executor_state import (
    build_streaming_topology,
    update_operator_states,
)
from ray.data._internal.execution.util import make_ref_bundles
from ray.data.context import DataContext, ShuffleStrategy
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from ray.data._internal.logical.optimizers import get_execution_plan
from ray.data._internal.execution.operators.actor_pool_map_operator import (
    ActorPoolMapOperator,
)
from ray.data._internal.execution.operators.hash_shuffle import (
    HashShufflingOperatorBase,
)
from ray.data._internal.compute import ActorPoolStrategy


# ---------------------------------------------------------------------------
# simple helpers
# ---------------------------------------------------------------------------


def _mul2(block_iter, _ctx):
    """Dummy map: multiply every value by 2."""
    for blk in block_iter:
        yield pd.DataFrame({"id": [x * 2 for x in blk["id"]]})


_mul2_transformer = create_map_transformer_from_block_fn(_mul2)


def _identity_bulk(bundles, _ctx):
    """Dummy AllToAll bulk-fn that just forwards its input."""
    # NOTE: returns the same bundles, empty stats dict
    return bundles, {}


# ---------------------------------------------------------------------------
# tests
# ---------------------------------------------------------------------------


def _build_dag():
    """Input -> Map -> Shuffle(barrier) -> Map"""
    ctx = DataContext.get_current()

    # Upstream source with two tiny blocks.
    inp = InputDataBuffer(ctx, make_ref_bundles([[1, 2], [3]]))

    map1 = MapOperator.create(_mul2_transformer, inp, ctx, name="map1")

    shuffle = AllToAllOperator(
        _identity_bulk,
        map1,
        ctx,
        target_max_block_size=None,
        name="shuffle",
    )

    map2 = MapOperator.create(_mul2_transformer, shuffle, ctx, name="map2")
    return map2, (inp, map1, shuffle, map2)


def test_only_stage0_starts(ray_start_2_cpus_shared):
    final_op, (inp, map1, shuffle, map2) = _build_dag()

    # Build topology – only stage-0 ops should be started.
    topology, _ = build_streaming_topology(final_op, ExecutionOptions())

    # Input & first map & shuffle belong to stage 0, so they start immediately.
    assert inp._started, "Input should start immediately"
    assert map1._started, "First map should start immediately"
    assert shuffle._started, "Shuffle should start immediately"

    # Down-stream map must not start until the shuffle finishes.
    assert not map2._started, "Down-stream map must be lazy-started"


def test_downstream_starts_after_barrier(ray_start_2_cpus_shared):
    final_op, (inp, map1, shuffle, map2) = _build_dag()
    topology, _ = build_streaming_topology(final_op, ExecutionOptions())

    # Finish the shuffle stage manually.
    shuffle.mark_execution_finished()

    # Propagate state updates – this should trigger the lazy start.
    update_operator_states(topology)

    assert map2._started, "Down-stream operator should start after barrier"


# ---------------------------------------------------------------------------
def _write_parquet(tmp_path: Path, filename: str):
    tbl = pa.Table.from_pydict({"key": [1, 2], "val": [1, 2]})
    file_path = tmp_path / filename
    pq.write_table(tbl, file_path)
    return str(file_path)


def test_lazy_start_with_parquet(tmp_path, ray_start_2_cpus_shared):
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE
    # Two identical parquet sources
    path1 = _write_parquet(tmp_path, "d1.parquet")
    path2 = _write_parquet(tmp_path, "d2.parquet")

    # Build the Dataset pipeline under test.
    ds_left = (
        ray.data.read_parquet(path1)
        .repartition(num_blocks=2, shuffle=True, keys=["key"])  # keyed shuffle
        .map_batches(MultiplyByTwo, compute=ActorPoolStrategy(size=1))  # actor stage
        .map(lambda r: {"key": r["key"], "val": r["val"] + 1})
    )
    ds_right = ray.data.read_parquet(path2)
    ds = ds_left.join(ds_right, on=("key",), num_partitions=2, join_type="inner")

    # Get the physical DAG and build the streaming topology.
    phys_plan = get_execution_plan(ds._plan._logical_plan)
    dag = phys_plan.dag
    topology, _ = build_streaming_topology(dag, ExecutionOptions())

    # Locate operators of interest.
    shuffle = next(o for o in topology if isinstance(o, HashShufflingOperatorBase))
    actor_map = next(o for o in topology if isinstance(o, ActorPoolMapOperator))

    # Stage-0: shuffle already started, actor map not yet.
    assert shuffle._started
    assert not actor_map._started

    # Finish shuffle ⇒ actor map can now start.
    shuffle.mark_execution_finished()
    update_operator_states(topology)
    assert actor_map._started


class MultiplyByTwo:
    # one actor instance will be created and reused
    def __call__(self, df):
        return df.assign(val=df["val"] * 2)
