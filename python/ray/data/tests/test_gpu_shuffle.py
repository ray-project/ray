"""Unit tests for ShuffleStrategy.GPU_SHUFFLE.

These tests do NOT require GPUs or the rapidsmpf/cudf/ucxx packages.
All Ray actor calls are mocked so the tests run on a standard CPU cluster.
"""

from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

import ray
import ray.data._internal.gpu_shuffle.hash_aggregate as hash_aggregate
import ray.data._internal.gpu_shuffle.hash_shuffle as hash_shuffle
from ray.data._internal.execution.interfaces import (
    BlockEntry,
    ExecutionResources,
    PhysicalOperator,
    RefBundle,
)
from ray.data._internal.gpu_shuffle.hash_aggregate import (
    GPUAggregateFn,
    GPUHashAggregateActor,
    GPUHashAggregateOperator,
    build_gpu_aggregation_plan,
)
from ray.data._internal.gpu_shuffle.hash_shuffle import (
    GPURankPool,
    GPUShuffleActor,
    GPUShuffleOperator,
    _derive_num_gpu_ranks,
)
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.logical.operators import Aggregate, Repartition
from ray.data._internal.planner.plan_all_to_all_op import plan_all_to_all_op
from ray.data._internal.util import explain_plan
from ray.data.aggregate import AsList, Count, Max, Mean, Min, Sum
from ray.data.block import BlockMetadata
from ray.data.context import DataContext, ShuffleStrategy

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_input_op_mock(num_blocks=None, size_bytes=None):
    """Return a minimal PhysicalOperator mock compatible with HashShufflingOperatorBase."""
    logical_mock = MagicMock(LogicalOperator)
    logical_mock.infer_metadata.return_value = BlockMetadata(
        num_rows=None,
        size_bytes=size_bytes,
        exec_stats=None,
        input_files=None,
    )
    logical_mock.estimated_num_outputs.return_value = num_blocks

    op_mock = MagicMock(PhysicalOperator)
    op_mock._output_dependencies = []
    op_mock._logical_operators = [logical_mock]
    op_mock.num_output_splits.return_value = 1
    return op_mock


def _make_bundle(num_blocks: int = 1) -> RefBundle:
    """Return a RefBundle with *num_blocks* placeholder block refs."""
    meta = BlockMetadata(num_rows=10, size_bytes=100, exec_stats=None, input_files=None)
    blocks = [
        BlockEntry(ray.ObjectRef(bytes([i % 256]) * 28), meta)
        for i in range(num_blocks)
    ]
    return RefBundle(blocks, schema=None, owns_blocks=False)


def _make_data_context(
    *,
    gpu_shuffle_num_actors: int = 4,
    gpu_shuffle_rmm_pool_size="auto",
    gpu_shuffle_spill_memory_limit="auto",
) -> DataContext:
    ctx = DataContext()
    ctx._shuffle_strategy = ShuffleStrategy.GPU_SHUFFLE
    ctx.gpu_shuffle_num_actors = gpu_shuffle_num_actors
    ctx.gpu_shuffle_rmm_pool_size = gpu_shuffle_rmm_pool_size
    ctx.gpu_shuffle_spill_memory_limit = gpu_shuffle_spill_memory_limit
    return ctx


# ---------------------------------------------------------------------------
# Enum / DataContext field tests (no Ray required)
# ---------------------------------------------------------------------------


class TestDataContextGpuFields:
    def test_gpu_shuffle_default_values(self):
        ctx = DataContext()
        assert ctx.gpu_shuffle_num_actors is None
        assert ctx.gpu_shuffle_rmm_pool_size is None
        assert ctx.gpu_shuffle_spill_memory_limit == "auto"

    def test_gpu_shuffle_fields_settable(self):
        ctx = DataContext()
        ctx.shuffle_strategy = ShuffleStrategy.GPU_SHUFFLE
        ctx.gpu_shuffle_num_actors = 8
        ctx.gpu_shuffle_rmm_pool_size = 4 * 1024**3
        ctx.gpu_shuffle_spill_memory_limit = None
        assert ctx.shuffle_strategy == ShuffleStrategy.GPU_SHUFFLE
        assert ctx.gpu_shuffle_num_actors == 8
        assert ctx.gpu_shuffle_rmm_pool_size == 4 * 1024**3
        assert ctx.gpu_shuffle_spill_memory_limit is None


# ---------------------------------------------------------------------------
# Import isolation — gpu_shuffle.py must be importable without GPU packages
# ---------------------------------------------------------------------------


class TestImportIsolation:
    def test_module_importable_without_rapidsmpf(self):
        """The gpu_shuffle module must not import rapidsmpf at module level."""

        import ray.data._internal.gpu_shuffle.hash_shuffle as mod

        # If we got here the import succeeded on a CPU-only env.
        assert hasattr(mod, "GPUShuffleOperator")
        assert hasattr(mod, "GPURankPool")
        assert hasattr(mod, "GPUShuffleActor")

    def test_ray_data_importable_without_gpu_packages(self):
        import ray.data  # noqa: F401 — must not raise


# ---------------------------------------------------------------------------
# _derive_num_gpu_ranks
# ---------------------------------------------------------------------------


class TestDeriveNumGpuRanks:
    def test_explicit_count_used(self):
        ctx = DataContext()
        ctx.gpu_shuffle_num_actors = 7
        assert _derive_num_gpu_ranks(ctx) == 7

    def test_auto_detect_from_cluster(self):
        ctx = DataContext()
        ctx.gpu_shuffle_num_actors = None

        with patch(
            "ray.data._internal.gpu_shuffle.hash_shuffle"
            "._get_total_cluster_resources"
        ) as mock_res:
            mock_res.return_value = ExecutionResources(cpu=8, gpu=4)
            assert _derive_num_gpu_ranks(ctx) == 4

    def test_zero_gpus_raises(self):
        ctx = DataContext()
        ctx.gpu_shuffle_num_actors = None

        with patch(
            "ray.data._internal.gpu_shuffle.hash_shuffle"
            "._get_total_cluster_resources"
        ) as mock_res:
            mock_res.return_value = ExecutionResources(cpu=8, gpu=0)
            with pytest.raises(RuntimeError, match="GPU resources"):
                _derive_num_gpu_ranks(ctx)

    def test_fractional_gpu_count_truncated(self):
        """ExecutionResources.gpu may be fractional; int() truncates."""
        ctx = DataContext()
        ctx.gpu_shuffle_num_actors = None

        with patch(
            "ray.data._internal.gpu_shuffle.hash_shuffle"
            "._get_total_cluster_resources"
        ) as mock_res:
            mock_res.return_value = ExecutionResources(cpu=4, gpu=3.9)
            assert _derive_num_gpu_ranks(ctx) == 3


# ---------------------------------------------------------------------------
# GPURankPool
# ---------------------------------------------------------------------------


class TestGPURankPool:
    def _make_pool(self, nranks=4, total_nparts=8):
        return GPURankPool(
            nranks=nranks,
            total_nparts=total_nparts,
            setup_timeout_s=60.0,
            actor_cls_factory=lambda: hash_shuffle.GPUShuffleActor,
            actor_kwargs={
                "key_columns": ["user_id"],
                "columns": None,
                "rmm_pool_size": "auto",
                "spill_memory_limit": "auto",
            },
            log_label="GPUShufflePool",
        )

    def test_actors_empty_before_start(self):
        pool = self._make_pool()
        assert pool.actors == []

    def test_start_creates_correct_number_of_actors(self):
        pool = self._make_pool(nranks=3)

        mock_actor_handles = [MagicMock() for _ in range(3)]
        mock_root_address = b"ucxx://fake-address"

        # setup_root.remote() on rank 0 returns a future for (rank, address)
        mock_actor_handles[0].setup_root.remote.return_value = MagicMock()
        for handle in mock_actor_handles:
            handle.setup_worker.remote.return_value = MagicMock()

        with patch(
            "ray.data._internal.gpu_shuffle.hash_shuffle.GPUShuffleActor"
        ) as mock_actor_cls, patch(
            "ray.data._internal.gpu_shuffle.hash_shuffle.ray.get"
        ) as mock_ray_get, patch(
            "ray.data._internal.gpu_shuffle.hash_shuffle.ray.wait"
        ) as mock_ray_wait:
            mock_actor_cls.options.return_value.remote.side_effect = mock_actor_handles
            # First ray.get returns (rank, root_address); second returns None list (setup done)
            mock_ray_get.side_effect = [(0, mock_root_address), [None, None, None]]
            # ray.wait returns all refs as ready
            worker_refs = [
                h.setup_worker.remote.return_value for h in mock_actor_handles
            ]
            mock_ray_wait.return_value = (worker_refs, [])

            pool.start()

        assert len(pool.actors) == 3
        assert mock_actor_cls.options.call_count == 3

    def test_get_actor_for_block_round_robin(self):
        pool = self._make_pool(nranks=3)
        mock_actors = [MagicMock(name=f"actor_{i}") for i in range(3)]
        pool._actors = mock_actors

        # Blocks 0,1,2,3,4 should map to actors 0,1,2,0,1
        expected = [mock_actors[i % 3] for i in range(5)]
        actual = [pool.get_actor_for_block(i) for i in range(5)]
        assert actual == expected

    def test_shutdown_force_kills_actors(self):
        pool = self._make_pool(nranks=2)
        mock_actors = [MagicMock(), MagicMock()]
        pool._actors = mock_actors

        with patch("ray.data._internal.gpu_shuffle.hash_shuffle.ray.kill") as mock_kill:
            pool.shutdown(force=True)

        assert mock_kill.call_count == 2
        assert pool.actors == []
        assert pool.is_shutdown

    def test_shutdown_without_force_clears_actors(self):
        pool = self._make_pool(nranks=2)
        pool._actors = [MagicMock(), MagicMock()]

        with patch("ray.data._internal.gpu_shuffle.hash_shuffle.ray.kill") as mock_kill:
            pool.shutdown(force=False)

        mock_kill.assert_not_called()
        assert pool.actors == []
        assert pool.is_shutdown


# ---------------------------------------------------------------------------
# GPUShuffleOperator constructor
# ---------------------------------------------------------------------------


class TestGPUShuffleOperatorConstructor:
    def _make_op(self, num_partitions=None, nranks=4, default_parallelism=200):
        ctx = _make_data_context(gpu_shuffle_num_actors=nranks)
        ctx.default_hash_shuffle_parallelism = default_parallelism
        input_op = _make_input_op_mock()

        return GPUShuffleOperator(
            input_op,
            ctx,
            key_columns=("user_id",),
            num_partitions=num_partitions,
        )

    def test_name_contains_key_columns(self):
        op = self._make_op(num_partitions=8)
        assert "user_id" in op.name

    def test_name_contains_num_partitions(self):
        op = self._make_op(num_partitions=8)
        assert "8" in op.name

    def test_num_partitions_clamped_to_nranks(self):
        """When requested partitions < nranks, partitions is raised to nranks."""
        op = self._make_op(num_partitions=2, nranks=8)
        assert op._num_partitions == 8

    def test_num_partitions_exceeds_nranks_unchanged(self):
        op = self._make_op(num_partitions=16, nranks=4)
        assert op._num_partitions == 16

    def test_num_partitions_defaults_to_context_parallelism(self):
        op = self._make_op(num_partitions=None, nranks=4, default_parallelism=200)
        assert op._num_partitions == 200

    def test_base_resource_usage_is_nranks_gpus(self):
        op = self._make_op(nranks=6, num_partitions=6)
        usage = op.base_resource_usage
        assert usage.gpu == 6

    def test_current_logical_usage_reserves_nranks_before_pool_start(self):
        """Empty actor list before start must still reserve configured GPUs."""
        op = self._make_op(nranks=5, num_partitions=5)
        assert op.current_logical_usage().gpu == 5

    def test_current_logical_usage_matches_len_actors_when_running(self):
        op = self._make_op(nranks=4, num_partitions=4)
        op._rank_pool._actors = [MagicMock() for _ in range(4)]
        assert op.current_logical_usage().gpu == 4

    def test_current_logical_usage_zero_after_pool_shutdown(self):
        """Early actor release must drop logical GPU usage for the scheduler."""
        op = self._make_op(nranks=4, num_partitions=4)
        op._rank_pool._actors = [MagicMock() for _ in range(4)]
        op._rank_pool.shutdown(force=False)
        assert op.current_logical_usage().gpu == 0

    def test_incremental_resource_usage_is_one_gpu(self):
        op = self._make_op()
        usage = op.incremental_resource_usage()
        assert usage.gpu == 1

    def test_progress_bar_names(self):
        op = self._make_op()
        names = op.get_sub_progress_bar_names()
        assert names == ["GPU Shuffle", "GPU Reduce"]

    def test_set_sub_progress_bar_shuffle(self):
        op = self._make_op()
        mock_bar = MagicMock()
        op.set_sub_progress_bar("GPU Shuffle", mock_bar)
        assert op._shuffle_bar is mock_bar

    def test_set_sub_progress_bar_reduce(self):
        op = self._make_op()
        mock_bar = MagicMock()
        op.set_sub_progress_bar("GPU Reduce", mock_bar)
        assert op._reduce_bar is mock_bar

    def test_initial_state(self):
        op = self._make_op()
        assert op._next_block_idx == 0
        assert op._insert_tasks == {}
        assert op._extraction_tasks == {}
        assert not op._finalization_started
        assert len(op._output_queue) == 0


# ---------------------------------------------------------------------------
# GPUShuffleOperator: _add_input_inner block routing
# ---------------------------------------------------------------------------


class TestGPUShuffleOperatorInputRouting:
    def _make_op_with_mock_pool(self, nranks=3, num_partitions=6):
        ctx = _make_data_context(gpu_shuffle_num_actors=nranks)
        input_op = _make_input_op_mock()
        op = GPUShuffleOperator(
            input_op, ctx, key_columns=("k",), num_partitions=num_partitions
        )
        # Replace the real pool with a mock
        mock_actors = [MagicMock(name=f"actor_{i}") for i in range(nranks)]
        for actor in mock_actors:
            actor.insert_batch.remote.return_value = MagicMock()
        op._rank_pool._actors = mock_actors
        op._rank_pool._nranks = nranks
        return op, mock_actors

    def test_single_block_routed_to_first_actor(self):
        op, actors = self._make_op_with_mock_pool(nranks=3)
        bundle = _make_bundle(num_blocks=1)
        op._add_input_inner(bundle, input_index=0)

        actors[0].insert_batch.remote.assert_called_once()

    def test_round_robin_across_three_ranks(self):
        op, actors = self._make_op_with_mock_pool(nranks=3)

        # Submit 6 single-block bundles
        for _ in range(6):
            op._add_input_inner(_make_bundle(1), input_index=0)

        # Each actor should have received exactly 2 blocks
        for actor in actors:
            assert actor.insert_batch.remote.call_count == 2

    def test_block_idx_increments_per_block(self):
        op, actors = self._make_op_with_mock_pool(nranks=3)

        bundle_with_2 = _make_bundle(num_blocks=2)
        op._add_input_inner(bundle_with_2, input_index=0)

        assert op._next_block_idx == 2

    def test_insert_tasks_tracked(self):
        op, actors = self._make_op_with_mock_pool(nranks=2)
        op._add_input_inner(_make_bundle(1), 0)
        assert len(op._insert_tasks) == 1

    def test_insert_task_callback_removes_task(self):
        op, actors = self._make_op_with_mock_pool(nranks=2)
        op._add_input_inner(_make_bundle(1), 0)
        # Grab the callback and invoke it
        task = list(op._insert_tasks.values())[0]
        assert 0 in op._insert_tasks
        task._task_done_callback()
        assert 0 not in op._insert_tasks


# ---------------------------------------------------------------------------
# GPUShuffleOperator: finalization and completion
# ---------------------------------------------------------------------------


class TestGPUShuffleOperatorFinalization:
    def _make_op(self, nranks=2, num_partitions=4):
        ctx = _make_data_context(gpu_shuffle_num_actors=nranks)
        input_op = _make_input_op_mock()
        op = GPUShuffleOperator(
            input_op, ctx, key_columns=("k",), num_partitions=num_partitions
        )
        mock_actors = [MagicMock(name=f"actor_{i}") for i in range(nranks)]
        for actor in mock_actors:
            actor.finish_and_extract.options.return_value.remote.return_value = (
                MagicMock()
            )
        op._rank_pool._actors = mock_actors
        op._rank_pool._nranks = nranks
        return op, mock_actors

    def test_finalization_not_started_until_inputs_complete(self):
        op, _ = self._make_op()
        op._inputs_complete = False
        op._try_finalize()
        assert not op._finalization_started

    def test_finalization_not_started_while_inserts_pending(self):
        op, _ = self._make_op()
        op._inputs_complete = True
        op._insert_tasks[0] = MagicMock()  # fake pending insert
        op._try_finalize()
        assert not op._finalization_started

    def test_finalization_starts_after_all_inserts_done(self):
        op, mock_actors = self._make_op(nranks=2)
        op._inputs_complete = True
        # No pending inserts

        with patch.object(op._reduce_metrics, "on_task_submitted"):
            op._try_finalize()

        assert op._finalization_started

    def test_finish_and_extract_called_on_all_ranks(self):
        op, mock_actors = self._make_op(nranks=2)
        op._inputs_complete = True

        with patch.object(op._reduce_metrics, "on_task_submitted"):
            op._try_finalize()

        for actor in mock_actors:
            actor.finish_and_extract.options.assert_called_once()

    def test_try_finalize_idempotent(self):
        op, mock_actors = self._make_op(nranks=2)
        op._inputs_complete = True

        with patch.object(op._reduce_metrics, "on_task_submitted"):
            op._try_finalize()
            op._try_finalize()  # second call should be no-op

        # finish_and_extract should only be called once per actor
        for actor in mock_actors:
            assert actor.finish_and_extract.options.call_count == 1

    def test_has_next_false_initially(self):
        op, _ = self._make_op()
        op._inputs_complete = False
        assert not op.has_next()

    def test_has_next_true_when_output_queued(self):
        op, _ = self._make_op()
        bundle = _make_bundle(1)
        op._output_queue.add(bundle, key=0)
        op._output_queue.finalize(key=0)
        op._finalization_started = True
        assert op.has_next()

    def test_get_next_inner_dequeues(self):
        op, _ = self._make_op()
        b1 = _make_bundle(1)
        b2 = _make_bundle(1)
        op._output_queue.add(b1, key=0)
        op._output_queue.finalize(key=0)
        op._output_queue.add(b2, key=1)
        op._output_queue.finalize(key=1)

        with patch.object(op._reduce_metrics, "on_output_dequeued"), patch.object(
            op._reduce_metrics, "on_output_taken"
        ):
            result = op._get_next_inner()

        assert result is b1
        assert op._output_queue.has_next()

    def test_has_completed_false_while_extracting(self):
        op, _ = self._make_op()
        op._finalization_started = True
        op._extraction_tasks[0] = MagicMock()  # still running
        assert not op.has_completed()

    def test_output_order_is_partition_order_regardless_of_arrival(self):
        """Bundles arriving out of order must be emitted in ascending partition order."""
        op, _ = self._make_op(nranks=2, num_partitions=4)
        op._finalization_started = True

        # Build 4 bundles, insert in reverse order (3, 2, 1, 0)
        bundles = {}
        for partition_id in reversed(range(4)):
            meta = BlockMetadata(
                num_rows=1, size_bytes=8, exec_stats=None, input_files=None
            )
            bundle = RefBundle(
                [BlockEntry(ray.ObjectRef(bytes([partition_id]) * 28), meta)],
                schema=None,
                owns_blocks=False,
            )
            bundles[partition_id] = bundle
            op._output_queue.add(bundle, key=partition_id)
            op._output_queue.finalize(key=partition_id)

        with patch.object(op._reduce_metrics, "on_output_dequeued"), patch.object(
            op._reduce_metrics, "on_output_taken"
        ):
            results = [op._get_next_inner() for _ in range(4)]

        # Output must be in partition order 0, 1, 2, 3 — not insertion order 3, 2, 1, 0
        assert results == [bundles[i] for i in range(4)]

    def test_get_active_tasks_combines_both_phases(self):
        op, _ = self._make_op()
        insert_task = MagicMock()
        extract_task = MagicMock()
        op._insert_tasks[0] = insert_task
        op._extraction_tasks[0] = extract_task

        active = op.get_active_tasks()
        assert insert_task in active
        assert extract_task in active
        assert len(active) == 2

    def test_shutdown_clears_tasks_and_kills_actors(self):
        op, mock_actors = self._make_op(nranks=2)
        op._insert_tasks[0] = MagicMock()
        op._extraction_tasks[0] = MagicMock()
        expected_kill_count = len(mock_actors)

        with patch(
            "ray.data._internal.gpu_shuffle.hash_shuffle.ray.kill"
        ) as mock_kill, patch.object(
            PhysicalOperator, "_do_shutdown", return_value=None
        ):
            op._do_shutdown(force=True)

        assert op._insert_tasks == {}
        assert op._extraction_tasks == {}
        assert mock_kill.call_count == expected_kill_count


# ---------------------------------------------------------------------------
# plan_all_to_all_op routing
# ---------------------------------------------------------------------------


class TestPlanAllToAllOpRouting:
    """Verify that plan_all_to_all_op routes GPU_SHUFFLE to GPUShuffleOperator."""

    def _make_repartition_op(self, keys=("user_id",), num_outputs=8):
        return Repartition(
            num_outputs=num_outputs,
            input_dependencies=[MagicMock(LogicalOperator)],
            shuffle=True,
            keys=list(keys),
        )

    def test_gpu_shuffle_routes_to_gpu_operator(self):
        ctx = DataContext()
        ctx.gpu_shuffle_num_actors = 4
        ctx._shuffle_strategy = ShuffleStrategy.GPU_SHUFFLE

        logical_op = self._make_repartition_op(keys=["user_id"], num_outputs=8)
        input_physical_op = _make_input_op_mock()

        op = plan_all_to_all_op(logical_op, [input_physical_op], ctx)

        assert isinstance(op, GPUShuffleOperator)

    def test_hash_shuffle_still_routes_to_hash_operator(self):
        from ray.data._internal.execution.operators.hash_shuffle import (
            HashShuffleOperator,
        )

        ctx = DataContext()
        ctx._shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE

        with patch(
            "ray.data._internal.execution.operators.hash_shuffle"
            "._get_total_cluster_resources",
            return_value=ExecutionResources(cpu=4, gpu=0),
        ), patch(
            "ray.data._internal.execution.operators.hash_shuffle.ray.put",
            return_value=MagicMock(),
        ):
            logical_op = self._make_repartition_op(keys=["user_id"], num_outputs=8)
            input_physical_op = _make_input_op_mock()
            op = plan_all_to_all_op(logical_op, [input_physical_op], ctx)

        assert isinstance(op, HashShuffleOperator)

    def test_unsupported_strategy_with_keys_raises(self):
        ctx = DataContext()
        ctx._shuffle_strategy = ShuffleStrategy.SORT_SHUFFLE_PULL_BASED

        logical_op = self._make_repartition_op(keys=["user_id"], num_outputs=8)
        input_physical_op = _make_input_op_mock()

        with pytest.raises(ValueError, match="HASH_SHUFFLE"):
            plan_all_to_all_op(logical_op, [input_physical_op], ctx)

    def test_gpu_shuffle_respects_num_outputs(self):
        ctx = DataContext()
        ctx.gpu_shuffle_num_actors = 4
        ctx._shuffle_strategy = ShuffleStrategy.GPU_SHUFFLE

        logical_op = self._make_repartition_op(keys=["id"], num_outputs=16)
        input_physical_op = _make_input_op_mock()
        op = plan_all_to_all_op(logical_op, [input_physical_op], ctx)

        assert op._num_partitions == 16

    def test_gpu_shuffle_key_columns_normalised(self):
        """Key columns from SortKey.get_columns() should propagate correctly."""
        ctx = DataContext()
        ctx.gpu_shuffle_num_actors = 4
        ctx._shuffle_strategy = ShuffleStrategy.GPU_SHUFFLE

        logical_op = self._make_repartition_op(keys=["col_a", "col_b"], num_outputs=8)
        input_physical_op = _make_input_op_mock()
        op = plan_all_to_all_op(logical_op, [input_physical_op], ctx)

        assert "col_a" in op._key_columns
        assert "col_b" in op._key_columns


# ---------------------------------------------------------------------------
# GPU hash aggregate planning
# ---------------------------------------------------------------------------


class TestGPUHashAggregatePlanning:
    def _make_aggregate_op(
        self,
        aggs,
        key="user_id",
        num_partitions=8,
        input_schema=pa.schema([("user_id", pa.int64()), ("value", pa.int64())]),
    ):
        input_op = MagicMock(LogicalOperator)
        input_op.infer_schema.return_value = input_schema
        return Aggregate(
            key=key,
            aggs=list(aggs),
            input_dependencies=[input_op],
            num_partitions=num_partitions,
        )

    def test_builtin_aggregation_plan_supported(self):
        schema = pa.schema([("user_id", pa.int64()), ("value", pa.int64())])
        plan = build_gpu_aggregation_plan(
            ("user_id",),
            (
                Count(),
                Count(on="value", ignore_nulls=True),
                Sum("value"),
                Min("value"),
                Max("value"),
                Mean("value"),
            ),
            input_schema=schema,
        )

        assert plan is not None
        assert plan.shuffle_key_columns == ("user_id",)
        assert plan.output_names == (
            "count()",
            "count(value)",
            "sum(value)",
            "min(value)",
            "max(value)",
            "mean(value)",
        )
        assert tuple(type(agg).__name__ for agg in plan._gpu_aggregates) == (
            "GPUCount",
            "GPUCount",
            "GPUSum",
            "GPUMin",
            "GPUMax",
            "GPUMean",
        )
        assert "user_id" in plan.required_columns
        assert "value" in plan.required_columns

    def test_unsupported_aggregation_plan_rejected(self):
        schema = pa.schema([("user_id", pa.int64())])
        assert (
            build_gpu_aggregation_plan(
                ("user_id",), (AsList("value"),), input_schema=schema
            )
            is None
        )
        assert build_gpu_aggregation_plan(("user_id",), (Sum("value"),)) is None

    def test_gpu_shuffle_routes_supported_aggregate_to_gpu_operator(self):
        ctx = DataContext()
        ctx.gpu_shuffle_num_actors = 4
        ctx._shuffle_strategy = ShuffleStrategy.GPU_SHUFFLE

        logical_op = self._make_aggregate_op([Count(), Sum("value")])
        input_physical_op = _make_input_op_mock()
        original_build_plan = hash_aggregate.build_gpu_aggregation_plan
        built_plans: List[Optional[hash_aggregate.GPUAggregationPlan]] = []

        def _build_plan_once(
            *args: Any, **kwargs: Any
        ) -> Optional[hash_aggregate.GPUAggregationPlan]:
            plan = original_build_plan(*args, **kwargs)
            built_plans.append(plan)
            return plan

        with patch(
            "ray.data._internal.gpu_shuffle.hash_aggregate.build_gpu_aggregation_plan",
            side_effect=_build_plan_once,
        ) as mock_build_plan:
            op = plan_all_to_all_op(logical_op, [input_physical_op], ctx)

        assert isinstance(op, GPUHashAggregateOperator)
        assert op._num_partitions == 8
        assert "GPUHashAggregate" in op.name
        mock_build_plan.assert_called_once()
        assert op._aggregation_plan is built_plans[0]

    def test_gpu_hash_aggregate_operator_uses_prebuilt_plan(self):
        ctx = DataContext()
        ctx.gpu_shuffle_num_actors = 4
        ctx._shuffle_strategy = ShuffleStrategy.GPU_SHUFFLE
        input_physical_op = _make_input_op_mock()
        schema = pa.schema([("user_id", pa.int64()), ("value", pa.int64())])
        aggregation_plan = build_gpu_aggregation_plan(
            ("user_id",), (Count(), Sum("value")), input_schema=schema
        )

        with patch(
            "ray.data._internal.gpu_shuffle.hash_shuffle.GPURankPool"
        ) as mock_default_pool:
            op = GPUHashAggregateOperator(
                ctx,
                input_physical_op,
                key_columns=("user_id",),
                aggregation_plan=aggregation_plan,
                num_partitions=8,
            )

        mock_default_pool.assert_not_called()
        assert op._aggregation_plan is aggregation_plan
        assert op._rank_pool.nranks == 4

    def test_gpu_shuffle_unsupported_aggregate_falls_back_to_cpu_hash_aggregate(self):
        from ray.data._internal.execution.operators.hash_aggregate import (
            HashAggregateOperator,
        )

        ctx = DataContext()
        ctx.gpu_shuffle_num_actors = 4
        ctx._shuffle_strategy = ShuffleStrategy.GPU_SHUFFLE

        logical_op = self._make_aggregate_op([AsList("value")])
        input_physical_op = _make_input_op_mock(num_blocks=8, size_bytes=1024)

        with patch(
            "ray.data._internal.execution.operators.hash_shuffle"
            "._get_total_cluster_resources",
            return_value=ExecutionResources(cpu=4, memory=1024 * 1024 * 1024),
        ), patch(
            "ray.data._internal.execution.operators.hash_shuffle.ray.put",
            return_value=MagicMock(),
        ):
            op = plan_all_to_all_op(logical_op, [input_physical_op], ctx)

        assert isinstance(op, HashAggregateOperator)

    def test_gpu_shuffle_missing_key_schema_falls_back_to_cpu_hash_aggregate(self):
        from ray.data._internal.execution.operators.hash_aggregate import (
            HashAggregateOperator,
        )

        ctx = DataContext()
        ctx.gpu_shuffle_num_actors = 4
        ctx._shuffle_strategy = ShuffleStrategy.GPU_SHUFFLE

        logical_op = self._make_aggregate_op([Sum("value")], input_schema=None)
        input_physical_op = _make_input_op_mock(num_blocks=8, size_bytes=1024)

        with patch(
            "ray.data._internal.execution.operators.hash_shuffle"
            "._get_total_cluster_resources",
            return_value=ExecutionResources(cpu=4, memory=1024 * 1024 * 1024),
        ), patch(
            "ray.data._internal.execution.operators.hash_shuffle.ray.put",
            return_value=MagicMock(),
        ):
            op = plan_all_to_all_op(logical_op, [input_physical_op], ctx)

        assert isinstance(op, HashAggregateOperator)

    def test_global_aggregate_uses_synthetic_shuffle_key(self):
        plan = build_gpu_aggregation_plan(tuple(), (Count(), Sum("value")))

        assert plan is not None
        assert plan.shuffle_key_columns == (hash_aggregate._GLOBAL_AGGREGATE_KEY,)

    def test_required_columns_excludes_unused_input_columns(self):
        table = pa.table(
            {
                "user_id": pa.array([0, 0, 1], type=pa.int64()),
                "small": pa.array([1, 2, 3], type=pa.int64()),
                "unused": pa.array([[1], [2], [3]], type=pa.list_(pa.int64())),
            }
        )
        plan = build_gpu_aggregation_plan(
            ("user_id",), (Count(), Sum("small")), input_schema=table.schema
        )
        assert plan is not None
        assert plan.required_columns == ("user_id", "small")

        projected = table.select(list(plan.required_columns))
        assert projected.column_names == ["user_id", "small"]

    def test_global_count_required_columns_empty(self):
        table = pa.table({"unused": pa.array([10, 20, 30], type=pa.int64())})
        plan = build_gpu_aggregation_plan(
            tuple(), (Count(),), input_schema=table.schema
        )
        assert plan is not None
        assert plan.required_columns == ()

    def test_merge_input_schema_unifies_value_dtype_across_blocks(self):
        block1 = pa.table({"value": pa.array([1], type=pa.int8())})
        block2 = pa.table({"value": pa.array([2], type=pa.int32())})
        plan = build_gpu_aggregation_plan(tuple(), (Sum("value"),))
        assert plan is not None

        runtime_schema = None
        runtime_schema = plan.merge_input_schema(runtime_schema, block1.schema)
        runtime_schema = plan.merge_input_schema(runtime_schema, block2.schema)

        assert runtime_schema.field("value").type == pa.int32()

    def test_merge_input_schema_uses_logical_schema_for_shuffle_keys(self):
        logical_schema = pa.schema(
            [
                ("user_id", pa.int32()),
                ("value", pa.int64()),
            ]
        )
        plan = build_gpu_aggregation_plan(
            ("user_id",), (Sum("value"),), input_schema=logical_schema
        )
        assert plan is not None

        runtime_schema = plan.merge_input_schema(
            None, pa.schema([("user_id", pa.int8()), ("value", pa.int64())])
        )
        runtime_schema = plan.merge_input_schema(
            runtime_schema,
            pa.schema([("user_id", pa.int32()), ("value", pa.int64())]),
        )

        assert runtime_schema.field("user_id").type == pa.int32()
        assert runtime_schema.field("value").type == pa.int64()

    def test_normalize_output_arrow_null_schema(self):
        null_schema = pa.schema(
            [
                ("user_id", pa.int64()),
                ("value", pa.null()),
            ]
        )
        null_plan = build_gpu_aggregation_plan(
            ("user_id",), (Sum("value"),), input_schema=null_schema
        )
        output_table = pa.table(
            {
                "user_id": pa.array([0, 1, 2], type=pa.int64()),
                "sum(value)": pa.array([None, None, None], type=pa.int64()),
            }
        )

        assert null_plan.normalize_output_arrow(output_table).schema == pa.schema(
            [
                ("user_id", pa.int64()),
                ("sum(value)", pa.null()),
            ]
        )


# ---------------------------------------------------------------------------
# GPUShuffleActor: deferred import guard
# ---------------------------------------------------------------------------


class TestGPUShuffleActorImportGuard:
    """GPUShuffleActor.__init__ must raise ImportError with a helpful message
    when rapidsmpf is not installed, not a generic ModuleNotFoundError."""

    def test_missing_rapidsmpf_raises_import_error(self):
        from ray.data._internal.gpu_shuffle.hash_shuffle import GPUShuffleActor

        # Access the underlying class (bypass Ray actor wrapper)
        ActorClass = GPUShuffleActor.__ray_actor_class__

        with patch.dict(
            "sys.modules",
            {"ray.data._internal.gpu_shuffle.rapidsmpf_backend": None},
        ):
            with pytest.raises(ImportError, match="rapidsmpf"):
                ActorClass(
                    nranks=2,
                    total_nparts=4,
                    key_columns=["k"],
                )


# ---------------------------------------------------------------------------
# GPU fixtures — shared by all real-GPU test classes below
# ---------------------------------------------------------------------------


def _num_cluster_gpus() -> int:
    """Return the number of GPUs in the Ray cluster (0 if Ray not initialised)."""
    if not ray.is_initialized():
        return 0
    return int(ray.cluster_resources().get("GPU", 0))


@pytest.fixture(scope="module")
def ray_with_gpu():
    """Skip the test if GPU packages or GPU hardware are absent.

    Imports ``cudf``, ``rapidsmpf``, and ``ucp`` (ucxx Python bindings) with
    ``pytest.importorskip`` so the skip message clearly names the missing
    package.  Also initialises Ray (if not already running) and checks that
    at least one GPU is visible in the cluster.
    """
    pytest.importorskip("cudf", reason="cudf (GPU DataFrame library) not installed")
    pytest.importorskip("rapidsmpf", reason="rapidsmpf not installed")

    if not ray.is_initialized():
        ray.init()

    num_gpus = _num_cluster_gpus()
    if num_gpus < 1:
        pytest.skip("No GPU resources found in the Ray cluster")

    yield num_gpus


# ---------------------------------------------------------------------------
# GPUShuffleActor — real GPU paths (conditional)
# ---------------------------------------------------------------------------


@pytest.mark.gpu
class TestGPUShuffleActorReal:
    """Exercises GPU actor methods on actual hardware.

    All tests are skipped automatically when GPU packages or GPU resources are
    absent.  Run them explicitly with ``pytest -m gpu``.
    """

    def _make_setup_actor(self, total_nparts: int = 2, key_columns=None):
        """Create, UCXX-initialise, and return a single-rank GPUShuffleActor."""
        key_columns = key_columns or ["id"]
        actor = GPUShuffleActor.options(num_gpus=1).remote(
            nranks=1,
            total_nparts=total_nparts,
            key_columns=key_columns,
        )
        _, root_address = ray.get(actor.setup_root.remote())
        ray.get(actor.setup_worker.remote(root_address))
        return actor

    def test_actor_init_succeeds(self, ray_with_gpu):
        """GPUShuffleActor.__init__ succeeds (rapidsmpf import guard passes)."""
        actor = GPUShuffleActor.options(num_gpus=1).remote(
            nranks=1,
            total_nparts=2,
            key_columns=["id"],
        )
        ray.kill(actor)

    def test_setup_root_returns_rank_and_address(self, ray_with_gpu):
        """setup_root() returns a (rank, address_bytes) tuple for UCXX setup."""
        actor = GPUShuffleActor.options(num_gpus=1).remote(
            nranks=1, total_nparts=1, key_columns=["k"]
        )
        rank, addr = ray.get(actor.setup_root.remote())
        assert isinstance(rank, int)
        assert isinstance(addr, bytes)
        assert len(addr) > 0
        ray.kill(actor)

    def test_insert_batch_returns_row_count(self, ray_with_gpu):
        """insert_batch() returns the number of rows in the Arrow batch."""
        actor = self._make_setup_actor()
        table = pa.table({"id": [1, 2, 3], "val": [0.1, 0.2, 0.3]})
        count = ray.get(actor.insert_batch.remote(table))
        assert count == 3
        ray.kill(actor)

    def test_insert_batch_large_table(self, ray_with_gpu):
        """insert_batch handles a larger Arrow Table without error."""

        n = 5_000
        actor = self._make_setup_actor(total_nparts=4)
        table = pa.table(
            {
                "id": pa.array(np.arange(n, dtype=np.int64)),
                "val": pa.array(np.random.rand(n)),
            }
        )
        count = ray.get(actor.insert_batch.remote(table))
        assert count == n
        ray.kill(actor)

    def test_insert_batch_multiple_batches(self, ray_with_gpu):
        """Multiple insert_batch calls each return the correct row count."""
        actor = self._make_setup_actor(total_nparts=2)
        sizes = [3, 7, 5]
        offset = 0
        for size in sizes:
            table = pa.table(
                {
                    "id": list(range(offset, offset + size)),
                    "label": ["x"] * size,
                }
            )
            count = ray.get(actor.insert_batch.remote(table))
            assert count == size
            offset += size
        ray.kill(actor)

    def test_finish_and_extract_succeeds_after_inserts(self, ray_with_gpu):
        """finish_and_extract() completes without error after a batch insert."""
        actor = self._make_setup_actor()
        table = pa.table({"id": [0, 1, 2], "v": [10, 20, 30]})
        ray.get(actor.insert_batch.remote(table))
        gen = actor.finish_and_extract.options(num_returns="streaming").remote()
        # Drain the generator to ensure it completes.
        for ref in gen:
            ray.get(ref)
        ray.kill(actor)


# ---------------------------------------------------------------------------
# Single-rank end-to-end roundtrip (conditional)
# ---------------------------------------------------------------------------


@pytest.mark.gpu
class TestGPUSingleRankRoundtrip:
    """Full insert → finish_and_extract roundtrip (1 GPU)."""

    @staticmethod
    def _collect_partitions(actor) -> List[pa.Table]:
        """Drain a streaming finish_and_extract generator.

        finish_and_extract follows the Ray Data streaming protocol: each
        partition yields a block (pa.Table) followed by a BlockMetadataWithSchema.
        Collect only the blocks.
        """

        gen = actor.finish_and_extract.options(num_returns="streaming").remote()
        return [
            item for ref in gen for item in [ray.get(ref)] if isinstance(item, pa.Table)
        ]

    def _actor_with_data(
        self,
        table: pa.Table,
        key_columns: List[str],
        total_nparts: int = 2,
    ):
        """Create a single-rank actor, feed *table* into it, return ready actor."""
        actor = GPUShuffleActor.options(num_gpus=1).remote(
            nranks=1,
            total_nparts=total_nparts,
            key_columns=key_columns,
        )
        _, root_address = ray.get(actor.setup_root.remote())
        ray.get(actor.setup_worker.remote(root_address))
        ray.get(actor.insert_batch.remote(table))
        return actor

    def test_roundtrip_preserves_row_count(self, ray_with_gpu):
        """All inserted rows appear in the extracted partitions."""
        n_rows = 30
        table = pa.table(
            {
                "key": list(range(n_rows)),
                "data": [float(i) for i in range(n_rows)],
            }
        )
        actor = self._actor_with_data(table, ["key"], total_nparts=3)
        partitions = self._collect_partitions(actor)
        assert sum(t.num_rows for t in partitions) == n_rows
        ray.kill(actor)

    def test_roundtrip_output_is_arrow_tables(self, ray_with_gpu):
        """finish_and_extract yields pyarrow.Table objects."""
        table = pa.table({"id": [1, 2, 3, 4], "name": ["a", "b", "c", "d"]})
        actor = self._actor_with_data(table, ["id"], total_nparts=2)
        partitions = self._collect_partitions(actor)
        for part in partitions:
            assert isinstance(part, pa.Table)
        ray.kill(actor)

    def test_roundtrip_multiple_batches_no_rows_lost(self, ray_with_gpu):
        """Rows from multiple insert_batch calls are all recovered."""
        actor = GPUShuffleActor.options(num_gpus=1).remote(
            nranks=1, total_nparts=2, key_columns=["k"]
        )
        _, root_address = ray.get(actor.setup_root.remote())
        ray.get(actor.setup_worker.remote(root_address))

        batch_sizes = [4, 6, 10]
        offset = 0
        for size in batch_sizes:
            table = pa.table({"k": list(range(offset, offset + size)), "v": [0] * size})
            ray.get(actor.insert_batch.remote(table))
            offset += size

        partitions = self._collect_partitions(actor)
        assert sum(t.num_rows for t in partitions) == sum(batch_sizes)
        ray.kill(actor)

    def test_roundtrip_column_names_preserved(self, ray_with_gpu):
        """Column names in extracted partitions match the inserted schema."""
        col_names = ["alpha", "beta", "gamma"]
        table = pa.table({"alpha": [1, 2], "beta": [3.0, 4.0], "gamma": ["x", "y"]})
        actor = self._actor_with_data(table, ["alpha"], total_nparts=1)
        partitions = self._collect_partitions(actor)
        for part in partitions:
            if part.num_rows > 0:
                for name in col_names:
                    assert name in part.schema.names
        ray.kill(actor)

    def test_roundtrip_key_column_hash_partitions_consistently(self, ray_with_gpu):
        """Each key value is always routed to exactly one partition."""
        # Hash partitioning guarantees that all rows sharing a key land in the
        # same partition, but makes no promise that *distinct* keys go to
        # *distinct* partitions (collisions are possible, especially with few
        # partitions).  Test the actual guarantee: no key is split across
        # multiple partitions.
        n_rows, n_keys = 100, 10
        table = pa.table(
            {
                "group": [i % n_keys for i in range(n_rows)],
                "val": list(range(n_rows)),
            }
        )
        actor = self._actor_with_data(table, ["group"], total_nparts=2)
        all_partitions = self._collect_partitions(actor)
        ray.kill(actor)

        # For each key, collect the set of partition indices it appears in.
        key_to_part_indices: dict = {}
        for idx, part in enumerate(all_partitions):
            for key in part.column("group").unique().to_pylist():
                key_to_part_indices.setdefault(key, set()).add(idx)

        for key, part_indices in key_to_part_indices.items():
            assert (
                len(part_indices) == 1
            ), f"Key {key!r} was split across partitions {part_indices}"


# ---------------------------------------------------------------------------
# GPUHashAggregateActor — real GPU aggregate paths (conditional)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# GPUAggregationPlan — real cuDF execution (conditional)
# ---------------------------------------------------------------------------


@pytest.mark.gpu
class TestGPUAggregationPlanReal:
    """Exercises aggregation plan methods with real cuDF on GPU hardware."""

    def test_sum_and_mean_schema_source_dtypes_use_cudf_accumulators(
        self, ray_with_gpu
    ):
        import cudf

        schema = pa.schema(
            [
                ("user_id", pa.int64()),
                ("small", pa.int8()),
                ("flag", pa.bool_()),
                ("ratio", pa.float32()),
            ]
        )
        plan = build_gpu_aggregation_plan(
            ("user_id",),
            (Sum("small"), Mean("flag"), Sum("ratio")),
            input_schema=schema,
        )
        assert plan is not None

        df = cudf.DataFrame(
            {
                "user_id": [0, 0, 1],
                "small": np.array([100, 100, 1], dtype=np.int8),
                "flag": [True, True, False],
                "ratio": np.array([1.5, 2.5, 3.5], dtype=np.float32),
            }
        )
        partial = plan.partial_aggregate(df, input_schema=schema)

        sum_col = plan.accumulator_columns[0]
        mean_sum_col = plan.accumulator_columns[1]
        float_sum_col = plan.accumulator_columns[4]
        group_zero = partial[partial["user_id"] == 0].iloc[0]

        assert str(partial[sum_col].dtype) == "int64"
        assert str(partial[mean_sum_col].dtype) == "int64"
        assert str(partial[float_sum_col].dtype) == "float64"
        assert group_zero[sum_col] == 200
        assert group_zero[mean_sum_col] == 2
        assert group_zero[float_sum_col] == pytest.approx(4.0)

    def test_empty_final_aggregate_preserves_output_dtypes(self, ray_with_gpu):
        import cudf

        schema = pa.schema(
            [
                ("user_id", pa.int64()),
                ("value", pa.int8()),
            ]
        )
        plan = build_gpu_aggregation_plan(
            ("user_id",),
            (Count(), Sum("value"), Min("value"), Mean("value")),
            input_schema=schema,
        )
        assert plan is not None

        result = plan.final_aggregate(cudf.DataFrame())
        result_table = result.to_arrow(preserve_index=False)
        expected_schema = pa.schema(
            [
                ("user_id", pa.int64()),
                ("count()", pa.int64()),
                ("sum(value)", pa.int64()),
                ("min(value)", pa.int64()),
                ("mean(value)", pa.float64()),
            ]
        )
        assert result_table.schema.equals(expected_schema, check_metadata=False)

        global_plan = build_gpu_aggregation_plan(
            tuple(), (Count(),), input_schema=schema
        )
        assert global_plan is not None

        global_result = global_plan.final_aggregate(cudf.DataFrame())
        global_table = global_result.to_arrow(preserve_index=False)
        assert global_table.schema.equals(
            pa.schema([("count()", pa.int64())]), check_metadata=False
        )

        runtime_plan = build_gpu_aggregation_plan(
            ("user_id",),
            (Count(), Sum("value")),
            input_schema=pa.schema(
                [
                    ("user_id", pa.int64()),
                    ("value", pa.int8()),
                ]
            ),
        )
        assert runtime_plan is not None

        runtime_result = runtime_plan.final_aggregate(
            cudf.DataFrame(),
            input_schema=pa.schema(
                [
                    ("user_id", pa.int64()),
                    ("value", pa.int8()),
                ]
            ),
        )
        runtime_table = runtime_result.to_arrow(preserve_index=False)
        assert runtime_table.schema.equals(
            pa.schema(
                [
                    ("user_id", pa.int64()),
                    ("count()", pa.int64()),
                    ("sum(value)", pa.int64()),
                ]
            ),
            check_metadata=False,
        )

    def test_custom_gpu_aggregate_fn_receives_input_schema(self, ray_with_gpu):
        import cudf

        class _CustomGPUAggregate(GPUAggregateFn):
            def __init__(self) -> None:
                super().__init__(
                    "custom(value)",
                    zero_factory=lambda: 0,
                    on="value",
                    ignore_nulls=True,
                )
                self.seen_schema: Optional[pa.Schema] = None

            def aggregate_block(self, block: Any) -> int:
                return 0

            def combine(self, current_accumulator: int, new: int) -> int:
                return current_accumulator + new

            def gpu_accumulator_columns(
                self, accumulator_prefix: str
            ) -> Tuple[str, ...]:
                return (f"{accumulator_prefix}_acc",)

            def gpu_partial_aggregate(
                self,
                df: cudf.DataFrame,
                key_columns: Tuple[str, ...],
                *,
                output_name: str,
                accumulator_prefix: str,
                input_schema: Any = None,
            ) -> cudf.DataFrame:
                self.seen_schema = input_schema
                acc_col = self.gpu_accumulator_columns(accumulator_prefix)[0]
                return df[[key_columns[0], "value"]].rename(columns={"value": acc_col})

            def gpu_final_aggregate(
                self,
                df: cudf.DataFrame,
                key_columns: Tuple[str, ...],
                *,
                output_name: str,
                accumulator_prefix: str,
            ) -> cudf.DataFrame:
                acc_col = self.gpu_accumulator_columns(accumulator_prefix)[0]
                return df[[key_columns[0], acc_col]].rename(
                    columns={acc_col: output_name}
                )

            def gpu_partial_accumulator_dtypes(
                self,
                df: cudf.DataFrame,
                accumulator_prefix: str,
                *,
                input_schema: Any = None,
            ) -> Dict[str, Any]:
                acc_col = self.gpu_accumulator_columns(accumulator_prefix)[0]
                return {acc_col: df["value"].dtype}

            def gpu_final_output_dtypes(
                self,
                df: cudf.DataFrame,
                output_name: str,
                accumulator_prefix: str,
                *,
                input_schema: Any = None,
            ) -> Dict[str, Any]:
                acc_col = self.gpu_accumulator_columns(accumulator_prefix)[0]
                return {output_name: df[acc_col].dtype}

        schema = pa.schema([("user_id", pa.int64()), ("value", pa.int32())])
        gpu_agg = _CustomGPUAggregate()
        plan = build_gpu_aggregation_plan(("user_id",), (gpu_agg,), input_schema=schema)
        assert plan is not None

        partial = plan.partial_aggregate(
            cudf.DataFrame({"user_id": [1], "value": np.array([2], dtype=np.int32)}),
            input_schema=schema,
        )
        result = plan.final_aggregate(partial, input_schema=schema)

        assert gpu_agg.seen_schema is schema
        assert partial.to_pandas().to_dict("list") == {
            "user_id": [1],
            plan.accumulator_columns[0]: [2],
        }
        assert str(partial[plan.accumulator_columns[0]].dtype) == "int32"
        assert result.to_pandas().to_dict("list") == {
            "user_id": [1],
            "custom(value)": [2],
        }
        assert str(result["custom(value)"].dtype) == "int32"

    def test_null_reductions_preserve_groups_and_accumulator_dtypes(
        self, ray_with_gpu, monkeypatch
    ):
        import cudf

        original_group_with_optional_reduction = (
            hash_aggregate._BuiltinGPUAggregateFn._group_with_optional_reduction
        )
        original_gpu_partial_aggregate = hash_aggregate.GPUCount.gpu_partial_aggregate

        def _drop_all_null_group_with_optional_reduction(
            self,
            df,
            key_columns,
            *,
            value_column,
            size_col,
            count_col,
            aggregate_name,
            output_column,
            output_dtype,
        ):
            result, count_dtype = original_group_with_optional_reduction(
                self,
                df,
                key_columns,
                value_column=value_column,
                size_col=size_col,
                count_col=count_col,
                aggregate_name=aggregate_name,
                output_column=output_column,
                output_dtype=output_dtype,
            )
            if hash_aggregate._all_counts_zero(result, count_col):
                return result, count_dtype
            grouped = df.groupby(list(key_columns), dropna=False)
            out = grouped[value_column].count().reset_index()
            counts = out.rename(columns={out.columns[-1]: "__count"})
            non_null_keys = counts[counts["__count"] > 0][list(key_columns)]
            result = result.merge(non_null_keys, on=list(key_columns), how="inner")
            if output_column in result.columns:
                result[output_column] = result[output_column].astype("float64")
            return result, count_dtype

        def _gpu_partial_aggregate_drop_zero_counts(
            self,
            df,
            key_columns,
            *,
            output_name,
            accumulator_prefix,
            input_schema=None,
        ):
            acc_col = self.gpu_accumulator_columns(accumulator_prefix)[0]
            if self.target_column is None or not self.ignore_nulls:
                return original_gpu_partial_aggregate(
                    self,
                    df,
                    key_columns,
                    output_name=output_name,
                    accumulator_prefix=accumulator_prefix,
                    input_schema=input_schema,
                )

            grouped = df.groupby(list(key_columns), dropna=False)
            result = grouped.agg(
                **{acc_col: (self.target_column, "count")}
            ).reset_index()
            count_dtype = hash_aggregate._cudf_column_dtype(result, acc_col)
            counts = result[result[acc_col] > 0]
            result = result[list(key_columns)].merge(
                counts, on=list(key_columns), how="left"
            )
            hash_aggregate._fill_missing_count(result, acc_col, count_dtype)
            return result[list(key_columns) + [acc_col]]

        monkeypatch.setattr(
            hash_aggregate._BuiltinGPUAggregateFn,
            "_group_with_optional_reduction",
            _drop_all_null_group_with_optional_reduction,
        )
        monkeypatch.setattr(
            hash_aggregate.GPUCount,
            "gpu_partial_aggregate",
            _gpu_partial_aggregate_drop_zero_counts,
        )

        schema = pa.schema([("user_id", pa.int64())])
        plan = build_gpu_aggregation_plan(
            ("user_id",), (Sum("value"),), input_schema=schema
        )
        assert plan is not None
        acc_col = plan.accumulator_columns[0]

        df = cudf.DataFrame(
            {
                "user_id": [0, 1, 2, 0],
                "value": cudf.Series([None, None, None, None], dtype="int64"),
            }
        )
        partial = plan.partial_aggregate(df)
        assert str(partial[acc_col].dtype) == "int64"

        result = (
            plan.final_aggregate(partial)
            .to_pandas()
            .sort_values("user_id")
            .reset_index(drop=True)
        )
        assert result["user_id"].tolist() == [0, 1, 2]
        assert result["sum(value)"].isna().all()

        count_plan = build_gpu_aggregation_plan(
            ("user_id",),
            (Count("value", ignore_nulls=True),),
            input_schema=schema,
        )
        assert count_plan is not None

        count_partial = count_plan.partial_aggregate(df)
        count_result = (
            count_plan.final_aggregate(count_partial)
            .to_pandas()
            .sort_values("user_id")
            .reset_index(drop=True)
        )
        assert count_result.to_dict("records") == [
            {"user_id": 0, "count(value)": 0},
            {"user_id": 1, "count(value)": 0},
            {"user_id": 2, "count(value)": 0},
        ]

    def test_partial_aggregate_widens_shuffle_key_to_logical_schema(self, ray_with_gpu):
        import cudf

        logical_schema = pa.schema(
            [
                ("user_id", pa.int32()),
                ("value", pa.int64()),
            ]
        )
        plan = build_gpu_aggregation_plan(
            ("user_id",), (Sum("value"),), input_schema=logical_schema
        )
        assert plan is not None

        df = cudf.DataFrame(
            {
                "user_id": np.array([1], dtype=np.int8),
                "value": np.array([1], dtype=np.int64),
            }
        )
        partial = plan.partial_aggregate(
            df,
            input_schema=pa.schema([("user_id", pa.int8()), ("value", pa.int64())]),
        )

        assert partial["user_id"].iloc[0] == 1
        assert str(partial["user_id"].dtype) == "int32"

    def test_global_count_partial_aggregate(self, ray_with_gpu):
        import cudf

        plan = build_gpu_aggregation_plan(tuple(), (Count(),))
        assert plan is not None

        df = cudf.DataFrame(index=range(3))
        partial = plan.partial_aggregate(df)
        assert partial[plan.accumulator_columns[0]].iloc[0] == 3

    def test_normalize_partial_output_null_key_column(self, ray_with_gpu):
        import cudf

        nan_key_plan = build_gpu_aggregation_plan(
            ("item",), (Count(),), input_schema=pa.schema([("item", pa.null())])
        )
        nan_key_partial = cudf.DataFrame(
            {
                "item": cudf.Series([None], dtype="float64"),
                nan_key_plan.accumulator_columns[0]: np.array([1], dtype=np.int64),
            }
        )
        normalized_nan_key_partial = nan_key_plan._normalize_partial_output(
            nan_key_partial,
            nan_key_partial,
            ("item",),
            input_schema=pa.schema([("item", pa.null())]),
        )

        assert str(normalized_nan_key_partial["item"].dtype) == "float64"
        assert (
            str(normalized_nan_key_partial[nan_key_plan.accumulator_columns[0]].dtype)
            == "int64"
        )

    def test_normalize_partial_output_unknown_schema_widening(self, ray_with_gpu):
        import cudf

        unknown_schema_plan = build_gpu_aggregation_plan(tuple(), (Sum("B"),))
        assert unknown_schema_plan is not None
        unknown_schema_acc_col = unknown_schema_plan.accumulator_columns[0]
        int_input = cudf.DataFrame({"A": [0], "B": np.array([1], dtype=np.int64)})
        int_partial = cudf.DataFrame(
            {
                "A": np.array([0], dtype=np.int64),
                unknown_schema_acc_col: np.array([1], dtype=np.int64),
            }
        )
        double_input = cudf.DataFrame(
            {"A": [0], "B": np.array([1.0], dtype=np.float64)}
        )
        double_partial = cudf.DataFrame(
            {
                "A": np.array([0], dtype=np.int64),
                unknown_schema_acc_col: np.array([1.0], dtype=np.float64),
            }
        )

        normalized_int_partial = unknown_schema_plan._normalize_partial_output(
            int_partial,
            int_input,
            ("A",),
            input_schema=pa.schema([("A", pa.int64()), ("B", pa.int64())]),
        )
        normalized_double_partial = unknown_schema_plan._normalize_partial_output(
            double_partial,
            double_input,
            ("A",),
            input_schema=pa.schema([("A", pa.int64()), ("B", pa.float64())]),
        )

        assert str(normalized_int_partial[unknown_schema_acc_col].dtype) == "int64"
        assert str(normalized_double_partial[unknown_schema_acc_col].dtype) == "float64"

    def test_unsupported_cudf_dtype_cast_is_noop(self, monkeypatch):
        cudf = pytest.importorskip("cudf")

        df = cudf.DataFrame({"value": np.array([1, 2], dtype=np.int64)})

        def raise_type_error(_self):
            raise TypeError("unsupported cuDF dtype")

        monkeypatch.setattr(hash_aggregate.DataType, "to_cudf_type", raise_type_error)

        hash_aggregate._cast_column_to_dtype(
            df, "value", hash_aggregate.DataType.from_numpy("int64")
        )

        assert df["value"].to_pandas().tolist() == [1, 2]
        assert str(df["value"].dtype) == "int64"


# ---------------------------------------------------------------------------
# GPUHashAggregateActor — real GPU paths (conditional)
# ---------------------------------------------------------------------------


@pytest.mark.gpu
class TestGPUHashAggregateActorReal:
    """Exercises GPU aggregate methods on actual hardware."""

    def _make_setup_actor(self, aggregation_plan, total_nparts: int = 2):
        actor = GPUHashAggregateActor.options(num_gpus=1).remote(
            nranks=1,
            total_nparts=total_nparts,
            aggregation_plan=aggregation_plan,
        )
        _, root_address = ray.get(actor.setup_root.remote())
        ray.get(actor.setup_worker.remote(root_address))
        return actor

    @staticmethod
    def _collect_tables(actor) -> List[pa.Table]:
        gen = actor.finish_and_extract.options(num_returns="streaming").remote()
        return [
            item for ref in gen for item in [ray.get(ref)] if isinstance(item, pa.Table)
        ]

    @staticmethod
    def _collect_frame(actor) -> pd.DataFrame:
        tables = TestGPUHashAggregateActorReal._collect_tables(actor)
        frames = [table.to_pandas() for table in tables if table.num_rows > 0]
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    def test_grouped_builtin_aggregates_real_gpu(self, ray_with_gpu):
        table = pa.table(
            {
                "group": pa.array([0, 0, 1, 1, 1, 2], type=pa.int64()),
                "value": pa.array([1, None, 2, 3, None, 10], type=pa.int64()),
            }
        )
        plan = build_gpu_aggregation_plan(
            ("group",),
            (
                Count(),
                Count("value", ignore_nulls=True),
                Sum("value"),
                Min("value"),
                Max("value"),
                Mean("value"),
            ),
            input_schema=table.schema,
        )
        assert plan is not None

        actor = self._make_setup_actor(plan)
        try:
            assert ray.get(actor.insert_batch.remote(table)) == table.num_rows
            result = (
                self._collect_frame(actor).sort_values("group").reset_index(drop=True)
            )
        finally:
            ray.kill(actor)

        assert result["group"].tolist() == [0, 1, 2]
        assert result["count()"].tolist() == [2, 3, 1]
        assert result["count(value)"].tolist() == [1, 2, 1]
        assert result["sum(value)"].tolist() == [1, 5, 10]
        assert result["min(value)"].tolist() == [1, 2, 10]
        assert result["max(value)"].tolist() == [1, 3, 10]
        assert result["mean(value)"].tolist() == pytest.approx([1.0, 2.5, 10.0])

    def test_grouped_aggregate_output_has_unique_join_keys(self, ray_with_gpu):
        import cudf

        schema = pa.schema(
            [
                ("key1", pa.int64()),
                ("key2", pa.int64()),
                ("value", pa.int64()),
            ]
        )
        plan = build_gpu_aggregation_plan(
            ("key1", "key2"),
            (
                Min(on="value", alias_name="min_value"),
                Count(alias_name="row_count"),
            ),
            input_schema=schema,
        )
        assert plan is not None

        df = cudf.DataFrame(
            {
                "key1": [0, 0, 1],
                "key2": [10, 10, 20],
                "value": [5, 6, 7],
            }
        )
        partial = plan.partial_aggregate(df, input_schema=schema)
        result = plan.final_aggregate(partial, input_schema=schema)
        result_table = result.to_arrow(preserve_index=False)

        assert result_table.column_names == ["key1", "key2", "min_value", "row_count"]
        assert len(result_table.column_names) == len(set(result_table.column_names))

        key_table = pa.table(
            {
                "key1": pa.array([0, 1], type=pa.int64()),
                "key2": pa.array([10, 20], type=pa.int64()),
            }
        )
        joined = key_table.join(
            result_table,
            keys=["key1", "key2"],
            join_type="inner",
        )
        assert joined.num_rows == 2

    def test_global_builtin_aggregates_real_gpu(self, ray_with_gpu):
        plan = build_gpu_aggregation_plan(
            tuple(),
            (
                Count(),
                Count("value", ignore_nulls=True),
                Sum("value"),
                Mean("value"),
            ),
        )
        assert plan is not None

        table = pa.table({"value": pa.array([1, None, 2, 5], type=pa.int64())})
        actor = self._make_setup_actor(plan, total_nparts=1)
        try:
            assert ray.get(actor.insert_batch.remote(table)) == table.num_rows
            result = self._collect_frame(actor)
        finally:
            ray.kill(actor)

        assert result.columns.tolist() == [
            "count()",
            "count(value)",
            "sum(value)",
            "mean(value)",
        ]
        assert len(result) == 1
        row = result.iloc[0].to_dict()
        assert row["count()"] == 4
        assert row["count(value)"] == 3
        assert row["sum(value)"] == 8
        assert row["mean(value)"] == pytest.approx(8 / 3)


# ---------------------------------------------------------------------------
# GPURankPool — real GPU lifecycle (conditional)
# ---------------------------------------------------------------------------


@pytest.mark.gpu
class TestGPURankPoolReal:
    """Tests that exercise GPURankPool with actual GPU actors."""

    def _make_pool(self, nranks: int = 1, total_nparts: int = 2) -> GPURankPool:
        return GPURankPool(
            nranks=nranks,
            total_nparts=total_nparts,
            setup_timeout_s=60.0,
            actor_cls_factory=lambda: hash_shuffle.GPUShuffleActor,
            actor_kwargs={
                "key_columns": ["id"],
                "columns": None,
                "rmm_pool_size": "auto",
                "spill_memory_limit": "auto",
            },
            log_label="GPUShufflePool",
        )

    def test_pool_start_creates_actors(self, ray_with_gpu):
        """GPURankPool.start() creates the expected number of actors."""
        pool = self._make_pool(nranks=1)
        pool.start()
        assert len(pool.actors) == 1
        pool.shutdown(force=True)

    def test_pool_shutdown_clears_actors(self, ray_with_gpu):
        """GPURankPool.shutdown(force=True) kills actors and empties the list."""
        pool = self._make_pool(nranks=1)
        pool.start()
        pool.shutdown(force=True)
        assert pool.actors == []

    def test_pool_actors_respond_after_start(self, ray_with_gpu):
        """Actors returned by the pool respond to remote calls after start()."""
        pool = self._make_pool(nranks=1, total_nparts=1)
        pool.start()
        actor = pool.actors[0]
        # Actor is fully set up by pool.start(); insert_batch should work immediately
        table = pa.table({"id": [1], "v": [2]})
        ray.get(actor.insert_batch.remote(table))
        pool.shutdown(force=True)


# ---------------------------------------------------------------------------
# GPU Hash Shuffle - end to end
# ---------------------------------------------------------------------------


@pytest.mark.gpu
class TestGPUHashShuffle:
    def test_hash_shuffle(self, ray_with_gpu):
        """Test that hash shuffle works end to end."""
        # ray.init(num_gpus=1)
        num_gpus = ray_with_gpu
        ray.data.context.DataContext.get_current().shuffle_strategy = (
            ShuffleStrategy.GPU_SHUFFLE
        )

        num_rows = 10000
        parallelism = 1000
        num_blocks = int(parallelism / 10)

        ds = ray.data.range(num_rows, parallelism=parallelism).materialize()
        ds = ds.repartition(keys=["id"], num_blocks=num_blocks)
        assert "GPUShuffle" in explain_plan(ds._logical_plan)
        ds = ds.materialize()
        assert ds.num_blocks() == max(num_blocks, num_gpus)
        assert ds.count() == num_rows


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
