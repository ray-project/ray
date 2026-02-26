"""Unit tests for ShuffleStrategy.GPU_SHUFFLE.

These tests do NOT require GPUs or the rapidsmpf/cudf/ucxx packages.
All Ray actor calls are mocked so the tests run on a standard CPU cluster.
"""

from typing import List
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

import ray
from ray.data._internal.execution.interfaces import (
    ExecutionResources,
    PhysicalOperator,
    RefBundle,
)
from ray.data._internal.gpu_shuffle.hash_shuffle import (
    GPURankPool,
    GPUShuffleActor,
    GPUShuffleOperator,
    _derive_num_gpu_ranks,
)
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.logical.operators import Repartition
from ray.data._internal.planner.plan_all_to_all_op import plan_all_to_all_op
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
    return op_mock


def _make_bundle(num_blocks: int = 1) -> RefBundle:
    """Return a RefBundle with *num_blocks* placeholder block refs."""
    meta = BlockMetadata(num_rows=10, size_bytes=100, exec_stats=None, input_files=None)
    blocks = [(ray.ObjectRef(bytes([i % 256]) * 28), meta) for i in range(num_blocks)]
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
        assert ctx.gpu_shuffle_rmm_pool_size == "auto"
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
            key_columns=["user_id"],
            rmm_pool_size="auto",
            spill_memory_limit="auto",
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
        ) as mock_ray_get:
            mock_actor_cls.options.return_value.remote.side_effect = mock_actor_handles
            # First ray.get returns (rank, root_address); second returns None list (setup done)
            mock_ray_get.side_effect = [(0, mock_root_address), [None, None, None]]

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

    def test_shutdown_without_force_clears_actors(self):
        pool = self._make_pool(nranks=2)
        pool._actors = [MagicMock(), MagicMock()]

        with patch("ray.data._internal.gpu_shuffle.hash_shuffle.ray.kill") as mock_kill:
            pool.shutdown(force=False)

        mock_kill.assert_not_called()
        assert pool.actors == []


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
            actor.insert_finished.remote.return_value = MagicMock()
            actor.extract_partitions.options.return_value.remote.return_value = (
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

    def test_insert_finished_called_on_all_ranks(self):
        op, mock_actors = self._make_op(nranks=2)
        op._inputs_complete = True

        with patch.object(op._reduce_metrics, "on_task_submitted"):
            op._try_finalize()

        for actor in mock_actors:
            actor.insert_finished.remote.assert_called_once()

    def test_extract_partitions_called_on_all_ranks(self):
        op, mock_actors = self._make_op(nranks=2)
        op._inputs_complete = True

        with patch.object(op._reduce_metrics, "on_task_submitted"):
            op._try_finalize()

        for actor in mock_actors:
            actor.extract_partitions.options.assert_called_once()

    def test_try_finalize_idempotent(self):
        op, mock_actors = self._make_op(nranks=2)
        op._inputs_complete = True

        with patch.object(op._reduce_metrics, "on_task_submitted"):
            op._try_finalize()
            op._try_finalize()  # second call should be no-op

        # insert_finished should only be called once per actor
        for actor in mock_actors:
            assert actor.insert_finished.remote.call_count == 1

    def test_has_next_false_initially(self):
        op, _ = self._make_op()
        op._inputs_complete = False
        assert not op.has_next()

    def test_has_next_true_when_output_queued(self):
        op, _ = self._make_op()
        bundle = _make_bundle(1)
        op._output_queue.append(bundle)
        # Mark finalization as done so has_next doesn't try to finalize
        op._finalization_started = True
        assert op.has_next()

    def test_get_next_inner_dequeues(self):
        op, _ = self._make_op()
        b1 = _make_bundle(1)
        b2 = _make_bundle(1)
        op._output_queue.extend([b1, b2])

        with patch.object(op._reduce_metrics, "on_output_dequeued"), patch.object(
            op._reduce_metrics, "on_output_taken"
        ):
            result = op._get_next_inner()

        assert result is b1
        assert len(op._output_queue) == 1

    def test_has_completed_false_while_extracting(self):
        op, _ = self._make_op()
        op._finalization_started = True
        op._extraction_tasks[0] = MagicMock()  # still running
        assert not op.has_completed()

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
            input_op=MagicMock(LogicalOperator),
            num_outputs=num_outputs,
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
        import numpy as np

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

    def test_insert_finished_succeeds_after_inserts(self, ray_with_gpu):
        """insert_finished() completes without error after a batch insert."""
        actor = self._make_setup_actor()
        table = pa.table({"id": [0, 1, 2], "v": [10, 20, 30]})
        ray.get(actor.insert_batch.remote(table))
        ray.get(actor.insert_finished.remote())  # must not raise
        ray.kill(actor)


# ---------------------------------------------------------------------------
# Single-rank end-to-end roundtrip (conditional)
# ---------------------------------------------------------------------------


@pytest.mark.gpu
class TestGPUSingleRankRoundtrip:
    """Full insert → insert_finished → extract_partitions roundtrip (1 GPU)."""

    @staticmethod
    def _collect_partitions(actor) -> List[pa.Table]:
        """Drain a streaming extract_partitions generator.

        extract_partitions follows the Ray Data streaming protocol: each
        partition yields a block (pa.Table) followed by a BlockMetadataWithSchema.
        Collect only the blocks.
        """

        gen = actor.extract_partitions.options(num_returns="streaming").remote()
        return [
            item for ref in gen for item in [ray.get(ref)] if isinstance(item, pa.Table)
        ]

    def _actor_with_data(
        self,
        table: pa.Table,
        key_columns: List[str],
        total_nparts: int = 2,
    ):
        """Create a single-rank actor, feed *table* into it, and signal done."""
        actor = GPUShuffleActor.options(num_gpus=1).remote(
            nranks=1,
            total_nparts=total_nparts,
            key_columns=key_columns,
        )
        _, root_address = ray.get(actor.setup_root.remote())
        ray.get(actor.setup_worker.remote(root_address))
        ray.get(actor.insert_batch.remote(table))
        ray.get(actor.insert_finished.remote())
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
        """extract_partitions yields pyarrow.Table objects."""
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

        ray.get(actor.insert_finished.remote())
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
# GPURankPool — real GPU lifecycle (conditional)
# ---------------------------------------------------------------------------


@pytest.mark.gpu
class TestGPURankPoolReal:
    """Tests that exercise GPURankPool with actual GPU actors."""

    def _make_pool(self, nranks: int = 1, total_nparts: int = 2) -> GPURankPool:
        return GPURankPool(
            nranks=nranks,
            total_nparts=total_nparts,
            key_columns=["id"],
            rmm_pool_size="auto",
            spill_memory_limit="auto",
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
        # Actor is fully set up by pool.start(); insert_finished should work immediately
        ray.get(actor.insert_finished.remote())
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
        assert "GPUShuffle" in ds._plan.explain()
        ds = ds.materialize()
        assert ds.num_blocks() == max(num_blocks, num_gpus)
        assert ds.count() == num_rows


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
