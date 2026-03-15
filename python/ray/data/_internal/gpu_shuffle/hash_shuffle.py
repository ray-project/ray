"""GPU-native shuffle operator using RAPIDS MPF (rapidsmpf) + UCXX.

This module provides an opt-in ``ShuffleStrategy.GPU_SHUFFLE`` strategy for
Ray Data repartition operations that avoids the CPU/object-store round-trip
incurred by the default hash-shuffle path.

Dependencies (all optional at import time):
    rapidsmpf  — RAPIDS Multi-GPU Partition Framework
    cudf       — GPU DataFrame library
    ucxx       — UCX Python bindings for point-to-point GPU transfer

Import errors are deferred to actor construction so that CPU-only environments
can still ``import ray.data`` without failure.
"""
import functools
import logging
import typing
from collections import deque
from typing import (
    Deque,
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
)

import pyarrow as pa

import ray
from ray.actor import ActorHandle
from ray.data import ExecutionOptions
from ray.data._internal.execution.interfaces import (
    ExecutionResources,
    PhysicalOperator,
    RefBundle,
)
from ray.data._internal.execution.interfaces.physical_operator import (
    DataOpTask,
    MetadataOpTask,
    OpTask,
    estimate_total_num_of_blocks,
)
from ray.data._internal.execution.operators.hash_shuffle import (
    _get_total_cluster_resources,
)
from ray.data._internal.execution.operators.sub_progress import SubProgressBarMixin
from ray.data._internal.stats import OpRuntimeMetrics
from ray.data.block import BlockStats, to_stats
from ray.data.context import DataContext

if typing.TYPE_CHECKING:
    import cudf

    from ray.data._internal.progress.base_progress import BaseProgressBar

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# GPU shuffle actor
# ---------------------------------------------------------------------------


@ray.remote(num_gpus=1)
class GPUShuffleActor:
    """One GPU rank in a RAPIDS MPF-based distributed shuffle.

    Each instance wraps a ``BulkRapidsMPFShuffler`` via composition rather than
    inheritance to keep CPU-only environments unaffected.

    Actors are arranged in a virtual communicator ring coordinated
    through UCXX; data never passes through the Ray object store or the CPU
    after initial ingestion.

    Constructor is intentionally lightweight — expensive UCXX setup happens in
    :meth:`setup_worker`, which is called once from :class:`GPURankPool`.
    """

    def __init__(
        self,
        nranks: int,
        total_nparts: int,
        key_columns: List[str],
        rmm_pool_size: Union[int, str, None] = None,
        spill_memory_limit: Union[int, str, None] = "auto",
    ):
        from ray.data._internal.gpu_shuffle.rapidsmpf_backend import (
            BulkRapidsMPFShuffler,
        )

        self._shuffler = BulkRapidsMPFShuffler(
            nranks=nranks,
            total_nparts=total_nparts,
            shuffle_on=key_columns,
            rmm_pool_size=rmm_pool_size,
            spill_memory_limit=spill_memory_limit,
        )
        self._columns: Optional[List[str]] = None

    # ------------------------------------------------------------------
    # UCXX communicator setup
    # ------------------------------------------------------------------

    def setup_root(self) -> tuple[int, bytes]:
        """Initialize the root communicator and return ``(rank, root_address_bytes)``.

        Only called on rank 0; the returned address is broadcast to all ranks
        via :meth:`setup_worker`.
        """
        return self._shuffler.setup_root()

    def setup_worker(self, root_address: bytes) -> None:
        """Finish UCXX communicator setup and create the internal shuffler.

        Must be called on *every* rank (including rank 0) after
        :meth:`get_root_address` has been called on rank 0 and its result
        broadcast to all ranks.
        """
        self._shuffler.setup_worker(root_address)

    # ------------------------------------------------------------------
    # Insert / extract interface (called by GPUShuffleOperator)
    # ------------------------------------------------------------------

    def insert_batch(self, batch: Union[pa.Table, "cudf.DataFrame"]) -> int:
        """Hash-partition *batch* (Arrow Table or cuDF DataFrame) and route
        shards to peers.

        Returns the number of rows in the incoming batch so the driver can
        track throughput without serialising the data back.
        """
        import cudf

        if isinstance(batch, pa.Table):
            df = cudf.DataFrame.from_arrow(batch)
        else:
            # Already a cuDF DataFrame (e.g. when the upstream op is GPU-native)
            df = batch

        if self._columns is None:
            self._columns = list(df.columns)

        self._shuffler.insert_chunk(table=df, column_names=self._columns)
        return len(df)

    def insert_finished(self) -> None:
        """Signal that no more batches will be inserted into this rank."""
        self._shuffler.insert_finished()

    def extract_partitions(self) -> Iterator:
        """Yield one Arrow Table per output partition assigned to this rank.

        This is a streaming generator; it must be called *after*
        :meth:`insert_finished` (Ray's per-actor task ordering guarantees
        this when both calls are submitted to the same actor handle).

        Follows the Ray Data streaming generator protocol: yield block then
        BlockMetadataWithSchema for each output partition.
        """
        from rapidsmpf.utils.cudf import pylibcudf_to_cudf_dataframe

        from ray.data.block import BlockExecStats, BlockMetadataWithSchema

        columns = self._columns or []
        for _, partition in self._shuffler.extract():
            exec_stats_builder = BlockExecStats.builder()
            cdf = pylibcudf_to_cudf_dataframe(partition, column_names=columns).copy(
                deep=True
            )
            block = cdf.to_arrow(preserve_index=False)
            exec_stats = exec_stats_builder.build()
            stats = yield block
            if stats:
                exec_stats.block_ser_time_s = stats.object_creation_dur_s
            yield BlockMetadataWithSchema.from_block(block, block_exec_stats=exec_stats)


# ---------------------------------------------------------------------------
# GPURankPool — lifecycle manager for a set of GPUShuffleActors
# ---------------------------------------------------------------------------


class GPURankPool:
    """Manages the lifecycle of ``GPUShuffleActor`` instances.

    Analogous to ``AggregatorPool`` in the CPU hash-shuffle path, but for GPU
    ranks coordinated through UCXX.
    """

    def __init__(
        self,
        nranks: int,
        total_nparts: int,
        key_columns: List[str],
        rmm_pool_size: Union[int, str, None],
        spill_memory_limit: Union[int, str, None],
    ):
        self._nranks = nranks
        self._total_nparts = total_nparts
        self._key_columns = key_columns
        self._rmm_pool_size = rmm_pool_size
        self._spill_memory_limit = spill_memory_limit
        self._actors: List[ActorHandle] = []

    @property
    def nranks(self) -> int:
        return self._nranks

    @property
    def actors(self) -> List[ActorHandle]:
        return self._actors

    def start(self) -> None:
        """Create actors and coordinate UCXX setup.

        This call *blocks* until all actors have finished UCXX initialisation.
        It is invoked once from ``GPUShuffleOperator.start()`` before any data
        flows through the pipeline.
        """
        self._actors = [
            GPUShuffleActor.options(num_gpus=1, scheduling_strategy="SPREAD",).remote(
                nranks=self._nranks,
                total_nparts=self._total_nparts,
                key_columns=self._key_columns,
                rmm_pool_size=self._rmm_pool_size,
                spill_memory_limit=self._spill_memory_limit,
            )
            for _ in range(self._nranks)
        ]

        # Rank 0 establishes the root communicator; all ranks connect to it.
        _, root_address_bytes = ray.get(self._actors[0].setup_root.remote())
        ray.get(
            [actor.setup_worker.remote(root_address_bytes) for actor in self._actors]
        )

    def get_actor_for_block(self, block_idx: int) -> ActorHandle:
        """Round-robin distribution of input blocks across ranks."""
        return self._actors[block_idx % self._nranks]

    def shutdown(self, force: bool = False) -> None:
        if force:
            for actor in self._actors:
                ray.kill(actor)
        self._actors.clear()


# ---------------------------------------------------------------------------
# Helper: derive number of GPU ranks from the cluster
# ---------------------------------------------------------------------------


def _derive_num_gpu_ranks(data_context: DataContext) -> int:
    """Return the configured or auto-detected number of GPU ranks."""
    if data_context.gpu_shuffle_num_actors is not None:
        return data_context.gpu_shuffle_num_actors

    total_resources = _get_total_cluster_resources()
    num_gpus = int(total_resources.gpu or 0)
    if num_gpus == 0:
        raise RuntimeError(
            "ShuffleStrategy.GPU_SHUFFLE requires GPU resources in the cluster. "
            "Set DataContext.gpu_shuffle_num_actors to override the number of ranks."
        )
    return num_gpus


# ---------------------------------------------------------------------------
# GPUShuffleOperator
# ---------------------------------------------------------------------------


class GPUShuffleOperator(PhysicalOperator, SubProgressBarMixin):
    """GPU-native shuffle operator using RAPIDS MPF + UCXX.

    Unlike the CPU ``HashShuffleOperator``, this operator:

    * Uses UCXX point-to-point communication instead of the Ray object store
      for inter-rank data movement.
    * Accepts Arrow Tables from upstream (converting to cuDF on the actor) so
      it remains compatible with non-GPU upstream operators.
    * Supports repartition-only (no reduce/aggregate phase on the driver side).

    Lifecycle::

        start()                    # creates actors, blocks for UCXX setup
        _add_input_inner(bundle)   # routes blocks to actors round-robin
        [inputs_done()]            # called by the executor
        has_next() / _get_next_inner()   # streams output bundles

    The ``insert_finished`` + ``extract_partitions`` actor tasks are submitted
    as fire-and-forget once all inserts complete; Ray's per-actor task ordering
    guarantees correct sequencing without blocking the driver.
    """

    def __init__(
        self,
        input_op: PhysicalOperator,
        data_context: DataContext,
        *,
        key_columns: Tuple[str, ...],
        num_partitions: Optional[int] = None,
    ):
        nranks = _derive_num_gpu_ranks(data_context)
        target_num_partitions = (
            num_partitions or data_context.default_hash_shuffle_parallelism
        )
        # rapidsmpf requires total_nparts >= nranks
        target_num_partitions = max(target_num_partitions, nranks)

        super().__init__(
            name=(
                f"GPUShuffle("
                f"key_columns={key_columns}, "
                f"num_partitions={target_num_partitions})"
            ),
            input_dependencies=[input_op],
            data_context=data_context,
        )

        self._key_columns = key_columns
        self._num_partitions = target_num_partitions
        self._rank_pool = GPURankPool(
            nranks=nranks,
            total_nparts=target_num_partitions,
            key_columns=list(key_columns),
            rmm_pool_size=data_context.gpu_shuffle_rmm_pool_size,
            spill_memory_limit=data_context.gpu_shuffle_spill_memory_limit,
        )

        self._next_block_idx: int = 0
        self._insert_tasks: Dict[int, MetadataOpTask] = {}
        self._extraction_tasks: Dict[int, DataOpTask] = {}
        self._finalization_started: bool = False
        self._output_queue: Deque[RefBundle] = deque()
        self._shuffled_blocks_stats: List[BlockStats] = []
        self._output_blocks_stats: List[BlockStats] = []

        # Progress bars (populated by SubProgressBarMixin callbacks)
        self._shuffle_bar = None
        self._reduce_bar = None

        # Metrics
        self._shuffle_metrics = OpRuntimeMetrics(self)
        self._reduce_metrics = OpRuntimeMetrics(self)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self, options: ExecutionOptions) -> None:
        super().start(options)
        self._rank_pool.start()

    def _add_input_inner(self, bundle: RefBundle, input_index: int) -> None:
        self._shuffle_metrics.on_input_received(bundle)
        self._shuffled_blocks_stats.extend(to_stats(bundle.metadata))

        for block_ref, metadata in zip(bundle.block_refs, bundle.metadata):
            actor = self._rank_pool.get_actor_for_block(self._next_block_idx)
            insert_ref = actor.insert_batch.remote(block_ref)
            task_idx = self._next_block_idx
            self._next_block_idx += 1

            def _on_insert_done(idx: int = task_idx) -> None:
                self._insert_tasks.pop(idx, None)

            task = MetadataOpTask(
                task_index=task_idx,
                object_ref=insert_ref,
                task_done_callback=_on_insert_done,
                task_resource_bundle=None,
            )
            self._insert_tasks[task_idx] = task
            self._reduce_metrics.on_task_submitted(
                task_idx, bundle, task_id=task.get_task_id()
            )

            if self._shuffle_bar is not None:
                self._shuffle_bar.update(total=self._next_block_idx)

    def _is_inserting_done(self) -> bool:
        return self._inputs_complete and len(self._insert_tasks) == 0

    def _try_finalize(self) -> None:
        """Schedule extraction once all inserts have completed."""
        if self._finalization_started or not self._is_inserting_done():
            return

        self._finalization_started = True

        def _on_bundle_ready(partition_id: int, bundle: RefBundle) -> None:
            # Add finalized block to the output queue
            self._output_queue.append(bundle)

            # Update Finalize Metrics on task output generated
            self._reduce_metrics.on_output_queued(bundle)
            self._reduce_metrics.on_task_output_generated(
                task_index=partition_id, output=bundle
            )
            _, num_outputs, num_rows = estimate_total_num_of_blocks(
                partition_id + 1,
                self.upstream_op_num_outputs(),
                self._reduce_metrics,
                total_num_tasks=self._num_partitions,
            )
            self._estimated_num_output_bundles = num_outputs
            self._estimated_output_num_rows = num_rows

            # Update Finalize progress bar
            self._reduce_bar.update(
                increment=bundle.num_rows() or 0, total=self.num_output_rows_total()
            )

        def _on_extraction_done(
            exc: Optional[Exception],
            worker_stats=None,
            driver_stats=None,
            rank: int = -1,
        ) -> None:
            self._extraction_tasks.pop(rank, None)

        for rank_idx, actor in enumerate(self._rank_pool.actors):
            # Fire-and-forget: Ray serialises actor tasks per actor, so
            # insert_finished is guaranteed to run before extract_partitions.
            actor.insert_finished.remote()
            block_gen = actor.extract_partitions.options(
                num_returns="streaming"
            ).remote()

            data_task = DataOpTask(
                task_index=rank_idx,
                streaming_gen=block_gen,
                output_ready_callback=functools.partial(_on_bundle_ready, rank_idx),
                task_done_callback=functools.partial(
                    _on_extraction_done, rank=rank_idx
                ),
            )
            self._extraction_tasks[rank_idx] = data_task

    # ------------------------------------------------------------------
    # Output interface
    # ------------------------------------------------------------------

    def has_next(self) -> bool:
        self._try_finalize()
        return len(self._output_queue) > 0

    def _get_next_inner(self) -> RefBundle:
        bundle = self._output_queue.popleft()
        self._reduce_metrics.on_output_dequeued(bundle)
        self._reduce_metrics.on_output_taken(bundle)
        self._output_blocks_stats.extend(to_stats(bundle.metadata))
        return bundle

    # ------------------------------------------------------------------
    # Task / completion tracking
    # ------------------------------------------------------------------

    def get_active_tasks(self) -> List[OpTask]:
        return list(self._insert_tasks.values()) + list(self._extraction_tasks.values())

    def has_completed(self) -> bool:
        return (
            self._finalization_started
            and len(self._extraction_tasks) == 0
            and super().has_completed()
        )

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------

    def _do_shutdown(self, force: bool = False) -> None:
        self._rank_pool.shutdown(force=True)
        super()._do_shutdown(force)
        self._insert_tasks.clear()
        self._extraction_tasks.clear()

    # ------------------------------------------------------------------
    # Resource accounting
    # ------------------------------------------------------------------

    def current_logical_usage(self) -> ExecutionResources:
        return ExecutionResources(gpu=self._rank_pool.nranks)

    @property
    def base_resource_usage(self) -> ExecutionResources:
        return ExecutionResources(gpu=self._rank_pool.nranks)

    def incremental_resource_usage(self) -> ExecutionResources:
        return ExecutionResources(gpu=1)

    def get_actor_info(self):
        from ray.data._internal.execution.interfaces.physical_operator import (
            _ActorPoolInfo,
        )

        n = len(self._rank_pool.actors)
        return _ActorPoolInfo(running=n, pending=0, restarting=0)

    # ------------------------------------------------------------------
    # SubProgressBarMixin
    # ------------------------------------------------------------------

    def get_sub_progress_bar_names(self) -> List[str]:
        return ["GPU Shuffle", "GPU Reduce"]

    def set_sub_progress_bar(self, name: str, pg: "BaseProgressBar") -> None:
        if name == "GPU Shuffle":
            self._shuffle_bar = pg
        elif name == "GPU Reduce":
            self._reduce_bar = pg

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    def get_stats(self):
        shuffle_name = f"{self._name}_shuffle"
        reduce_name = f"{self._name}_finalize"
        return {
            shuffle_name: self._shuffled_blocks_stats,
            reduce_name: self._output_blocks_stats,
        }
