import collections
import logging
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, TypeVar, Union

import ray
from ray.data._internal.block_list import BlockList
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.progress_bar import ProgressBar
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data.block import (
    BatchUDF,
    Block,
    BlockAccessor,
    BlockExecStats,
    BlockMetadata,
    BlockPartition,
    CallableClass,
    RowUDF,
)
from ray.data.context import DEFAULT_SCHEDULING_STRATEGY, DatasetContext
from ray.util.annotations import DeveloperAPI, PublicAPI

logger = logging.getLogger(__name__)

T = TypeVar("T")
U = TypeVar("U")

# Block transform function applied by task and actor pools.
BlockTransform = Union[
    # TODO(Clark): Once Ray only supports Python 3.8+, use protocol to constrain block
    # transform type.
    # Callable[[Block, ...], Iterable[Block]]
    # Callable[[Block, BatchUDF, ...], Iterable[Block]],
    Callable[[Block], Iterable[Block]],
    Callable[[Block, Union[BatchUDF, RowUDF]], Iterable[Block]],
    Callable[..., Iterable[Block]],
]

# UDF on a batch or row.
UDF = Union[BatchUDF, RowUDF]


@DeveloperAPI
class ComputeStrategy:
    def _apply(
        self,
        block_fn: BlockTransform,
        remote_args: dict,
        blocks: BlockList,
        clear_input_blocks: bool,
    ) -> BlockList:
        raise NotImplementedError


@DeveloperAPI
class TaskPoolStrategy(ComputeStrategy):
    def _apply(
        self,
        block_fn: BlockTransform,
        remote_args: dict,
        block_list: BlockList,
        clear_input_blocks: bool,
        name: Optional[str] = None,
        fn: Optional[UDF] = None,
        fn_args: Optional[Iterable[Any]] = None,
        fn_kwargs: Optional[Dict[str, Any]] = None,
        fn_constructor_args: Optional[Iterable[Any]] = None,
        fn_constructor_kwargs: Optional[Dict[str, Any]] = None,
    ) -> BlockList:
        assert fn_constructor_args is None and fn_constructor_kwargs is None
        if fn_args is None:
            fn_args = tuple()
        if fn_kwargs is None:
            fn_kwargs = {}

        context = DatasetContext.get_current()

        # Handle empty datasets.
        if block_list.initial_num_blocks() == 0:
            return block_list

        blocks = block_list.get_blocks_with_metadata()
        if name is None:
            name = "map"
        name = name.title()
        map_bar = ProgressBar(name, total=len(blocks))

        if context.block_splitting_enabled:
            map_block = cached_remote_fn(_map_block_split).options(
                num_returns="dynamic", **remote_args
            )
            refs = [
                map_block.remote(b, block_fn, m.input_files, fn, *fn_args, **fn_kwargs)
                for b, m in blocks
            ]
        else:
            map_block = cached_remote_fn(_map_block_nosplit).options(
                **dict(remote_args, num_returns=2)
            )
            all_refs = [
                map_block.remote(b, block_fn, m.input_files, fn, *fn_args, **fn_kwargs)
                for b, m in blocks
            ]
            data_refs = [r[0] for r in all_refs]
            refs = [r[1] for r in all_refs]

        in_block_owned_by_consumer = block_list._owned_by_consumer
        # Release input block references.
        if clear_input_blocks:
            del blocks
            block_list.clear()

        # Common wait for non-data refs.
        try:
            results = map_bar.fetch_until_complete(refs)
        except (ray.exceptions.RayTaskError, KeyboardInterrupt) as e:
            # One or more mapper tasks failed, or we received a SIGINT signal
            # while waiting; either way, we cancel all map tasks.
            for ref in refs:
                ray.cancel(ref)
            # Wait until all tasks have failed or been cancelled.
            for ref in refs:
                try:
                    ray.get(ref)
                except (ray.exceptions.RayTaskError, ray.exceptions.TaskCancelledError):
                    pass
            # Reraise the original task failure exception.
            raise e from None

        new_blocks, new_metadata = [], []
        if context.block_splitting_enabled:
            for ref_generator in results:
                refs = list(ref_generator)
                metadata = ray.get(refs.pop(-1))
                assert len(metadata) == len(refs)
                new_blocks += refs
                new_metadata += metadata
        else:
            for block, metadata in zip(data_refs, results):
                new_blocks.append(block)
                new_metadata.append(metadata)
        return BlockList(
            list(new_blocks),
            list(new_metadata),
            owned_by_consumer=in_block_owned_by_consumer,
        )


@PublicAPI
class ActorPoolStrategy(ComputeStrategy):
    """Specify the compute strategy for a Dataset transform.

    ActorPoolStrategy specifies that an autoscaling pool of actors should be used
    for a given Dataset transform. This is useful for stateful setup of callable
    classes.

    To autoscale from ``m`` to ``n`` actors, specify
    ``compute=ActorPoolStrategy(m, n)``.
    For a fixed-sized pool of size ``n``, specify ``compute=ActorPoolStrategy(n, n)``.

    To increase opportunities for pipelining task dependency prefetching with
    computation and avoiding actor startup delays, set max_tasks_in_flight_per_actor
    to 2 or greater; to try to decrease the delay due to queueing of tasks on the worker
    actors, set max_tasks_in_flight_per_actor to 1.
    """

    def __init__(
        self,
        min_size: int = 1,
        max_size: Optional[int] = None,
        max_tasks_in_flight_per_actor: Optional[int] = 2,
    ):
        """Construct ActorPoolStrategy for a Dataset transform.

        Args:
            min_size: The minimize size of the actor pool.
            max_size: The maximum size of the actor pool.
            max_tasks_in_flight_per_actor: The maximum number of tasks to concurrently
                send to a single actor worker. Increasing this will increase
                opportunities for pipelining task dependency prefetching with
                computation and avoiding actor startup delays, but will also increase
                queueing delay.
        """
        if min_size < 1:
            raise ValueError("min_size must be > 1", min_size)
        if max_size is not None and min_size > max_size:
            raise ValueError("min_size must be <= max_size", min_size, max_size)
        if max_tasks_in_flight_per_actor < 1:
            raise ValueError(
                "max_tasks_in_flight_per_actor must be >= 1, got: ",
                max_tasks_in_flight_per_actor,
            )
        self.min_size = min_size
        self.max_size = max_size or float("inf")
        self.max_tasks_in_flight_per_actor = max_tasks_in_flight_per_actor
        self.num_workers = 0
        self.ready_to_total_workers_ratio = 0.8

    def _apply(
        self,
        block_fn: BlockTransform,
        remote_args: dict,
        block_list: BlockList,
        clear_input_blocks: bool,
        name: Optional[str] = None,
        fn: Optional[UDF] = None,
        fn_args: Optional[Iterable[Any]] = None,
        fn_kwargs: Optional[Dict[str, Any]] = None,
        fn_constructor_args: Optional[Iterable[Any]] = None,
        fn_constructor_kwargs: Optional[Dict[str, Any]] = None,
    ) -> BlockList:
        """Note: this is not part of the Dataset public API."""
        if fn_args is None:
            fn_args = tuple()
        if fn_kwargs is None:
            fn_kwargs = {}
        if fn_constructor_args is None:
            fn_constructor_args = tuple()
        if fn_constructor_kwargs is None:
            fn_constructor_kwargs = {}

        blocks_in = block_list.get_blocks_with_metadata()
        owned_by_consumer = block_list._owned_by_consumer

        # Early release block references.
        if clear_input_blocks:
            block_list.clear()

        orig_num_blocks = len(blocks_in)
        results = []
        if name is None:
            name = "map"
        name = name.title()
        map_bar = ProgressBar(name, total=orig_num_blocks)

        class BlockWorker:
            def __init__(
                self,
                *fn_constructor_args: Any,
                **fn_constructor_kwargs: Any,
            ):
                if not isinstance(fn, CallableClass):
                    if fn_constructor_args or fn_constructor_kwargs:
                        raise ValueError(
                            "fn_constructor_{kw}args only valid for CallableClass "
                            f"UDFs, but got: {fn}"
                        )
                    self.fn = fn
                else:
                    self.fn = fn(*fn_constructor_args, **fn_constructor_kwargs)

            def ready(self):
                return "ok"

            def map_block_split(
                self,
                block: Block,
                input_files: List[str],
                *fn_args,
                **fn_kwargs,
            ) -> BlockPartition:
                return _map_block_split(
                    block, block_fn, input_files, self.fn, *fn_args, **fn_kwargs
                )

            @ray.method(num_returns=2)
            def map_block_nosplit(
                self,
                block: Block,
                input_files: List[str],
                *fn_args,
                **fn_kwargs,
            ) -> Tuple[Block, BlockMetadata]:
                return _map_block_nosplit(
                    block, block_fn, input_files, self.fn, *fn_args, **fn_kwargs
                )

        if "num_cpus" not in remote_args:
            remote_args["num_cpus"] = 1

        if "scheduling_strategy" not in remote_args:
            ctx = DatasetContext.get_current()
            if ctx.scheduling_strategy == DEFAULT_SCHEDULING_STRATEGY:
                remote_args["scheduling_strategy"] = "SPREAD"
            else:
                remote_args["scheduling_strategy"] = ctx.scheduling_strategy

        BlockWorker = ray.remote(**remote_args)(BlockWorker)

        workers = [
            BlockWorker.remote(*fn_constructor_args, **fn_constructor_kwargs)
            for _ in range(self.min_size)
        ]
        tasks = {w.ready.remote(): w for w in workers}
        tasks_in_flight = collections.defaultdict(int)
        metadata_mapping = {}
        block_indices = {}
        ready_workers = set()

        try:
            while len(results) < orig_num_blocks:
                ready, _ = ray.wait(
                    list(tasks.keys()), timeout=0.01, num_returns=1, fetch_local=False
                )
                if not ready:
                    if (
                        len(workers) < self.max_size
                        and len(ready_workers) / len(workers)
                        > self.ready_to_total_workers_ratio
                    ):
                        w = BlockWorker.remote(
                            *fn_constructor_args, **fn_constructor_kwargs
                        )
                        workers.append(w)
                        tasks[w.ready.remote()] = w
                        map_bar.set_description(
                            "Map Progress ({} actors {} pending)".format(
                                len(ready_workers), len(workers) - len(ready_workers)
                            )
                        )
                    continue

                [obj_id] = ready
                worker = tasks.pop(obj_id)

                # Process task result.
                if worker in ready_workers:
                    results.append(obj_id)
                    tasks_in_flight[worker] -= 1
                    map_bar.update(1)
                else:
                    ready_workers.add(worker)
                    map_bar.set_description(
                        "Map Progress ({} actors {} pending)".format(
                            len(ready_workers), len(workers) - len(ready_workers)
                        )
                    )

                # Schedule a new task.
                while (
                    blocks_in
                    and tasks_in_flight[worker] < self.max_tasks_in_flight_per_actor
                ):
                    block, meta = blocks_in.pop()
                    # TODO(swang): Support block splitting for compute="actors".
                    ref, meta_ref = worker.map_block_nosplit.remote(
                        block,
                        meta.input_files,
                        *fn_args,
                        **fn_kwargs,
                    )
                    metadata_mapping[ref] = meta_ref
                    tasks[ref] = worker
                    block_indices[ref] = len(blocks_in)
                    tasks_in_flight[worker] += 1

            map_bar.close()
            self.num_workers += len(workers)
            new_blocks, new_metadata = [], []
            # Put blocks in input order.
            results.sort(key=block_indices.get)
            # TODO(swang): Support block splitting for compute="actors".
            for block in results:
                new_blocks.append(block)
                new_metadata.append(metadata_mapping[block])
            new_metadata = ray.get(new_metadata)
            return BlockList(
                new_blocks, new_metadata, owned_by_consumer=owned_by_consumer
            )

        except Exception as e:
            try:
                for worker in workers:
                    ray.kill(worker)
            except Exception as err:
                logger.exception(f"Error killing workers: {err}")
            finally:
                raise e


def get_compute(compute_spec: Union[str, ComputeStrategy]) -> ComputeStrategy:
    if not compute_spec or compute_spec == "tasks":
        return TaskPoolStrategy()
    elif compute_spec == "actors":
        return ActorPoolStrategy()
    elif isinstance(compute_spec, ComputeStrategy):
        return compute_spec
    else:
        raise ValueError("compute must be one of [`tasks`, `actors`, ComputeStrategy]")


def is_task_compute(compute_spec: Union[str, ComputeStrategy]) -> bool:
    return (
        not compute_spec
        or compute_spec == "tasks"
        or isinstance(compute_spec, TaskPoolStrategy)
    )


def _map_block_split(
    block: Block,
    block_fn: BlockTransform,
    input_files: List[str],
    fn: Optional[UDF],
    *fn_args,
    **fn_kwargs,
) -> BlockPartition:
    stats = BlockExecStats.builder()
    if fn is not None:
        fn_args = (fn,) + fn_args
    new_metas = []
    for new_block in block_fn(block, *fn_args, **fn_kwargs):
        accessor = BlockAccessor.for_block(new_block)
        new_meta = BlockMetadata(
            num_rows=accessor.num_rows(),
            size_bytes=accessor.size_bytes(),
            schema=accessor.schema(),
            input_files=input_files,
            exec_stats=stats.build(),
        )
        yield new_block
        new_metas.append(new_meta)
        stats = BlockExecStats.builder()
    yield new_metas


def _map_block_nosplit(
    block: Block,
    block_fn: BlockTransform,
    input_files: List[str],
    fn: Optional[UDF],
    *fn_args,
    **fn_kwargs,
) -> Tuple[Block, BlockMetadata]:
    stats = BlockExecStats.builder()
    builder = DelegatingBlockBuilder()
    if fn is not None:
        fn_args = (fn,) + fn_args
    for new_block in block_fn(block, *fn_args, **fn_kwargs):
        builder.add_block(new_block)
    new_block = builder.build()
    accessor = BlockAccessor.for_block(new_block)
    return new_block, accessor.get_metadata(
        input_files=input_files, exec_stats=stats.build()
    )
