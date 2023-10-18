import collections
import logging
import math
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, TypeVar, Union

import ray
from ray.data._internal.block_list import BlockList
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.progress_bar import ProgressBar
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data.block import (
    Block,
    BlockAccessor,
    BlockExecStats,
    BlockMetadata,
    BlockPartition,
    CallableClass,
    UserDefinedFunction,
)
from ray.data.context import DataContext
from ray.types import ObjectRef
from ray.util.annotations import DeveloperAPI, PublicAPI

logger = logging.getLogger(__name__)

T = TypeVar("T")
U = TypeVar("U")

DEFAULT_MAX_TASKS_IN_FLIGHT_PER_ACTOR = 2


# Block transform function applied by task and actor pools.
BlockTransform = Union[
    # TODO(Clark): Once Ray only supports Python 3.8+, use protocol to constrain block
    # transform type.
    # Callable[[Block, ...], Iterable[Block]]
    # Callable[[Block, UserDefinedFunction, ...], Iterable[Block]],
    Callable[[Iterable[Block], TaskContext], Iterable[Block]],
    Callable[[Iterable[Block], TaskContext, UserDefinedFunction], Iterable[Block]],
    Callable[..., Iterable[Block]],
]


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
        min_rows_per_block: Optional[int] = None,
        fn: Optional[UserDefinedFunction] = None,
        fn_args: Optional[Iterable[Any]] = None,
        fn_kwargs: Optional[Dict[str, Any]] = None,
        fn_constructor_args: Optional[Iterable[Any]] = None,
        fn_constructor_kwargs: Optional[Dict[str, Any]] = None,
    ) -> BlockList:
        assert not DataContext.get_current().new_execution_backend, "Legacy backend off"
        assert fn_constructor_args is None and fn_constructor_kwargs is None
        if fn_args is None:
            fn_args = tuple()
        if fn_kwargs is None:
            fn_kwargs = {}

        # Handle empty datasets.
        if block_list.initial_num_blocks() == 0:
            return block_list

        if name is None:
            name = "map"
        blocks = block_list.get_blocks_with_metadata()
        # Bin blocks by target block size.
        if min_rows_per_block is not None:
            _check_batch_size(blocks, min_rows_per_block, name)
            block_bundles = _bundle_blocks_up_to_size(blocks, min_rows_per_block)
        else:
            block_bundles = [((b,), (m,)) for b, m in blocks]
        del blocks
        name = name.title()
        map_bar = ProgressBar(name, total=len(block_bundles))

        map_block = cached_remote_fn(_map_block_split).options(
            num_returns="dynamic", **remote_args
        )
        refs = [
            map_block.remote(
                block_fn,
                [f for m in ms for f in m.input_files],
                fn,
                len(bs),
                *(bs + fn_args),
                **fn_kwargs,
            )
            for bs, ms in block_bundles
        ]

        in_block_owned_by_consumer = block_list._owned_by_consumer
        # Release input block references.
        if clear_input_blocks:
            del block_bundles
            block_list.clear()

        # Common wait for non-data refs.
        try:
            results = map_bar.fetch_until_complete(refs)
        except (ray.exceptions.RayError, KeyboardInterrupt) as e:
            # One or more mapper tasks failed, or we received a SIGINT signal
            # while waiting; either way, we cancel all map tasks.
            for ref in refs:
                ray.cancel(ref)
            # Wait until all tasks have failed or been cancelled.
            for ref in refs:
                try:
                    ray.get(ref)
                except ray.exceptions.RayError:
                    # Cancellation either succeeded, or the task had already failed with
                    # a different error, or cancellation failed. In all cases, we
                    # swallow the exception.
                    pass
            # Reraise the original task failure exception.
            raise e from None

        new_blocks, new_metadata = [], []
        for ref_generator in results:
            refs = list(ref_generator)
            metadata = ray.get(refs.pop(-1))
            assert len(metadata) == len(refs)
            new_blocks += refs
            new_metadata += metadata
        return BlockList(
            list(new_blocks),
            list(new_metadata),
            owned_by_consumer=in_block_owned_by_consumer,
        )

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, TaskPoolStrategy) or other == "tasks"


@PublicAPI
class ActorPoolStrategy(ComputeStrategy):
    """Specify the compute strategy for a Dataset transform.

    ActorPoolStrategy specifies that an autoscaling pool of actors should be used
    for a given Dataset transform. This is useful for stateful setup of callable
    classes.

    For a fixed-sized pool of size ``n``, specify ``compute=ActorPoolStrategy(size=n)``.
    To autoscale from ``m`` to ``n`` actors, specify
    ``ActorPoolStrategy(min_size=m, max_size=n)``.

    To increase opportunities for pipelining task dependency prefetching with
    computation and avoiding actor startup delays, set max_tasks_in_flight_per_actor
    to 2 or greater; to try to decrease the delay due to queueing of tasks on the worker
    actors, set max_tasks_in_flight_per_actor to 1.
    """

    def __init__(
        self,
        # Deprecated: kwargs will be required for all args in a future release.
        legacy_min_size: Optional[int] = None,
        legacy_max_size: Optional[int] = None,
        *,
        size: Optional[int] = None,
        min_size: Optional[int] = None,
        max_size: Optional[int] = None,
        max_tasks_in_flight_per_actor: Optional[int] = None,
    ):
        """Construct ActorPoolStrategy for a Dataset transform.

        Args:
            size: Specify a fixed size actor pool of this size. It is an error to
                specify both `size` and `min_size` or `max_size`.
            min_size: The minimize size of the actor pool.
            max_size: The maximum size of the actor pool.
            max_tasks_in_flight_per_actor: The maximum number of tasks to concurrently
                send to a single actor worker. Increasing this will increase
                opportunities for pipelining task dependency prefetching with
                computation and avoiding actor startup delays, but will also increase
                queueing delay.
        """
        if legacy_min_size is not None or legacy_max_size is not None:
            raise ValueError(
                "In Ray 2.5, ActorPoolStrategy requires min_size and "
                "max_size to be explicit kwargs."
            )
        if size:
            if size < 1:
                raise ValueError("size must be >= 1", size)
            if max_size is not None or min_size is not None:
                raise ValueError(
                    "min_size and max_size cannot be set at the same time as `size`"
                )
            min_size = size
            max_size = size
        if min_size is not None and min_size < 1:
            raise ValueError("min_size must be >= 1", min_size)
        if max_size is not None:
            if min_size is None:
                min_size = 1  # Legacy default.
            if min_size > max_size:
                raise ValueError("min_size must be <= max_size", min_size, max_size)
        if (
            max_tasks_in_flight_per_actor is not None
            and max_tasks_in_flight_per_actor < 1
        ):
            raise ValueError(
                "max_tasks_in_flight_per_actor must be >= 1, got: ",
                max_tasks_in_flight_per_actor,
            )
        self.min_size = min_size or 1
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
        min_rows_per_block: Optional[int] = None,
        fn: Optional[UserDefinedFunction] = None,
        fn_args: Optional[Iterable[Any]] = None,
        fn_kwargs: Optional[Dict[str, Any]] = None,
        fn_constructor_args: Optional[Iterable[Any]] = None,
        fn_constructor_kwargs: Optional[Dict[str, Any]] = None,
    ) -> BlockList:
        """Note: this is not part of the Dataset public API."""
        assert not DataContext.get_current().new_execution_backend, "Legacy backend off"
        if fn_args is None:
            fn_args = tuple()
        if fn_kwargs is None:
            fn_kwargs = {}
        if fn_constructor_args is None:
            fn_constructor_args = tuple()
        if fn_constructor_kwargs is None:
            fn_constructor_kwargs = {}

        if name is None:
            name = "map"
        blocks_in: List[
            Tuple[ObjectRef[Block], BlockMetadata]
        ] = block_list.get_blocks_with_metadata()

        # We bundle blocks according to the following rules:
        # 1. Attempt to bundle up to the target block size.
        # 2. If the max concurrency of the ActorPool is set, then
        #    cap the number of bundles to match the size of the ActorPool.
        #    This avoids additional overhead in submitting new actor tasks and allows
        #    the actor task to do optimizations such as batch prefetching.
        if min_rows_per_block is None:
            min_rows_per_block = 0
        if not math.isinf(self.max_size):
            total_size = sum(
                meta.num_rows if meta.num_rows is not None else 0
                for _, meta in blocks_in
            )
            pool_max_block_size = total_size // self.max_size
            min_rows_per_block = max(min_rows_per_block, pool_max_block_size)
        if min_rows_per_block > 0:
            _check_batch_size(blocks_in, min_rows_per_block, name)
            block_bundles: List[
                Tuple[Tuple[ObjectRef[Block]], Tuple[BlockMetadata]]
            ] = _bundle_blocks_up_to_size(blocks_in, min_rows_per_block)
        else:
            block_bundles = [((b,), (m,)) for b, m in blocks_in]

        del blocks_in
        owned_by_consumer = block_list._owned_by_consumer

        # Early release block references.
        if clear_input_blocks:
            block_list.clear()

        orig_num_blocks = len(block_bundles)
        results = []
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
                input_files: List[str],
                num_blocks: int,
                *blocks_and_fn_args,
                **fn_kwargs,
            ) -> BlockPartition:
                return _map_block_split(
                    block_fn,
                    input_files,
                    self.fn,
                    num_blocks,
                    *blocks_and_fn_args,
                    **fn_kwargs,
                )

            @ray.method(num_returns=2)
            def map_block_nosplit(
                self,
                input_files: List[str],
                num_blocks: int,
                *blocks_and_fn_args,
                **fn_kwargs,
            ) -> Tuple[Block, BlockMetadata]:
                return _map_block_nosplit(
                    block_fn,
                    input_files,
                    self.fn,
                    num_blocks,
                    *blocks_and_fn_args,
                    **fn_kwargs,
                )

        if "num_cpus" not in remote_args:
            remote_args["num_cpus"] = 1

        if "scheduling_strategy" not in remote_args:
            ctx = DataContext.get_current()
            remote_args["scheduling_strategy"] = ctx.scheduling_strategy

        BlockWorker = ray.remote(**remote_args)(BlockWorker)

        workers = [
            BlockWorker.remote(*fn_constructor_args, **fn_constructor_kwargs)
            for _ in range(self.min_size)
        ]
        tasks = {w.ready.remote(): w for w in workers}
        tasks_in_flight = collections.defaultdict(int)
        max_tasks_in_flight_per_actor = (
            self.max_tasks_in_flight_per_actor or DEFAULT_MAX_TASKS_IN_FLIGHT_PER_ACTOR
        )
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
                    block_bundles
                    and tasks_in_flight[worker] < max_tasks_in_flight_per_actor
                ):
                    blocks, metas = block_bundles.pop()
                    # TODO(swang): Support block splitting for compute="actors".
                    ref, meta_ref = worker.map_block_nosplit.remote(
                        [f for meta in metas for f in meta.input_files],
                        len(blocks),
                        *(blocks + fn_args),
                        **fn_kwargs,
                    )
                    metadata_mapping[ref] = meta_ref
                    tasks[ref] = worker
                    block_indices[ref] = len(block_bundles)
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
                raise e from None

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, ActorPoolStrategy) and (
            self.min_size == other.min_size
            and self.max_size == other.max_size
            and self.max_tasks_in_flight_per_actor
            == other.max_tasks_in_flight_per_actor
        )


def get_compute(compute_spec: Union[str, ComputeStrategy]) -> ComputeStrategy:
    if not isinstance(compute_spec, (TaskPoolStrategy, ActorPoolStrategy)):
        raise ValueError(
            "In Ray 2.5, the compute spec must be either "
            f"TaskPoolStrategy or ActorPoolStategy, was: {compute_spec}."
        )
    elif not compute_spec or compute_spec == "tasks":
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
    block_fn: BlockTransform,
    input_files: List[str],
    fn: Optional[UserDefinedFunction],
    num_blocks: int,
    *blocks_and_fn_args: Union[Block, Any],
    **fn_kwargs,
) -> BlockPartition:
    stats = BlockExecStats.builder()
    blocks, fn_args = blocks_and_fn_args[:num_blocks], blocks_and_fn_args[num_blocks:]
    if fn is not None:
        fn_args = (fn,) + fn_args
    new_metas = []
    for new_block in block_fn(blocks, *fn_args, **fn_kwargs):
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
    block_fn: BlockTransform,
    input_files: List[str],
    fn: Optional[UserDefinedFunction],
    num_blocks: int,
    *blocks_and_fn_args: Union[Block, Any],
    **fn_kwargs,
) -> Tuple[Block, BlockMetadata]:
    stats = BlockExecStats.builder()
    builder = DelegatingBlockBuilder()
    blocks, fn_args = blocks_and_fn_args[:num_blocks], blocks_and_fn_args[num_blocks:]
    if fn is not None:
        fn_args = (fn,) + fn_args
    for new_block in block_fn(blocks, *fn_args, **fn_kwargs):
        builder.add_block(new_block)
    new_block = builder.build()
    accessor = BlockAccessor.for_block(new_block)
    return new_block, accessor.get_metadata(
        input_files=input_files, exec_stats=stats.build()
    )


def _bundle_blocks_up_to_size(
    blocks: List[Tuple[ObjectRef[Block], BlockMetadata]],
    target_size: int,
) -> List[Tuple[Tuple[ObjectRef[Block]], Tuple[BlockMetadata]]]:
    """Group blocks into bundles that are up to (but not exceeding) the provided target
    size.
    """
    block_bundles: List[List[Tuple[ObjectRef[Block], BlockMetadata]]] = []
    curr_bundle: List[Tuple[ObjectRef[Block], BlockMetadata]] = []
    curr_bundle_size = 0
    for b, m in blocks:
        num_rows = m.num_rows
        if num_rows is None:
            num_rows = float("inf")
        if curr_bundle_size > 0 and curr_bundle_size + num_rows > target_size:
            block_bundles.append(curr_bundle)
            curr_bundle = []
            curr_bundle_size = 0
        curr_bundle.append((b, m))
        curr_bundle_size += num_rows
    if curr_bundle:
        block_bundles.append(curr_bundle)
    if len(blocks) / len(block_bundles) >= 10:
        logger.warning(
            f"`batch_size` is set to {target_size}, which reduces parallelism from "
            f"{len(blocks)} to {len(block_bundles)}. If the performance is worse than "
            "expected, this may indicate that the batch size is too large or the "
            "input block size is too small. To reduce batch size, consider decreasing "
            "`batch_size` or use the default in `map_batches`. To increase input "
            "block size, consider decreasing `parallelism` in read."
        )
    return [tuple(zip(*block_bundle)) for block_bundle in block_bundles]


def _check_batch_size(
    blocks_and_meta: List[Tuple[ObjectRef[Block], BlockMetadata]],
    batch_size: int,
    name: str,
):
    """Log a warning if the provided batch size exceeds the configured target max block
    size.
    """
    batch_size_bytes = None
    for _, meta in blocks_and_meta:
        if meta.num_rows and meta.size_bytes:
            batch_size_bytes = math.ceil(batch_size * (meta.size_bytes / meta.num_rows))
            break
    context = DataContext.get_current()
    if (
        batch_size_bytes is not None
        and batch_size_bytes > context.target_max_block_size
    ):
        logger.warning(
            f"Requested batch size {batch_size} results in batches of "
            f"{batch_size_bytes} bytes for {name} tasks, which is larger than the "
            f"configured target max block size {context.target_max_block_size}. This "
            "may result in out-of-memory errors for certain workloads, and you may "
            "want to decrease your batch size or increase the configured target max "
            "block size, e.g.: "
            "from ray.data.context import DataContext; "
            "DataContext.get_current().target_max_block_size = 4_000_000_000"
        )
