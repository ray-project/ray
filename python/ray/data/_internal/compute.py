import logging
import math
from typing import Any, Callable, Iterable, List, Optional, Tuple, TypeVar, Union

from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.execution.interfaces import TaskContext
from ray.data.block import (
    Block,
    BlockAccessor,
    BlockExecStats,
    BlockMetadata,
    BlockPartition,
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
    pass


@DeveloperAPI
class TaskPoolStrategy(ComputeStrategy):
    def __init__(
        self,
        size: Optional[int] = None,
    ):
        """Construct TaskPoolStrategy for a Dataset transform.

        Args:
            size: Specify the maximum size of the task pool.
        """

        if size is not None and size < 1:
            raise ValueError("`size` must be >= 1", size)
        self.size = size

    def __eq__(self, other: Any) -> bool:
        return (isinstance(other, TaskPoolStrategy) and self.size == other.size) or (
            other == "tasks" and self.size is None
        )


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
