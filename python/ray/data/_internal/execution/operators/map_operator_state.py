from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable, Optional, List, Dict, Any, Union, Tuple, Iterator

import ray
from ray.data._internal.compute import (
    ComputeStrategy,
    TaskPoolStrategy,
    ActorPoolStrategy,
)
from ray.data._internal.execution.util import merge_ref_bundles
from ray.data._internal.execution.interfaces import (
    RefBundle,
)
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.memory_tracing import trace_allocation
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data.block import Block, BlockAccessor, BlockMetadata, BlockExecStats
from ray.data.context import DEFAULT_SCHEDULING_STRATEGY, DatasetContext
from ray.types import ObjectRef
from ray._raylet import ObjectRefGenerator


class MapOperatorState:
    def __init__(
        self,
        transform_fn: Callable[[Iterator[Block]], Iterator[Block]],
        compute_strategy: ComputeStrategy,
        ray_remote_args: Optional[Dict[str, Any]],
        min_rows_per_bundle: Optional[int],
    ):
        # Execution arguments.
        self._min_rows_per_bundle: Optional[int] = min_rows_per_bundle

        # Submitter of Ray tasks mapping transform_fn over data.
        if ray_remote_args is None:
            ray_remote_args = {}
        if isinstance(compute_strategy, TaskPoolStrategy):
            task_submitter = TaskPoolSubmitter(ray_remote_args)
        elif isinstance(compute_strategy, ActorPoolStrategy):
            task_submitter = ActorPoolSubmitter(compute_strategy, ray_remote_args)
        else:
            raise ValueError(f"Unsupported execution strategy {compute_strategy}")
        self._task_submitter: MapTaskSubmitter = task_submitter
        # Whether we have started the task submitter yet.
        self._have_started_submitter = False

        # Put the function def in the object store to avoid repeated serialization
        # in case it's large (i.e., closure captures large objects).
        self._transform_fn_ref = ray.put(transform_fn)

        # The temporary block bundle used to accumulate inputs until they meet the
        # min_rows_per_bundle requirement.
        self._block_bundle: Optional[RefBundle] = None

        # Execution state.
        self._tasks: Dict[ObjectRef[Union[ObjectRefGenerator, Block]], _TaskState] = {}
        self._tasks_by_output_order: Dict[int, _TaskState] = {}
        self._next_task_index: int = 0
        self._next_output_index: int = 0
        self._obj_store_mem_alloc: int = 0
        self._obj_store_mem_freed: int = 0
        self._obj_store_mem_cur: int = 0
        self._obj_store_mem_peak: int = 0

    def add_input(self, bundle: RefBundle) -> None:
        if not self._have_started_submitter:
            # Start the task submitter on the first input.
            self._task_submitter.start()
            self._have_started_submitter = True

        if self._min_rows_per_bundle is None:
            self._create_task(bundle)
            return

        def get_num_rows(bundle: Optional[RefBundle]):
            if bundle is None:
                return 0
            if bundle.num_rows() is None:
                return float("inf")
            return bundle.num_rows()

        bundle_rows = get_num_rows(bundle)
        acc_num_rows = get_num_rows(self._block_bundle) + bundle_rows
        if acc_num_rows > self._min_rows_per_bundle:
            if self._block_bundle:
                if get_num_rows(self._block_bundle) > 0:
                    self._create_task(self._block_bundle)
                self._block_bundle = bundle
            else:
                self._create_task(bundle)
        else:
            # TODO(ekl) add a warning if we merge 10+ blocks per bundle.
            self._block_bundle = merge_ref_bundles(self._block_bundle, bundle)

    def inputs_done(self, input_index: int) -> None:
        assert input_index == 0, "Map operator only supports one input."
        if self._block_bundle:
            self._create_task(self._block_bundle)
            self._block_bundle = None
        self._task_submitter.task_submission_done()

    def work_completed(self, ref: ObjectRef[Union[ObjectRefGenerator, Block]]) -> None:
        self._task_submitter.task_done(ref)
        task: _TaskState = self._tasks.pop(ref)
        if task.block_metadata_ref is not None:
            # Non-dynamic block splitting path.
            # TODO(Clark): Remove this special case once dynamic block splitting is
            # supported for actors.
            block_refs = [ref]
            block_metas = [ray.get(task.block_metadata_ref)]
        else:
            # Dynamic block splitting path.
            all_refs = list(ray.get(ref))
            del ref
            block_refs = all_refs[:-1]
            block_metas = ray.get(all_refs[-1])
        assert len(block_metas) == len(block_refs), (block_refs, block_metas)
        for ref in block_refs:
            trace_allocation(ref, "map_operator_work_completed")
        task.output = RefBundle(list(zip(block_refs, block_metas)), owns_blocks=True)
        allocated = task.output.size_bytes()
        self._obj_store_mem_alloc += allocated
        self._obj_store_mem_cur += allocated
        # TODO(ekl) this isn't strictly correct if multiple operators depend on this
        # bundle, but it doesn't happen in linear dags for now.
        freed = task.inputs.destroy_if_owned()
        if freed:
            self._obj_store_mem_freed += freed
            self._obj_store_mem_cur -= freed
        if self._obj_store_mem_cur > self._obj_store_mem_peak:
            self._obj_store_mem_peak = self._obj_store_mem_cur

    def has_next(self) -> bool:
        i = self._next_output_index
        return (
            i in self._tasks_by_output_order
            and self._tasks_by_output_order[i].output is not None
        )

    def get_next(self) -> RefBundle:
        i = self._next_output_index
        self._next_output_index += 1
        bundle = self._tasks_by_output_order.pop(i).output
        self._obj_store_mem_cur -= bundle.size_bytes()
        return bundle

    def get_work_refs(self) -> List[ray.ObjectRef]:
        return list(self._tasks)

    def shutdown(self) -> None:
        if self._task_submitter.cancellable():
            # Cancel all active tasks.
            for task in self._tasks:
                ray.cancel(task)
            # Wait until all tasks have failed or been cancelled.
            for task in self._tasks:
                try:
                    ray.get(task)
                except ray.exceptions.RayError:
                    # Cancellation either succeeded, or the task had already failed with
                    # a different error, or cancellation failed. In all cases, we
                    # swallow the exception.
                    pass

    @property
    def obj_store_mem_alloc(self) -> int:
        """Return the object store memory allocated by this operator execution."""
        return self._obj_store_mem_alloc

    @property
    def obj_store_mem_freed(self) -> int:
        """Return the object store memory freed by this operator execution."""
        return self._obj_store_mem_freed

    @property
    def obj_store_mem_peak(self) -> int:
        """Return the peak object store memory utilization during this operator
        execution.
        """
        return self._obj_store_mem_peak

    def _create_task(self, bundle: RefBundle) -> None:
        input_blocks = []
        for block, _ in bundle.blocks:
            input_blocks.append(block)
        # TODO fix for Ray client: https://github.com/ray-project/ray/issues/30458
        if not DatasetContext.get_current().block_splitting_enabled:
            raise NotImplementedError("New backend requires block splitting")
        ref: Union[
            ObjectRef[ObjectRefGenerator],
            Tuple[ObjectRef[Block], ObjectRef[BlockMetadata]],
        ] = self._task_submitter.submit(self._transform_fn_ref, input_blocks)
        task = _TaskState(bundle)
        if isinstance(ref, tuple):
            # Task submitter returned a block ref and block metadata ref tuple; we make
            # the block ref the canonical task ref, and store the block metadata ref for
            # future resolution, when the task completes.
            # TODO(Clark): Remove this special case once dynamic block splitting is
            # supported for actors.
            ref, block_metadata_ref = ref
            task.block_metadata_ref = block_metadata_ref
        self._tasks[ref] = task
        self._tasks_by_output_order[self._next_task_index] = task
        self._next_task_index += 1
        self._obj_store_mem_cur += bundle.size_bytes()
        if self._obj_store_mem_cur > self._obj_store_mem_peak:
            self._obj_store_mem_peak = self._obj_store_mem_cur


@dataclass
class _TaskState:
    """Tracks the driver-side state for an MapOperator task.

    Attributes:
        inputs: The input ref bundle.
        output: The output ref bundle that is set when the task completes.
        block_metadata_ref: A future for the block metadata; this will only be set for
            the ActorPoolTaskSubmitter, which doesn't yet support dynamic block
            splitting.
    """

    inputs: RefBundle
    output: Optional[RefBundle] = None
    #  TODO(Clark): Remove this once dynamic block splitting is supported for actors.
    block_metadata_ref: Optional[ObjectRef[BlockMetadata]] = None


class MapTaskSubmitter(ABC):
    """A task submitter for MapOperator.

    This abstraction is in charge of submitting tasks, reserving resources for their
    execution, and cleaning up said resources when a task completes or when task
    submission is done.
    """

    def start(self):
        """Start the task submitter so it's ready to submit tasks.

        This is called when execution of the map operator actually starts, and is where
        the submitter can initialize expensive state, reserve resources, start workers,
        etc.
        """
        pass

    @abstractmethod
    def submit(
        self,
        transform_fn: ObjectRef[Callable[[Iterator[Block]], Iterator[Block]]],
        input_blocks: List[ObjectRef[Block]],
    ) -> Union[
        ObjectRef[ObjectRefGenerator], Tuple[ObjectRef[Block], ObjectRef[BlockMetadata]]
    ]:
        """Submit a map task.

        Args:
            transform_fn: The function to apply to a block bundle in the submitted
                map task.
            input_blocks: The block bundle on which to apply transform_fn.

        Returns:
            An object ref representing the output of the map task.
        """
        raise NotImplementedError

    def task_done(self, task_ref: ray.ObjectRef):
        """Indicates that the task that output the provided ref is done.

        Args:
            task_ref: The output ref for the task that's done.
        """
        pass

    def task_submission_done(self):
        """Indicates that no more tasks will be submitter."""
        pass

    @abstractmethod
    def cancellable(self):
        """Whether the submitted tasks are cancellable."""
        raise NotImplementedError


class TaskPoolSubmitter(MapTaskSubmitter):
    """A task submitter for MapOperator that uses normal Ray tasks."""

    def __init__(self, ray_remote_args: Dict[str, Any]):
        """Create a TaskPoolSubmitter instance.

        Args:
            ray_remote_args: Remote arguments for the Ray tasks to be launched.
        """
        self._ray_remote_args = ray_remote_args

    def submit(
        self,
        transform_fn: ObjectRef[Callable[[Iterator[Block]], Iterator[Block]]],
        input_blocks: List[ObjectRef[Block]],
    ) -> ObjectRef[ObjectRefGenerator]:
        # Submit the task as a normal Ray task.
        map_task = cached_remote_fn(_map_task, num_returns="dynamic")
        return map_task.options(**self._ray_remote_args).remote(
            transform_fn, *input_blocks
        )

    def cancellable(self):
        # Normal Ray tasks are cancellable.
        return True


def _map_task(
    fn: Callable[[Iterator[Block]], Iterator[Block]],
    *blocks: Block,
) -> Iterator[Union[Block, List[BlockMetadata]]]:
    """Remote function for a single operator task.

    Args:
        fn: The callable that takes Iterator[Block] as input and returns
            Iterator[Block] as output.
        blocks: The concrete block values from the task ref bundle.

    Returns:
        A generator of blocks, followed by the list of BlockMetadata for the blocks
        as the last generator return.
    """
    output_metadata = []
    stats = BlockExecStats.builder()
    for b_out in fn(iter(blocks)):
        # TODO(Clark): Add input file propagation from input blocks.
        m_out = BlockAccessor.for_block(b_out).get_metadata([], None)
        m_out.exec_stats = stats.build()
        output_metadata.append(m_out)
        yield b_out
        stats = BlockExecStats.builder()
    yield output_metadata


class ActorPoolSubmitter(MapTaskSubmitter):
    """A task submitter for MapOperator that uses a Ray actor pool."""

    def __init__(
        self, compute_strategy: ActorPoolStrategy, ray_remote_args: Dict[str, Any]
    ):
        """Create an ActorPoolSubmitter instance.

        Args:
            compute_strategy: The configured ActorPoolStrategy.
            ray_remote_args: Remote arguments for the Ray actors to be created.
        """
        # TODO(Clark): Better mapping from configured min/max pool size to static pool
        # size?
        pool_size = compute_strategy.max_size
        if pool_size == float("inf"):
            pool_size = compute_strategy.min_size
        self._pool_size = pool_size
        self._ray_remote_args = ray_remote_args
        # A map from task output futures to the actors on which they are running.
        self._active_actors: Dict[ObjectRef[Block], ray.actor.ActorHandle] = {}
        # The actor pool on which we are running map tasks.
        self._actor_pool = ActorPool()

    def start(self):
        # Create the actor workers and add them to the pool.
        ray_remote_args = self._apply_default_remote_args(self._ray_remote_args)
        cls_ = ray.remote(**ray_remote_args)(MapWorker)
        for _ in range(self._pool_size):
            self._actor_pool.add_actor(cls_.remote())

    def submit(
        self,
        transform_fn: ObjectRef[Callable[[Iterator[Block]], Iterator[Block]]],
        input_blocks: List[ObjectRef[Block]],
    ) -> Tuple[ObjectRef[Block], ObjectRef[BlockMetadata]]:
        # Pick an actor from the pool.
        actor = self._actor_pool.pick_actor()
        # Submit the map task.
        block, block_metadata = actor.submit.options(num_returns=2).remote(
            transform_fn, *input_blocks
        )
        self._active_actors[block] = actor
        return block, block_metadata

    def task_done(self, ref: ObjectRef[Block]):
        # Return the actor that was running the task to the pool.
        actor = self._active_actors.pop(ref)
        self._actor_pool.return_actor(actor)

    def task_submission_done(self):
        # Kill all idle actors in the pool, and ensure that all remaining actors in the
        # pool will be killed as they become idle.
        self._actor_pool.kill_idle_actors()
        self._actor_pool.kill_future_idle_actors()

    def cancellable(self):
        # Actor tasks are not cancellable.
        return False

    @staticmethod
    def _apply_default_remote_args(ray_remote_args: Dict[str, Any]) -> Dict[str, Any]:
        """Apply defaults to the actor creation remote args."""
        ray_remote_args = ray_remote_args.copy()
        if "num_cpus" not in ray_remote_args:
            ray_remote_args["num_cpus"] = 1
        if "scheduling_strategy" not in ray_remote_args:
            ctx = DatasetContext.get_current()
            if ctx.scheduling_strategy == DEFAULT_SCHEDULING_STRATEGY:
                ray_remote_args["scheduling_strategy"] = "SPREAD"
            else:
                ray_remote_args["scheduling_strategy"] = ctx.scheduling_strategy
        return ray_remote_args


class MapWorker:
    """An actor worker for MapOperator."""

    def ready(self):
        return "ok"

    def submit(
        self, fn: Callable[[Iterator[Block]], Iterator[Block]], *blocks: Block
    ) -> Tuple[Block, BlockMetadata]:
        # Coalesce all fn output blocks.
        # TODO(Clark): Remove this coalescing once dynamic block splitting is supported
        # for actors.
        # yield from _map_task(fn, *blocks)
        stats = BlockExecStats.builder()
        builder = DelegatingBlockBuilder()
        for block in fn(iter(blocks)):
            builder.add_block(block)
        block = builder.build()
        block_metadata = BlockAccessor.for_block(block).get_metadata([], None)
        block_metadata.exec_stats = stats.build()
        return block, block_metadata


class ActorPool:
    """A pool of actors for map task execution.

    This class is in charge of tracking the number of in-flight tasks per actor,
    providing the least heavily loaded actor to the task submitter, and killing idle
    actors when the task submitter is done submitting work to the pool.
    """

    def __init__(self):
        # Number of tasks in flight per actor.
        self._tasks_in_flight: Dict[ray.actor.ActorHandle, int] = {}
        # Whether actors that become idle should be eagerly killed. This is False until
        # the first call to kill_idle_actors().
        self._should_kill_idle_actors = False

    def add_actor(self, actor: ray.actor.ActorHandle):
        """Adds an actor to the pool."""
        self._tasks_in_flight[actor] = 0

    def pick_actor(self) -> ray.actor.ActorHandle:
        """Provides the least heavily loaded actor in the pool for task submission."""
        actor = min(
            self._tasks_in_flight.keys(), key=lambda actor: self._tasks_in_flight[actor]
        )
        self._tasks_in_flight[actor] += 1
        return actor

    def return_actor(self, actor: ray.actor.ActorHandle):
        """Returns the provided actor to the pool."""
        self._tasks_in_flight[actor] -= 1
        if self._should_kill_idle_actors and self._tasks_in_flight[actor] == 0:
            self._kill_actor(actor)

    def has_actor(self, actor: ray.actor.ActorHandle) -> bool:
        """Whether the provided actor is in this pool."""
        return actor in self._tasks_in_flight

    def get_tasks_in_flight(self, actor: ray.actor.ActorHandle) -> int:
        """The number of tasks in flight for the provided actor."""
        return self._tasks_in_flight[actor]

    @property
    def num_actors(self) -> int:
        """The number of actors in the pool."""
        return len(self._tasks_in_flight)

    def kill_idle_actors(self):
        """Kills all currently idle actors, and ensures that all actors that become idle
        in the future will be eagerly killed.

        This is called once the task submitter is done submitting work to the pool.
        """
        idle_actors = [
            actor
            for actor, tasks_in_flight in self._tasks_in_flight.items()
            if tasks_in_flight == 0
        ]
        for actor in idle_actors:
            self._kill_actor(actor)

    def kill_future_idle_actors(self):
        """Ensure that all actors that become idle in the future will be eagerly killed.

        This is called once the task submitter is done submitting work to the pool.
        """
        self._should_kill_idle_actors = True

    def _kill_actor(self, actor: ray.actor.ActorHandle):
        """Kill the provided actor and remove it from the pool."""
        ray.kill(actor)
        del self._tasks_in_flight[actor]
