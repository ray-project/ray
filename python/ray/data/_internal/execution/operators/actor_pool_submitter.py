from typing import Dict, Any, Iterator, Callable, List, Tuple
import ray
from ray.data.block import Block, BlockAccessor, BlockMetadata, BlockExecStats
from ray.data.context import DEFAULT_SCHEDULING_STRATEGY, DatasetContext
from ray.data._internal.compute import ActorPoolStrategy
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.execution.operators.map_task_submitter import MapTaskSubmitter
from ray.types import ObjectRef


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
        # Check that actor pool hasn't yet been initialized.
        assert self._actor_pool.num_actors == 0
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

    def cancellable(self) -> bool:
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
