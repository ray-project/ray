from typing import Dict, Any, Iterator, Callable, List, Union
import ray
from ray.data.block import Block, BlockMetadata
from ray.data.context import DatasetContext
from ray.data.context import DEFAULT_SCHEDULING_STRATEGY
from ray.data._internal.execution.operators.map_task_submitter import (
    MapTaskSubmitter,
    _map_task,
)
from ray.types import ObjectRef
from ray._raylet import ObjectRefGenerator


class ActorPoolSubmitter(MapTaskSubmitter):
    """A task submitter for MapOperator that uses a Ray actor pool."""

    def __init__(
        self,
        transform_fn_ref: ObjectRef[Callable[[Iterator[Block]], Iterator[Block]]],
        ray_remote_args: Dict[str, Any],
        pool_size: int,
    ):
        """Create an ActorPoolSubmitter instance.

        Args:
            transform_fn_ref: The function to apply to a block bundle in the submitted
                map task.
            ray_remote_args: Remote arguments for the Ray actors to be created.
            pool_size: The size of the actor pool.
        """
        self._transform_fn_ref = transform_fn_ref
        self._ray_remote_args = ray_remote_args
        self._pool_size = pool_size
        # A map from task output futures to the actors on which they are running.
        self._active_actors: Dict[ObjectRef[Block], ray.actor.ActorHandle] = {}
        # The actor pool on which we are running map tasks.
        self._actor_pool = ActorPool()

    def progress_str(self) -> str:
        return f"{self._actor_pool.size()} actors"

    def start(self):
        # Create the actor workers and add them to the pool.
        ray_remote_args = self._apply_default_remote_args(self._ray_remote_args)
        cls_ = ray.remote(**ray_remote_args)(MapWorker)
        for _ in range(self._pool_size):
            self._actor_pool.add_actor(cls_.remote())

    def submit(
        self, input_blocks: List[ObjectRef[Block]]
    ) -> ObjectRef[ObjectRefGenerator]:
        # Pick an actor from the pool.
        actor = self._actor_pool.pick_actor()
        # Submit the map task.
        ref = actor.submit.options(num_returns="dynamic").remote(
            self._transform_fn_ref, *input_blocks
        )
        self._active_actors[ref] = actor
        return ref

    def task_done(self, ref: ObjectRef[ObjectRefGenerator]):
        # Return the actor that was running the task to the pool.
        actor = self._active_actors.pop(ref)
        self._actor_pool.return_actor(actor)

    def task_submission_done(self):
        # Kill all idle actors in the pool, and ensure that all remaining actors in the
        # pool will be killed as they become idle.
        self._actor_pool.kill_idle_actors()

    def shutdown(self, _):
        self._actor_pool.kill_all_actors()

    @staticmethod
    def _apply_default_remote_args(ray_remote_args: Dict[str, Any]) -> Dict[str, Any]:
        """Apply defaults to the actor creation remote args."""
        ray_remote_args = ray_remote_args.copy()
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
    ) -> Iterator[Union[Block, List[BlockMetadata]]]:
        yield from _map_task(fn, *blocks)


class ActorPool:
    """A pool of actors for map task execution.

    This class is in charge of tracking the number of in-flight tasks per actor,
    providing the least heavily loaded actor to the task submitter, and killing idle
    actors when the task submitter is done submitting work to the pool.
    """

    def __init__(self):
        # Number of tasks in flight per actor.
        self._num_tasks_in_flight: Dict[ray.actor.ActorHandle, int] = {}
        # Whether actors that become idle should be eagerly killed. This is False until
        # the first call to kill_idle_actors().
        self._should_kill_idle_actors = False

    def size(self) -> int:
        """Return the current actor pool size."""
        return len(self._num_tasks_in_flight)

    def add_actor(self, actor: ray.actor.ActorHandle):
        """Adds an actor to the pool."""
        self._num_tasks_in_flight[actor] = 0

    def pick_actor(self) -> ray.actor.ActorHandle:
        """Provides the least heavily loaded actor in the pool for task submission."""
        if not self._num_tasks_in_flight:
            raise ValueError("Actor pool is empty.")

        actor = min(
            self._num_tasks_in_flight.keys(),
            key=lambda actor: self._num_tasks_in_flight[actor],
        )
        self._num_tasks_in_flight[actor] += 1
        return actor

    def return_actor(self, actor: ray.actor.ActorHandle):
        """Returns the provided actor to the pool."""
        if actor not in self._num_tasks_in_flight:
            raise ValueError(
                f"Actor {actor} doesn't exist in pool: "
                f"{list(self._num_tasks_in_flight.keys())}"
            )
        if self._num_tasks_in_flight[actor] == 0:
            raise ValueError(f"Actor {actor} has already been returned by all pickers.")

        self._num_tasks_in_flight[actor] -= 1
        if self._should_kill_idle_actors and self._num_tasks_in_flight[actor] == 0:
            self._kill_actor(actor)

    def kill_idle_actors(self):
        """Kills all currently idle actors and ensures that all actors that become idle
        in the future will be eagerly killed.

        This is called once the task submitter is done submitting work to the pool.
        """
        idle_actors = [
            actor
            for actor, tasks_in_flight in self._num_tasks_in_flight.items()
            if tasks_in_flight == 0
        ]
        for actor in idle_actors:
            self._kill_actor(actor)
        self._should_kill_idle_actors = True

    def kill_all_actors(self):
        """Kills all currently idle actors.

        This is called once the task submitter is shutting down.
        """
        all_actors = list(self._num_tasks_in_flight.keys())
        for actor in all_actors:
            self._kill_actor(actor)

    def _kill_actor(self, actor: ray.actor.ActorHandle):
        """Kill the provided actor and remove it from the pool."""
        ray.kill(actor)
        del self._num_tasks_in_flight[actor]
