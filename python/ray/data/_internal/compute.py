import logging
from typing import Any, Callable, Iterable, Optional, TypeVar, Union

from ray.data._internal.execution.interfaces import TaskContext
from ray.data.block import Block, UserDefinedFunction
from ray.util.annotations import DeveloperAPI, PublicAPI

logger = logging.getLogger(__name__)

T = TypeVar("T")
U = TypeVar("U")


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


@PublicAPI
class TaskPoolStrategy(ComputeStrategy):
    """Specify the task-based compute strategy for a Dataset transform.

    TaskPoolStrategy executes dataset transformations using Ray tasks that are
    scheduled through a pool. Provide ``size`` to cap the number of concurrent
    tasks; leave it unset to allow Ray Data to scale the task count
    automatically.
    """

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

    def __repr__(self) -> str:
        return f"TaskPoolStrategy(size={self.size})"


@PublicAPI
class ActorPoolStrategy(ComputeStrategy):
    """Specify the actor-based compute strategy for a Dataset transform.

    ActorPoolStrategy specifies that an autoscaling pool of actors should be used
    for a given Dataset transform. This is useful for stateful setup of callable
    classes.

    For a fixed-sized pool of size ``n``, use ``ActorPoolStrategy(size=n)``.

    To autoscale from ``m`` to ``n`` actors, use
    ``ActorPoolStrategy(min_size=m, max_size=n)``.

    To autoscale from ``m`` to ``n`` actors, with an initial size of ``initial``, use
    ``ActorPoolStrategy(min_size=m, max_size=n, initial_size=initial)``.

    To increase opportunities for pipelining task dependency prefetching with
    computation and avoiding actor startup delays, set max_tasks_in_flight_per_actor
    to 2 or greater; to try to decrease the delay due to queueing of tasks on the worker
    actors, set max_tasks_in_flight_per_actor to 1.

    The `enable_true_multi_threading` argument primarily exists to prevent GPU OOM issues with multi-threaded actors.
    The life cycle of an actor task involves 3 main steps:

        1. Batching Inputs
        2. Running actor UDF
        3. Batching Outputs

    The `enable_true_multi_threading` flag affects step 2. If set to `True`, then the UDF can be run concurrently.
    By default, it is set to `False`, so at most 1 actor UDF is running at a time per actor. The `max_concurrency`
    flag on `ray.remote` affects steps 1 and 3. Below is a matrix summary:

    - [`enable_true_multi_threading=False or True`, `max_concurrency=1`] = 1 actor task running per actor. So at most 1
        of steps 1, 2, or 3 is running at any point in time.
    - [`enable_true_multi_threading=False`, `max_concurrency>1`] = multiple tasks running per actor
      (respecting GIL) but UDF runs 1 at a time. This is useful for doing CPU and GPU work,
      where you want to use a large batch size but want to hide the overhead of *batching*
      the inputs. In this case, CPU *batching* is done concurrently, while GPU *inference*
      is done 1 at a time. Concretely, steps 1 and 3 can have multiple threads, while step 2 is done serially.
    - [`enable_true_multi_threading=True`, `max_concurrency>1`] = multiple tasks running per actor.
      Unlike bullet #3 ^, the UDF runs concurrently (respecting GIL). No restrictions on steps 1, 2, or 3

    NOTE: `enable_true_multi_threading` does not apply to async actors
    """

    def __init__(
        self,
        *,
        size: Optional[int] = None,
        min_size: Optional[int] = None,
        max_size: Optional[int] = None,
        initial_size: Optional[int] = None,
        max_tasks_in_flight_per_actor: Optional[int] = None,
        enable_true_multi_threading: bool = False,
    ):
        """Construct ActorPoolStrategy for a Dataset transform.

        Args:
            size: Specify a fixed size actor pool of this size. It is an error to
                specify both `size` and `min_size` or `max_size`.
            min_size: The minimum size of the actor pool.
            max_size: The maximum size of the actor pool.
            initial_size: The initial number of actors to start with. If not specified,
                defaults to min_size. Must be between min_size and max_size.
            max_tasks_in_flight_per_actor: The maximum number of tasks to concurrently
                send to a single actor worker. Increasing this will increase
                opportunities for pipelining task dependency prefetching with
                computation and avoiding actor startup delays, but will also increase
                queueing delay.
            enable_true_multi_threading: If enable_true_multi_threading=True, no more than 1 actor task
                runs per actor. Otherwise, respects the `max_concurrency` argument.
        """
        if size is not None:
            if size < 1:
                raise ValueError("size must be >= 1", size)
            if max_size is not None or min_size is not None or initial_size is not None:
                raise ValueError(
                    "min_size, max_size, and initial_size cannot be set at the same time as `size`"
                )
            min_size = size
            max_size = size
            initial_size = size
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

        # Validate and set initial_size
        if initial_size is not None:
            if initial_size < self.min_size:
                raise ValueError(
                    f"initial_size ({initial_size}) must be >= min_size ({self.min_size})"
                )
            if self.max_size != float("inf") and initial_size > self.max_size:
                raise ValueError(
                    f"initial_size ({initial_size}) must be <= max_size ({self.max_size})"
                )

        self.initial_size = initial_size or self.min_size
        self.max_tasks_in_flight_per_actor = max_tasks_in_flight_per_actor
        self.num_workers = 0
        self.ready_to_total_workers_ratio = 0.8
        self.enable_true_multi_threading = enable_true_multi_threading

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, ActorPoolStrategy) and (
            self.min_size == other.min_size
            and self.max_size == other.max_size
            and self.initial_size == other.initial_size
            and self.enable_true_multi_threading == other.enable_true_multi_threading
            and self.max_tasks_in_flight_per_actor
            == other.max_tasks_in_flight_per_actor
        )

    def __repr__(self) -> str:
        return (
            f"ActorPoolStrategy(min_size={self.min_size}, "
            f"max_size={self.max_size}, "
            f"initial_size={self.initial_size}, "
            f"max_tasks_in_flight_per_actor={self.max_tasks_in_flight_per_actor})"
            f"num_workers={self.num_workers}, "
            f"enable_true_multi_threading={self.enable_true_multi_threading}, "
            f"ready_to_total_workers_ratio={self.ready_to_total_workers_ratio})"
        )


def get_compute(compute_spec: Union[str, ComputeStrategy]) -> ComputeStrategy:
    if not isinstance(compute_spec, (TaskPoolStrategy, ActorPoolStrategy)):
        raise ValueError(
            "In Ray 2.5, the compute spec must be either "
            f"TaskPoolStrategy or ActorPoolStrategy, was: {compute_spec}."
        )
    elif not compute_spec or compute_spec == "tasks":
        return TaskPoolStrategy()
    elif compute_spec == "actors":
        return ActorPoolStrategy()
    elif isinstance(compute_spec, ComputeStrategy):
        return compute_spec
    else:
        raise ValueError("compute must be one of [`tasks`, `actors`, ComputeStrategy]")
