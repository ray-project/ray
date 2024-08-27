import logging
from typing import Any, Callable, Iterable, Optional, TypeVar, Union

from ray.data._internal.execution.interfaces import TaskContext
from ray.data.block import Block, UserDefinedFunction
from ray.util.annotations import DeveloperAPI

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
        if size is not None:
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
