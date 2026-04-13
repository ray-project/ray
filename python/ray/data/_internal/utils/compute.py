import warnings
from typing import Callable, Iterable, Union

from ray.data._internal.execution.interfaces import TaskContext
from ray.data.block import Block, UserDefinedFunction

warnings.warn(
    "Importing from ray.data._internal.utils.compute is deprecated. "
    "Use ray.data._internal.public_api.compute instead.",
    DeprecationWarning,
    stacklevel=2,
)

from ray.data._internal.public_api.compute import (  # noqa: E402, F401
    ActorPoolStrategy,
    ComputeStrategy,
    TaskPoolStrategy,
)

# Internal-only type alias — not part of the public API.
BlockTransform = Union[
    Callable[[Iterable[Block], TaskContext], Iterable[Block]],
    Callable[[Iterable[Block], TaskContext, UserDefinedFunction], Iterable[Block]],
    Callable[..., Iterable[Block]],
]


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
