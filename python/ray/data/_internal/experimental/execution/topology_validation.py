"""Topology validation for the actor-only backend.

The actor-only backend runs every operator on an actor pool, so the shuffle /
all-to-all operators — which schedule their own tasks — have no actor-only
implementation. Reject them (and the configs the backend cannot honour) once
the topology is built, rather than deadlocking partway through execution.
"""
from typing import TYPE_CHECKING, Tuple, Type

from ray.data._internal.execution.interfaces import ExecutionOptions, PhysicalOperator
from ray.data._internal.execution.operators.base_physical_operator import (
    AllToAllOperator,
)
from ray.data._internal.execution.operators.hash_aggregate import HashAggregateOperator
from ray.data._internal.execution.operators.hash_shuffle import HashShuffleOperator
from ray.data._internal.execution.operators.join import JoinOperator
from ray.data._internal.execution.operators.shuffle_operators.shuffle_map_operator import (  # noqa: E501
    ShuffleMapOp,
)
from ray.data._internal.execution.operators.shuffle_operators.shuffle_reduce_operator import (  # noqa: E501
    ShuffleReduceOp,
)
from ray.data._internal.execution.operators.zip_operator import ZipOperator

if TYPE_CHECKING:
    from ray.data._internal.execution.streaming_executor_state import Topology

__all__ = ["validate_actor_only_supported_topology"]

# Shuffle / all-to-all physical ops that the actor-only backend does not support.
_UNSUPPORTED_ACTOR_ONLY_OPS: Tuple[Type[PhysicalOperator], ...] = (
    AllToAllOperator,
    HashShuffleOperator,
    HashAggregateOperator,
    JoinOperator,
    ShuffleMapOp,
    ShuffleReduceOp,
    ZipOperator,
)


def validate_actor_only_supported_topology(
    topology: "Topology", options: ExecutionOptions
) -> None:
    """Reject configs / operators the actor-only backend cannot run.

    Args:
        topology: The built streaming topology, iterated for physical operators.
        options: The execution options this run was configured with.

    Raises:
        ValueError: If ``preserve_order`` is set, or the topology contains an
            operator the actor-only backend cannot execute.
    """
    # Operators first: plans that require ordering (Zip, Sort) have
    # ``preserve_order`` forced on by ``Dataset._execute``, so checking the
    # option first would report "preserve_order" for an operator that is itself
    # the unsupported thing, hiding the real reason.
    for op in topology:
        if isinstance(op, _UNSUPPORTED_ACTOR_ONLY_OPS):
            raise ValueError(
                f"{type(op).__name__} is not supported when actor only "
                "backend is enabled"
            )
    if options.preserve_order:
        raise ValueError("preserve_order=True is not supported for actor only backend")
