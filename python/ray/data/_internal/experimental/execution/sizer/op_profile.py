"""Static per-operator sizing traits, derived once per run."""

from __future__ import annotations

import math
import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, Tuple

from ray.data._internal.execution.operators.output_splitter import OutputSplitter
from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
    ExperimentalActorPoolMapOperator,
    next_actor_pool_ops,
    prev_actor_pool_ops,
)
from ray.data._internal.logical.operators.write_operator import Write

if TYPE_CHECKING:
    from ray.data._internal.actor_autoscaler import AutoscalingActorPool
    from ray.data._internal.execution.interfaces.physical_operator import (
        PhysicalOperator,
    )
    from ray.data._internal.execution.streaming_executor_state import Topology


def is_sink_or_write(op: "PhysicalOperator") -> bool:
    if isinstance(op, OutputSplitter):
        return True
    return any(isinstance(lop, Write) for lop in op._logical_operators)


def bounded_max_size(pool: "AutoscalingActorPool") -> int:
    """The pool's max size as an int; an unbounded pool (``inf``) reads as
    ``sys.maxsize`` so cap arithmetic stays integral."""
    max_size = pool.max_size()
    return int(max_size) if math.isfinite(max_size) else sys.maxsize


@dataclass(frozen=True)
class OpProfile:
    """One operator's sizing traits, derived once from its configuration.

    Everything here is fixed for the operator's lifetime, so it is computed
    once per run and every per-op special case becomes a field read rather
    than a predicate re-evaluated in the hot loop.
    """

    # "gpu" if the op's actors each request GPU, else "cpu" -- the
    # scarce resource type the warmup equal-share sizes against.
    resource_type: str
    # The pool's max size as an int (see ``bounded_max_size``).
    pool_cap: int
    # True if the op's actors are placed through a placement group -- a
    # static PG (sized against its bundles) or a ``ray_remote_args_fn`` that
    # mints one per actor (capped at user concurrency) -- so the sizer never
    # picks their node.
    pg_managed: bool = False


def build_op_profile(op: ExperimentalActorPoolMapOperator) -> OpProfile:
    pool = op.actor_pool
    return OpProfile(
        resource_type="gpu" if pool.per_actor_resource_usage().gpu > 0 else "cpu",
        pool_cap=bounded_max_size(pool),
        pg_managed=op.placement_constraint().is_pg_managed,
    )


class OpProfiles:
    """Per-operator sizing traits and the operator listings every phase shares.

    All of it is fixed for the run: the topology's operator set never changes
    once execution starts, and each operator's traits come from its
    configuration. So this is built once and read as attributes afterwards,
    rather than re-derived from the topology on every call.
    """

    def __init__(self, topology: "Topology"):
        self.topology = topology
        # Every actor-pool operator that has a pool.
        self.actor_pool_ops: Tuple[ExperimentalActorPoolMapOperator, ...] = tuple(
            op
            for op in topology
            if isinstance(op, ExperimentalActorPoolMapOperator)
            and op.get_autoscaling_actor_pools()
        )
        self._profiles: Dict[ExperimentalActorPoolMapOperator, OpProfile] = {
            op: build_op_profile(op) for op in self.actor_pool_ops
        }
        # Ops the sizer places itself, and ops placed through a placement group.
        self.non_pg_ops: Tuple[ExperimentalActorPoolMapOperator, ...] = tuple(
            op for op in self.actor_pool_ops if not self._profiles[op].pg_managed
        )
        self.pg_ops: Tuple[ExperimentalActorPoolMapOperator, ...] = tuple(
            op for op in self.actor_pool_ops if self._profiles[op].pg_managed
        )
        # Nearest actor-pool neighbours of each op (see next_actor_pool_ops).
        self.next_ops: Dict[
            ExperimentalActorPoolMapOperator,
            Tuple[ExperimentalActorPoolMapOperator, ...],
        ] = {op: next_actor_pool_ops(op) for op in self.actor_pool_ops}
        self.prev_ops: Dict[
            ExperimentalActorPoolMapOperator,
            Tuple[ExperimentalActorPoolMapOperator, ...],
        ] = {op: prev_actor_pool_ops(op) for op in self.actor_pool_ops}

    def profile(self, op: ExperimentalActorPoolMapOperator) -> OpProfile:
        return self._profiles[op]

    def is_pg_managed(self, op: ExperimentalActorPoolMapOperator) -> bool:
        """True if ``op`` is placed through a placement group, not by the
        sizer -- see ``OpProfile.pg_managed`` for the cases."""
        return self._profiles[op].pg_managed

    def resource_type(self, op: ExperimentalActorPoolMapOperator) -> str:
        """ "gpu" if the op's actors each request GPU, else "cpu"."""
        return self._profiles[op].resource_type

    def pool_cap(self, op: ExperimentalActorPoolMapOperator) -> int:
        """Hard pool ceiling: the pool's max size as an int."""
        return self._profiles[op].pool_cap
