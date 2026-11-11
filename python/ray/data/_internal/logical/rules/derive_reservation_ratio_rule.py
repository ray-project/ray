"""Physical optimizer rule deriving an advisory resource reservation ratio."""

import logging
from typing import Iterable, List, Optional, Tuple, Type

from ray.data._internal.cbo_stats.operator_statistics import ConfidenceLevel
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.logical.interfaces import Plan, Rule
from ray.data._internal.logical.rules.operator_fusion import FuseOperators

logger = logging.getLogger(__name__)


class DeriveReservationRatioRule(Rule):
    """Derives ``op_resource_reservation_ratio`` from plan statistics.

    The rule never modifies the plan. When ``enable_cost_based_optimization`` is
    on and the user hasn't configured the ratio explicitly, it records the
    derived ratio in ``DataContext._cbo_derived_reservation_ratio``, which is
    picked up by ``create_resource_allocator``. Any failure or lack of reliable
    statistics leaves the default ratio in place.
    """

    @classmethod
    def dependencies(cls) -> List[Type[Rule]]:
        # Fusion changes the set of physical operators that get reservations.
        return [FuseOperators]

    def apply(self, plan: Plan) -> Plan:
        ctx = plan.context
        if ctx is None:
            return plan
        # Always reset first, so that a value derived for a previous plan sharing
        # this context never leaks into the current execution.
        ctx._cbo_derived_reservation_ratio = None

        if (
            not ctx.enable_cost_based_optimization
            or not ctx.op_resource_reservation_enabled
            or getattr(ctx, "_user_set_reservation_ratio", True)
        ):
            return plan

        try:
            from ray.data._internal.cbo_stats.reservation_ratio import (
                ReservationRatioDeriver,
            )

            demands, confidence = _collect_demands(plan.dag)
            decision = ReservationRatioDeriver.derive(
                demands, confidence, ctx.op_resource_reservation_ratio
            )
        except Exception:
            logger.debug("CBO: failed to derive reservation ratio", exc_info=True)
            return plan

        logger.debug(f"CBO: reservation ratio decision: {decision}")
        if decision.ratio is not None:
            ctx._cbo_derived_reservation_ratio = decision.ratio
        return plan


def _is_eligible(op: PhysicalOperator) -> bool:
    """Mirrors ``ResourceManager.is_op_eligible`` at planning time.

    ``InputDataBuffer`` finishes immediately, so it never holds a reservation.
    """
    return not isinstance(op, InputDataBuffer) and not op.throttling_disabled()


def _iter_ops(dag: PhysicalOperator) -> Iterable[PhysicalOperator]:
    seen = set()
    stack = [dag]
    while stack:
        op = stack.pop()
        if id(op) in seen:
            continue
        seen.add(id(op))
        yield op
        stack.extend(op.input_dependencies)


def _output_estimate(op: PhysicalOperator) -> Tuple[Optional[int], float]:
    """Estimated output bytes of ``op`` and their confidence.

    An estimate without a proven upper bound (a UDF prior, an extrapolated
    read size) can't exclude a much larger output, so it is never trusted
    above ``LOW``. Raw input bytes are not a bound for downstream volume.
    """
    logical_ops = getattr(op, "_logical_operators", None)
    if not logical_ops:
        return None, 0.0
    # Fused operators list logical ops upstream-first; the last one produces
    # the output of the physical operator.
    stats = logical_ops[-1].infer_statistics()
    if stats is None or stats.size_bytes is None:
        return None, 0.0
    confidence = stats.confidence
    if not stats.has_size_upper_bound():
        confidence = min(confidence, ConfidenceLevel.LOW.value)
    return stats.size_bytes, confidence


def _collect_demands(
    dag: PhysicalOperator,
) -> Tuple[List[Optional[float]], float]:
    """Per eligible op memory demand estimates and their minimum confidence.

    The usage of ineligible downstream operators (e.g. all-to-all, limit) is
    charged to the closest upstream eligible operator by the allocator, so it is
    added to that operator's demand here as well.
    """
    demands: List[Optional[float]] = []
    confidence = 1.0
    ops = list(_iter_ops(dag))
    # Derive consumers from the reachable DAG itself rather than trusting
    # `output_dependencies`, which may reference operators replaced by rules.
    consumers = {id(op): [] for op in ops}
    for op in ops:
        for upstream in op.input_dependencies:
            consumers[id(upstream)].append(op)

    for op in ops:
        if not _is_eligible(op):
            continue
        total: Optional[float] = 0.0
        group = [op]
        frontier = list(consumers[id(op)])
        # A node reachable through several paths must be charged only once.
        seen = set()
        while frontier:
            downstream = frontier.pop()
            if id(downstream) in seen:
                continue
            seen.add(id(downstream))
            if not _is_eligible(downstream):
                group.append(downstream)
                frontier.extend(consumers[id(downstream)])
        for member in group:
            size, conf = _output_estimate(member)
            if size is None:
                total = None
                break
            total += size
            confidence = min(confidence, conf)
        if total is None:
            confidence = 0.0
        demands.append(total)
    return demands, confidence
