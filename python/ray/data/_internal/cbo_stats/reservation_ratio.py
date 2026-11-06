"""Derivation of ``op_resource_reservation_ratio`` from plan statistics.

Background (see ``ReservationOpResourceAllocator``): each eligible operator
reserves ``R * limits / N`` resources and the remaining ``(1 - R) * limits`` form
a shared pool. Reserved-but-unused resources of one operator cannot be used by
another one, so:

* when per-operator demand is *even*, reservations match demand and the default
  ratio is appropriate;
* when demand is *skewed*, the reservations of light operators sit idle while the
  heavy operator is capped, so a *lower* R (larger shared pool) is preferable.

The deriver is therefore monotone in the evenness of the demand distribution and
never raises R above the configured default. Compile-time statistics are coarse,
so the result is shrunk towards the default proportionally to the statistics
confidence, and derivation is skipped when confidence is unknown or a
per-operator demand is missing.

Confidence values are capped upstream (see
``DeriveReservationRatioRule._output_estimate``) when an estimate has no proven
upper bound, so UDF and sampled-read priors can only move the ratio slightly.
"""

import math
from dataclasses import dataclass
from typing import Optional, Sequence

from ray.data._internal.cbo_stats.operator_statistics import ConfidenceLevel


@dataclass(frozen=True)
class ReservationRatioDecision:
    """Outcome of a reservation ratio derivation."""

    # Derived ratio, or ``None`` if the default should be kept.
    ratio: Optional[float]
    # Human readable explanation (logged and shown in tests/debugging).
    reason: str
    # Normalized entropy of per-op demand shares in [0, 1] (1 == perfectly even).
    evenness: Optional[float] = None
    # Aggregated (minimum) statistics confidence across eligible ops.
    confidence: Optional[float] = None
    num_ops: int = 0


class ReservationRatioDeriver:
    """Maps per-operator memory demand estimates to a reservation ratio."""

    # Lower bound of the derived ratio. Keeps enough reservation so that every
    # eligible operator can still make progress (and pull its outputs).
    MIN_RATIO = 0.3

    @classmethod
    def derive(
        cls,
        demands: Sequence[Optional[float]],
        confidence: float,
        default_ratio: float,
    ) -> ReservationRatioDecision:
        """Derive the reservation ratio.

        Args:
            demands: Estimated memory demand (bytes) of each resource-eligible
                operator. ``None`` means unknown.
            confidence: Aggregated confidence of the estimates in [0, 1].
            default_ratio: Ratio used when no derivation is possible. Also the
                upper bound of the derived ratio.

        Returns:
            A ``ReservationRatioDecision``; ``ratio`` is ``None`` when the
            default should be kept.
        """
        num_ops = len(demands)
        if num_ops < 2:
            # With a single eligible op it gets both its reservation and the
            # whole shared pool, so the ratio has no effect.
            return ReservationRatioDecision(
                None, "fewer than 2 resource-eligible operators", num_ops=num_ops
            )
        if any(d is None or d < 0 for d in demands):
            return ReservationRatioDecision(
                None, "missing demand estimate for some operator", num_ops=num_ops
            )
        total = float(sum(demands))
        if total <= 0:
            return ReservationRatioDecision(
                None, "estimated total demand is zero", num_ops=num_ops
            )
        if confidence <= ConfidenceLevel.UNKNOWN.value:
            # A low-but-known confidence is *not* noise by itself: the ratio is
            # shrunk proportionally, so a LOW-level estimate moves R by at most
            # ``LOW * (default - MIN_RATIO)``. Only an unknown confidence has no
            # information at all. Decay makes a UDF prior land below LOW after a
            # couple of operators; gating on LOW would silently exclude exactly
            # the pipelines this estimate exists for.
            return ReservationRatioDecision(
                None,
                "statistics confidence is unknown",
                confidence=confidence,
                num_ops=num_ops,
            )

        evenness = cls.evenness(demands)
        lower = min(cls.MIN_RATIO, default_ratio)
        raw = lower + (default_ratio - lower) * evenness
        # Shrink towards the default proportionally to (1 - confidence).
        ratio = default_ratio + min(1.0, confidence) * (raw - default_ratio)
        ratio = round(min(default_ratio, max(lower, ratio)), 2)
        return ReservationRatioDecision(
            ratio,
            "derived from demand evenness",
            evenness=evenness,
            confidence=confidence,
            num_ops=num_ops,
        )

    @staticmethod
    def evenness(demands: Sequence[float]) -> float:
        """Normalized Shannon entropy of demand shares in [0, 1]."""
        total = float(sum(demands))
        n = len(demands)
        if n < 2 or total <= 0:
            return 1.0
        entropy = 0.0
        for d in demands:
            p = d / total
            if p > 0:
                entropy -= p * math.log(p)
        return max(0.0, min(1.0, entropy / math.log(n)))
