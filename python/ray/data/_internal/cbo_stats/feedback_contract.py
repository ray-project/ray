"""Reuse contract for cross-execution runtime feedback (design §7.4.3).

A data or UDF version token only proves *identity*. It says nothing about
whether the next execution will produce the same output volume: the actual
batch size, batch coalescing, operator fusion, Task vs. Actor execution and
actor state scope all change how a UDF is invoked, and therefore how much data
it emits. A feedback entry that was observed under one execution shape must not
be reused under another, even when both sides report the same version token.

This module defines the key that a feedback entry is stored under and the gate
that every consumer (planner, partition deriver, R shadow estimate) must call
before reusing an observation. It is deliberately dependency free so it can be
used from the logical, physical and execution layers.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Optional, Tuple

from ray.data._internal.cbo_stats.operator_statistics import ConfidenceLevel

# Number of complete observations after which a stable boundary is trusted at
# ``HIGH`` instead of ``MEDIUM``. EMA merging beyond this adds no confidence.
RECOMMENDED_SAMPLES = 3

# A boundary whose output volume varies by more than this across executions is
# not predictable enough to drive a partition or reservation decision.
DEFAULT_MAX_RELATIVE_SPREAD = 0.10

_SHAPE_FIELDS = (
    "batch_size",
    "batch_format",
    "coalesce_batches",
    "fused_operators",
    "execution_mode",
    "state_scope",
)


@dataclass(frozen=True)
class ExecutionShape:
    """How an operator was actually invoked by the executor.

    Two executions with the same ``ExecutionShape`` are expected to chunk and
    buffer the UDF identically; any difference invalidates an observation.
    ``None`` means "unknown", which is *not* equal to a concrete value.
    """

    # Target batch size in rows as configured for the execution.
    batch_size: Optional[int] = None
    # Batch layout handed to the UDF (e.g. ``numpy``, ``pandas``, ``pyarrow``).
    batch_format: Optional[str] = None
    # Whether small blocks were coalesced into full batches before the call.
    coalesce_batches: Optional[bool] = None
    # Logical operator names of the fused segment, in execution order. Fusion
    # changes how many rows reach one UDF invocation.
    fused_operators: Tuple[str, ...] = ()
    # ``task`` or ``actor_pool``.
    execution_mode: Optional[str] = None
    # Actor pool identity for stateful UDFs; ``None`` for stateless execution.
    state_scope: Optional[str] = None

    def is_known(self) -> bool:
        """Whether every volume-affecting field was observed."""
        return all(
            value is not None
            for value in (
                self.batch_size,
                self.batch_format,
                self.coalesce_batches,
                self.execution_mode,
            )
        )

    def diff(self, other: "ExecutionShape") -> Tuple[str, ...]:
        """Human-readable list of differing fields."""
        differences = []
        for name in _SHAPE_FIELDS:
            mine, theirs = getattr(self, name), getattr(other, name)
            if mine != theirs:
                differences.append(f"{name}: {mine!r} != {theirs!r}")
        return tuple(differences)


@dataclass(frozen=True)
class FeedbackKey:
    """Identity of a boundary observation.

    The identity fields say *what* was executed; ``shape`` says *how*. Both are
    required: an identity match with a different shape is a different
    experiment, not a cache hit.
    """

    job_scope: str
    data_version: str
    udf_version: str
    operator: str
    params_digest: str
    shape: ExecutionShape

    def identity_matches(self, other: "FeedbackKey") -> bool:
        """Whether both keys describe the same job, data, UDF and parameters."""
        return (
            self.job_scope == other.job_scope
            and self.data_version == other.data_version
            and self.udf_version == other.udf_version
            and self.operator == other.operator
            and self.params_digest == other.params_digest
        )

    def fingerprint(self) -> str:
        """Stable cache key covering identity *and* execution shape."""
        payload = asdict(self)
        payload["shape"]["fused_operators"] = list(self.shape.fused_operators)
        encoded = json.dumps(payload, sort_keys=True, default=str)
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class BoundaryObservation:
    """A frozen measurement of one operator boundary."""

    key: FeedbackKey
    # ``None`` means "not observable" and never becomes zero.
    output_rows: Optional[int] = None
    output_bytes: Optional[int] = None
    # Number of complete executions merged into this observation.
    sample_count: int = 1
    # (max - min) / median of the observed output volume across executions.
    relative_spread: Optional[float] = None
    # Fraction of the input blocks that reached this boundary (1.0 = all).
    coverage: float = 1.0
    # Natural exhaustion with normal consumption, not a cancel/limit/failure.
    complete: bool = False


@dataclass(frozen=True)
class ReuseDecision:
    """Outcome of the gate. ``reasons`` is always populated on rejection."""

    allowed: bool
    reasons: Tuple[str, ...] = ()
    confidence: float = ConfidenceLevel.UNKNOWN.value
    # A conservative width multiplier applied to the observation when it is
    # reused downstream (``1.0`` for a direct reuse).
    safety_factor: float = field(default=1.0)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "allowed": self.allowed,
            "reasons": list(self.reasons),
            "confidence": self.confidence,
            "safety_factor": self.safety_factor,
        }


def evaluate_reuse(
    observation: BoundaryObservation,
    *,
    planned_key: Optional[FeedbackKey] = None,
    repeatable: bool = False,
    max_relative_spread: float = DEFAULT_MAX_RELATIVE_SPREAD,
    min_coverage: float = 1.0,
    min_confidence: float = ConfidenceLevel.LOW.value,
) -> ReuseDecision:
    """Decides whether an observation may drive a planning decision.

    All of the following must hold; otherwise the caller must fall back to the
    static estimate and keep the observation for diagnostics only:

    * a planning-time key exists and its execution shape equals the recorded one
      (an identity/version match alone proves nothing about output volume);
    * the UDF/data was explicitly declared repeatable (random or stateful UDFs
      are observation-only);
    * the observation came from a complete, fully covered execution;
    * the observed volume is dispersed tightly enough to be predictable.
    """
    reasons = []

    if planned_key is None:
        reasons.append(
            "no planning-time execution shape: a version token alone does not "
            "prove that the next execution chunks identically"
        )
    elif not observation.key.identity_matches(planned_key):
        reasons.append("identity mismatch (job/data/UDF/params)")
    else:
        if not observation.key.shape.is_known() or not planned_key.shape.is_known():
            reasons.append("execution shape is incomplete")
        differences = observation.key.shape.diff(planned_key.shape)
        if differences:
            reasons.append("execution shape changed: " + "; ".join(differences))

    if not repeatable:
        reasons.append(
            "UDF/data is not declared repeatable (random or external-state "
            "dependent UDFs are observation-only)"
        )
    if not observation.complete:
        reasons.append("observation does not come from a complete execution")
    if observation.coverage < min_coverage:
        reasons.append(
            f"coverage {observation.coverage:.2f} < required {min_coverage:.2f}"
        )

    confidence = _confidence(observation, max_relative_spread)
    if observation.relative_spread is None:
        reasons.append("dispersion is unknown, so predictability cannot be proven")
    elif observation.relative_spread > max_relative_spread:
        reasons.append(
            f"relative spread {observation.relative_spread:.2f} exceeds "
            f"{max_relative_spread:.2f}"
        )
    if confidence < min_confidence:
        reasons.append(f"confidence {confidence:.2f} < required {min_confidence:.2f}")

    if reasons:
        return ReuseDecision(
            allowed=False,
            reasons=tuple(reasons),
            confidence=confidence,
        )

    return ReuseDecision(
        allowed=True,
        confidence=confidence,
        # Reusing a boundary still widens it by the observed dispersion: the
        # next execution will not reproduce the median exactly.
        safety_factor=1.0 + float(observation.relative_spread or 0.0),
    )


def _confidence(
    observation: BoundaryObservation,
    max_relative_spread: float,
) -> float:
    """Confidence in a boundary, from sample count and dispersion.

    A single observation is capped at ``MEDIUM``: repeating an execution is the
    only evidence that the boundary is stable across runs.
    """
    if observation.relative_spread is None:
        return ConfidenceLevel.UNKNOWN.value
    if observation.sample_count >= RECOMMENDED_SAMPLES:
        base = ConfidenceLevel.HIGH.value
    elif observation.sample_count >= 1:
        base = ConfidenceLevel.MEDIUM.value
    else:
        return ConfidenceLevel.UNKNOWN.value

    if max_relative_spread <= 0:
        return base
    stability = 1.0 - min(observation.relative_spread / max_relative_spread, 1.0)
    return round(base * stability, 4)
