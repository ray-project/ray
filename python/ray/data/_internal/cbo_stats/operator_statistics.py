"""Plan statistics propagated through the logical DAG for the Ray Data CBO.

NOTE: This module must stay free of Ray-internal imports. It is loaded by file
path from ``ray/data/benchmarks/cbo_benchmark.py``.
"""

import dataclasses
import datetime
import decimal
import logging
import math
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Iterable, Optional

logger = logging.getLogger(__name__)


class ConfidenceLevel(Enum):
    """Confidence of a statistic, in [0, 1]."""

    # Exact value (e.g. in-memory data with known metadata).
    EXACT = 1.0
    # Derived from complete metadata (e.g. all Parquet footers).
    HIGH = 0.95
    # Derived from a sample (e.g. a subset of Parquet footers).
    MEDIUM = 0.7
    # Heuristic default (e.g. UDF filter selectivity).
    LOW = 0.4
    UNKNOWN = 0.0


# Each propagation step through an operator multiplies the confidence by this
# factor, so long chains of estimates are trusted less.
CONFIDENCE_DECAY_PER_OP = 0.9

# Textbook (System R style) defaults used when column statistics are missing.
DEFAULT_EQ_SELECTIVITY = 0.1
DEFAULT_RANGE_SELECTIVITY = 1.0 / 3
DEFAULT_NULL_FRACTION = 0.1
DEFAULT_UNKNOWN_SELECTIVITY = 0.5


@dataclass
class ColumnStatistics:
    """Statistics of a single column (e.g. from Parquet footers)."""

    name: str
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None
    null_count: Optional[int] = None
    # Number of distinct values (NDV). Rarely present in Parquet footers.
    distinct_count: Optional[int] = None
    avg_size_bytes: Optional[float] = None
    encoding: Optional[str] = None
    compression: Optional[str] = None
    confidence: float = ConfidenceLevel.EXACT.value

    def __post_init__(self):
        if self.null_count is not None and self.null_count < 0:
            self.null_count = 0
        if self.distinct_count is not None and self.distinct_count <= 0:
            self.distinct_count = None

    def is_available(self) -> bool:
        return (
            self.min_value is not None
            or self.max_value is not None
            or self.null_count is not None
            or self.distinct_count is not None
        )

    def merge(self, other: "ColumnStatistics") -> "ColumnStatistics":
        """Combine statistics of the same column from disjoint row sets.

        Bounds and null counts known on only one side aren't valid for the
        combined rows, so they become unknown.
        """
        return ColumnStatistics(
            name=self.name,
            min_value=_combine_bounds(self.min_value, other.min_value, min),
            max_value=_combine_bounds(self.max_value, other.max_value, max),
            null_count=_sum_or_none(self.null_count, other.null_count),
            # Max of the parts is a lower bound of the combined NDV, which makes
            # equality selectivity (1 / NDV) an over-estimate: the safe direction.
            distinct_count=_safe_extreme(
                self.distinct_count, other.distinct_count, max
            ),
            avg_size_bytes=self.avg_size_bytes
            if self.avg_size_bytes is not None
            else other.avg_size_bytes,
            encoding=self.encoding if self.encoding == other.encoding else None,
            compression=self.compression
            if self.compression == other.compression
            else None,
            confidence=min(self.confidence, other.confidence),
        )

    def to_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ColumnStatistics":
        return cls(**data)


@dataclass
class OperatorStatistics:
    """Estimated statistics of an operator's output.

    All fields are optional: ``None`` means unknown, ``0`` means known to be zero.
    """

    num_rows: Optional[int] = None
    size_bytes: Optional[int] = None
    # Proven upper bound of the output size, or None if no bound is proven.
    #
    # A point estimate is *not* a bound: a UDF, an aggregation or a row
    # amplification can make the output wider than its input. Consumers that
    # need a bound (e.g. shuffle volume estimates) must read this field instead
    # of reusing ``size_bytes``.
    size_bytes_upper: Optional[int] = None
    num_blocks: Optional[int] = None
    avg_row_bytes: Optional[float] = None
    column_stats: Dict[str, ColumnStatistics] = field(default_factory=dict)
    # Fraction of input rows kept by the producing filter, in [0, 1].
    selectivity: Optional[float] = None
    # Output rows / input rows of the producing operator (may be > 1).
    amplification_ratio: Optional[float] = None
    skew_factor: Optional[float] = None
    confidence: float = ConfidenceLevel.EXACT.value

    def __post_init__(self):
        if self.num_rows is not None and self.num_rows < 0:
            logger.debug(f"Negative num_rows={self.num_rows}, clamping to 0")
            self.num_rows = 0
        if self.size_bytes is not None and self.size_bytes < 0:
            logger.debug(f"Negative size_bytes={self.size_bytes}, clamping to 0")
            self.size_bytes = 0
        if self.size_bytes_upper is not None and self.size_bytes_upper < 0:
            logger.debug(
                f"Negative size_bytes_upper={self.size_bytes_upper}, clamping to 0"
            )
            self.size_bytes_upper = 0
        if (
            self.size_bytes is not None
            and self.size_bytes_upper is not None
            and self.size_bytes_upper < self.size_bytes
        ):
            # A bound below the point estimate contradicts the estimate itself.
            self.size_bytes_upper = self.size_bytes
        if self.avg_row_bytes is not None and self.avg_row_bytes < 0:
            self.avg_row_bytes = None
        if self.avg_row_bytes is None and self.num_rows and self.size_bytes is not None:
            self.avg_row_bytes = self.size_bytes / self.num_rows
        if self.selectivity is not None:
            self.selectivity = _clamp01(self.selectivity)
        self.confidence = _clamp01(self.confidence)

    def is_available(self) -> bool:
        return (
            self.num_rows is not None
            or self.size_bytes is not None
            or bool(self.column_stats)
        )

    def has_size_upper_bound(self) -> bool:
        """Whether the output size has a proven upper bound."""
        return self.size_bytes_upper is not None

    def get_column_stat(self, column_name: str) -> Optional[ColumnStatistics]:
        return self.column_stats.get(column_name)

    def scale(
        self,
        ratio: float,
        confidence_decay: float = CONFIDENCE_DECAY_PER_OP,
    ) -> "OperatorStatistics":
        """Statistics after an operator that keeps ``ratio`` of the rows.

        ``ratio <= 1`` is recorded as ``selectivity``; ``ratio > 1`` as
        ``amplification_ratio``. Column min/max are kept (they remain valid
        bounds); null and distinct counts are scaled accordingly.
        """
        if ratio < 0:
            logger.debug(f"Negative scale ratio={ratio}, clamping to 0")
            ratio = 0.0
        num_rows = int(self.num_rows * ratio) if self.num_rows is not None else None
        column_stats = {
            name: dataclasses.replace(
                col,
                null_count=(
                    int(col.null_count * ratio) if col.null_count is not None else None
                ),
                distinct_count=(
                    min(col.distinct_count, num_rows)
                    if col.distinct_count is not None and num_rows is not None
                    else col.distinct_count
                ),
            )
            for name, col in self.column_stats.items()
        }
        return OperatorStatistics(
            num_rows=num_rows,
            size_bytes=(
                int(self.size_bytes * ratio) if self.size_bytes is not None else None
            ),
            # A selectivity is not a bound, so the input bound is carried over
            # unchanged: the output is a subset of the input rows.
            size_bytes_upper=self.size_bytes_upper,
            num_blocks=self.num_blocks,
            avg_row_bytes=self.avg_row_bytes,
            column_stats=column_stats,
            selectivity=ratio if ratio <= 1.0 else None,
            amplification_ratio=ratio if ratio > 1.0 else None,
            skew_factor=self.skew_factor,
            confidence=self.confidence * confidence_decay,
        )

    def _projection_ratio(self, columns: Iterable[str]) -> Optional[float]:
        """Share of the average row width kept by ``columns``.

        Columns without statistics are missing from the total, which
        over-estimates the share of the selected columns: the safe direction.
        Returns None if the share cannot be computed.
        """
        if not self.column_stats:
            return None
        sizes = {name: col.avg_size_bytes for name, col in self.column_stats.items()}
        if any(size is None for size in sizes.values()):
            return None
        selected = 0.0
        for name in columns:
            if name not in sizes:
                return None
            selected += sizes[name]
        total = sum(sizes.values())
        if total <= 0:
            return None
        return min(1.0, selected / total)

    def projected_size_bytes(self, columns: Iterable[str]) -> Optional[int]:
        """Estimated size after keeping only ``columns``."""
        if self.size_bytes is None:
            return None
        ratio = self._projection_ratio(columns)
        if ratio is None:
            return None
        return int(self.size_bytes * ratio)

    def projected_size_upper_bytes(self, columns: Iterable[str]) -> Optional[int]:
        """Upper bound of the size after keeping only ``columns``.

        Returns None when the input has no proven upper bound. The ratio is
        rounded up: truncating would understate a bound.
        """
        if self.size_bytes_upper is None:
            return None
        ratio = self._projection_ratio(columns)
        if ratio is None:
            return None
        return min(self.size_bytes_upper, math.ceil(self.size_bytes_upper * ratio))

    def with_confidence_at_most(self, confidence: float) -> "OperatorStatistics":
        return dataclasses.replace(self, confidence=min(self.confidence, confidence))

    def passthrough(
        self, confidence_decay: float = CONFIDENCE_DECAY_PER_OP, **changes
    ) -> "OperatorStatistics":
        """Statistics after a row-preserving operator."""
        return dataclasses.replace(
            self,
            selectivity=None,
            amplification_ratio=None,
            confidence=self.confidence * confidence_decay,
            **changes,
        )

    def merge(self, other: Optional["OperatorStatistics"]) -> "OperatorStatistics":
        """Statistics of the concatenation of two disjoint outputs (Union).

        An unknown count on either side makes the merged count unknown.
        """
        if other is None:
            return self
        # Column statistics known for only one side are not valid bounds of the
        # combined output, so only columns known on both sides are kept.
        column_stats = {
            name: col.merge(other.column_stats[name])
            for name, col in self.column_stats.items()
            if name in other.column_stats
        }
        return OperatorStatistics(
            num_rows=_sum_or_none(self.num_rows, other.num_rows),
            size_bytes=_sum_or_none(self.size_bytes, other.size_bytes),
            size_bytes_upper=_sum_or_none(
                self.size_bytes_upper, other.size_bytes_upper
            ),
            num_blocks=_sum_or_none(self.num_blocks, other.num_blocks),
            column_stats=column_stats,
            confidence=min(self.confidence, other.confidence),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "num_rows": self.num_rows,
            "size_bytes": self.size_bytes,
            "size_bytes_upper": self.size_bytes_upper,
            "num_blocks": self.num_blocks,
            "avg_row_bytes": self.avg_row_bytes,
            "selectivity": self.selectivity,
            "amplification_ratio": self.amplification_ratio,
            "skew_factor": self.skew_factor,
            "confidence": round(self.confidence, 2),
            "num_columns": len(self.column_stats),
        }

    def to_log_string(self) -> str:
        parts = []
        if self.num_rows is not None:
            parts.append(f"rows={self.num_rows}")
        if self.size_bytes is not None:
            parts.append(f"size={_format_bytes(self.size_bytes)}")
        if self.avg_row_bytes is not None:
            parts.append(f"avg_row={self.avg_row_bytes:.0f}B")
        if self.selectivity is not None:
            parts.append(f"selectivity={self.selectivity:.2%}")
        if self.amplification_ratio is not None:
            parts.append(f"amplification={self.amplification_ratio:.2f}x")
        if self.skew_factor is not None:
            parts.append(f"skew={self.skew_factor:.2f}")
        parts.append(f"confidence={self.confidence:.2f}")
        return "[" + ", ".join(parts) + "]"

    @classmethod
    def zero(cls) -> "OperatorStatistics":
        return cls(num_rows=0, size_bytes=0, confidence=ConfidenceLevel.EXACT.value)

    @classmethod
    def unknown(cls) -> "OperatorStatistics":
        return cls(confidence=ConfidenceLevel.UNKNOWN.value)


def estimate_selectivity_from_column_stats(
    column_name: str,
    operator: str,
    value: Any,
    column_stat: Optional[ColumnStatistics],
    num_rows: Optional[int] = None,
) -> float:
    """Estimate the selectivity of ``<column> <operator> <value>``.

    Args:
        column_name: Column name (used for logging only).
        operator: One of ``EQ``, ``NE``, ``GT``, ``GE``/``GTE``, ``LT``,
            ``LE``/``LTE``, ``IN``, ``NOT_IN``, ``IS_NULL``, ``IS_NOT_NULL``.
        value: Literal operand (a collection for ``IN``/``NOT_IN``).
        column_stat: Column statistics, if available.
        num_rows: Row count of the input, used to derive the null fraction.

    Returns:
        A selectivity in [0, 1]. Falls back to textbook defaults when
        statistics are missing. Assumes uniform distribution and treats ``GT``
        and ``GE`` (resp. ``LT``/``LE``) identically.
    """
    try:
        return _clamp01(_estimate_selectivity(operator, value, column_stat, num_rows))
    except Exception as e:
        logger.debug(f"Failed to estimate selectivity of {column_name} {operator}: {e}")
        return DEFAULT_UNKNOWN_SELECTIVITY


def _estimate_selectivity(
    operator: str,
    value: Any,
    col: Optional[ColumnStatistics],
    num_rows: Optional[int],
) -> float:
    operator = {"GTE": "GE", "LTE": "LE"}.get(operator, operator)

    null_fraction = None
    if col is not None and col.null_count is not None and num_rows:
        null_fraction = _clamp01(col.null_count / num_rows)

    if operator == "IS_NULL":
        return null_fraction if null_fraction is not None else DEFAULT_NULL_FRACTION
    if operator == "IS_NOT_NULL":
        return (
            1.0 - null_fraction
            if null_fraction is not None
            else 1.0 - DEFAULT_NULL_FRACTION
        )
    if operator == "NE":
        return 1.0 - _estimate_selectivity("EQ", value, col, num_rows)
    if operator == "NOT_IN":
        return 1.0 - _estimate_selectivity("IN", value, col, num_rows)

    # Nulls never satisfy comparisons.
    non_null = 1.0 - null_fraction if null_fraction is not None else 1.0

    if operator == "EQ":
        return non_null * _eq_selectivity(value, col)
    if operator == "IN":
        values = value if isinstance(value, (list, tuple, set, frozenset)) else [value]
        values = set(values)
        return non_null * min(1.0, sum(_eq_selectivity(v, col) for v in values))
    if operator in ("GT", "GE", "LT", "LE"):
        return non_null * _range_selectivity(operator, value, col)
    return DEFAULT_UNKNOWN_SELECTIVITY


def _eq_selectivity(value: Any, col: Optional[ColumnStatistics]) -> float:
    if col is None:
        return DEFAULT_EQ_SELECTIVITY
    lo, hi, v = _to_float(col.min_value), _to_float(col.max_value), _to_float(value)
    out_of_range = (
        lo is not None and hi is not None and v is not None and not lo <= v <= hi
    )
    # Bounds from a sample of the data aren't global bounds.
    if out_of_range and col.confidence >= ConfidenceLevel.HIGH.value:
        return 0.0
    if col.distinct_count:
        return 1.0 / col.distinct_count
    if (
        isinstance(col.min_value, int)
        and isinstance(col.max_value, int)
        and not isinstance(col.min_value, bool)
        and col.max_value >= col.min_value
    ):
        # Without NDV, assume a dense integer domain.
        return 1.0 / (col.max_value - col.min_value + 1)
    return DEFAULT_EQ_SELECTIVITY


def _range_selectivity(
    operator: str, value: Any, col: Optional[ColumnStatistics]
) -> float:
    if col is None:
        return DEFAULT_RANGE_SELECTIVITY
    lo, hi, v = _to_float(col.min_value), _to_float(col.max_value), _to_float(value)
    if lo is None or hi is None or v is None:
        return DEFAULT_RANGE_SELECTIVITY
    if hi == lo:
        satisfied = {
            "GT": lo > v,
            "GE": lo >= v,
            "LT": lo < v,
            "LE": lo <= v,
        }[operator]
        return 1.0 if satisfied else 0.0
    if operator in ("GT", "GE"):
        return (hi - v) / (hi - lo)
    return (v - lo) / (hi - lo)


def _to_float(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float, decimal.Decimal)):
        return float(value)
    if isinstance(value, datetime.datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=datetime.timezone.utc)
        return value.timestamp()
    if isinstance(value, datetime.date):
        return float(value.toordinal())
    return None


def _clamp01(x: float) -> float:
    return max(0.0, min(1.0, float(x)))


def _sum_or_none(a: Optional[int], b: Optional[int]) -> Optional[int]:
    if a is None or b is None:
        return None
    return a + b


def _combine_bounds(a: Any, b: Any, fn) -> Any:
    if a is None or b is None:
        return None
    try:
        return fn(a, b)
    except TypeError:
        return None


def _safe_extreme(a: Any, b: Any, fn) -> Any:
    if a is None:
        return b
    if b is None:
        return a
    try:
        return fn(a, b)
    except TypeError:
        return None


def _format_bytes(num_bytes: int) -> str:
    if num_bytes >= 1024**3:
        return f"{num_bytes / 1024**3:.2f}GB"
    return f"{num_bytes / 1024**2:.2f}MB"
