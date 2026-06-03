from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

import pyarrow.compute as pc

from ray.data.datatype import DataType
from ray.data.expressions import _create_pyarrow_compute_udf, pyarrow_udf

if TYPE_CHECKING:
    import pyarrow

    from ray.data.expressions import Expr, PyArrowComputeUDFExpr, UDFExpr

TemporalUnit = Literal[
    "year",
    "quarter",
    "month",
    "week",
    "day",
    "hour",
    "minute",
    "second",
    "millisecond",
    "microsecond",
    "nanosecond",
]


@dataclass
class _DatetimeNamespace:
    """Datetime namespace for operations on datetime-typed expression columns."""

    _expr: "Expr"

    # extractors

    def year(self) -> "PyArrowComputeUDFExpr":
        """Extract year component."""
        return _create_pyarrow_compute_udf(pc.year, DataType.int32())(self._expr)

    def month(self) -> "PyArrowComputeUDFExpr":
        """Extract month component."""
        return _create_pyarrow_compute_udf(pc.month, DataType.int32())(self._expr)

    def day(self) -> "PyArrowComputeUDFExpr":
        """Extract day component."""
        return _create_pyarrow_compute_udf(pc.day, DataType.int32())(self._expr)

    def hour(self) -> "PyArrowComputeUDFExpr":
        """Extract hour component."""
        return _create_pyarrow_compute_udf(pc.hour, DataType.int32())(self._expr)

    def minute(self) -> "PyArrowComputeUDFExpr":
        """Extract minute component."""
        return _create_pyarrow_compute_udf(pc.minute, DataType.int32())(self._expr)

    def second(self) -> "PyArrowComputeUDFExpr":
        """Extract second component."""
        return _create_pyarrow_compute_udf(pc.second, DataType.int32())(self._expr)

    # formatting

    def strftime(self, fmt: str) -> "PyArrowComputeUDFExpr":
        """Format timestamps with a strftime pattern."""
        return _create_pyarrow_compute_udf(pc.strftime, DataType.string())(
            self._expr, format=fmt
        )

    # rounding

    def ceil(self, unit: TemporalUnit) -> "PyArrowComputeUDFExpr":
        """Ceil timestamps to the next multiple of the given unit."""
        return _create_pyarrow_compute_udf(pc.ceil_temporal, self._expr.data_type)(
            self._expr, multiple=1, unit=unit
        )

    def floor(self, unit: TemporalUnit) -> "PyArrowComputeUDFExpr":
        """Floor timestamps to the previous multiple of the given unit."""
        return _create_pyarrow_compute_udf(pc.floor_temporal, self._expr.data_type)(
            self._expr, multiple=1, unit=unit
        )

    def round(self, unit: TemporalUnit) -> "PyArrowComputeUDFExpr":
        """Round timestamps to the nearest multiple of the given unit."""
        return _create_pyarrow_compute_udf(pc.round_temporal, self._expr.data_type)(
            self._expr, multiple=1, unit=unit
        )

    # timezone operations

    def assume_timezone(
        self, timezone: str, *, ambiguous: str = "raise", nonexistent: str = "raise"
    ) -> "PyArrowComputeUDFExpr":
        """Localize naive (timezone-unaware) timestamps to a given timezone.

        Args:
            timezone: The timezone to assign (e.g., "America/New_York", "UTC").
            ambiguous: How to handle ambiguous times ("raise", "earliest", "latest").
            nonexistent: How to handle nonexistent times ("raise", "earliest", "latest").

        Returns:
            Expression with timezone-aware timestamps.
        """
        import pyarrow as pa
        import pyarrow.types

        return_dtype = self._expr.data_type
        try:
            arrow_type = return_dtype.to_arrow_dtype()
            if pyarrow.types.is_timestamp(arrow_type):
                return_dtype = DataType.from_arrow(
                    pa.timestamp(arrow_type.unit, tz=timezone)
                )
        except Exception:
            pass

        return _create_pyarrow_compute_udf(pc.assume_timezone, return_dtype)(
            self._expr, timezone=timezone, ambiguous=ambiguous, nonexistent=nonexistent
        )

    def tz_convert(self, target_tz: str) -> "UDFExpr":
        """Convert timezone-aware timestamps to a different timezone.

        This uses ``pyarrow_udf`` rather than ``_create_pyarrow_compute_udf``
        because timezone conversion requires ``pc.cast`` with a dynamically
        constructed target type, which is not a direct 1:1 ``pc.*`` call and
        cannot participate in predicate pushdown.

        Args:
            target_tz: The target timezone (e.g., "Europe/London", "US/Pacific").

        Returns:
            Expression with timestamps converted to the target timezone.
        """
        import pyarrow as pa
        import pyarrow.types

        return_dtype = self._expr.data_type
        try:
            arrow_type = return_dtype.to_arrow_dtype()
            if pyarrow.types.is_timestamp(arrow_type):
                return_dtype = DataType.from_arrow(
                    pa.timestamp(arrow_type.unit, tz=target_tz)
                )
        except Exception:
            pass

        @pyarrow_udf(return_dtype=return_dtype)
        def _tz_convert(
            arr: "pyarrow.Array",
        ) -> "pyarrow.Array":
            target_type = pa.timestamp(arr.type.unit, tz=target_tz)
            return pc.cast(arr, target_type)

        return _tz_convert(self._expr)
