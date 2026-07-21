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

    def _timestamp_dtype_with_tz(self, tz: str) -> DataType:
        """Return this expression's timestamp DataType re-tagged with ``tz``.

        Falls back to the expression's current data type when it is not a
        timestamp (the underlying compute call will then raise a clear error).
        """
        import pyarrow as pa

        dtype = self._expr.data_type
        if dtype.is_arrow_type():
            arrow_type = dtype.to_arrow_dtype()
            if pa.types.is_timestamp(arrow_type):
                return DataType.from_arrow(pa.timestamp(arrow_type.unit, tz=tz))
        return dtype

    def assume_timezone(
        self,
        timezone: str,
        *,
        ambiguous: Literal["raise", "earliest", "latest"] = "raise",
        nonexistent: Literal["raise", "earliest", "latest"] = "raise",
    ) -> "PyArrowComputeUDFExpr":
        """Localize timezone-naive timestamps to the given timezone.

        Args:
            timezone: Timezone to assume for the timestamps, e.g. ``"UTC"``
                or ``"America/New_York"``.
            ambiguous: How to handle timestamps that are ambiguous in the
                assumed timezone (DST fall-back), one of ``"raise"``,
                ``"earliest"``, or ``"latest"``.
            nonexistent: How to handle timestamps that don't exist in the
                assumed timezone (DST spring-forward), one of ``"raise"``,
                ``"earliest"``, or ``"latest"``.

        Returns:
            Expression producing timezone-aware timestamps.
        """
        return _create_pyarrow_compute_udf(
            pc.assume_timezone, self._timestamp_dtype_with_tz(timezone)
        )(self._expr, timezone=timezone, ambiguous=ambiguous, nonexistent=nonexistent)

    def tz_convert(self, target_tz: str) -> "UDFExpr":
        """Convert timezone-aware timestamps to another timezone.

        The stored instant is unchanged; only the timezone metadata used for
        display and component extraction changes.

        Args:
            target_tz: Target timezone, e.g. ``"Europe/London"``.

        Returns:
            Expression producing timestamps in the target timezone.
        """
        import pyarrow as pa

        @pyarrow_udf(return_dtype=self._timestamp_dtype_with_tz(target_tz))
        def _tz_convert(arr: "pyarrow.Array") -> "pyarrow.Array":
            if not pa.types.is_timestamp(arr.type):
                raise TypeError(
                    f"dt.tz_convert() requires a timestamp column, got {arr.type}."
                )
            if arr.type.tz is None:
                raise ValueError(
                    "dt.tz_convert() requires timezone-aware timestamps; use "
                    "dt.assume_timezone() to localize naive timestamps first."
                )
            return pc.cast(arr, pa.timestamp(arr.type.unit, tz=target_tz))

        return _tz_convert(self._expr)
