from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

import pyarrow.compute as pc

from ray.data.datatype import DataType
from ray.data.expressions import _create_pyarrow_compute_udf

if TYPE_CHECKING:
    from ray.data.expressions import Expr, PyArrowComputeUDFExpr

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

    def millisecond(self) -> "PyArrowComputeUDFExpr":
        """Extract millisecond component (the fractional second in [0, 999])."""
        return _create_pyarrow_compute_udf(pc.millisecond, DataType.int32())(self._expr)

    def microsecond(self) -> "PyArrowComputeUDFExpr":
        """Extract microsecond component (the fractional millisecond in [0, 999])."""
        return _create_pyarrow_compute_udf(pc.microsecond, DataType.int32())(self._expr)

    def nanosecond(self) -> "PyArrowComputeUDFExpr":
        """Extract nanosecond component (the fractional microsecond in [0, 999])."""
        return _create_pyarrow_compute_udf(pc.nanosecond, DataType.int32())(self._expr)

    # calendar

    def quarter(self) -> "PyArrowComputeUDFExpr":
        """Extract the quarter of the year (1-4)."""
        return _create_pyarrow_compute_udf(pc.quarter, DataType.int32())(self._expr)

    def day_of_week(self) -> "PyArrowComputeUDFExpr":
        """Extract the day of the week, where Monday is 0 and Sunday is 6."""
        return _create_pyarrow_compute_udf(pc.day_of_week, DataType.int32())(self._expr)

    def day_of_year(self) -> "PyArrowComputeUDFExpr":
        """Extract the ordinal day of the year (1-366)."""
        return _create_pyarrow_compute_udf(pc.day_of_year, DataType.int32())(self._expr)

    def iso_week(self) -> "PyArrowComputeUDFExpr":
        """Extract the ISO 8601 week of the year (1-53)."""
        return _create_pyarrow_compute_udf(pc.iso_week, DataType.int32())(self._expr)

    def iso_year(self) -> "PyArrowComputeUDFExpr":
        """Extract the ISO 8601 year.

        The ISO year can differ from the calendar year for days near the
        year boundary that the ISO 8601 calendar assigns to the adjacent year.
        """
        return _create_pyarrow_compute_udf(pc.iso_year, DataType.int32())(self._expr)

    def is_leap_year(self) -> "PyArrowComputeUDFExpr":
        """Return whether each timestamp falls in a leap year."""
        return _create_pyarrow_compute_udf(pc.is_leap_year, DataType.bool())(self._expr)

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
