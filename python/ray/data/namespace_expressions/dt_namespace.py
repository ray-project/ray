from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Literal

import pyarrow
import pyarrow.compute as pc

from ray.data.datatype import DataType
from ray.data.expressions import pyarrow_udf

if TYPE_CHECKING:
    from ray.data.expressions import Expr, UDFExpr

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

    def _unary_temporal_int(
        self, func: Callable[[pyarrow.Array], pyarrow.Array]
    ) -> "UDFExpr":
        """Helper for year/month/… that return int32."""

        @pyarrow_udf(return_dtype=DataType.int32())
        def _udf(arr: pyarrow.Array) -> pyarrow.Array:
            return func(arr)

        return _udf(self._expr)

    # extractors

    def year(self) -> "UDFExpr":
        """Extract year component."""
        return self._unary_temporal_int(pc.year)

    def month(self) -> "UDFExpr":
        """Extract month component."""
        return self._unary_temporal_int(pc.month)

    def day(self) -> "UDFExpr":
        """Extract day component."""
        return self._unary_temporal_int(pc.day)

    def hour(self) -> "UDFExpr":
        """Extract hour component."""
        return self._unary_temporal_int(pc.hour)

    def minute(self) -> "UDFExpr":
        """Extract minute component."""
        return self._unary_temporal_int(pc.minute)

    def second(self) -> "UDFExpr":
        """Extract second component."""
        return self._unary_temporal_int(pc.second)

    # formatting

    def strftime(self, fmt: str) -> "UDFExpr":
        """Format timestamps with a strftime pattern."""

        @pyarrow_udf(return_dtype=DataType.string())
        def _format(arr: pyarrow.Array) -> pyarrow.Array:
            return pc.strftime(arr, format=fmt)

        return _format(self._expr)

    # rounding

    def ceil(self, unit: TemporalUnit) -> "UDFExpr":
        """Ceil timestamps to the next multiple of the given unit."""
        return_dtype = self._expr.data_type

        @pyarrow_udf(return_dtype=return_dtype)
        def _ceil(arr: pyarrow.Array) -> pyarrow.Array:
            return pc.ceil_temporal(arr, multiple=1, unit=unit)

        return _ceil(self._expr)

    def floor(self, unit: TemporalUnit) -> "UDFExpr":
        """Floor timestamps to the previous multiple of the given unit."""
        return_dtype = self._expr.data_type

        @pyarrow_udf(return_dtype=return_dtype)
        def _floor(arr: pyarrow.Array) -> pyarrow.Array:
            return pc.floor_temporal(arr, multiple=1, unit=unit)

        return _floor(self._expr)

    def round(self, unit: TemporalUnit) -> "UDFExpr":
        """Round timestamps to the nearest multiple of the given unit."""
        return_dtype = self._expr.data_type

        @pyarrow_udf(return_dtype=return_dtype)
        def _round(arr: pyarrow.Array) -> pyarrow.Array:

            return pc.round_temporal(arr, multiple=1, unit=unit)

        return _round(self._expr)
