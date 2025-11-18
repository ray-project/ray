"""
Datetime namespace for expression operations on datetime-typed columns.

This module defines the ``_DatetimeNamespace`` class which exposes a set of
convenience methods for working with timestamp and date columns in Ray Data
expressions.  The API mirrors pandas' ``Series.dt`` accessor and is backed by
PyArrow compute functions for efficient execution.

Example
-------

>>> from ray.data.expressions import col
>>> # Extract year, month and day from a timestamp column
>>> expr_year = col("timestamp").dt.year()
>>> expr_month = col("timestamp").dt.month()
>>> expr_day = col("timestamp").dt.day()
>>> # Format the timestamp as a string
>>> expr_fmt = col("timestamp").dt.strftime("%Y-%m-%d")
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pyarrow
import pyarrow.compute as pc

from ray.data.datatype import DataType
from ray.data.expressions import pyarrow_udf

if TYPE_CHECKING:
    from ray.data.expressions import Expr, UDFExpr

@dataclass
class _DatetimeNamespace:
    """Namespace for datetime operations on expression columns."""
    _expr: "Expr"

    def year(self) -> "UDFExpr":
        @pyarrow_udf(return_dtype=DataType.int32())
        def _year(arr: pyarrow.Array) -> pyarrow.Array:
            return pc.year(arr)
        return _year(self._expr)

    def month(self) -> "UDFExpr":
        @pyarrow_udf(return_dtype=DataType.int32())
        def _month(arr: pyarrow.Array) -> pyarrow.Array:
            return pc.month(arr)
        return _month(self._expr)

    def day(self) -> "UDFExpr":
        @pyarrow_udf(return_dtype=DataType.int32())
        def _day(arr: pyarrow.Array) -> pyarrow.Array:
            return pc.day(arr)
        return _day(self._expr)

    def hour(self) -> "UDFExpr":
        @pyarrow_udf(return_dtype=DataType.int32())
        def _hour(arr: pyarrow.Array) -> pyarrow.Array:
            return pc.hour(arr)
        return _hour(self._expr)

    def minute(self) -> "UDFExpr":
        @pyarrow_udf(return_dtype=DataType.int32())
        def _minute(arr: pyarrow.Array) -> pyarrow.Array:
            return pc.minute(arr)
        return _minute(self._expr)

    def second(self) -> "UDFExpr":
        @pyarrow_udf(return_dtype=DataType.int32())
        def _second(arr: pyarrow.Array) -> pyarrow.Array:
            return pc.second(arr)
        return _second(self._expr)

    def strftime(self, fmt: str) -> "UDFExpr":
        """Format each timestamp using a strftime format string."""
        @pyarrow_udf(return_dtype=DataType.string())
        def _format(arr: pyarrow.Array) -> pyarrow.Array:
            return pc.strftime(arr, format=fmt)
        return _format(self._expr)

    def ceil(self, unit: str) -> "UDFExpr":
        """Ceil timestamps up to the nearest boundary of the given unit."""
        return_dtype = self._expr.data_type
        @pyarrow_udf(return_dtype=return_dtype)
        def _ceil(arr: pyarrow.Array) -> pyarrow.Array:
            return pc.ceil_temporal(arr, multiple=1, unit=unit)
        return _ceil(self._expr)

    def floor(self, unit: str) -> "UDFExpr":
        """Floor timestamps down to the previous boundary of the given unit."""
        return_dtype = self._expr.data_type
        @pyarrow_udf(return_dtype=return_dtype)
        def _floor(arr: pyarrow.Array) -> pyarrow.Array:
            return pc.floor_temporal(arr, multiple=1, unit=unit)
        return _floor(self._expr)

    def round(self, unit: str, tie_breaker: str = "half_to_even") -> "UDFExpr":
        """Round timestamps to the nearest boundary of the given unit."""
        return_dtype = self._expr.data_type
        @pyarrow_udf(return_dtype=return_dtype)
        def _round(arr: pyarrow.Array) -> pyarrow.Array:
            return pc.round_temporal(arr, multiple=1, unit=unit,
                                      tie_breaker=tie_breaker)
        return _round(self._expr)
