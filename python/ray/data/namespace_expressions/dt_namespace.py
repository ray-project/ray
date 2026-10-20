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
