"""Struct namespace for expression operations on struct-typed columns."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Union

import pyarrow
import pyarrow.compute as pc

from ray.data.datatype import DataType
from ray.data.expressions import _create_pyarrow_compute_udf

if TYPE_CHECKING:
    from ray.data.expressions import Expr, PyArrowComputeUDFExpr


@dataclass
class _StructNamespace:
    """Namespace for struct operations on expression columns.

    This namespace provides methods for operating on struct-typed columns using
    PyArrow compute functions.

    Example:
        >>> from ray.data.expressions import col
        >>> # Access a field using method
        >>> expr = col("user_record").struct.field("age")
        >>> # Access a field using bracket notation
        >>> expr = col("user_record").struct["age"]
        >>> # Access nested field
        >>> expr = col("user_record").struct["address"].struct["city"]
    """

    _expr: Expr

    def __getitem__(self, key: Union[str, int]) -> "PyArrowComputeUDFExpr":
        """Extract a field using bracket notation.

        Args:
            key: The field name or index to extract.

        Returns:
            PyArrowComputeUDFExpr that extracts the specified field from each struct.

        Example:
            >>> col("user").struct["age"]  # Get age field by name  # doctest: +SKIP
            >>> col("user").struct[1]  # Get second field by index  # doctest: +SKIP
            >>> col("user").struct["address"].struct["city"]  # Get nested city field  # doctest: +SKIP
        """
        if isinstance(key, str):
            return self.field(key)
        if isinstance(key, int) and not isinstance(key, bool):
            return self.field_by_index(key)
        raise TypeError(
            f"Struct indices must be strings or integers, not {type(key).__name__}"
        )

    def field(self, field_name: str) -> "PyArrowComputeUDFExpr":
        """Extract a field from a struct.

        Args:
            field_name: The name of the field to extract.

        Returns:
            UDFExpr that extracts the specified field from each struct.
        """
        return_dtype = DataType(object)
        if self._expr.data_type.is_arrow_type():
            arrow_type = self._expr.data_type.to_arrow_dtype()
            if pyarrow.types.is_struct(arrow_type):
                try:
                    field_type = arrow_type.field(field_name).type
                    return_dtype = DataType.from_arrow(field_type)
                except KeyError:
                    pass

        return _create_pyarrow_compute_udf(pc.struct_field, return_dtype)(
            self._expr, field_name
        )

    def field_by_index(self, index: int) -> "PyArrowComputeUDFExpr":
        """Extract a field from a struct by index.

        Args:
            index: The index of the field to extract.

        Returns:
            UDFExpr that extracts the specified field from each struct.
        """
        if not isinstance(index, int) or isinstance(index, bool):
            raise TypeError(
                f"Struct field index must be an integer, not {type(index).__name__}"
            )
        if index < 0:
            raise ValueError(f"Struct field index must be non-negative, got {index}")
        return_dtype = DataType(object)
        if self._expr.data_type.is_arrow_type():
            arrow_type = self._expr.data_type.to_arrow_dtype()
            if pyarrow.types.is_struct(arrow_type):
                try:
                    field_type = arrow_type[index].type
                    return_dtype = DataType.from_arrow(field_type)
                except IndexError:
                    pass

        return _create_pyarrow_compute_udf(pc.struct_field, return_dtype)(
            self._expr, index
        )
