"""Struct namespace for expression operations on struct-typed columns."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow
import pyarrow.compute as pc

from ray.data.datatype import DataType
from ray.data.expressions import pyarrow_udf

if TYPE_CHECKING:
    from ray.data.expressions import Expr, UDFExpr


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

    def __getitem__(self, field_name: str) -> "UDFExpr":
        """Extract a field using bracket notation.

        Args:
            field_name: The name of the field to extract.

        Returns:
            UDFExpr that extracts the specified field from each struct.

        Example:
            >>> col("user").struct["age"]  # Get age field  # doctest: +SKIP
            >>> col("user").struct["address"].struct["city"]  # Get nested city field  # doctest: +SKIP
        """
        return self.field(field_name)

    def field(self, field_name: str) -> "UDFExpr":
        """Extract a field from a struct.

        Args:
            field_name: The name of the field to extract.

        Returns:
            UDFExpr that extracts the specified field from each struct.
        """
        # Infer return type from the struct field type
        return_dtype = DataType(object)  # fallback
        if self._expr.data_type.is_arrow_type():
            arrow_type = self._expr.data_type.to_arrow_dtype()
            if pyarrow.types.is_struct(arrow_type):
                try:
                    field_type = arrow_type.field(field_name).type
                    return_dtype = DataType.from_arrow(field_type)
                except KeyError:
                    # Field not found in schema, fallback to object
                    pass

        @pyarrow_udf(return_dtype=return_dtype)
        def _struct_field(arr: pyarrow.Array) -> pyarrow.Array:
            return pc.struct_field(arr, field_name)

        return _struct_field(self._expr)
