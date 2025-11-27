"""String namespace for expression operations on string-typed columns."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Literal

import pyarrow
import pyarrow.compute as pc

from ray.data.datatype import DataType
from ray.data.expressions import pyarrow_udf

if TYPE_CHECKING:
    from ray.data.expressions import Expr, UDFExpr


def _create_str_udf(
    pc_func: Callable[..., pyarrow.Array], return_dtype: DataType
) -> Callable[..., "UDFExpr"]:
    """Helper to create a string UDF that wraps a PyArrow compute function.

    This helper handles all types of PyArrow compute operations:
    - Unary operations (no args): upper(), lower(), reverse()
    - Pattern operations (pattern + args): starts_with(), contains()
    - Multi-argument operations: replace(), replace_slice()

    Args:
        pc_func: PyArrow compute function that takes (array, *positional, **kwargs)
        return_dtype: The return data type

    Returns:
        A callable that creates UDFExpr instances
    """

    def wrapper(expr: Expr, *positional: Any, **kwargs: Any) -> "UDFExpr":
        @pyarrow_udf(return_dtype=return_dtype)
        def udf(arr: pyarrow.Array) -> pyarrow.Array:
            return pc_func(arr, *positional, **kwargs)

        return udf(expr)

    return wrapper


@dataclass
class _StringNamespace:
    """Namespace for string operations on expression columns.

    This namespace provides methods for operating on string-typed columns using
    PyArrow compute functions.

    Example:
        >>> from ray.data.expressions import col
        >>> # Convert to uppercase
        >>> expr = col("name").str.upper()
        >>> # Get string length
        >>> expr = col("name").str.len()
        >>> # Check if string starts with a prefix
        >>> expr = col("name").str.starts_with("A")
    """

    _expr: Expr

    # Length methods
    def len(self) -> "UDFExpr":
        """Get the length of each string in characters."""
        return _create_str_udf(pc.utf8_length, DataType.int32())(self._expr)

    def byte_len(self) -> "UDFExpr":
        """Get the length of each string in bytes."""
        return _create_str_udf(pc.binary_length, DataType.int32())(self._expr)

    # Case methods
    def upper(self) -> "UDFExpr":
        """Convert strings to uppercase."""
        return _create_str_udf(pc.utf8_upper, DataType.string())(self._expr)

    def lower(self) -> "UDFExpr":
        """Convert strings to lowercase."""
        return _create_str_udf(pc.utf8_lower, DataType.string())(self._expr)

    def capitalize(self) -> "UDFExpr":
        """Capitalize the first character of each string."""
        return _create_str_udf(pc.utf8_capitalize, DataType.string())(self._expr)

    def title(self) -> "UDFExpr":
        """Convert strings to title case."""
        return _create_str_udf(pc.utf8_title, DataType.string())(self._expr)

    def swapcase(self) -> "UDFExpr":
        """Swap the case of each character."""
        return _create_str_udf(pc.utf8_swapcase, DataType.string())(self._expr)

    # Predicate methods
    def is_alpha(self) -> "UDFExpr":
        """Check if strings contain only alphabetic characters."""
        return _create_str_udf(pc.utf8_is_alpha, DataType.bool())(self._expr)

    def is_alnum(self) -> "UDFExpr":
        """Check if strings contain only alphanumeric characters."""
        return _create_str_udf(pc.utf8_is_alnum, DataType.bool())(self._expr)

    def is_digit(self) -> "UDFExpr":
        """Check if strings contain only digits."""
        return _create_str_udf(pc.utf8_is_digit, DataType.bool())(self._expr)

    def is_decimal(self) -> "UDFExpr":
        """Check if strings contain only decimal characters."""
        return _create_str_udf(pc.utf8_is_decimal, DataType.bool())(self._expr)

    def is_numeric(self) -> "UDFExpr":
        """Check if strings contain only numeric characters."""
        return _create_str_udf(pc.utf8_is_numeric, DataType.bool())(self._expr)

    def is_space(self) -> "UDFExpr":
        """Check if strings contain only whitespace."""
        return _create_str_udf(pc.utf8_is_space, DataType.bool())(self._expr)

    def is_lower(self) -> "UDFExpr":
        """Check if strings are lowercase."""
        return _create_str_udf(pc.utf8_is_lower, DataType.bool())(self._expr)

    def is_upper(self) -> "UDFExpr":
        """Check if strings are uppercase."""
        return _create_str_udf(pc.utf8_is_upper, DataType.bool())(self._expr)

    def is_title(self) -> "UDFExpr":
        """Check if strings are title-cased."""
        return _create_str_udf(pc.utf8_is_title, DataType.bool())(self._expr)

    def is_printable(self) -> "UDFExpr":
        """Check if strings contain only printable characters."""
        return _create_str_udf(pc.utf8_is_printable, DataType.bool())(self._expr)

    def is_ascii(self) -> "UDFExpr":
        """Check if strings contain only ASCII characters."""
        return _create_str_udf(pc.string_is_ascii, DataType.bool())(self._expr)

    # Searching methods
    def starts_with(self, pattern: str, *args: Any, **kwargs: Any) -> "UDFExpr":
        """Check if strings start with a pattern."""
        return _create_str_udf(pc.starts_with, DataType.bool())(
            self._expr, pattern, *args, **kwargs
        )

    def ends_with(self, pattern: str, *args: Any, **kwargs: Any) -> "UDFExpr":
        """Check if strings end with a pattern."""
        return _create_str_udf(pc.ends_with, DataType.bool())(
            self._expr, pattern, *args, **kwargs
        )

    def contains(self, pattern: str, *args: Any, **kwargs: Any) -> "UDFExpr":
        """Check if strings contain a substring."""
        return _create_str_udf(pc.match_substring, DataType.bool())(
            self._expr, pattern, *args, **kwargs
        )

    def match(self, pattern: str, *args: Any, **kwargs: Any) -> "UDFExpr":
        """Match strings against a SQL LIKE pattern."""
        return _create_str_udf(pc.match_like, DataType.bool())(
            self._expr, pattern, *args, **kwargs
        )

    def find(self, pattern: str, *args: Any, **kwargs: Any) -> "UDFExpr":
        """Find the first occurrence of a substring."""
        return _create_str_udf(pc.find_substring, DataType.int32())(
            self._expr, pattern, *args, **kwargs
        )

    def count(self, pattern: str, *args: Any, **kwargs: Any) -> "UDFExpr":
        """Count occurrences of a substring."""
        return _create_str_udf(pc.count_substring, DataType.int32())(
            self._expr, pattern, *args, **kwargs
        )

    def find_regex(self, pattern: str, *args: Any, **kwargs: Any) -> "UDFExpr":
        """Find the first occurrence matching a regex pattern."""
        return _create_str_udf(pc.find_substring_regex, DataType.int32())(
            self._expr, pattern, *args, **kwargs
        )

    def count_regex(self, pattern: str, *args: Any, **kwargs: Any) -> "UDFExpr":
        """Count occurrences matching a regex pattern."""
        return _create_str_udf(pc.count_substring_regex, DataType.int32())(
            self._expr, pattern, *args, **kwargs
        )

    def match_regex(self, pattern: str, *args: Any, **kwargs: Any) -> "UDFExpr":
        """Check if strings match a regex pattern."""
        return _create_str_udf(pc.match_substring_regex, DataType.bool())(
            self._expr, pattern, *args, **kwargs
        )

    # Transformation methods
    def reverse(self) -> "UDFExpr":
        """Reverse each string."""
        return _create_str_udf(pc.utf8_reverse, DataType.string())(self._expr)

    def slice(self, *args: Any, **kwargs: Any) -> "UDFExpr":
        """Slice strings by codeunit indices."""
        return _create_str_udf(pc.utf8_slice_codeunits, DataType.string())(
            self._expr, *args, **kwargs
        )

    def replace(
        self, pattern: str, replacement: str, *args: Any, **kwargs: Any
    ) -> "UDFExpr":
        """Replace occurrences of a substring."""
        return _create_str_udf(pc.replace_substring, DataType.string())(
            self._expr, pattern, replacement, *args, **kwargs
        )

    def replace_regex(
        self, pattern: str, replacement: str, *args: Any, **kwargs: Any
    ) -> "UDFExpr":
        """Replace occurrences matching a regex pattern."""
        return _create_str_udf(pc.replace_substring_regex, DataType.string())(
            self._expr, pattern, replacement, *args, **kwargs
        )

    def replace_slice(
        self, start: int, stop: int, replacement: str, *args: Any, **kwargs: Any
    ) -> "UDFExpr":
        """Replace a slice with a string."""
        return _create_str_udf(pc.binary_replace_slice, DataType.string())(
            self._expr, start, stop, replacement, *args, **kwargs
        )

    def split(self, pattern: str, *args: Any, **kwargs: Any) -> "UDFExpr":
        """Split strings by a pattern."""
        return _create_str_udf(pc.split_pattern, DataType(object))(
            self._expr, pattern, *args, **kwargs
        )

    def split_regex(self, pattern: str, *args: Any, **kwargs: Any) -> "UDFExpr":
        """Split strings by a regex pattern."""
        return _create_str_udf(pc.split_pattern_regex, DataType(object))(
            self._expr, pattern, *args, **kwargs
        )

    def split_whitespace(self, *args: Any, **kwargs: Any) -> "UDFExpr":
        """Split strings on whitespace."""
        return _create_str_udf(pc.utf8_split_whitespace, DataType(object))(
            self._expr, *args, **kwargs
        )

    def extract(self, pattern: str, *args: Any, **kwargs: Any) -> "UDFExpr":
        """Extract a substring matching a regex pattern."""
        return _create_str_udf(pc.extract_regex, DataType.string())(
            self._expr, pattern, *args, **kwargs
        )

    def repeat(self, n: int, *args: Any, **kwargs: Any) -> "UDFExpr":
        """Repeat each string n times."""
        return _create_str_udf(pc.binary_repeat, DataType.string())(
            self._expr, n, *args, **kwargs
        )

    def center(
        self, width: int, padding: str = " ", *args: Any, **kwargs: Any
    ) -> "UDFExpr":
        """Center strings in a field of given width."""
        return _create_str_udf(pc.utf8_center, DataType.string())(
            self._expr, width, padding, *args, **kwargs
        )

    # Custom methods that need special logic beyond simple PyArrow function calls
    def strip(self, characters: str | None = None) -> "UDFExpr":
        """Remove leading and trailing whitespace or specified characters.

        Args:
            characters: Characters to remove. If None, removes whitespace.

        Returns:
            UDFExpr that strips characters from both ends.
        """

        @pyarrow_udf(return_dtype=DataType.string())
        def _str_strip(arr: pyarrow.Array) -> pyarrow.Array:
            if characters is None:
                return pc.utf8_trim_whitespace(arr)
            else:
                return pc.utf8_trim(arr, characters=characters)

        return _str_strip(self._expr)

    def lstrip(self, characters: str | None = None) -> "UDFExpr":
        """Remove leading whitespace or specified characters.

        Args:
            characters: Characters to remove. If None, removes whitespace.

        Returns:
            UDFExpr that strips characters from the left.
        """

        @pyarrow_udf(return_dtype=DataType.string())
        def _str_lstrip(arr: pyarrow.Array) -> pyarrow.Array:
            if characters is None:
                return pc.utf8_ltrim_whitespace(arr)
            else:
                return pc.utf8_ltrim(arr, characters=characters)

        return _str_lstrip(self._expr)

    def rstrip(self, characters: str | None = None) -> "UDFExpr":
        """Remove trailing whitespace or specified characters.

        Args:
            characters: Characters to remove. If None, removes whitespace.

        Returns:
            UDFExpr that strips characters from the right.
        """

        @pyarrow_udf(return_dtype=DataType.string())
        def _str_rstrip(arr: pyarrow.Array) -> pyarrow.Array:
            if characters is None:
                return pc.utf8_rtrim_whitespace(arr)
            else:
                return pc.utf8_rtrim(arr, characters=characters)

        return _str_rstrip(self._expr)

    # Padding
    def pad(
        self,
        width: int,
        fillchar: str = " ",
        side: Literal["left", "right", "both"] = "right",
    ) -> "UDFExpr":
        """Pad strings to a specified width.

        Args:
            width: Target width.
            fillchar: Character to use for padding.
            side: "left", "right", or "both" for padding side.

        Returns:
            UDFExpr that pads strings.
        """

        @pyarrow_udf(return_dtype=DataType.string())
        def _str_pad(arr: pyarrow.Array) -> pyarrow.Array:
            if side == "right":
                return pc.utf8_rpad(arr, width=width, padding=fillchar)
            elif side == "left":
                return pc.utf8_lpad(arr, width=width, padding=fillchar)
            elif side == "both":
                return pc.utf8_center(arr, width=width, padding=fillchar)
            else:
                raise ValueError("side must be 'left', 'right', or 'both'")

        return _str_pad(self._expr)
