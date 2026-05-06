"""String namespace for expression operations on string-typed columns."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, List, Literal, Tuple, Union

import pyarrow
import pyarrow.compute as pc

from ray.data.datatype import DataType
from ray.data.expressions import _create_pyarrow_compute_udf, pyarrow_udf

if TYPE_CHECKING:
    from ray.data.expressions import Expr, PyArrowComputeUDFExpr, UDFExpr


def _create_str_udf(
    pc_func: Callable[..., pyarrow.Array], return_dtype: DataType
) -> Callable[..., "PyArrowComputeUDFExpr"]:
    """Helper to create a string UDF that wraps a PyArrow compute function.

    This helper handles all types of PyArrow compute operations:
    - Unary operations (no args): upper(), lower(), reverse()
    - Pattern operations (pattern + args): starts_with(), contains()
    - Multi-argument operations: replace(), replace_slice()

    Args:
        pc_func: PyArrow compute function that takes (array, *positional, **kwargs)
        return_dtype: The return data type

    Returns:
        A callable that creates PyArrowComputeUDFExpr instances
    """

    return _create_pyarrow_compute_udf(pc_func, return_dtype=return_dtype)


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
    def len(self) -> "PyArrowComputeUDFExpr":
        """Get the length of each string in characters."""
        return _create_str_udf(pc.utf8_length, DataType.int32())(self._expr)

    def byte_len(self) -> "PyArrowComputeUDFExpr":
        """Get the length of each string in bytes."""
        return _create_str_udf(pc.binary_length, DataType.int32())(self._expr)

    # Case methods
    def upper(self) -> "PyArrowComputeUDFExpr":
        """Convert strings to uppercase."""
        return _create_str_udf(pc.utf8_upper, DataType.string())(self._expr)

    def lower(self) -> "PyArrowComputeUDFExpr":
        """Convert strings to lowercase."""
        return _create_str_udf(pc.utf8_lower, DataType.string())(self._expr)

    def capitalize(self) -> "PyArrowComputeUDFExpr":
        """Capitalize the first character of each string."""
        return _create_str_udf(pc.utf8_capitalize, DataType.string())(self._expr)

    def title(self) -> "PyArrowComputeUDFExpr":
        """Convert strings to title case."""
        return _create_str_udf(pc.utf8_title, DataType.string())(self._expr)

    def swapcase(self) -> "PyArrowComputeUDFExpr":
        """Swap the case of each character."""
        return _create_str_udf(pc.utf8_swapcase, DataType.string())(self._expr)

    # Predicate methods
    def is_alpha(self) -> "PyArrowComputeUDFExpr":
        """Check if strings contain only alphabetic characters."""
        return _create_str_udf(pc.utf8_is_alpha, DataType.bool())(self._expr)

    def is_alnum(self) -> "PyArrowComputeUDFExpr":
        """Check if strings contain only alphanumeric characters."""
        return _create_str_udf(pc.utf8_is_alnum, DataType.bool())(self._expr)

    def is_digit(self) -> "PyArrowComputeUDFExpr":
        """Check if strings contain only digits."""
        return _create_str_udf(pc.utf8_is_digit, DataType.bool())(self._expr)

    def is_decimal(self) -> "PyArrowComputeUDFExpr":
        """Check if strings contain only decimal characters."""
        return _create_str_udf(pc.utf8_is_decimal, DataType.bool())(self._expr)

    def is_numeric(self) -> "PyArrowComputeUDFExpr":
        """Check if strings contain only numeric characters."""
        return _create_str_udf(pc.utf8_is_numeric, DataType.bool())(self._expr)

    def is_space(self) -> "PyArrowComputeUDFExpr":
        """Check if strings contain only whitespace."""
        return _create_str_udf(pc.utf8_is_space, DataType.bool())(self._expr)

    def is_lower(self) -> "PyArrowComputeUDFExpr":
        """Check if strings are lowercase."""
        return _create_str_udf(pc.utf8_is_lower, DataType.bool())(self._expr)

    def is_upper(self) -> "PyArrowComputeUDFExpr":
        """Check if strings are uppercase."""
        return _create_str_udf(pc.utf8_is_upper, DataType.bool())(self._expr)

    def is_title(self) -> "PyArrowComputeUDFExpr":
        """Check if strings are title-cased."""
        return _create_str_udf(pc.utf8_is_title, DataType.bool())(self._expr)

    def is_printable(self) -> "PyArrowComputeUDFExpr":
        """Check if strings contain only printable characters."""
        return _create_str_udf(pc.utf8_is_printable, DataType.bool())(self._expr)

    def is_ascii(self) -> "PyArrowComputeUDFExpr":
        """Check if strings contain only ASCII characters."""
        return _create_str_udf(pc.string_is_ascii, DataType.bool())(self._expr)

    # Searching methods
    def starts_with(
        self, pattern: str, *args: Any, **kwargs: Any
    ) -> "PyArrowComputeUDFExpr":
        """Check if strings start with a pattern."""
        return _create_str_udf(pc.starts_with, DataType.bool())(
            self._expr, pattern, *args, **kwargs
        )

    def ends_with(
        self, pattern: str, *args: Any, **kwargs: Any
    ) -> "PyArrowComputeUDFExpr":
        """Check if strings end with a pattern."""
        return _create_str_udf(pc.ends_with, DataType.bool())(
            self._expr, pattern, *args, **kwargs
        )

    def contains(
        self, pattern: str, *args: Any, **kwargs: Any
    ) -> "PyArrowComputeUDFExpr":
        """Check if strings contain a substring."""
        return _create_str_udf(pc.match_substring, DataType.bool())(
            self._expr, pattern, *args, **kwargs
        )

    def match(self, pattern: str, *args: Any, **kwargs: Any) -> "PyArrowComputeUDFExpr":
        """Match strings against a SQL LIKE pattern."""
        return _create_str_udf(pc.match_like, DataType.bool())(
            self._expr, pattern, *args, **kwargs
        )

    def find(self, pattern: str, *args: Any, **kwargs: Any) -> "PyArrowComputeUDFExpr":
        """Find the first occurrence of a substring."""
        return _create_str_udf(pc.find_substring, DataType.int32())(
            self._expr, pattern, *args, **kwargs
        )

    def count(self, pattern: str, *args: Any, **kwargs: Any) -> "PyArrowComputeUDFExpr":
        """Count occurrences of a substring."""
        return _create_str_udf(pc.count_substring, DataType.int32())(
            self._expr, pattern, *args, **kwargs
        )

    def find_regex(
        self, pattern: str, *args: Any, **kwargs: Any
    ) -> "PyArrowComputeUDFExpr":
        """Find the first occurrence matching a regex pattern."""
        return _create_str_udf(pc.find_substring_regex, DataType.int32())(
            self._expr, pattern, *args, **kwargs
        )

    def count_regex(
        self, pattern: str, *args: Any, **kwargs: Any
    ) -> "PyArrowComputeUDFExpr":
        """Count occurrences matching a regex pattern."""
        return _create_str_udf(pc.count_substring_regex, DataType.int32())(
            self._expr, pattern, *args, **kwargs
        )

    def match_regex(
        self, pattern: str, *args: Any, **kwargs: Any
    ) -> "PyArrowComputeUDFExpr":
        """Check if strings match a regex pattern."""
        return _create_str_udf(pc.match_substring_regex, DataType.bool())(
            self._expr, pattern, *args, **kwargs
        )

    # Transformation methods
    def reverse(self) -> "PyArrowComputeUDFExpr":
        """Reverse each string."""
        return _create_str_udf(pc.utf8_reverse, DataType.string())(self._expr)

    def slice(self, *args: Any, **kwargs: Any) -> "PyArrowComputeUDFExpr":
        """Slice strings by codeunit indices."""
        return _create_str_udf(pc.utf8_slice_codeunits, DataType.string())(
            self._expr, *args, **kwargs
        )

    def replace(
        self, pattern: str, replacement: str, *args: Any, **kwargs: Any
    ) -> "PyArrowComputeUDFExpr":
        """Replace occurrences of a substring."""
        return _create_str_udf(pc.replace_substring, DataType.string())(
            self._expr, pattern, replacement, *args, **kwargs
        )

    def replace_regex(
        self, pattern: str, replacement: str, *args: Any, **kwargs: Any
    ) -> "PyArrowComputeUDFExpr":
        """Replace occurrences matching a regex pattern."""
        return _create_str_udf(pc.replace_substring_regex, DataType.string())(
            self._expr, pattern, replacement, *args, **kwargs
        )

    def replace_slice(
        self, start: int, stop: int, replacement: str, *args: Any, **kwargs: Any
    ) -> "PyArrowComputeUDFExpr":
        """Replace a slice with a string."""
        return _create_str_udf(pc.binary_replace_slice, DataType.string())(
            self._expr, start, stop, replacement, *args, **kwargs
        )

    def split(self, pattern: str, *args: Any, **kwargs: Any) -> "PyArrowComputeUDFExpr":
        """Split strings by a pattern."""
        return _create_str_udf(pc.split_pattern, DataType(object))(
            self._expr, pattern, *args, **kwargs
        )

    def split_regex(
        self, pattern: str, *args: Any, **kwargs: Any
    ) -> "PyArrowComputeUDFExpr":
        """Split strings by a regex pattern."""
        return _create_str_udf(pc.split_pattern_regex, DataType(object))(
            self._expr, pattern, *args, **kwargs
        )

    def split_whitespace(self, *args: Any, **kwargs: Any) -> "PyArrowComputeUDFExpr":
        """Split strings on whitespace."""
        return _create_str_udf(pc.utf8_split_whitespace, DataType(object))(
            self._expr, *args, **kwargs
        )

    def extract(
        self, pattern: str, *args: Any, **kwargs: Any
    ) -> "PyArrowComputeUDFExpr":
        """Extract a substring matching a regex pattern."""
        return _create_str_udf(pc.extract_regex, DataType.string())(
            self._expr, pattern, *args, **kwargs
        )

    def repeat(self, n: int, *args: Any, **kwargs: Any) -> "PyArrowComputeUDFExpr":
        """Repeat each string n times."""
        return _create_str_udf(pc.binary_repeat, DataType.string())(
            self._expr, n, *args, **kwargs
        )

    def center(
        self, width: int, padding: str = " ", *args: Any, **kwargs: Any
    ) -> "PyArrowComputeUDFExpr":
        """Center strings in a field of given width."""
        return _create_str_udf(pc.utf8_center, DataType.string())(
            self._expr, width, padding, *args, **kwargs
        )

    def lpad(
        self, width: int, padding: str = " ", *args: Any, **kwargs: Any
    ) -> "PyArrowComputeUDFExpr":
        """Right-align strings by padding with a given character while respecting ``width``.

        If the string is longer than the specified width, it remains intact (no truncation occurs).
        """
        return _create_str_udf(pc.utf8_lpad, DataType.string())(
            self._expr, width, padding, *args, **kwargs
        )

    def rpad(
        self, width: int, padding: str = " ", *args: Any, **kwargs: Any
    ) -> "PyArrowComputeUDFExpr":
        """Left-align strings by padding with a given character while respecting ``width``.

        If the string is longer than the specified width, it remains intact (no truncation occurs).
        """
        return _create_str_udf(pc.utf8_rpad, DataType.string())(
            self._expr, width, padding, *args, **kwargs
        )

    def strip(self, characters: str | None = None) -> "PyArrowComputeUDFExpr":
        """Remove leading and trailing whitespace or specified characters.

        Args:
            characters: Characters to remove. If None, removes whitespace.

        Returns:
            Expression that strips characters from both ends.
        """
        if characters is None:
            return _create_str_udf(pc.utf8_trim_whitespace, DataType.string())(
                self._expr
            )
        return _create_str_udf(pc.utf8_trim, DataType.string())(
            self._expr, characters=characters
        )

    def lstrip(self, characters: str | None = None) -> "PyArrowComputeUDFExpr":
        """Remove leading whitespace or specified characters.

        Args:
            characters: Characters to remove. If None, removes whitespace.

        Returns:
            Expression that strips characters from the left.
        """
        if characters is None:
            return _create_str_udf(pc.utf8_ltrim_whitespace, DataType.string())(
                self._expr
            )
        return _create_str_udf(pc.utf8_ltrim, DataType.string())(
            self._expr, characters=characters
        )

    def rstrip(self, characters: str | None = None) -> "PyArrowComputeUDFExpr":
        """Remove trailing whitespace or specified characters.

        Args:
            characters: Characters to remove. If None, removes whitespace.

        Returns:
            Expression that strips characters from the right.
        """
        if characters is None:
            return _create_str_udf(pc.utf8_rtrim_whitespace, DataType.string())(
                self._expr
            )
        return _create_str_udf(pc.utf8_rtrim, DataType.string())(
            self._expr, characters=characters
        )

    def pad(
        self,
        width: int,
        fillchar: str = " ",
        side: Literal["left", "right", "both"] = "right",
    ) -> "PyArrowComputeUDFExpr":
        """Pad strings to a specified width.

        Args:
            width: Target width.
            fillchar: Character to use for padding.
            side: "left", "right", or "both" for padding side.

        Returns:
            Expression that pads strings to the given width.
        """
        if side == "right":
            pc_func = pc.utf8_rpad
        elif side == "left":
            pc_func = pc.utf8_lpad
        elif side == "both":
            pc_func = pc.utf8_center
        else:
            raise ValueError("side must be 'left', 'right', or 'both'")
        return _create_str_udf(pc_func, DataType.string())(
            self._expr, width=width, padding=fillchar
        )

    def jq(self, filter_expr: str) -> "UDFExpr":
        """Apply a jq-style filter expression to JSON strings.

        Parses each string as JSON and extracts values using a jq-like path
        expression. The result is returned as a JSON-encoded string (scalars
        like strings and numbers are returned as their JSON representation).

        Supported syntax:
            - Field access: ``.name``, ``.user.email``
            - Array indexing: ``.[0]``, ``.items[1]``
            - Chained paths: ``.items[0].name``

        Args:
            filter_expr: A jq-style filter expression starting with ``.``.

        Returns:
            Expression that extracts the value from each JSON string.

        Example:
            >>> from ray.data.expressions import col  # doctest: +SKIP
            >>> col("data").str.jq(".name")  # doctest: +SKIP
            >>> col("data").str.jq(".items[0].name")  # doctest: +SKIP
        """
        steps = _parse_jq_filter(filter_expr)

        @pyarrow_udf(return_dtype=DataType.string())
        def _jq_filter(arr: pyarrow.Array) -> pyarrow.Array:
            def _process(val):
                if val is None:
                    return None
                try:
                    obj = json.loads(val)
                    obj = _apply_jq_steps(obj, steps)
                    if obj is None:
                        return None
                    return (
                        obj
                        if isinstance(obj, str)
                        else json.dumps(obj, ensure_ascii=False)
                    )
                except (json.JSONDecodeError, TypeError):
                    return None

            return pyarrow.array(
                [_process(v.as_py()) for v in arr], type=pyarrow.string()
            )

        return _jq_filter(self._expr)


_JQ_TOKEN_RE = re.compile(
    r"""
    (\w+)         # fieldName (after a dot)
    | \[(\d+)\]   # [index]
    """,
    re.VERBOSE,
)


def _parse_jq_filter(
    expr: str,
) -> List[Tuple[str, Union[str, int]]]:
    """Parse a jq filter like ``.foo.bar[0].baz`` into a list of steps.

    Args:
        expr: A jq-style filter expression starting with ``"."``.

    Returns:
        A list of ``("field", name)`` or ``("index", int)`` tuples.

    Raises:
        ValueError: If the expression is empty or cannot be fully parsed.
    """
    expr = expr.strip()
    if not expr.startswith("."):
        raise ValueError(f"jq filter must start with '.', got: {expr!r}")

    steps: List[Tuple[str, Union[str, int]]] = []
    # Skip the leading dot if it's just the identity '.'
    if expr == ".":
        return steps

    # Start after the mandatory leading dot.
    pos = 1
    for m in _JQ_TOKEN_RE.finditer(expr, pos=1):
        if m.start() < pos:
            continue
        # Ensure there are no gaps (unparsed characters)
        if m.start() > pos:
            raise ValueError(f"Unsupported jq syntax at position {pos}: {expr!r}")
        pos = m.end()
        # Consume a dot separator before the next field name.
        # A dot is only valid when followed by a word character (field name).
        if pos < len(expr) and expr[pos] == ".":
            if pos + 1 < len(expr) and re.match(r"\w", expr[pos + 1]):
                pos += 1  # consume the dot separator
            else:
                raise ValueError(f"Unsupported jq syntax at position {pos}: {expr!r}")
        field, index = m.group(1), m.group(2)
        if field is not None:
            steps.append(("field", field))
        else:
            steps.append(("index", int(index)))

    if pos < len(expr):
        raise ValueError(f"Unsupported jq syntax at position {pos}: {expr!r}")

    if not steps:
        raise ValueError(f"Empty jq filter: {expr!r}")

    return steps


def _apply_jq_steps(obj: Any, steps: List[Tuple[str, Union[str, int]]]) -> Any:
    """Traverse *obj* according to the parsed jq steps.

    Returns ``None`` if any step fails (missing key, out-of-bounds index, or
    type mismatch).
    """
    for kind, key in steps:
        if obj is None:
            return None
        if kind == "field":
            if isinstance(obj, dict):
                obj = obj.get(key)
            else:
                return None
        elif kind == "index":
            if isinstance(obj, list) and -len(obj) <= key < len(obj):
                obj = obj[key]
            else:
                return None
    return obj
