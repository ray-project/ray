"""Block budgets for streaming :meth:`ray.data.Dataset.repartition`.

A :class:`BlockBudget` describes how output blocks are sized during a streaming
(no-shuffle, order-preserving) repartition. Pass one as
``ds.repartition(block_budget=...)``:

* :class:`RowCount` -- cap each block at ``n`` rows.
* :class:`ByteSize` -- cap each block at roughly ``n`` bytes.

A budget encapsulates two questions the block-shaping machinery asks: how full a
block is (:meth:`BlockBudget.measure`) and where to cut an over-full block
(:meth:`BlockBudget.boundary_row`).
"""
from __future__ import annotations

import math
import re
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Union

from ray.data.context import MAX_SAFE_BLOCK_SIZE_FACTOR
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.data.block import BlockAccessor

# Mirrors ``SSR_API_GROUP`` in ``ray.data.dataset``. Kept inline to avoid an
# import cycle (``ray.data.dataset`` imports this module).
_SSR_API_GROUP = "Sorting, Shuffling and Repartitioning"


@PublicAPI(stability="alpha", api_group=_SSR_API_GROUP)
class BlockBudget(ABC):
    """Base class describing how to size output blocks for ``repartition``.

    Subclasses define the per-block ``limit`` and how to measure and cut a
    block. Output blocks satisfy ``measure(block) <= limit``, except a block
    forced to hold a single row whose own measure already exceeds ``limit``
    (rows are atomic and never split).
    """

    @property
    @abstractmethod
    def limit(self) -> int:
        """The per-block budget, in this budget's units (rows / bytes)."""

    @abstractmethod
    def measure(self, block: "BlockAccessor") -> int:
        """The total amount of this budget's quantity contained in ``block``."""

    @abstractmethod
    def boundary_row(self, block: "BlockAccessor") -> int:
        """Number of leading rows of ``block`` that form the next output block.

        Returns the largest row prefix whose ``measure`` stays ``<= limit``, but
        always ``>= 1`` for a non-empty block (a single over-budget row forms
        its own block), and ``0`` for an empty block. Cuts land on row
        boundaries only.
        """


@PublicAPI(stability="alpha", api_group=_SSR_API_GROUP)
class RowCount(BlockBudget):
    """Cap each output block at ``n`` rows.

    Args:
        n: Maximum number of rows per block.
        strict: If ``True``, every output block except the last is made to have
            exactly ``n`` rows (blocks are split as needed). If ``False``
            (default), uses best-effort bundling.
    """

    def __init__(self, n: int, *, strict: bool = False):
        if n <= 0:
            raise ValueError(f"RowCount `n` must be positive, got {n}")
        self._n = int(n)
        self.strict = strict

    @property
    def limit(self) -> int:
        return self._n

    def measure(self, block: "BlockAccessor") -> int:
        return block.num_rows()

    def boundary_row(self, block: "BlockAccessor") -> int:
        return min(self._n, block.num_rows())

    def __repr__(self) -> str:
        return f"RowCount(n={self._n}, strict={self.strict})"

    def __eq__(self, other: object) -> bool:
        return isinstance(other, RowCount) and (self._n, self.strict) == (
            other._n,
            other.strict,
        )

    def __hash__(self) -> int:
        return hash((RowCount, self._n, self.strict))


@PublicAPI(stability="alpha", api_group=_SSR_API_GROUP)
class ByteSize(BlockBudget):
    """Cap each output block at roughly ``n`` bytes.

    This is the budget form of ``DataContext.target_max_block_size``.

    Args:
        n: Target maximum bytes per block. Either an ``int`` (bytes) or a string
            with a unit suffix, e.g. ``"128MiB"``, ``"1GiB"``, ``"500MB"``.
    """

    def __init__(self, n: Union[int, str]):
        n = _parse_byte_size(n)
        if n <= 0:
            raise ValueError(f"ByteSize must be positive, got {n}")
        self._n = n

    @property
    def limit(self) -> int:
        return self._n

    def measure(self, block: "BlockAccessor") -> int:
        return block.size_bytes()

    def boundary_row(self, block: "BlockAccessor") -> int:
        num_rows = block.num_rows()
        if num_rows == 0:
            return 0
        # Only split a block that meaningfully exceeds the target, so the last
        # block produced is at least half the target size. Mirrors the historical
        # ``MAX_SAFE_BLOCK_SIZE_FACTOR`` slicing behavior in ``BlockOutputBuffer``.
        if block.size_bytes() < MAX_SAFE_BLOCK_SIZE_FACTOR * self._n:
            return num_rows
        bytes_per_row = block.size_bytes() / num_rows
        return max(1, math.ceil(self._n / bytes_per_row))

    def __repr__(self) -> str:
        return f"ByteSize(n={self._n})"

    def __eq__(self, other: object) -> bool:
        return isinstance(other, ByteSize) and self._n == other._n

    def __hash__(self) -> int:
        return hash((ByteSize, self._n))


_BYTE_UNITS = {
    "B": 1,
    "KB": 1000,
    "KIB": 1024,
    "MB": 1000**2,
    "MIB": 1024**2,
    "GB": 1000**3,
    "GIB": 1024**3,
    "TB": 1000**4,
    "TIB": 1024**4,
}


def _parse_byte_size(n: Union[int, str]) -> int:
    """Parse a byte size given as an int or a unit string like ``"128MiB"``."""
    if isinstance(n, bool):
        raise TypeError("ByteSize does not accept a bool")
    if isinstance(n, int):
        return n
    if not isinstance(n, str):
        raise TypeError(f"ByteSize expects an int or str, got {type(n).__name__}")
    match = re.fullmatch(r"\s*([0-9]*\.?[0-9]+)\s*([a-zA-Z]*)\s*", n)
    if match is None:
        raise ValueError(f"Invalid byte size string: {n!r}")
    value, unit = match.group(1), match.group(2).upper()
    if unit == "":
        # A bare number string is interpreted as raw bytes.
        return int(float(value))
    if unit not in _BYTE_UNITS:
        raise ValueError(
            f"Unknown byte unit {match.group(2)!r} in {n!r}; "
            f"expected one of {sorted(_BYTE_UNITS)}"
        )
    return int(float(value) * _BYTE_UNITS[unit])
