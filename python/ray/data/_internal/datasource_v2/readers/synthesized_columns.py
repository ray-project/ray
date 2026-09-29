"""Columns a reader appends to each batch instead of reading them.

``include_paths=True`` adds a ``path`` column and ``include_row_hash=True``
adds ``row_hash``; a checkpoint ID column is the next one. Each used to be a
boolean on the reader plus four hand-written steps: advertise the column in
the schema, keep pyarrow from looking for it in the file, respect a
projection that dropped it, and build it per batch. A
:class:`SynthesizedColumn` replaces the boolean, and the reader performs the
four steps once for whatever columns it is given.

A column is computed from a :class:`ReadUnitPosition`: the read unit the
rows came from and their offset within it. A column whose values depend on that
offset (``row_hash``, a checkpoint ID) sets
:attr:`SynthesizedColumn.requires_read_unit_boundaries` so the reader never
lets one batch span two row groups.
"""

import hashlib
from abc import ABC, abstractmethod
from dataclasses import dataclass

import numpy as np
import pyarrow as pa

from ray.data._internal.datasource_v2.read_units import ReadUnit
from ray.util.annotations import DeveloperAPI

# Synthetic column name produced when ``include_paths=True``. Shared with
# the V2 datasource and scanner layers so all references are spelled the
# same way.
INCLUDE_PATHS_COLUMN_NAME = "path"

ROW_HASH_COLUMN_NAME = "row_hash"


def _compute_row_hashes(file_path: str, start_row: int, num_rows: int) -> np.ndarray:
    """Compute deterministic uint64 hashes from file path and output row position.

    ``start_row`` is the position within the output stream (post-filter), not
    the physical file offset.  This means hashes are reproducible for a given
    pipeline configuration (same file + same filter) but will differ across
    reads with different filters.

    Hashes the file path with MD5 to obtain a 64-bit seed, adds the row indices,
    then applies the splitmix64 finalizer (a bijective 64-bit mixing function) to
    produce well-distributed, reproducible hashes.  Fully vectorized via numpy.
    """
    path_seed = np.uint64(
        int.from_bytes(
            hashlib.md5(file_path.encode("utf-8"), usedforsecurity=False).digest()[:8],
            byteorder="little",
        )
    )
    keys = path_seed + np.arange(start_row, start_row + num_rows, dtype=np.uint64)

    # splitmix64 finalizer – a bijective 64-bit mixing function from
    # Steele, Lea & Flood, "Fast Splittable Pseudorandom Number Generators",
    # OOPSLA 2014.  Also used in Java's SplittableRandom.
    # Reference: https://xorshift.di.unimi.it/splitmix64.c
    keys ^= keys >> np.uint64(30)
    keys *= np.uint64(0xBF58476D1CE4E5B9)
    keys ^= keys >> np.uint64(27)
    keys *= np.uint64(0x94D049BB133111EB)
    keys ^= keys >> np.uint64(31)

    return keys


@DeveloperAPI
@dataclass(frozen=True)
class ReadUnitPosition:
    """Where one table of yielded rows sits within its read unit; the input
    to :meth:`SynthesizedColumn.compute`.

    The reader attaches one to every table it yields. A unit usually comes
    out as several tables, so ``rows_before`` is a cursor that advances by
    each table's row count: a row group read as tables of 1000, 1000 and 500
    rows gets positions with ``rows_before`` 0, 1000 and 2000.

    Attributes:
        unit: The read unit the rows came from. ``unit.source`` is the file
            path for file readers.
        rows_before: Rows the reader already yielded from ``unit`` before
            this table, counted after any pushed-down filter. Row ``i`` of
            the table is row ``rows_before + i`` of the unit's output.
        unit_start_row: Pre-filter index in ``unit.source`` of the unit's
            first row; ``0`` when the unit is the whole file. Constant for a
            unit, copied from :class:`~ray.data._internal.datasource_v2.read_units.ReadUnitFragment`.
            ``unit_start_row + rows_before`` places the table in its file,
            which is what ``row_hash`` hashes.
    """

    unit: ReadUnit
    rows_before: int = 0
    unit_start_row: int = 0


@DeveloperAPI
class SynthesizedColumn(ABC):
    """A column the reader appends to every batch rather than reading it.

    The reader owns the plumbing: it advertises :attr:`name`/:attr:`type` in
    the schema, excludes the name from what pyarrow reads, skips the column
    when a projection dropped it, and replaces any same-named column that
    the file happens to contain. An implementation sets the three class
    attributes and says how to build the column.

    Attributes:
        name: Column name as it appears in the output schema.
        type: Arrow type of the column; :meth:`compute` must return it.
        requires_read_unit_boundaries: Whether values depend on a row's
            position within its read unit. ``True`` makes the reader scan
            each unit (row group) separately and report a precise
            :attr:`ReadUnitPosition.unit` /
            :attr:`ReadUnitPosition.rows_before`.
            ``False`` (the default) lets it scan a file's row groups
            together, and the unit is the whole file.
    """

    name: str
    type: pa.DataType
    requires_read_unit_boundaries: bool = False

    @abstractmethod
    def compute(self, position: ReadUnitPosition, num_rows: int) -> pa.Array:
        """Build the column for ``num_rows`` rows read at ``position``."""
        ...


@DeveloperAPI
@dataclass(frozen=True)
class PathColumn(SynthesizedColumn):
    """The ``path`` column behind ``include_paths=True``: the source file path."""

    name = INCLUDE_PATHS_COLUMN_NAME
    type = pa.string()

    def compute(self, position: ReadUnitPosition, num_rows: int) -> pa.Array:
        return pa.repeat(pa.scalar(position.unit.source, type=pa.string()), num_rows)


@DeveloperAPI
@dataclass(frozen=True)
class RowHashColumn(SynthesizedColumn):
    """The ``row_hash`` column behind ``include_row_hash=True``.

    A deterministic uint64 per row, derived from the source path and the
    row's post-filter position within the source (V1 semantics). Needs read
    unit boundaries so two read tasks holding different row groups of the
    same file seed their positions from where their groups start, instead
    of both counting from zero and colliding.
    """

    name = ROW_HASH_COLUMN_NAME
    type = pa.uint64()
    requires_read_unit_boundaries = True

    def compute(self, position: ReadUnitPosition, num_rows: int) -> pa.Array:
        hashes = _compute_row_hashes(
            position.unit.source,
            position.unit_start_row + position.rows_before,
            num_rows,
        )
        return pa.array(hashes, type=pa.uint64())
