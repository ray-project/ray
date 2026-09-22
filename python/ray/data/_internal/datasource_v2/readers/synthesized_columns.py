"""Columns a reader appends to each batch instead of reading them.

``include_paths=True`` adds a ``path`` column and ``include_row_hash=True``
adds ``row_hash``; a checkpoint ID column is the next one. Each used to be a
boolean on the reader plus four hand-written steps: advertise the column in
the schema, keep pyarrow from looking for it in the file, respect a
projection that dropped it, and build it per batch. A
:class:`SynthesizedColumn` replaces the boolean, and the reader performs the
four steps once for whatever columns it is given.

A column is computed from a :class:`BatchOrigin`: the read unit the batch
came from and the row offset within it. A column whose values depend on that
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
            hashlib.md5(file_path.encode("utf-8")).digest()[:8], byteorder="little"
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
class BatchOrigin:
    """Where a batch of rows came from; the input to a synthesized column.

    Attributes:
        unit: The read unit the batch belongs to. ``unit.source`` is the
            file path for file readers.
        unit_row_offset: Rows the reader already yielded from ``unit``
            before this batch, after any pushed-down filter. Together with
            ``unit.id`` this positions every row of the batch.
        source_row_offset: Pre-filter index within ``unit.source`` of the
            unit's first row; ``0`` when the unit is the whole file. Lets
            ``row_hash`` keep its file-wide position semantics.
    """

    unit: ReadUnit
    unit_row_offset: int = 0
    source_row_offset: int = 0


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
            :attr:`BatchOrigin.unit` / :attr:`BatchOrigin.unit_row_offset`.
            ``False`` (the default) lets it scan a file's row groups
            together, and the origin names the whole file.
    """

    name: str
    type: pa.DataType
    requires_read_unit_boundaries: bool = False

    @abstractmethod
    def compute(self, origin: BatchOrigin, num_rows: int) -> pa.Array:
        """Build the column for a batch of ``num_rows`` rows read at ``origin``."""
        ...


@DeveloperAPI
@dataclass(frozen=True)
class PathColumn(SynthesizedColumn):
    """The ``path`` column behind ``include_paths=True``: the source file path."""

    name = INCLUDE_PATHS_COLUMN_NAME
    type = pa.string()

    def compute(self, origin: BatchOrigin, num_rows: int) -> pa.Array:
        return pa.repeat(pa.scalar(origin.unit.source, type=pa.string()), num_rows)


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

    def compute(self, origin: BatchOrigin, num_rows: int) -> pa.Array:
        hashes = _compute_row_hashes(
            origin.unit.source,
            origin.source_row_offset + origin.unit_row_offset,
            num_rows,
        )
        return pa.array(hashes, type=pa.uint64())
