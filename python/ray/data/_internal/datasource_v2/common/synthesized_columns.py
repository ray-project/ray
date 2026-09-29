"""The built-in synthesized columns: ``path`` and ``row_hash``.

The interface they implement, :class:`SynthesizedColumn`, and its input
:class:`ReadUnitPosition` live in
:mod:`ray.data._internal.datasource_v2.interfaces.synthesized_columns`.
"""

import hashlib
from dataclasses import dataclass

import numpy as np
import pyarrow as pa

from ray.data._internal.datasource_v2.interfaces.synthesized_columns import (
    ReadUnitPosition,
    SynthesizedColumn,
)
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
