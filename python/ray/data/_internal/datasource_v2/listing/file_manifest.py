from functools import cached_property
from typing import List, Optional

import numpy as np
import pyarrow as pa

from ray.data.block import Block, BlockAccessor, BlockColumnAccessor

# File manifest column names
PATH_COLUMN_NAME = "__path"
FILE_SIZE_COLUMN_NAME = "__file_size"
ESTIMATED_IN_MEMORY_SIZE_COLUMN_NAME = "__estimated_in_memory_size"
# Largest raw (codec-uncompressed) Parquet row group footprint for the file, in bytes.
MAX_UNCOMPRESSED_ROW_GROUP_SIZE_COLUMN_NAME = "__max_uncompressed_row_group_size"


class FileManifest:
    """Structured view over file paths and file sizes.

    Provides structured access to file paths and sizes. This avoids making implicit assumptions about block structure as
    data moves between file listing, partitioning, and reading stages.

    All extracted views (i.e., `paths`, `file_sizes`) share the same row order as the
    underlying block. Any transformation must preserve this.
    """

    def __init__(self, block: Block):
        """Create a new `FileManifest` from a block.

        Args:
            block: Block with ``PATH_COLUMN_NAME``, ``FILE_SIZE_COLUMN_NAME``,
                and optionally ``ESTIMATED_IN_MEMORY_SIZE_COLUMN_NAME`` /
                ``MAX_UNCOMPRESSED_ROW_GROUP_SIZE_COLUMN_NAME``.
                Any other columns are optional and treated as input data.
        """
        column_names = BlockAccessor.for_block(block).column_names()
        assert FILE_SIZE_COLUMN_NAME in column_names
        assert PATH_COLUMN_NAME in column_names

        self._block = block

        self._paths = block[PATH_COLUMN_NAME]
        self._file_sizes = block[FILE_SIZE_COLUMN_NAME]

        # Estimated in-memory sizes are optional; if missing, compute from file sizes
        if ESTIMATED_IN_MEMORY_SIZE_COLUMN_NAME in column_names:
            self._estimated_in_memory_sizes = block[
                ESTIMATED_IN_MEMORY_SIZE_COLUMN_NAME
            ]
        else:
            # Fallback: use 5x compression ratio (conservative estimate)
            self._estimated_in_memory_sizes = None

        if MAX_UNCOMPRESSED_ROW_GROUP_SIZE_COLUMN_NAME in column_names:
            self._max_uncompressed_row_group_sizes = block[
                MAX_UNCOMPRESSED_ROW_GROUP_SIZE_COLUMN_NAME
            ]
        else:
            self._max_uncompressed_row_group_sizes = None

    def __len__(self) -> int:
        return len(self._block)

    def __repr__(self):
        return f"<{self.__class__.__name__} length={len(self._block)}>"

    @cached_property
    def paths(self) -> np.ndarray:
        return BlockColumnAccessor.for_column(self._paths).to_numpy()

    @cached_property
    def file_sizes(self) -> np.ndarray:
        return BlockColumnAccessor.for_column(self._file_sizes).to_numpy()

    @cached_property
    def estimated_in_memory_sizes(self) -> np.ndarray:
        """Return estimated in-memory sizes, computing from file sizes if needed.

        Uses the __estimated_in_memory_size column if available, otherwise
        applies a 5x compression ratio to file sizes as a conservative estimate.
        """
        if self._estimated_in_memory_sizes is not None:
            return BlockColumnAccessor.for_column(
                self._estimated_in_memory_sizes
            ).to_numpy()
        else:
            # Fallback: 5x compression ratio (typical for Parquet)
            return (self.file_sizes * 5).astype(np.int64)

    @cached_property
    def total_estimated_in_memory_size(self) -> int:
        """Return the sum of all estimated in-memory byte sizes in this manifest."""
        return int(np.sum(self.estimated_in_memory_sizes))

    @cached_property
    def max_uncompressed_row_group_sizes(self) -> np.ndarray:
        """Per-file maximum raw uncompressed Parquet row-group size in bytes.

        When the optional metadata column is absent, returns zeros (unknown).
        """
        if self._max_uncompressed_row_group_sizes is not None:
            return (
                BlockColumnAccessor.for_column(self._max_uncompressed_row_group_sizes)
                .to_numpy()
                .astype(np.int64, copy=False)
            )
        return np.zeros(len(self), dtype=np.int64)

    @cached_property
    def max_max_uncompressed_row_group_size(self) -> int:
        """Maximum over files of each file's largest row group's uncompressed size."""
        if len(self) == 0:
            return 0
        return int(np.max(self.max_uncompressed_row_group_sizes))

    def as_block(self) -> Block:
        """Return the underlying block for the `FileManifest`.

        This doesn't make a copy of the underlying data.
        """
        return self._block

    @classmethod
    def concat(cls, manifests: List["FileManifest"]) -> "FileManifest":
        """Return a new `FileManifest` whose rows are the concatenation of
        ``manifests`` in order.

        Row alignment of ``paths`` / ``file_sizes`` is preserved because
        each input already satisfies it.
        """
        assert len(manifests) > 0, "concat requires at least one manifest"
        if len(manifests) == 1:
            return manifests[0]

        merged = pa.concat_tables(
            [
                BlockAccessor.for_block(manifest._block).to_arrow()
                for manifest in manifests
            ]
        )
        return cls(merged)

    def shuffle(self, seed: Optional[int]) -> "FileManifest":
        """Return a new `FileManifest` with rows permuted.

        Args:
            seed: Random seed. ``None`` for non-deterministic shuffling.
                When set, input rows are first sorted by path so the shuffle
                is reproducible regardless of upstream listing order
                (the threaded ``FileIndexer`` doesn't preserve order).

        Returns:
            A new `FileManifest` with the same rows in a shuffled order. The
            underlying row alignment between `paths` and `file_sizes` is
            preserved because the permutation is applied to the block as a
            whole.
        """
        n = len(self)
        if n <= 1:
            return self
        block = self._block
        if seed is not None:
            sort_indices = pa.compute.sort_indices(
                BlockAccessor.for_block(block).to_arrow(),
                sort_keys=[(PATH_COLUMN_NAME, "ascending")],
            )
            block = block.take(sort_indices)
        permutation = np.random.default_rng(seed).permutation(n)
        return FileManifest(block.take(permutation))

    @classmethod
    def construct_manifest(
        cls,
        paths: List[str],
        sizes: List[int],
        estimated_in_memory_sizes: Optional[List[int]] = None,
        max_uncompressed_row_group_sizes: Optional[List[int]] = None,
    ) -> "FileManifest":
        assert len(paths) == len(sizes)
        if estimated_in_memory_sizes is not None:
            assert len(paths) == len(estimated_in_memory_sizes)
        if max_uncompressed_row_group_sizes is not None:
            assert len(paths) == len(max_uncompressed_row_group_sizes)

        table_dict = {
            PATH_COLUMN_NAME: paths,
            FILE_SIZE_COLUMN_NAME: sizes,
        }

        # Add estimated sizes if provided
        if estimated_in_memory_sizes is not None:
            table_dict[ESTIMATED_IN_MEMORY_SIZE_COLUMN_NAME] = estimated_in_memory_sizes

        if max_uncompressed_row_group_sizes is not None:
            table_dict[
                MAX_UNCOMPRESSED_ROW_GROUP_SIZE_COLUMN_NAME
            ] = max_uncompressed_row_group_sizes

        block = pa.table(table_dict)
        return cls(block)
