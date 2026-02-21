from functools import cached_property
from typing import List

import numpy as np
import pyarrow as pa

from ray.data.block import Block, BlockAccessor, BlockColumnAccessor

# File manifest column names
PATH_COLUMN_NAME = "__path"
FILE_SIZE_COLUMN_NAME = "__file_size"


class FileManifest:
    """Structured view over file paths and file sizes.

    A thin wrapper over `ListFiles` outputs that provides structured access to file
    paths and sizes. This avoids making implicit assumptions about block structure as
    data moves between file listing, partitioning, and reading stages.

    All extracted views (i.e., `paths`, `file_sizes`) share the same row order as the
    underlying block. Any transformation must preserve this.
    """

    def __init__(self, block: Block):
        """Create a new `FileManifest` from a block.

        Args:
            block: Block with `PATH_COLUMN_NAME`, `FILE_SIZE_COLUMN_NAME`
                Any other columns are optional and treated as input data.
        """
        column_names = BlockAccessor.for_block(block).column_names()
        assert FILE_SIZE_COLUMN_NAME in column_names
        assert PATH_COLUMN_NAME in column_names

        self._block = block

        self._paths = block[PATH_COLUMN_NAME]
        self._file_sizes = block[FILE_SIZE_COLUMN_NAME]

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

    def as_block(self) -> Block:
        """Return the underlying block for the `FileManifest`.

        This doesn't make a copy of the underlying data.
        """
        return self._block

    @classmethod
    def construct_manifest(
        cls,
        paths: List[str],
        sizes: List[int],
    ) -> "FileManifest":
        assert len(paths) == len(sizes)

        block = pa.table(
            {
                PATH_COLUMN_NAME: paths,
                FILE_SIZE_COLUMN_NAME: sizes,
            }
        )
        return cls(block)
