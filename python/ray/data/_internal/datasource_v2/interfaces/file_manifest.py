from functools import cached_property
from typing import List, Optional, Type, TypedDict, TypeVar, cast, get_type_hints

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from ray.data._internal.table_block import TableBlockAccessor
from ray.data.block import Block, BlockAccessor, BlockColumnAccessor


class ChunkMetadata(TypedDict):
    """Base type for the per-row chunk metadata a ``FileManifest`` carries.

    A manifest row is a whole file (``chunk_metadata`` is ``None``) or a part of
    one that an indexer chose to read separately, such as a run of Parquet row
    groups emitted by ``FooterFileIndexer``. The metadata names that part so the
    partitioner and reader never re-derive it.
    """

    pass


_ChunkMetadataT = TypeVar("_ChunkMetadataT", bound=ChunkMetadata)


def create_chunk_metadata(cls: Type[_ChunkMetadataT], **kwargs) -> _ChunkMetadataT:
    """Create a metadata instance with validation, ensure the keys are correct."""
    required_keys = list(get_type_hints(cls).keys())

    missing_keys = [key for key in required_keys if key not in kwargs]
    if missing_keys:
        raise ValueError(f"Missing required keys: {missing_keys}")

    extra_keys = [key for key in kwargs if key not in required_keys]
    if extra_keys:
        raise ValueError(f"Unexpected keys: {extra_keys}")

    return cast(_ChunkMetadataT, kwargs)


# File manifest column names
PATH_COLUMN_NAME = "__path"
FILE_SIZE_COLUMN_NAME = "__file_size"
FILE_CHUNK_METADATA_COLUMN_NAME = "__file_chunk_metadata"


class FileManifest:
    """Structured view over file paths, sizes, and per-chunk metadata.

    Provides structured access to file paths, sizes, and chunk metadata. This avoids
    making implicit assumptions about block structure as data moves between file
    listing, partitioning, and reading stages.

    All extracted views (i.e., `paths`, `file_sizes`, `file_chunk_metadatas`) share
    the same row order as the underlying block. Any transformation must preserve this.

    Each row represents a single chunk of a file. For unchunked files (whole-file
    reads), the chunk-metadata entry is ``None`` and ``file_sizes`` equals the
    on-disk file size. For chunked files, multiple rows can share the same path
    but carry different chunk metadata.
    """

    def __init__(self, block: Block):
        """Create a new `FileManifest` from a block.

        Args:
            block: Block with `PATH_COLUMN_NAME`, `FILE_SIZE_COLUMN_NAME`, and
                `FILE_CHUNK_METADATA_COLUMN_NAME` columns. Any other columns are
                optional and treated as input data.
        """
        accessor = BlockAccessor.for_block(block)
        assert isinstance(accessor, TableBlockAccessor)
        column_names = accessor.column_names()
        assert FILE_SIZE_COLUMN_NAME in column_names
        assert PATH_COLUMN_NAME in column_names
        assert FILE_CHUNK_METADATA_COLUMN_NAME in column_names

        self._block = block

        self._paths = block[PATH_COLUMN_NAME]
        self._file_sizes = block[FILE_SIZE_COLUMN_NAME]
        self._file_chunk_metadatas = block[FILE_CHUNK_METADATA_COLUMN_NAME]

    def __len__(self) -> int:
        return len(self._block)

    def __repr__(self):
        return f"<{self.__class__.__name__} length={len(self._block)}>"

    # TODO Use arrow arrays instead of numpy for these properties.
    @cached_property
    def paths(self) -> np.ndarray:
        return BlockColumnAccessor.for_column(self._paths).to_numpy()

    @cached_property
    def file_sizes(self) -> np.ndarray:
        return BlockColumnAccessor.for_column(self._file_sizes).to_numpy()

    @cached_property
    def file_chunk_metadatas(self) -> np.ndarray:
        return BlockColumnAccessor.for_column(self._file_chunk_metadatas).to_numpy()

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
            # pyrefly: ignore[missing-attribute]  # pc functions are runtime-generated.
            sort_indices = pc.sort_indices(
                BlockAccessor.for_block(block).to_arrow(),
                sort_keys=[(PATH_COLUMN_NAME, "ascending")],
            )
            block = block.take(sort_indices)
        permutation = np.random.default_rng(seed).permutation(n)
        return FileManifest(block.take(permutation))

    @classmethod
    def construct_manifest(
        cls,
        *,
        paths: List[str],
        sizes: List[int],
        chunk_metadatas: List[Optional[ChunkMetadata]],
    ) -> "FileManifest":
        assert len(paths) == len(sizes) == len(chunk_metadatas)

        block = pa.table(
            {
                PATH_COLUMN_NAME: paths,
                FILE_SIZE_COLUMN_NAME: sizes,
                FILE_CHUNK_METADATA_COLUMN_NAME: chunk_metadatas,
            }
        )
        return cls(block)
