from dataclasses import asdict, dataclass
from functools import cached_property
from typing import Any, Dict, List, Mapping, Optional, Tuple

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from ray.data._internal.table_block import TableBlockAccessor
from ray.data.block import Block, BlockAccessor, BlockColumnAccessor

# The per-row chunk metadata a ``FileManifest`` carries: ``None`` for a whole
# file, else the dict form of a ``FileChunk`` (see ``FileChunk.to_metadata``).
ChunkMetadata = Dict[str, Any]


@dataclass(frozen=True)
class FileChunk:
    """A run of one file's read units: what an indexer lists, a partitioner
    packs and a reader scans.

    A read unit is the piece of a file a reader can scan on its own (see
    ``ReadUnit``); for Parquet it is a row group, and ``unit_ids`` are the
    ``ReadUnit.index`` values. The Parquet footer reader builds one run per
    row group, coalescing merges neighbours, the manifest stores
    :meth:`to_metadata` in its chunk column, and ``OnlineBinPacker`` reads it
    back, splits it at unit boundaries and emits one merged run per file. The
    packer only sums ``size_bytes`` against its budget; the indexer decides
    what the number measures (Parquet records the projection-scoped
    uncompressed size from the footer).
    """

    unit_ids: Tuple[int, ...]
    num_rows: int  # summed over the run
    size_bytes: int  # summed over the run; what a partitioner budgets on
    # Whether every row in the run survives the pushed predicate, so
    # ``num_rows`` is an exact survivor count that limit push-down may use.
    fully_matched: bool = True
    # Per-unit breakdown in ``unit_ids`` order, only for runs of more than
    # one unit, so a partitioner can split the run at exact boundaries.
    unit_sizes: Tuple[int, ...] = ()
    unit_rows: Tuple[int, ...] = ()

    def __post_init__(self):
        for name in ("unit_sizes", "unit_rows"):
            breakdown = getattr(self, name)
            assert not breakdown or len(breakdown) == len(self.unit_ids), (
                f"{name} has {len(breakdown)} entries for {len(self.unit_ids)} "
                "unit_ids; a breakdown is empty or has one entry per unit"
            )

    def to_metadata(self) -> ChunkMetadata:
        return asdict(self)

    @classmethod
    def from_metadata(cls, metadata: Mapping[str, Any]) -> "FileChunk":
        # Values come back from the manifest block as lists and numpy scalars.
        return cls(
            unit_ids=tuple(metadata["unit_ids"]),
            num_rows=int(metadata["num_rows"]),
            size_bytes=int(metadata["size_bytes"]),
            fully_matched=bool(metadata["fully_matched"]),
            unit_sizes=tuple(metadata["unit_sizes"]),
            unit_rows=tuple(metadata["unit_rows"]),
        )


# File manifest column names
PATH_COLUMN_NAME = "__path"
FILE_SIZE_COLUMN_NAME = "__file_size"
FILE_CHUNK_METADATA_COLUMN_NAME = "__file_chunk_metadata"

# The columns every manifest carries. A source may add its own beyond these;
# see :meth:`FileManifest.extra_columns`.
REQUIRED_COLUMN_NAMES = (
    PATH_COLUMN_NAME,
    FILE_SIZE_COLUMN_NAME,
    FILE_CHUNK_METADATA_COLUMN_NAME,
)


class FileManifest:
    """Structured view over file paths, sizes, and per-chunk metadata.

    Provides structured access to file paths, sizes, and chunk metadata. This avoids
    making implicit assumptions about block structure as data moves between file
    listing, partitioning, and reading stages.

    All extracted views (i.e., `paths`, `file_sizes`, `file_chunk_metadatas`) share
    the same row order as the underlying block. Any transformation must preserve this.

    Read it as a manifest of chunks, not of files: each row is one chunk of
    work for a reader, which is a whole file unless its chunk-metadata entry
    narrows it to a run of read units. For unchunked files (whole-file reads),
    the chunk-metadata entry is ``None`` and ``file_sizes`` equals the on-disk
    file size. For chunked files, multiple rows can share the same path but
    carry different chunk metadata.
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

    @cached_property
    def extra_columns(self) -> Optional[pa.Table]:
        """The source's own columns, beside the three required ones.

        The manifest is the only thing the framework carries from listing to
        reading, so a source that finds its own data puts what the reader
        needs here -- for Iceberg, each file's delete files, partition values
        and record count.

        A partitioner that rebuilds its output rows instead of slicing its
        input must pass these to :meth:`construct_manifest`, or that state
        never reaches the reader.

        Returns:
            One row per manifest row, or ``None`` when the manifest carries
            only the required columns -- every plain file format.
        """
        table = BlockAccessor.for_block(self._block).to_arrow()
        names = [
            name for name in table.column_names if name not in REQUIRED_COLUMN_NAMES
        ]
        if not names:
            return None
        return table.select(names)

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
        extra_columns: Optional[pa.Table] = None,
    ) -> "FileManifest":
        """Build a manifest from the required columns, plus any source columns.

        Args:
            paths: One file path per row.
            sizes: On-disk size of each row's file, aligned with ``paths``.
            chunk_metadatas: Per-row chunk metadata, ``None`` for a whole file.
            extra_columns: The source's own columns (see
                :attr:`extra_columns`), one row each, in the same row order.
                Appended as-is, so their Arrow types survive the round trip.

        Returns:
            A manifest whose rows are ``paths`` in the order given.
        """
        assert len(paths) == len(sizes) == len(chunk_metadatas)

        block = pa.table(
            {
                PATH_COLUMN_NAME: paths,
                FILE_SIZE_COLUMN_NAME: sizes,
                FILE_CHUNK_METADATA_COLUMN_NAME: chunk_metadatas,
            }
        )
        if extra_columns is not None:
            assert extra_columns.num_rows == len(
                paths
            ), f"{extra_columns.num_rows} extra-column rows for {len(paths)} paths"
            for field in extra_columns.schema:
                block = block.append_column(field, extra_columns[field.name])
        return cls(block)
