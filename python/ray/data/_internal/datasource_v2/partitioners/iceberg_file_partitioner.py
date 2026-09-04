"""``FilePartitioner`` that groups Iceberg listing rows into read units."""

from __future__ import annotations

import logging
from collections import deque
from typing import Deque, List, Optional

from ray._common.utils import env_integer
from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest
from ray.data._internal.datasource_v2.partitioners.file_partitioner import (
    FilePartitioner,
)
from ray.data._internal.util import MiB
from ray.data.block import Block, BlockAccessor

logger = logging.getLogger(__name__)

# Estimated *decoded* bytes per read unit. This sets task granularity only:
# the reader decodes one file at a time, so a bin's size does not bound peak
# memory.
DEFAULT_BIN_PACKING_BYTES = env_integer("RAY_DATA_ICEBERG_BIN_PACKING_BYTES", 128 * MiB)

# Placeholder expansion factor from compressed file bytes to decoded Arrow
# bytes. Iceberg metadata records a file's compressed size and its row count,
# and nothing anywhere in it records the uncompressed or decoded size, so there
# is no exact number to read -- and measured expansion spans 1x (incompressible
# int64) to 26x (a low-cardinality string column), so no constant is right
# either. Real estimation is separate work landing with the shared size
# estimator; until then this one knob stands in for it.
_DECODE_RATIO = env_integer("RAY_DATA_ICEBERG_DECODE_RATIO", 4)


def estimate_decoded_size(compressed_size: int) -> int:
    """Guess what a file of ``compressed_size`` bytes occupies once decoded.

    Deliberately crude -- see :data:`_DECODE_RATIO`. It exists so that bin
    packing is written against decoded bytes, which is the quantity that
    matters, rather than against compressed bytes, which is merely the one
    Iceberg happens to record. Replacing the guess then changes this function
    and nothing else.
    """
    return compressed_size * _DECODE_RATIO


class IcebergFilePartitioner(FilePartitioner):
    """Packs Iceberg listing rows into byte-budgeted read units.

    One manifest row is one whole data file: an Iceberg scan task names a file,
    never a range within one, so there is nothing to split and packing is a
    single sequential sweep -- fill the open bin, seal it when it reaches the
    budget, start the next.

    Two properties are the reason this exists rather than reusing one of the
    partitioners already in the tree. ``RoundRobinPartitioner`` sizes rows with
    an ``InMemorySizeEstimator``, which an Iceberg read has no use for since the
    listing rows already carry per-file sizes and row counts. ``OnlineBinPacker``
    would size them correctly, but rebuilds each bin from paths, sizes and chunk
    metadata alone, dropping every other manifest column -- and for Iceberg
    those columns *are* the listing-to-reading contract (delete files, spec id,
    record count, partition values). This partitioner only ever slices and
    concatenates the blocks it is handed, so whatever the indexer encoded
    arrives at the reader intact.
    """

    def __init__(self, max_bin_bytes: Optional[int] = None):
        self._cap = (
            max_bin_bytes if max_bin_bytes is not None else DEFAULT_BIN_PACKING_BYTES
        )
        # Rows of the bin currently being filled, as contiguous slices of the
        # blocks they came from -- one slice per input block, not one per file.
        self._open: List[Block] = []
        self._open_bytes = 0
        self._output: Deque[FileManifest] = deque()

    @property
    def requires_global_input(self) -> bool:
        # Packing keeps one open bin across the whole listing, so it has to see
        # every row. PyIceberg's planner is single-machine anyway, so listing is
        # one task either way.
        return True

    def add_input(self, input_manifest: FileManifest) -> None:
        accessor = BlockAccessor.for_block(input_manifest.as_block())
        num_rows = len(input_manifest)
        # Start of the rows of this block not yet handed to a bin. Rows are
        # consumed in order, so every bin boundary just closes one slice and
        # opens the next.
        run_start = 0
        for idx, compressed_size in enumerate(input_manifest.file_sizes):
            self._open_bytes += estimate_decoded_size(int(compressed_size))
            # Checked after adding, so a lone file bigger than the whole budget
            # gets a bin to itself instead of dragging others in with it.
            if self._open_bytes >= self._cap:
                self._open.append(accessor.slice(run_start, idx + 1))
                run_start = idx + 1
                self._seal()
        if run_start < num_rows:
            self._open.append(accessor.slice(run_start, num_rows))

    def has_partition(self) -> bool:
        return len(self._output) > 0

    def next_partition(self) -> FileManifest:
        return self._output.popleft()

    def finalize(self) -> None:
        if self._open:
            self._seal()

    def _seal(self) -> None:
        manifest = FileManifest.concat([FileManifest(block) for block in self._open])
        logger.debug(
            "Emitting Iceberg read unit: %d files, ~%d estimated decoded bytes",
            len(manifest),
            self._open_bytes,
        )
        self._output.append(manifest)
        self._open = []
        self._open_bytes = 0
