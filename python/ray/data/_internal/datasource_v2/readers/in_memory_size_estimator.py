import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Optional

import numpy as np

from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest
from ray.data._internal.datasource_v2.readers.file_reader import FileReader
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data.block import BlockAccessor
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from pyarrow.fs import FileSystem

logger = logging.getLogger(__name__)


@DeveloperAPI
class InMemorySizeEstimator(ABC):
    @abstractmethod
    def estimate_in_memory_sizes(self, manifest: FileManifest) -> np.ndarray:
        """Estimate the in-memory sizes of the paths in the given manifest.

        Some `FilePartitioner` implementations use this method to ensure that each
        read task receives an appropriate amount of data. To ensure that file listing
        is efficient, this method must be cheap to call, on average.

        Args:
            manifest: A manifest containing the paths and on-disk sizes of the files.

        Returns:
            The estimated in-memory sizes of the data in bytes.
        """
        ...

    def estimate_max_uncompressed_row_group_sizes(
        self, manifest: FileManifest
    ) -> Optional[np.ndarray]:
        """Optional per-file max raw uncompressed Parquet row-group size in bytes.

        Default ``None`` means the listing stage does not populate
        ``FileManifest``'s ``__max_uncompressed_row_group_size`` column.
        """
        return None


def _parquet_max_uncompressed_row_group_bytes(
    path: str, filesystem: Optional["FileSystem"]
) -> int:
    import pyarrow.parquet as pq

    try:
        if filesystem is not None:
            pf = pq.ParquetFile(path, filesystem=filesystem)
        else:
            pf = pq.ParquetFile(path)
        metadata = pf.metadata
    except Exception as e:
        logger.debug(
            "Could not read Parquet footer for row-group sizing (%s): %s", path, e
        )
        return 0

    max_uc = 0
    for rg_idx in range(metadata.num_row_groups):
        rg_meta = metadata.row_group(rg_idx)
        uc = sum(
            rg_meta.column(col_idx).total_uncompressed_size
            for col_idx in range(rg_meta.num_columns)
        )
        max_uc = max(max_uc, uc)
    return max_uc


@DeveloperAPI
class SamplingInMemorySizeEstimator(InMemorySizeEstimator):
    """Estimates in-memory sizes by reading files.

    This class estimates the in-memory size of files by multiplying the on-disk
    size by an estimated encoding ratio. If an instance hasn't estimated an encoding
    ratio yet, it'll read a file to estimate it. Otherwise, it'll use the previously
    estimated encoding ratio.

    TODO: This approach doesn't work well for formats that produce multiple batches
    (because we assume a 1:1 encoding ratio) or for formats that vary in encoding
    ratios (e.g. videos).
    """

    def __init__(self, reader: "FileReader"):
        self._reader = reader

        self._encoding_ratio = None

    def estimate_in_memory_sizes(self, manifest: FileManifest) -> np.ndarray:
        assert np.all(manifest.file_sizes >= 0)

        for path, file_size in zip(manifest.paths, manifest.file_sizes):
            if self._encoding_ratio is None:
                # Estimating the encoding ratio can be expensive since it requires
                # reading the file. So, we only estimate the encoding ratio if we don't
                # already have one.
                self._encoding_ratio = self._estimate_encoding_ratio(path, file_size)
                break

        if self._encoding_ratio is None:
            # If we couldn't estimate the encoding ratio, assume a 1:1 encoding ratio.
            return manifest.file_sizes
        else:
            return manifest.file_sizes * self._encoding_ratio

    def _estimate_encoding_ratio(
        self,
        path: str,
        file_size: int,
    ) -> Optional[float]:
        """
        Estimate the encoding ratio (in-memory size / on-disk size) for a file.

        Args:
            path: The path to the file.
            file_size: The on-disk size of the file/chunk in bytes.

        Returns:
            The estimated encoding ratio of the file, or `None` if the ratio can't
            be estimated.
        """
        # If the file is empty, we can't estimate the encoding ratio.
        if not file_size:
            return None

        manifest = FileManifest.construct_manifest(
            [path],
            [file_size],
        )
        batches = self._reader.read(manifest)

        try:
            first_batch = next(batches)
        except StopIteration:
            # If there's no data, we can't estimate the encoding ratio.
            return None

        try:
            # Try to read a second batch. If it succeeds, it means the file contains
            # multiple batches.
            next(batches)
        except StopIteration:
            # Each file contains exactly one batch.
            builder = DelegatingBlockBuilder()
            builder.add_batch(first_batch)
            block = builder.build()

            in_memory_size = BlockAccessor.for_block(block).size_bytes()
        else:
            # Each file contains multiple batches.
            #
            # NOTE: To avoid reading the entire file to estimate the encoding ratio,
            # we assume the file is 1:1 encoded. We can't return `None` because if
            # all files contain multiple batches, then we'd try to re-estimate the
            # encoding ratio for every file, and that'd be very expensive.
            in_memory_size = file_size

        return in_memory_size / file_size


# Default Parquet encoding ratio: in-memory is ~5x on-disk size.
# Parquet uses columnar compression and encoding, so Arrow in-memory
# representation is significantly larger than the on-disk format.
PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT = 5


@DeveloperAPI
class ParquetInMemorySizeEstimator(InMemorySizeEstimator):
    """Estimates in-memory sizes for Parquet files using a fixed encoding ratio.

    Parquet files are typically much smaller on disk than in memory due to
    columnar compression and encoding. This estimator applies a constant
    ratio (default 5x) to avoid the overhead of reading file metadata or
    sampling data, which can be slow for Parquet files and hurt startup time.

    When a PyArrow ``filesystem`` is supplied (resolved read paths), also reads each
    file's footer to populate max uncompressed row-group footprints for read-task
    memory hints.
    """

    def __init__(
        self,
        encoding_ratio: float = PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT,
        filesystem: Optional["FileSystem"] = None,
    ):
        self._encoding_ratio = encoding_ratio
        self._filesystem = filesystem

    def estimate_in_memory_sizes(self, manifest: FileManifest) -> np.ndarray:
        return self._encoding_ratio * manifest.file_sizes

    def estimate_max_uncompressed_row_group_sizes(
        self, manifest: FileManifest
    ) -> Optional[np.ndarray]:
        paths = manifest.paths
        out = np.empty(len(paths), dtype=np.int64)
        for i, path in enumerate(paths):
            out[i] = _parquet_max_uncompressed_row_group_bytes(path, self._filesystem)
        return out
