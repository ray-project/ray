import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import numpy as np

from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest
from ray.data._internal.datasource_v2.readers.file_reader import FileReader
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data.block import BlockAccessor
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    import pyarrow as pa
    import pyarrow.fs

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

        # Use ``None`` chunk metadata: the size estimator reads the file whole
        # to estimate the encoding ratio; chunk-level splitting is irrelevant here.
        manifest = FileManifest.construct_manifest(
            [path],
            [file_size],
            [None],
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

# Rows per row group to read when sampling the encoding ratio. Matches V1's
# ``PARQUET_ENCODING_RATIO_ESTIMATE_NUM_ROWS``. Capped against the actual row
# group size — see ``ParquetSamplingInMemorySizeEstimator``.
PARQUET_ENCODING_RATIO_ESTIMATE_NUM_ROWS = 1024


@DeveloperAPI
class ParquetInMemorySizeEstimator(InMemorySizeEstimator):
    """Estimates in-memory sizes for Parquet files using a fixed encoding ratio.

    Parquet files are typically much smaller on disk than in memory due to
    columnar compression and encoding. This estimator applies a constant
    ratio (default 5x) to avoid the overhead of reading file metadata or
    sampling data, which can be slow for Parquet files and hurt startup time.
    """

    def __init__(self, encoding_ratio: float = PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT):
        self._encoding_ratio = encoding_ratio

    def estimate_in_memory_sizes(self, manifest: FileManifest) -> np.ndarray:
        return self._encoding_ratio * manifest.file_sizes


@DeveloperAPI
class ParquetSamplingInMemorySizeEstimator(InMemorySizeEstimator):
    """Samples one row group to measure the actual Parquet encoding ratio.

    Mirrors V1's ``_estimate_files_encoding_ratio``: open the first manifest
    file, read up to ``PARQUET_ENCODING_RATIO_ESTIMATE_NUM_ROWS`` rows from
    its first row group, compute ``nbytes / num_rows`` on the resulting
    batch, multiply by the file's total row count to estimate the file's
    full in-memory footprint, and divide by on-disk size to get the
    encoding ratio. The ratio is cached on the estimator and applied to
    every file in subsequent calls.

    Why this and not :class:`SamplingInMemorySizeEstimator`?

    - **Cheap.** The sample is one row group of one file — typically
      milliseconds — not a full-file scan.
    - **Multi-batch-correct.** The generic
      :class:`SamplingInMemorySizeEstimator` falls back to a 1:1 ratio
      when a file yields more than one batch (which Parquet files with
      multiple row groups always do), defeating the point on most real
      datasets. Row-group sampling is invariant to row-group count.
    - **Accurate for dictionary-encoded columns.** Parquet's
      ``total_uncompressed_size`` reports dict-index bytes, not the
      expanded Arrow representation — a single sampled batch's actual
      ``nbytes`` does include the expansion.
    """

    def __init__(
        self,
        filesystem: "Optional[pyarrow.fs.FileSystem]" = None,
        columns: Optional[List[str]] = None,
        schema: "Optional[pa.Schema]" = None,
        parquet_format_kwargs: Optional[Dict[str, Any]] = None,
    ):
        """Initialize the sampler.

        Args:
            filesystem: Filesystem the sampled file lives on.
            columns: Projected columns. If ``None``, all columns are
                measured (matches ``read_parquet`` without ``columns=``).
                Sampling only what's read keeps the ratio honest when the
                caller projects a small subset of a wide schema.
            schema: Optional unified schema. Forwarded to
                ``pyarrow.dataset.dataset`` so per-fragment null-fill /
                cast matches the eventual scan.
            parquet_format_kwargs: Extra kwargs threaded into
                ``pyarrow.dataset.ParquetFileFormat`` (e.g.
                ``coerce_int96_timestamp_unit``).
        """
        self._filesystem = filesystem
        self._columns = columns
        self._schema = schema
        self._parquet_format_kwargs = parquet_format_kwargs or {}
        self._encoding_ratio: Optional[float] = None

    @classmethod
    def from_reader(
        cls, reader: "ParquetFileReader"
    ) -> "ParquetSamplingInMemorySizeEstimator":
        """Build a sampler from a configured :class:`ParquetFileReader`.

        The reader already knows the filesystem, projection, unified schema,
        and ``ParquetFileFormat`` overrides used by the read path, so we
        sample with the same configuration that will eventually be applied
        — anything else can produce a ratio that doesn't match real reads.
        """
        return cls(
            filesystem=reader._filesystem,
            columns=reader._columns,
            schema=reader._schema,
            parquet_format_kwargs=reader._parquet_format_kwargs,
        )

    def estimate_in_memory_sizes(self, manifest: FileManifest) -> np.ndarray:
        if self._encoding_ratio is None:
            for path, file_size in zip(manifest.paths, manifest.file_sizes):
                if file_size <= 0:
                    continue
                ratio = self._sample_one_row_group(path, file_size)
                if ratio is not None:
                    self._encoding_ratio = ratio
                    logger.debug(
                        "Sampled Parquet encoding ratio %.2fx from %s",
                        ratio,
                        path,
                    )
                    break
        ratio = (
            self._encoding_ratio
            if self._encoding_ratio is not None
            else PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT
        )
        return manifest.file_sizes * ratio

    def _sample_one_row_group(self, path: str, file_size: int) -> Optional[float]:
        """Sample one row group from ``path`` and return its encoding ratio,
        or ``None`` if the file can't be sampled (empty file, no row groups,
        per-row size collapses to zero, or any I/O error).

        Delegates the actual row-group read to V1's
        :func:`_fetch_parquet_file_info`, which already handles the
        ARROW-5030 nested-type fallback path. We wrap the pyarrow fragment
        in V1's :class:`_ParquetFragment` to satisfy that API.
        """
        import pyarrow.dataset as pds

        from ray.data._internal.datasource.parquet_datasource import (
            _fetch_parquet_file_info,
            _ParquetFragment,
        )

        try:
            fmt = pds.ParquetFileFormat(**self._parquet_format_kwargs)
            dataset = pds.dataset(
                path,
                format=fmt,
                filesystem=self._filesystem,
                schema=self._schema,
            )
            fragments = list(dataset.get_fragments())
            if not fragments:
                return None
            file_info = _fetch_parquet_file_info(
                _ParquetFragment(fragments[0], file_size),
                columns=self._columns,
                schema=self._schema,
            )
            if file_info is None or file_info.avg_row_in_mem_bytes is None:
                return None
            in_memory_size = file_info.estimate_in_memory_bytes()
            if in_memory_size is None or in_memory_size == 0:
                return None
            return in_memory_size / file_size
        except Exception as e:
            # Sampling is best-effort; any failure falls back to the fixed
            # 5x default so a malformed sample can't break the read.
            logger.debug("Parquet encoding-ratio sampling failed for %s: %s", path, e)
            return None


if TYPE_CHECKING:
    from ray.data._internal.datasource_v2.readers.parquet_file_reader import (
        ParquetFileReader,
    )
