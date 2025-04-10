import abc
from typing import TYPE_CHECKING, Optional

from pyarrow.fs import FileSystem

from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data.block import BlockAccessor

if TYPE_CHECKING:
    from .file_reader import FileReader


class InMemorySizeEstimator(abc.ABC):
    @abc.abstractmethod
    def estimate_in_memory_size(
        self,
        path: str,
        file_size: int,
        *,
        filesystem: FileSystem,
    ) -> int:
        """Estimate the in-memory size of the data at the given path.

        This `ListFiles` operator uses this method to ensure that each read task
        receives an appropriate amount of data. To ensure that file listing is
        efficient, this method must be cheap to call, on average.

        Args:
            path: The path to the file.
            file_size: The on-disk size of the file in bytes.
            filesystem: The filesystem to read from.

        Returns:
            The estimated in-memory size of the data in bytes.
        """


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

    def estimate_in_memory_size(
        self,
        path: str,
        file_size: int,
        *,
        filesystem: FileSystem,
    ) -> int:
        assert file_size >= 0

        if self._encoding_ratio is None:
            # Estimating the encoding ratio can be expensive since it requires
            # reading the file. So, we only estimate the encoding ratio if we don't
            # already have one.
            self._encoding_ratio = self._estimate_encoding_ratio(
                path, file_size, filesystem
            )

        if self._encoding_ratio is None:
            # If we couldn't estimate the encoding ratio, assume a 1:1 encoding ratio.
            return file_size
        else:
            return file_size * self._encoding_ratio

    def _estimate_encoding_ratio(
        self,
        path: str,
        file_size: int,
        filesystem: FileSystem,
    ) -> Optional[float]:
        """

        Returns:
            The estimated encoding ratio of the file, or `None` if the ratio can't
            be estimated.
        """
        # If the file is empty, we can't estimate the encoding ratio.
        if not file_size:
            return None

        batches = self._reader.read_paths([path], filesystem=filesystem)

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
