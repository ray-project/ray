import abc
from enum import Enum
from typing import Iterator, Optional, Set

from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest
from ray.data.block import BlockMetadata
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class MetadataType(Enum):
    NUM_ROWS = 0
    NUM_BYTES = 1


@DeveloperAPI
class SupportsMetadata(abc.ABC):
    """A mix-in for readers that can cheaply produce ``BlockMetadata``.

    The ``PushdownCountFiles`` rule uses this interface to optimize row
    counting: when it applies, ``Dataset.count()`` reads only file metadata
    (e.g. the Parquet footer) instead of the actual data columns.

    Unlike V1 datasources, a V2 :class:`FileReader` already owns the
    ``filesystem`` (and any pushdowns), so ``read_metadata`` takes only the
    manifest.
    """

    @abc.abstractmethod
    def read_metadata(self, file_manifest: FileManifest) -> Iterator[BlockMetadata]:
        """Yield ``BlockMetadata`` for the files in ``file_manifest``.

        Used by the ``PushdownCountFiles`` rule to avoid reading data columns
        when only the row count is needed.

        Args:
            file_manifest: A manifest of files to read metadata from.

        Returns:
            An iterator of ``BlockMetadata`` (one per file).
        """
        ...

    @abc.abstractmethod
    def available_metadata(self) -> Set[MetadataType]:
        """Return the metadata this reader instance can produce exactly."""
        ...

    @abc.abstractmethod
    def get_target_metadata_batch_size(self) -> Optional[int]:
        """Return how many files to pass to ``read_metadata`` per batch.

        The count pushdown rule reads metadata via a ``MapBatches`` op, so the
        batch-size semantics are the same.
        """
        ...
