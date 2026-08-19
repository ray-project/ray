from abc import ABC, abstractmethod
from typing import ClassVar

from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest


class FilePartitioner(ABC):
    """Abstract base class for partitioning file manifests.

    A ``FilePartitioner`` groups file paths and their associated metadata into new
    file manifests based on a specific partitioning strategy.

    Implementations must be deterministic to ensure consistent partitioning across
    retries.
    """

    #: Whether every input row must reach a single instance. ``False`` (default)
    #: means each listing task may partition its own shard independently, so
    #: listing can be parallelized. An implementation that packs globally --
    #: keeping one pool of open partitions across all files -- sets ``True``,
    #: and ``plan_list_files_op`` then runs listing as a single task.
    requires_global_input: ClassVar[bool] = False

    @abstractmethod
    def add_input(self, input_manifest: FileManifest):
        """Add a file manifest to be partitioned.

        Args:
            input_manifest: A ``FileManifest`` containing paths and metadata to partition.
        """
        ...

    @abstractmethod
    def has_partition(self) -> bool:
        """Check if there are any partitions available.

        Returns:
            ``True`` if there are partitions ready to be retrieved via
            ``next_partition()``, ``False`` otherwise.
        """
        ...

    @abstractmethod
    def next_partition(self) -> FileManifest:
        """Get the next available partition.

        Returns:
            A ``FileManifest`` containing the paths and metadata for the next partition.
        """
        ...

    @abstractmethod
    def finalize(self):
        """Process any remaining files and complete the partitioning.

        This method is called after all inputs have been added via ``add_input()`` to
        ensure any buffered files are properly partitioned.
        """
        ...
