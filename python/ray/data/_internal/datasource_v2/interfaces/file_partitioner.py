from abc import ABC, abstractmethod
from dataclasses import dataclass

from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest


@dataclass(frozen=True)
class PartitionHints:
    """Sizing hints ``read_api`` derives for ``DataSourceV2.get_file_partitioner``.

    They are hints: a partitioner that sizes read units from its own metadata
    (Parquet's ``OnlineBinPacker``) ignores them.

    Attributes:
        min_bucket_size: Lower bound on estimated bytes per read unit, from
            ``DataContext.target_min_block_size`` (``0`` when unset).
        max_bucket_size: Upper bound on estimated bytes per read unit, from
            ``DataContext.target_max_block_size`` (``sys.maxsize`` when unset).
        num_buckets: Target read-unit count: ``override_num_blocks`` when the
            user set it, else ``DataContext.read_op_min_num_blocks``.
    """

    min_bucket_size: int
    max_bucket_size: int
    num_buckets: int


class FilePartitioner(ABC):
    """Abstract base class for partitioning file manifests.

    A ``FilePartitioner`` groups file paths and their associated metadata into new
    file manifests based on a specific partitioning strategy.

    Implementations must be deterministic to ensure consistent partitioning across
    retries.
    """

    @property
    def requires_global_input(self) -> bool:
        """Whether every input row must reach a single instance.

        ``False`` (the default) means each listing task may partition its own shard
        independently, so listing can be parallelized. An implementation that packs
        globally -- keeping one pool of open partitions across all files -- returns
        ``True``, and ``plan_list_files_op`` then runs listing as a single task.
        """
        return False

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
