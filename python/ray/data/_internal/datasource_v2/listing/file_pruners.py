from abc import ABC, abstractmethod
from typing import List

from ray.data.datasource import PathPartitionFilter
from ray.data.datasource.path_util import _has_file_extension


class FilePruner(ABC):
    """Generic file-level filter applied during listing."""

    @abstractmethod
    def should_include(self, path: str) -> bool:
        """Return True if this file should be included, False to skip it."""
        ...


class FileExtensionPruner(FilePruner):
    """Skip files that don't match the expected extensions."""

    def __init__(self, file_extensions: List[str]):
        self._file_extensions = file_extensions

    def should_include(self, path: str) -> bool:
        return _has_file_extension(path, self._file_extensions)


class PartitionPruner(FilePruner):
    """Skip files based on partition column predicates (e.g., hive partitioning)."""

    def __init__(self, partition_filter: PathPartitionFilter):
        self._filter = partition_filter

    def should_include(self, path: str) -> bool:
        return self._filter.apply(path)
