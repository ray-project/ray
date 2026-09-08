from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, List

from ray.data.datasource import PathPartitionFilter, PathPartitionParser
from ray.data.datasource.path_util import _has_file_extension

if TYPE_CHECKING:
    from ray.data.datasource.partitioning import Partitioning
    from ray.data.expressions import Expr


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


class PartitionPredicatePruner(FilePruner):
    """Skip files whose partition values fail a pushed-down predicate.

    The reader applies the same predicate to the same paths in
    ``ArrowFileScanner.prune_manifest``. Evaluating it here as well is not
    redundant work that could disagree: both go through
    :meth:`PathPartitionParser.evaluate_predicate_on_partition`, so listing
    drops exactly the files the reader would have dropped. That equality is
    what lets a pushed-down limit stop listing early -- every row listing
    counts belongs to a file the reader keeps.
    """

    def __init__(self, partitioning: "Partitioning", predicate: "Expr"):
        self._parser = PathPartitionParser(partitioning)
        self._predicate = predicate

    def should_include(self, path: str) -> bool:
        return self._parser.evaluate_predicate_on_partition(path, self._predicate)
