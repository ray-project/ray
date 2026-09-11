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
    """Skip files based on partition column predicates (e.g., hive partitioning).

    Backs the ``partition_filter=`` argument of ``read_parquet`` and friends: a
    user-supplied callback over the partition values parsed from a path.
    Listing is the only place it is applied -- the reader has no counterpart --
    so there is nothing downstream for it to agree with. Contrast
    :class:`PartitionPredicatePruner`, which mirrors a filter the reader
    applies too.
    """

    def __init__(self, partition_filter: PathPartitionFilter):
        self._filter = partition_filter

    def should_include(self, path: str) -> bool:
        return self._filter.apply(path)


class PartitionPredicatePruner(FilePruner):
    """Skip files whose partition values fail a pushed-down predicate.

    Backs ``ds.filter(expr=...)`` on a partition column, once the optimizer has
    pushed that expression onto the scanner -- not something a caller
    constructs. Unlike :class:`PartitionPruner`, it duplicates pruning the
    reader also performs (``ArrowFileScanner.prune_manifest``), so the two must
    drop exactly the same files: a pushed-down limit stops listing early, and
    every row listing counted towards it has to belong to a file the reader
    keeps. Sharing
    :meth:`PathPartitionParser.evaluate_predicate_on_partition` with
    ``prune_manifest`` is what holds that equality.

    The two are therefore not interchangeable. A ``PathPartitionFilter`` is an
    opaque callable carrying no such obligation, and expressing a pushed-down
    ``Expr`` as one would parse every path twice and hide the shared evaluator
    behind a lambda.
    """

    def __init__(self, partitioning: "Partitioning", predicate: "Expr"):
        self._parser = PathPartitionParser(partitioning)
        self._predicate = predicate

    def should_include(self, path: str) -> bool:
        return self._parser.evaluate_predicate_on_partition(path, self._predicate)
