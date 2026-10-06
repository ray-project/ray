import logging
from dataclasses import dataclass, field, replace
from typing import Literal, Optional, Set, Union

import pyarrow as pa
from typing_extensions import override

from ray.data._internal.datasource_v2.common.file_pruners import (
    PartitionPredicatePruner,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.datasource_v2.interfaces.file_pruner import FilePruner
from ray.data._internal.datasource_v2.interfaces.pushdown import (
    SupportsPartitionPruning,
)
from ray.data._internal.datasource_v2.interfaces.scanner import Scanner
from ray.data.datasource.file_based_datasource import (
    FileShuffleConfig,
    _validate_shuffle_arg,
)
from ray.data.datasource.partitioning import Partitioning, PathPartitionParser
from ray.data.expressions import Expr
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(__name__)


@DeveloperAPI
@dataclass(frozen=True)
class FileScanner(Scanner[FileManifest], SupportsPartitionPruning):
    """Base scanner for file-based datasources.

    Subclasses implement format-specific ``read_schema()`` and
    ``create_reader()``. File shuffling is handled by the ``FileIndexer``
    after path discovery and before metadata fetch; parallel bucketing is
    handled upstream in the ``ListFiles`` transform chain
    (``RoundRobinPartitioner`` via ``plan_list_files_op``), not here.

    Partition pruning is implemented here for every file format: partition
    values come from file paths and the ``Partitioning`` spec, never from the
    file contents, so a predicate on a partition column drops whole files
    before any of them is opened. It is a no-op while ``partitioning`` is
    ``None``.

    PyArrow Dataset-based scanners should subclass ``ArrowFileScanner``;
    use ``FileScanner`` directly for non-Arrow file formats.
    """

    # kw_only so subclass dataclasses can declare their own required fields
    # (like ``ArrowFileScanner.schema``) without running into the "non-default
    # argument follows default argument" dataclass inheritance rule.
    shuffle: Union[Literal["files"], FileShuffleConfig, None] = field(
        default=None, kw_only=True
    )
    partitioning: Optional[Partitioning] = field(default=None, kw_only=True)
    partition_predicate: Optional[Expr] = field(default=None, kw_only=True)

    def __post_init__(self) -> None:
        _validate_shuffle_arg(self.shuffle)

    @property
    def partition_columns(self) -> Set[str]:
        """Return the set of partition column names, or empty if unpartitioned."""
        if self.partitioning is None:
            return set()
        return set(self.partitioning.field_names or [])

    @override
    def prune_partitions(self, predicate: "Expr") -> "FileScanner":
        """Store a partition predicate for file-level pruning at read time.

        The predicate is ANDed with any existing partition predicate. Actual
        file pruning happens in :meth:`prune_input_split` when the manifest is
        available, using :class:`PathPartitionParser` to evaluate partition
        values from file paths.

        Args:
            predicate: Expression referencing only partition columns.

        Returns:
            New scanner with partition predicate stored.
        """
        if self.partition_predicate is not None:
            combined = self.partition_predicate & predicate
        else:
            combined = predicate

        return replace(self, partition_predicate=combined)

    @override
    def pushed_partition_predicate(self) -> Optional["Expr"]:
        return self.partition_predicate

    @override
    def pushed_partition_pruner(self) -> Optional["FilePruner"]:
        if self.partition_predicate is None or self.partitioning is None:
            # No spec, no partition values -- same guard as ``prune_input_split``.
            return None
        return PartitionPredicatePruner(self.partitioning, self.partition_predicate)

    @override
    def prune_input_split(self, input_split: FileManifest) -> FileManifest:
        """Keep only the files matching ``self.partition_predicate``.

        No-op when either the predicate or the partitioning spec is absent.
        Partition values are parsed out of each file path by
        :class:`PathPartitionParser`.
        """
        if self.partition_predicate is None or self.partitioning is None:
            return input_split

        parser = PathPartitionParser(self.partitioning)
        keep_indices = []

        for i, path in enumerate(input_split.paths):
            if parser.evaluate_predicate_on_partition(path, self.partition_predicate):
                keep_indices.append(i)

        if len(keep_indices) == len(input_split):
            return input_split

        pruned_count = len(input_split) - len(keep_indices)
        logger.debug(
            "Partition pruning removed %d of %d files",
            pruned_count,
            len(input_split),
        )

        block = input_split.as_block()
        # An untyped empty list infers null indices: ArrowNotImplementedError.
        pruned_block = block.take(pa.array(keep_indices, type=pa.int64()))
        return FileManifest(pruned_block)
