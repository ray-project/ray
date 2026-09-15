"""
DataSourceV2 API - Unified Abstraction for Reading Data Sources

This module defines a unified, extensible API for reading data from diverse sources
in Ray Data. The API provides a common abstraction layer that enables datasources to
declaratively expose their capabilities—such as filter pushdown, projection pruning,
and parallel reads—while allowing the execution engine to leverage these capabilities
transparently.

Core Principles:
- Modularity: Separate concerns (indexing, scanning, reading)
- Expressivity: Declarative capability exposure via mixins
- Extensibility: Easy to add new datasources with custom optimizations
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    List,
    Literal,
    Optional,
    Union,
)

import pyarrow as pa

from ray.data._internal.datasource_v2 import InputSplit
from ray.data._internal.datasource_v2.listing.file_indexer import FileIndexer
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from pyarrow.fs import FileSystem

    from ray.data._internal.datasource_v2.partitioners.file_partitioner import (
        FilePartitioner,
    )
    from ray.data._internal.datasource_v2.readers.in_memory_size_estimator import (
        InMemorySizeEstimator,
    )
    from ray.data._internal.datasource_v2.scanners.scanner import Scanner
    from ray.data.datasource.file_based_datasource import FileShuffleConfig


@DeveloperAPI
class DatasourceCategory(Enum):
    """Categories of datasources with different capability profiles.

    Each category has a distinct set of applicable optimizations:
    - FILE_BASED: Local/cloud files (parquet, csv, json, images)
    - DATABASE: SQL databases (postgres, mysql, snowflake)
    - DATA_LAKE: Table formats (iceberg, delta, hudi)
    - IN_MEMORY: In-process data (pandas, numpy, arrow)
    - SYNTHETIC: Generated data (range, range_tensor)
    - STREAMING: Unbounded sources (kafka, kinesis)
    """

    FILE_BASED = "file_based"
    DATABASE = "database"
    DATA_LAKE = "data_lake"
    IN_MEMORY = "in_memory"
    SYNTHETIC = "synthetic"
    STREAMING = "streaming"


@DeveloperAPI
class DataSourceV2(ABC, Generic[InputSplit]):
    """Abstract base class for V2 datasources.

    The framework touches a datasource in exactly one place,
    ``ray.data.read_api._read_datasource_v2``, which builds the
    ``ListFiles -> ReadFiles`` plan and then drops it. Every attribute that
    function reads is declared here, so implementing the abstract members is
    enough to work end to end.

    That function always builds a ``ListFiles`` op, which is why ``paths``,
    ``filesystem`` and ``_get_file_indexer`` live here. A future non-file
    source (a database scan, say) moves them into a ``FileDataSourceV2``
    subclass.

    Example::

        datasource = ParquetDatasourceV2(paths)
        indexer = datasource._get_file_indexer()
        # List files with optional sampling
        for manifest in indexer.list_files(paths, filesystem=fs):
            schema = datasource.infer_schema(manifest)
            break  # Just need first manifest for schema
        scanner = datasource.create_scanner(schema)
        scanner = scanner.prune_columns(["col1", "col2"])
        reader = scanner.create_reader()
        for table in reader.read(manifest):
            process(table)
    """

    def __init__(self, name: str, category: DatasourceCategory):
        """Initialize the datasource.

        Args:
            name: Human-readable name for this datasource.
            category: Category of this datasource.
        """
        self._name = name
        self._category = category
        # File-based subclasses set this to ``False`` in their ``__init__``
        # when the user-supplied paths are in the ``local://`` scheme —
        # the driver node is the only one that can read those files.
        # ``_read_datasource_v2`` consults the flag to decide whether to
        # pin read tasks via a ``label_selector``.
        self._supports_distributed_reads: bool = True

    @property
    def name(self) -> str:
        """Human-readable name for this datasource."""
        return self._name

    @property
    def category(self) -> DatasourceCategory:
        """Category of this datasource."""
        return self._category

    @property
    def supports_distributed_reads(self) -> bool:
        """Whether read tasks may run on any cluster node.

        Defaults to ``True``. File-based subclasses (e.g.
        :class:`ParquetDatasourceV2`) flip this to ``False`` when the
        user supplies ``local://``-scheme paths so ``_read_datasource_v2``
        can pin reads to the driver node via a ``ray.io/node-id``
        label selector. Mirrors V1 ``Datasource.supports_distributed_reads``.
        """
        return self._supports_distributed_reads

    @property
    @abstractmethod
    def paths(self) -> List[str]:
        """Listing inputs, one ``ListFiles`` task each, passed to the indexer
        unread -- the framework never interprets them.

        File sources return the roots to walk. A catalog- or engine-backed
        source, whose indexer already knows what to read, returns a single
        identifier label. Must be non-empty, or ``ListFiles`` schedules no task
        and the read is silently empty.
        """
        ...

    @property
    @abstractmethod
    def filesystem(self) -> Optional["FileSystem"]:
        """PyArrow filesystem the indexer and scanner read through, or ``None``
        when they do their own IO.

        The framework only forwards it and never dereferences it, so whether
        ``None`` is acceptable is up to the components this datasource returns.
        The stock indexers require one -- resolve it in ``__init__``, see
        ``_resolve_paths_and_filesystem``. A source read through its own library
        (PyIceberg, Lance, hudi-rs) returns ``None``.
        """
        ...

    @property
    def file_extensions(self) -> Optional[List[str]]:
        """File extensions to keep while listing; ``None`` keeps every file."""
        return None

    @property
    def shuffle(self) -> Optional[Union[Literal["files"], "FileShuffleConfig"]]:
        """File-level shuffle the user asked for; ``None`` means no shuffle.

        ``"files"`` shuffles with a seed drawn per execution; a
        :class:`FileShuffleConfig` pins the seed.
        """
        return None

    @abstractmethod
    def _get_file_indexer(self) -> FileIndexer:
        """Indexer that ``ListFiles`` runs to turn :attr:`paths` into
        ``FileManifest`` blocks.

        Abstract rather than defaulted, because a default would commit a new
        format to whole-file chunking without anyone choosing it. Formats
        without usable file metadata return ``NonSamplingFileIndexer``; Parquet
        returns ``FooterFileIndexer``.
        """
        ...

    def get_file_partitioner(self, **kwargs) -> Optional["FilePartitioner"]:
        """Partitioner that groups this source's listing rows into read units.

        Defaults to the size-estimating ``RoundRobinPartitioner``. Override when
        the indexer emits rows carrying richer metadata (e.g. Parquet row-group
        stats) that a different grouping strategy can exploit.
        """
        from ray.data._internal.datasource_v2.partitioners.round_robin_partitioner import (  # noqa: E501
            RoundRobinPartitioner,
        )

        return RoundRobinPartitioner(**kwargs)

    def get_size_estimator(self) -> Optional[InMemorySizeEstimator]:
        """Return size estimator for this datasource.

        Override this to provide format-specific size estimation.

        Returns:
            InMemorySizeEstimator instance, or None if not supported.
        """
        return None

    @property
    @abstractmethod
    def schema_needs_file_sample(self) -> bool:
        """Whether :meth:`infer_schema` needs data files to look at.

        ``True`` for a plain file format: the caller lists a sample of files
        and passes it to :meth:`infer_schema`. ``False`` when the schema comes
        from somewhere else -- a catalog, a config -- and :meth:`infer_schema`
        is called with ``None`` instead. Answering ``False`` also skips the
        "no files found" error, so a table that is empty but declared still
        reads, and skips discovering partition fields from path names.

        Abstract rather than defaulted so that adding a datasource forces the
        question.
        """
        ...

    @abstractmethod
    def infer_schema(self, sample: Optional[InputSplit]) -> pa.Schema:
        """Infer schema from a sample of data.

        Args:
            sample: Sample data to infer schema from, or ``None`` when
                :attr:`schema_needs_file_sample` is ``False``.

        Returns:
            PyArrow Schema inferred from the sample. This is the dataset
            schema; do not pre-apply column pruning, which is pushed down
            later and applied by ``Scanner.read_schema``.

        Raises:
            ValueError: If schema cannot be inferred from the sample.
        """
        ...

    @abstractmethod
    def create_scanner(
        self,
        schema: pa.Schema,
        filesystem: Optional["FileSystem"] = None,
        **options: Any,
    ) -> Scanner[InputSplit]:
        """Create a Scanner for reading data.

        Args:
            schema: Schema for the data to read.
            filesystem: Optional filesystem for file-based sources.
            **options: Additional datasource-specific options.

        Returns:
            Configured Scanner instance.
        """
        ...

    def resolve_partitioning(self, sample: Optional[InputSplit]) -> Optional[Any]:
        """Return a partitioning descriptor derived from ``sample``, or ``None``.

        Override this for file-based sources whose partition keys must be
        discovered from a sample path (e.g. hive layouts where field names
        are not known up front). The resolved descriptor is passed into
        :meth:`create_scanner`.

        ``sample`` is ``None`` when :attr:`schema_needs_file_sample` is
        ``False``; an override must then return ``None`` too, since there is no
        path to read keys from.
        """
        return None
