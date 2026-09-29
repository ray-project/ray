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
from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from pyarrow.fs import FileSystem

    from ray.data._internal.datasource_v2.partitioners.file_partitioner import (
        FilePartitioner,
        PartitionHints,
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

    The entry point for reading data from a source. It provides:

    1. File listing, via ``_get_file_indexer()``
    2. Schema inference
    3. Read-task grouping, via ``get_file_partitioner()``
    4. Scanner creation

    Do not extend this class directly. Every datasource extends one of its two
    subclasses, and the read path rejects anything else:

    - :class:`FileDataSourceV2` when the framework finds the files by walking a
      filesystem (Parquet, CSV, JSON, images).
    - :class:`DataSourceWithMetadata` when the source finds its own data through a
      catalog, table metadata or a database (Iceberg, Delta, Hudi, Lance, SQL).

    Implementing the abstract members is enough for a new source to work end
    to end. ``resolve_partitioning()`` has a default and is optional to
    override.

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

    @abstractmethod
    def _get_file_indexer(self) -> FileIndexer:
        """Indexer that ``ListFiles`` runs to turn :attr:`paths` into
        ``FileManifest`` blocks.

        Abstract rather than defaulted, because a default would commit a new
        format to per-file listing without anyone choosing it. Formats without
        usable file metadata return ``NonSamplingFileIndexer``; Parquet returns
        ``FooterFileIndexer``.
        """
        ...

    @abstractmethod
    def get_file_partitioner(
        self, *, hints: Optional["PartitionHints"] = None
    ) -> Optional["FilePartitioner"]:
        """Partitioner that groups this source's listing rows into read units.

        Each listing task holds its own pickled copy of the partitioner, so
        anything it carries (such as an ``InMemorySizeEstimator``) must pickle
        cheaply; any I/O the estimator does runs once per listing task.
        ``RoundRobinPartitioner(estimator, hints=hints)`` fits most formats;
        return something else when the listing rows carry metadata worth
        grouping on (Parquet row-group stats), and ``None`` to emit each
        listing block as one read unit.

        Args:
            hints: Sizing hints derived from ``DataContext`` and
                ``override_num_blocks``, always passed by keyword. Optional
                so an override that ignores them can take ``**kwargs``.

        Returns:
            The partitioner, or ``None`` to emit listing blocks unchanged.
        """
        ...

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
            filesystem: :attr:`FileDataSourceV2.filesystem`, or ``None`` for
                a :class:`DataSourceWithMetadata`.
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


@DeveloperAPI
class FileDataSourceV2(DataSourceV2[FileManifest]):
    """Base class for sources whose files the framework finds itself.

    ``ListFiles`` walks :attr:`paths` through :attr:`filesystem`, keeps the
    files matching :attr:`file_extensions` and applies :attr:`shuffle` to the
    listing. Parquet, CSV and every other plain file format belong here; a
    source that finds its own data extends :class:`DataSourceWithMetadata` instead.
    """

    @property
    @abstractmethod
    def filesystem(self) -> "FileSystem":
        """PyArrow filesystem the indexer and scanner list and read through.

        Resolve it in ``__init__`` together with :attr:`paths`, see
        ``_resolve_paths_and_filesystem``.
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


@DeveloperAPI
class DataSourceWithMetadata(DataSourceV2[InputSplit]):
    """Base class for sources that find their own data.

    The indexer from :meth:`_get_file_indexer` asks a catalog, a table format's
    metadata or a database what to read, so the framework has no filesystem to
    walk and passes ``None`` wherever a :class:`FileDataSourceV2` supplies one.
    Iceberg, Delta, Hudi, Lance and SQL sources belong here.

    :attr:`paths` stays abstract and is usually one label such as
    ``"iceberg://db.table"``, handed to the indexer unread.
    """

    @property
    def schema_needs_file_sample(self) -> bool:
        """``False``: the schema comes from the same metadata as the listing,
        not from opening a data file. Override if a format needs the sample.
        """
        return False
