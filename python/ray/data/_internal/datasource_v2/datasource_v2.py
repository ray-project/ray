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
    Optional,
)

import pyarrow as pa

from ray.data._internal.datasource_v2 import InputSplit
from ray.data._internal.datasource_v2.listing.file_indexer import FileIndexer
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from pyarrow.fs import FileSystem

    from ray.data._internal.datasource_v2.readers.in_memory_size_estimator import (
        InMemorySizeEstimator,
    )
    from ray.data._internal.datasource_v2.scanners.scanner import Scanner


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

    DataSourceV2 is the entry point for reading data from a source. It provides:
    1. File listing (for file-based sources) - via _get_file_indexer()
    2. Schema inference
    3. Size estimation
    4. Scanner creation

    Subclasses should implement the abstract methods and can optionally
    override _get_file_indexer() and get_size_estimator() for file-based sources.

    Example::

        datasource = ParquetDatasourceV2()
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

    @property
    def name(self) -> str:
        """Human-readable name for this datasource."""
        return self._name

    @property
    def category(self) -> DatasourceCategory:
        """Category of this datasource."""
        return self._category

    def _get_file_indexer(self) -> Optional[FileIndexer]:
        """Return FileIndexer component if applicable.

        Override this for file-based datasources to provide file discovery.

        Returns:
            FileIndexer instance, or None for non-file-based sources.
        """
        return None

    def get_size_estimator(self) -> Optional[InMemorySizeEstimator]:
        """Return size estimator for this datasource.

        Override this to provide format-specific size estimation.

        Returns:
            InMemorySizeEstimator instance, or None if not supported.
        """
        return None

    @abstractmethod
    def infer_schema(self, sample: InputSplit) -> pa.Schema:
        """Infer schema from a sample of data.

        Args:
            sample: Sample data to infer schema from.

        Returns:
            PyArrow Schema inferred from the sample.

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
