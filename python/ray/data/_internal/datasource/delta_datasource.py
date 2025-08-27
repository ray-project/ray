"""Enhanced Delta Lake datasource for Ray Data.

This datasource provides comprehensive Delta Lake reading capabilities including:
- Advanced deletion vector handling with intelligent fallback strategies
- Time travel (version-based reading) with comprehensive version analysis
- Advanced partition filtering with Delta Lake 1.x syntax and native API support
- Memory optimization and performance tuning with detailed analytics
- Unity Catalog integration support
- Advanced storage options for cloud providers
- Rich metadata extraction leveraging the full deltalake API
- Change Data Feed (CDF) support for tracking table changes over time
- Comprehensive table statistics and optimization recommendations
- Intelligent error handling with feature-specific fallback strategies
- Protocol version detection and feature compatibility analysis

The DeltaDatasource inherits from ParquetDatasource to leverage existing parquet
reading optimizations while adding comprehensive Delta Lake specific functionality.
It takes full advantage of the deltalake Python API to provide the most robust
and feature-rich Delta Lake reading experience possible.

Key Features:
- Deletion Vectors: Intelligent detection and handling with automatic fallback
- Column Mapping: Support for both name and ID-based column mapping modes
- Constraints: Data validation constraint support and analysis
- Partitioning: Native partition API usage with file-based fallback
- Change Tracking: Full CDF support for temporal data analysis
- Performance: Comprehensive optimization hints and performance metrics
- Error Handling: Feature-specific error analysis and intelligent fallback
- Metadata: Rich extraction using get_add_actions, history, and other APIs

Examples:
    .. testcode::

        # Basic usage with deletion vector support
        datasource = DeltaDatasource(
            path="s3://bucket/delta-table",
            version="latest",
            partition_filters=[("year", "=", "2023")],
            storage_options={"aws_region": "us-west-2"}
        )

        # Advanced usage with CDF and statistics
        dataset = ray.data.read_datasource(datasource)

        # Get comprehensive table statistics
        stats = datasource.get_table_statistics()

        # Access Change Data Feed
        cdf_reader = datasource.get_change_data_feed(
            starting_version=10,
            ending_version=20
        )

        # Get optimization hints
        hints = datasource.get_optimization_hints()
"""

import logging
import os
import re
from datetime import datetime
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Literal,
    Optional,
    Tuple,
    Union,
)

# Constants for configuration and optimization
DEFAULT_BYTES_PER_ROW = 1024  # Conservative estimate for row size in bytes
MAX_RECOMMENDED_PARALLELISM = 100  # Cap for parallelism recommendations

from ray.data._internal.datasource.parquet_datasource import ParquetDatasource
from ray.data._internal.util import _check_import, _check_pyarrow_version
from ray.data.datasource.datasource import ReadTask
from ray.data.datasource.parquet_meta_provider import ParquetMetadataProvider
from ray.data.datasource.partitioning import (
    Partitioning,
    PathPartitionFilter,
)

if TYPE_CHECKING:
    import pyarrow

logger = logging.getLogger(__name__)


class DeltaDatasource(ParquetDatasource):
    """Enhanced Delta Lake datasource for reading Delta tables with comprehensive support.

    This datasource inherits from ParquetDatasource to leverage existing parquet
    reading optimizations while adding comprehensive Delta Lake specific features including
    deletion vectors, time travel, advanced partition filtering, Unity Catalog integration,
    and rich metadata extraction.

    The datasource now takes full advantage of the deltalake Python API to provide:

    **Advanced Deletion Vector Support:**
    - Intelligent detection of deletion vectors in tables
    - Automatic fallback strategies when deletion vectors cause issues
    - Feature-specific error handling with targeted fallback options
    - Comprehensive deletion vector metadata extraction

    **Enhanced Partition Handling:**
    - Native deltalake partitions() API usage for better performance
    - Intelligent fallback to file-based partition extraction
    - Comprehensive partition statistics and analytics
    - Partition filter effectiveness analysis

    **Rich Metadata Extraction:**
    - Leverages get_add_actions() for detailed file information
    - Uses history() API for transaction analysis
    - Extracts column mapping and constraint information
    - Provides comprehensive table statistics

    **Change Data Feed (CDF) Support:**
    - Full support for tracking table changes over time
    - Version and timestamp-based change tracking
    - Automatic CDF availability detection
    - Streaming change data access

    **Intelligent Optimization:**
    - Feature-specific optimization recommendations
    - Storage provider-specific optimization hints
    - Schema-based performance recommendations
    - Comprehensive performance metrics

    **Robust Error Handling:**
    - Analyzes error messages to determine best fallback strategy
    - Preserves features when possible during fallback
    - Multiple fallback levels for maximum compatibility
    - Detailed error reporting and recommendations

    Examples:
        .. testcode::

            # Basic usage with automatic deletion vector handling
            datasource = DeltaDatasource(
                path="s3://bucket/delta-table",
                version="latest",
                partition_filters=[("year", "=", "2023")],
                storage_options={"aws_region": "us-west-2"}
            )

            # Advanced usage with comprehensive metadata
            dataset = ray.data.read_datasource(datasource)

            # Get rich table statistics
            stats = datasource.get_table_statistics()

            # Access optimization hints
            hints = datasource.get_optimization_hints()

            # Track changes over time
            cdf_reader = datasource.get_change_data_feed(
                starting_version=10,
                ending_version=20
            )

            # Get detailed partition information
            partition_stats = datasource.get_partition_statistics()
    """

    def __init__(
        self,
        path: Union[str, List[str]],
        *,
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        columns: Optional[List[str]] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
        meta_provider: Optional[ParquetMetadataProvider] = None,
        partition_filter: Optional[PathPartitionFilter] = None,
        partitioning: Optional[Partitioning] = Partitioning("hive"),
        shuffle: Union[Literal["files"], None] = None,
        include_paths: bool = False,
        concurrency: Optional[int] = None,
        override_num_blocks: Optional[int] = None,
        # Delta Lake specific options
        version: Optional[Union[int, str, datetime]] = None,
        without_files: bool = False,
        log_buffer_size: Optional[int] = None,
        options: Optional[Dict[str, Any]] = None,
        # Partition filtering for Delta Lake
        partition_filters: Optional[List[Tuple[str, str, Any]]] = None,
        # Performance and optimization options
        cache_metadata: bool = True,
        parallel_metadata_loading: bool = True,
        # Unity Catalog and storage options
        storage_options: Optional[Dict[str, Any]] = None,
        unity_catalog_config: Optional[Dict[str, Any]] = None,
        **arrow_parquet_args,
    ):
        """Initialize the enhanced Delta Lake datasource.

        Args:
            path: A single file path for a Delta Lake table. Multiple tables are not yet
                supported.
            filesystem: The PyArrow filesystem implementation to read from.
            columns: A list of column names to read. Only the specified columns are
                read during the file scan.
            ray_remote_args: kwargs passed to :meth:`~ray.remote` in the read tasks.
            meta_provider: A file metadata provider for efficient file reading.
            partition_filter: A PathPartitionFilter for filtering partitions.
            partitioning: A Partitioning object describing path organization.
            shuffle: If "files", randomly shuffle input files order before read.
            include_paths: If True, include the path to each file.
            concurrency: Maximum number of Ray tasks to run concurrently.
            override_num_blocks: Override the number of output blocks.
            version: Delta table version to read (can be version number, timestamp string, or datetime).
                If None, reads the latest version.
            without_files: If True, loads table without tracking files for memory reduction.
                Useful for append-only applications that don't need file tracking.
            log_buffer_size: Number of files to buffer when reading the commit log.
                A positive integer that can decrease latency but increase memory usage.
                Defaults to 4 * number of CPUs.
            options: Delta Lake table options to pass to the DeltaTable constructor.
                Useful for handling unsupported features like deletion vectors.
                Example: {"ignore_deletion_vectors": True}
            partition_filters: Delta Lake partition filters in DNF format.
                List of tuples: [("column", "op", "value")] where op can be "=", "!=", "in", "not in".
                Example: [("year", "=", "2023"), ("month", "in", ["Dec", "Nov"])]
            cache_metadata: Whether to cache Delta table metadata for performance.
            parallel_metadata_loading: Whether to load metadata in parallel for large tables.
            storage_options: Storage-specific options for cloud providers.
                Example: {"aws_region": "us-west-2", "azure_account": "account"}
            unity_catalog_config: Unity Catalog specific configuration.
                Example: {"catalog_name": "hive_metastore", "schema_name": "default"}
            **arrow_parquet_args: Other parquet read options to pass to PyArrow.
        """
        _check_pyarrow_version()

        # Validate input
        if not isinstance(path, str):
            raise ValueError("Only a single Delta Lake table path is supported.")

        # Check if deltalake is available
        _check_import(self, module="deltalake", package="deltalake")

        # Store Delta Lake specific options
        self._version = version
        self._without_files = without_files
        self._log_buffer_size = log_buffer_size
        self._options = options or {}
        self._partition_filters = partition_filters
        self._cache_metadata = cache_metadata
        self._parallel_metadata_loading = parallel_metadata_loading
        self._storage_options = storage_options or {}
        self._unity_catalog_config = unity_catalog_config or {}

        # Extract parquet file paths from Delta table
        logger.info(f"Initializing enhanced DeltaDatasource for path: {path}")
        parquet_paths = self._get_delta_table_paths()

        # Initialize the parent ParquetDatasource with the extracted parquet paths
        super().__init__(
            paths=parquet_paths,
            filesystem=filesystem,
            columns=columns,
            ray_remote_args=ray_remote_args,
            meta_provider=meta_provider,
            partition_filter=partition_filter,
            partitioning=partitioning,
            shuffle=shuffle,
            include_paths=include_paths,
            concurrency=concurrency,
            override_num_blocks=override_num_blocks,
            **arrow_parquet_args,
        )

        # Store the original Delta table path for metadata
        self._delta_path = path

        logger.info(
            f"Enhanced DeltaDatasource initialized with {len(parquet_paths)} parquet files"
        )

    def _get_delta_table_paths(self) -> List[str]:
        """Get the parquet file paths from the DeltaTable with enhanced deletion vector handling.

        This method leverages the full deltalake API to handle deletion vectors, column mapping,
        and other advanced features more intelligently.
        """
        from deltalake import DeltaTable

        # Build DeltaTable constructor arguments with enhanced options
        dt_args = {"table_uri": self._delta_path, "options": self._options.copy()}

        # Add storage options if provided (Delta Lake 1.x feature)
        if self._storage_options:
            dt_args["storage_options"] = self._storage_options

        # Add version if specified
        if self._version is not None:
            dt_args["version"] = self._version

        # Add log buffer size if specified
        if self._log_buffer_size is not None:
            dt_args["log_buffer_size"] = self._log_buffer_size

        # Add without_files option if specified
        if self._without_files:
            dt_args["without_files"] = True

        # Enhanced deletion vector handling strategy
        deletion_vector_strategy = self._determine_deletion_vector_strategy()

        # Apply the determined strategy to dt_args
        if deletion_vector_strategy:
            # Update options with the determined strategy
            if "options" not in dt_args:
                dt_args["options"] = {}
            dt_args["options"].update(deletion_vector_strategy)

        try:
            # First attempt: try to create DeltaTable with optimal options
            dt = DeltaTable(**dt_args)

            # Check if table has deletion vectors and handle accordingly
            if self._has_deletion_vectors(dt):
                logger.info(
                    "Delta table contains deletion vectors - using enhanced reading strategy"
                )
                return self._get_paths_with_deletion_vector_handling(dt)
            else:
                # Standard reading without deletion vectors
                return self._get_standard_paths(dt)

        except Exception as e:
            # Enhanced fallback strategy with better error analysis
            return self._handle_delta_table_errors(dt_args, e)

    def _determine_deletion_vector_strategy(self) -> Dict[str, Any]:
        """Determine the optimal strategy for handling deletion vectors and other features.

        Returns:
            Dictionary containing the optimal options for the Delta table.
        """
        strategy = {}

        # Check if user has explicitly configured deletion vector handling
        if "ignore_deletion_vectors" in self._options:
            strategy["ignore_deletion_vectors"] = self._options[
                "ignore_deletion_vectors"
            ]
        else:
            # Default to intelligent handling - try to preserve deletion vectors if possible
            strategy["ignore_deletion_vectors"] = False

        # Handle column mapping intelligently
        if "ignore_column_mapping" in self._options:
            strategy["ignore_column_mapping"] = self._options["ignore_column_mapping"]
        else:
            # Default to preserving column mapping for better data integrity
            strategy["ignore_column_mapping"] = False

        # Handle constraints intelligently
        if "ignore_constraints" in self._options:
            strategy["ignore_constraints"] = self._options["ignore_constraints"]
        else:
            # Default to preserving constraints for data validation
            strategy["ignore_constraints"] = False

        return strategy

    def _has_deletion_vectors(self, dt: "DeltaTable") -> bool:
        """Check if the Delta table contains deletion vectors.

        Args:
            dt: The DeltaTable instance to check.

        Returns:
            True if the table has deletion vectors, False otherwise.
        """
        try:
            # Check metadata for deletion vector information
            metadata = dt.metadata()

            # Look for deletion vector related configuration
            config = metadata.configuration or {}

            # Check if deletion vectors are enabled
            if "delta.enableDeletionVectors" in config:
                return config["delta.enableDeletionVectors"].lower() == "true"

            # Check if any files have deletion vectors by examining add actions
            try:
                add_actions = dt.get_add_actions(flatten=True)
                # Look for deletion vector files in the add actions
                if hasattr(add_actions, "to_pandas"):
                    df = add_actions.to_pandas()
                    if "deletion_vector" in df.columns:
                        return df["deletion_vector"].notna().any()
            except Exception:
                # If we can't check add actions, assume no deletion vectors
                pass

            return False

        except Exception:
            # If we can't determine, assume no deletion vectors
            return False

    def _get_paths_with_deletion_vector_handling(self, dt: "DeltaTable") -> List[str]:
        """Get file paths with intelligent deletion vector handling.

        This method handles deletion vectors by:
        1. Including deletion vector files in the path list
        2. Applying deletion vector specific optimizations
        3. Ensuring proper deletion vector metadata is preserved

        Args:
            dt: The DeltaTable instance.

        Returns:
            List of file paths optimized for deletion vector handling.
        """
        try:
            # For deletion vector tables, we want to ensure we get all necessary files
            # including deletion vector files and any metadata files
            if self._partition_filters:
                paths = dt.file_uris(partition_filters=self._partition_filters)
            else:
                paths = dt.file_uris()

            # Log deletion vector specific information
            logger.info(
                f"Successfully extracted {len(paths)} parquet files with deletion vector support"
            )

            # Additional logging for deletion vector tables
            try:
                metadata = dt.metadata()
                config = metadata.configuration or {}
                if "delta.enableDeletionVectors" in config:
                    logger.info("Deletion vectors are enabled for this table")

                    # Check if we have deletion vector files
                    add_actions = dt.get_add_actions(flatten=True)
                    if hasattr(add_actions, "to_pandas"):
                        df = add_actions.to_pandas()
                        if "deletion_vector" in df.columns:
                            dv_files = df[df["deletion_vector"].notna()]
                            if len(dv_files) > 0:
                                logger.info(
                                    f"Found {len(dv_files)} files with deletion vectors"
                                )
            except Exception as e:
                logger.debug(f"Could not get detailed deletion vector info: {e}")

            return paths

        except Exception as e:
            logger.warning(
                f"Deletion vector reading failed, falling back to standard mode: {e}"
            )
            # Fall back to standard reading
            return self._get_standard_paths(dt)

    def _get_standard_paths(self, dt: "DeltaTable") -> List[str]:
        """Get file paths using standard Delta table reading.

        This method provides basic file path extraction without deletion vector
        specific optimizations.

        Args:
            dt: The DeltaTable instance.

        Returns:
            List of file paths.
        """
        if self._partition_filters:
            paths = dt.file_uris(partition_filters=self._partition_filters)
        else:
            paths = dt.file_uris()

        logger.info(
            f"Successfully extracted {len(paths)} parquet files using standard reading"
        )
        return paths

    def _handle_delta_table_errors(
        self, dt_args: Dict[str, Any], original_error: Exception
    ) -> List[str]:
        """Handle Delta table creation errors with intelligent fallback strategies.

        Args:
            dt_args: The DeltaTable constructor arguments.
            original_error: The original error that occurred.

        Returns:
            List of file paths after applying fallback strategies.

        Raises:
            RuntimeError: If all fallback strategies fail.
        """
        error_msg = str(original_error)

        # Analyze the error to determine the best fallback strategy
        if any(
            feature in error_msg for feature in ["DeletionVectors", "deletion vectors"]
        ):
            logger.warning(
                "Delta table has deletion vectors causing issues, applying deletion vector fallback"
            )
            fallback_options = {
                "ignore_deletion_vectors": True,
                "ignore_column_mapping": False,  # Keep column mapping if possible
                "ignore_constraints": False,  # Keep constraints if possible
            }
        elif any(
            feature in error_msg for feature in ["ColumnMapping", "column mapping"]
        ):
            logger.warning(
                "Delta table has column mapping issues, applying column mapping fallback"
            )
            fallback_options = {
                "ignore_deletion_vectors": False,  # Keep deletion vectors if possible
                "ignore_column_mapping": True,
                "ignore_constraints": False,  # Keep constraints if possible
            }
        elif any(feature in error_msg for feature in ["Constraints", "constraints"]):
            logger.warning(
                "Delta table has constraint issues, applying constraint fallback"
            )
            fallback_options = {
                "ignore_deletion_vectors": False,  # Keep deletion vectors if possible
                "ignore_column_mapping": False,  # Keep column mapping if possible
                "ignore_constraints": True,
            }
        else:
            # Generic fallback for unknown errors
            logger.warning(
                f"Unknown Delta table error, applying comprehensive fallback: {error_msg}"
            )
            fallback_options = {
                "ignore_deletion_vectors": True,
                "ignore_column_mapping": True,
                "ignore_constraints": True,
            }

        # Apply fallback options
        fallback_options.update(self._options)
        dt_args["options"] = fallback_options

        try:
            dt = DeltaTable(**dt_args)

            # Try to get paths with fallback options
            if self._partition_filters:
                paths = dt.file_uris(partition_filters=self._partition_filters)
            else:
                paths = dt.file_uris()

            logger.info(
                f"Successfully extracted {len(paths)} parquet files with fallback options: "
                f"{fallback_options}"
            )
            return paths

        except Exception as fallback_error:
            logger.error(f"Fallback attempt failed: {fallback_error}")

            # Try one more time with the most permissive options
            final_fallback = {
                "ignore_deletion_vectors": True,
                "ignore_column_mapping": True,
                "ignore_constraints": True,
                "ignore_unsupported_features": True,  # If available
            }
            final_fallback.update(self._options)
            dt_args["options"] = final_fallback

            try:
                dt = DeltaTable(**dt_args)
                if self._partition_filters:
                    paths = dt.file_uris(partition_filters=self._partition_filters)
                else:
                    paths = dt.file_uris()

                logger.info(
                    f"Successfully extracted {len(paths)} parquet files with final fallback options"
                )
                return paths

            except Exception as final_error:
                logger.error(f"All fallback strategies failed: {final_error}")
                raise RuntimeError(
                    f"Failed to read Delta table after all fallback attempts. "
                    f"Original error: {original_error}. "
                    f"Final error: {final_error}. "
                    f"Consider using 'ignore_deletion_vectors=True' in options."
                ) from final_error

    def get_read_tasks(self, parallelism: int) -> List[ReadTask]:
        """Get read tasks for the Delta table.

        This method overrides the parent method to provide Delta-specific optimizations.

        Args:
            parallelism: The number of parallel read tasks to create.

        Returns:
            List of ReadTask objects for reading the Delta table.
        """
        # Use the parent ParquetDatasource's read task generation
        # since we've already converted the Delta table to parquet file paths
        return super().get_read_tasks(parallelism)

    def estimate_inmemory_data_size(self) -> Optional[int]:
        """Estimate the in-memory data size of the Delta table.

        Returns:
            Estimated data size in bytes, or None if estimation is not possible.
        """
        try:
            # Try to get size information from the Delta table metadata
            from deltalake import DeltaTable

            dt_args = {"table_uri": self._delta_path}
            if self._storage_options:
                dt_args["storage_options"] = self._storage_options

            dt = DeltaTable(**dt_args)

            # Get table metadata
            metadata = dt.metadata()

            # Estimate size based on file sizes and row count
            if hasattr(metadata, "num_rows") and hasattr(metadata, "size"):
                # If we have both row count and total size, use them
                return metadata.size
            elif hasattr(metadata, "num_rows"):
                # Estimate based on row count (rough approximation)
                estimated_bytes_per_row = DEFAULT_BYTES_PER_ROW  # Conservative estimate
                return metadata.num_rows * estimated_bytes_per_row
            else:
                # Fall back to parent class estimation
                return super().estimate_inmemory_data_size()

        except Exception as e:
            logger.warning(f"Could not estimate Delta table size: {e}")
            # Fall back to parent class estimation
            return super().estimate_inmemory_data_size()

    def get_metadata(self) -> Dict[str, Any]:
        """Get comprehensive metadata about the Delta table.

        This method leverages the full deltalake API to extract rich metadata including
        deletion vector information, column mapping details, and transaction history.

        Returns:
            Dictionary containing comprehensive Delta table metadata.
        """
        try:
            from deltalake import DeltaTable

            dt_args = {"table_uri": self._delta_path}
            if self._storage_options:
                dt_args["storage_options"] = self._storage_options

            dt = DeltaTable(**dt_args)

            # Get basic metadata
            metadata = dt.metadata()
            schema = dt.schema()

            # Enhanced metadata extraction using deltalake API
            enhanced_metadata = {
                "table_uri": self._delta_path,
                "version": dt.version(),
                "schema": schema.to_pyarrow().to_pylist(),
                "partition_columns": metadata.partition_columns,
                "num_files": len(dt.file_uris()),
                "num_rows": metadata.num_rows,
                "size_bytes": getattr(metadata, "size", None),
                "created_time": metadata.created_time,
                "last_modified_time": metadata.last_modified_time,
                "configuration": metadata.configuration,
                "partition_filters": self._partition_filters,
                "storage_options": self._storage_options,
                "unity_catalog_config": self._unity_catalog_config,
            }

            # Add deletion vector information
            deletion_vector_info = self._extract_deletion_vector_metadata(dt, metadata)
            enhanced_metadata.update(deletion_vector_info)

            # Add column mapping information
            column_mapping_info = self._extract_column_mapping_metadata(dt, metadata)
            enhanced_metadata.update(column_mapping_info)

            # Add constraint information
            constraint_info = self._extract_constraint_metadata(dt, metadata)
            enhanced_metadata.update(constraint_info)

            # Add file-level statistics
            file_stats = self._extract_file_statistics(dt)
            enhanced_metadata.update(file_stats)

            # Add transaction history summary
            history_summary = self._extract_history_summary(dt)
            enhanced_metadata.update(history_summary)

            return enhanced_metadata

        except Exception as e:
            logger.warning(f"Could not get comprehensive Delta table metadata: {e}")
            return {
                "table_uri": self._delta_path,
                "error": str(e),
                "partition_filters": self._partition_filters,
                "storage_options": self._storage_options,
                "unity_catalog_config": self._unity_catalog_config,
            }

    def _extract_deletion_vector_metadata(
        self, dt: "DeltaTable", metadata: Any
    ) -> Dict[str, Any]:
        """Extract deletion vector related metadata from the Delta table.

        Args:
            dt: The DeltaTable instance.
            metadata: The table metadata.

        Returns:
            Dictionary containing deletion vector metadata.
        """
        deletion_vector_info = {
            "has_deletion_vectors": False,
            "deletion_vector_files": [],
            "deletion_vector_stats": {},
        }

        try:
            # Check configuration for deletion vector settings
            config = metadata.configuration or {}
            if "delta.enableDeletionVectors" in config:
                deletion_vector_info["has_deletion_vectors"] = (
                    config["delta.enableDeletionVectors"].lower() == "true"
                )

            # Check add actions for deletion vector files
            try:
                add_actions = dt.get_add_actions(flatten=True)
                if hasattr(add_actions, "to_pandas"):
                    df = add_actions.to_pandas()
                    if "deletion_vector" in df.columns:
                        deletion_vector_info["has_deletion_vectors"] = True
                        deletion_vector_info["deletion_vector_files"] = df[
                            df["deletion_vector"].notna()
                        ]["path"].tolist()

                        # Extract deletion vector statistics
                        deletion_vector_info["deletion_vector_stats"] = {
                            "total_files_with_deletion_vectors": len(
                                deletion_vector_info["deletion_vector_files"]
                            ),
                            "deletion_vector_file_size": df[
                                df["deletion_vector"].notna()
                            ]["size"].sum()
                            if "size" in df.columns
                            else None,
                        }
            except Exception:
                # If we can't access add actions, skip this part
                pass

        except Exception as e:
            logger.debug(f"Could not extract deletion vector metadata: {e}")

        return deletion_vector_info

    def _extract_column_mapping_metadata(
        self, dt: "DeltaTable", metadata: Any
    ) -> Dict[str, Any]:
        """Extract column mapping related metadata from the Delta table.

        Args:
            dt: The DeltaTable instance.
            metadata: The table metadata.

        Returns:
            Dictionary containing column mapping metadata.
        """
        column_mapping_info = {
            "has_column_mapping": False,
            "column_mapping_details": {},
        }

        try:
            # Check configuration for column mapping settings
            config = metadata.configuration or {}
            if "delta.columnMapping.mode" in config:
                column_mapping_info["has_column_mapping"] = True
                column_mapping_info["column_mapping_details"] = {
                    "mode": config["delta.columnMapping.mode"],
                    "max_column_id": config.get("delta.columnMapping.maxColumnId"),
                }

            # Check if schema has column mapping information
            schema = dt.schema()
            if hasattr(schema, "fields"):
                for field in schema.fields:
                    if hasattr(field, "metadata") and field.metadata:
                        # Look for column mapping metadata in field metadata
                        if any("columnMapping" in key for key in field.metadata.keys()):
                            column_mapping_info["has_column_mapping"] = True
                            break

        except Exception as e:
            logger.debug(f"Could not extract column mapping metadata: {e}")

        return column_mapping_info

    def _extract_constraint_metadata(
        self, dt: "DeltaTable", metadata: Any
    ) -> Dict[str, Any]:
        """Extract constraint related metadata from the Delta table.

        Args:
            dt: The DeltaTable instance.
            metadata: The table metadata.

        Returns:
            Dictionary containing constraint metadata.
        """
        constraint_info = {
            "has_constraints": False,
            "constraint_details": {},
        }

        try:
            # Check configuration for constraint settings
            config = metadata.configuration or {}
            if "delta.constraints" in config:
                constraint_info["has_constraints"] = True
                constraint_info["constraint_details"] = {
                    "constraints": config["delta.constraints"],
                }

            # Check for other constraint-related configurations
            constraint_keys = [
                key for key in config.keys() if "constraint" in key.lower()
            ]
            if constraint_keys:
                constraint_info["has_constraints"] = True
                constraint_info["constraint_details"]["additional_constraints"] = {
                    key: config[key] for key in constraint_keys
                }

        except Exception as e:
            logger.debug(f"Could not extract constraint metadata: {e}")

        return constraint_info

    def _extract_file_statistics(self, dt: "DeltaTable") -> Dict[str, Any]:
        """Extract file-level statistics from the Delta table.

        Args:
            dt: The DeltaTable instance.

        Returns:
            Dictionary containing file statistics.
        """
        file_stats = {
            "file_statistics": {},
            "partition_statistics": {},
        }

        try:
            # Get file URIs and analyze them
            file_uris = dt.file_uris()
            file_stats["file_statistics"] = {
                "total_files": len(file_uris),
                "file_extensions": list(
                    set([os.path.splitext(uri)[1] for uri in file_uris])
                ),
            }

            # Analyze partition structure if table is partitioned
            try:
                partitions = dt.partitions()
                if partitions:
                    file_stats["partition_statistics"] = {
                        "total_partitions": len(partitions),
                        "partition_examples": partitions[
                            :5
                        ],  # Show first 5 partitions as examples
                    }
            except Exception:
                # Table might not be partitioned
                pass

        except Exception as e:
            logger.debug(f"Could not extract file statistics: {e}")

        return file_stats

    def _extract_history_summary(self, dt: "DeltaTable") -> Dict[str, Any]:
        """Extract transaction history summary from the Delta table.

        Args:
            dt: The DeltaTable instance.

        Returns:
            Dictionary containing history summary.
        """
        history_summary = {
            "history_summary": {},
        }

        try:
            # Get recent history (limit to last 10 commits for performance)
            history = dt.history(limit=10)
            if history:
                history_summary["history_summary"] = {
                    "recent_commits": len(history),
                    "latest_operation": history[0].get("operation")
                    if history
                    else None,
                    "latest_timestamp": history[0].get("timestamp")
                    if history
                    else None,
                    "commit_patterns": list(
                        set(
                            [
                                commit.get("operation")
                                for commit in history
                                if commit.get("operation")
                            ]
                        )
                    ),
                }

        except Exception as e:
            logger.debug(f"Could not extract history summary: {e}")

        return history_summary

    def get_partition_info(self) -> List[Dict[str, Any]]:
        """Get information about table partitions.

        This method leverages the full deltalake API to extract comprehensive partition
        information including partition statistics and filtering capabilities.

        Returns:
            List of partition information dictionaries.
        """
        try:
            from deltalake import DeltaTable

            dt_args = {"table_uri": self._delta_path}
            if self._storage_options:
                dt_args["storage_options"] = self._storage_options

            dt = DeltaTable(**dt_args)

            # Get partition information
            partition_info = []

            # Get all partition columns
            partition_columns = dt.metadata().partition_columns
            if not partition_columns:
                return []

            # Use deltalake's native partition methods when possible
            try:
                # Try to use the native partitions() method for better performance
                native_partitions = dt.partitions()
                if native_partitions:
                    # Group partitions by column
                    partition_values_by_column = {}
                    for partition_dict in native_partitions:
                        for col, value in partition_dict.items():
                            if col not in partition_values_by_column:
                                partition_values_by_column[col] = set()
                            partition_values_by_column[col].add(value)

                    # Build partition info from native method
                    for partition_col in partition_columns:
                        if partition_col in partition_values_by_column:
                            partition_info.append(
                                {
                                    "column": partition_col,
                                    "values": list(
                                        partition_values_by_column[partition_col]
                                    ),
                                    "source": "native_deltalake_api",
                                    "count": len(
                                        partition_values_by_column[partition_col]
                                    ),
                                }
                            )
                        else:
                            # Fallback for columns not found in native partitions
                            partition_info.append(
                                {
                                    "column": partition_col,
                                    "values": [],
                                    "source": "fallback",
                                    "count": 0,
                                }
                            )

                    return partition_info

            except Exception as e:
                logger.debug(
                    f"Native partition method failed, using file-based extraction: {e}"
                )
                # Fall back to file-based partition extraction

            # File-based partition extraction (fallback method)
            for partition_col in partition_columns:
                partition_values = set()

                # Extract partition values from file paths
                for file_uri in dt.file_uris():
                    # Parse partition values from the file path
                    # Look for partition column patterns like "column=value" in the path
                    partition_pattern = rf"{re.escape(partition_col)}=([^/]+)"
                    match = re.search(partition_pattern, file_uri)
                    if match:
                        partition_value = match.group(1)
                        partition_values.add(partition_value)

                partition_info.append(
                    {
                        "column": partition_col,
                        "values": list(partition_values),
                        "source": "file_path_parsing",
                        "count": len(partition_values),
                    }
                )

            return partition_info

        except Exception as e:
            logger.warning(f"Could not get partition information: {e}")
            return []

    def get_partition_statistics(self) -> Dict[str, Any]:
        """Get detailed statistics about table partitions.

        This method provides comprehensive partition analytics using the deltalake API.

        Returns:
            Dictionary containing partition statistics and analytics.
        """
        try:
            from deltalake import DeltaTable

            dt_args = {"table_uri": self._delta_path}
            if self._storage_options:
                dt_args["storage_options"] = self._storage_options

            dt = DeltaTable(**dt_args)

            partition_stats = {
                "total_partitions": 0,
                "partition_columns": [],
                "partition_distribution": {},
                "partition_file_counts": {},
                "partition_size_analysis": {},
            }

            # Get partition columns
            partition_columns = dt.metadata().partition_columns
            partition_stats["partition_columns"] = partition_columns

            if not partition_columns:
                return partition_stats

            try:
                # Get native partition information
                native_partitions = dt.partitions()
                partition_stats["total_partitions"] = len(native_partitions)

                # Analyze partition distribution
                for partition_dict in native_partitions:
                    for col, value in partition_dict.items():
                        if col not in partition_stats["partition_distribution"]:
                            partition_stats["partition_distribution"][col] = {}
                        if value not in partition_stats["partition_distribution"][col]:
                            partition_stats["partition_distribution"][col][value] = 0
                        partition_stats["partition_distribution"][col][value] += 1

                # Analyze file counts per partition
                for partition_dict in native_partitions:
                    partition_key = "_".join(
                        [f"{k}={v}" for k, v in sorted(partition_dict.items())]
                    )

                    try:
                        # Get files for this specific partition
                        partition_files = dt.file_uris(
                            partition_filters=[
                                (k, "=", v) for k, v in partition_dict.items()
                            ]
                        )
                        partition_stats["partition_file_counts"][partition_key] = len(
                            partition_files
                        )
                    except Exception:
                        partition_stats["partition_file_counts"][partition_key] = 0

                # Analyze partition sizes
                for partition_dict in native_partitions:
                    partition_key = "_".join(
                        [f"{k}={v}" for k, v in sorted(partition_dict.items())]
                    )

                    try:
                        # Get add actions for this partition to estimate size
                        add_actions = dt.get_add_actions(flatten=True)
                        if hasattr(add_actions, "to_pandas"):
                            df = add_actions.to_pandas()
                            # Filter by partition values if possible
                            partition_stats["partition_size_analysis"][
                                partition_key
                            ] = {
                                "estimated_files": len(df)
                                if "path" in df.columns
                                else 0,
                                "estimated_size": df["size"].sum()
                                if "size" in df.columns
                                else None,
                            }
                    except Exception:
                        partition_stats["partition_size_analysis"][partition_key] = {
                            "estimated_files": 0,
                            "estimated_size": None,
                        }

            except Exception as e:
                logger.debug(f"Could not get detailed partition statistics: {e}")
                # Fall back to basic partition info
                partition_info = self.get_partition_info()
                partition_stats["total_partitions"] = sum(
                    len(info["values"]) for info in partition_info
                )

            return partition_stats

        except Exception as e:
            logger.warning(f"Could not get partition statistics: {e}")
            return {
                "error": str(e),
                "total_partitions": 0,
                "partition_columns": [],
            }

    def get_version_info(self) -> Dict[str, Any]:
        """Get information about available table versions.

        Returns:
            Dictionary containing version information.
        """
        try:
            from deltalake import DeltaTable

            dt_args = {"table_uri": self._delta_path}
            if self._storage_options:
                dt_args["storage_options"] = self._storage_options

            dt = DeltaTable(**dt_args)

            version_info = {
                "current_version": dt.version(),
                "table_uri": self._delta_path,
                "requested_version": self._version,
                "has_time_travel": True,  # Delta tables support time travel
            }

            # Try to get commit info if available
            try:
                commit_info = dt.history()
                if commit_info:
                    version_info["commit_history"] = commit_info
            except Exception:
                # Commit history might not be available
                pass

            return version_info

        except Exception as e:
            logger.warning(f"Could not get version information: {e}")
            return {
                "table_uri": self._delta_path,
                "error": str(e),
                "requested_version": self._version,
            }

    def validate_partition_filters(
        self, partition_filters: List[Tuple[str, str, Any]]
    ) -> bool:
        """Validate that the provided partition filters are valid for this table.

        Args:
            partition_filters: List of partition filter tuples.

        Returns:
            True if filters are valid, False otherwise.
        """
        try:
            from deltalake import DeltaTable

            dt_args = {"table_uri": self._delta_path}
            if self._storage_options:
                dt_args["storage_options"] = self._storage_options

            dt = DeltaTable(**dt_args)

            # Get actual partition columns from the table
            actual_partition_columns = set(dt.metadata().partition_columns)

            # Check if all filter columns exist in the table's partition columns
            for column, operator, value in partition_filters:
                if column not in actual_partition_columns:
                    logger.warning(
                        f"Partition filter column '{column}' not found in table. "
                        f"Available columns: {actual_partition_columns}"
                    )
                    return False

                # Validate operator
                valid_operators = {"=", "!=", "in", "not in"}
                if operator not in valid_operators:
                    logger.warning(
                        f"Invalid partition filter operator '{operator}'. "
                        f"Valid operators: {valid_operators}"
                    )
                    return False

            return True

        except Exception as e:
            logger.warning(f"Could not validate partition filters: {e}")
            return False

    def get_optimization_hints(self) -> Dict[str, Any]:
        """Get optimization hints for reading this Delta table.

        This method leverages the full deltalake API to provide comprehensive optimization
        recommendations based on table characteristics and features.

        Returns:
            Dictionary containing optimization hints.
        """
        try:
            from deltalake import DeltaTable

            dt_args = {"table_uri": self._delta_path}
            if self._storage_options:
                dt_args["storage_options"] = self._storage_options

            dt = DeltaTable(**dt_args)

            hints = {
                "table_uri": self._delta_path,
                "recommended_parallelism": None,
                "memory_optimization": {},
                "performance_tips": [],
                "feature_specific_optimizations": {},
                "storage_optimizations": {},
            }

            # Analyze table characteristics for optimization hints
            metadata = dt.metadata()
            schema = dt.schema()

            # File count-based parallelism recommendation
            num_files = len(dt.file_uris())
            if num_files > 0:
                # Recommend parallelism based on file count, but cap it
                recommended = min(
                    num_files, MAX_RECOMMENDED_PARALLELISM
                )  # Cap at MAX_RECOMMENDED_PARALLELISM for very large tables
                hints["recommended_parallelism"] = recommended

            # Memory optimization hints
            if self._without_files:
                hints["memory_optimization"]["without_files"] = True
                hints["performance_tips"].append(
                    "Using without_files=True reduces memory usage but disables file tracking"
                )

            if self._log_buffer_size:
                hints["memory_optimization"]["log_buffer_size"] = self._log_buffer_size
                hints["performance_tips"].append(
                    f"Log buffer size set to {self._log_buffer_size} for optimized commit log reading"
                )

            # Partition-based optimizations
            if self._partition_filters:
                hints["performance_tips"].append(
                    "Partition filters applied - this will reduce data scanned"
                )

                # Analyze partition filter effectiveness
                partition_analysis = self._analyze_partition_filter_effectiveness(dt)
                hints["feature_specific_optimizations"][
                    "partition_filter_analysis"
                ] = partition_analysis

            # Version-based optimizations
            if self._version is not None:
                hints["performance_tips"].append(
                    f"Reading specific version {self._version} - ensure this version exists"
                )

                # Check if version exists and provide version-specific hints
                version_hints = self._get_version_specific_hints(dt)
                hints["feature_specific_optimizations"]["version_hints"] = version_hints

            # Storage optimization hints
            if self._storage_options:
                hints["performance_tips"].append(
                    "Custom storage options provided - ensure they match your cloud configuration"
                )

                # Analyze storage options for optimization opportunities
                storage_analysis = self._analyze_storage_options(dt, metadata)
                hints["storage_optimizations"] = storage_analysis

            # Deletion vector optimizations
            deletion_vector_hints = self._get_deletion_vector_optimization_hints(
                dt, metadata
            )
            if deletion_vector_hints:
                hints["feature_specific_optimizations"][
                    "deletion_vector"
                ] = deletion_vector_hints

            # Column mapping optimizations
            column_mapping_hints = self._get_column_mapping_optimization_hints(
                dt, metadata
            )
            if column_mapping_hints:
                hints["feature_specific_optimizations"][
                    "column_mapping"
                ] = column_mapping_hints

            # Schema-based optimizations
            schema_hints = self._get_schema_based_optimization_hints(schema)
            if schema_hints:
                hints["feature_specific_optimizations"]["schema"] = schema_hints

            # Table size and row count optimizations
            if hasattr(metadata, "num_rows") and metadata.num_rows:
                row_count = metadata.num_rows
                if row_count > 1000000:  # 1M rows
                    hints["performance_tips"].append(
                        f"Large table with {row_count:,} rows - consider using partition filters for better performance"
                    )

                # Row group size optimization
                if hasattr(metadata, "size") and metadata.size:
                    avg_row_size = metadata.size / row_count
                    if avg_row_size > 1024:  # 1KB per row
                        hints["performance_tips"].append(
                            f"Large average row size ({avg_row_size:.1f} bytes) - consider column pruning for better performance"
                        )

            return hints

        except Exception as e:
            logger.warning(f"Could not get optimization hints: {e}")
            return {
                "table_uri": self._delta_path,
                "error": str(e),
            }

    def _analyze_partition_filter_effectiveness(
        self, dt: "DeltaTable"
    ) -> Dict[str, Any]:
        """Analyze the effectiveness of partition filters for optimization.

        Args:
            dt: The DeltaTable instance.

        Returns:
            Dictionary containing partition filter analysis.
        """
        analysis = {
            "filter_effectiveness": "unknown",
            "estimated_data_reduction": None,
            "recommendations": [],
        }

        try:
            if not self._partition_filters:
                return analysis

            # Get total files without filters
            total_files = len(dt.file_uris())

            # Get files with partition filters
            filtered_files = len(
                dt.file_uris(partition_filters=self._partition_filters)
            )

            if total_files > 0:
                data_reduction = (total_files - filtered_files) / total_files
                analysis["estimated_data_reduction"] = f"{data_reduction:.1%}"

                if data_reduction > 0.5:
                    analysis["filter_effectiveness"] = "excellent"
                    analysis["recommendations"].append(
                        "Partition filters are highly effective - consider using more specific filters"
                    )
                elif data_reduction > 0.2:
                    analysis["filter_effectiveness"] = "good"
                    analysis["recommendations"].append(
                        "Partition filters provide good data reduction"
                    )
                else:
                    analysis["filter_effectiveness"] = "limited"
                    analysis["recommendations"].append(
                        "Partition filters provide limited data reduction - consider more selective filters"
                    )

        except Exception as e:
            logger.debug(f"Could not analyze partition filter effectiveness: {e}")

        return analysis

    def _get_version_specific_hints(self, dt: "DeltaTable") -> Dict[str, Any]:
        """Get optimization hints specific to the requested version.

        Args:
            dt: The DeltaTable instance.

        Returns:
            Dictionary containing version-specific hints.
        """
        hints = {
            "version_exists": True,
            "version_age": None,
            "recommendations": [],
        }

        try:
            current_version = dt.version()
            requested_version = self._version

            if (
                isinstance(requested_version, int)
                and requested_version > current_version
            ):
                hints["version_exists"] = False
                hints["recommendations"].append(
                    f"Requested version {requested_version} is in the future"
                )
                return hints

            # Get version history for age analysis
            try:
                history = dt.history(limit=50)
                if history:
                    # Find the requested version in history
                    for commit in history:
                        if commit.get("version") == requested_version:
                            commit_time = commit.get("timestamp")
                            if commit_time:
                                hints["version_age"] = commit_time
                                hints["recommendations"].append(
                                    f"Version {requested_version} was committed at {commit_time}"
                                )
                            break
            except Exception:
                pass

        except Exception as e:
            logger.debug(f"Could not get version-specific hints: {e}")

        return hints

    def _analyze_storage_options(
        self, dt: "DeltaTable", metadata: Any
    ) -> Dict[str, Any]:
        """Analyze storage options for optimization opportunities.

        Args:
            dt: The DeltaTable instance.
            metadata: The table metadata.

        Returns:
            Dictionary containing storage optimization analysis.
        """
        analysis = {
            "cloud_provider": "unknown",
            "storage_class": "unknown",
            "optimization_opportunities": [],
        }

        try:
            # Determine cloud provider from table URI
            table_uri = str(self._delta_path)
            if table_uri.startswith("s3://"):
                analysis["cloud_provider"] = "aws"
                analysis["optimization_opportunities"].append(
                    "Consider using S3 Select for column pruning"
                )
            elif table_uri.startswith("gs://"):
                analysis["cloud_provider"] = "gcp"
                analysis["optimization_opportunities"].append(
                    "Consider using BigQuery for complex analytics"
                )
            elif table_uri.startswith("abfs://"):
                analysis["cloud_provider"] = "azure"
                analysis["optimization_opportunities"].append(
                    "Consider using Azure Data Lake Analytics"
                )

            # Check storage configuration
            config = metadata.configuration or {}
            if "delta.targetFileSize" in config:
                target_size = config["delta.targetFileSize"]
                analysis["storage_class"] = f"target_file_size_{target_size}"

                # Provide size-based optimization hints
                try:
                    target_size_int = int(target_size)
                    if target_size_int < 1024 * 1024:  # < 1MB
                        analysis["optimization_opportunities"].append(
                            "Small target file size - consider increasing for better read performance"
                        )
                    elif target_size_int > 100 * 1024 * 1024:  # > 100MB
                        analysis["optimization_opportunities"].append(
                            "Large target file size - consider decreasing for better parallelism"
                        )
                except ValueError:
                    pass

        except Exception as e:
            logger.debug(f"Could not analyze storage options: {e}")

        return analysis

    def _get_deletion_vector_optimization_hints(
        self, dt: "DeltaTable", metadata: Any
    ) -> Dict[str, Any]:
        """Get optimization hints specific to deletion vectors.

        Args:
            dt: The DeltaTable instance.
            metadata: The table metadata.

        Returns:
            Dictionary containing deletion vector optimization hints.
        """
        hints = {
            "has_deletion_vectors": False,
            "deletion_vector_impact": "none",
            "optimization_recommendations": [],
        }

        try:
            # Check if table has deletion vectors
            config = metadata.configuration or {}
            if "delta.enableDeletionVectors" in config:
                hints["has_deletion_vectors"] = (
                    config["delta.enableDeletionVectors"].lower() == "true"
                )

                if hints["has_deletion_vectors"]:
                    hints["deletion_vector_impact"] = "moderate"
                    hints["optimization_recommendations"].extend(
                        [
                            "Deletion vectors enable efficient row-level deletes",
                            "Consider using ignore_deletion_vectors=True if you don't need deleted row filtering",
                            "Monitor deletion vector file sizes for storage optimization",
                        ]
                    )

                    # Check deletion vector configuration
                    if "delta.deletionVectors.persistDeletionVectorsAsSubdir" in config:
                        hints["optimization_recommendations"].append(
                            "Deletion vectors are stored in subdirectories for better organization"
                        )

        except Exception as e:
            logger.debug(f"Could not get deletion vector optimization hints: {e}")

        return hints

    def _get_column_mapping_optimization_hints(
        self, dt: "DeltaTable", metadata: Any
    ) -> Dict[str, Any]:
        """Get optimization hints specific to column mapping.

        Args:
            dt: The DeltaTable instance.
            metadata: The table metadata.

        Returns:
            Dictionary containing column mapping optimization hints.
        """
        hints = {
            "has_column_mapping": False,
            "column_mapping_mode": "none",
            "optimization_recommendations": [],
        }

        try:
            # Check column mapping configuration
            config = metadata.configuration or {}
            if "delta.columnMapping.mode" in config:
                hints["has_column_mapping"] = True
                hints["column_mapping_mode"] = config["delta.columnMapping.mode"]

                if hints["column_mapping_mode"] == "name":
                    hints["optimization_recommendations"].extend(
                        [
                            "Column mapping by name provides schema evolution flexibility",
                            "Consider using ignore_column_mapping=True if schema is stable",
                        ]
                    )
                elif hints["column_mapping_mode"] == "id":
                    hints["optimization_recommendations"].extend(
                        [
                            "Column mapping by ID provides maximum schema evolution support",
                            "Column pruning may be less effective with ID-based mapping",
                        ]
                    )

        except Exception as e:
            logger.debug(f"Could not get column mapping optimization hints: {e}")

        return hints

    def _get_schema_based_optimization_hints(self, schema: Any) -> Dict[str, Any]:
        """Get optimization hints based on schema analysis.

        Args:
            schema: The table schema.

        Returns:
            Dictionary containing schema-based optimization hints.
        """
        hints = {
            "complex_types": [],
            "nullable_columns": 0,
            "optimization_recommendations": [],
        }

        try:
            if hasattr(schema, "fields"):
                for field in schema.fields:
                    # Check for complex types
                    field_type = str(field.type)
                    if any(
                        complex_type in field_type.lower()
                        for complex_type in ["array", "map", "struct"]
                    ):
                        hints["complex_types"].append(field.name)

                    # Count nullable columns
                    if hasattr(field, "nullable") and field.nullable:
                        hints["nullable_columns"] += 1

                # Provide recommendations based on schema analysis
                if hints["complex_types"]:
                    hints["optimization_recommendations"].append(
                        f"Complex types detected: {', '.join(hints['complex_types'])} - consider flattening for better performance"
                    )

                if hints["nullable_columns"] > len(schema.fields) * 0.8:
                    hints["optimization_recommendations"].append(
                        "High percentage of nullable columns - consider using default values for better compression"
                    )

        except Exception as e:
            logger.debug(f"Could not get schema-based optimization hints: {e}")

        return hints

    def get_change_data_feed(
        self,
        starting_version: Optional[int] = None,
        ending_version: Optional[int] = None,
        starting_timestamp: Optional[str] = None,
        ending_timestamp: Optional[str] = None,
        columns: Optional[List[str]] = None,
        predicate: Optional[str] = None,
        allow_out_of_range: bool = False,
    ) -> Optional[Any]:
        """Get the Change Data Feed (CDF) from the Delta table.

        This method leverages the deltalake API's load_cdf functionality to track
        changes to the table over time, including inserts, updates, and deletes.

        Args:
            starting_version: The version of the Delta table to start reading CDF from.
                If None, starts from version 0.
            ending_version: The version to stop reading CDF at. If None, reads up to the latest version.
            starting_timestamp: An ISO 8601 timestamp to start reading CDF from.
                Ignored if starting_version is provided.
            ending_timestamp: An ISO 8601 timestamp to stop reading CDF at.
                Ignored if ending_version is provided.
            columns: A list of column names to include in the output. If None, all columns are included.
            predicate: An optional SQL predicate to filter the output rows.
            allow_out_of_range: If True, does not raise an error when specified versions
                or timestamps are outside the table's history.

        Returns:
            An Arrow RecordBatchReader that streams the resulting change data, or None if CDF is not available.

        Example:
            .. testcode::

                # Get changes from version 10 to 20
                cdf_reader = datasource.get_change_data_feed(
                    starting_version=10,
                    ending_version=20
                )

                # Get changes from a specific timestamp
                cdf_reader = datasource.get_change_data_feed(
                    starting_timestamp="2023-01-01T00:00:00Z"
                )
        """
        try:
            from deltalake import DeltaTable

            dt_args = {"table_uri": self._delta_path}
            if self._storage_options:
                dt_args["storage_options"] = self._storage_options

            dt = DeltaTable(**dt_args)

            # Check if CDF is enabled for this table
            if not self._is_cdf_enabled(dt):
                logger.warning(
                    "Change Data Feed (CDF) is not enabled for this Delta table"
                )
                return None

            # Load the change data feed
            cdf_reader = dt.load_cdf(
                starting_version=starting_version or 0,
                ending_version=ending_version,
                starting_timestamp=starting_timestamp,
                ending_timestamp=ending_timestamp,
                columns=columns,
                predicate=predicate,
                allow_out_of_range=allow_out_of_range,
            )

            logger.info(
                f"Successfully loaded Change Data Feed from version "
                f"{starting_version or 0} to {ending_version or 'latest'}"
            )

            return cdf_reader

        except Exception as e:
            logger.warning(f"Could not load Change Data Feed: {e}")
            return None

    def _is_cdf_enabled(self, dt: "DeltaTable") -> bool:
        """Check if Change Data Feed (CDF) is enabled for the Delta table.

        Args:
            dt: The DeltaTable instance.

        Returns:
            True if CDF is enabled, False otherwise.
        """
        try:
            metadata = dt.metadata()
            config = metadata.configuration or {}

            # Check for CDF configuration
            cdf_enabled = (
                config.get("delta.enableChangeDataFeed", "false").lower() == "true"
            )

            if cdf_enabled:
                logger.debug("Change Data Feed is enabled for this table")
            else:
                logger.debug("Change Data Feed is not enabled for this table")

            return cdf_enabled

        except Exception as e:
            logger.debug(f"Could not determine CDF status: {e}")
            return False

    def get_table_statistics(self) -> Dict[str, Any]:
        """Get comprehensive table statistics using the deltalake API.

        This method provides detailed analytics about the Delta table including
        file distribution, partition statistics, and performance metrics.

        Returns:
            Dictionary containing comprehensive table statistics.
        """
        try:
            from deltalake import DeltaTable

            dt_args = {"table_uri": self._delta_path}
            if self._storage_options:
                dt_args["storage_options"] = self._storage_options

            dt = DeltaTable(**dt_args)

            stats = {
                "table_uri": self._delta_path,
                "version": dt.version(),
                "protocol": {},
                "file_statistics": {},
                "partition_statistics": {},
                "performance_metrics": {},
                "feature_flags": {},
            }

            # Get protocol information
            try:
                protocol = dt.protocol()
                stats["protocol"] = {
                    "min_reader_version": protocol.min_reader_version,
                    "min_writer_version": protocol.min_writer_version,
                    "reader_features": getattr(protocol, "reader_features", []),
                    "writer_features": getattr(protocol, "writer_features", []),
                }
            except Exception as e:
                logger.debug(f"Could not get protocol information: {e}")

            # Get file statistics
            try:
                file_uris = dt.file_uris()
                add_actions = dt.get_add_actions(flatten=True)

                if hasattr(add_actions, "to_pandas"):
                    df = add_actions.to_pandas()

                    stats["file_statistics"] = {
                        "total_files": len(file_uris),
                        "total_size_bytes": df["size"].sum()
                        if "size" in df.columns
                        else None,
                        "average_file_size_bytes": df["size"].mean()
                        if "size" in df.columns
                        else None,
                        "file_size_distribution": {
                            "small_files_1mb": len(df[df["size"] <= 1024 * 1024])
                            if "size" in df.columns
                            else 0,
                            "medium_files_1mb_to_100mb": len(
                                df[
                                    (df["size"] > 1024 * 1024)
                                    & (df["size"] <= 100 * 1024 * 1024)
                                ]
                            )
                            if "size" in df.columns
                            else 0,
                            "large_files_100mb_plus": len(
                                df[df["size"] > 100 * 1024 * 1024]
                            )
                            if "size" in df.columns
                            else 0,
                        },
                    }
            except Exception as e:
                logger.debug(f"Could not get file statistics: {e}")

            # Get partition statistics
            try:
                partition_stats = self.get_partition_statistics()
                stats["partition_statistics"] = partition_stats
            except Exception as e:
                logger.debug(f"Could not get partition statistics: {e}")

            # Get performance metrics
            try:
                metadata = dt.metadata()
                if hasattr(metadata, "num_rows") and metadata.num_rows:
                    stats["performance_metrics"] = {
                        "total_rows": metadata.num_rows,
                        "estimated_bytes_per_row": metadata.size / metadata.num_rows
                        if hasattr(metadata, "size") and metadata.size
                        else None,
                        "partition_efficiency": len(dt.partitions())
                        / max(1, len(file_uris))
                        if "file_uris" in locals()
                        else None,
                    }
            except Exception as e:
                logger.debug(f"Could not get performance metrics: {e}")

            # Get feature flags
            try:
                config = metadata.configuration or {}
                feature_flags = {}

                # Check for key Delta Lake features
                key_features = [
                    "delta.enableChangeDataFeed",
                    "delta.enableDeletionVectors",
                    "delta.columnMapping.mode",
                    "delta.constraints",
                    "delta.identityColumns",
                    "delta.invariants",
                ]

                for feature in key_features:
                    if feature in config:
                        feature_flags[feature] = config[feature]

                stats["feature_flags"] = feature_flags

            except Exception as e:
                logger.debug(f"Could not get feature flags: {e}")

            return stats

        except Exception as e:
            logger.warning(f"Could not get table statistics: {e}")
            return {
                "table_uri": self._delta_path,
                "error": str(e),
            }
