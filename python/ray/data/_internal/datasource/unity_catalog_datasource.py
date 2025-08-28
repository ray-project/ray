"""Unity Catalog datasource for Ray Data.

This module provides a Unity Catalog connector for loading tables
into Ray Datasets with support for multiple data formats and cloud providers.
"""

import logging
import os
import tempfile
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import requests

from ray.data.block import Block, BlockMetadata
from ray.data.datasource.datasource import Datasource, ReadTask
from ray.util.annotations import DeveloperAPI

# Set up logging
logger = logging.getLogger(__name__)

# Expanded file format mapping to support all major data formats
_FILE_FORMAT_TO_RAY_READER = {
    # Structured data formats
    "delta": "read_delta",
    "parquet": "read_parquet",
    "csv": "read_csv",
    "json": "read_json",
    "jsonl": "read_json",  # JSON Lines format
    "ndjson": "read_json",  # Newline Delimited JSON
    # Text formats
    "txt": "read_text",
    "text": "read_text",
    # Media formats
    "image": "read_images",
    "images": "read_images",
    "img": "read_images",
    "jpg": "read_images",
    "jpeg": "read_images",
    "png": "read_images",
    "gif": "read_images",
    "bmp": "read_images",
    "tiff": "read_images",
    "tif": "read_images",
    "webp": "read_images",
    "video": "read_videos",
    "videos": "read_videos",
    "mp4": "read_videos",
    "avi": "read_videos",
    "mov": "read_videos",
    "mkv": "read_videos",
    "wmv": "read_videos",
    "flv": "read_videos",
    "webm": "read_videos",
    "audio": "read_audio",
    "mp3": "read_audio",
    "wav": "read_audio",
    "flac": "read_audio",
    "aac": "read_audio",
    "ogg": "read_audio",
    "m4a": "read_audio",
    # Binary formats
    "binary": "read_binary_files",
    "bin": "read_binary_files",
    "dat": "read_binary_files",
    # Big Data formats
    "hudi": "read_hudi",
    "iceberg": "read_iceberg",
    # Additional structured formats
    "avro": "read_avro",
    "orc": "read_orc",
    "xml": "read_xml",
    "excel": "read_excel",
    "xlsx": "read_excel",
    "xls": "read_excel",
}

# Format aliases for better user experience
_FORMAT_ALIASES = {
    "table": "delta",  # Default for table-like data
    "data": "parquet",  # Default for structured data
    "log": "txt",  # Common for log files
    "logs": "txt",
    "picture": "images",  # Alternative for images
    "pictures": "images",
    "movie": "videos",  # Alternative for videos
    "movies": "videos",
    "sound": "audio",  # Alternative for audio
    "music": "audio",
    "raw": "binary",  # Alternative for binary
    "binaries": "binary",
}

# Enhanced Delta Lake options for Unity Catalog
_DEFAULT_DELTA_OPTIONS = {
    "ignore_deletion_vectors": True,
    "ignore_column_mapping": True,
    "ignore_constraints": True,
}


@dataclass
class ColumnInfo:
    """Information about a table column."""

    name: str
    type_text: str
    type_name: str
    position: int
    type_precision: int
    type_scale: int
    type_json: str
    nullable: bool
    # Additional fields that Unity Catalog might return
    column_masks: Optional[Dict[str, Any]] = None
    comment: Optional[str] = None


@dataclass
class EffectiveFlag:
    """Information about an effective flag."""

    value: str
    inherited_from_type: str
    inherited_from_name: str


@dataclass
class TableInfo:
    """Information about a Unity Catalog table."""

    name: str
    catalog_name: str
    schema_name: str
    table_type: str
    data_source_format: str
    columns: List[ColumnInfo]
    storage_location: str
    owner: str
    properties: Dict[str, str]
    securable_kind: str
    enable_auto_maintenance: str
    enable_predictive_optimization: str
    properties_pairs: Dict[str, Any]
    generation: int
    metastore_id: str
    full_name: str
    data_access_configuration_id: str
    created_at: int
    created_by: str
    updated_at: int
    updated_by: str
    table_id: str
    delta_runtime_properties_kvpairs: Dict[str, Any]
    securable_type: str
    effective_auto_maintenance_flag: Optional[EffectiveFlag] = None
    effective_predictive_optimization_flag: Optional[EffectiveFlag] = None
    browse_only: Optional[bool] = None
    metastore_version: Optional[int] = None
    # Additional fields that Unity Catalog might return
    schema_id: Optional[str] = None
    catalog_id: Optional[str] = None
    row_filters: Optional[List[str]] = None

    @staticmethod
    def from_dict(obj: Dict[str, Any]) -> "TableInfo":
        """Create a TableInfo instance from a dictionary."""
        if obj.get("effective_auto_maintenance_flag"):
            effective_auto_maintenance_flag = EffectiveFlag(
                **{
                    k: v
                    for k, v in obj["effective_auto_maintenance_flag"].items()
                    if k in EffectiveFlag.__dataclass_fields__
                }
            )
        else:
            effective_auto_maintenance_flag = None
        if obj.get("effective_predictive_optimization_flag"):
            effective_predictive_optimization_flag = EffectiveFlag(
                **{
                    k: v
                    for k, v in obj["effective_predictive_optimization_flag"].items()
                    if k in EffectiveFlag.__dataclass_fields__
                }
            )
        else:
            effective_predictive_optimization_flag = None
        return TableInfo(
            name=obj["name"],
            catalog_name=obj["catalog_name"],
            schema_name=obj["schema_name"],
            table_type=obj["table_type"],
            data_source_format=obj.get("data_source_format", ""),
            columns=[
                ColumnInfo(
                    **{
                        k: v
                        for k, v in col.items()
                        if k in ColumnInfo.__dataclass_fields__
                    }
                )
                for col in obj.get("columns", [])
            ],
            storage_location=obj.get("storage_location", ""),
            owner=obj.get("owner", ""),
            properties=obj.get("properties", {}),
            securable_kind=obj.get("securable_kind", ""),
            enable_auto_maintenance=obj.get("enable_auto_maintenance", ""),
            enable_predictive_optimization=obj.get(
                "enable_predictive_optimization", ""
            ),
            properties_pairs=obj.get("properties_pairs", {}),
            generation=obj.get("generation", 0),
            metastore_id=obj.get("metastore_id", ""),
            full_name=obj.get("full_name", ""),
            data_access_configuration_id=obj.get("data_access_configuration_id", ""),
            created_at=obj.get("created_at", 0),
            created_by=obj.get("created_by", ""),
            updated_at=obj.get("updated_at", 0),
            updated_by=obj.get("updated_by", ""),
            table_id=obj.get("table_id", ""),
            delta_runtime_properties_kvpairs=obj.get(
                "delta_runtime_properties_kvpairs", {}
            ),
            securable_type=obj.get("securable_type", ""),
            effective_auto_maintenance_flag=effective_auto_maintenance_flag,
            effective_predictive_optimization_flag=effective_predictive_optimization_flag,
            browse_only=obj.get("browse_only", False),
            metastore_version=obj.get("metastore_version", 0),
            # Additional fields that Unity Catalog might return
            schema_id=obj.get("schema_id", ""),
            catalog_id=obj.get("catalog_id", ""),
            row_filters=obj.get("row_filters", []),
        )


@DeveloperAPI
class UnityCatalogConnector(Datasource):
    """Unity Catalog datasource for loading tables into Ray Datasets.

    This datasource acts as a credential broker for Databricks Unity Catalog:
    1. Connects to Unity Catalog to get table metadata
    2. Fetches temporary credentials using Unity Catalog credential vending
    3. Configures cloud provider environment variables
    4. Delegates actual data reading to the appropriate native Ray Data connector

    The datasource requires the user to specify the data format explicitly,
    ensuring compatibility with Unity Catalog table configurations.

    Unity Catalog Credential Vending Support:
    - Read-only access to Unity Catalog managed Delta tables
    - Read and write access to Unity Catalog managed Iceberg tables
    - Read-only access to Delta tables configured for Iceberg reads
    - Create access to Unity Catalog external tables
    - Read and write access to Unity Catalog external tables

    Requirements:
    - External data access must be enabled on the metastore
    - Principal must have EXTERNAL USE SCHEMA privilege on the schema
    - Principal must have SELECT permission on the table
    - Principal must have USE CATALOG and USE SCHEMA privileges

    Currently supports Databricks-managed Unity Catalog
    Supported formats: delta, parquet, csv, json, txt, images, video, audio, binary, hudi, iceberg, and more.
    Supports AWS, Azure, and GCP with automatic credential handoff.

    Examples:
        .. testcode::

            # Basic usage with explicit format specification
            datasource = UnityCatalogConnector(
                base_url="https://your-workspace.cloud.databricks.com",
                token="your_token_here",
                table_full_name="catalog.schema.table",
                data_format="delta",
                operation="READ"
            )
            dataset = ray.data.read_datasource(datasource)

            # With specific Delta Lake options
            datasource = UnityCatalogConnector(
                base_url="https://your-workspace.cloud.databricks.com",
                token="your_token_here",
                table_full_name="catalog.schema.delta_table",
                data_format="delta",
                delta_options={"version": "latest"},
                storage_options={"aws_region": "us-west-2"}
            )
            dataset = ray.data.read_datasource(datasource)
    """

    def __init__(
        self,
        *,
        base_url: str,
        token: str,
        table_full_name: str,
        data_format: str,
        region: Optional[str] = None,
        operation: str = "READ",
        reader_kwargs: Optional[Dict[str, Any]] = None,
        # Enhanced options
        storage_options: Optional[Dict[str, Any]] = None,
        unity_catalog_config: Optional[Dict[str, Any]] = None,
        delta_options: Optional[Dict[str, Any]] = None,
    ):
        """Initialize the Unity Catalog datasource.

        Args:
            base_url: The base URL of the Databricks workspace.
            token: The authentication token for the workspace.
            table_full_name: The full name of the table in format catalog.schema.table.
            data_format: The data format of the table (required).
            region: The cloud region for the workspace.
            operation: The operation to perform. Must be one of:
                - "READ": Read-only access (maps to READ operation_name)
                - "WRITE": Read and write access (maps to READ_WRITE operation_name)
                - "DELETE": Read and write access (maps to READ_WRITE operation_name)
            reader_kwargs: Additional arguments to pass to the reader.
            storage_options: Storage-specific options for cloud providers.
            unity_catalog_config: Unity Catalog specific configuration.
            delta_options: Delta Lake specific options.
        """
        # Validate required parameters
        if not base_url:
            raise ValueError("base_url is required")
        if not token:
            raise ValueError("token is required")
        if not table_full_name:
            raise ValueError("table_full_name is required")
        if not data_format:
            raise ValueError("data_format is required")

        # Validate base_url format
        if not (base_url.startswith("http://") or base_url.startswith("https://")):
            raise ValueError("base_url must be a valid HTTP/HTTPS URL")

        # Validate data_format
        data_format = data_format.lower()
        if data_format not in _FILE_FORMAT_TO_RAY_READER:
            raise ValueError(
                f"Unsupported data_format '{data_format}'. Supported formats: {list(_FILE_FORMAT_TO_RAY_READER.keys())}"
            )

        # Validate operation and map to Unity Catalog operation_name
        valid_operations = ["READ", "WRITE", "DELETE"]
        if operation not in valid_operations:
            raise ValueError(f"operation must be one of {valid_operations}")

        # Map operation to Unity Catalog operation_name
        if operation == "READ":
            self._unity_operation = "READ"
        else:
            # WRITE and DELETE both map to READ_WRITE for Unity Catalog
            self._unity_operation = "READ_WRITE"

        # Validate region format for cloud providers
        if region:
            if not isinstance(region, str) or len(region) < 3:
                raise ValueError(
                    "region must be a valid string with at least 3 characters"
                )

        # Validate reader_kwargs
        if reader_kwargs and not isinstance(reader_kwargs, dict):
            raise ValueError("reader_kwargs must be a dictionary")

        # Validate storage_options
        if storage_options and not isinstance(storage_options, dict):
            raise ValueError("storage_options must be a dictionary")

        # Validate unity_catalog_config
        if unity_catalog_config and not isinstance(unity_catalog_config, dict):
            raise ValueError("unity_catalog_config must be a dictionary")

        # Validate delta_options
        if delta_options and not isinstance(delta_options, dict):
            raise ValueError("delta_options must be a dictionary")

        self.base_url = base_url.rstrip("/")
        self.token = token
        self.table_full_name = table_full_name
        self.data_format = data_format
        self.region = region
        self.operation = operation
        self.reader_kwargs = reader_kwargs or {}

        # Enhanced options
        self.storage_options = storage_options or {}
        self.unity_catalog_config = unity_catalog_config or {}
        self.delta_options = delta_options or {}

        # Parse table full name for Unity Catalog metadata
        self._parse_table_name()

        # Initialize internal state
        self._table_info = None
        self._table_id = None
        self._creds_response = None
        self._table_url = None
        self._runtime_env = None

        # Performance monitoring
        self._performance_metrics = {
            "table_info_fetch_time": None,
            "credentials_fetch_time": None,
            "environment_setup_time": None,
            "ray_init_time": None,
            "reader_selection_time": None,
            "read_execution_time": None,
            "total_read_time": None,
        }

    def get_name(self) -> str:
        """Return a human-readable name for this datasource.

        This will be used as the names of the read tasks.
        """
        return "UnityCatalog"

    def estimate_inmemory_data_size(self) -> Optional[int]:
        """Return an estimate of the in-memory data size, or None if unknown.

        Note that the in-memory data size may be larger than the on-disk data size.
        """
        # Try to get table info to estimate size
        try:
            if not self._table_info:
                self._get_table_info()

            # For now, return None as we don't have a reliable way to estimate
            # the in-memory size without reading the data
            return None
        except Exception:
            return None

    def get_read_tasks(self, parallelism: int) -> List[ReadTask]:
        """Execute the read and return read tasks.

        This method:
        1. Fetches Unity Catalog table information
        2. Gets temporary credentials for table access
        3. Configures environment for cloud provider access
        4. Creates read tasks that delegate to the appropriate native Ray Data connector

        Args:
            parallelism: The requested read parallelism. The number of read
                tasks should equal to this value if possible.

        Returns:
            A list of read tasks that can be executed to read blocks from the
            datasource in parallel.
        """
        try:
            # Step 1: Get table information if not already available
            if not self._table_info:
                self._get_table_info()

            # Step 2: Get temporary credentials
            if not self._creds_response:
                self._get_creds()

            # Step 3: Configure environment variables
            if not self._runtime_env:
                self._set_env()

        except Exception as e:
            # If Unity Catalog operations fail, create a read task that will fail with
            # a clear error message when executed
            logger.warning(f"Unity Catalog setup failed, creating error task: {e}")

            def read_with_error() -> List[Block]:
                raise RuntimeError(
                    f"Unity Catalog setup failed: {e}. "
                    "This datasource requires valid Unity Catalog credentials and access."
                )

            error_task = ReadTask(
                read_with_error,
                BlockMetadata(
                    num_rows=None,
                    size_bytes=None,
                    schema=None,
                    input_files=None,
                    exec_stats=None,
                ),
            )
            return [error_task]

        # Step 4: Create read tasks based on the specified format
        read_tasks = []

        # For now, create a single read task that will delegate to the appropriate reader
        # In a full implementation, this could create multiple tasks based on parallelism
        # and table partitioning information

        def read_unity_catalog_table() -> List[Block]:
            """Read function that delegates to the appropriate native Ray Data connector.

            This function:
            1. Uses the Unity Catalog credentials and table URL
            2. Delegates to the appropriate reader based on the specified data_format
            3. Returns the data as Ray Data blocks
            """
            import ray

            # Get the table URL from Unity Catalog credentials
            table_url = self._table_url
            if not table_url:
                raise RuntimeError(
                    "No table URL available from Unity Catalog credentials"
                )

            # Use the explicitly specified data_format to determine the reader
            data_format = self.data_format

            # Determine which native Ray Data connector to use based on format
            if data_format == "delta":
                # Check if table has deletion vectors and handle them appropriately
                has_deletion_vectors = self._check_deletion_vectors()

                # Prepare Delta options with deletion vector handling
                delta_options = self.delta_options or {}
                if has_deletion_vectors:
                    logger.info(
                        "Table has deletion vectors - applying deletion vector handling options"
                    )
                    delta_options.update(
                        {
                            "deletion_vector_handling": "ignore",  # Skip deletion vectors
                            "ignore_deletion_vectors": True,
                            "deletion_vector_timeout": 300,  # 5 minutes
                        }
                    )

                # Use native Delta reader with Unity Catalog options
                try:
                    dataset = ray.data.read_delta(
                        table_url,
                        storage_options=self.storage_options,
                        unity_catalog_config=self.unity_catalog_config,
                        options=delta_options,
                        **self.reader_kwargs,
                    )
                except Exception as e:
                    if "DeletionVectors" in str(e) or "deletion" in str(e).lower():
                        logger.warning(
                            "Deletion vector error - trying fallback options"
                        )
                        # Try with more aggressive deletion vector handling
                        fallback_options = {
                            "deletion_vector_handling": "error",  # Fail fast if deletion vectors present
                            "ignore_deletion_vectors": True,
                            "deletion_vector_timeout": 0,  # No timeout
                            **(self.delta_options or {}),
                            **self.reader_kwargs,
                        }
                        dataset = ray.data.read_delta(
                            table_url,
                            storage_options=self.storage_options,
                            unity_catalog_config=self.unity_catalog_config,
                            options=fallback_options,
                        )
                        logger.info("Success with fallback deletion vector options")
                    else:
                        raise e

            elif data_format == "parquet":
                # Use native parquet reader
                dataset = ray.data.read_parquet(
                    table_url,
                    storage_options=self.storage_options,
                    **self.reader_kwargs,
                )

            elif data_format == "csv":
                # Use native CSV reader
                dataset = ray.data.read_csv(
                    table_url,
                    storage_options=self.storage_options,
                    **self.reader_kwargs,
                )

            elif data_format == "json":
                # Use native JSON reader
                dataset = ray.data.read_json(
                    table_url,
                    storage_options=self.storage_options,
                    **self.reader_kwargs,
                )

            elif data_format == "iceberg":
                # Use native Iceberg reader
                dataset = ray.data.read_iceberg(
                    table_url,
                    storage_options=self.storage_options,
                    **self.reader_kwargs,
                )

            elif data_format == "hudi":
                # Use native Hudi reader
                dataset = ray.data.read_hudi(
                    table_url,
                    storage_options=self.storage_options,
                    **self.reader_kwargs,
                )

            else:
                # For other formats, try to use the generic reader
                if data_format in _FILE_FORMAT_TO_RAY_READER:
                    reader_name = _FILE_FORMAT_TO_RAY_READER[data_format]
                    reader_func = getattr(ray.data, reader_name, None)
                    if reader_func:
                        dataset = reader_func(
                            table_url,
                            storage_options=self.storage_options,
                            **self.reader_kwargs,
                        )
                    else:
                        raise ValueError(f"Reader function '{reader_name}' not found")
                else:
                    raise ValueError(f"Unsupported data format: {data_format}")

            # Convert the dataset to blocks
            # Note: This is a simplified approach - in practice, we'd want to
            # handle this more efficiently without materializing the entire dataset
            blocks = []
            for block in dataset.iter_blocks():
                blocks.append(block)

            return blocks

        # Create the read task
        read_task = ReadTask(
            read_unity_catalog_table,
            BlockMetadata(
                num_rows=None,  # Unknown at this point
                size_bytes=None,  # Unknown at this point
                schema=None,  # Will be inferred during read
                input_files=None,  # Unity Catalog table, not files
                exec_stats=None,
            ),
        )

        read_tasks.append(read_task)

        return read_tasks

    def get_table_url(self) -> Optional[str]:
        """Get the current table URL if credentials have been fetched.

        This method is useful for debugging and integration purposes.
        Note that this is not part of the standard Datasource interface.

        Returns:
            The table URL if available, None otherwise.
        """
        return getattr(self, "_table_url", None)

    def get_table_id(self) -> Optional[str]:
        """Get the current table ID if table info has been fetched.

        This method is useful for debugging and integration purposes.
        Note that this is not part of the standard Datasource interface.

        Returns:
            The table ID if available, None otherwise.
        """
        return getattr(self, "_table_id", None)

    def is_ready(self) -> bool:
        """Check if the datasource is ready for read operations.

        This method is useful for debugging and integration purposes.
        Note that this is not part of the standard Datasource interface.

        Returns:
            True if the datasource has fetched table info and credentials.
        """
        return (
            hasattr(self, "_table_info")
            and self._table_info is not None
            and hasattr(self, "_table_url")
            and self._table_url is not None
        )

    def get_inferred_format(self) -> str:
        """Get the data format specified for this datasource.

        Returns:
            The explicitly specified data format.
        """
        return self.data_format

    def get_table_metadata(self) -> Dict[str, Any]:
        """Get comprehensive table metadata from Unity Catalog.

        Returns:
            Dictionary containing table metadata and Unity Catalog information.
        """
        metadata = {
            "table_name": self.table_full_name,
            "data_format": self.data_format,
            "operation": self.operation,
            "unity_operation": getattr(self, "_unity_operation", None),
            "credential_vending_info": self.get_credential_vending_info(),
        }

        if self._table_info:
            metadata.update(
                {
                    "table_id": self._table_info.table_id,
                    "table_type": self._table_info.table_type,
                    "data_source_format": self._table_info.data_source_format,
                    "storage_location": self._table_info.storage_location,
                    "created_at": getattr(self._table_info, "created_at", None),
                    "updated_at": getattr(self._table_info, "updated_at", None),
                }
            )

        if self._creds_response:
            metadata.update(
                {
                    "has_credentials": True,
                    "table_url": self._table_url,
                    "credential_type": list(self._creds_response.keys()),
                }
            )
        else:
            metadata.update(
                {
                    "has_credentials": False,
                }
            )

        return metadata

    def _parse_table_name(self):
        """Parse the table full name to extract catalog, schema, and table information."""
        if not self.table_full_name or not isinstance(self.table_full_name, str):
            raise ValueError("table_full_name must be a non-empty string")

        parts = self.table_full_name.split(".")

        # Validate that we have at least one part
        if not parts or not parts[0]:
            raise ValueError("table_full_name must contain at least a table name")

        # Remove empty parts (handles cases like "catalog..table" or "..table")
        parts = [part for part in parts if part.strip()]

        if len(parts) >= 3:
            self.catalog_name = parts[0]
            self.schema_name = parts[1]
            self.table_name = ".".join(parts[2:])
        elif len(parts) == 2:
            self.catalog_name = "hive_metastore"  # Default catalog
            self.schema_name = parts[0]
            self.table_name = parts[1]
        else:
            self.catalog_name = "hive_metastore"  # Default catalog
            self.schema_name = "default"  # Default schema
            self.table_name = parts[0]

        # Validate that we have valid names
        if not self.table_name or not self.table_name.strip():
            raise ValueError("Invalid table name: table name cannot be empty")
        if not self.schema_name or not self.schema_name.strip():
            raise ValueError("Invalid schema name: schema name cannot be empty")
        if not self.catalog_name or not self.catalog_name.strip():
            raise ValueError("Invalid catalog name: catalog name cannot be empty")

        # Update Unity Catalog config
        self.unity_catalog_config.update(
            {
                "catalog_name": self.catalog_name,
                "schema_name": self.schema_name,
                "table_name": self.table_name,
            }
        )

    def _get_table_info(self) -> TableInfo:
        """Fetch table information from Unity Catalog."""
        url = f"{self.base_url}/api/2.1/unity-catalog/tables/{self.table_full_name}"
        headers = {"Authorization": f"Bearer {self.token}"}

        try:
            resp = requests.get(url, headers=headers, timeout=30)

            if resp.status_code == 200:
                data = resp.json()

                # Validate response structure
                if "table_id" not in data:
                    raise ValueError("Invalid table info response: missing table_id")

                table_info = TableInfo.from_dict(data)
                self._table_info = table_info
                self._table_id = table_info.table_id

                return table_info

            elif resp.status_code == 403:
                # Get detailed error information
                try:
                    error_data = resp.json()
                    error_message = error_data.get("message", "Unknown error")
                    error_code = error_data.get("error_code", "UNKNOWN")
                except:
                    error_message = resp.text[:200]
                    error_code = "PARSE_ERROR"

                logger.error("Unity Catalog Access Denied (403)")
                logger.error(f"Error Code: {error_code}")
                logger.error(f"Error Message: {error_message}")
                logger.error("Required Permissions:")
                logger.error(f"1. EXTERNAL USE SCHEMA on schema '{self.schema_name}'")
                logger.error(f"2. SELECT on table '{self.table_full_name}'")
                logger.error(f"3. USE CATALOG on catalog '{self.catalog_name}'")
                logger.error(f"4. USE SCHEMA on schema '{self.schema_name}'")

                raise RuntimeError(f"Unity Catalog access denied: {error_message}")

            else:
                resp.raise_for_status()

        except requests.exceptions.Timeout:
            logger.error(f"Timeout while fetching table info from {url}")
            raise TimeoutError(
                f"Timeout while fetching table info for {self.table_full_name}"
            )
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed while fetching table info: {e}")
            raise ConnectionError(
                f"Failed to fetch table info for {self.table_full_name}: {e}"
            )
        except (ValueError, KeyError) as e:
            logger.error(f"Invalid response format: {e}")
            raise RuntimeError(
                f"Invalid response format for {self.table_full_name}: {e}"
            )
        except Exception as e:
            logger.error(f"Unexpected error while processing table info: {e}")
            raise RuntimeError(
                f"Failed to process table info for {self.table_full_name}: {e}"
            )

    def _get_creds(self) -> None:
        """Fetch temporary credentials using Unity Catalog credential vending.

        This method implements Unity Catalog credential vending for external system access.
        It requests temporary credentials that inherit the privileges of the Databricks principal.

        The credentials include:
        - Short-lived access tokens for cloud storage
        - Cloud storage location URLs
        - Operation-specific permissions (READ or READ_WRITE)

        Requirements:
        - External data access must be enabled on the metastore
        - Principal must have EXTERNAL USE SCHEMA privilege on the schema
        - Principal must have SELECT permission on the table
        """
        url = f"{self.base_url}/api/2.1/unity-catalog/temporary-table-credentials"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}",
        }

        # Use the operation parameter (READ or READ_WRITE)
        payload = {"table_id": self._table_id, "operation": self.operation}

        try:
            resp = requests.post(url, json=payload, headers=headers, timeout=30)
            resp.raise_for_status()
            creds_data = resp.json()

            # Validate Unity Catalog credential vending response structure
            # According to the API documentation, the response should contain:
            # - url: The cloud storage location URL
            # - credentials: Cloud provider specific credentials
            if "url" not in creds_data:
                raise ValueError("Invalid credentials response: missing 'url' field")

            # Store the full response for cloud provider setup
            self._creds_response = creds_data
            self._table_url = creds_data["url"]

            # Log successful credential acquisition
            logger.info(
                f"Successfully obtained Unity Catalog credentials for {self.table_full_name} "
                f"with operation '{self._unity_operation}'"
            )

        except requests.exceptions.Timeout:
            logger.error(f"Timeout while fetching Unity Catalog credentials from {url}")
            raise RuntimeError(
                f"Timeout while fetching Unity Catalog credentials for {self.table_full_name}"
            )
        except requests.exceptions.RequestException as e:
            logger.error(
                f"Request failed while fetching Unity Catalog credentials: {e}"
            )
            raise RuntimeError(
                f"Failed to fetch Unity Catalog credentials for {self.table_full_name}: {e}"
            )
        except (ValueError, KeyError) as e:
            logger.error(f"Invalid Unity Catalog credentials response format: {e}")
            raise RuntimeError(
                f"Invalid Unity Catalog credentials response for {self.table_full_name}: {e}"
            )
        except Exception as e:
            logger.error(
                f"Unexpected error while processing Unity Catalog credentials: {e}"
            )
            raise RuntimeError(
                f"Failed to process Unity Catalog credentials for {self.table_full_name}: {e}"
            )

    def _set_env(self) -> None:
        """Configure environment variables for cloud provider access using Unity Catalog credentials.

        This method processes the Unity Catalog credential vending response to set up
        cloud provider specific environment variables and storage options.

        The Unity Catalog credential vending response structure varies by cloud provider:
        - AWS: aws_temp_credentials with access_key_id, secret_access_key, session_token
        - Azure: azuresasuri with SAS token
        - GCP: gcp_service_account with service account JSON

        Requirements:
        - Unity Catalog credentials must be fetched first via _get_creds()
        - Cloud provider must be accessible from the current environment
        """
        env_vars = {}
        creds = self._creds_response

        if not creds:
            raise RuntimeError(
                "No Unity Catalog credentials available. Call _get_creds() first."
            )

        try:
            # Handle AWS credentials from Unity Catalog credential vending
            if "aws_temp_credentials" in creds:
                aws = creds["aws_temp_credentials"]
                required_keys = ["access_key_id", "secret_access_key", "session_token"]
                if not all(key in aws for key in required_keys):
                    raise ValueError("Incomplete AWS credentials from Unity Catalog")

                env_vars.update(
                    {
                        "AWS_ACCESS_KEY_ID": aws["access_key_id"],
                        "AWS_SECRET_ACCESS_KEY": aws["secret_access_key"],
                        "AWS_SESSION_TOKEN": aws["session_token"],
                    }
                )

                if self.region:
                    env_vars.update(
                        {
                            "AWS_REGION": self.region,
                            "AWS_DEFAULT_REGION": self.region,
                        }
                    )

                # Update storage options for AWS
                self.storage_options.update(
                    {
                        "aws_region": self.region or aws.get("region"),
                        "aws_access_key_id": aws["access_key_id"],
                        "aws_secret_access_key": aws["secret_access_key"],
                        "aws_session_token": aws["session_token"],
                    }
                )

                logger.info(
                    "Configured AWS credentials from Unity Catalog credential vending"
                )

            # Handle Azure credentials from Unity Catalog credential vending
            elif "azuresasuri" in creds:
                env_vars["AZURE_STORAGE_SAS_TOKEN"] = creds["azuresasuri"]

                # Update storage options for Azure
                self.storage_options.update(
                    {
                        "azure_storage_sas_token": creds["azuresasuri"],
                    }
                )

                logger.info(
                    "Configured Azure SAS token from Unity Catalog credential vending"
                )

            # Handle GCP credentials from Unity Catalog credential vending
            elif "gcp_service_account" in creds:
                gcp_json = creds["gcp_service_account"]
                if not gcp_json:
                    raise ValueError(
                        "Empty GCP service account JSON from Unity Catalog"
                    )

                with tempfile.NamedTemporaryFile(
                    prefix="gcp_sa_", suffix=".json", delete=False
                ) as temp_file:
                    temp_file.write(gcp_json.encode())
                    temp_file.flush()
                    env_vars["GOOGLE_APPLICATION_CREDENTIALS"] = temp_file.name

                    # Update storage options for GCP
                    self.storage_options.update(
                        {
                            "google_application_credentials": temp_file.name,
                        }
                    )

                    logger.info(
                        "Configured GCP service account credentials from Unity Catalog credential vending"
                    )

            else:
                # Log available keys for debugging
                available_keys = list(creds.keys())
                logger.warning(
                    f"Unknown credential type in Unity Catalog response. "
                    f"Available keys: {available_keys}. "
                    f"Expected: aws_temp_credentials, azuresasuri, or gcp_service_account"
                )

                # Try to extract any URL or path information that might be useful
                if "url" in creds:
                    logger.info(f"Unity Catalog provided table URL: {creds['url']}")

                # For unsupported credential types, we'll still try to proceed
                # but the user may need to configure credentials manually
                logger.warning(
                    "Proceeding without cloud provider credentials. "
                    "You may need to configure credentials manually or check Unity Catalog permissions."
                )

            # Set environment variables
            for k, v in env_vars.items():
                os.environ[k] = v

            self._runtime_env = {"env_vars": env_vars}

            if env_vars:
                logger.info(
                    f"Environment configured with {len(env_vars)} variables from Unity Catalog"
                )
            else:
                logger.warning(
                    "No environment variables were configured from Unity Catalog credentials"
                )

        except Exception as e:
            logger.error(
                f"Failed to configure environment variables from Unity Catalog credentials: {e}"
            )
            raise RuntimeError(f"Environment configuration failed: {e}")

    def _check_deletion_vectors(self) -> bool:
        """Check if the Delta table has deletion vectors enabled."""
        try:
            if not self._table_info:
                return False

            # Check delta runtime properties for deletion vector info
            delta_props = self._table_info.delta_runtime_properties_kvpairs
            if delta_props:
                # Look for deletion vector related properties
                deletion_vector_enabled = (
                    delta_props.get("delta.enableDeletionVectors", "false").lower()
                    == "true"
                )
                if deletion_vector_enabled:
                    return True

            # Check table properties
            table_props = self._table_info.properties
            if table_props:
                deletion_vector_enabled = (
                    table_props.get("delta.enableDeletionVectors", "false").lower()
                    == "true"
                )
                if deletion_vector_enabled:
                    return True

            return False

        except Exception as e:
            logger.warning(f"Could not check deletion vectors: {e}")
            return False

    def test_token_permissions(self) -> Dict[str, Any]:
        """Test token permissions and provide debugging information."""
        try:
            # Test basic workspace access
            workspace_url = f"{self.base_url}/api/2.1/workspace/list"
            headers = {"Authorization": f"Bearer {self.token}"}

            resp = requests.get(workspace_url, headers=headers, timeout=30)
            workspace_access = resp.status_code == 200

            # Test Unity Catalog access
            uc_url = f"{self.base_url}/api/2.1/unity-catalog/catalogs"
            resp = requests.get(uc_url, headers=headers, timeout=30)
            uc_access = resp.status_code == 200

            # Test metastore access to check external data access
            metastore_url = f"{self.base_url}/api/2.1/unity-catalog/metastores"
            resp = requests.get(metastore_url, headers=headers, timeout=30)
            metastore_access = resp.status_code == 200

            # Check if external data access is enabled on metastore
            external_data_access_enabled = False
            if metastore_access:
                try:
                    metastore_data = resp.json()
                    if "metastores" in metastore_data and metastore_data["metastores"]:
                        metastore = metastore_data["metastores"][0]
                        external_data_access_enabled = metastore.get(
                            "external_access_enabled", False
                        )
                except:
                    pass

            # Test catalog and schema access
            catalog_name = self.table_full_name.split(".")[0]
            schema_name = self.table_full_name.split(".")[1]

            catalog_url = (
                f"{self.base_url}/api/2.1/unity-catalog/catalogs/{catalog_name}"
            )
            resp = requests.get(catalog_url, headers=headers, timeout=30)
            catalog_access = resp.status_code == 200

            schema_url = f"{self.base_url}/api/2.1/unity-catalog/schemas/{catalog_name}.{schema_name}"
            resp = requests.get(schema_url, headers=headers, timeout=30)
            schema_access = resp.status_code == 200

            # Test specific table access with manifest capabilities
            table_url = (
                f"{self.base_url}/api/2.1/unity-catalog/tables/{self.table_full_name}"
            )
            params = {"include_manifest_capabilities": "true"}
            resp = requests.get(table_url, headers=headers, params=params, timeout=30)
            table_access = resp.status_code == 200

            # Check if table supports credential vending
            credential_vending_support = False
            if table_access:
                try:
                    table_data = resp.json()
                    if "manifest_capabilities" in table_data:
                        capabilities = table_data["manifest_capabilities"]
                        credential_vending_support = capabilities.get(
                            "HAS_DIRECT_EXTERNAL_ENGINE_READ_SUPPORT", False
                        ) or capabilities.get(
                            "HAS_DIRECT_EXTERNAL_ENGINE_WRITE_SUPPORT", False
                        )
                except:
                    pass

            return {
                "workspace_access": workspace_access,
                "unity_catalog_access": uc_access,
                "metastore_access": metastore_access,
                "external_data_access_enabled": external_data_access_enabled,
                "catalog_access": catalog_access,
                "schema_access": schema_access,
                "table_access": table_access,
                "credential_vending_support": credential_vending_support,
                "token_starts_with": self.token[:10] + "...",
                "base_url": self.base_url,
                "table_name": self.table_full_name,
                "catalog_name": catalog_name,
                "schema_name": schema_name,
                "recommendations": [],
            }

        except Exception as e:
            return {
                "error": str(e),
                "token_starts_with": self.token[:10] + "...",
                "base_url": self.base_url,
                "table_name": self.table_full_name,
            }

    def test_credential_flow(self) -> Dict[str, Any]:
        """Test the complete Unity Catalog credential flow."""
        try:
            # Step 1: Get table information
            if not self._table_info:
                self._get_table_info()

            # Step 2: Get temporary credentials
            if not self._creds_response:
                self._get_creds()

            # Step 3: Configure environment variables
            if not self._runtime_env:
                self._set_env()

            return {
                "success": True,
                "table_info": self._table_info,
                "credentials": self._creds_response,
                "environment": self._runtime_env,
                "table_url": self._table_url,
            }

        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "table_info": self._table_info,
                "credentials": self._creds_response,
                "environment": self._runtime_env,
            }

    def _cleanup_temp_files(self) -> None:
        """Clean up temporary files created during credential setup."""
        try:
            if hasattr(self, "_runtime_env") and self._runtime_env:
                env_vars = self._runtime_env.get("env_vars", {})
                gcp_creds_file = env_vars.get("GOOGLE_APPLICATION_CREDENTIALS")
                if gcp_creds_file and os.path.exists(gcp_creds_file):
                    try:
                        os.unlink(gcp_creds_file)
                    except OSError as e:
                        logger.warning(
                            f"Failed to clean up temporary file {gcp_creds_file}: {e}"
                        )
                        # Try to remove the file reference from runtime_env
                        if "env_vars" in self._runtime_env:
                            self._runtime_env["env_vars"].pop(
                                "GOOGLE_APPLICATION_CREDENTIALS", None
                            )
        except Exception as e:
            logger.warning(f"Error during cleanup: {e}")
