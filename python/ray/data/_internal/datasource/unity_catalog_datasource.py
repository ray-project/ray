"""Unity Catalog datasource for Ray Data.

This module provides a comprehensive Unity Catalog connector for loading tables
into Ray Datasets with support for multiple data formats and cloud providers.
"""

import logging
import os
import tempfile
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

import ray
import requests

from .delta_datasource import DeltaDatasource

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

# Storage options for different cloud providers
_CLOUD_STORAGE_OPTIONS = {
    "aws": {
        "aws_region": None,  # Will be set from environment or config
        "aws_access_key_id": None,
        "aws_secret_access_key": None,
        "aws_session_token": None,
    },
    "azure": {
        "azure_storage_account": None,
        "azure_storage_sas_token": None,
        "azure_storage_connection_string": None,
    },
    "gcp": {
        "google_application_credentials": None,
        "gcp_project_id": None,
    },
}


def _read_delta_native(path: str, **kwargs: Any) -> Any:
    """Use the native ray.data.read_delta function with enhanced options.

    This function directly calls the enhanced ray.data.read_delta function
    which now supports all Delta Lake features including deletion vectors,
    time travel, partition filtering, and Unity Catalog integration.
    """
    # The native read_delta function now handles all these features directly
    return ray.data.read_delta(path, **kwargs)


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
        )


class UnityCatalogConnector:
    """Enhanced Unity Catalog connector for loading tables into Ray Datasets.

    Supports comprehensive Delta Lake configurations including:
    - Deletion vector handling
    - Time travel (version-based reading)
    - Advanced partition filtering
    - Memory optimization
    - Cloud provider specific storage options
    - Unity Catalog metadata integration

    Currently supports Databricks-managed Unity Catalog
    Supported formats: delta, parquet, csv, json, txt, images, video, audio, binary, hudi, and more.
    Supports AWS, Azure, and GCP with automatic credential handoff.

    Examples:
        .. testcode::

            connector = UnityCatalogConnector(
                base_url="https://your-workspace.cloud.databricks.com",
                token="your_token_here",
                table_full_name="catalog.schema.table",
                operation="READ"
            )
            dataset = connector.read()
    """

    def __init__(
        self,
        *,
        base_url: str,
        token: str,
        table_full_name: str,
        region: Optional[str] = None,
        data_format: Optional[str] = "delta",
        operation: str = "READ",
        reader_kwargs: Optional[Dict[str, Any]] = None,
        # Enhanced options
        storage_options: Optional[Dict[str, Any]] = None,
        unity_catalog_config: Optional[Dict[str, Any]] = None,
        delta_options: Optional[Dict[str, Any]] = None,
    ):
        """Initialize the Unity Catalog connector.

        Args:
            base_url: The base URL of the Databricks workspace.
            token: The authentication token for the workspace.
            table_full_name: The full name of the table in format catalog.schema.table.
            region: The cloud region for the workspace.
            data_format: The expected data format of the table.
            operation: The operation to perform (READ, WRITE, DELETE).
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

        # Validate operation
        valid_operations = ["READ", "WRITE", "DELETE"]
        if operation not in valid_operations:
            raise ValueError(f"operation must be one of {valid_operations}")

        # Validate data_format if provided
        if data_format:
            data_format = data_format.lower()
            if (
                data_format not in _FILE_FORMAT_TO_RAY_READER
                and data_format not in _FORMAT_ALIASES
            ):
                logger.warning(
                    f"Unknown data_format '{data_format}'. Will attempt auto-detection."
                )

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
            "format_inference_time": None,
            "reader_selection_time": None,
            "read_execution_time": None,
            "total_read_time": None,
        }

        logger.info(f"Initialized UnityCatalogConnector for table: {table_full_name}")

    def _parse_table_name(self):
        """Parse the table full name to extract catalog, schema, and table information."""
        parts = self.table_full_name.split(".")
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
            self.table_name = parts[0] if parts else "unknown"

        # Update Unity Catalog config
        self.unity_catalog_config.update({
            "catalog_name": self.catalog_name,
            "schema_name": self.schema_name,
            "table_name": self.table_name,
        })

    def validate_configuration(self) -> List[str]:
        """Validate the current configuration and return any issues found.

        Returns:
            List of validation error messages. Empty list means configuration is valid.
        """
        errors = []

        # Check required fields
        if not self.base_url:
            errors.append("base_url is required")

        if not self.token:
            errors.append("token is required")

        if not self.table_full_name:
            errors.append("table_full_name is required")

        # Validate base_url format
        if self.base_url and not (
            self.base_url.startswith("http://") or self.base_url.startswith("https://")
        ):
            errors.append("base_url must be a valid HTTP/HTTPS URL")

        # Validate table_full_name format (should be catalog.schema.table)
        if self.table_full_name and len(self.table_full_name.split(".")) != 3:
            errors.append("table_full_name must be in format: catalog.schema.table")

        # Validate operation
        valid_operations = ["READ", "WRITE", "DELETE"]
        if self.operation not in valid_operations:
            errors.append(f"operation must be one of {valid_operations}")

        # Validate data_format if specified
        if self.data_format:
            if (
                self.data_format not in _FILE_FORMAT_TO_RAY_READER
                and self.data_format not in _FORMAT_ALIASES
            ):
                errors.append(f"data_format '{self.data_format}' is not supported")

        # Validate region format for cloud providers
        if self.region:
            # Basic region validation (can be enhanced for specific cloud providers)
            if not isinstance(self.region, str) or len(self.region) < 3:
                errors.append("region must be a valid string")

        # Validate reader_kwargs
        if self.reader_kwargs and not isinstance(self.reader_kwargs, dict):
            errors.append("reader_kwargs must be a dictionary")

        return errors

    def is_configuration_valid(self) -> bool:
        """Check if the current configuration is valid.

        Returns:
            True if configuration is valid, False otherwise.
        """
        return len(self.validate_configuration()) == 0

    def _get_table_info(self) -> TableInfo:
        """Fetch table information from Unity Catalog."""
        url = f"{self.base_url}/api/2.1/unity-catalog/tables/{self.table_full_name}"
        headers = {"Authorization": f"Bearer {self.token}"}

        try:
            logger.debug(f"Fetching table info from: {url}")
            resp = requests.get(url, headers=headers, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            # Validate response structure
            if "table_id" not in data:
                raise ValueError("Invalid table info response: missing table_id")

            table_info = TableInfo.from_dict(data)
            self._table_info = table_info
            self._table_id = table_info.table_id

            logger.info(
                f"Successfully retrieved table info for: {self.table_full_name}"
            )
            return table_info

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
        except Exception as e:
            logger.error(f"Unexpected error while processing table info: {e}")
            raise RuntimeError(
                f"Failed to process table info for {self.table_full_name}: {e}"
            )

    def _get_creds(self) -> None:
        """Fetch temporary credentials for table access."""
        url = f"{self.base_url}/api/2.1/unity-catalog/temporary-table-credentials"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}",
        }
        payload = {"table_id": self._table_id, "operation": self.operation}

        try:
            logger.debug(
                f"Fetching temporary credentials for operation: {self.operation}"
            )
            resp = requests.post(url, json=payload, headers=headers, timeout=30)
            resp.raise_for_status()
            creds_data = resp.json()

            # Validate credentials response
            if "url" not in creds_data:
                raise ValueError("Invalid credentials response: missing url")

            self._creds_response = creds_data
            self._table_url = creds_data["url"]

            logger.info(
                f"Successfully obtained temporary credentials for {self.table_full_name}"
            )

        except requests.exceptions.Timeout:
            logger.error(f"Timeout while fetching credentials from {url}")
            raise RuntimeError(
                f"Timeout while fetching credentials for {self.table_full_name}"
            )
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed while fetching credentials: {e}")
            raise RuntimeError(
                f"Failed to fetch credentials for {self.table_full_name}: {e}"
            )
        except Exception as e:
            logger.error(f"Unexpected error while processing credentials: {e}")
            raise RuntimeError(
                f"Failed to process credentials for {self.table_full_name}: {e}"
            )

    def _set_env(self) -> None:
        """Configure environment variables for cloud provider access."""
        env_vars = {}
        creds = self._creds_response

        try:
            if "aws_temp_credentials" in creds:
                aws = creds["aws_temp_credentials"]
                required_keys = ["access_key_id", "secret_access_key", "session_token"]
                if not all(key in aws for key in required_keys):
                    raise ValueError("Incomplete AWS credentials")

                env_vars["AWS_ACCESS_KEY_ID"] = aws["access_key_id"]
                env_vars["AWS_SECRET_ACCESS_KEY"] = aws["secret_access_key"]
                env_vars["AWS_SESSION_TOKEN"] = aws["session_token"]

                if self.region:
                    env_vars["AWS_REGION"] = self.region
                    env_vars["AWS_DEFAULT_REGION"] = self.region

                # Update storage options for AWS
                self.storage_options.update({
                    "aws_region": self.region or aws.get("region"),
                    "aws_access_key_id": aws["access_key_id"],
                    "aws_secret_access_key": aws["secret_access_key"],
                    "aws_session_token": aws["session_token"],
                })

                logger.info("Configured AWS credentials")

            elif "azuresasuri" in creds:
                env_vars["AZURE_STORAGE_SAS_TOKEN"] = creds["azuresasuri"]

                # Update storage options for Azure
                self.storage_options.update({
                    "azure_storage_sas_token": creds["azuresasuri"],
                })

                logger.info("Configured Azure SAS token")

            elif "gcp_service_account" in creds:
                gcp_json = creds["gcp_service_account"]
                if not gcp_json:
                    raise ValueError("Empty GCP service account JSON")

                with tempfile.NamedTemporaryFile(
                    prefix="gcp_sa_", suffix=".json", delete=False
                ) as temp_file:
                    temp_file.write(gcp_json.encode())
                    temp_file.flush()
                    env_vars["GOOGLE_APPLICATION_CREDENTIALS"] = temp_file.name

                    # Update storage options for GCP
                    self.storage_options.update({
                        "google_application_credentials": temp_file.name,
                    })

                    logger.info(
                        f"Configured GCP service account credentials: {temp_file.name}"
                    )

            else:
                raise ValueError(
                    "No known credential type found in Databricks UC response. "
                    f"Available keys: {list(creds.keys())}"
                )

            # Set environment variables
            for k, v in env_vars.items():
                os.environ[k] = v

            self._runtime_env = {"env_vars": env_vars}
            logger.info(f"Environment configured with {len(env_vars)} variables")

        except Exception as e:
            logger.error(f"Failed to configure environment variables: {e}")
            raise RuntimeError(f"Environment configuration failed: {e}")

    def _infer_data_format(self) -> str:
        """Infer the data format from table metadata and configuration."""
        try:
            # If format is explicitly specified, validate and return it
            if self.data_format:
                logger.debug(f"Using explicitly specified format: {self.data_format}")
                if self.data_format in _FORMAT_ALIASES:
                    resolved_format = _FORMAT_ALIASES[self.data_format]
                    logger.debug(
                        f"Resolved alias '{self.data_format}' to '{resolved_format}'"
                    )
                    return resolved_format
                if self.data_format in _FILE_FORMAT_TO_RAY_READER:
                    return self.data_format
                else:
                    logger.warning(
                        f"Unknown format '{self.data_format}', attempting auto-detection"
                    )

            # Get table info if not already available
            info = self._table_info or self._get_table_info()

            # Try to infer from data_source_format
            if info.data_source_format and info.data_source_format.strip():
                fmt = info.data_source_format.lower().strip()
                logger.debug(f"Detected format from data_source_format: {fmt}")

                if fmt in _FORMAT_ALIASES:
                    resolved_format = _FORMAT_ALIASES[fmt]
                    logger.debug(f"Resolved alias '{fmt}' to '{resolved_format}'")
                    return resolved_format
                if fmt in _FILE_FORMAT_TO_RAY_READER:
                    return fmt
                else:
                    logger.warning(
                        f"Unknown data_source_format '{fmt}', trying other methods"
                    )

            # Try to infer from storage location or table URL
            storage_loc = (
                info.storage_location
                if info.storage_location
                else getattr(self, "_table_url", None)
            )
            if storage_loc:
                ext = os.path.splitext(storage_loc)[-1].replace(".", "").lower()
                logger.debug(f"Attempting to infer format from extension: {ext}")

                if ext in _FILE_FORMAT_TO_RAY_READER:
                    logger.info(f"Inferred format '{ext}' from file extension")
                    return ext
                if ext in _FORMAT_ALIASES:
                    resolved_format = _FORMAT_ALIASES[ext]
                    logger.info(
                        f"Inferred format '{resolved_format}' from alias '{ext}'"
                    )
                    return resolved_format
                else:
                    logger.warning(f"Unknown file extension '{ext}'")

            # Try to infer from table properties or other metadata
            if hasattr(info, "properties") and info.properties:
                # Look for format hints in table properties
                format_hints = ["format", "type", "file_type", "data_type"]
                for hint in format_hints:
                    if hint in info.properties:
                        fmt = info.properties[hint].lower()
                        if fmt in _FILE_FORMAT_TO_RAY_READER:
                            logger.info(
                                f"Inferred format '{fmt}' from table property '{hint}'"
                            )
                            return fmt
                        if fmt in _FORMAT_ALIASES:
                            resolved_format = _FORMAT_ALIASES[fmt]
                            logger.info(
                                f"Inferred format '{resolved_format}' from table property '{hint}'"
                            )
                            return resolved_format

            # If all else fails, provide a detailed error message
            available_formats = list(_FILE_FORMAT_TO_RAY_READER.keys()) + list(
                _FORMAT_ALIASES.keys()
            )
            error_msg = (
                f"Could not infer data format for table '{self.table_full_name}'. "
                f"Available formats: {available_formats}. "
                f"Consider explicitly specifying the data_format parameter."
            )
            logger.error(error_msg)
            raise ValueError(error_msg)

        except Exception as e:
            logger.error(f"Error during format inference: {e}")
            raise RuntimeError(f"Format inference failed: {e}")

    @staticmethod
    def _get_ray_reader(data_format: str) -> Callable[..., Any]:
        """Get the appropriate Ray reader function for the given data format."""
        fmt = data_format.lower()

        # Handle special cases first
        if fmt == "delta":
            # Use the DeltaDatasource for Delta tables
            return lambda path, **kwargs: ray.data.read_datasource(
                DeltaDatasource(path, **kwargs)
            )

        # Check if the format is directly supported
        if fmt in _FILE_FORMAT_TO_RAY_READER:
            reader_name = _FILE_FORMAT_TO_RAY_READER[fmt]
            reader_func = getattr(ray.data, reader_name, None)
            if reader_func:
                return reader_func

        # Check format aliases
        if fmt in _FORMAT_ALIASES:
            alias_format = _FORMAT_ALIASES[fmt]
            if alias_format in _FILE_FORMAT_TO_RAY_READER:
                reader_name = _FILE_FORMAT_TO_RAY_READER[alias_format]
                reader_func = getattr(ray.data, reader_name, None)
                if reader_func:
                    return reader_func

        # Try to find a reader function by checking common patterns
        for pattern, reader_name in _FILE_FORMAT_TO_RAY_READER.items():
            if fmt in pattern or pattern in fmt:
                reader_func = getattr(ray.data, reader_name, None)
                if reader_func:
                    return reader_func

        raise ValueError(
            f"Unsupported data format: '{fmt}'. Supported formats: {list(_FILE_FORMAT_TO_RAY_READER.keys())}"
        )

    def _get_reader_kwargs_with_deletion_vector_support(
        self, data_format: str
    ) -> Dict[str, Any]:
        """Get reader kwargs with comprehensive support for various data formats."""
        kwargs = self.reader_kwargs.copy()

        if data_format == "delta":
            # Ensure we have the basic Delta Lake options available
            # The DeltaDatasource handles these options directly

            # Add other Delta Lake specific options if not present
            if "without_files" not in kwargs:
                kwargs["without_files"] = False  # Default to tracking files
            if "log_buffer_size" not in kwargs:
                kwargs["log_buffer_size"] = None  # Use default (4 * num_cpus)

            # Add partition filtering support
            if "partition_filters" not in kwargs:
                kwargs["partition_filters"] = None

            # Add version support for time travel
            if "version" not in kwargs:
                kwargs["version"] = None

            # Support for DeltaReadConfig if provided
            if "delta_read_config" in kwargs:
                config = kwargs.pop("delta_read_config")
                if hasattr(config, "to_deltatable_args"):
                    # Convert DeltaReadConfig to individual kwargs
                    dt_args = config.to_deltatable_args()
                    kwargs.update(dt_args)

                    # Handle partition filters separately
                    if hasattr(config, "to_partition_filters"):
                        partition_filters = config.to_partition_filters()
                        if partition_filters:
                            kwargs["partition_filters"] = partition_filters

        elif data_format in ["csv", "json", "jsonl", "ndjson"]:
            # Add common structured data options
            if "encoding" not in kwargs:
                kwargs["encoding"] = "utf-8"  # Default encoding

            if data_format == "csv":
                # CSV specific options
                if "delimiter" not in kwargs:
                    kwargs["delimiter"] = ","
                if "header" not in kwargs:
                    kwargs["header"] = "infer"

            elif data_format in ["json", "jsonl", "ndjson"]:
                # JSON specific options
                if "lines" not in kwargs:
                    kwargs["lines"] = True  # Default to JSON Lines format

        elif data_format in ["txt", "text"]:
            # Text file options
            if "encoding" not in kwargs:
                kwargs["encoding"] = "utf-8"
            if "strip" not in kwargs:
                kwargs["strip"] = True  # Remove leading/trailing whitespace

        elif data_format in [
            "image",
            "images",
            "img",
            "jpg",
            "jpeg",
            "png",
            "gif",
            "bmp",
            "tiff",
            "tif",
            "webp",
        ]:
            # Image specific options
            if "size" not in kwargs:
                kwargs["size"] = (224, 224)  # Default image size
            if "mode" not in kwargs:
                kwargs["mode"] = "RGB"  # Default color mode

        elif data_format in [
            "video",
            "videos",
            "mp4",
            "avi",
            "mov",
            "mkv",
            "wmv",
            "flv",
            "webm",
        ]:
            # Video specific options
            if "fps" not in kwargs:
                kwargs["fps"] = 1  # Default frames per second for sampling
            if "max_frames" not in kwargs:
                kwargs["max_frames"] = 100  # Default max frames to extract

        elif data_format in ["audio", "mp3", "wav", "flac", "aac", "ogg", "m4a"]:
            # Audio specific options
            if "sample_rate" not in kwargs:
                kwargs["sample_rate"] = 16000  # Default sample rate
            if "channels" not in kwargs:
                kwargs["channels"] = 1  # Default to mono

        elif data_format in ["binary", "bin", "dat"]:
            # Binary file options
            if "chunk_size" not in kwargs:
                kwargs["chunk_size"] = 1024 * 1024  # Default 1MB chunks

        elif data_format == "hudi":
            # Hudi specific options
            if "table_type" not in kwargs:
                kwargs["table_type"] = "COPY_ON_WRITE"  # Default Hudi table type
            if "use_optimized_reader" not in kwargs:
                kwargs["use_optimized_reader"] = True

        elif data_format == "iceberg":
            # Iceberg specific options
            if "snapshot_id" not in kwargs:
                kwargs["snapshot_id"] = None  # Use latest snapshot by default

        return kwargs

    def _get_enhanced_reader_kwargs(self, data_format: str) -> Dict[str, Any]:
        """Get enhanced reader kwargs with Delta Lake specific options."""
        kwargs = self._get_reader_kwargs_with_deletion_vector_support(data_format)

        if data_format == "delta":
            # Merge default Delta Lake options
            default_options = _DEFAULT_DELTA_OPTIONS.copy()
            default_options.update(self.delta_options)

            # Add enhanced Delta Lake options
            kwargs.update({
                "options": default_options,
                "storage_options": self.storage_options,
                "unity_catalog_config": self.unity_catalog_config,
            })

            # Add other Delta Lake specific options if not present
            if "without_files" not in kwargs:
                kwargs["without_files"] = False  # Default to tracking files
            if "log_buffer_size" not in kwargs:
                kwargs["log_buffer_size"] = None  # Use default (4 * num_cpus)

            # Add partition filtering support
            if "partition_filters" not in kwargs:
                kwargs["partition_filters"] = None

            # Add version support for time travel
            if "version" not in kwargs:
                kwargs["version"] = None

            # Support for DeltaReadConfig if provided
            if "delta_read_config" in kwargs:
                config = kwargs.pop("delta_read_config")
                if hasattr(config, "to_deltatable_args"):
                    # Convert DeltaReadConfig to individual kwargs
                    dt_args = config.to_deltatable_args()
                    kwargs.update(dt_args)

                    # Handle partition filters separately
                    if hasattr(config, "to_partition_filters"):
                        partition_filters = config.to_partition_filters()
                        if partition_filters:
                            kwargs["partition_filters"] = partition_filters

        return kwargs

    def read(self) -> Any:
        """Read data from Unity Catalog table and return a Ray Dataset."""
        start_time = time.time()
        try:
            logger.info(f"Starting read operation for table: {self.table_full_name}")

            # Step 0: Validate configuration
            logger.debug("Step 0: Validating configuration")
            validation_errors = self.validate_configuration()
            if validation_errors:
                error_msg = (
                    f"Configuration validation failed: {', '.join(validation_errors)}"
                )
                logger.error(error_msg)
                raise ValueError(error_msg)

            # Step 1: Get table information
            logger.debug("Step 1: Fetching table information")
            step_start = time.time()
            self._get_table_info()
            self._performance_metrics["table_info_fetch_time"] = (
                time.time() - step_start
            )

            # Step 2: Get temporary credentials
            logger.debug("Step 2: Obtaining temporary credentials")
            step_start = time.time()
            self._get_creds()
            self._performance_metrics["credentials_fetch_time"] = (
                time.time() - step_start
            )

            # Step 3: Configure environment variables
            logger.debug("Step 3: Configuring environment variables")
            step_start = time.time()
            self._set_env()
            self._performance_metrics["environment_setup_time"] = (
                time.time() - step_start
            )

            # Step 4: Initialize Ray with runtime environment
            logger.debug("Step 4: Initializing Ray runtime environment")
            step_start = time.time()
            try:
                ray.init(ignore_reinit_error=True, runtime_env=self._runtime_env)
                logger.debug("Ray runtime environment initialized successfully")
            except Exception as e:
                logger.warning(f"Ray initialization warning (continuing): {e}")
            self._performance_metrics["ray_init_time"] = time.time() - step_start

            # Step 5: Infer data format
            logger.debug("Step 5: Inferring data format")
            step_start = time.time()
            data_format = self._infer_data_format()
            self._performance_metrics["format_inference_time"] = (
                time.time() - step_start
            )
            logger.info(f"Inferred data format: {data_format}")

            # Step 6: Get appropriate reader function
            logger.debug("Step 6: Selecting reader function")
            step_start = time.time()
            reader = self._get_ray_reader(data_format)
            self._performance_metrics["reader_selection_time"] = (
                time.time() - step_start
            )
            logger.debug(
                f"Selected reader: {reader.__name__ if hasattr(reader, '__name__') else type(reader)}"
            )

            # Step 7: Prepare reader arguments
            logger.debug("Step 7: Preparing reader arguments")
            reader_kwargs = self._get_enhanced_reader_kwargs(data_format)
            logger.debug(f"Enhanced reader kwargs: {reader_kwargs}")

            # Step 8: Execute read operation
            logger.debug("Step 8: Executing read operation")
            step_start = time.time()
            url = self._table_url
            ds = reader(url, **reader_kwargs)
            self._performance_metrics["read_execution_time"] = time.time() - step_start

            # Record total time
            self._performance_metrics["total_read_time"] = time.time() - start_time

            logger.info(
                f"Successfully read data from {self.table_full_name}. Dataset type: {type(ds)}"
            )
            logger.info(f"Performance metrics: {self._performance_metrics}")
            return ds

        except Exception as e:
            logger.error(f"Read operation failed for table {self.table_full_name}: {e}")
            # Clean up any temporary files
            self._cleanup_temp_files()
            raise RuntimeError(
                f"Failed to read from Unity Catalog table {self.table_full_name}: {e}"
            )

    def _cleanup_temp_files(self) -> None:
        """Clean up temporary files created during credential setup."""
        try:
            if hasattr(self, "_runtime_env") and self._runtime_env:
                env_vars = self._runtime_env.get("env_vars", {})
                gcp_creds_file = env_vars.get("GOOGLE_APPLICATION_CREDENTIALS")
                if gcp_creds_file and os.path.exists(gcp_creds_file):
                    try:
                        os.unlink(gcp_creds_file)
                        logger.debug(
                            f"Cleaned up temporary GCP credentials file: {gcp_creds_file}"
                        )
                    except OSError as e:
                        logger.warning(
                            f"Failed to clean up temporary file {gcp_creds_file}: {e}"
                        )
        except Exception as e:
            logger.warning(f"Error during cleanup: {e}")

    def __enter__(self) -> "UnityCatalogConnector":
        """Context manager entry."""
        return self

    def __exit__(
        self,
        exc_type: Optional[type],
        exc_val: Optional[Exception],
        exc_tb: Optional[Any]
    ) -> bool:
        """Context manager exit with cleanup."""
        self._cleanup_temp_files()
        if exc_type is not None:
            logger.error(f"Exception in context manager: {exc_val}")
        return False  # Re-raise exceptions

    def get_performance_metrics(self) -> Dict[str, float]:
        """Get performance metrics for the last read operation.

        Returns:
            Dictionary containing timing information for each step of the read process.
        """
        return self._performance_metrics.copy()

    def get_table_metadata(self) -> Dict[str, Any]:
        """Get comprehensive table metadata including Unity Catalog information."""
        info = self._table_info or self._get_table_info()

        metadata = {
            "table_info": info,
            "unity_catalog": self.unity_catalog_config,
            "storage_options": self.storage_options,
            "data_format": self._infer_data_format(),
        }

        return metadata

    def get_performance_summary(self) -> str:
        """Get a human-readable performance summary.

        Returns:
            Formatted string with performance metrics.
        """
        if not self._performance_metrics["total_read_time"]:
            return "No performance metrics available. Run a read operation first."

        summary = f"Performance Summary for {self.table_full_name}:\n"
        summary += "=" * 50 + "\n"

        for step, time_taken in self._performance_metrics.items():
            if time_taken is not None:
                step_name = step.replace("_", " ").title()
                summary += f"{step_name}: {time_taken:.3f}s\n"

        return summary

    def reset_performance_metrics(self):
        """Reset all performance metrics to None."""
        for key in self._performance_metrics:
            self._performance_metrics[key] = None
        logger.debug("Performance metrics reset")
