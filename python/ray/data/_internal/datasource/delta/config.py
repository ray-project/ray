"""
Configuration classes and enums for Delta Lake datasource.

This module contains all the configuration data classes and enums used
by the Delta Lake datasource implementation.
"""

import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Mapping, Optional, Tuple, Union

import pyarrow as pa


class WriteMode(Enum):
    # Error if the table already exists
    ERROR = "error"
    # Append to the table if it exists
    APPEND = "append"
    # Overwrite the table if it exists
    OVERWRITE = "overwrite"
    # Ignore the write if the table already exists
    IGNORE = "ignore"
    # Merge the table if it exists
    MERGE = "merge"


class MergeMode(Enum):
    """Merge operation modes."""

    UPSERT = "upsert"  # Insert if not exists, update if exists
    SCD_TYPE_1 = "scd_type_1"  # Slowly Changing Dimensions Type 1 (overwrite)
    SCD_TYPE_2 = "scd_type_2"  # Slowly Changing Dimensions Type 2 (versioning)
    INSERT_ONLY = "insert_only"  # Insert only new records
    UPDATE_ONLY = "update_only"  # Update only existing records
    DELETE = "delete"  # Delete matching records
    CUSTOM = "custom"  # Custom merge logic with user-defined conditions


class OptimizationMode(Enum):
    """Table optimization modes."""

    COMPACT = "compact"  # File compaction
    Z_ORDER = "z_order"  # Z-order optimization
    VACUUM = "vacuum"  # Remove old files


@dataclass
class AddAction:
    path: str
    size: int
    partition_values: Mapping[str, Optional[str]]
    modification_time: int
    data_change: bool
    stats: str


@dataclass
class SCDConfig:
    """Configuration for Slowly Changing Dimensions operations."""

    scd_type: Optional[int] = None  # 1, 2, or 3
    key_columns: List[str] = None  # Business key columns for matching

    # SCD Type 1 settings
    overwrite_columns: Optional[List[str]] = None  # Columns to overwrite in Type 1

    # SCD Type 2 settings
    version_column: str = "version"  # Version tracking column
    current_flag_column: str = "is_current"  # Current record flag
    start_time_column: str = "start_time"  # Valid from timestamp
    end_time_column: str = "end_time"  # Valid to timestamp
    hash_columns: Optional[List[str]] = None  # Columns to hash for change detection

    # SCD Type 3 settings
    previous_value_suffix: str = "_previous"  # Suffix for previous value columns
    change_columns: Optional[List[str]] = None  # Columns to track changes for

    def __post_init__(self):
        """Validate SCD configuration."""
        if self.scd_type is not None and self.scd_type not in [1, 2, 3]:
            raise ValueError("scd_type must be 1, 2, or 3")

        if self.scd_type in [1, 2, 3] and not self.key_columns:
            raise ValueError(f"key_columns required for SCD Type {self.scd_type}")


@dataclass
class MergeConditions:
    """Configuration for merge operation conditions (DeltaMergeBuilder)."""

    # Main merge predicate
    merge_predicate: str

    # When matched conditions
    when_matched_update: Optional[Dict[str, str]] = None  # column -> expression
    when_matched_update_condition: Optional[
        str
    ] = None  # Additional condition for update
    when_matched_delete: bool = False
    when_matched_delete_condition: Optional[str] = None  # Condition for delete

    # When not matched by target conditions
    when_not_matched_insert: Optional[Dict[str, str]] = None  # column -> expression
    when_not_matched_insert_condition: Optional[str] = None  # Condition for insert

    # When not matched by source conditions
    when_not_matched_by_source_update: Optional[Dict[str, str]] = None
    when_not_matched_by_source_update_condition: Optional[str] = None
    when_not_matched_by_source_delete: bool = False
    when_not_matched_by_source_delete_condition: Optional[str] = None


@dataclass
class MergeConfig:
    """Enhanced configuration for merge operations with DeltaMergeBuilder functionality."""

    # Basic configuration
    source_alias: str = "source"
    target_alias: str = "target"
    error_on_type_mismatch: bool = True

    # Retry configuration for concurrent write conflicts
    max_retry_attempts: int = 3  # Maximum number of retry attempts for merge conflicts
    base_retry_delay: float = (
        0.1  # Base delay in seconds between retries (exponential backoff)
    )
    max_retry_delay: float = 5.0  # Maximum delay in seconds between retries

    # SCD configuration (optional)
    scd_config: Optional[SCDConfig] = None

    # DeltaMergeBuilder merge conditions (optional)
    merge_conditions: Optional[MergeConditions] = None

    # Simple upsert configuration (legacy compatibility)
    mode: MergeMode = MergeMode.UPSERT
    predicate: Optional[str] = None
    update_columns: Optional[Dict[str, str]] = None
    insert_columns: Optional[Dict[str, str]] = None
    delete_predicate: Optional[str] = None

    def __post_init__(self):
        """Validate merge configuration."""
        if self.scd_config and self.merge_conditions:
            raise ValueError("Cannot specify both scd_config and merge_conditions")

        # Validate retry parameters
        if self.max_retry_attempts < 0:
            raise ValueError(
                f"max_retry_attempts must be non-negative, got {self.max_retry_attempts}"
            )
        if self.base_retry_delay < 0:
            raise ValueError(
                f"base_retry_delay must be non-negative, got {self.base_retry_delay}"
            )
        if self.max_retry_delay < self.base_retry_delay:
            raise ValueError(
                f"max_retry_delay ({self.max_retry_delay}) must be >= base_retry_delay ({self.base_retry_delay})"
            )
        if self.max_retry_attempts > 10:
            import warnings

            warnings.warn(
                f"max_retry_attempts={self.max_retry_attempts} is very high and may cause long delays. "
                "Consider using 3-5 retries for most use cases.",
                UserWarning,
            )


@dataclass
class OptimizationConfig:
    """
    Configuration for table optimization operations.

    Note: For vacuum operations, files are always deleted (no dry-run mode).
    Use the standalone vacuum_delta_table function for preview/dry-run functionality.
    """

    mode: OptimizationMode = OptimizationMode.COMPACT
    target_size_bytes: Optional[int] = None  # Target file size for compaction
    z_order_columns: Optional[List[str]] = None  # Columns for Z-order optimization
    max_concurrent_tasks: Optional[int] = None  # Max parallel tasks
    min_commit_interval: Optional[Union[int, timedelta]] = None  # Commit interval
    partition_filters: Optional[List[Tuple[str, str, Any]]] = None  # Partition filters
    retention_hours: Optional[int] = None  # Vacuum retention period
    max_spill_size: int = 21474836480  # 20GB default for Z-order


@dataclass
class DeltaSinkWriteResult:
    actions: List[AddAction]
    schema: Optional[pa.Schema] = None
    merge_metrics: Optional[Dict[str, Any]] = None
    optimization_metrics: Optional[Dict[str, Any]] = None


class DeltaJSONEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
        if isinstance(obj, bytes):
            return obj.decode("unicode_escape", "backslashreplace")
        elif isinstance(obj, date):
            return obj.isoformat()
        elif isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, Decimal):
            return str(obj)


@dataclass
class DeltaWriteConfig:
    """Configuration for Delta Lake write operations."""

    mode: WriteMode = WriteMode.APPEND
    partition_cols: Optional[List[str]] = None
    schema_mode: str = "merge"
    name: Optional[str] = None
    description: Optional[str] = None
    configuration: Optional[Dict[str, str]] = None
    custom_metadata: Optional[Dict[str, str]] = None
    target_file_size: Optional[int] = None
    writer_properties: Optional[Dict[str, str]] = None
    post_commithook_properties: Optional[Dict[str, str]] = None
    commit_properties: Optional[Dict[str, str]] = None
    storage_options: Optional[Dict[str, str]] = None
    engine: str = "rust"
    overwrite_schema: bool = False
    schema: Optional[pa.Schema] = None
    merge_config: Optional[MergeConfig] = None
    optimization_config: Optional[OptimizationConfig] = None
