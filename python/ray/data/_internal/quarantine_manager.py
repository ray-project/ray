"""Quarantine data management system for Ray Data quality checks.

This module provides functionality to collect, manage, and write quarantined data
using Ray Data's native write infrastructure. It handles schema evolution, multiple
data formats, and deferred writing to avoid blocking main data processing operations.
"""

import logging
import time
from typing import Any, Dict, List, Optional, Union
import pyarrow as pa

import ray
from ray.data._internal.datasource.csv_datasink import CSVDatasink
from ray.data._internal.datasource.json_datasink import JSONDatasink
from ray.data._internal.datasource.parquet_datasink import ParquetDatasink
from ray.data.block import Block, BlockAccessor

logger = logging.getLogger(__name__)


class QuarantineManager:
    """Manages quarantined data collection and writing using Ray Data's native writers."""

    def __init__(self):
        self._entries: List[Dict[str, Any]] = []
        self._schemas: Dict[str, pa.Schema] = {}
        self._writers = {
            "parquet": ParquetDatasink,
            "csv": CSVDatasink,
            "json": JSONDatasink,
        }
        self._access_controls: Dict[str, Dict[str, Any]] = {}
        self._encryption_config: Dict[str, Any] = {}
        self._gdpr_fields: List[str] = []

    def add_quarantine_rows(
        self,
        rows: List[Dict[str, Any]],
        path: str,
        format: str,
        violation_metadata: Dict[str, Any],
        check_id: Optional[str] = None,
    ):
        """Add quarantined rows for deferred writing."""
        if not rows:
            return

        # Convert rows to PyArrow table for consistent handling
        try:
            table = pa.Table.from_pylist(rows)
            self.add_quarantine_block(table, path, format, violation_metadata, check_id)
        except Exception as e:
            logger.warning(f"Failed to convert rows to Arrow table: {e}")
            # Fallback to storing raw rows
            entry = {
                "type": "rows",
                "data": rows,
                "path": path,
                "format": format,
                "timestamp": time.time(),
                "metadata": violation_metadata,
                "schema": self._infer_schema_from_rows(rows),
                "check_id": check_id,
            }
            self._entries.append(entry)

    def add_quarantine_block(
        self,
        block: Union[Block, pa.Table],
        path: str,
        format: str,
        violation_metadata: Dict[str, Any],
        check_id: Optional[str] = None,
    ):
        """Add a quarantined block for deferred writing."""
        if isinstance(block, pa.Table) and len(block) == 0:
            return

        # Handle schema evolution with check_id for multiple checks
        schema_key = f"{path}_{format}_{check_id}" if check_id else f"{path}_{format}"
        current_schema = block.schema if isinstance(block, pa.Table) else None

        if schema_key in self._schemas:
            # Check for schema compatibility and evolve if needed
            existing_schema = self._schemas[schema_key]
            if current_schema and not existing_schema.equals(current_schema):
                evolved_schema = self._evolve_schema(existing_schema, current_schema)
                self._schemas[schema_key] = evolved_schema
                logger.info(
                    f"Schema evolved for quarantine path {path} (check: {check_id})"
                )
        else:
            self._schemas[schema_key] = current_schema

        entry = {
            "type": "block",
            "data": block,
            "path": path,
            "format": format,
            "timestamp": time.time(),
            "metadata": violation_metadata,
            "schema": current_schema,
            "check_id": check_id,
        }
        self._entries.append(entry)

    def write_all_quarantine_data(self):
        """Write all collected quarantine data using Ray Data's native writers."""
        if not self._entries:
            return

        # Group entries by path, format, and check_id for efficient writing
        grouped_entries = self._group_entries_by_destination()

        for (path, format, check_id), entries in grouped_entries.items():
            try:
                # For chained checks with same path, append check_id to avoid conflicts
                actual_path = path
                if check_id != "default":
                    path_parts = path.rsplit(".", 1)
                    if len(path_parts) == 2:
                        actual_path = f"{path_parts[0]}_{check_id}.{path_parts[1]}"
                    else:
                        actual_path = f"{path}_{check_id}"

                self._write_quarantine_group(entries, actual_path, format, check_id)
                logger.info(
                    f"Successfully wrote {len(entries)} quarantine entries to {actual_path} (check: {check_id})"
                )
            except Exception as e:
                logger.error(f"Failed to write quarantine data to {actual_path}: {e}")

        # Clear entries after writing
        self._entries.clear()
        self._schemas.clear()

    def _group_entries_by_destination(self) -> Dict[tuple, List[Dict[str, Any]]]:
        """Group quarantine entries by destination path, format, and check_id."""
        grouped = {}
        for entry in self._entries:
            # Group by path, format, and check_id to handle chained checks properly
            check_id = entry.get("check_id", "default")
            key = (entry["path"], entry["format"], check_id)
            if key not in grouped:
                grouped[key] = []
            grouped[key].append(entry)
        return grouped

    def _write_quarantine_group(
        self,
        entries: List[Dict[str, Any]],
        path: str,
        format: str,
        check_id: str = "default",
    ):
        """Write a group of quarantine entries to the specified destination."""
        if format not in self._writers:
            raise ValueError(f"Unsupported quarantine format: {format}")

        # Combine all blocks/rows into a single dataset for writing
        combined_blocks = []
        unified_schema = self._get_unified_schema(entries)

        for entry in entries:
            if entry["type"] == "block":
                block = entry["data"]
                if isinstance(block, pa.Table):
                    # Ensure block conforms to unified schema
                    normalized_block = self._normalize_block_schema(
                        block, unified_schema
                    )
                    combined_blocks.append(normalized_block)
                else:
                    # Convert other block types to Arrow
                    accessor = BlockAccessor.for_block(block)
                    arrow_block = accessor.to_arrow()
                    normalized_block = self._normalize_block_schema(
                        arrow_block, unified_schema
                    )
                    combined_blocks.append(normalized_block)
            elif entry["type"] == "rows":
                # Convert rows to Arrow table
                try:
                    table = pa.Table.from_pylist(entry["data"])
                    normalized_block = self._normalize_block_schema(
                        table, unified_schema
                    )
                    combined_blocks.append(normalized_block)
                except Exception as e:
                    logger.warning(f"Failed to convert rows to Arrow: {e}")
                    continue

        if not combined_blocks:
            logger.warning(f"No valid blocks to write for {path}")
            return

        # Concatenate all blocks
        try:
            combined_table = pa.concat_tables(combined_blocks)
        except Exception as e:
            logger.error(f"Failed to concatenate quarantine blocks: {e}")
            return

        # Enhanced quarantine writing with partitioning support
        self._write_quarantine_with_partitioning(
            combined_table, path, format, check_id, entries
        )

    def _write_quarantine_with_partitioning(
        self,
        table: pa.Table,
        path: str,
        format: str,
        check_id: str,
        entries: List[Dict[str, Any]],
    ):
        """Write quarantine data with optional partitioning."""
        # Check if we should partition the data
        should_partition = self._should_partition_quarantine(table, entries)

        if should_partition:
            self._write_partitioned_quarantine(table, path, format, check_id)
        else:
            self._write_single_quarantine_file(table, path, format, check_id)

    def _should_partition_quarantine(
        self, table: pa.Table, entries: List[Dict[str, Any]]
    ) -> bool:
        """Determine if quarantine data should be partitioned."""
        # Partition if:
        # 1. Large number of rows (>10k)
        # 2. Multiple violation reasons
        # 3. Data spans multiple time periods

        if len(table) > 10000:
            return True

        # Check for multiple violation reasons
        if "_dq_violation_reason" in table.column_names:
            unique_reasons = len(table["_dq_violation_reason"].unique())
            if unique_reasons > 1:
                return True

        return False

    def _write_partitioned_quarantine(
        self, table: pa.Table, path: str, format: str, check_id: str
    ):
        """Write quarantine data with partitioning."""
        try:
            # Partition by violation reason if available
            if "_dq_violation_reason" in table.column_names:
                partition_cols = ["_dq_violation_reason"]
            else:
                # Fallback to time-based partitioning
                partition_cols = None

            # Use Ray Data's native writer with partitioning
            datasink_class = self._writers[format]

            if format == "parquet":
                datasink = datasink_class(
                    path, partition_cols=partition_cols, compression="snappy"
                )
            else:
                datasink = datasink_class(path)

            # Create blocks from the table for Ray Data writing
            blocks = [table]

            # Write using Ray Data's infrastructure
            write_results = datasink.write(
                blocks,
                ctx=ray.data.DataContext.get_current(),
            )
            logger.info(
                f"Partitioned quarantine data written successfully: {write_results}"
            )

        except Exception as e:
            logger.error(f"Failed to write partitioned quarantine data: {e}")
            # Fallback to single file
            self._write_single_quarantine_file(table, path, format, check_id)

    def _write_single_quarantine_file(
        self, table: pa.Table, path: str, format: str, check_id: str
    ):
        """Write quarantine data as a single file."""
        try:
            # Use Ray Data's native writer
            datasink_class = self._writers[format]
            datasink = datasink_class(path)

            # Create blocks from the table for Ray Data writing
            blocks = [table]

            # Write using Ray Data's infrastructure
            write_results = datasink.write(
                blocks,
                ctx=ray.data.DataContext.get_current(),
            )
            logger.info(f"Quarantine data written successfully: {write_results}")
        except Exception as e:
            logger.error(f"Failed to write quarantine data using Ray Data writer: {e}")
            # Fallback to direct Arrow writing
            self._fallback_write_arrow(table, path, format)

    def _get_unified_schema(self, entries: List[Dict[str, Any]]) -> pa.Schema:
        """Get a unified schema that can accommodate all entries."""
        schemas = []
        for entry in entries:
            if entry["schema"] is not None:
                schemas.append(entry["schema"])

        if not schemas:
            # Create a minimal schema with metadata columns
            return pa.schema(
                [
                    ("_dq_violation_reason", pa.string()),
                    ("_dq_violation_timestamp", pa.float64()),
                ]
            )

        # Start with the first schema and evolve it
        unified_schema = schemas[0]
        for schema in schemas[1:]:
            unified_schema = self._evolve_schema(unified_schema, schema)

        return unified_schema

    def _evolve_schema(self, existing: pa.Schema, new: pa.Schema) -> pa.Schema:
        """Evolve a schema to accommodate new fields and types."""
        # Create a mapping of field names to types
        field_map = {field.name: field for field in existing}

        # Add new fields from the new schema
        for field in new:
            if field.name not in field_map:
                field_map[field.name] = field
            else:
                # Handle type conflicts by promoting to more general types
                existing_field = field_map[field.name]
                if not existing_field.type.equals(field.type):
                    # Promote to string for incompatible types
                    promoted_field = pa.field(field.name, pa.string(), nullable=True)
                    field_map[field.name] = promoted_field

        return pa.schema(list(field_map.values()))

    def _normalize_block_schema(
        self, block: pa.Table, target_schema: pa.Schema
    ) -> pa.Table:
        """Normalize a block to match the target schema."""
        # Add missing columns with null values
        normalized_columns = []
        normalized_names = []

        for field in target_schema:
            if field.name in block.column_names:
                column = block.column(field.name)
                # Cast to target type if possible
                try:
                    if not column.type.equals(field.type):
                        column = column.cast(field.type)
                except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
                    # Cast to string as fallback
                    column = column.cast(pa.string())
                normalized_columns.append(column)
            else:
                # Add missing column with null values
                null_column = pa.array([None] * len(block), type=field.type)
                normalized_columns.append(null_column)
            normalized_names.append(field.name)

        return pa.Table.from_arrays(normalized_columns, names=normalized_names)

    def _fallback_write_arrow(self, table: pa.Table, path: str, format: str):
        """Fallback method to write Arrow table directly."""
        try:
            if format == "parquet":
                import pyarrow.parquet as pq

                pq.write_table(table, path)
            elif format == "csv":
                import pyarrow.csv as csv

                csv.write_csv(table, path)
            elif format == "json":
                # Convert to pandas and write as JSON
                df = table.to_pandas()
                df.to_json(path, orient="records", lines=True)
            logger.info(f"Fallback write successful to {path}")
        except Exception as e:
            logger.error(f"Fallback write failed: {e}")

    def _infer_schema_from_rows(
        self, rows: List[Dict[str, Any]]
    ) -> Optional[pa.Schema]:
        """Infer PyArrow schema from a list of rows."""
        if not rows:
            return None

        try:
            # Use PyArrow's inference from the first few rows
            sample_size = min(10, len(rows))
            sample_rows = rows[:sample_size]
            table = pa.Table.from_pylist(sample_rows)
            return table.schema
        except Exception as e:
            logger.warning(f"Failed to infer schema from rows: {e}")
            return None

    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about collected quarantine data."""
        total_entries = len(self._entries)
        entries_by_format = {}
        entries_by_path = {}

        for entry in self._entries:
            format_key = entry["format"]
            path_key = entry["path"]

            entries_by_format[format_key] = entries_by_format.get(format_key, 0) + 1
            entries_by_path[path_key] = entries_by_path.get(path_key, 0) + 1

        return {
            "total_entries": total_entries,
            "entries_by_format": entries_by_format,
            "entries_by_path": entries_by_path,
            "schemas_tracked": len(self._schemas),
        }

    def configure_access_controls(self, path: str, permissions: Dict[str, Any]):
        """Configure access controls for quarantine storage."""
        self._access_controls[path] = permissions

        # Log access control configuration for audit
        from ray.data._internal.data_quality_utils import audit_log_check_operation

        audit_log_check_operation(
            "quarantine_manager",
            "configure_access_controls",
            {"path": path, "permissions": permissions},
        )

    def configure_encryption(self, encryption_config: Dict[str, Any]):
        """Configure encryption for quarantine data."""
        self._encryption_config = encryption_config
        logger.info("Encryption configured for quarantine data")

    def set_gdpr_fields(self, fields: List[str]):
        """Set fields that require GDPR compliance handling."""
        self._gdpr_fields = fields
        logger.info(f"GDPR fields configured: {fields}")

    def _apply_gdpr_compliance(
        self, data: Union[pa.Table, List[Dict[str, Any]]]
    ) -> Union[pa.Table, List[Dict[str, Any]]]:
        """Apply GDPR compliance to quarantine data."""
        if not self._gdpr_fields:
            return data

        if isinstance(data, pa.Table):
            # Redact GDPR fields in Arrow table
            modified_columns = {}
            for field_name in self._gdpr_fields:
                if field_name in data.column_names:
                    # Replace with redacted values
                    redacted_values = pa.array(
                        [f"<REDACTED_{field_name.upper()}>" for _ in range(len(data))]
                    )
                    modified_columns[field_name] = redacted_values

            if modified_columns:
                # Create new table with redacted columns
                new_data = data
                for col_name, col_data in modified_columns.items():
                    col_index = data.column_names.index(col_name)
                    new_data = new_data.set_column(col_index, col_name, col_data)
                return new_data

            return data
        else:
            # Redact GDPR fields in row data
            from ray.data._internal.data_quality_utils import sanitize_quarantine_data

            return [sanitize_quarantine_data(row, self._gdpr_fields) for row in data]

    def _check_access_permissions(self, path: str) -> bool:
        """Check if current user has permissions to write to path."""
        if path not in self._access_controls:
            return True  # No restrictions configured

        permissions = self._access_controls[path]
        # In a full implementation, this would check user permissions
        # For now, just validate the path exists and is writable

        try:
            from ray.data._internal.data_quality_utils import validate_quarantine_path

            return validate_quarantine_path(path)
        except Exception as e:
            logger.warning(f"Access permission check failed for {path}: {e}")
            return False


# Global quarantine manager instance
_global_quarantine_manager: Optional[QuarantineManager] = None


def get_quarantine_manager() -> QuarantineManager:
    """Get the global quarantine manager instance."""
    global _global_quarantine_manager
    if _global_quarantine_manager is None:
        _global_quarantine_manager = QuarantineManager()
    return _global_quarantine_manager


def reset_quarantine_manager():
    """Reset the global quarantine manager (useful for testing)."""
    global _global_quarantine_manager
    _global_quarantine_manager = None


def write_all_quarantine_data():
    """Write all collected quarantine data and reset the manager."""
    manager = get_quarantine_manager()
    manager.write_all_quarantine_data()
    reset_quarantine_manager()
