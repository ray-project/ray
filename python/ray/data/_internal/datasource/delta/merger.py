"""
Table merge functionality for Delta Lake datasource.

This module contains the DeltaTableMerger class which handles
various merge operations including SCD, upserts, and complex merges.
"""

import logging
from typing import Any, Dict, Optional

import deltalake as dl
import pyarrow as pa

from ray.data._internal.datasource.delta.config import MergeConfig, MergeMode, SCDConfig

logger = logging.getLogger(__name__)


class DeltaTableMerger:
    """Handles merge operations for Delta tables."""

    def __init__(
        self,
        table_uri: str,
        storage_options: Optional[Dict[str, str]] = None,
    ):
        """
        Initialize Delta table merger.

        Args:
            table_uri: Path to the Delta table
            storage_options: Storage options for the filesystem
        """
        self.table_uri = table_uri
        self.storage_options = storage_options or {}

    def execute_merge(
        self, source_data: pa.Table, config: MergeConfig
    ) -> Dict[str, Any]:
        """
        Execute merge operation based on configuration.

        Args:
            source_data: Source data as PyArrow Table
            config: Merge configuration

        Returns:
            Dict with merge metrics

        Raises:
            ValueError: If merge configuration is invalid
            Exception: If merge operation fails after all retry attempts
        """
        import random
        import time

        import deltalake as dl

        # Use configurable retry parameters
        max_retries = config.max_retry_attempts
        base_delay = config.base_retry_delay
        max_delay = config.max_retry_delay

        for attempt in range(max_retries + 1):
            try:
                # Get fresh table instance for each attempt
                dt = dl.DeltaTable(self.table_uri, storage_options=self.storage_options)

                # Determine merge strategy based on configuration
                if config.scd_config:
                    return self._execute_scd_merge(dt, source_data, config)
                elif config.merge_conditions:
                    return self._execute_delta_merge_builder(dt, source_data, config)
                else:
                    return self._execute_simple_merge(dt, source_data, config)

            except Exception as e:
                if attempt < max_retries:
                    # Exponential backoff with jitter
                    delay = min(base_delay * (2**attempt), max_delay)
                    jitter = random.uniform(0, 0.1) * delay
                    total_delay = delay + jitter

                    logger.warning(
                        f"Merge attempt {attempt + 1} failed: {e}. "
                        f"Retrying in {total_delay:.2f} seconds..."
                    )
                    time.sleep(total_delay)
                else:
                    logger.error(f"Merge failed after {max_retries + 1} attempts: {e}")
                    raise

        # This should never be reached, but included for completeness
        raise Exception("Merge failed: Unexpected exit from retry loop")

    def _execute_scd_merge(
        self, dt: dl.DeltaTable, source_data: pa.Table, config: MergeConfig
    ) -> Dict[str, Any]:
        """Execute SCD (Slowly Changing Dimensions) merge."""
        scd_config = config.scd_config

        if scd_config.scd_type == 1:
            return self._execute_scd_type_1(dt, source_data, scd_config)
        elif scd_config.scd_type == 2:
            return self._execute_scd_type_2(dt, source_data, scd_config)
        elif scd_config.scd_type == 3:
            return self._execute_scd_type_3(dt, source_data, scd_config)
        else:
            raise ValueError(f"Unsupported SCD type: {scd_config.scd_type}")

    def _execute_scd_type_1(
        self, dt: dl.DeltaTable, source_data: pa.Table, scd_config: SCDConfig
    ) -> Dict[str, Any]:
        """Execute SCD Type 1 merge (overwrite changed attributes)."""
        # Build key predicate
        key_predicate = " AND ".join(
            [f"target.{col} = source.{col}" for col in scd_config.key_columns]
        )

        # Build update expressions
        if scd_config.overwrite_columns:
            update_exprs = {
                col: f"source.{col}" for col in scd_config.overwrite_columns
            }
        else:
            # Update all non-key columns
            source_columns = set(source_data.schema.names)
            key_columns = set(scd_config.key_columns)
            update_columns = source_columns - key_columns
            update_exprs = {col: f"source.{col}" for col in update_columns}

        # Build insert expressions
        insert_exprs = {col: f"source.{col}" for col in source_data.schema.names}

        # Execute merge
        merge_builder = (
            dt.merge(
                source=source_data,
                predicate=key_predicate,
                source_alias="source",
                target_alias="target",
            )
            .when_matched_update(set=update_exprs)
            .when_not_matched_insert(values=insert_exprs)
        )

        result = merge_builder.execute()

        return {
            "scd_type": 1,
            "key_columns": scd_config.key_columns,
            "updated_columns": list(update_exprs.keys()),
            "merge_result": result,
        }

    def _execute_scd_type_2(
        self, dt: dl.DeltaTable, source_data: pa.Table, scd_config: SCDConfig
    ) -> Dict[str, Any]:
        """Execute SCD Type 2 merge (track history with versioning)."""
        import pandas as pd
        from datetime import datetime

        # Add SCD Type 2 columns to source data if not present
        source_df = source_data.to_pandas()

        if scd_config.version_column not in source_df.columns:
            source_df[scd_config.version_column] = 1
        if scd_config.current_flag_column not in source_df.columns:
            source_df[scd_config.current_flag_column] = True
        if scd_config.start_time_column not in source_df.columns:
            source_df[scd_config.start_time_column] = datetime.now()
        if scd_config.end_time_column not in source_df.columns:
            source_df[scd_config.end_time_column] = None

        # Convert back to PyArrow
        source_data_with_scd = pa.Table.from_pandas(source_df)

        # Build key predicate
        key_predicate = " AND ".join(
            [f"target.{col} = source.{col}" for col in scd_config.key_columns]
        )

        # For SCD Type 2, we need to:
        # 1. Update existing records to mark them as not current
        # 2. Insert new versions of changed records
        # 3. Insert completely new records

        # First, update existing records to close them
        update_exprs = {
            scd_config.current_flag_column: "false",
            scd_config.end_time_column: f"'{datetime.now().isoformat()}'",
        }

        # Build condition to detect changes (if hash columns specified)
        change_condition = None
        if scd_config.hash_columns:
            hash_conditions = []
            for col in scd_config.hash_columns:
                hash_conditions.append(f"target.{col} != source.{col}")
            change_condition = " OR ".join(hash_conditions)

        # Execute the merge
        merge_builder = dt.merge(
            source=source_data_with_scd,
            predicate=key_predicate,
            source_alias="source",
            target_alias="target",
        )

        if change_condition:
            merge_builder = merge_builder.when_matched_update(
                condition=change_condition, set=update_exprs
            )
        else:
            merge_builder = merge_builder.when_matched_update(set=update_exprs)

        # Insert new version for all records (new and changed)
        insert_exprs = {
            col: f"source.{col}" for col in source_data_with_scd.schema.names
        }
        merge_builder = merge_builder.when_not_matched_insert(values=insert_exprs)

        result = merge_builder.execute()

        return {
            "scd_type": 2,
            "key_columns": scd_config.key_columns,
            "version_column": scd_config.version_column,
            "merge_result": result,
        }

    def _execute_scd_type_3(
        self, dt: dl.DeltaTable, source_data: pa.Table, scd_config: SCDConfig
    ) -> Dict[str, Any]:
        """Execute SCD Type 3 merge (keep previous values in same row)."""
        # Build key predicate
        key_predicate = " AND ".join(
            [f"target.{col} = source.{col}" for col in scd_config.key_columns]
        )

        # Build update expressions for SCD Type 3
        update_exprs = {}
        change_columns = scd_config.change_columns or []

        for col in source_data.schema.names:
            if col not in scd_config.key_columns:
                if col in change_columns:
                    # Store previous value and update current
                    prev_col = f"{col}{scd_config.previous_value_suffix}"
                    update_exprs[prev_col] = f"target.{col}"  # Move current to previous
                    update_exprs[col] = f"source.{col}"  # Update current
                else:
                    # Just update the column normally
                    update_exprs[col] = f"source.{col}"

        # Build insert expressions
        insert_exprs = {col: f"source.{col}" for col in source_data.schema.names}

        # Execute merge
        merge_builder = (
            dt.merge(
                source=source_data,
                predicate=key_predicate,
                source_alias="source",
                target_alias="target",
            )
            .when_matched_update(set=update_exprs)
            .when_not_matched_insert(values=insert_exprs)
        )

        result = merge_builder.execute()

        return {
            "scd_type": 3,
            "key_columns": scd_config.key_columns,
            "change_columns": change_columns,
            "merge_result": result,
        }

    def _execute_delta_merge_builder(
        self, dt: dl.DeltaTable, source_data: pa.Table, config: MergeConfig
    ) -> Dict[str, Any]:
        """Execute DeltaMergeBuilder merge with complex conditions."""
        conditions = config.merge_conditions

        # Start building the merge
        merge_builder = dt.merge(
            source=source_data,
            predicate=conditions.merge_predicate,
            source_alias=config.source_alias,
            target_alias=config.target_alias,
        )

        # Add when matched conditions
        if conditions.when_matched_update:
            if conditions.when_matched_update_condition:
                merge_builder = merge_builder.when_matched_update(
                    condition=conditions.when_matched_update_condition,
                    set=conditions.when_matched_update,
                )
            else:
                merge_builder = merge_builder.when_matched_update(
                    set=conditions.when_matched_update
                )

        if conditions.when_matched_delete:
            if conditions.when_matched_delete_condition:
                merge_builder = merge_builder.when_matched_delete(
                    predicate=conditions.when_matched_delete_condition
                )
            else:
                merge_builder = merge_builder.when_matched_delete()

        # Add when not matched conditions
        if conditions.when_not_matched_insert:
            if conditions.when_not_matched_insert_condition:
                merge_builder = merge_builder.when_not_matched_insert(
                    condition=conditions.when_not_matched_insert_condition,
                    values=conditions.when_not_matched_insert,
                )
            else:
                merge_builder = merge_builder.when_not_matched_insert(
                    values=conditions.when_not_matched_insert
                )

        # Add when not matched by source conditions
        if conditions.when_not_matched_by_source_update:
            if conditions.when_not_matched_by_source_update_condition:
                merge_builder = merge_builder.when_not_matched_by_source_update(
                    condition=conditions.when_not_matched_by_source_update_condition,
                    set=conditions.when_not_matched_by_source_update,
                )
            else:
                merge_builder = merge_builder.when_not_matched_by_source_update(
                    set=conditions.when_not_matched_by_source_update
                )

        if conditions.when_not_matched_by_source_delete:
            if conditions.when_not_matched_by_source_delete_condition:
                merge_builder = merge_builder.when_not_matched_by_source_delete(
                    predicate=conditions.when_not_matched_by_source_delete_condition
                )
            else:
                merge_builder = merge_builder.when_not_matched_by_source_delete()

        # Execute the merge
        result = merge_builder.execute()

        return {
            "merge_type": "delta_merge_builder",
            "predicate": conditions.merge_predicate,
            "merge_result": result,
        }

    def _execute_simple_merge(
        self, dt: dl.DeltaTable, source_data: pa.Table, config: MergeConfig
    ) -> Dict[str, Any]:
        """Execute simple merge based on mode."""
        if config.mode == MergeMode.UPSERT:
            return self._execute_upsert(dt, source_data, config)
        elif config.mode == MergeMode.INSERT_ONLY:
            return self._execute_insert_only(dt, source_data, config)
        elif config.mode == MergeMode.UPDATE_ONLY:
            return self._execute_update_only(dt, source_data, config)
        elif config.mode == MergeMode.DELETE:
            return self._execute_delete(dt, source_data, config)
        else:
            raise ValueError(f"Unsupported merge mode: {config.mode}")

    def _execute_upsert(
        self, dt: dl.DeltaTable, source_data: pa.Table, config: MergeConfig
    ) -> Dict[str, Any]:
        """Execute simple upsert merge."""
        if not config.predicate:
            raise ValueError("Predicate is required for upsert operations")

        # Build update expressions
        update_exprs = config.update_columns or {
            col: f"source.{col}" for col in source_data.schema.names
        }

        # Build insert expressions
        insert_exprs = config.insert_columns or {
            col: f"source.{col}" for col in source_data.schema.names
        }

        # Execute merge
        merge_builder = (
            dt.merge(
                source=source_data,
                predicate=config.predicate,
                source_alias=config.source_alias,
                target_alias=config.target_alias,
            )
            .when_matched_update(set=update_exprs)
            .when_not_matched_insert(values=insert_exprs)
        )

        result = merge_builder.execute()

        return {
            "merge_mode": "upsert",
            "predicate": config.predicate,
            "merge_result": result,
        }

    def _execute_insert_only(
        self, dt: dl.DeltaTable, source_data: pa.Table, config: MergeConfig
    ) -> Dict[str, Any]:
        """Execute insert-only merge."""
        if not config.predicate:
            raise ValueError("Predicate is required for insert-only operations")

        # Build insert expressions
        insert_exprs = config.insert_columns or {
            col: f"source.{col}" for col in source_data.schema.names
        }

        # Execute merge (only insert when not matched)
        merge_builder = dt.merge(
            source=source_data,
            predicate=config.predicate,
            source_alias=config.source_alias,
            target_alias=config.target_alias,
        ).when_not_matched_insert(values=insert_exprs)

        result = merge_builder.execute()

        return {
            "merge_mode": "insert_only",
            "predicate": config.predicate,
            "merge_result": result,
        }

    def _execute_update_only(
        self, dt: dl.DeltaTable, source_data: pa.Table, config: MergeConfig
    ) -> Dict[str, Any]:
        """Execute update-only merge."""
        if not config.predicate:
            raise ValueError("Predicate is required for update-only operations")

        # Build update expressions
        update_exprs = config.update_columns or {
            col: f"source.{col}" for col in source_data.schema.names
        }

        # Execute merge (only update when matched)
        merge_builder = dt.merge(
            source=source_data,
            predicate=config.predicate,
            source_alias=config.source_alias,
            target_alias=config.target_alias,
        ).when_matched_update(set=update_exprs)

        result = merge_builder.execute()

        return {
            "merge_mode": "update_only",
            "predicate": config.predicate,
            "merge_result": result,
        }

    def _execute_delete(
        self, dt: dl.DeltaTable, source_data: pa.Table, config: MergeConfig
    ) -> Dict[str, Any]:
        """Execute delete merge."""
        delete_predicate = config.delete_predicate or config.predicate
        if not delete_predicate:
            raise ValueError("Delete predicate is required for delete operations")

        # Execute merge (only delete when matched)
        merge_builder = dt.merge(
            source=source_data,
            predicate=delete_predicate,
            source_alias=config.source_alias,
            target_alias=config.target_alias,
        ).when_matched_delete()

        result = merge_builder.execute()

        return {
            "merge_mode": "delete",
            "predicate": delete_predicate,
            "merge_result": result,
        }
