"""Utilities for detecting and leveraging partition-aware optimizations.

This module provides functionality to detect when a dataset is already
partitioned by the groupby columns, enabling optimizations like skipping
the shuffle operation.
"""

from typing import List, Optional, Tuple, Dict, Any
import logging

logger = logging.getLogger(__name__)


def extract_partition_values_from_paths(
    input_files: List[str],
    partition_columns: List[str],
) -> Optional[Dict[str, Any]]:
    """Extract partition values from file paths using Hive-style partitioning.
    
    Args:
        input_files: List of file paths (e.g., from BlockMetadata.input_files)
        partition_columns: List of partition column names to extract
        
    Returns:
        Dict mapping partition column names to their values if all files have
        consistent partition values, None otherwise.
    """
    if not input_files or not partition_columns:
        return None
    
    partition_values: Optional[Dict[str, Any]] = None

    for file_path in input_files:
        # Extract partition values from Hive-style paths
        # e.g., /path/date=2024-01-01/hour=12/partition=1/file.parquet
        current_partitions: Dict[str, Any] = {}

        path_parts = file_path.split("/")
        for part in path_parts:
            if "=" in part:
                key, value = part.split("=", 1)
                if key in partition_columns:
                    current_partitions[key] = value

        # If this file lacks partition columns, bail out early.
        if not current_partitions:
            return None

        # On first valid file, store its partition values.
        if partition_values is None:
            # Ensure all requested partition columns are present.
            if any(col not in current_partitions for col in partition_columns):
                return None
            partition_values = current_partitions
        else:
            # All files must have the same partition values
            for col in partition_columns:
                if col not in current_partitions:
                    return None
                if partition_values.get(col) != current_partitions[col]:
                    return None

    # At this point we have a consistent partition_values dict
    return partition_values


def is_partition_aware_groupby_possible(
    dataset_blocks_metadata: List[Any],
    groupby_columns: List[str],
) -> Tuple[bool, Optional[str]]:
    """Check if GroupBy can skip shuffle due to existing partitioning.
    
    This function checks if all blocks in the dataset are already partitioned
    by the groupby columns in a consistent way.
    
    Args:
        dataset_blocks_metadata: List of BlockMetadata objects
        groupby_columns: List of groupby column names
        
    Returns:
        Tuple of (can_skip_shuffle, reason_if_false)
        - can_skip_shuffle: True if shuffle can be skipped
        - reason_if_false: String explaining why if can't skip, None if can skip
    """
    if not dataset_blocks_metadata or not groupby_columns:
        return False, "No blocks or groupby columns"
    
    # Collect partition values for each block
    block_partitions: List[Optional[Dict[str, Any]]] = []
    
    for metadata in dataset_blocks_metadata:
        if not metadata.input_files:
            return False, "Block has no input files information"
        
        partitions = extract_partition_values_from_paths(
            metadata.input_files,
            groupby_columns,
        )
        
        if partitions is None:
            return False, f"Could not extract partition info from block"
        
        block_partitions.append(partitions)
    
    # Verify all blocks have different partition values
    # (i.e., each block is a separate partition)
    seen_partitions = set()
    for partitions in block_partitions:
        # Convert dict to tuple for hashing
        partition_key = tuple(sorted(partitions.items()))
        if partition_key in seen_partitions:
            return False, "Multiple blocks have same partition values"
        seen_partitions.add(partition_key)
    
    logger.debug(
        f"Partition awareness check passed for groupby on {groupby_columns}. "
        f"Found {len(block_partitions)} distinct partitions."
    )
    
    return True, None
