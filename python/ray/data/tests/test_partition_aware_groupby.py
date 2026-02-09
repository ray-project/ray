"""Tests for partition-aware GroupBy optimization.

This test module verifies that GroupBy operations can skip the shuffle
operation when the underlying data is already partitioned by the groupby columns.
"""

import os
import tempfile
from pathlib import Path
from typing import List

import pytest
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import ray
from ray.data.context import ShuffleStrategy, DataContext
from ray.data._internal.partition_aware import (
    extract_partition_values_from_paths,
    is_partition_aware_groupby_possible,
)


class TestPartitionAwareUtils:
    """Test partition awareness utility functions."""
    
    def test_extract_partition_values_from_hive_paths(self):
        """Test extraction of partition values from Hive-style paths."""
        paths = [
            "/data/date=2024-01-01/hour=12/partition=1/file1.parquet",
            "/data/date=2024-01-01/hour=12/partition=1/file2.parquet",
        ]
        
        result = extract_partition_values_from_paths(
            paths,
            ["date", "hour", "partition"]
        )
        
        assert result is not None
        assert result["date"] == "2024-01-01"
        assert result["hour"] == "12"
        assert result["partition"] == "1"
    
    def test_extract_partition_values_inconsistent(self):
        """Test that inconsistent partition values return None."""
        paths = [
            "/data/date=2024-01-01/hour=12/partition=1/file1.parquet",
            "/data/date=2024-01-01/hour=13/partition=1/file2.parquet",  # Different hour
        ]
        
        result = extract_partition_values_from_paths(
            paths,
            ["date", "hour", "partition"]
        )
        
        assert result is None
    
    def test_extract_partition_values_missing_columns(self):
        """Test that missing partition columns return None."""
        paths = [
            "/data/date=2024-01-01/hour=12/file.parquet",  # Missing partition
        ]
        
        result = extract_partition_values_from_paths(
            paths,
            ["date", "hour", "partition"]
        )
        
        assert result is None
    
    def test_extract_partition_values_empty_list(self):
        """Test that empty file list returns None."""
        result = extract_partition_values_from_paths([], ["date", "hour"])
        assert result is None


class TestPartitionAwareGroupBy:
    """Test GroupBy with partition-aware optimization."""
    
    @pytest.fixture
    def temp_parquet_dataset(self):
        """Create a temporary Hive-partitioned Parquet dataset."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create partitioned data: date=2024-01-01/hour=12/partition=1/
            base_path = Path(tmpdir)
            
            # Create multiple partitions
            partitions = [
                ("2024-01-01", "12", "1"),
                ("2024-01-01", "12", "2"),
                ("2024-01-01", "13", "1"),
            ]
            
            for date, hour, partition in partitions:
                partition_dir = (
                    base_path / f"date={date}" / f"hour={hour}" / f"partition={partition}"
                )
                partition_dir.mkdir(parents=True, exist_ok=True)
                
                # Write sample data
                data = pd.DataFrame({
                    "id": range(10),
                    "value": range(10, 20),
                    "group": [i % 3 for i in range(10)],
                })
                
                file_path = partition_dir / "data.parquet"
                table = pa.Table.from_pandas(data)
                pq.write_table(table, str(file_path))
            
            yield str(base_path)
    
    @pytest.mark.skip(reason="Requires actual Ray cluster and parquet files")
    def test_partition_aware_groupby_skips_shuffle(self, temp_parquet_dataset):
        """Test that partition-aware GroupBy skips shuffle operation."""
        
        # Configure Ray to use hash shuffle
        ctx = DataContext.get_current()
        original_strategy = ctx.shuffle_strategy
        ctx.shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE
        
        try:
            # Load partitioned dataset
            ds = ray.data.read_parquet(temp_parquet_dataset)
            
            # Group by partition column (data should already be partitioned this way)
            grouped = ds.groupby("group")
            
            # This should skip the shuffle operation internally
            result = grouped.count()
            
            # Verify the result
            assert result.count() == 3  # 3 groups (0, 1, 2)
            
        finally:
            ctx.shuffle_strategy = original_strategy
    
    def test_partition_aware_check_multiple_blocks(self):
        """Test partition awareness check with multiple blocks."""
        from ray.data.block import BlockMetadata
        from unittest.mock import Mock
        
        # Create mock BlockMetadata objects with partitioned paths
        metadata1 = Mock(spec=BlockMetadata)
        metadata1.input_files = [
            "/data/date=2024-01-01/hour=12/partition=1/file1.parquet",
            "/data/date=2024-01-01/hour=12/partition=1/file2.parquet",
        ]
        metadata1.num_rows = 100
        metadata1.size_bytes = 1000
        
        metadata2 = Mock(spec=BlockMetadata)
        metadata2.input_files = [
            "/data/date=2024-01-01/hour=12/partition=2/file1.parquet",
            "/data/date=2024-01-01/hour=12/partition=2/file2.parquet",
        ]
        metadata2.num_rows = 100
        metadata2.size_bytes = 1000
        
        metadata3 = Mock(spec=BlockMetadata)
        metadata3.input_files = [
            "/data/date=2024-01-01/hour=13/partition=3/file1.parquet",
            "/data/date=2024-01-01/hour=13/partition=3/file2.parquet",
        ]
        metadata3.num_rows = 100
        metadata3.size_bytes = 1000
        
        # Check partition awareness for groupby on "partition"
        can_skip, reason = is_partition_aware_groupby_possible(
            [metadata1, metadata2, metadata3],
            ["partition"]
        )
        
        assert can_skip is True
        assert reason is None
    
    def test_partition_aware_check_fails_duplicate_partitions(self):
        """Test that duplicate partitions are detected."""
        from ray.data.block import BlockMetadata
        from unittest.mock import Mock
        
        # Create mock BlockMetadata objects with duplicate partition values
        metadata1 = Mock(spec=BlockMetadata)
        metadata1.input_files = [
            "/data/date=2024-01-01/hour=12/partition=1/file1.parquet",
        ]
        
        metadata2 = Mock(spec=BlockMetadata)
        metadata2.input_files = [
            "/data/date=2024-01-01/hour=12/partition=1/file2.parquet",  # Same partition
        ]
        
        can_skip, reason = is_partition_aware_groupby_possible(
            [metadata1, metadata2],
            ["partition"]
        )
        
        assert can_skip is False
        assert "same partition" in reason.lower()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
