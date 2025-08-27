import os
import shutil
import tempfile
import uuid
from contextlib import contextmanager

import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.data import read_delta
from ray.data.datasource import SaveMode
from ray.data.tests.conftest import *  # noqa


@contextmanager
def delta_test_context(test_name: str, base_path: str = None):
    """Context manager for Delta tests with proper setup and cleanup."""
    temp_name = f"test_delta_{test_name}_{uuid.uuid4().hex[:8]}"

    if base_path is None:
        # Use system temporary directory as fallback
        temp_dir = tempfile.mkdtemp(prefix=temp_name)
    else:
        temp_dir = os.path.join(base_path, temp_name)

    try:
        os.makedirs(temp_dir, exist_ok=True)
        yield temp_dir
    finally:
        # Cleanup
        try:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
        except Exception:
            pass


def create_test_dataframe():
    """Create a sample DataFrame for testing."""
    return pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        "value": [100, 200, 300, 400, 500],
        "category": ["A", "B", "A", "B", "A"]
    })


def create_test_dataset():
    """Create a Ray Dataset from the test DataFrame."""
    df = create_test_dataframe()
    return ray.data.from_pandas(df)


def test_delta_datasink_append(ray_start_regular_shared, tmp_path):
    """Test Delta datasink append functionality."""
    with delta_test_context("append", str(tmp_path)) as test_path:
        # Create initial dataset
        ds = create_test_dataset()
        
        # Write initial data
        ds.write_delta(test_path, mode=SaveMode.OVERWRITE)
        
        # Verify initial write
        result_ds = read_delta(test_path)
        assert result_ds.count() == 5
        
        # Create new data to append
        new_df = pd.DataFrame({
            "id": [6, 7],
            "name": ["Frank", "Grace"],
            "value": [600, 700],
            "category": ["B", "A"]
        })
        new_ds = ray.data.from_pandas(new_df)
        
        # Append new data
        new_ds.write_delta(test_path, mode=SaveMode.APPEND)
        
        # Verify append
        final_ds = read_delta(test_path)
        assert final_ds.count() == 7
        
        # Check that original data is still there
        rows = final_ds.take_all()
        ids = [row["id"] for row in rows]
        assert 1 in ids and 2 in ids and 3 in ids
        assert 6 in ids and 7 in ids


def test_delta_datasink_overwrite(ray_start_regular_shared, tmp_path):
    """Test Delta datasink overwrite functionality."""
    with delta_test_context("overwrite", str(tmp_path)) as test_path:
        # Create initial dataset
        ds = create_test_dataset()
        
        # Write initial data
        ds.write_delta(test_path, mode=SaveMode.OVERWRITE)
        
        # Verify initial write
        result_ds = read_delta(test_path)
        assert result_ds.count() == 5
        
        # Create completely new data
        new_df = pd.DataFrame({
            "id": [10, 20, 30],
            "name": ["New1", "New2", "New3"],
            "value": [1000, 2000, 3000],
            "category": ["X", "Y", "Z"]
        })
        new_ds = ray.data.from_pandas(new_df)
        
        # Overwrite with new data
        new_ds.write_delta(test_path, mode=SaveMode.OVERWRITE)
        
        # Verify overwrite
        final_ds = read_delta(test_path)
        assert final_ds.count() == 3
        
        # Check that only new data exists
        rows = final_ds.take_all()
        ids = [row["id"] for row in rows]
        assert 10 in ids and 20 in ids and 30 in ids
        assert 1 not in ids  # Original data should be gone


def test_delta_datasink_ignore(ray_start_regular_shared, tmp_path):
    """Test Delta datasink ignore functionality."""
    with delta_test_context("ignore", str(tmp_path)) as test_path:
        # Create initial dataset
        ds = create_test_dataset()
        
        # Write initial data
        ds.write_delta(test_path, mode=SaveMode.OVERWRITE)
        
        # Verify initial write
        result_ds = read_delta(test_path)
        assert result_ds.count() == 5
        
        # Try to write again with IGNORE mode
        new_df = pd.DataFrame({
            "id": [10, 20, 30],
            "name": ["New1", "New2", "New3"],
            "value": [1000, 2000, 3000],
            "category": ["X", "Y", "Z"]
        })
        new_ds = ray.data.from_pandas(new_df)
        
        # This should not fail, but also not write anything
        new_ds.write_delta(test_path, mode=SaveMode.IGNORE)
        
        # Verify that data is unchanged
        final_ds = read_delta(test_path)
        assert final_ds.count() == 5
        
        # Check that original data is still there
        rows = final_ds.take_all()
        ids = [row["id"] for row in rows]
        assert 1 in ids and 2 in ids and 3 in ids
        assert 10 not in ids  # New data should not be there


def test_delta_datasink_error_mode(ray_start_regular_shared, tmp_path):
    """Test Delta datasink error mode functionality."""
    with delta_test_context("error", str(tmp_path)) as test_path:
        # Create initial dataset
        ds = create_test_dataset()
        
        # Write initial data
        ds.write_delta(test_path, mode=SaveMode.OVERWRITE)
        
        # Verify initial write
        result_ds = read_delta(test_path)
        assert result_ds.count() == 5
        
        # Try to write again with ERROR mode (default)
        new_df = pd.DataFrame({
            "id": [10, 20, 30],
            "name": ["New1", "New2", "New3"],
            "value": [1000, 2000, 3000],
            "category": ["X", "Y", "Z"]
        })
        new_ds = ray.data.from_pandas(new_df)
        
        # This should raise an error
        with pytest.raises(Exception):
            new_ds.write_delta(test_path, mode=SaveMode.ERROR)
        
        # Verify that data is unchanged
        final_ds = read_delta(test_path)
        assert final_ds.count() == 5


if __name__ == "__main__":
    # Run tests directly if file is executed
    import sys
    sys.exit(pytest.main([__file__]))
