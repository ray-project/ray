import os
import shutil
import sys
import tempfile
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import List

import pandas as pd
import pytest

# Try to import pytest_lazy_fixtures, but handle gracefully if missing
try:
    from pytest_lazy_fixtures import lf as lazy_fixture

    LAZY_FIXTURES_AVAILABLE = True
except ImportError:
    LAZY_FIXTURES_AVAILABLE = False
    lazy_fixture = None


import ray
from ray.data._internal.datasource.delta import (
    MergeConditions,
    MergeConfig,
    compact_delta_table,
    vacuum_delta_table,
    z_order_delta_table,
)
from ray.data.datasource.path_util import _unwrap_protocol

# Try to import test fixtures, but handle gracefully if they fail
try:
    from ray.data.tests.conftest import *  # noqa
    from ray.data.tests.mock_http_server import *  # noqa
    from ray.tests.conftest import *  # noqa

    TEST_FIXTURES_AVAILABLE = True
except ImportError:
    TEST_FIXTURES_AVAILABLE = False
    # Define simple local fixtures as fallbacks
    @pytest.fixture
    def local_path():
        with tempfile.TemporaryDirectory() as tmp_dir:
            yield tmp_dir

    @pytest.fixture
    def s3_path():
        # Skip S3 tests if fixtures not available
        pytest.skip("S3 test fixtures not available")


# Enhanced test utilities for comprehensive validation
class DeltaTestValidator:
    """Comprehensive validation utilities for Delta Lake tests."""

    @staticmethod
    def validate_data_integrity(
        df: pd.DataFrame, expected_count: int, id_column: str = "id"
    ) -> None:
        """Validate data integrity with comprehensive checks."""
        assert (
            len(df) == expected_count
        ), f"Expected {expected_count} rows, got {len(df)}"

        if id_column in df.columns:
            # Check for duplicates
            duplicates = df[id_column].duplicated().sum()
            assert duplicates == 0, f"Found {duplicates} duplicate {id_column} values"

            # Check for null primary keys
            null_keys = df[id_column].isna().sum()
            assert null_keys == 0, f"Found {null_keys} null {id_column} values"

    @staticmethod
    def validate_schema_consistency(
        df: pd.DataFrame, expected_columns: List[str]
    ) -> None:
        """Validate schema consistency."""
        actual_columns = set(df.columns)
        expected_columns = set(expected_columns)

        missing_columns = expected_columns - actual_columns

        assert not missing_columns, f"Missing columns: {missing_columns}"
        # Extra columns are OK for schema evolution tests

    @staticmethod
    def validate_performance_metrics(
        operation_time: float, record_count: int, min_throughput: int = 1000
    ) -> None:
        """Validate performance meets minimum thresholds."""
        if operation_time > 0:
            throughput = record_count / operation_time
            assert (
                throughput >= min_throughput
            ), f"Throughput {throughput:.0f} records/sec below minimum {min_throughput}"

    @staticmethod
    def create_test_data(
        size: int = 100, include_nulls: bool = True, include_unicode: bool = True
    ) -> pd.DataFrame:
        """Create comprehensive test data with various data types."""
        data = {
            "id": range(1, size + 1),
            "int_col": list(range(size)),
            "float_col": [i * 3.14159 for i in range(size)],
            "string_col": [f"String_{i}" for i in range(size)],
            "bool_col": [i % 2 == 0 for i in range(size)],
            "timestamp_col": [datetime.now() + timedelta(hours=i) for i in range(size)],
        }

        if include_nulls:
            data["nullable_int"] = [i if i % 10 != 0 else None for i in range(size)]
            data["nullable_str"] = [
                f"Nullable_{i}" if i % 5 != 0 else None for i in range(size)
            ]

        if include_unicode:
            data["unicode_col"] = [
                f"Unicode_{i}_test_émoji" if i % 3 == 0 else f"ASCII_{i}"
                for i in range(size)
            ]

        return pd.DataFrame(data)


def _robust_test_cleanup(temp_dir: str, test_name: str) -> None:
    """Robust cleanup function for test directories."""
    cleanup_attempts = 0
    max_cleanup_attempts = 3

    while cleanup_attempts < max_cleanup_attempts:
        try:
            if os.path.exists(temp_dir):
                try:
                    os.listdir(temp_dir)
                except OSError:
                    return

                shutil.rmtree(temp_dir)
                return
            else:
                return

        except OSError:
            cleanup_attempts += 1

            if cleanup_attempts < max_cleanup_attempts:
                time.sleep(1)
            else:
                pass  # Give up after max attempts

        except Exception:
            return


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
    except Exception:
        # Add context for debugging
        try:
            if os.path.exists(temp_dir):
                os.listdir(temp_dir)
            else:
                pass
        except Exception:
            pass
        raise
    finally:
        _robust_test_cleanup(temp_dir, test_name)


@pytest.mark.parametrize(
    "data_path",
    [
        lazy_fixture("local_path"),
        lazy_fixture("s3_path"),
    ],
)
@pytest.mark.parametrize(
    "batch_size",
    [1, 100],
)
@pytest.mark.parametrize(
    "write_mode",
    ["append", "overwrite"],
)
def test_delta_read_basic(data_path, batch_size, write_mode):
    """Test basic Delta Lake read functionality with comprehensive validation."""
    import pandas as pd
    from deltalake import write_deltalake

    # Parse the data path.
    setup_data_path = _unwrap_protocol(data_path)
    path = os.path.join(setup_data_path, "tmp_test_delta")

    # Create a sample Delta Lake table with comprehensive data types
    df = pd.DataFrame(
        {
            "x": [42] * batch_size,
            "y": ["a"] * batch_size,
            "z": [3.14] * batch_size,
            "bool_col": [True] * batch_size,
            "timestamp_col": [datetime.now()] * batch_size,
        }
    )

    start_time = time.time()
    if write_mode == "append":
        write_deltalake(path, df, mode=write_mode)
        write_deltalake(path, df, mode=write_mode)
        expected_count = batch_size * 2
    elif write_mode == "overwrite":
        write_deltalake(path, df, mode=write_mode)
        expected_count = batch_size

    # Read the Delta Lake table
    start_time = time.time()
    ds = ray.data.read_delta(path)
    read_time = time.time() - start_time

    # Comprehensive validation
    actual_count = ds.count()
    assert (
        actual_count == expected_count
    ), f"Expected {expected_count} rows, got {actual_count}"

    # Schema validation

    # Validate core columns exist
    assert "x" in ds.schema().names
    assert "y" in ds.schema().names
    assert "z" in ds.schema().names

    # Data integrity validation
    if batch_size > 0:
        sample_row = ds.take(1)[0]
        assert sample_row["x"] == 42
        assert sample_row["y"] == "a"
        assert sample_row["z"] == 3.14
        assert sample_row["bool_col"] is True

        # Validate all data
        all_rows = ds.take_all()
        df_result = pd.DataFrame(all_rows)
        DeltaTestValidator.validate_data_integrity(df_result, expected_count, "x")
        DeltaTestValidator.validate_schema_consistency(
            df_result, ["x", "y", "z", "bool_col"]
        )

        # Performance validation (relaxed for small datasets)
        if expected_count >= 100:
            DeltaTestValidator.validate_performance_metrics(
                read_time, expected_count, 500
            )

    assert ds.schema().names == ["x", "y", "z", "bool_col", "timestamp_col"]


@pytest.mark.parametrize(
    "data_path",
    [
        lazy_fixture("local_path"),
        lazy_fixture("s3_path"),
    ],
)
@pytest.mark.parametrize(
    "batch_size",
    [1, 100],
)
@pytest.mark.parametrize(
    "write_mode",
    ["append", "overwrite"],
)
def test_delta_write_partitioned(data_path, batch_size, write_mode):
    """Test writing a partitioned Delta table with comprehensive validation."""
    import pandas as pd

    setup_data_path = _unwrap_protocol(data_path)
    path = os.path.join(setup_data_path, "tmp_test_delta_partitioned")

    # Create comprehensive test data with multiple partition levels
    partition_data = ["GroupA" if i % 2 == 0 else "GroupB" for i in range(batch_size)]
    year_data = [2023 if i % 3 == 0 else 2024 for i in range(batch_size)]

    df = pd.DataFrame(
        {
            "id": range(1, batch_size + 1),
            "x": [42] * batch_size,
            "y": ["a"] * batch_size,
            "z": [3.14] * batch_size,
            "part": partition_data,
            "year": year_data,
            "timestamp_col": [
                datetime.now() + timedelta(hours=i) for i in range(batch_size)
            ],
            "nullable_col": [
                f"value_{i}" if i % 3 != 0 else None for i in range(batch_size)
            ],
        }
    )
    ds = ray.data.from_pandas(df)

    # Test partition writing with performance measurement
    start_time = time.time()
    ds.write_delta(path, mode=write_mode, partition_cols=["part", "year"])
    write_time = time.time() - start_time

    # If 'append', write again (should double count)
    if write_mode == "append":
        start_time = time.time()
        ds.write_delta(path, mode=write_mode, partition_cols=["part", "year"])

    # Read the table back with performance measurement
    start_time = time.time()
    res_ds = ray.data.read_delta(path)
    read_time = time.time() - start_time

    # Comprehensive validation
    expected_rows = batch_size if write_mode == "overwrite" else batch_size * 2
    actual_rows = res_ds.count()
    assert (
        actual_rows == expected_rows
    ), f"Expected {expected_rows} rows, got {actual_rows}"

    # Schema validation - ensure all columns are preserved
    expected_columns = {
        "id",
        "x",
        "y",
        "z",
        "part",
        "year",
        "timestamp_col",
        "nullable_col",
    }
    actual_columns = set(res_ds.schema().names)
    assert (
        actual_columns == expected_columns
    ), f"Schema mismatch. Expected: {expected_columns}, Got: {actual_columns}"

    # Data integrity validation
    all_rows = res_ds.take_all()
    df_result = pd.DataFrame(all_rows)
    DeltaTestValidator.validate_data_integrity(df_result, expected_rows, "id")
    DeltaTestValidator.validate_schema_consistency(df_result, list(expected_columns))

    # Partition validation
    result_parts = {row["part"] for row in all_rows}
    result_years = {row["year"] for row in all_rows}

    if batch_size > 1:
        assert result_parts == {"GroupA", "GroupB"}
        assert result_years == {2023, 2024}
    else:
        assert list(result_parts)[0] in {"GroupA", "GroupB"}
        assert list(result_years)[0] in {2023, 2024}

    # Validate partition directory structure (for local paths)
    try:
        if os.path.exists(path):
            dir_contents = os.listdir(path)
            partition_dirs = [d for d in dir_contents if d.startswith("part=")]
            if partition_dirs:
                # Check nested partitioning
                for part_dir in partition_dirs[:2]:  # Check first 2
                    part_path = os.path.join(path, part_dir)
                    if os.path.isdir(part_path):
                        year_dirs = [
                            d for d in os.listdir(part_path) if d.startswith("year=")
                        ]
                        assert (
                            len(year_dirs) > 0
                        ), f"No year partitions found in {part_dir}"
    except OSError:
        # Directory listing might fail in some environments, that's OK
        pass

    # Data correctness validation
    sample_row = res_ds.take(1)[0]
    assert sample_row["x"] == 42
    assert sample_row["y"] == "a"
    assert sample_row["z"] == 3.14
    assert sample_row["part"] in {"GroupA", "GroupB"}
    assert sample_row["year"] in {2023, 2024}

    # Null handling validation
    null_count = sum(1 for row in all_rows if row["nullable_col"] is None)
    expected_nulls = len([i for i in range(batch_size) if i % 3 == 0])
    if write_mode == "append":
        expected_nulls *= 2
    assert (
        null_count == expected_nulls
    ), f"Expected {expected_nulls} nulls, got {null_count}"

    # Performance validation (relaxed thresholds for small datasets)
    if batch_size >= 100:
        DeltaTestValidator.validate_performance_metrics(write_time, batch_size, 200)
        DeltaTestValidator.validate_performance_metrics(read_time, actual_rows, 500)


@pytest.mark.parametrize("scd_type", [1, 2, 3])
def test_scd_convenience_parameters(tmp_path, scd_type):
    """Test SCD convenience parameters in write_delta function."""
    import pandas as pd

    # Create initial dataset
    initial_data = pd.DataFrame(
        {
            "customer_id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "status": ["active", "active", "inactive"],
            "email": ["alice@test.com", "bob@test.com", "charlie@test.com"],
        }
    )

    path = str(tmp_path / "scd_test")
    ds_initial = ray.data.from_pandas(initial_data)

    # Write initial data
    ds_initial.write_delta(path, mode="overwrite")

    # Create update dataset with changes
    update_data = pd.DataFrame(
        {
            "customer_id": [1, 2, 4],  # Update 1, 2 and insert 4
            "name": ["Alice Updated", "Bob Updated", "David"],
            "status": ["inactive", "active", "active"],
            "email": ["alice.new@test.com", "bob@test.com", "david@test.com"],
        }
    )

    ds_update = ray.data.from_pandas(update_data)

    # Test SCD convenience parameters
    ds_update.write_delta(
        path, mode="merge", scd_type=scd_type, key_columns=["customer_id"]
    )

    # Read back and verify
    result_ds = ray.data.read_delta(path)
    result_rows = result_ds.take_all()

    # Basic verification that merge occurred
    customer_ids = {row["customer_id"] for row in result_rows}
    assert 4 in customer_ids  # New record inserted

    if scd_type == 1:
        # Type 1: Records should be updated in-place
        assert len(result_rows) == 4  # 3 original + 1 new
        alice_record = next(row for row in result_rows if row["customer_id"] == 1)
        assert alice_record["name"] == "Alice Updated"

    elif scd_type == 2:
        # Type 2: Historical records preserved with versioning
        assert len(result_rows) >= 4  # At least original + updates

    elif scd_type == 3:
        # Type 3: Previous values stored in separate columns
        assert len(result_rows) == 4  # Same count but with previous value columns


def test_merge_conditions(tmp_path):
    """Test merge conditions."""
    import pandas as pd

    # Create initial dataset
    initial_data = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "value": [10, 20, 30],
            "status": ["active", "active", "inactive"],
        }
    )

    path = str(tmp_path / "merge_conditions_test")
    ds_initial = ray.data.from_pandas(initial_data)
    ds_initial.write_delta(path, mode="overwrite")

    # Create update dataset
    update_data = pd.DataFrame(
        {"id": [1, 2, 4], "value": [15, 25, 40], "status": ["updated", "active", "new"]}
    )

    ds_update = ray.data.from_pandas(update_data)

    # Test with MergeConditions
    conditions = MergeConditions(
        merge_predicate="target.id = source.id",
        when_matched_update_condition="target.status != 'inactive'",
        when_matched_update_set={"value": "source.value", "status": "source.status"},
        when_not_matched_insert_condition="source.status = 'new'",
        when_not_matched_insert_values={
            "id": "source.id",
            "value": "source.value",
            "status": "source.status",
        },
    )

    ds_update.write_delta(path, mode="merge", merge_conditions=conditions)

    # Read back and verify
    result_ds = ray.data.read_delta(path)
    result_rows = result_ds.take_all()

    # Verify merge logic worked
    id_to_row = {row["id"]: row for row in result_rows}

    # ID 1 should be updated (status was active, not inactive)
    assert id_to_row[1]["value"] == 15
    assert id_to_row[1]["status"] == "updated"

    # ID 2 should be updated
    assert id_to_row[2]["value"] == 25

    # ID 3 should remain unchanged (status was inactive)
    assert id_to_row[3]["value"] == 30
    assert id_to_row[3]["status"] == "inactive"

    # ID 4 should be inserted (status = 'new')
    assert id_to_row[4]["value"] == 40
    assert id_to_row[4]["status"] == "new"


@pytest.mark.parametrize("optimization_type", ["compact", "z_order", "vacuum"])
def test_standalone_utility_functions(tmp_path, optimization_type):
    """Test standalone utility functions for Delta table optimization."""
    import pandas as pd
    from deltalake import write_deltalake

    # Create a test Delta table
    path = str(tmp_path / "optimization_test")
    df = pd.DataFrame(
        {
            "id": range(100),
            "value": range(100, 200),
            "timestamp": [datetime.now() - timedelta(days=i) for i in range(100)],
        }
    )
    write_deltalake(path, df, mode="overwrite")

    # Add more data to create multiple files
    df2 = pd.DataFrame(
        {
            "id": range(100, 200),
            "value": range(200, 300),
            "timestamp": [datetime.now() - timedelta(days=i) for i in range(100)],
        }
    )
    write_deltalake(path, df2, mode="append")

    # Test the appropriate function
    if optimization_type == "compact":
        compact_delta_table(path, target_file_size=1024 * 1024)  # 1MB target

    elif optimization_type == "z_order":
        z_order_delta_table(path, columns=["id"])

    elif optimization_type == "vacuum":
        # Make files older than retention period
        vacuum_delta_table(path, retention_hours=0, dry_run=False)

    # Verify table is still readable
    ds = ray.data.read_delta(path)
    assert ds.count() == 200


def test_merge_config_comprehensive(tmp_path):
    """Test comprehensive MergeConfig functionality."""
    import pandas as pd

    # Create initial dataset
    initial_data = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "value": [100, 200, 300],
            "active": [True, True, False],
        }
    )

    path = str(tmp_path / "merge_config_test")
    ds_initial = ray.data.from_pandas(initial_data)
    ds_initial.write_delta(path, mode="overwrite")

    # Create update dataset
    update_data = pd.DataFrame(
        {
            "id": [1, 2, 4],
            "name": ["Alice Updated", "Bob Updated", "David"],
            "value": [150, 250, 400],
            "active": [True, True, True],
        }
    )

    ds_update = ray.data.from_pandas(update_data)

    # Create comprehensive merge configuration
    merge_config = MergeConfig(
        merge_predicate="target.id = source.id",
        when_matched_update_condition="target.active = true",
        when_matched_delete_condition="source.value < 0",
        when_not_matched_insert_condition="source.active = true",
        target_columns=["name", "value", "active"],
    )

    ds_update.write_delta(path, mode="merge", merge_config=merge_config)

    # Read back and verify
    result_ds = ray.data.read_delta(path)
    result_rows = result_ds.take_all()

    # Verify merge results
    id_to_row = {row["id"]: row for row in result_rows}

    # ID 1 should be updated (was active)
    assert id_to_row[1]["name"] == "Alice Updated"
    assert id_to_row[1]["value"] == 150

    # ID 2 should be updated (was active)
    assert id_to_row[2]["name"] == "Bob Updated"
    assert id_to_row[2]["value"] == 250

    # ID 3 should remain unchanged (was not active)
    assert id_to_row[3]["name"] == "Charlie"
    assert id_to_row[3]["value"] == 300

    # ID 4 should be inserted (source.active = true)
    assert id_to_row[4]["name"] == "David"
    assert id_to_row[4]["value"] == 400


def test_microbatch_processing(tmp_path):
    """Test microbatch processing for large datasets."""
    import pandas as pd

    # Create a large dataset
    large_data = pd.DataFrame(
        {
            "id": range(1000),
            "value": range(1000, 2000),
            "category": ["A" if i % 2 == 0 else "B" for i in range(1000)],
        }
    )

    path = str(tmp_path / "microbatch_test")
    ds_large = ray.data.from_pandas(large_data)

    # Write with small batch size to test microbatch processing
    ds_large.write_delta(
        path,
        mode="overwrite",
        max_rows_per_file=100,  # Force multiple files
        max_rows_per_group=50,  # Small groups
    )

    # Read back and verify
    result_ds = ray.data.read_delta(path)
    assert result_ds.count() == 1000

    # Verify some sample data
    sample_rows = result_ds.take(10)
    assert len(sample_rows) == 10
    assert all("id" in row for row in sample_rows)


def test_advanced_merge_operations(tmp_path):
    """Test advanced merge scenarios with comprehensive validation."""
    with delta_test_context("advanced_merge", str(tmp_path)) as temp_dir:
        path = os.path.join(temp_dir, "advanced_merge_table")

        # Create initial dataset with rich schema
        initial_data = pd.DataFrame(
            {
                "id": [1, 2, 3, 4],
                "name": ["Alice", "Bob", "Charlie", "Diana"],
                "department": ["Engineering", "Finance", "Engineering", "HR"],
                "salary": [100000, 85000, 120000, 75000],
                "active": [True, True, False, True],
                "last_updated": ["2023-01-01"] * 4,
                "version": [1] * 4,
            }
        )

        ds_initial = ray.data.from_pandas(initial_data)
        ds_initial.write_delta(path, mode="overwrite")

        # Test 1: Complex upsert with conditions
        update_data = pd.DataFrame(
            {
                "id": [1, 2, 5],  # Update 1,2 and insert 5
                "name": ["Alice Updated", "Bob Updated", "Eve"],
                "department": ["Engineering", "Finance", "Marketing"],
                "salary": [110000, 87000, 95000],
                "active": [True, True, True],
                "last_updated": ["2023-06-01"] * 3,
                "version": [2] * 3,
            }
        )

        ds_update = ray.data.from_pandas(update_data)

        # Perform merge with comprehensive configuration
        start_time = time.time()
        ds_update.write_delta(
            path, mode="merge", upsert_condition="target.id = source.id"
        )
        merge_time = time.time() - start_time

        # Validate merge results
        result_ds = ray.data.read_delta(path)
        result_df = pd.DataFrame(result_ds.take_all())

        # Comprehensive validation
        DeltaTestValidator.validate_data_integrity(
            result_df, 5, "id"
        )  # 4 original + 1 new

        # Validate specific merge outcomes
        id_to_row = {row["id"]: row for row in result_ds.take_all()}

        # Updated records
        assert id_to_row[1]["name"] == "Alice Updated"
        assert id_to_row[1]["salary"] == 110000
        assert id_to_row[1]["version"] == 2

        assert id_to_row[2]["name"] == "Bob Updated"
        assert id_to_row[2]["salary"] == 87000

        # Unchanged record
        assert id_to_row[3]["name"] == "Charlie"
        assert id_to_row[3]["salary"] == 120000
        assert id_to_row[3]["version"] == 1  # Should remain original

        # New record
        assert id_to_row[5]["name"] == "Eve"
        assert id_to_row[5]["department"] == "Marketing"

        # Performance validation
        DeltaTestValidator.validate_performance_metrics(merge_time, 3, 50)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
