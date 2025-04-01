import numpy as np
import pandas as pd
import pytest

import ray
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa


def test_join_basic(ray_start_regular):
    # Create sample datasets
    ds1 = ray.data.from_items([
        {"key": 1, "value1": "a"},
        {"key": 2, "value1": "b"},
        {"key": 3, "value1": "c"},
    ])
    ds2 = ray.data.from_items([
        {"key": 1, "value2": "x"},
        {"key": 2, "value2": "y"},
        {"key": 4, "value2": "z"},
    ])
    
    # Test inner join
    result = ds1.join(ds2, on="key")
    assert result.count() == 2
    result_list = result.take_all()
    assert {"key": 1, "value1": "a", "value2": "x"} in result_list
    assert {"key": 2, "value1": "b", "value2": "y"} in result_list
    
    # Test left outer join
    result = ds1.join(ds2, on="key", how="left_outer")
    assert result.count() == 3
    result_list = result.take_all()
    assert {"key": 1, "value1": "a", "value2": "x"} in result_list
    assert {"key": 2, "value1": "b", "value2": "y"} in result_list
    # Find the row with key=3, then check that value2 is None
    key3_row = next(row for row in result_list if row["key"] == 3)
    assert key3_row["value2"] is None
    
    # Test right outer join
    result = ds1.join(ds2, on="key", how="right_outer")
    assert result.count() == 3
    result_list = result.take_all()
    assert {"key": 1, "value1": "a", "value2": "x"} in result_list
    assert {"key": 2, "value1": "b", "value2": "y"} in result_list
    # Find the row with key=4, then check that value1 is None
    key4_row = next(row for row in result_list if row["key"] == 4)
    assert key4_row["value1"] is None
    
    # Test full outer join
    result = ds1.join(ds2, on="key", how="full_outer")
    assert result.count() == 4
    result_list = result.take_all()
    assert {"key": 1, "value1": "a", "value2": "x"} in result_list
    assert {"key": 2, "value1": "b", "value2": "y"} in result_list
    # Find the row with key=3, then check that value2 is None
    key3_row = next(row for row in result_list if row["key"] == 3)
    assert key3_row["value2"] is None
    # Find the row with key=4, then check that value1 is None
    key4_row = next(row for row in result_list if row["key"] == 4)
    assert key4_row["value1"] is None


def test_join_with_different_columns(ray_start_regular):
    # Create sample datasets with different key column names
    customers = ray.data.from_items([
        {"customer_id": 1, "name": "Alice"},
        {"customer_id": 2, "name": "Bob"},
        {"customer_id": 3, "name": "Charlie"},
    ])
    orders = ray.data.from_items([
        {"cust_id": 1, "amount": 100},
        {"cust_id": 2, "amount": 200},
        {"cust_id": 4, "amount": 400},
    ])
    
    # Test joining with different column names
    result = customers.join(orders, left_on="customer_id", right_on="cust_id")
    assert result.count() == 2
    result_list = result.take_all()
    assert {"customer_id": 1, "name": "Alice", "cust_id": 1, "amount": 100} in result_list
    assert {"customer_id": 2, "name": "Bob", "cust_id": 2, "amount": 200} in result_list


def test_join_with_multiple_keys(ray_start_regular):
    # Create sample datasets with compound keys
    ds1 = ray.data.from_items([
        {"key1": 1, "key2": "a", "value1": "x"},
        {"key1": 2, "key2": "b", "value1": "y"},
        {"key1": 3, "key2": "c", "value1": "z"},
        {"key1": 1, "key2": "d", "value1": "w"},
    ])
    ds2 = ray.data.from_items([
        {"key1": 1, "key2": "a", "value2": 100},
        {"key1": 2, "key2": "b", "value2": 200},
        {"key1": 1, "key2": "d", "value2": 300},
        {"key1": 4, "key2": "e", "value2": 400},
    ])
    
    # Test joining with multiple keys
    result = ds1.join(ds2, on=["key1", "key2"])
    assert result.count() == 3
    result_list = result.take_all()
    
    # Check for expected results
    assert {"key1": 1, "key2": "a", "value1": "x", "value2": 100} in result_list
    assert {"key1": 2, "key2": "b", "value1": "y", "value2": 200} in result_list
    assert {"key1": 1, "key2": "d", "value1": "w", "value2": 300} in result_list


def test_merge_with_suffixes(ray_start_regular):
    # Create datasets with overlapping column names
    ds1 = ray.data.from_items([
        {"key": 1, "value": "a", "common": "left1"},
        {"key": 2, "value": "b", "common": "left2"},
        {"key": 3, "value": "c", "common": "left3"},
    ])
    ds2 = ray.data.from_items([
        {"key": 1, "value": "x", "common": "right1"},
        {"key": 2, "value": "y", "common": "right2"},
        {"key": 4, "value": "z", "common": "right3"},
    ])
    
    # Test merge with custom suffixes
    result = ds1.merge(ds2, on="key", suffixes=("_left", "_right"))
    result_list = result.take_all()
    
    # Check that duplicate columns have been correctly suffixed
    assert result.count() == 2
    for row in result_list:
        if row["key"] == 1:
            assert row["value_left"] == "a"
            assert row["value_right"] == "x"
            assert row["common_left"] == "left1"
            assert row["common_right"] == "right1"
        elif row["key"] == 2:
            assert row["value_left"] == "b"
            assert row["value_right"] == "y"
            assert row["common_left"] == "left2"
            assert row["common_right"] == "right2"


def test_join_large_datasets(ray_start_regular):
    # Create larger datasets to test performance with more data
    size = 1000
    np.random.seed(42)
    
    # Create left dataset with random data
    left_data = []
    for i in range(size):
        left_data.append({
            "id": i % 100,  # Some duplicates to test join behavior
            "value": f"left_{i}",
            "rand": np.random.randint(1, 100)
        })
    left_ds = ray.data.from_items(left_data)
    
    # Create right dataset with overlapping keys
    right_data = []
    for i in range(size // 2):
        right_data.append({
            "id": i % 150,  # Partial overlap with left
            "info": f"right_{i}",
            "rand": np.random.randint(1, 100)
        })
    right_ds = ray.data.from_items(right_data)
    
    # Test inner join
    result = left_ds.join(right_ds, on="id")
    # Calculate expected result size (matches between left and right)
    # We expect matches for ids 0-99 from left with 0-99 from right
    expected_count = sum(1 for i in range(100) if i < 150)
    assert result.count() > 0, "Join should produce results"
    
    # Test with multiple blocks to ensure distributed join works
    left_ds_multi = left_ds.repartition(10)
    right_ds_multi = right_ds.repartition(5)
    result_multi = left_ds_multi.join(right_ds_multi, on="id")
    
    # Both joins should produce the same number of results
    assert result.count() == result_multi.count()


def test_join_error_cases(ray_start_regular):
    # Create sample datasets
    ds1 = ray.data.from_items([{"key": 1, "value": "a"}])
    ds2 = ray.data.from_items([{"key": 1, "value": "x"}])
    
    # Test error when specifying both 'on' and 'left_on'
    with pytest.raises(ValueError) as exc_info:
        ds1.join(ds2, on="key", left_on="key")
    assert "Cannot specify both 'on' and 'left_on'" in str(exc_info.value)
    
    # Test error when 'left_on' is specified but 'right_on' is not
    with pytest.raises(ValueError) as exc_info:
        ds1.join(ds2, left_on="key")
    assert "Both 'left_on' and 'right_on' must be specified together" in str(exc_info.value)
    
    # Test error with invalid join type
    with pytest.raises(ValueError) as exc_info:
        ds1.join(ds2, on="key", how="invalid_join_type")
    assert "Invalid join type" in str(exc_info.value)
    

def test_join_empty_datasets(ray_start_regular):
    # Create an empty dataset
    empty_ds = ray.data.from_items([])
    
    # Create a non-empty dataset
    non_empty_ds = ray.data.from_items([
        {"key": 1, "value": "a"},
        {"key": 2, "value": "b"},
    ])
    
    # Test joining empty with non-empty
    result1 = empty_ds.join(non_empty_ds, on="key")
    assert result1.count() == 0
    
    # Test joining non-empty with empty
    result2 = non_empty_ds.join(empty_ds, on="key")
    assert result2.count() == 0
    
    # Test outer join with empty dataset
    result3 = non_empty_ds.join(empty_ds, on="key", how="full_outer")
    assert result3.count() == 2  # Should contain all rows from non-empty
    

def test_join_pandas_compatibility(ray_start_regular):
    # Create sample datasets
    ds1 = ray.data.from_items([
        {"key": 1, "a": "foo"},
        {"key": 2, "a": "bar"},
        {"key": 3, "a": "baz"},
    ])
    ds2 = ray.data.from_items([
        {"key": 1, "b": "one"},
        {"key": 2, "b": "two"},
        {"key": 4, "b": "four"},
    ])
    
    # Create equivalent pandas DataFrames for comparison
    pdf1 = pd.DataFrame({
        "key": [1, 2, 3],
        "a": ["foo", "bar", "baz"]
    })
    pdf2 = pd.DataFrame({
        "key": [1, 2, 4],
        "b": ["one", "two", "four"]
    })
    
    # Compare Ray Data join with pandas merge
    # Inner join
    ray_result = ds1.join(ds2, on="key").take_all()
    pandas_result = pdf1.merge(pdf2, on="key").to_dict('records')
    assert sorted(ray_result, key=lambda x: x["key"]) == sorted(pandas_result, key=lambda x: x["key"])
    
    # Left join
    ray_result = ds1.join(ds2, on="key", how="left_outer").take_all()
    pandas_result = pdf1.merge(pdf2, on="key", how="left").to_dict('records')
    # Sort results for comparison, treating None and NaN as equivalent
    ray_result_sorted = sorted(ray_result, key=lambda x: x["key"])
    pandas_result_sorted = sorted(pandas_result, key=lambda x: x["key"])
    # Compare with handling for NaN/None equivalence
    for ray_row, pd_row in zip(ray_result_sorted, pandas_result_sorted):
        assert ray_row["key"] == pd_row["key"]
        assert ray_row["a"] == pd_row["a"]
        # Handle NaN/None comparison
        if pd.isna(pd_row.get("b")):
            assert ray_row.get("b") is None or pd.isna(ray_row.get("b"))
        else:
            assert ray_row.get("b") == pd_row.get("b")


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))