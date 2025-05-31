import pyarrow as pa
import pytest

import ray

def test_distinct_all_columns_local(ray_start):
    ds = ray.data.from_arrow(pa.table({"a": [1,2,2,1,3], "b": ["x","y","y","x","z"]}))
    out = ds.distinct().sort(key=["a", "b"]).take_all()
    assert out == [{'a': 1, 'b': 'x'}, {'a': 2, 'b': 'y'}, {'a': 3, 'b': 'z'}]

def test_distinct_across_blocks(ray_start):
    table = pa.table({"a": [1, 2, 2, 3, 1, 3], "b": ["x", "y", "y", "z", "x", "z"]})
    ds = ray.data.from_arrow(table).repartition(3)
    # Should still dedupe globally
    out = ds.distinct().sort(key=["a", "b"]).take_all()
    assert out == [{'a': 1, 'b': 'x'}, {'a': 2, 'b': 'y'}, {'a': 3, 'b': 'z'}]

def test_distinct_all_unique(ray_start):
    ds = ray.data.from_arrow(pa.table({"a": [1,2,3], "b": ["x","y","z"]}))
    out = ds.distinct().sort(key=["a", "b"]).take_all()
    assert out == [{'a': 1, 'b': 'x'}, {'a': 2, 'b': 'y'}, {'a': 3, 'b': 'z'}]

def test_distinct_all_duplicate(ray_start):
    ds = ray.data.from_arrow(pa.table({"a": [5,5,5,5], "b": ["foo","foo","foo","foo"]}))
    out = ds.distinct().take_all()
    assert out == [{'a': 5, 'b': 'foo'}]

def test_distinct_empty(ray_start):
    ds = ray.data.from_arrow(pa.table({"a": [], "b": []}))
    out = ds.distinct().take_all()
    assert out == []

if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
