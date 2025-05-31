import pytest
import ray
import pyarrow as pa


def test_fill_scalar_default(ray_start):
    table = pa.table({"a": [None, 1], "b": [None, "x"]})
    ds = ray.data.from_arrow(table)
    out = ds.fillna(0).take_all()
    # Only a filled; b is left as None (string column)
    assert out == [{"a": 0, "b": None}, {"a": 1, "b": "x"}]


def test_fill_scalar_string(ray_start):
    table = pa.table({"a": [None, 1], "b": [None, "x"]})
    ds = ray.data.from_arrow(table)
    out = ds.fillna("missing").take_all()
    # Only b filled, a is left as None
    assert out == [{"a": None, "b": "missing"}, {"a": 1, "b": "x"}]


def test_fill_dict(ray_start):
    table = pa.table({"a": [None, 1], "b": [None, "x"]})
    ds = ray.data.from_arrow(table)
    out = ds.fillna({"a": 9, "b": "y"}).take_all()
    assert out == [{"a": 9, "b": "y"}, {"a": 1, "b": "x"}]


def test_fill_subset(ray_start):
    table = pa.table({"a": [None, 1], "b": [None, None], "c": ["x", "y"]})
    ds = ray.data.from_arrow(table)
    out = ds.fillna("zzz", subset=["b", "c"]).take_all()
    assert out == [{"a": None, "b": "zzz", "c": "x"}, {"a": 1, "b": "zzz", "c": "y"}]


def test_fill_scalar_enforce_schema_raises(ray_start):
    table = pa.table({"a": [None, 1], "b": [None, "x"]})
    ds = ray.data.from_arrow(table)
    # Will raise ArrowInvalid because 0 isn't allowed for string column
    with pytest.raises((pa.ArrowInvalid, pa.ArrowTypeError)):
        ds.fillna(0, enforce_schema=True).take_all()
    # This works (int for int, string for string)
    ds.fillna({"a": 0, "b": "x"}, enforce_schema=True).take_all()


def test_fill_dict_enforce_schema_raises(ray_start):
    table = pa.table({"a": [None, 1], "b": [None, "x"]})
    ds = ray.data.from_arrow(table)
    # b can't take integer
    with pytest.raises((pa.ArrowInvalid, pa.ArrowTypeError)):
        ds.fillna({"b": 0}, enforce_schema=True).take_all()


def test_fill_missing_column(ray_start):
    # Should ignore fill for missing column
    table = pa.table({"a": [None, 42]})
    ds = ray.data.from_arrow(table)
    result = ds.fillna({"notthere": 15, "a": 10}).take_all()
    assert result == [{"a": 10}, {"a": 42}]
    # Subset with not-present column
    result = ds.fillna(0, subset="notthere").take_all()
    assert result == [{"a": None}, {"a": 42}]


def test_fill_on_multiple_batches(ray_start):
    # Test with more data than single batch
    size = 1000
    table = pa.table(
        {
            "a": [None if i % 10 == 0 else i for i in range(size)],
            "b": [None if i % 5 == 0 else str(i) for i in range(size)],
        }
    )
    ds = ray.data.from_arrow(table)
    out = ds.fillna({"a": -1, "b": "xx"}).take(limit=size)
    # Every 10th a is -1, every 5th b is xx
    for i, row in enumerate(out):
        if i % 10 == 0:
            assert row["a"] == -1
        else:
            assert row["a"] == i
        if i % 5 == 0:
            assert row["b"] == "xx"
        else:
            assert row["b"] == str(i)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
