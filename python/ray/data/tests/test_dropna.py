import pyarrow as pa
import pytest

import ray


def test_dropna_any_default():
    ds = ray.data.from_arrow(pa.table({"a": [1, None, 3], "b": ["x", None, "y"]}))
    result = ds.dropna().sort(key="a").take_all()
    assert result == [{"a": 1, "b": "x"}, {"a": 3, "b": "y"}]


def test_dropna_how_all():
    ds = ray.data.from_arrow(pa.table({"a": [None, 2, None], "b": [None, 2, 3]}))
    result = ds.dropna(how="all").sort(key="b").take_all()
    assert result == [{"a": 2, "b": 2}, {"a": None, "b": 3}]


def test_dropna_thresh():
    ds = ray.data.from_arrow(pa.table({"a": [None, 2, 3], "b": [None, None, 4]}))
    out = ds.dropna(thresh=2).take_all()
    assert out == [{"a": 3, "b": 4}]


def test_dropna_subset_str():
    ds = ray.data.from_arrow(pa.table({"a": [None, 2, 3], "b": ["a", "b", "c"]}))
    result = ds.dropna(subset="a").sort(key="a").take_all()
    assert result == [{"a": 2, "b": "b"}, {"a": 3, "b": "c"}]


def test_dropna_subset_list():
    ds = ray.data.from_arrow(pa.table({"a": [None, 2, None], "b": [None, "b", "c"]}))
    result = ds.dropna(subset=["a", "b"]).take_all()
    assert result == [{"a": 2, "b": "b"}]


def test_dropna_all_null():
    ds = ray.data.from_arrow(pa.table({"a": [None, None], "b": [None, None]}))
    assert ds.dropna().take_all() == []
    assert ds.dropna(how="all").take_all() == []


def test_dropna_no_null():
    ds = ray.data.from_arrow(pa.table({"a": [1, 2], "b": [3, 4]}))
    assert ds.dropna().take_all() == [{"a": 1, "b": 3}, {"a": 2, "b": 4}]


def test_dropna_subset_tuple():
    ds = ray.data.from_arrow(
        pa.table({"a": [None, 2], "b": [None, "z"], "c": [10, 20]})
    )
    result = ds.dropna(subset=("a", "b")).take_all()
    assert result == [{"a": 2, "b": "z", "c": 20}]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
