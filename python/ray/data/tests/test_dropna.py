import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.data.tests.conftest import *  # noqa


class TestDropNa:
    def setup_method(self):
        context = ray.data.DataContext.get_current()
        self.original_preserve_order = context.execution_options.preserve_order
        context.execution_options.preserve_order = True

    def teardown_method(self):
        context = ray.data.DataContext.get_current()
        context.execution_options.preserve_order = self.original_preserve_order

    def test_dropna_any(self, ray_start_regular_shared):
        ds = ray.data.from_items(
            [
                {"a": 1, "b": 2.0, "c": "x"},
                {"a": None, "b": 3.0, "c": "y"},
                {"a": 2, "b": np.nan, "c": "z"},
                {"a": 3, "b": 4.0, "c": None},
                {"a": 4, "b": 5.0, "c": "w"},
            ]
        )
        result = ds.dropna(how="any")
        assert result.take_all() == [
            {"a": 1, "b": 2.0, "c": "x"},
            {"a": 4, "b": 5.0, "c": "w"},
        ]

    def test_dropna_all(self, ray_start_regular_shared):
        ds = ray.data.from_items(
            [
                {"a": 1, "b": 2.0, "c": "x"},
                {"a": None, "b": None, "c": None},
                {"a": 2, "b": np.nan, "c": "z"},
                {"a": None, "b": 4.0, "c": None},
            ]
        )
        result = ds.dropna(how="all")
        rows = result.take_all()
        assert len(rows) == 3
        assert rows[0]["a"] == 1
        assert rows[1]["a"] == 2
        assert rows[2]["a"] is None

    def test_dropna_subset(self, ray_start_regular_shared):
        ds = ray.data.from_items(
            [
                {"a": 1, "b": 2.0, "c": "x"},
                {"a": None, "b": 3.0, "c": "y"},
                {"a": 2, "b": np.nan, "c": "z"},
                {"a": 3, "b": 4.0, "c": None},
            ]
        )
        result = ds.dropna(subset=["a", "b"])
        assert result.take_all() == [
            {"a": 1, "b": 2.0, "c": "x"},
            {"a": 3, "b": 4.0, "c": None},
        ]

    def test_dropna_thresh(self, ray_start_regular_shared):
        ds = ray.data.from_items(
            [
                {"a": 1, "b": 2.0, "c": "x"},
                {"a": None, "b": 3.0, "c": "y"},
                {"a": 2, "b": np.nan, "c": None},
                {"a": None, "b": None, "c": None},
                {"a": 4, "b": 5.0, "c": "w"},
            ]
        )
        result = ds.dropna(thresh=2)
        rows = result.take_all()
        assert len(rows) == 3
        assert rows[0]["a"] == 1
        assert rows[1]["a"] is None
        assert rows[2]["a"] == 4

    def test_dropna_empty_dataset(self, ray_start_regular_shared):
        schema = pa.schema([("a", pa.int64())])
        ds = ray.data.from_arrow(pa.table({"a": []}, schema=schema))
        assert ds.dropna().count() == 0

    def test_dropna_ignore_values(self, ray_start_regular_shared):
        ds = ray.data.from_items(
            [
                {"a": 1, "b": "valid"},
                {"a": 0, "b": ""},
                {"a": 2, "b": "valid"},
                {"a": None, "b": "valid"},
            ]
        )
        result = ds.dropna(ignore_values=[0, ""])
        rows = result.take_all()
        assert len(rows) == 2
        assert rows[0]["a"] == 1
        assert rows[1]["a"] == 2

    @pytest.mark.parametrize("how", ["any", "all"])
    def test_dropna_how_parameter(self, ray_start_regular_shared, how):
        ds = ray.data.from_items(
            [
                {"a": 1, "b": 2, "c": 3},
                {"a": None, "b": 2, "c": 3},
                {"a": None, "b": None, "c": None},
                {"a": 1, "b": None, "c": 3},
            ]
        )
        result = ds.dropna(how=how)
        rows = result.take_all()
        if how == "any":
            assert len(rows) == 1
            assert rows[0]["a"] == 1
        else:
            assert len(rows) == 3

    def test_dropna_validation(self, ray_start_regular_shared):
        ds = ray.data.from_items([{"a": 1}])
        with pytest.raises(ValueError, match="'how' must be 'any' or 'all'"):
            ds.dropna(how="invalid")
        with pytest.raises(ValueError, match="'thresh' must be non-negative"):
            ds.dropna(thresh=-1)

    def test_dropna_preserves_schema(self, ray_start_regular_shared):
        ds = ray.data.from_items([{"a": 1, "b": None}])
        result = ds.dropna()
        assert result.schema() == ds.schema()

    def test_dropna_nonexistent_subset(self, ray_start_regular_shared):
        ds = ray.data.from_items([{"a": 1, "b": 2}, {"a": None, "b": 3}])
        result = ds.dropna(subset=["a", "nonexistent", "b"])
        assert len(result.take_all()) == 1


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__]))
