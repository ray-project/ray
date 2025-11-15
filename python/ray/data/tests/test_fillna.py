import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.data.tests.conftest import *  # noqa


class TestFillNa:
    def setup_method(self):
        context = ray.data.DataContext.get_current()
        self.original_preserve_order = context.execution_options.preserve_order
        context.execution_options.preserve_order = True

    def teardown_method(self):
        context = ray.data.DataContext.get_current()
        context.execution_options.preserve_order = self.original_preserve_order

    def test_fillna_scalar_value(self, ray_start_regular_shared):
        ds = ray.data.from_items(
            [
                {"a": 1, "b": 2.0, "c": "x"},
                {"a": None, "b": np.nan, "c": None},
                {"a": 3, "b": None, "c": "z"},
            ]
        )
        result = ds.fillna(0)
        assert result.take_all() == [
            {"a": 1, "b": 2.0, "c": "x"},
            {"a": 0, "b": 0.0, "c": "0"},
            {"a": 3, "b": 0.0, "c": "z"},
        ]

    def test_fillna_dict_values(self, ray_start_regular_shared):
        ds = ray.data.from_items(
            [
                {"a": 1, "b": 2.0, "c": "x"},
                {"a": None, "b": np.nan, "c": None},
                {"a": 3, "b": None, "c": "z"},
            ]
        )
        result = ds.fillna({"a": -1, "b": -2.0, "c": "missing"})
        assert result.take_all() == [
            {"a": 1, "b": 2.0, "c": "x"},
            {"a": -1, "b": -2.0, "c": "missing"},
            {"a": 3, "b": -2.0, "c": "z"},
        ]

    def test_fillna_with_subset(self, ray_start_regular_shared):
        ds = ray.data.from_items(
            [
                {"a": 1, "b": 2.0, "c": "x"},
                {"a": None, "b": np.nan, "c": None},
            ]
        )
        result = ds.fillna(0, subset=["a"])
        rows = result.take_all()
        assert rows[0]["a"] == 1
        assert rows[1]["a"] == 0
        assert pd.isna(rows[1]["b"])

    def test_fillna_empty_dataset(self, ray_start_regular_shared):
        schema = pa.schema([("a", pa.int64())])
        ds = ray.data.from_arrow(pa.table({"a": []}, schema=schema))
        assert ds.fillna(0).count() == 0

    @pytest.mark.parametrize("method", ["forward", "backward"])
    def test_fillna_directional(self, ray_start_regular_shared, method):
        ds = ray.data.from_items(
            [
                {"a": 1, "b": 1.0},
                {"a": None, "b": None},
                {"a": 3, "b": 3.0},
                {"a": None, "b": None},
                {"a": 5, "b": 5.0},
            ]
        )
        result = ds.fillna(method=method)
        rows = result.take_all()
        if method == "forward":
            assert rows == [
                {"a": 1, "b": 1.0},
                {"a": 1, "b": 1.0},
                {"a": 3, "b": 3.0},
                {"a": 3, "b": 3.0},
                {"a": 5, "b": 5.0},
            ]
        else:
            assert rows == [
                {"a": 1, "b": 1.0},
                {"a": 3, "b": 3.0},
                {"a": 3, "b": 3.0},
                {"a": 5, "b": 5.0},
                {"a": 5, "b": 5.0},
            ]

    def test_fillna_interpolate(self, ray_start_regular_shared):
        ds = ray.data.from_items(
            [
                {"a": 1, "b": 1.0},
                {"a": None, "b": None},
                {"a": 5, "b": 5.0},
                {"a": None, "b": None},
                {"a": 9, "b": 9.0},
            ]
        )
        result = ds.fillna(method="interpolate")
        rows = result.take_all()
        assert rows[0]["a"] == 1
        assert rows[1]["a"] == 3
        assert rows[2]["a"] == 5
        assert rows[3]["a"] == 7
        assert rows[4]["a"] == 9

    def test_fillna_with_limit(self, ray_start_regular_shared):
        ds = ray.data.from_items(
            [{"a": 1}, {"a": None}, {"a": None}, {"a": None}, {"a": 5}]
        )
        result = ds.fillna(method="forward", limit=2)
        rows = result.take_all()
        assert rows[0]["a"] == 1
        assert rows[1]["a"] == 1
        assert rows[2]["a"] == 1
        assert rows[3]["a"] is None
        assert rows[4]["a"] == 5

    def test_fillna_method_validation(self, ray_start_regular_shared):
        ds = ray.data.from_items([{"a": 1}])
        with pytest.raises(ValueError, match="Unsupported method"):
            ds.fillna(method="invalid")
        with pytest.raises(ValueError, match="'value' parameter is required"):
            ds.fillna(method="value")

    def test_fillna_preserves_schema(self, ray_start_regular_shared):
        ds = ray.data.from_items([{"a": 1, "b": None}])
        result = ds.fillna({"a": 0, "b": 0.0})
        assert result.schema() == ds.schema()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__]))
