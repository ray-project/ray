import numpy as np
import pandas as pd
import pytest

import ray
from ray.data.preprocessors import Concatenator


class TestConcatenator:
    def test_basic(self):
        df = pd.DataFrame(
            {
                "a": [1, 2, 3, 4],
                "b": [5, 6, 7, 8],
            }
        )
        ds = ray.data.from_pandas(df)
        prep = Concatenator(output_column_name="c")
        new_ds = prep.transform(ds)
        for i, row in enumerate(new_ds.take()):
            assert np.array_equal(row["c"], np.array([i + 1, i + 5]))

    def test_raise_if_missing(self):
        df = pd.DataFrame({"a": [1, 2, 3, 4]})
        ds = ray.data.from_pandas(df)
        prep = Concatenator(
            output_column_name="c", exclude=["b"], raise_if_missing=True
        )

        with pytest.raises(ValueError, match="'b'"):
            prep.transform(ds)

    @pytest.mark.parametrize("exclude", ("b", ["b"]))
    def test_exclude(self, exclude):
        df = pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, 5], "c": [3, 4, 5, 6]})
        ds = ray.data.from_pandas(df)
        prep = Concatenator(exclude=exclude)
        new_ds = prep.transform(ds)
        for _, row in enumerate(new_ds.take()):
            assert set(row) == {"concat_out", "b"}

    def test_include(self):
        df = pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, 5], "c": [3, 4, 5, 6]})
        ds = ray.data.from_pandas(df)
        prep = Concatenator(include=["a", "b"])
        new_ds = prep.transform(ds)
        for _, row in enumerate(new_ds.take()):
            assert set(row) == {"concat_out", "c"}

    def test_exclude_overrides_include(self):
        df = pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, 5], "c": [3, 4, 5, 6]})
        ds = ray.data.from_pandas(df)
        prep = Concatenator(include=["a", "b"], exclude=["b"])
        new_ds = prep.transform(ds)
        for _, row in enumerate(new_ds.take()):
            assert set(row) == {"concat_out", "b", "c"}

    def test_strings(self):
        df = pd.DataFrame({"a": ["string", "string2", "string3"]})
        ds = ray.data.from_pandas(df)
        prep = Concatenator(output_column_name="huh")
        new_ds = prep.transform(ds)
        assert "huh" in set(new_ds.schema().names)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
