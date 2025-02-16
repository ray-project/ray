import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

import ray
from ray.data.exceptions import UserCodeException
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
        prep = Concatenator(columns=["a", "b"], output_column_name="c")
        new_ds = prep.transform(ds)
        for i, row in enumerate(new_ds.take()):
            assert np.array_equal(row["c"], np.array([i + 1, i + 5]))

    def test_raise_if_missing(self):
        df = pd.DataFrame({"a": [1, 2, 3, 4]})
        ds = ray.data.from_pandas(df)
        prep = Concatenator(
            columns=["a", "b"], output_column_name="c", raise_if_missing=True
        )

        with pytest.raises(UserCodeException):
            with pytest.raises(ValueError, match="'b'"):
                prep.transform(ds).materialize()

    def test_exclude_column(self):
        df = pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, 5], "c": [3, 4, 5, 6]})
        ds = ray.data.from_pandas(df)
        prep = Concatenator(columns=["a", "c"])
        new_ds = prep.transform(ds)
        for _, row in enumerate(new_ds.take()):
            assert set(row) == {"concat_out", "b"}

    def test_include_columns(self):
        df = pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, 5], "c": [3, 4, 5, 6]})
        ds = ray.data.from_pandas(df)
        prep = Concatenator(columns=["a", "b"])
        new_ds = prep.transform(ds)
        for _, row in enumerate(new_ds.take()):
            assert set(row) == {"concat_out", "c"}

    def test_change_column_order(self):
        df = pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, 5]})
        ds = ray.data.from_pandas(df)
        prep = Concatenator(columns=["b", "a"])
        new_ds = prep.transform(ds)
        expected_df = pd.DataFrame({"concat_out": [[2, 1], [3, 2], [4, 3], [5, 4]]})
        print(new_ds.to_pandas())
        assert_frame_equal(new_ds.to_pandas(), expected_df)

    def test_strings(self):
        df = pd.DataFrame({"a": ["string", "string2", "string3"]})
        ds = ray.data.from_pandas(df)
        prep = Concatenator(columns=["a"], output_column_name="huh")
        new_ds = prep.transform(ds)
        assert "huh" in set(new_ds.schema().names)

    def test_preserves_order(self):
        df = pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, 5]})
        ds = ray.data.from_pandas(df)
        prep = Concatenator(columns=["a", "b"], output_column_name="c")
        prep = prep.fit(ds)

        df = pd.DataFrame({"a": [5, 6, 7, 8], "b": [6, 7, 8, 9]})
        concatenated_df = prep.transform_batch(df)
        expected_df = pd.DataFrame({"c": [[5, 6], [6, 7], [7, 8], [8, 9]]})
        assert_frame_equal(concatenated_df, expected_df)

        other_df = pd.DataFrame({"a": [9, 10, 11, 12], "b": [10, 11, 12, 13]})
        concatenated_other_df = prep.transform_batch(other_df)
        expected_df = pd.DataFrame(
            {
                "c": [
                    [9, 10],
                    [10, 11],
                    [11, 12],
                    [12, 13],
                ]
            }
        )
        assert_frame_equal(concatenated_other_df, expected_df)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
