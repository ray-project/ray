import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

import ray
from ray.data.exceptions import UserCodeException
from ray.data.preprocessors import Concatenator, OneHotEncoder


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

    @pytest.mark.parametrize("col_b", [[[2, 3], [3, 4], [4, 5], [5, 6]], [2, 3, 4, 5]])
    @pytest.mark.parametrize("flatten", [True, False])
    def test_flatten(self, col_b, flatten):
        col_a = [1, 2, 3, 4]
        col_b = [np.array(v) for v in col_b] if isinstance(col_b[0], list) else col_b
        df = pd.DataFrame({"a": col_a, "b": col_b})
        ds = ray.data.from_pandas(df)
        prep = Concatenator(columns=["a", "b"], flatten=flatten)
        new_ds = prep.transform(ds)

        for i, row in enumerate(new_ds.take()):
            if flatten or not isinstance(col_b[i], np.ndarray):
                # When flatten=True or when col_b contains simple values
                if isinstance(col_b[i], np.ndarray):
                    expected = np.concatenate([np.array([col_a[i]]), col_b[i]])
                else:
                    expected = np.array([col_a[i], col_b[i]])
                assert np.array_equal(row["concat_out"], expected)
            else:
                # When flatten=False and col_b contains numpy arrays
                # The output should be a list containing the scalar and the array
                assert len(row["concat_out"]) == 2
                assert row["concat_out"][0] == col_a[i]
                assert np.array_equal(row["concat_out"][1], col_b[i])

    @pytest.mark.parametrize("flatten", [True, False])
    def test_concatenate_with_onehotencoder(self, flatten):
        df = pd.DataFrame(
            {
                "color": ["red", "green", "blue", "red"],
                "value": [1, 2, 3, 4],
            }
        )
        ds = ray.data.from_pandas(df)
        # OneHot encode the color column
        encoder = OneHotEncoder(columns=["color"], output_columns=["color_encoded"])
        encoder = encoder.fit(ds)
        encoded_ds = encoder.transform(ds)
        # Concatenate the one-hot encoded column with the value column
        prep = Concatenator(
            columns=["color_encoded", "value"],
            output_column_name="features",
            flatten=flatten,
        )
        new_ds = prep.transform(encoded_ds)
        # Get the expected one-hot vectors
        color_map = {"blue": [1, 0, 0], "green": [0, 1, 0], "red": [0, 0, 1]}
        for i, row in enumerate(new_ds.take()):
            if flatten:
                expected = color_map[df["color"][i]] + [df["value"][i]]
                assert np.array_equal(row["features"], np.array(expected))
            else:
                expected = [np.array(color_map[df["color"][i]]), df["value"][i]]
                assert np.array_equal(row["features"][0], expected[0])
                assert row["features"][1] == expected[1]

    @pytest.mark.parametrize("flatten", [True, False])
    def test_nested_list_with_dtype(self, flatten: bool):
        # Tests Concatenator with nested lists and dtype: flattens and coerces when flatten=True,
        # raises ValueError when flatten=False.
        output_column = "c"
        df = pd.DataFrame(
            {
                "a": [12.0],
                "b": [[1, 0, 0, 0]],
            }
        )
        prep = Concatenator(
            columns=["a", "b"],
            output_column_name=output_column,
            dtype=np.float32,
            flatten=flatten,
        )

        if flatten:
            pd_ds = prep._transform_pandas(df)
            expected_pd = pd.DataFrame(
                {output_column: pd.Series([[12.0, 1.0, 0.0, 0.0, 0.0]])}
            )
            assert_frame_equal(pd_ds, expected_pd)
        else:
            # Only for flattened output do we expect the dtype coercion to apply
            with pytest.raises(ValueError):
                pd_ds = prep._transform_pandas(df)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
