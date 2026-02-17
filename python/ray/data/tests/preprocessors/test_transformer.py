import numpy as np
import pandas as pd
import pytest

import ray
from ray.data.preprocessors import PowerTransformer


def test_power_transformer():
    """Tests basic PowerTransformer functionality."""

    # yeo-johnson
    col_a = [-1, 0]
    col_b = [0, 1]
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b})
    ds = ray.data.from_pandas(in_df)

    # yeo-johnson power=0
    transformer = PowerTransformer(["A", "B"], power=0)
    transformed = transformer.transform(ds)
    out_df = transformed.to_pandas()

    processed_col_a = [-1.5, 0]
    processed_col_b = [0, np.log(2)]
    expected_df = pd.DataFrame.from_dict({"A": processed_col_a, "B": processed_col_b})

    pd.testing.assert_frame_equal(out_df, expected_df, check_like=True)

    # yeo-johnson power=2
    transformer = PowerTransformer(["A", "B"], power=2)
    transformed = transformer.transform(ds)
    out_df = transformed.to_pandas()
    processed_col_a = [-np.log(2), 0]
    processed_col_b = [0, 1.5]
    expected_df = pd.DataFrame.from_dict({"A": processed_col_a, "B": processed_col_b})

    pd.testing.assert_frame_equal(out_df, expected_df, check_like=True)

    # box-cox
    col_a = [1, 2]
    col_b = [3, 4]
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b})
    ds = ray.data.from_pandas(in_df)

    # box-cox power=0
    transformer = PowerTransformer(["A", "B"], power=0, method="box-cox")
    transformed = transformer.transform(ds)
    out_df = transformed.to_pandas()

    processed_col_a = [0, np.log(2)]
    processed_col_b = [np.log(3), np.log(4)]
    expected_df = pd.DataFrame.from_dict({"A": processed_col_a, "B": processed_col_b})

    pd.testing.assert_frame_equal(out_df, expected_df, check_like=True)

    # box-cox power=2
    transformer = PowerTransformer(["A", "B"], power=2, method="box-cox")
    transformed = transformer.transform(ds)
    out_df = transformed.to_pandas()
    processed_col_a = [0, 1.5]
    processed_col_b = [4, 7.5]
    expected_df = pd.DataFrame.from_dict({"A": processed_col_a, "B": processed_col_b})

    pd.testing.assert_frame_equal(out_df, expected_df, check_like=True)

    # Test append mode
    # First test that providing wrong number of output columns raises error
    with pytest.raises(
        ValueError, match="The length of columns and output_columns must match."
    ):
        PowerTransformer(columns=["A", "B"], power=2, output_columns=["A_transformed"])

    # Test append mode with correct output columns
    transformer = PowerTransformer(
        columns=["A", "B"],
        power=2,
        method="box-cox",
        output_columns=["A_transformed", "B_transformed"],
    )
    transformed = transformer.transform(ds)
    out_df = transformed.to_pandas()

    # Transformed columns should have the expected values
    processed_col_a = [0, 1.5]
    processed_col_b = [4, 7.5]

    expected_df = pd.DataFrame(
        {
            "A": col_a,
            "B": col_b,
            "A_transformed": processed_col_a,
            "B_transformed": processed_col_b,
        }
    )

    pd.testing.assert_frame_equal(out_df, expected_df, check_like=True)


def test_power_transformer_serialization():
    """Test PowerTransformer serialization and deserialization functionality."""
    from ray.data.preprocessor import SerializablePreprocessorBase

    # Create transformer with test data
    transformer = PowerTransformer(columns=["A", "B"], power=2.0, method="yeo-johnson")

    # Serialize using CloudPickle
    serialized = transformer.serialize()

    # Verify it's binary CloudPickle format
    assert isinstance(serialized, bytes)
    assert serialized.startswith(SerializablePreprocessorBase.MAGIC_CLOUDPICKLE)

    # Deserialize
    deserialized = PowerTransformer.deserialize(serialized)

    # Verify type and field values
    assert isinstance(deserialized, PowerTransformer)
    assert deserialized.columns == ["A", "B"]
    assert deserialized.power == 2.0
    assert deserialized.method == "yeo-johnson"
    assert deserialized.output_columns == ["A", "B"]

    # Verify it works correctly
    df = pd.DataFrame({"A": [1.0, 2.0, 3.0], "B": [4.0, 5.0, 6.0]})
    result = deserialized.transform_batch(df.copy())

    # Verify transformation was applied
    # For power=2, yeo-johnson on positive values: ((x+1)^2 - 1) / 2
    expected_a_0 = ((1.0 + 1) ** 2.0 - 1) / 2.0
    assert abs(result["A"][0] - expected_a_0) < 1e-10
    assert "A" in result.columns
    assert "B" in result.columns


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
