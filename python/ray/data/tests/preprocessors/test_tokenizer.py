import pandas as pd
import pytest

import ray
from ray.data.preprocessors import Tokenizer


def test_tokenizer():
    """Tests basic Tokenizer functionality."""

    col_a = ["this is a test", "apple"]
    col_b = ["the quick brown fox jumps over the lazy dog", "banana banana"]
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b})
    ds = ray.data.from_pandas(in_df)

    tokenizer = Tokenizer(["A", "B"])
    transformed = tokenizer.transform(ds)
    out_df = transformed.to_pandas()

    processed_col_a = [["this", "is", "a", "test"], ["apple"]]
    processed_col_b = [
        ["the", "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog"],
        ["banana", "banana"],
    ]
    expected_df = pd.DataFrame.from_dict({"A": processed_col_a, "B": processed_col_b})

    pd.testing.assert_frame_equal(out_df, expected_df, check_like=True)

    # Test append mode
    with pytest.raises(
        ValueError, match="The length of columns and output_columns must match."
    ):
        Tokenizer(columns=["A", "B"], output_columns=["A_tokenized"])

    tokenizer = Tokenizer(
        columns=["A", "B"], output_columns=["A_tokenized", "B_tokenized"]
    )
    transformed = tokenizer.transform(ds)
    out_df = transformed.to_pandas()
    print(out_df)
    expected_df = pd.DataFrame.from_dict(
        {
            "A": col_a,
            "B": col_b,
            "A_tokenized": processed_col_a,
            "B_tokenized": processed_col_b,
        }
    )

    pd.testing.assert_frame_equal(out_df, expected_df, check_like=True)

    # Test custom tokenization function
    def custom_tokenizer(s: str) -> list:
        return s.replace("banana", "fruit").split()

    tokenizer = Tokenizer(
        columns=["A", "B"],
        tokenization_fn=custom_tokenizer,
        output_columns=["A_custom", "B_custom"],
    )
    transformed = tokenizer.transform(ds)
    out_df = transformed.to_pandas()

    custom_processed_col_a = [["this", "is", "a", "test"], ["apple"]]
    custom_processed_col_b = [
        ["the", "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog"],
        ["fruit", "fruit"],
    ]
    expected_df = pd.DataFrame.from_dict(
        {
            "A": col_a,
            "B": col_b,
            "A_custom": custom_processed_col_a,
            "B_custom": custom_processed_col_b,
        }
    )

    pd.testing.assert_frame_equal(out_df, expected_df, check_like=True)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
