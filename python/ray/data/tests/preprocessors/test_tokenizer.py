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

    assert out_df.equals(expected_df)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
