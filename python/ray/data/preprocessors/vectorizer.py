from collections import Counter
from typing import List, Callable, Optional

import pandas as pd

from ray.data import Dataset
from ray.data.preprocessor import Preprocessor
from ray.data.preprocessors.utils import simple_split_tokenizer, simple_hash


class HashingVectorizer(Preprocessor):
    """Tokenize and hash string columns into token hash columns.

    The created columns will have names in the format ``hash_{column_name}_{hash}``.

    Args:
        columns: The columns that will individually be hashed.
        num_features: The size of each hashed feature vector.
        tokenization_fn: The tokenization function to use.
            If not specified, a simple ``string.split(" ")`` will be used.
    """

    _is_fittable = False

    def __init__(
        self,
        columns: List[str],
        num_features: int,
        tokenization_fn: Optional[Callable[[str], List[str]]] = None,
    ):
        self.columns = columns
        # TODO(matt): Set default number of features.
        # This likely requires sparse matrix support to avoid explosion of columns.
        self.num_features = num_features
        # TODO(matt): Add a more robust default tokenizer.
        self.tokenization_fn = tokenization_fn or simple_split_tokenizer

    def _transform_pandas(self, df: pd.DataFrame):
        # TODO(matt): Use sparse matrix for efficiency.

        def hash_count(tokens: List[str]) -> Counter:
            hashed_tokens = [simple_hash(token, self.num_features) for token in tokens]
            return Counter(hashed_tokens)

        for col in self.columns:
            tokenized = df[col].map(self.tokenization_fn)
            hashed = tokenized.map(hash_count)
            for i in range(self.num_features):
                df[f"hash_{col}_{i}"] = hashed.map(lambda counts: counts[i])

        # Drop original columns.
        df.drop(columns=self.columns, inplace=True)
        return df

    def __repr__(self):
        fn_name = getattr(self.tokenization_fn, "__name__", self.tokenization_fn)
        return (
            f"HashingVectorizer("
            f"Columns={self.columns}, "
            f"num_features={self.num_features}, "
            f"tokenization_fn={fn_name})"
        )


class CountVectorizer(Preprocessor):
    """Tokenize string columns and convert into token columns.

    The created columns will have names in the format ``{column_name}_{token}``.

    Token features will be sorted by count in descending order.

    Args:
        columns: The columns that will individually be tokenized.
        tokenization_fn: The tokenization function to use.
            If not specified, a simple ``string.split(" ")`` will be used.
        max_features: If specified, limit the number of tokens.
            The tokens with the largest counts will be kept.
    """

    def __init__(
        self,
        columns: List[str],
        tokenization_fn: Optional[Callable[[str], List[str]]] = None,
        max_features: Optional[int] = None,
    ):
        # TODO(matt): Add fit_transform to avoid recomputing tokenization step.
        self.columns = columns
        # TODO(matt): Add a more robust default tokenizer.
        self.tokenization_fn = tokenization_fn or simple_split_tokenizer
        self.max_features = max_features

    def _fit(self, dataset: Dataset) -> Preprocessor:
        def get_pd_value_counts(df: pd.DataFrame) -> List[Counter]:
            def get_token_counts(col):
                token_series = df[col].apply(self.tokenization_fn)
                tokens = token_series.sum()
                return Counter(tokens)

            return [get_token_counts(col) for col in self.columns]

        value_counts = dataset.map_batches(get_pd_value_counts, batch_format="pandas")
        total_counts = [Counter() for _ in self.columns]
        for batch in value_counts.iter_batches():
            for i, col_value_counts in enumerate(batch):
                total_counts[i].update(col_value_counts)

        def most_common(counter: Counter, n: int):
            return Counter(dict(counter.most_common(n)))

        top_counts = [
            most_common(counter, self.max_features) for counter in total_counts
        ]

        self.stats_ = {
            f"token_counts({col})": counts
            for (col, counts) in zip(self.columns, top_counts)
        }

        return self

    def _transform_pandas(self, df: pd.DataFrame):

        for col in self.columns:
            token_counts = self.stats_[f"token_counts({col})"]
            sorted_tokens = [token for (token, count) in token_counts.most_common()]
            tokenized = df[col].map(self.tokenization_fn).map(Counter)
            for token in sorted_tokens:
                df[f"{col}_{token}"] = tokenized.map(lambda val: val[token])

        # Drop original columns.
        df.drop(columns=self.columns, inplace=True)
        return df

    def __repr__(self):
        fn_name = getattr(self.tokenization_fn, "__name__", self.tokenization_fn)
        return (
            f"CountVectorizer("
            f"columns={self.columns}, "
            f"tokenization_fn={fn_name}, "
            f"max_features={self.max_features})"
        )
