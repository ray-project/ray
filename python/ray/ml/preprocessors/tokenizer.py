from typing import List, Callable, Optional

import pandas as pd

from ray.ml.preprocessor import Preprocessor
from ray.ml.preprocessors.utils import simple_split_tokenizer


class Tokenizer(Preprocessor):
    """Tokenize string columns.

    Each string entry will be replaced with a list of tokens.

    Args:
        columns: The columns that will individually be tokenized.
        tokenization_fn: The tokenization function to use.
            If not specified, a simple ``string.split(" ")`` will be used.
    """

    _is_fittable = False

    def __init__(
        self,
        columns: List[str],
        tokenization_fn: Optional[Callable[[str], List[str]]] = None,
    ):
        self.columns = columns
        # TODO(matt): Add a more robust default tokenizer.
        self.tokenization_fn = tokenization_fn or simple_split_tokenizer

    def _transform_pandas(self, df: pd.DataFrame):
        def column_tokenizer(s: pd.Series):
            return s.map(self.tokenization_fn)

        df.loc[:, self.columns] = df.loc[:, self.columns].transform(column_tokenizer)
        return df

    def __repr__(self):
        name = getattr(self.tokenization_fn, "__name__", self.tokenization_fn)
        return f"Tokenizer(columns={self.columns}, tokenization_fn={name})"
