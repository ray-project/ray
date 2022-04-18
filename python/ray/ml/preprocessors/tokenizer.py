from typing import List, Callable, Optional

import pandas as pd

from ray.ml.preprocessor import Preprocessor


class Tokenizer(Preprocessor):
    """Tokenize string columns.

    Each string entry will be replaced with a list of tokens.

    Args:
        columns: The columns that will individually be tokenized.
        fn: The tokenization function to use.
            If not specified, a simple ``string.split(" ")`` will be used.
    """

    _is_fittable = False

    def __init__(
        self, columns: List[str], fn: Optional[Callable[[str], List[str]]] = None
    ):
        super().__init__()
        self.columns = columns
        # TODO(matt): Add a more robust default tokenizer.
        self.fn = fn or _split

    def _transform_pandas(self, df: pd.DataFrame):
        def column_tokenizer(s: pd.Series):
            return s.map(self.fn)

        df.loc[:, self.columns] = df.loc[:, self.columns].transform(column_tokenizer)
        return df

    def __repr__(self):
        name = getattr(self.fn, __name__, self.fn)
        return f"<Columns={self.columns} fn={name}>"


def _split(value: str) -> List[str]:
    return value.split(" ")
