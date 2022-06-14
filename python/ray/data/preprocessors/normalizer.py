from typing import List

import numpy as np
import pandas as pd

from ray.data.preprocessor import Preprocessor


class Normalizer(Preprocessor):
    """Normalize each record to have unit norm.

    Supports the following normalization types:
        * l1: Sum of the absolute values.
        * l2: Square root of the sum of the squared values.
        * max: Maximum value.

    Args:
        columns: The columns that in combination define the record to normalize.
        norm: "l1", "l2", or "max". Defaults to "l2"
    """

    _norm_fns = {
        "l1": lambda cols: np.abs(cols).sum(axis=1),
        "l2": lambda cols: np.sqrt(np.power(cols, 2).sum(axis=1)),
        "max": lambda cols: np.max(abs(cols), axis=1),
    }

    _is_fittable = False

    def __init__(self, columns: List[str], norm="l2"):
        self.columns = columns
        self.norm = norm

        if norm not in self._norm_fns:
            raise ValueError(
                f"Norm {norm} is not supported."
                f"Supported values are: {self._norm_fns.keys()}"
            )

    def _transform_pandas(self, df: pd.DataFrame):
        columns = df.loc[:, self.columns]
        column_norms = self._norm_fns[self.norm](columns)

        df.loc[:, self.columns] = columns.div(column_norms, axis=0)
        return df

    def __repr__(self):
        return f"Normalizer(columns={self.columns}, norm={self.norm})>"
