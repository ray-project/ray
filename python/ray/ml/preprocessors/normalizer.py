from typing import List

import numpy as np
import pandas as pd

from ray.ml.preprocessor import Preprocessor


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

    _valid_norms = ["l1", "l2", "max"]
    _is_fittable = False

    def __init__(self, columns: List[str], norm="l2"):
        super().__init__()
        self.columns = columns
        self.norm = norm

        if norm not in self._valid_norms:
            raise ValueError(
                f"Norm {norm} is not supported."
                f"Supported values are: {self._valid_norms}"
            )

    def _transform_pandas(self, df: pd.DataFrame):

        columns = df.loc[:, self.columns]

        if self.norm == "l1":
            norm = np.abs(columns).sum(axis=1)
        elif self.norm == "l2":
            norm = np.sqrt(np.power(columns, 2).sum(axis=1))
        elif self.norm == "max":
            norm = np.max(abs(columns), axis=1)

        df.loc[:, self.columns] = columns.div(norm, axis=0)
        return df

    def __repr__(self):
        return f"<Columns={self.columns} norm={self.norm}>"
