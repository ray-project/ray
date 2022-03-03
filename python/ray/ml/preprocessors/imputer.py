from typing import List

import pandas as pd

from ray.data import Dataset
from ray.data.aggregate import Mean
from ray.ml.preprocessor import Preprocessor


class SimpleImputer(Preprocessor):
    """Populate missing values within columns using their average value.

    Args:
        columns: The columns that will individually be imputed.
    """

    def __init__(self, columns: List[str]):
        super().__init__()
        self.columns = columns

    def _fit(self, dataset: Dataset) -> Preprocessor:
        aggregates = [Mean(col) for col in self.columns]
        self.stats_ = dataset.aggregate(*aggregates)
        return self

    def _transform_pandas(self, df: pd.DataFrame):
        means = {column: self.stats_[f"mean({column})"] for column in self.columns}
        df = df.fillna(means)
        return df

    def __repr__(self):
        return f"<Imputer columns={self.columns} stats={self.stats_}>"
