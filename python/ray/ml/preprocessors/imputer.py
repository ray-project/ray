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
        self.stats = None

    def _fit(self, dataset: Dataset) -> Preprocessor:
        aggregates = [Mean(col) for col in self.columns]
        self.stats = dataset.aggregate(*aggregates)
        return self

    def _transform_pandas(self, df: pd.DataFrame):
        for column in self.columns:
            mean = self.stats[f"mean({column})"]
            df[column] = df[column].fillna(mean)
        return df

    def __repr__(self):
        return f"<Imputer columns={self.columns} stats={self.stats}>"
