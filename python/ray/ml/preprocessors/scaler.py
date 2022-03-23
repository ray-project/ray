from typing import List

import pandas as pd

from ray.data import Dataset
from ray.data.aggregate import Mean, Std, Min, Max
from ray.ml.preprocessor import Preprocessor


class StandardScaler(Preprocessor):
    """Scale values within columns based on mean and standard deviation.

    For each column, each value will be transformed to ``(value-mean)/std``,
    where ``mean`` and ``std`` are calculated from the fitted dataset.

    Args:
        columns: The columns that will individually be scaled.
        ddof: The delta degrees of freedom used to calculate standard deviation.
    """

    def __init__(self, columns: List[str], ddof=0):
        super().__init__()
        self.columns = columns
        self.ddof = ddof

    def _fit(self, dataset: Dataset) -> Preprocessor:
        mean_aggregates = [Mean(col) for col in self.columns]
        std_aggregates = [Std(col, ddof=self.ddof) for col in self.columns]
        self.stats_ = dataset.aggregate(*mean_aggregates, *std_aggregates)
        return self

    def _transform_pandas(self, df: pd.DataFrame):
        def column_standard_scaler(s: pd.Series):
            s_mean = self.stats_[f"mean({s.name})"]
            s_std = self.stats_[f"std({s.name})"]

            # Handle division by zero.
            # TODO: extend this to handle near-zero values.
            if s_std == 0:
                s_std = 1

            return (s - s_mean) / s_std

        df.loc[:, self.columns] = df.loc[:, self.columns].transform(
            column_standard_scaler
        )
        return df

    def __repr__(self):
        return f"<Scaler columns={self.columns} stats={self.stats_}>"


class MinMaxScaler(Preprocessor):
    """Scale values within columns based on min and max values.

    For each column, each value will be transformed to
    ``(value - min) / (max - min)``,
    where ``min`` and ``max`` are calculated from the fitted dataset.

    When transforming the fitted dataset, transformed values will be in the
    range [0, 1].

    Args:
        columns: The columns that will individually be scaled.
    """

    def __init__(self, columns: List[str]):
        super().__init__()
        self.columns = columns

    def _fit(self, dataset: Dataset) -> Preprocessor:
        aggregates = [Agg(col) for Agg in [Min, Max] for col in self.columns]
        self.stats_ = dataset.aggregate(*aggregates)
        return self

    def _transform_pandas(self, df: pd.DataFrame):
        def column_min_max_scaler(s: pd.Series):
            s_min = self.stats_[f"min({s.name})"]
            s_max = self.stats_[f"max({s.name})"]
            diff = s_max - s_min

            # Handle division by zero.
            # TODO: extend this to handle near-zero values.
            if diff == 0:
                diff = 1

            return (s - s_min) / diff

        df.loc[:, self.columns] = df.loc[:, self.columns].transform(
            column_min_max_scaler
        )
        return df

    def __repr__(self):
        return f"<Scaler columns={self.columns} stats={self.stats_}>"
