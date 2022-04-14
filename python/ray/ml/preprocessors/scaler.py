from typing import List

import numpy as np
import pandas as pd

from ray.data import Dataset
from ray.data.aggregate import Mean, Std, Min, Max, AbsMax
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


class MaxAbsScaler(Preprocessor):
    """Scale values within columns based on the absolute max value.

    For each column, each value will be transformed to ``value / abs_max``,
    where ``abs_max`` is calculated from the fitted dataset.

    When transforming the fitted dataset, transformed values will be in the
    range [-1, 1].

    Args:
        columns: The columns that will individually be scaled.
    """

    def __init__(self, columns: List[str]):
        super().__init__()
        self.columns = columns

    def _fit(self, dataset: Dataset) -> Preprocessor:
        aggregates = [AbsMax(col) for col in self.columns]
        self.stats_ = dataset.aggregate(*aggregates)
        return self

    def _transform_pandas(self, df: pd.DataFrame):
        def column_abs_max_scaler(s: pd.Series):
            s_abs_max = self.stats_[f"abs_max({s.name})"]

            # Handle division by zero.
            # All values are 0.
            if s_abs_max == 0:
                s_abs_max = 1

            return s / s_abs_max

        df.loc[:, self.columns] = df.loc[:, self.columns].transform(
            column_abs_max_scaler
        )
        return df

    def __repr__(self):
        return f"<Scaler columns={self.columns} stats={self.stats_}>"


class RobustScaler(Preprocessor):
    """Scale values within columns based on their quantile range.

    For each column, each value will be transformed to
    ``(value - median) / (high_quartile - low_quartile)``,
    where ``median`` , ``high_quartile``, and ``low_quartile``
    are  calculated from the fitted dataset.

    Args:
        columns: The columns that will individually be scaled.
    """

    def __init__(self, columns: List[str]):
        super().__init__()
        self.columns = columns

    def _fit(self, dataset: Dataset) -> Preprocessor:
        # TODO(matt): Support user-defined ranges.
        low = 0.25
        med = 0.50
        high = 0.75

        num_records = dataset.count()
        max_index = num_records - 1
        split_indices = [int(percentile * max_index) for percentile in (low, med, high)]

        self.stats_ = {}

        # TODO(matt): Handle case where quartile lands between 2 numbers.
        # The current implementation will simply choose the closest index.
        # This will affect the results of small datasets more than large datasets.
        for col in self.columns:
            sorted_dataset = dataset.sort(col)
            split_datasets = sorted_dataset.split_at_indices(split_indices)

            def _get_first_value(ds: Dataset, c: str):
                return ds.take(1)[0][c]

            low_val = _get_first_value(split_datasets[1], col)
            med_val = _get_first_value(split_datasets[2], col)
            high_val = _get_first_value(split_datasets[3], col)

            self.stats_[f"low_quartile({col})"] = low_val
            self.stats_[f"median({col})"] = med_val
            self.stats_[f"high_quartile({col})"] = high_val

        return self

    def _transform_pandas(self, df: pd.DataFrame):
        def column_robust_scaler(s: pd.Series):
            s_low_q = self.stats_[f"low_quartile({s.name})"]
            s_median = self.stats_[f"median({s.name})"]
            s_high_q = self.stats_[f"high_quartile({s.name})"]
            diff = s_high_q - s_low_q

            # Handle division by zero.
            # Return all zeros.
            if diff == 0:
                return np.zeros_like(s)

            return (s - s_median) / diff

        df.loc[:, self.columns] = df.loc[:, self.columns].transform(
            column_robust_scaler
        )
        return df

    def __repr__(self):
        return f"<Scaler columns={self.columns} stats={self.stats_}>"
