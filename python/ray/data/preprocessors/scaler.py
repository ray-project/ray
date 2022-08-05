from typing import List, Tuple

import numpy as np
import pandas as pd

from ray.data import Dataset
from ray.data.aggregate import Mean, Std, Min, Max, AbsMax
from ray.data.preprocessor import Preprocessor


class StandardScaler(Preprocessor):
    r"""Translate and scale each column by its mean and standard deviation, respectively.

    The general formula is given by

    .. math::

        x' = \frac{x - \bar{x}}{s}

    where :math:`x` is the column, :math:`x'` is the transformed column,
    :math:`\bar{x}` is the column average, and :math:`s` is the column's sample
    standard deviation. If :math:`s = 0` (i.e., the column is constant-valued),
    then the transformed column will contain zeros.

    .. warning::
        :class:`StandardScaler` works best when your data is normal. If your data isn't
        approximately normal, the transformed features won't be meaningful.

    Args:
        columns: The columns to separately scale.

    Examples:
        >>> import pandas as pd
        >>> import ray
        >>> from ray.data.preprocessors import StandardScaler
        >>>
        >>> df = pd.DataFrame({"X1": [-2, 0, 2], "X2": [-3, -3, 3], "X3": [1, 1, 1]})  # doctest: +SKIP # noqa: E501
        >>> ds = ray.data.from_pandas(df)  # doctest: +SKIP
        >>> ds.to_pandas()  # doctest: +SKIP
           X1  X2  X3
        0  -2  -3   1
        1   0  -3   1
        2   2   3   1

        Columns are scaled separately.

        >>> preprocessor = StandardScaler(columns=["X1", "X2"])  # doctest: +SKIP
        >>> preprocessor.fit_transform(ds).to_pandas()  # doctest: +SKIP
                 X1        X2  X3
        0 -1.224745 -0.707107   1
        1  0.000000 -0.707107   1
        2  1.224745  1.414214   1

        Constant-valued columns get filled with zeros.

        >>> preprocessor = StandardScaler(columns=["X3"])  # doctest: +SKIP
        >>> preprocessor.fit_transform(ds).to_pandas()  # doctest: +SKIP
           X1  X2   X3
        0  -2  -3  0.0
        1   0  -3  0.0
        2   2   3  0.0
    """

    def __init__(self, columns: List[str]):
        self.columns = columns

    def _fit(self, dataset: Dataset) -> Preprocessor:
        mean_aggregates = [Mean(col) for col in self.columns]
        std_aggregates = [Std(col, ddof=0) for col in self.columns]
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
        stats = getattr(self, "stats_", None)
        return f"StandardScaler(columns={self.columns}, stats={stats})"


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
        stats = getattr(self, "stats_", None)
        return f"MixMaxScaler(columns={self.columns}, stats={stats})"


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
        stats = getattr(self, "stats_", None)
        return f"MaxAbsScaler(columns={self.columns}, stats={stats})"


class RobustScaler(Preprocessor):
    """Scale values within columns based on their quantile range.

    For each column, each value will be transformed to
    ``(value - median) / (high_quantile - low_quantile)``,
    where ``median`` , ``high_quantile``, and ``low_quantile``
    are calculated from the fitted dataset.

    Args:
        columns: The columns that will be scaled individually.
        quantile_range: A tuple that defines the lower and upper quantile to scale to.
                        Defaults to the 1st and 3rd quartiles: (0.25, 0.75).
    """

    def __init__(
        self, columns: List[str], quantile_range: Tuple[float, float] = (0.25, 0.75)
    ):
        self.columns = columns
        self.quantile_range = quantile_range

    def _fit(self, dataset: Dataset) -> Preprocessor:
        low = self.quantile_range[0]
        med = 0.50
        high = self.quantile_range[1]

        num_records = dataset.count()
        max_index = num_records - 1
        split_indices = [int(percentile * max_index) for percentile in (low, med, high)]

        self.stats_ = {}

        # TODO(matt): Handle case where quantile lands between 2 numbers.
        # The current implementation will simply choose the closest index.
        # This will affect the results of small datasets more than large datasets.
        for col in self.columns:
            filtered_dataset = dataset.map_batches(
                lambda df: df[[col]], batch_format="pandas"
            )
            sorted_dataset = filtered_dataset.sort(col)
            _, low, med, high = sorted_dataset.split_at_indices(split_indices)

            def _get_first_value(ds: Dataset, c: str):
                return ds.take(1)[0][c]

            low_val = _get_first_value(low, col)
            med_val = _get_first_value(med, col)
            high_val = _get_first_value(high, col)

            self.stats_[f"low_quantile({col})"] = low_val
            self.stats_[f"median({col})"] = med_val
            self.stats_[f"high_quantile({col})"] = high_val

        return self

    def _transform_pandas(self, df: pd.DataFrame):
        def column_robust_scaler(s: pd.Series):
            s_low_q = self.stats_[f"low_quantile({s.name})"]
            s_median = self.stats_[f"median({s.name})"]
            s_high_q = self.stats_[f"high_quantile({s.name})"]
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
        stats = getattr(self, "stats_", None)
        return (
            f"RobustScaler("
            f"columns={self.columns}, "
            f"quantile_range={self.quantile_range}, "
            f"stats={stats})>"
        )
