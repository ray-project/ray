from typing import List, Optional, Tuple

import numpy as np
import pandas as pd

from ray.data import Dataset
from ray.data.aggregate import AbsMax, Max, Mean, Min, Std
from ray.data.preprocessor import Preprocessor
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class StandardScaler(Preprocessor):
    r"""Translate and scale each column by its mean and standard deviation,
    respectively.

    The general formula is given by

    .. math::

        x' = \frac{x - \bar{x}}{s}

    where :math:`x` is the column, :math:`x'` is the transformed column,
    :math:`\bar{x}` is the column average, and :math:`s` is the column's sample
    standard deviation. If :math:`s = 0` (i.e., the column is constant-valued),
    then the transformed column will contain zeros.

    .. warning::
        :class:`StandardScaler` works best when your data is normal. If your data isn't
        approximately normal, then the transformed features won't be meaningful.

    Examples:
        >>> import pandas as pd
        >>> import ray
        >>> from ray.data.preprocessors import StandardScaler
        >>>
        >>> df = pd.DataFrame({"X1": [-2, 0, 2], "X2": [-3, -3, 3], "X3": [1, 1, 1]})
        >>> ds = ray.data.from_pandas(df)  # doctest: +SKIP
        >>> ds.to_pandas()  # doctest: +SKIP
           X1  X2  X3
        0  -2  -3   1
        1   0  -3   1
        2   2   3   1

        Columns are scaled separately.

        >>> preprocessor = StandardScaler(columns=["X1", "X2"])
        >>> preprocessor.fit_transform(ds).to_pandas()  # doctest: +SKIP
                 X1        X2  X3
        0 -1.224745 -0.707107   1
        1  0.000000 -0.707107   1
        2  1.224745  1.414214   1

        Constant-valued columns get filled with zeros.

        >>> preprocessor = StandardScaler(columns=["X3"])
        >>> preprocessor.fit_transform(ds).to_pandas()  # doctest: +SKIP
           X1  X2   X3
        0  -2  -3  0.0
        1   0  -3  0.0
        2   2   3  0.0

        >>> preprocessor = StandardScaler(
        ...     columns=["X1", "X2"],
        ...     output_columns=["X1_scaled", "X2_scaled"]
        ... )
        >>> preprocessor.fit_transform(ds).to_pandas()  # doctest: +SKIP
           X1  X2  X3  X1_scaled  X2_scaled
        0  -2  -3   1  -1.224745  -0.707107
        1   0  -3   1   0.000000  -0.707107
        2   2   3   1   1.224745   1.414214

    Args:
        columns: The columns to separately scale.
        output_columns: The names of the transformed columns. If None, the transformed
            columns will be the same as the input columns. If not None, the length of
            ``output_columns`` must match the length of ``columns``, othwerwise an error
            will be raised.
    """

    def __init__(self, columns: List[str], output_columns: Optional[List[str]] = None):
        self.columns = columns
        self.output_columns = Preprocessor._derive_and_validate_output_columns(
            columns, output_columns
        )

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

        df[self.output_columns] = df[self.columns].transform(column_standard_scaler)
        return df

    def __repr__(self):
        return f"{self.__class__.__name__}(columns={self.columns!r}, output_columns={self.output_columns!r})"


@PublicAPI(stability="alpha")
class MinMaxScaler(Preprocessor):
    r"""Scale each column by its range.

    The general formula is given by

    .. math::

        x' = \frac{x - \min(x)}{\max{x} - \min{x}}

    where :math:`x` is the column and :math:`x'` is the transformed column. If
    :math:`\max{x} - \min{x} = 0` (i.e., the column is constant-valued), then the
    transformed column will get filled with zeros.

    Transformed values are always in the range :math:`[0, 1]`.

    .. tip::
        This can be used as an alternative to :py:class:`StandardScaler`.

    Examples:
        >>> import pandas as pd
        >>> import ray
        >>> from ray.data.preprocessors import MinMaxScaler
        >>>
        >>> df = pd.DataFrame({"X1": [-2, 0, 2], "X2": [-3, -3, 3], "X3": [1, 1, 1]})   # noqa: E501
        >>> ds = ray.data.from_pandas(df)  # doctest: +SKIP
        >>> ds.to_pandas()  # doctest: +SKIP
           X1  X2  X3
        0  -2  -3   1
        1   0  -3   1
        2   2   3   1

        Columns are scaled separately.

        >>> preprocessor = MinMaxScaler(columns=["X1", "X2"])
        >>> preprocessor.fit_transform(ds).to_pandas()  # doctest: +SKIP
            X1   X2  X3
        0  0.0  0.0   1
        1  0.5  0.0   1
        2  1.0  1.0   1

        Constant-valued columns get filled with zeros.

        >>> preprocessor = MinMaxScaler(columns=["X3"])
        >>> preprocessor.fit_transform(ds).to_pandas()  # doctest: +SKIP
           X1  X2   X3
        0  -2  -3  0.0
        1   0  -3  0.0
        2   2   3  0.0

        >>> preprocessor = MinMaxScaler(columns=["X1", "X2"], output_columns=["X1_scaled", "X2_scaled"])
        >>> preprocessor.fit_transform(ds).to_pandas()  # doctest: +SKIP
           X1  X2  X3  X1_scaled  X2_scaled
        0  -2  -3   1        0.0        0.0
        1   0  -3   1        0.5        0.0
        2   2   3   1        1.0        1.0

    Args:
        columns: The columns to separately scale.
        output_columns: The names of the transformed columns. If None, the transformed
            columns will be the same as the input columns. If not None, the length of
            ``output_columns`` must match the length of ``columns``, othwerwise an error
            will be raised.
    """

    def __init__(self, columns: List[str], output_columns: Optional[List[str]] = None):
        self.columns = columns
        self.output_columns = Preprocessor._derive_and_validate_output_columns(
            columns, output_columns
        )

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

        df[self.output_columns] = df[self.columns].transform(column_min_max_scaler)
        return df

    def __repr__(self):
        return f"{self.__class__.__name__}(columns={self.columns!r}, output_columns={self.output_columns!r})"


@PublicAPI(stability="alpha")
class MaxAbsScaler(Preprocessor):
    r"""Scale each column by its absolute max value.

    The general formula is given by

    .. math::

        x' = \frac{x}{\max{\vert x \vert}}

    where :math:`x` is the column and :math:`x'` is the transformed column. If
    :math:`\max{\vert x \vert} = 0` (i.e., the column contains all zeros), then the
    column is unmodified.

    .. tip::
        This is the recommended way to scale sparse data. If you data isn't sparse,
        you can use :class:`MinMaxScaler` or :class:`StandardScaler` instead.

    Examples:
        >>> import pandas as pd
        >>> import ray
        >>> from ray.data.preprocessors import MaxAbsScaler
        >>>
        >>> df = pd.DataFrame({"X1": [-6, 3], "X2": [2, -4], "X3": [0, 0]})   # noqa: E501
        >>> ds = ray.data.from_pandas(df)  # doctest: +SKIP
        >>> ds.to_pandas()  # doctest: +SKIP
           X1  X2  X3
        0  -6   2   0
        1   3  -4   0

        Columns are scaled separately.

        >>> preprocessor = MaxAbsScaler(columns=["X1", "X2"])
        >>> preprocessor.fit_transform(ds).to_pandas()  # doctest: +SKIP
            X1   X2  X3
        0 -1.0  0.5   0
        1  0.5 -1.0   0

        Zero-valued columns aren't scaled.

        >>> preprocessor = MaxAbsScaler(columns=["X3"])
        >>> preprocessor.fit_transform(ds).to_pandas()  # doctest: +SKIP
           X1  X2   X3
        0  -6   2  0.0
        1   3  -4  0.0

        >>> preprocessor = MaxAbsScaler(columns=["X1", "X2"], output_columns=["X1_scaled", "X2_scaled"])
        >>> preprocessor.fit_transform(ds).to_pandas()  # doctest: +SKIP
           X1  X2  X3  X1_scaled  X2_scaled
        0  -2  -3   1       -1.0       -1.0
        1   0  -3   1        0.0       -1.0
        2   2   3   1        1.0        1.0

    Args:
        columns: The columns to separately scale.
        output_columns: The names of the transformed columns. If None, the transformed
            columns will be the same as the input columns. If not None, the length of
            ``output_columns`` must match the length of ``columns``, othwerwise an error
            will be raised.
    """

    def __init__(self, columns: List[str], output_columns: Optional[List[str]] = None):
        self.columns = columns
        self.output_columns = Preprocessor._derive_and_validate_output_columns(
            columns, output_columns
        )

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

        df[self.output_columns] = df[self.columns].transform(column_abs_max_scaler)
        return df

    def __repr__(self):
        return f"{self.__class__.__name__}(columns={self.columns!r}, output_columns={self.output_columns!r})"


@PublicAPI(stability="alpha")
class RobustScaler(Preprocessor):
    r"""Scale and translate each column using quantiles.

    The general formula is given by

    .. math::
        x' = \frac{x - \mu_{1/2}}{\mu_h - \mu_l}

    where :math:`x` is the column, :math:`x'` is the transformed column,
    :math:`\mu_{1/2}` is the column median. :math:`\mu_{h}` and :math:`\mu_{l}` are the
    high and low quantiles, respectively. By default, :math:`\mu_{h}` is the third
    quartile and :math:`\mu_{l}` is the first quartile.

    .. tip::
        This scaler works well when your data contains many outliers.

    Examples:
        >>> import pandas as pd
        >>> import ray
        >>> from ray.data.preprocessors import RobustScaler
        >>>
        >>> df = pd.DataFrame({
        ...     "X1": [1, 2, 3, 4, 5],
        ...     "X2": [13, 5, 14, 2, 8],
        ...     "X3": [1, 2, 2, 2, 3],
        ... })
        >>> ds = ray.data.from_pandas(df)  # doctest: +SKIP
        >>> ds.to_pandas()  # doctest: +SKIP
           X1  X2  X3
        0   1  13   1
        1   2   5   2
        2   3  14   2
        3   4   2   2
        4   5   8   3

        :class:`RobustScaler` separately scales each column.

        >>> preprocessor = RobustScaler(columns=["X1", "X2"])
        >>> preprocessor.fit_transform(ds).to_pandas()  # doctest: +SKIP
            X1     X2  X3
        0 -1.0  0.625   1
        1 -0.5 -0.375   2
        2  0.0  0.750   2
        3  0.5 -0.750   2
        4  1.0  0.000   3

        >>> preprocessor = RobustScaler(
        ...    columns=["X1", "X2"],
        ...    output_columns=["X1_scaled", "X2_scaled"]
        ... )
        >>> preprocessor.fit_transform(ds).to_pandas()  # doctest: +SKIP
           X1  X2  X3  X1_scaled  X2_scaled
        0   1  13   1       -1.0      0.625
        1   2   5   2       -0.5     -0.375
        2   3  14   2        0.0      0.750
        3   4   2   2        0.5     -0.750
        4   5   8   3        1.0      0.000

    Args:
        columns: The columns to separately scale.
        quantile_range: A tuple that defines the lower and upper quantiles. Values
            must be between 0 and 1. Defaults to the 1st and 3rd quartiles:
            ``(0.25, 0.75)``.
        output_columns: The names of the transformed columns. If None, the transformed
            columns will be the same as the input columns. If not None, the length of
            ``output_columns`` must match the length of ``columns``, othwerwise an error
            will be raised.
    """

    def __init__(
        self,
        columns: List[str],
        quantile_range: Tuple[float, float] = (0.25, 0.75),
        output_columns: Optional[List[str]] = None,
    ):
        self.columns = columns
        self.quantile_range = quantile_range

        self.output_columns = Preprocessor._derive_and_validate_output_columns(
            columns, output_columns
        )

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

        df[self.output_columns] = df[self.columns].transform(column_robust_scaler)
        return df

    def __repr__(self):
        return (
            f"{self.__class__.__name__}(columns={self.columns!r}, "
            f"quantile_range={self.quantile_range!r}), "
            f"output_columns={self.output_columns!r})"
        )
