from typing import List, Union, Optional, Dict
from numbers import Number
from collections import Counter

import pandas as pd
from pandas.api.types import is_categorical_dtype

from ray.data import Dataset
from ray.data.aggregate import Mean
from ray.data.preprocessor import Preprocessor
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class SimpleImputer(Preprocessor):
    """Replace missing values with imputed values.

    Examples:
        >>> import pandas as pd
        >>> import ray
        >>> from ray.data.preprocessors import SimpleImputer
        >>> df = pd.DataFrame({"X": [0, None, 3, 3], "Y": [None, "b", "c", "c"]})
        >>> ds = ray.data.from_pandas(df)  # doctest: +SKIP
        >>> ds.to_pandas()  # doctest: +SKIP
             X     Y
        0  0.0  None
        1  NaN     b
        2  3.0     c
        3  3.0     c

        The `"mean"` strategy imputes missing values with the mean of non-missing
        values. This strategy doesn't work with categorical data.

        >>> preprocessor = SimpleImputer(columns=["X"], strategy="mean")
        >>> preprocessor.fit_transform(ds).to_pandas()  # doctest: +SKIP
             X     Y
        0  0.0  None
        1  2.0     b
        2  3.0     c
        3  3.0     c

        The `"most_frequent"` strategy imputes missing values with the most frequent
        value in each column.

        >>> preprocessor = SimpleImputer(columns=["X", "Y"], strategy="most_frequent")
        >>> preprocessor.fit_transform(ds).to_pandas()  # doctest: +SKIP
             X  Y
        0  0.0  c
        1  3.0  b
        2  3.0  c
        3  3.0  c

        The `"constant"` strategy imputes missing values with the value specified by
        `fill_value`.

        >>> preprocessor = SimpleImputer(
        ...     columns=["Y"],
        ...     strategy="constant",
        ...     fill_value="?",
        ... )
        >>> preprocessor.fit_transform(ds).to_pandas()  # doctest: +SKIP
             X  Y
        0  0.0  ?
        1  NaN  b
        2  3.0  c
        3  3.0  c

    Args:
        columns: The columns to apply imputation to.
        strategy: How imputed values are chosen.

            * ``"mean"``: The mean of non-missing values. This strategy only works with numeric columns.
            * ``"most_frequent"``: The most common value.
            * ``"constant"``: The value passed to ``fill_value``.

        fill_value: The value to use when ``strategy`` is ``"constant"``.

    Raises:
        ValueError: if ``strategy`` is not ``"mean"``, ``"most_frequent"``, or
            ``"constant"``.
    """  # noqa: E501

    _valid_strategies = ["mean", "most_frequent", "constant"]

    def __init__(
        self,
        columns: List[str],
        strategy: str = "mean",
        fill_value: Optional[Union[str, Number]] = None,
    ):
        self.columns = columns
        self.strategy = strategy
        self.fill_value = fill_value

        if strategy not in self._valid_strategies:
            raise ValueError(
                f"Strategy {strategy} is not supported."
                f"Supported values are: {self._valid_strategies}"
            )

        if strategy == "constant":
            # There is no information to be fitted.
            self._is_fittable = False
            if fill_value is None:
                raise ValueError(
                    '`fill_value` must be set when using "constant" strategy.'
                )

    def _fit(self, dataset: Dataset) -> Preprocessor:
        if self.strategy == "mean":
            aggregates = [Mean(col) for col in self.columns]
            self.stats_ = dataset.aggregate(*aggregates)
        elif self.strategy == "most_frequent":
            self.stats_ = _get_most_frequent_values(dataset, *self.columns)

        return self

    def _transform_pandas(self, df: pd.DataFrame):
        if self.strategy == "mean":
            new_values = {
                column: self.stats_[f"mean({column})"] for column in self.columns
            }
        elif self.strategy == "most_frequent":
            new_values = {
                column: self.stats_[f"most_frequent({column})"]
                for column in self.columns
            }
        elif self.strategy == "constant":
            new_values = {column: self.fill_value for column in self.columns}
            for column, value in new_values.items():
                if is_categorical_dtype(df.dtypes[column]):
                    df[column] = df[column].cat.add_categories(value)

        df = df.fillna(new_values)
        return df

    def __repr__(self):
        return (
            f"{self.__class__.__name__}(columns={self.columns!r}, "
            f"strategy={self.strategy!r}, fill_value={self.fill_value!r})"
        )


def _get_most_frequent_values(
    dataset: Dataset, *columns: str
) -> Dict[str, Union[str, Number]]:
    columns = list(columns)

    def get_pd_value_counts(df: pd.DataFrame) -> List[Dict[str, Counter]]:
        return [{col: Counter(df[col].value_counts().to_dict()) for col in columns}]

    value_counts = dataset.map_batches(get_pd_value_counts, batch_format="pandas")
    final_counters = {col: Counter() for col in columns}
    for batch in value_counts.iter_batches(batch_size=None):
        for col_value_counts in batch:
            for col, value_counts in col_value_counts.items():
                final_counters[col] += value_counts

    return {
        f"most_frequent({column})": final_counters[column].most_common(1)[0][0]
        for column in columns
    }
