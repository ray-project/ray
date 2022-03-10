from typing import List, Union, Optional, Dict
from numbers import Number

import pandas as pd

from ray.data import Dataset
from ray.data.aggregate import Mean, Max
from ray.ml.preprocessor import Preprocessor


class SimpleImputer(Preprocessor):
    """Populate missing values within columns.

    Args:
        columns: The columns that will individually be imputed.
        strategy: The strategy to compute the value to impute.
            - "mean": The mean of the column (numeric only).
            - "most_frequent": The most used value of the column (string or numeric).
            - "constant": The value of `fill_value` (string or numeric).
        fill_value: The value to use when `strategy` is "constant".
    """

    _valid_strategies = ["mean", "most_frequent", "constant"]

    def __init__(
        self,
        columns: List[str],
        strategy: str = "mean",
        fill_value: Optional[Union[str, Number]] = None,
    ):
        super().__init__()
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

        df = df.fillna(new_values)
        return df

    def __repr__(self):
        return f"<Imputer columns={self.columns} stats={self.stats_}>"


def _get_most_frequent_values(
    dataset: Dataset, *columns: str
) -> Dict[str, Union[str, Number]]:
    # TODO(matt): Optimize this.
    results = {}
    for column in columns:
        # Remove nulls.
        nonnull_dataset = dataset.map_batches(
            lambda df: df.dropna(subset=[column]), batch_format="pandas"
        )
        # Count values.
        counts = nonnull_dataset.groupby(column).count()
        # Find max count.
        max_aggregate = counts.aggregate(Max("count()"))
        max_count = max_aggregate["max(count())"]
        # Find values with max_count.
        most_frequent_values = counts.map_batches(
            lambda df: df.drop(df[df["count()"] < max_count].index),
            batch_format="pandas",
        )
        # Take first (sorted) value.
        most_frequent_value_count = most_frequent_values.take(1)[0]
        most_frequent_value = most_frequent_value_count[column]
        results[f"most_frequent({column})"] = most_frequent_value

    return results
