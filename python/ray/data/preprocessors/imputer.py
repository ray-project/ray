from typing import List, Union, Optional, Dict
from numbers import Number
from collections import Counter

import pandas as pd

from ray.data import Dataset
from ray.data.aggregate import Mean
from ray.data.preprocessor import Preprocessor


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
        stats = getattr(self, "stats_", None)
        return (
            f"SimpleImputer("
            f"columns={self.columns}, "
            f"strategy={self.strategy}, "
            f"fill_value={self.fill_value}, "
            f"stats={stats})"
        )


def _get_most_frequent_values(
    dataset: Dataset, *columns: str
) -> Dict[str, Union[str, Number]]:
    columns = list(columns)

    def get_pd_value_counts(df: pd.DataFrame) -> List[Dict[str, Counter]]:
        return [{col: Counter(df[col].value_counts().to_dict()) for col in columns}]

    value_counts = dataset.map_batches(get_pd_value_counts, batch_format="pandas")
    final_counters = {col: Counter() for col in columns}
    for batch in value_counts.iter_batches():
        for col_value_counts in batch:
            for col, value_counts in col_value_counts.items():
                final_counters[col] += value_counts

    return {
        f"most_frequent({column})": final_counters[column].most_common(1)[0][0]
        for column in columns
    }
