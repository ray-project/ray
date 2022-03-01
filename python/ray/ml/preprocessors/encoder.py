from typing import List, Dict

import pandas as pd

from ray.data import Dataset
from ray.data.aggregate import UniqueValues
from ray.ml.preprocessor import Preprocessor


class OrdinalEncoder(Preprocessor):
    """Encode values within columns as ordered integer values.

    Currently, order within a column is based on the values from the fitted
    dataset in sorted order.

    Transforming values not included in the fitted dataset will be encoded as ``None``.

    Args:
        columns: The columns that will individually be encoded.
    """

    def __init__(self, columns: List[str]):
        # TODO: allow user to specify order of values within each column.
        super().__init__()
        self.columns = columns

    def _fit(self, dataset: Dataset) -> Preprocessor:
        aggregates = [UniqueValues(col) for col in self.columns]
        stats = dataset.aggregate(*aggregates)
        self.stats = {row: _sorted_value_indices(stats[row]) for row in stats}
        return self

    def _transform_pandas(self, df: pd.DataFrame):
        def column_ordinal_encoder(s: pd.Series):
            s_values = self.stats[f"unique_values({s.name})"]
            return s.map(s_values)

        df.loc[:, self.columns] = df.loc[:, self.columns].transform(
            column_ordinal_encoder
        )
        return df

    def __repr__(self):
        return f"<Encoder columns={self.columns} stats={self.stats}>"


class OneHotEncoder(Preprocessor):
    """Encode columns as new columns using one-hot encoding.

    The transformed dataset will have a new column in the form ``{column}_{value}``
    for each of the values from the fitted dataset. The value of a column will
    be set to 1 if the value matches, otherwise 0.

    Transforming values not included in the fitted dataset will result in all
    of the encoded column values to be 0.

    Args:
        columns: The columns that will individually be encoded.
    """

    def __init__(self, columns: List[str]):
        super().__init__()
        self.columns = columns

    def _fit(self, dataset: Dataset) -> Preprocessor:
        aggregates = [UniqueValues(col) for col in self.columns]
        stats = dataset.aggregate(*aggregates)
        self.stats = {row: _sorted_value_indices(stats[row]) for row in stats}
        return self

    def _transform_pandas(self, df: pd.DataFrame):
        for column in self.columns:
            column_values = self.stats[f"unique_values({column})"]
            for column_value in column_values:
                df[f"{column}_{column_value}"] = df[column].map(
                    lambda x: int(x == column_value)
                )

        # Drop original unencoded columns.
        df = df.drop(columns=self.columns)
        return df

    def __repr__(self):
        return f"<Encoder columns={self.columns} stats={self.stats}>"


class LabelEncoder(Preprocessor):
    """Encode values within a label column as ordered integer values.

    Currently, order within a column is based on the values from the fitted
    dataset in sorted order.

    Transforming values not included in the fitted dataset will be encoded as ``None``.

    Args:
        label_column The label column that will be encoded.
    """

    def __init__(self, label_column: str):
        super().__init__()
        self.label_column = label_column

    def _fit(self, dataset: Dataset) -> Preprocessor:
        stats = dataset.aggregate(UniqueValues(self.label_column))
        self.stats = {row: _sorted_value_indices(stats[row]) for row in stats}
        return self

    def _transform_pandas(self, df: pd.DataFrame):
        def column_label_encoder(s: pd.Series):
            s_values = self.stats[f"unique_values({s.name})"]
            return s.map(s_values)

        df[self.label_column] = df[self.label_column].transform(column_label_encoder)
        return df

    def __repr__(self):
        return f"<Encoder label column={self.label_column} stats={self.stats}>"


def _sorted_value_indices(values: List) -> Dict:
    """Converts values to a Dict mapping to unique indexes.

    Values will be de-duped and sorted.

    Example:
        >>> _sorted_value_indices(["b", "a", "c", "a"])
        {"a": 0, "b": 1, "c": 2}
    """
    return {value: i for i, value in enumerate(sorted(set(values)))}
