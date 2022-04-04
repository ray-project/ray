from typing import List, Dict, Set

import pandas as pd

from ray.data import Dataset
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
        self.stats_ = _get_unique_value_indices(dataset, *self.columns)
        return self

    def _transform_pandas(self, df: pd.DataFrame):
        _validate_df(df, *self.columns)

        def column_ordinal_encoder(s: pd.Series):
            s_values = self.stats_[f"unique_values({s.name})"]
            return s.map(s_values)

        df.loc[:, self.columns] = df.loc[:, self.columns].transform(
            column_ordinal_encoder
        )
        return df

    def __repr__(self):
        return f"<Encoder columns={self.columns} stats={self.stats_}>"


class OneHotEncoder(Preprocessor):
    """Encode columns as new columns using one-hot encoding.

    The transformed dataset will have a new column in the form ``{column}_{value}``
    for each of the values from the fitted dataset. The value of a column will
    be set to 1 if the value matches, otherwise 0.

    Transforming values not included in the fitted dataset will result in all
    of the encoded column values being 0.

    Args:
        columns: The columns that will individually be encoded.
    """

    def __init__(self, columns: List[str]):
        # TODO: add `drop` parameter.
        super().__init__()
        self.columns = columns

    def _fit(self, dataset: Dataset) -> Preprocessor:
        self.stats_ = _get_unique_value_indices(dataset, *self.columns)
        return self

    def _transform_pandas(self, df: pd.DataFrame):
        _validate_df(df, *self.columns)
        # Compute new one-hot encoded columns
        for column in self.columns:
            column_values = self.stats_[f"unique_values({column})"]
            for column_value in column_values:
                df[f"{column}_{column_value}"] = (df[column] == column_value).astype(
                    int
                )
        # Drop original unencoded columns.
        df = df.drop(columns=self.columns)
        return df

    def __repr__(self):
        return f"<Encoder columns={self.columns} stats={self.stats_}>"


class LabelEncoder(Preprocessor):
    """Encode values within a label column as ordered integer values.

    Currently, order within a column is based on the values from the fitted
    dataset in sorted order.

    Transforming values not included in the fitted dataset will be encoded as ``None``.

    Args:
        label_column: The label column that will be encoded.
    """

    def __init__(self, label_column: str):
        super().__init__()
        self.label_column = label_column

    def _fit(self, dataset: Dataset) -> Preprocessor:
        self.stats_ = _get_unique_value_indices(dataset, self.label_column)
        return self

    def _transform_pandas(self, df: pd.DataFrame):
        _validate_df(df, self.label_column)

        def column_label_encoder(s: pd.Series):
            s_values = self.stats_[f"unique_values({s.name})"]
            return s.map(s_values)

        df[self.label_column] = df[self.label_column].transform(column_label_encoder)
        return df

    def __repr__(self):
        return f"<Encoder label column={self.label_column} stats={self.stats_}>"


def _get_unique_value_indices(
    dataset: Dataset, *columns: str
) -> Dict[str, Dict[str, int]]:
    results = {}
    for column in columns:
        values = _get_unique_values(dataset, column)
        if any(pd.isnull(v) for v in values):
            raise ValueError(
                f"Unable to fit column '{column}' because it contains null values. "
                f"Consider imputing missing values first."
            )
        value_to_index = _sorted_value_indices(values)
        results[f"unique_values({column})"] = value_to_index
    return results


def _get_unique_values(dataset: Dataset, column: str) -> Set[str]:
    agg_ds = dataset.groupby(column).count()
    # TODO: Support an upper limit by using `agg_ds.take(N)` instead.
    return {row[column] for row in agg_ds.iter_rows()}


def _sorted_value_indices(values: Set) -> Dict[str, int]:
    """Converts values to a Dict mapping to unique indexes.

    Values will be sorted.

    Example:
        >>> _sorted_value_indices({"b", "a", "c", "a"})
        {"a": 0, "b": 1, "c": 2}
    """
    return {value: i for i, value in enumerate(sorted(values))}


def _validate_df(df: pd.DataFrame, *columns: str) -> None:
    null_columns = [column for column in columns if df[column].isnull().values.any()]
    if null_columns:
        raise ValueError(
            f"Unable to transform columns {null_columns} because they contain "
            f"null values. Consider imputing missing values first."
        )
