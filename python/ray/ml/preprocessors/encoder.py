from typing import List, Dict, Optional, Union

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
        self.columns = columns

    def _fit(self, dataset: Dataset) -> Preprocessor:
        self.stats_ = _get_unique_value_indices(dataset, self.columns)
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
        stats = getattr(self, "stats_", None)
        return f"OrdinalEncoder(columns={self.columns}, stats={stats})"


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
        self.columns = columns

    def _fit(self, dataset: Dataset) -> Preprocessor:
        self.stats_ = _get_unique_value_indices(dataset, self.columns)
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
        stats = getattr(self, "stats_", None)
        return f"OneHotEncoder(columns={self.columns}, stats={stats})"


class LabelEncoder(Preprocessor):
    """Encode values within a label column as ordered integer values.

    Currently, order within a column is based on the values from the fitted
    dataset in sorted order.

    Transforming values not included in the fitted dataset will be encoded as ``None``.

    Args:
        label_column: The label column that will be encoded.
    """

    def __init__(self, label_column: str):
        self.label_column = label_column

    def _fit(self, dataset: Dataset) -> Preprocessor:
        self.stats_ = _get_unique_value_indices(dataset, [self.label_column])
        return self

    def _transform_pandas(self, df: pd.DataFrame):
        _validate_df(df, self.label_column)

        def column_label_encoder(s: pd.Series):
            s_values = self.stats_[f"unique_values({s.name})"]
            return s.map(s_values)

        df[self.label_column] = df[self.label_column].transform(column_label_encoder)
        return df

    def __repr__(self):
        stats = getattr(self, "stats_", None)
        return f"LabelEncoder(label_column={self.label_column}, stats={stats})"


class Categorizer(Preprocessor):
    """Transform Dataset columns to Categorical data type.

    Note that in case of automatic inferrence, you will most
    likely want to run this preprocessor on the entire dataset
    before splitting it (e.g. into train and test sets), so
    that all of the categories are inferred. There is no risk
    of data leakage when using this preprocessor.

    Args:
        columns: The columns whose data type to change. Can be
            either a list of columns, in which case the categories
            will be inferred automatically from the data, or
            a dict of `column:pd.CategoricalDtype or None` -
            if specified, the dtype will be applied, and if not,
            it will be automatically inferred.
    """

    def __init__(
        self, columns: Union[List[str], Dict[str, Optional[pd.CategoricalDtype]]]
    ):
        self.columns = columns

    def _fit(self, dataset: Dataset) -> Preprocessor:
        columns_to_get = (
            self.columns
            if isinstance(self.columns, list)
            else [
                column for column, cat_type in self.columns.items() if cat_type is None
            ]
        )
        if columns_to_get:
            unique_indices = _get_unique_value_indices(
                dataset, columns_to_get, drop_na_values=True, key_format="{0}"
            )
            unique_indices = {
                column: pd.CategoricalDtype(values_indices.keys())
                for column, values_indices in unique_indices.items()
            }
        else:
            unique_indices = {}
        if isinstance(self.columns, dict):
            unique_indices = {**self.columns, **unique_indices}
        self.stats_: Dict[str, pd.CategoricalDtype] = unique_indices
        return self

    def _transform_pandas(self, df: pd.DataFrame):
        df = df.astype(self.stats_)
        return df

    def __repr__(self):
        stats = getattr(self, "stats_", None)
        return f"<Categorizer columns={self.columns} stats={stats}>"


def _get_unique_value_indices(
    dataset: Dataset,
    columns: List[str],
    drop_na_values: bool = False,
    key_format: str = "unique_values({0})",
) -> Dict[str, Dict[str, int]]:
    """If drop_na_values is True, will silently drop NA values."""

    def get_pd_unique_values(df: pd.DataFrame) -> List[Dict[str, set]]:
        return [{col: set(df[col].unique()) for col in columns}]

    uniques = dataset.map_batches(get_pd_unique_values, batch_format="pandas")
    final_uniques = {col: set() for col in columns}
    for batch in uniques.iter_batches():
        for col_uniques in batch:
            for col, uniques in col_uniques.items():
                final_uniques[col].update(uniques)

    for col, uniques in final_uniques.items():
        if drop_na_values:
            final_uniques[col] = {v for v in uniques if not pd.isnull(v)}
        else:
            if any(pd.isnull(v) for v in uniques):
                raise ValueError(
                    f"Unable to fit column '{col}' because it contains null values. "
                    f"Consider imputing missing values first."
                )

    unique_values_with_indices = {
        key_format.format(column): {
            k: j for j, k in enumerate(sorted(final_uniques[column]))
        }
        for column in columns
    }
    return unique_values_with_indices


def _validate_df(df: pd.DataFrame, *columns: str) -> None:
    null_columns = [column for column in columns if df[column].isnull().values.any()]
    if null_columns:
        raise ValueError(
            f"Unable to transform columns {null_columns} because they contain "
            f"null values. Consider imputing missing values first."
        )
