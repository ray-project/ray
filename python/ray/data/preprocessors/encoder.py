from functools import partial
from typing import List, Dict, Optional, Union

from collections import Counter, OrderedDict
import pandas as pd
import pandas.api.types

from ray.data import Dataset
from ray.data.preprocessor import Preprocessor


class OrdinalEncoder(Preprocessor):
    """Encode values within columns as ordered integer values.

    Currently, order within a column is based on the values from the fitted
    dataset in sorted order.

    Transforming values not included in the fitted dataset will be encoded as ``None``.

    All column values must be hashable scalars or lists of hashable values. Those
    two types cannot be mixed.

    Example:

    .. code-block:: python

        import ray.data
        from ray.data.preprocessors import OrdinalEncoder
        import pandas as pd
        batch = pd.DataFrame(
            {
                "A": [["warm"], [], ["hot", "warm", "cold"], ["cold", "cold"]],
                "B": ["warm", "cold", "hot", "cold"],
            },
        )
        oe = OrdinalEncoder(columns=["A", "B"], encode_lists=True)
        oe.fit(ray.data.from_pandas(batch))
        transformed_batch = oe.transform_batch(batch)
        expected_batch = pd.DataFrame(
            {
                "A": [[2], [], [1, 2, 0], [0, 0]],
                "B": [2, 0, 1, 0],
            }
        )
        assert transformed_batch.equals(expected_batch)

        oe = OrdinalEncoder(columns=["A", "B"], encode_lists=False)
        oe.fit(ray.data.from_pandas(batch))
        transformed_batch = oe.transform_batch(batch)
        expected_batch = pd.DataFrame(
            {
                "A": [3, 0, 2, 1],
                "B": [2, 0, 1, 0],
            }
        )
        assert transformed_batch.equals(expected_batch)


    Args:
        columns: The columns that will individually be encoded.
        encode_lists: If True, each element of lists inside list
            columns will be encoded. If False, each list will
            be treated as a whole separate category. True
            by default.
    """

    def __init__(self, columns: List[str], *, encode_lists: bool = True):
        # TODO: allow user to specify order of values within each column.
        self.columns = columns
        self.encode_lists = encode_lists

    def _fit(self, dataset: Dataset) -> Preprocessor:
        self.stats_ = _get_unique_value_indices(
            dataset, self.columns, encode_lists=self.encode_lists
        )
        return self

    def _transform_pandas(self, df: pd.DataFrame):
        _validate_df(df, *self.columns)

        def encode_list(element: list, *, name: str):
            return [self.stats_[f"unique_values({name})"].get(x) for x in element]

        def column_ordinal_encoder(s: pd.Series):
            if _is_series_composed_of_lists(s):
                if self.encode_lists:
                    return s.map(partial(encode_list, name=s.name))

                # cannot simply use map here due to pandas thinking
                # tuples are to be used for indices
                def list_as_category(element):
                    element = tuple(element)
                    return self.stats_[f"unique_values({s.name})"].get(element)

                return s.apply(list_as_category)

            s_values = self.stats_[f"unique_values({s.name})"]
            return s.map(s_values)

        df[self.columns] = df[self.columns].apply(column_ordinal_encoder)
        return df

    def __repr__(self):
        stats = getattr(self, "stats_", None)
        return (
            f"OrdinalEncoder(columns={self.columns}, stats={stats}, "
            f"encode_lists={self.encode_lists})"
        )


class OneHotEncoder(Preprocessor):
    """Encode columns as new columns using one-hot encoding.

    The transformed dataset will have a new column in the form ``{column}_{value}``
    for each of the values from the fitted dataset. The value of a column will
    be set to 1 if the value matches, otherwise 0.

    Transforming values not included in the fitted dataset or not among
    the top popular values (see ``limit``) will result in all of the encoded column
    values being 0.

    All column values must be hashable or lists. Lists will be treated as separate
    categories. If you would like to encode list elements,
    use :class:`MultiHotEncoder`.

    Example:

    .. code-block:: python

        ohe = OneHotEncoder(
            columns=[
                "trip_start_hour",
                "trip_start_day",
                "trip_start_month",
                "dropoff_census_tract",
                "pickup_community_area",
                "dropoff_community_area",
                "payment_type",
                "company",
            ],
            limit={
                "dropoff_census_tract": 25,
                "pickup_community_area": 20,
                "dropoff_community_area": 20,
                "payment_type": 2,
                "company": 7,
            },
        )

    Args:
        columns: The columns that will individually be encoded.
        limit: If set, only the top "limit" number of most popular values become
            categorical variables. The less frequent ones will result in all
            the encoded column values being 0. This is a dict of column to
            its corresponding limit. The column in this dictionary has to be
            in ``columns``.
    """

    def __init__(self, columns: List[str], *, limit: Optional[Dict[str, int]] = None):
        # TODO: add `drop` parameter.
        self.columns = columns
        self.limit = limit

    def _fit(self, dataset: Dataset) -> Preprocessor:
        self.stats_ = _get_unique_value_indices(
            dataset, self.columns, limit=self.limit, encode_lists=False
        )
        return self

    def _transform_pandas(self, df: pd.DataFrame):
        _validate_df(df, *self.columns)

        columns_to_drop = set(self.columns)

        # Compute new one-hot encoded columns
        for column in self.columns:
            column_values = self.stats_[f"unique_values({column})"]
            if _is_series_composed_of_lists(df[column]):
                df[column] = df[column].map(lambda x: tuple(x))
            for column_value in column_values:
                df[f"{column}_{column_value}"] = (df[column] == column_value).astype(
                    int
                )
        # Drop original unencoded columns.
        df = df.drop(columns=list(columns_to_drop))
        return df

    def __repr__(self):
        stats = getattr(self, "stats_", None)
        return f"OneHotEncoder(columns={self.columns}, stats={stats})"


class MultiHotEncoder(Preprocessor):
    """Encode columns using multi-hot encoding.

    A column of lists or scalars (treated as one element lists) will be
    encoded as a column of one-hot encoded lists. This is useful for eg.
    generating embeddings for recommender systems.

    Example:

    .. code-block:: python

        import ray.data
        from ray.data.preprocessors import MultiHotEncoder
        import pandas as pd
        mhe = MultiHotEncoder(columns=["A", "B"])
        batch = pd.DataFrame(
            {
                "A": [["warm"], [], ["hot", "warm", "cold"], ["cold", "cold"]],
                "B": ["warm", "cold", "hot", "cold"],
            },
        )
        mhe.fit(ray.data.from_pandas(batch))
        transformed_batch = mhe.transform_batch(batch)
        expected_batch = pd.DataFrame(
            {
                "A": [[0, 0, 1], [0, 0, 0], [1, 1, 1], [2, 0, 0]],
                "B": [[0, 0, 1], [1, 0, 0], [0, 1, 0], [1, 0, 0]],
            }
        )
        assert transformed_batch.equals(expected_batch)

    Transforming values not included in the fitted dataset or not among
    the top popular values (see ``limit``) will result in all of the encoded column
    values being 0.

    The logic is similar to scikit-learn's `MultiLabelBinarizer \
<https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing\
.MultiLabelBinarizer.html>`_.

    All column values must be hashable scalars or lists of hashable values. Those
    two types cannot be mixed.

    See also: :class:`OneHotEncoder`.

    Args:
        columns: The columns that will individually be encoded.
        limit: If set, only the top "limit" number of most popular values become
            categorical variables. The less frequent ones will result in all
            the encoded values being 0. This is a dict of column to
            its corresponding limit. The column in this dictionary has to be
            in ``columns``.
    """

    def __init__(self, columns: List[str], *, limit: Optional[Dict[str, int]] = None):
        # TODO: add `drop` parameter.
        self.columns = columns
        self.limit = limit

    def _fit(self, dataset: Dataset) -> Preprocessor:
        self.stats_ = _get_unique_value_indices(
            dataset, self.columns, limit=self.limit, encode_lists=True
        )
        return self

    def _transform_pandas(self, df: pd.DataFrame):
        _validate_df(df, *self.columns)

        def encode_list(element: list, *, name: str):
            if not isinstance(element, list):
                element = [element]
            stats = self.stats_[f"unique_values({name})"]
            counter = Counter(element)
            return [counter.get(x, 0) for x in stats]

        for column in self.columns:
            df[column] = df[column].map(partial(encode_list, name=column))

        return df

    def __repr__(self):
        stats = getattr(self, "stats_", None)
        return f"MultiHotEncoder(columns={self.columns}, stats={stats})"


class LabelEncoder(Preprocessor):
    """Encode values within a label column as ordered integer values.

    Currently, order within a column is based on the values from the fitted
    dataset in sorted order.

    Transforming values not included in the fitted dataset will be encoded as ``None``.

    All column values must be hashable.

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
    limit: Optional[Dict[str, int]] = None,
    encode_lists: bool = True,
) -> Dict[str, Dict[str, int]]:
    """If drop_na_values is True, will silently drop NA values."""
    limit = limit or {}
    for column in limit:
        if column not in columns:
            raise ValueError(
                f"You set limit for {column}, which is not present in {columns}."
            )

    def get_pd_value_counts_per_column(col: pd.Series):
        # special handling for lists
        if _is_series_composed_of_lists(col):
            if encode_lists:
                counter = Counter()

                def update_counter(element):
                    counter.update(element)
                    return element

                col.map(update_counter)
                return counter
            else:
                # convert to tuples to make lists hashable
                col = col.map(lambda x: tuple(x))
        return Counter(col.value_counts(dropna=False).to_dict())

    def get_pd_value_counts(df: pd.DataFrame) -> List[Dict[str, Counter]]:
        result = [{col: get_pd_value_counts_per_column(df[col]) for col in columns}]
        return result

    value_counts = dataset.map_batches(get_pd_value_counts, batch_format="pandas")
    final_counters = {col: Counter() for col in columns}
    for batch in value_counts.iter_batches():
        for col_value_counts in batch:
            for col, value_counts in col_value_counts.items():
                final_counters[col] += value_counts

    # Inspect if there is any NA values.
    for col in columns:
        if drop_na_values:
            counter = final_counters[col]
            counter_dict = dict(counter)
            sanitized_dict = {k: v for k, v in counter_dict.items() if not pd.isnull(k)}
            final_counters[col] = Counter(sanitized_dict)
        else:
            if any(pd.isnull(k) for k in final_counters[col]):
                raise ValueError(
                    f"Unable to fit column '{col}' because it contains null"
                    f" values. Consider imputing missing values first."
                )

    unique_values_with_indices = OrderedDict()
    for column in columns:
        if column in limit:
            # Output sorted by freq.
            unique_values_with_indices[key_format.format(column)] = {
                k[0]: j
                for j, k in enumerate(final_counters[column].most_common(limit[column]))
            }
        else:
            # Output sorted by column name.
            unique_values_with_indices[key_format.format(column)] = {
                k: j for j, k in enumerate(sorted(dict(final_counters[column]).keys()))
            }
    return unique_values_with_indices


def _validate_df(df: pd.DataFrame, *columns: str) -> None:
    null_columns = [column for column in columns if df[column].isnull().values.any()]
    if null_columns:
        raise ValueError(
            f"Unable to transform columns {null_columns} because they contain "
            f"null values. Consider imputing missing values first."
        )


def _is_series_composed_of_lists(series: pd.Series) -> bool:
    # we assume that all elements are a list here
    first_not_none_element = next(
        (element for element in series if element is not None), None
    )
    return pandas.api.types.is_object_dtype(series.dtype) and isinstance(
        first_not_none_element, list
    )
