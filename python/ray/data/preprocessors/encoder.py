from functools import partial
from typing import List, Dict, Optional

from collections import Counter, OrderedDict
import numpy as np
import pandas as pd
import pandas.api.types

from ray.data import Dataset
from ray.data.preprocessor import Preprocessor
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class OrdinalEncoder(Preprocessor):
    """Encode values within columns as ordered integer values.

    :class:`OrdinalEncoder` encodes categorical features as integers that range from
    :math:`0` to :math:`n - 1`, where :math:`n` is the number of categories.

    If you transform a value that isn't in the fitted datset, then the value is encoded
    as ``float("nan")``.

    Columns must contain either hashable values or lists of hashable values. Also, you
    can't have both scalars and lists in the same column.

    Examples:
        Use :class:`OrdinalEncoder` to encode categorical features as integers.

        >>> import pandas as pd
        >>> import ray
        >>> from ray.data.preprocessors import OrdinalEncoder
        >>> df = pd.DataFrame({
        ...     "sex": ["male", "female", "male", "female"],
        ...     "level": ["L4", "L5", "L3", "L4"],
        ... })
        >>> ds = ray.data.from_pandas(df)  # doctest: +SKIP
        >>> encoder = OrdinalEncoder(columns=["sex", "level"])
        >>> encoder.fit_transform(ds).to_pandas()  # doctest: +SKIP
           sex  level
        0    1      1
        1    0      2
        2    1      0
        3    0      1

        If you transform a value not present in the original dataset, then the value
        is encoded as ``float("nan")``.

        >>> df = pd.DataFrame({"sex": ["female"], "level": ["L6"]})
        >>> ds = ray.data.from_pandas(df)  # doctest: +SKIP
        >>> encoder.transform(ds).to_pandas()  # doctest: +SKIP
           sex  level
        0    0    NaN

        :class:`OrdinalEncoder` can also encode categories in a list.

        >>> df = pd.DataFrame({
        ...     "name": ["Shaolin Soccer", "Moana", "The Smartest Guys in the Room"],
        ...     "genre": [
        ...         ["comedy", "action", "sports"],
        ...         ["animation", "comedy",  "action"],
        ...         ["documentary"],
        ...     ],
        ... })
        >>> ds = ray.data.from_pandas(df)  # doctest: +SKIP
        >>> encoder = OrdinalEncoder(columns=["genre"])
        >>> encoder.fit_transform(ds).to_pandas()  # doctest: +SKIP
                                    name      genre
        0                 Shaolin Soccer  [2, 0, 4]
        1                          Moana  [1, 2, 0]
        2  The Smartest Guys in the Room        [3]

    Args:
        columns: The columns to separately encode.
        encode_lists: If ``True``, encode list elements.  If ``False``, encode
            whole lists (i.e., replace each list with an integer). ``True``
            by default.

    .. seealso::

        :class:`OneHotEncoder`
            Another preprocessor that encodes categorical data.
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
        return (
            f"{self.__class__.__name__}(columns={self.columns!r}, "
            f"encode_lists={self.encode_lists!r})"
        )


@PublicAPI(stability="alpha")
class OneHotEncoder(Preprocessor):
    """`One-hot encode <https://en.wikipedia.org/wiki/One-hot#Machine_learning_and_statistics>`_
    categorical data.

    This preprocessor creates a column named ``{column}_{category}``
    for each unique ``{category}`` in ``{column}``. The value of a column is
    1 if the category matches and 0 otherwise.

    If you encode an infrequent category (see ``max_categories``) or a category
    that isn't in the fitted dataset, then the category is encoded as all 0s.

    Columns must contain hashable objects or lists of hashable objects.

    .. note::
        Lists are treated as categories. If you want to encode individual list
        elements, use :class:`MultiHotEncoder`.

    Example:
        >>> import pandas as pd
        >>> import ray
        >>> from ray.data.preprocessors import OneHotEncoder
        >>>
        >>> df = pd.DataFrame({"color": ["red", "green", "red", "red", "blue", "green"]})
        >>> ds = ray.data.from_pandas(df)  # doctest: +SKIP
        >>> encoder = OneHotEncoder(columns=["color"])
        >>> encoder.fit_transform(ds).to_pandas()  # doctest: +SKIP
           color_blue  color_green  color_red
        0           0            0          1
        1           0            1          0
        2           0            0          1
        3           0            0          1
        4           1            0          0
        5           0            1          0

        If you one-hot encode a value that isn't in the fitted dataset, then the
        value is encoded with zeros.

        >>> df = pd.DataFrame({"color": ["yellow"]})
        >>> batch = ray.data.from_pandas(df)  # doctest: +SKIP
        >>> encoder.transform(batch).to_pandas()  # doctest: +SKIP
           color_blue  color_green  color_red
        0           0            0          0

        Likewise, if you one-hot encode an infrequent value, then the value is encoded
        with zeros.

        >>> encoder = OneHotEncoder(columns=["color"], max_categories={"color": 2})
        >>> encoder.fit_transform(ds).to_pandas()  # doctest: +SKIP
           color_red  color_green
        0          1            0
        1          0            1
        2          1            0
        3          1            0
        4          0            0
        5          0            1

    Args:
        columns: The columns to separately encode.
        max_categories: The maximum number of features to create for each column.
            If a value isn't specified for a column, then a feature is created
            for every category in that column.

    .. seealso::

        :class:`MultiHotEncoder`
            If you want to encode individual list elements, use
            :class:`MultiHotEncoder`.

        :class:`OrdinalEncoder`
            If your categories are ordered, you may want to use
            :class:`OrdinalEncoder`.
    """  # noqa: E501

    def __init__(
        self, columns: List[str], *, max_categories: Optional[Dict[str, int]] = None
    ):
        # TODO: add `drop` parameter.
        self.columns = columns
        self.max_categories = max_categories

    def _fit(self, dataset: Dataset) -> Preprocessor:
        self.stats_ = _get_unique_value_indices(
            dataset,
            self.columns,
            max_categories=self.max_categories,
            encode_lists=False,
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
        return (
            f"{self.__class__.__name__}(columns={self.columns!r}, "
            f"max_categories={self.max_categories!r})"
        )


@PublicAPI(stability="alpha")
class MultiHotEncoder(Preprocessor):
    """Multi-hot encode categorical data.

    This preprocessor replaces each list of categories with an :math:`m`-length binary
    list, where :math:`m` is the number of unique categories in the column or the value
    specified in ``max_categories``. The :math:`i\\text{-th}` element of the binary list
    is :math:`1` if category :math:`i` is in the input list and :math:`0` otherwise.

    Columns must contain hashable objects or lists of hashable objects.
    Also, you can't have both types in the same column.

    .. note::
        The logic is similar to scikit-learn's `MultiLabelBinarizer \
    <https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing\
    .MultiLabelBinarizer.html>`_.

    Examples:
        >>> import pandas as pd
        >>> import ray
        >>> from ray.data.preprocessors import MultiHotEncoder
        >>>
        >>> df = pd.DataFrame({
        ...     "name": ["Shaolin Soccer", "Moana", "The Smartest Guys in the Room"],
        ...     "genre": [
        ...         ["comedy", "action", "sports"],
        ...         ["animation", "comedy",  "action"],
        ...         ["documentary"],
        ...     ],
        ... })
        >>> ds = ray.data.from_pandas(df)  # doctest: +SKIP
        >>>
        >>> encoder = MultiHotEncoder(columns=["genre"])
        >>> encoder.fit_transform(ds).to_pandas()  # doctest: +SKIP
                                    name            genre
        0                 Shaolin Soccer  [1, 0, 1, 0, 1]
        1                          Moana  [1, 1, 1, 0, 0]
        2  The Smartest Guys in the Room  [0, 0, 0, 1, 0]

        If you specify ``max_categories``, then :class:`MultiHotEncoder`
        creates features for only the most frequent categories.

        >>> encoder = MultiHotEncoder(columns=["genre"], max_categories={"genre": 3})
        >>> encoder.fit_transform(ds).to_pandas()  # doctest: +SKIP
                                    name      genre
        0                 Shaolin Soccer  [1, 1, 1]
        1                          Moana  [1, 1, 0]
        2  The Smartest Guys in the Room  [0, 0, 0]
        >>> encoder.stats_  # doctest: +SKIP
        OrderedDict([('unique_values(genre)', {'comedy': 0, 'action': 1, 'sports': 2})])

    Args:
        columns: The columns to separately encode.
        max_categories: The maximum number of features to create for each column.
            If a value isn't specified for a column, then a feature is created
            for every unique category in that column.

    .. seealso::

        :class:`OneHotEncoder`
            If you're encoding individual categories instead of lists of
            categories, use :class:`OneHotEncoder`.

        :class:`OrdinalEncoder`
            If your categories are ordered, you may want to use
            :class:`OrdinalEncoder`.
    """

    def __init__(
        self, columns: List[str], *, max_categories: Optional[Dict[str, int]] = None
    ):
        # TODO: add `drop` parameter.
        self.columns = columns
        self.max_categories = max_categories

    def _fit(self, dataset: Dataset) -> Preprocessor:
        self.stats_ = _get_unique_value_indices(
            dataset, self.columns, max_categories=self.max_categories, encode_lists=True
        )
        return self

    def _transform_pandas(self, df: pd.DataFrame):
        _validate_df(df, *self.columns)

        def encode_list(element: list, *, name: str):
            if isinstance(element, np.ndarray):
                element = element.tolist()
            elif not isinstance(element, list):
                element = [element]
            stats = self.stats_[f"unique_values({name})"]
            counter = Counter(element)
            return [counter.get(x, 0) for x in stats]

        for column in self.columns:
            df[column] = df[column].map(partial(encode_list, name=column))

        return df

    def __repr__(self):
        return (
            f"{self.__class__.__name__}(columns={self.columns!r}, "
            f"max_categories={self.max_categories!r})"
        )


@PublicAPI(stability="alpha")
class LabelEncoder(Preprocessor):
    """Encode labels as integer targets.

    :class:`LabelEncoder` encodes labels as integer targets that range from
    :math:`0` to :math:`n - 1`, where :math:`n` is the number of unique labels.

    If you transform a label that isn't in the fitted datset, then the label is encoded
    as ``float("nan")``.

    Examples:
        >>> import pandas as pd
        >>> import ray
        >>> df = pd.DataFrame({
        ...     "sepal_width": [5.1, 7, 4.9, 6.2],
        ...     "sepal_height": [3.5, 3.2, 3, 3.4],
        ...     "species": ["setosa", "versicolor", "setosa", "virginica"]
        ... })
        >>> ds = ray.data.from_pandas(df)  # doctest: +SKIP
        >>>
        >>> from ray.data.preprocessors import LabelEncoder
        >>> encoder = LabelEncoder(label_column="species")
        >>> encoder.fit_transform(ds).to_pandas()  # doctest: +SKIP
           sepal_width  sepal_height  species
        0          5.1           3.5        0
        1          7.0           3.2        1
        2          4.9           3.0        0
        3          6.2           3.4        2

        If you transform a label not present in the original dataset, then the new
        label is encoded as ``float("nan")``.

        >>> df = pd.DataFrame({
        ...     "sepal_width": [4.2],
        ...     "sepal_height": [2.7],
        ...     "species": ["bracteata"]
        ... })
        >>> ds = ray.data.from_pandas(df)  # doctest: +SKIP
        >>> encoder.transform(ds).to_pandas()  # doctest: +SKIP
           sepal_width  sepal_height  species
        0          4.2           2.7      NaN

    Args:
        label_column: A column containing labels that you want to encode.

    .. seealso::

        :class:`OrdinalEncoder`
            If you're encoding ordered features, use :class:`OrdinalEncoder` instead of
            :class:`LabelEncoder`.
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
        return f"{self.__class__.__name__}(label_column={self.label_column!r})"


@PublicAPI(stability="alpha")
class Categorizer(Preprocessor):
    """Convert columns to ``pd.CategoricalDtype``.

    Use this preprocessor with frameworks that have built-in support for
    ``pd.CategoricalDtype`` like LightGBM.

    .. warning::

        If you don't specify ``dtypes``, fit this preprocessor before splitting
        your dataset into train and test splits. This ensures categories are
        consistent across splits.

    Examples:
        >>> import pandas as pd
        >>> import ray
        >>> from ray.data.preprocessors import Categorizer
        >>>
        >>> df = pd.DataFrame(
        ... {
        ...     "sex": ["male", "female", "male", "female"],
        ...     "level": ["L4", "L5", "L3", "L4"],
        ... })
        >>> ds = ray.data.from_pandas(df)  # doctest: +SKIP
        >>> categorizer = Categorizer(columns=["sex", "level"])
        >>> categorizer.fit_transform(ds).schema().types  # doctest: +SKIP
        [CategoricalDtype(categories=['female', 'male'], ordered=False), CategoricalDtype(categories=['L3', 'L4', 'L5'], ordered=False)]

        If you know the categories in advance, you can specify the categories with the
        ``dtypes`` parameter.

        >>> categorizer = Categorizer(
        ...     columns=["sex", "level"],
        ...     dtypes={"level": pd.CategoricalDtype(["L3", "L4", "L5", "L6"], ordered=True)},
        ... )
        >>> categorizer.fit_transform(ds).schema().types  # doctest: +SKIP
        [CategoricalDtype(categories=['female', 'male'], ordered=False), CategoricalDtype(categories=['L3', 'L4', 'L5', 'L6'], ordered=True)]

    Args:
        columns: The columns to convert to ``pd.CategoricalDtype``.
        dtypes: An optional dictionary that maps columns to ``pd.CategoricalDtype``
            objects. If you don't include a column in ``dtypes``, the categories
            are inferred.
    """  # noqa: E501

    def __init__(
        self,
        columns: List[str],
        dtypes: Optional[Dict[str, pd.CategoricalDtype]] = None,
    ):
        if not dtypes:
            dtypes = {}

        self.columns = columns
        self.dtypes = dtypes

    def _fit(self, dataset: Dataset) -> Preprocessor:
        columns_to_get = [
            column for column in self.columns if column not in self.dtypes
        ]
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
        unique_indices = {**self.dtypes, **unique_indices}
        self.stats_: Dict[str, pd.CategoricalDtype] = unique_indices
        return self

    def _transform_pandas(self, df: pd.DataFrame):
        df = df.astype(self.stats_)
        return df

    def __repr__(self):
        return (
            f"{self.__class__.__name__}(columns={self.columns!r}, "
            f"dtypes={self.dtypes!r})"
        )


def _get_unique_value_indices(
    dataset: Dataset,
    columns: List[str],
    drop_na_values: bool = False,
    key_format: str = "unique_values({0})",
    max_categories: Optional[Dict[str, int]] = None,
    encode_lists: bool = True,
) -> Dict[str, Dict[str, int]]:
    """If drop_na_values is True, will silently drop NA values."""

    if max_categories is None:
        max_categories = {}
    for column in max_categories:
        if column not in columns:
            raise ValueError(
                f"You set `max_categories` for {column}, which is not present in "
                f"{columns}."
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
        df_columns = df.columns.tolist()
        result = {}
        for col in columns:
            if col in df_columns:
                result[col] = get_pd_value_counts_per_column(df[col])
            else:
                raise ValueError(
                    f"Column '{col}' does not exist in DataFrame, which has columns: {df_columns}"  # noqa: E501
                )
        return [result]

    value_counts = dataset.map_batches(get_pd_value_counts, batch_format="pandas")
    final_counters = {col: Counter() for col in columns}
    for batch in value_counts.iter_batches(batch_size=None):
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
        if column in max_categories:
            # Output sorted by freq.
            unique_values_with_indices[key_format.format(column)] = {
                k[0]: j
                for j, k in enumerate(
                    final_counters[column].most_common(max_categories[column])
                )
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
        first_not_none_element, (list, np.ndarray)
    )
