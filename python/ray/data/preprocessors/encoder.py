from collections import Counter, OrderedDict
from functools import partial
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import pandas.api.types

from ray.air.util.data_batch_conversion import BatchFormat
from ray.data import Dataset
from ray.data.preprocessor import Preprocessor, PreprocessorNotFittedException
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class OrdinalEncoder(Preprocessor):
    r"""Encode values within columns as ordered integer values.

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

        :class:`OrdinalEncoder` can also be used in append mode by providing the
        name of the output_columns that should hold the encoded values.

        >>> encoder = OrdinalEncoder(columns=["sex", "level"], output_columns=["sex_encoded", "level_encoded"])
        >>> encoder.fit_transform(ds).to_pandas()  # doctest: +SKIP
              sex level  sex_encoded  level_encoded
        0    male    L4            1              1
        1  female    L5            0              2
        2    male    L3            1              0
        3  female    L4            0              1


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
        output_columns: The names of the transformed columns. If None, the transformed
            columns will be the same as the input columns. If not None, the length of
            ``output_columns`` must match the length of ``columns``, othwerwise an error
            will be raised.

    .. seealso::

        :class:`OneHotEncoder`
            Another preprocessor that encodes categorical data.
    """

    def __init__(
        self,
        columns: List[str],
        *,
        encode_lists: bool = True,
        output_columns: Optional[List[str]] = None,
    ):
        # TODO: allow user to specify order of values within each column.
        self.columns = columns
        self.encode_lists = encode_lists
        self.output_columns = Preprocessor._derive_and_validate_output_columns(
            columns, output_columns
        )

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

        df[self.output_columns] = df[self.columns].apply(column_ordinal_encoder)
        return df

    def __repr__(self):
        return (
            f"{self.__class__.__name__}(columns={self.columns!r}, "
            f"encode_lists={self.encode_lists!r}, "
            f"output_columns={self.output_columns!r})"
        )


@PublicAPI(stability="alpha")
class OneHotEncoder(Preprocessor):
    r"""`One-hot encode <https://en.wikipedia.org/wiki/One-hot#Machine_learning_and_statistics>`_
    categorical data.

    This preprocessor transforms each specified column into a one-hot encoded vector.
    Each element in the vector corresponds to a unique category in the column, with a
    value of 1 if the category matches and 0 otherwise.

    If a category is infrequent (based on ``max_categories``) or not present in the
    fitted dataset, it is encoded as all 0s.

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
               color
        0  [0, 0, 1]
        1  [0, 1, 0]
        2  [0, 0, 1]
        3  [0, 0, 1]
        4  [1, 0, 0]
        5  [0, 1, 0]

        OneHotEncoder can also be used in append mode by providing the
        name of the output_columns that should hold the encoded values.

        >>> encoder = OneHotEncoder(columns=["color"], output_columns=["color_encoded"])
        >>> encoder.fit_transform(ds).to_pandas()  # doctest: +SKIP
           color color_encoded
        0    red     [0, 0, 1]
        1  green     [0, 1, 0]
        2    red     [0, 0, 1]
        3    red     [0, 0, 1]
        4   blue     [1, 0, 0]
        5  green     [0, 1, 0]

        If you one-hot encode a value that isn't in the fitted dataset, then the
        value is encoded with zeros.

        >>> df = pd.DataFrame({"color": ["yellow"]})
        >>> batch = ray.data.from_pandas(df)  # doctest: +SKIP
        >>> encoder.transform(batch).to_pandas()  # doctest: +SKIP
            color color_encoded
        0  yellow     [0, 0, 0]

        Likewise, if you one-hot encode an infrequent value, then the value is encoded
        with zeros.

        >>> encoder = OneHotEncoder(columns=["color"], max_categories={"color": 2})
        >>> encoder.fit_transform(ds).to_pandas()  # doctest: +SKIP
            color
        0  [1, 0]
        1  [0, 1]
        2  [1, 0]
        3  [1, 0]
        4  [0, 0]
        5  [0, 1]

    Args:
        columns: The columns to separately encode.
        max_categories: The maximum number of features to create for each column.
            If a value isn't specified for a column, then a feature is created
            for every category in that column.
        output_columns: The names of the transformed columns. If None, the transformed
            columns will be the same as the input columns. If not None, the length of
            ``output_columns`` must match the length of ``columns``, othwerwise an error
            will be raised.

    .. seealso::

        :class:`MultiHotEncoder`
            If you want to encode individual list elements, use
            :class:`MultiHotEncoder`.

        :class:`OrdinalEncoder`
            If your categories are ordered, you may want to use
            :class:`OrdinalEncoder`.
    """  # noqa: E501

    def __init__(
        self,
        columns: List[str],
        *,
        max_categories: Optional[Dict[str, int]] = None,
        output_columns: Optional[List[str]] = None,
    ):
        # TODO: add `drop` parameter.
        self.columns = columns
        self.max_categories = max_categories
        self.output_columns = Preprocessor._derive_and_validate_output_columns(
            columns, output_columns
        )

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
        from typing import Any

        def safe_get(v: Any, stats: Dict[str, int]):
            from collections.abc import Hashable

            if isinstance(v, Hashable):
                return stats.get(v, -1)
            else:
                return -1  # Unhashable type treated as a missing category

        # Compute new one-hot encoded columns
        for column, output_column in zip(self.columns, self.output_columns):
            if _is_series_composed_of_lists(df[column]):
                df[column] = df[column].map(tuple)

            stats = self.stats_[f"unique_values({column})"]
            num_categories = len(stats)
            one_hot = np.zeros((len(df), num_categories), dtype=int)
            codes = df[column].apply(lambda v: safe_get(v, stats)).to_numpy()
            valid_rows = codes != -1
            one_hot[np.nonzero(valid_rows)[0], codes[valid_rows].astype(int)] = 1
            df[output_column] = one_hot.tolist()

        return df

    def __repr__(self):
        return (
            f"{self.__class__.__name__}(columns={self.columns!r}, "
            f"max_categories={self.max_categories!r}, "
            f"output_columns={self.output_columns!r})"
        )


@PublicAPI(stability="alpha")
class MultiHotEncoder(Preprocessor):
    r"""Multi-hot encode categorical data.

    This preprocessor replaces each list of categories with an :math:`m`-length binary
    list, where :math:`m` is the number of unique categories in the column or the value
    specified in ``max_categories``. The :math:`i\\text{-th}` element of the binary list
    is :math:`1` if category :math:`i` is in the input list and :math:`0` otherwise.

    Columns must contain hashable objects or lists of hashable objects.
    Also, you can't have both types in the same column.

    .. note::
        The logic is similar to scikit-learn's [MultiLabelBinarizer][1]

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

        :class:`MultiHotEncoder` can also be used in append mode by providing the
        name of the output_columns that should hold the encoded values.

        >>> encoder = MultiHotEncoder(columns=["genre"], output_columns=["genre_encoded"])
        >>> encoder.fit_transform(ds).to_pandas()  # doctest: +SKIP
                                    name                        genre    genre_encoded
        0                 Shaolin Soccer     [comedy, action, sports]  [1, 0, 1, 0, 1]
        1                          Moana  [animation, comedy, action]  [1, 1, 1, 0, 0]
        2  The Smartest Guys in the Room                [documentary]  [0, 0, 0, 1, 0]

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
        output_columns: The names of the transformed columns. If None, the transformed
            columns will be the same as the input columns. If not None, the length of
            ``output_columns`` must match the length of ``columns``, othwerwise an error
            will be raised.

    .. seealso::

        :class:`OneHotEncoder`
            If you're encoding individual categories instead of lists of
            categories, use :class:`OneHotEncoder`.

        :class:`OrdinalEncoder`
            If your categories are ordered, you may want to use
            :class:`OrdinalEncoder`.

    [1]: https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.MultiLabelBinarizer.html
    """

    def __init__(
        self,
        columns: List[str],
        *,
        max_categories: Optional[Dict[str, int]] = None,
        output_columns: Optional[List[str]] = None,
    ):
        # TODO: add `drop` parameter.
        self.columns = columns
        self.max_categories = max_categories
        self.output_columns = Preprocessor._derive_and_validate_output_columns(
            columns, output_columns
        )

    def _fit(self, dataset: Dataset) -> Preprocessor:
        self.stats_ = _get_unique_value_indices(
            dataset,
            self.columns,
            max_categories=self.max_categories,
            encode_lists=True,
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

        for column, output_column in zip(self.columns, self.output_columns):
            df[output_column] = df[column].map(partial(encode_list, name=column))

        return df

    def __repr__(self):
        return (
            f"{self.__class__.__name__}(columns={self.columns!r}, "
            f"max_categories={self.max_categories!r}, "
            f"output_columns={self.output_columns})"
        )


@PublicAPI(stability="alpha")
class LabelEncoder(Preprocessor):
    r"""Encode labels as integer targets.

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

        You can also provide the name of the output column that should hold the encoded
        labels if you want to use :class:`LabelEncoder` in append mode.

        >>> encoder = LabelEncoder(label_column="species", output_column="species_encoded")
        >>> encoder.fit_transform(ds).to_pandas()  # doctest: +SKIP
           sepal_width  sepal_height     species  species_encoded
        0          5.1           3.5      setosa                0
        1          7.0           3.2  versicolor                1
        2          4.9           3.0      setosa                0
        3          6.2           3.4   virginica                2

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
        output_column: The name of the column that will contain the encoded
            labels. If None, the output column will have the same name as the
            input column.

    .. seealso::

        :class:`OrdinalEncoder`
            If you're encoding ordered features, use :class:`OrdinalEncoder` instead of
            :class:`LabelEncoder`.
    """

    def __init__(self, label_column: str, *, output_column: Optional[str] = None):
        self.label_column = label_column
        self.output_column = output_column or label_column

    def _fit(self, dataset: Dataset) -> Preprocessor:
        self.stats_ = _get_unique_value_indices(dataset, [self.label_column])
        return self

    def _transform_pandas(self, df: pd.DataFrame):
        _validate_df(df, self.label_column)

        def column_label_encoder(s: pd.Series):
            s_values = self.stats_[f"unique_values({s.name})"]
            return s.map(s_values)

        df[self.output_column] = df[self.label_column].transform(column_label_encoder)
        return df

    def inverse_transform(self, ds: "Dataset") -> "Dataset":
        """Inverse transform the given dataset.

        Args:
            ds: Input Dataset that has been fitted and/or transformed.

        Returns:
            ray.data.Dataset: The inverse transformed Dataset.

        Raises:
            PreprocessorNotFittedException: if ``fit`` is not called yet.
        """

        fit_status = self.fit_status()

        if fit_status in (
            Preprocessor.FitStatus.PARTIALLY_FITTED,
            Preprocessor.FitStatus.NOT_FITTED,
        ):
            raise PreprocessorNotFittedException(
                "`fit` must be called before `inverse_transform`, "
            )

        kwargs = self._get_transform_config()

        return ds.map_batches(
            self._inverse_transform_pandas, batch_format=BatchFormat.PANDAS, **kwargs
        )

    def _inverse_transform_pandas(self, df: pd.DataFrame):
        def column_label_decoder(s: pd.Series):
            inverse_values = {
                value: key
                for key, value in self.stats_[
                    f"unique_values({self.label_column})"
                ].items()
            }
            return s.map(inverse_values)

        df[self.label_column] = df[self.output_column].transform(column_label_decoder)
        return df

    def __repr__(self):
        return f"{self.__class__.__name__}(label_column={self.label_column!r}, output_column={self.output_column!r})"


@PublicAPI(stability="alpha")
class Categorizer(Preprocessor):
    r"""Convert columns to ``pd.CategoricalDtype``.

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

        :class:`Categorizer` can also be used in append mode by providing the
        name of the output_columns that should hold the categorized values.

        >>> categorizer = Categorizer(columns=["sex", "level"], output_columns=["sex_cat", "level_cat"])
        >>> categorizer.fit_transform(ds).to_pandas()  # doctest: +SKIP
              sex level sex_cat level_cat
        0    male    L4    male        L4
        1  female    L5  female        L5
        2    male    L3    male        L3
        3  female    L4  female        L4

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
        output_columns: The names of the transformed columns. If None, the transformed
            columns will be the same as the input columns. If not None, the length of
            ``output_columns`` must match the length of ``columns``, othwerwise an error
            will be raised.

    """  # noqa: E501

    def __init__(
        self,
        columns: List[str],
        dtypes: Optional[Dict[str, pd.CategoricalDtype]] = None,
        output_columns: Optional[List[str]] = None,
    ):
        if not dtypes:
            dtypes = {}

        self.columns = columns
        self.dtypes = dtypes
        self.output_columns = Preprocessor._derive_and_validate_output_columns(
            columns, output_columns
        )

    def _fit(self, dataset: Dataset) -> Preprocessor:
        columns_to_get = [
            column for column in self.columns if column not in set(self.dtypes)
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
        df[self.output_columns] = df[self.columns].astype(self.stats_)
        return df

    def __repr__(self):
        return (
            f"{self.__class__.__name__}(columns={self.columns!r}, "
            f"dtypes={self.dtypes!r}, output_columns={self.output_columns!r})"
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
    columns_set = set(columns)
    for column in max_categories:
        if column not in columns_set:
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
                result[col] = [get_pd_value_counts_per_column(df[col])]
            else:
                raise ValueError(
                    f"Column '{col}' does not exist in DataFrame, which has columns: {df_columns}"  # noqa: E501
                )
        return result

    value_counts = dataset.map_batches(get_pd_value_counts, batch_format="pandas")
    final_counters = {col: Counter() for col in columns}
    for batch in value_counts.iter_batches(batch_size=None):
        for col, counters in batch.items():
            for counter in counters:
                counter = {k: v for k, v in counter.items() if v is not None}
                final_counters[col] += Counter(counter)

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
