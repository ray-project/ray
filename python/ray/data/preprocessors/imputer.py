import logging
from collections import Counter
from numbers import Number
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Union

import numpy as np
import pandas as pd
from pandas.api.types import is_categorical_dtype

from ray.data.aggregate import Mean
from ray.data.preprocessor import SerializablePreprocessorBase
from ray.data.preprocessors.version_support import (
    SerializablePreprocessor as Serializable,
)
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.data.dataset import Dataset


logger = logging.getLogger(__name__)


@PublicAPI(stability="alpha")
@Serializable(version=1, identifier="io.ray.preprocessors.simple_imputer")
class SimpleImputer(SerializablePreprocessorBase):
    """Replace missing values with imputed values. If the column is missing from a
    batch, it will be filled with the imputed value.

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

        :class:`SimpleImputer` can also be used in append mode by providing the
        name of the output_columns that should hold the imputed values.

        >>> preprocessor = SimpleImputer(columns=["X"], output_columns=["X_imputed"], strategy="mean")
        >>> preprocessor.fit_transform(ds).to_pandas()  # doctest: +SKIP
             X     Y  X_imputed
        0  0.0  None        0.0
        1  NaN     b        2.0
        2  3.0     c        3.0
        3  3.0     c        3.0

    Args:
        columns: The columns to apply imputation to.
        strategy: How imputed values are chosen.

            * ``"mean"``: The mean of non-missing values. This strategy only works with numeric columns.
            * ``"most_frequent"``: The most common value.
            * ``"constant"``: The value passed to ``fill_value``.

        fill_value: The value to use when ``strategy`` is ``"constant"``.
        output_columns: The names of the transformed columns. If None, the transformed
            columns will be the same as the input columns. If not None, the length of
            ``output_columns`` must match the length of ``columns``, othwerwise an error
            will be raised.

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
        *,
        output_columns: Optional[List[str]] = None,
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

        self.output_columns = (
            SerializablePreprocessorBase._derive_and_validate_output_columns(
                columns, output_columns
            )
        )

    def _fit(self, dataset: "Dataset") -> SerializablePreprocessorBase:
        if self.strategy == "mean":
            self.stat_computation_plan.add_aggregator(
                aggregator_fn=Mean, columns=self.columns
            )
        elif self.strategy == "most_frequent":
            self.stat_computation_plan.add_callable_stat(
                stat_fn=lambda key_gen: _get_most_frequent_values(
                    dataset=dataset,
                    columns=self.columns,
                    key_gen=key_gen,
                ),
                stat_key_fn=lambda col: f"most_frequent({col})",
                columns=self.columns,
            )

        return self

    def _transform_pandas(self, df: pd.DataFrame):
        for column, output_column in zip(self.columns, self.output_columns):
            value = self._get_fill_value(column)

            if value is None:
                raise ValueError(
                    f"Column {column} has no fill value. "
                    "Check the data used to fit the SimpleImputer."
                )

            if column not in df.columns:
                # Create the column with the fill_value if it doesn't exist
                df[output_column] = value
            else:
                if is_categorical_dtype(df.dtypes[column]):
                    df[output_column] = df[column].cat.add_categories([value])

                if (
                    output_column != column
                    # If the backing array is memory-mapped from shared memory, then the
                    # array won't be writeable.
                    or (
                        isinstance(df[output_column].values, np.ndarray)
                        and not df[output_column].values.flags.writeable
                    )
                ):
                    df[output_column] = df[column].copy(deep=True)

                df.fillna({output_column: value}, inplace=True)

        return df

    def _get_fill_value(self, column):
        if self.strategy == "mean":
            return self.stats_[f"mean({column})"]
        elif self.strategy == "most_frequent":
            return self.stats_[f"most_frequent({column})"]
        elif self.strategy == "constant":
            return self.fill_value
        else:
            raise ValueError(
                f"Strategy {self.strategy} is not supported. "
                "Supported values are: {self._valid_strategies}"
            )

    def __repr__(self):
        return (
            f"{self.__class__.__name__}(columns={self.columns!r}, "
            f"strategy={self.strategy!r}, fill_value={self.fill_value!r}, "
            f"output_columns={self.output_columns!r})"
        )

    def _get_serializable_fields(self) -> Dict[str, Any]:
        return {
            "columns": self.columns,
            "output_columns": self.output_columns,
            "_fitted": getattr(self, "_fitted", None),
            "strategy": self.strategy,
            "fill_value": getattr(self, "fill_value", None),
        }

    def _set_serializable_fields(self, fields: Dict[str, Any], version: int):
        # required fields
        self.columns = fields["columns"]
        self.output_columns = fields["output_columns"]
        self.strategy = fields["strategy"]
        # optional fields
        self._fitted = fields.get("_fitted")
        self.fill_value = fields.get("fill_value")

        if self.strategy == "constant":
            self._is_fittable = False


def _get_most_frequent_values(
    dataset: "Dataset",
    columns: List[str],
    key_gen: Callable[[str], str],
) -> Dict[str, Union[str, Number]]:
    def get_pd_value_counts(df: pd.DataFrame) -> Dict[str, List[Counter]]:
        return {col: [Counter(df[col].value_counts().to_dict())] for col in columns}

    value_counts = dataset.map_batches(get_pd_value_counts, batch_format="pandas")
    final_counters = {col: Counter() for col in columns}
    for batch in value_counts.iter_batches(batch_size=None):
        for col, counters in batch.items():
            for counter in counters:
                final_counters[col] += counter

    return {
        key_gen(column): final_counters[column].most_common(1)[0][0]  # noqa
        for column in columns
    }
