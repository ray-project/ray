from typing import List, Optional
import numpy as np
import pandas as pd

from ray.data.extensions import TensorArray
from ray.data.preprocessor import Preprocessor


class Concatenator(Preprocessor):
    """Creates a tensor column via concatenation.

    A tensor column is a column consisting of ndarrays as elements.
    The tensor column will be generated from the provided list
    of columns and will take on the provided "output" label.
    Columns that are included in the concatenation
    will be dropped, while columns that are not included in concatenation
    will be preserved.

    Example:
        >>> import ray
        >>> import pandas as pd
        >>> from ray.data.preprocessors import Concatenator
        >>> df = pd.DataFrame({"a": [1, 2, 3, 4], "b": [1, 2, 3, 4],})
        >>> ds = ray.data.from_pandas(df)  # doctest: +SKIP
        >>> prep = Concatenator(output_column_name="c") # doctest: +SKIP
        >>> new_ds = prep.transform(ds) # doctest: +SKIP
        >>> assert set(new_ds.take(1)[0]) == {"c"} # doctest: +SKIP

    Args:
        output_column_name: output_column_name is a string that represents the
            name of the outputted, concatenated tensor column. Defaults to
            "concat_out".
        include: A list of column names to be included for
            concatenation. If None, then all columns will be included.
            Included columns will be dropped after concatenation.
        exclude: List of column names to be excluded
            from concatenation. Exclude takes precedence over include.
        dtype: Optional. The dtype to convert the output column array to.
        raise_if_missing: Optional. If True, an error will be raised if any
            of the columns to in 'include' or 'exclude' are
            not present in the dataset schema.

    Raises:
        ValueError if `raise_if_missing=True` and any column name in
            `include` or `exclude` does not exist in the dataset columns.
    """

    _is_fittable = False

    def __init__(
        self,
        output_column_name: str = "concat_out",
        include: Optional[List[str]] = None,
        exclude: Optional[List[str]] = None,
        dtype: Optional[np.dtype] = None,
        raise_if_missing: bool = False,
    ):
        self.output_column_name = output_column_name
        self.included_columns = include
        self.excluded_columns = exclude or []
        self.dtype = dtype
        self.raise_if_missing = raise_if_missing

    def _validate(self, df: pd.DataFrame):
        total_columns = set(df)
        if self.excluded_columns and self.raise_if_missing:
            missing_columns = set(self.excluded_columns) - total_columns.intersection(
                set(self.excluded_columns)
            )
            if missing_columns:
                raise ValueError(
                    f"Missing columns specified in 'exclude': {missing_columns}"
                )
        if self.included_columns and self.raise_if_missing:
            missing_columns = set(self.included_columns) - total_columns.intersection(
                set(self.included_columns)
            )
            if missing_columns:
                raise ValueError(
                    f"Missing columns specified in 'include': {missing_columns}"
                )

    def _transform_pandas(self, df: pd.DataFrame):
        self._validate(df)

        included_columns = set(df)
        if self.included_columns:  # subset of included columns
            included_columns = set(self.included_columns)

        columns_to_concat = list(included_columns - set(self.excluded_columns))
        concatenated = df[columns_to_concat].to_numpy(dtype=self.dtype)
        df = df.drop(columns=columns_to_concat)
        try:
            concatenated = TensorArray(concatenated)
        except TypeError:
            pass
        df[self.output_column_name] = concatenated
        return df

    def __repr__(self):
        return (
            f"Concatenator(output_column_name={self.output_column_name}, "
            f"include={self.included_columns}, "
            f"exclude={self.excluded_columns}, "
            f"dtype={self.dtype})"
        )
