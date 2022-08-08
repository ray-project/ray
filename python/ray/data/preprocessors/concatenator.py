from typing import List, Optional
import numpy as np
import pandas as pd

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
        self.include = include
        self.exclude = exclude or []
        self.dtype = dtype
        self.raise_if_missing = raise_if_missing

    def _validate(self, df: pd.DataFrame):
        total_columns = set(df)
        if self.exclude and self.raise_if_missing:
            missing_columns = set(self.exclude) - total_columns.intersection(
                set(self.exclude)
            )
            if missing_columns:
                raise ValueError(
                    f"Missing columns specified in 'exclude': {missing_columns}"
                )
        if self.include and self.raise_if_missing:
            missing_columns = set(self.include) - total_columns.intersection(
                set(self.include)
            )
            if missing_columns:
                raise ValueError(
                    f"Missing columns specified in 'include': {missing_columns}"
                )

    def _transform_pandas(self, df: pd.DataFrame):
        self._validate(df)

        included_columns = set(df)
        if self.include:  # subset of included columns
            included_columns = set(self.include)

        columns_to_concat = list(included_columns - set(self.exclude))
        ordered_columns_to_concat = [
            col for col in df.columns if col in columns_to_concat
        ]
        concatenated = df[ordered_columns_to_concat].to_numpy(dtype=self.dtype)
        df = df.drop(columns=columns_to_concat)
        # Use a Pandas Series for column assignment to get more consistent
        # behavior across Pandas versions.
        df.loc[:, self.output_column_name] = pd.Series(list(concatenated))
        return df

    def __repr__(self):
        default_values = {
            "output_column_name": "concat_out",
            "include": None,
            "exclude": [],
            "dtype": None,
            "raise_if_missing": False,
        }

        non_default_arguments = []
        for parameter, default_value in default_values.items():
            value = getattr(self, parameter)
            if value != default_value:
                non_default_arguments.append(f"{parameter}={value}")

        return f"{self.__class__.__name__}({', '.join(non_default_arguments)})"
