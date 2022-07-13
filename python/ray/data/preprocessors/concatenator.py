from typing import List, Optional
import numpy as np
import pandas as pd

from ray.data.extensions import TensorArray
from ray.data.preprocessor import Preprocessor


class Concatenator(Preprocessor):
    """Create tensor columns via concatenation.

    A tensor column is a a column consisting of ndarrays as elements.

    Example:
        >>> import pandas as pd
        >>> from ray.data.preprocessors import Concatenator
        >>> df = pd.DataFrame({"a": [1, 2, 3, 4], "b": [1, 2, 3, 4],})
        >>> ds = ray.data.from_pandas(df)
        >>> prep = Concatenator(["a", "b"], "c")
        >>> new_ds = prep.transform(ds)
        >>> df = new_ds.to_pandas()
        #         c
        # 0  [1, 1]
        # 1  [2, 2]
        #       ...
        >>> x = df["c"].iloc[0]
        >>> assert x.to_numpy().tolist() == [1, 1]

    Args:
        output_column: output_column is a string that represents the
            name of the outputted, concatenated tensor.
        exclude: A list of column names that should be excluded
            from concatenation. All other columns will be dropped.
        dtype: Optional. The dtype to convert the output column array to.

    Raises:
        ValueError if any element in `columns` does not exist in the dataset.
    """

    _is_fittable = False

    def __init__(
        self,
        output_column: str,
        exclude: Optional[List[str]] = None,
        dtype: Optional[np.dtype] = None,
    ):
        self.output_column = output_column
        self.exclude_columns = exclude or []
        self.dtype = dtype

    def _validate(self, df: pd.DataFrame):
        ds_columns = set(df)
        specified_set = set(self.exclude_columns)
        missing_columns = specified_set - ds_columns.intersection(specified_set)
        if missing_columns:
            raise ValueError(
                f"Missing columns specified in 'exclude': {missing_columns}"
            )

    def _transform_pandas(self, df: pd.DataFrame):
        self._validate(df)
        columns_to_concat = list(set(df) - set(self.exclude_columns))
        concatenated = df[columns_to_concat].to_numpy(dtype=self.dtype)
        df = df.drop(columns=columns_to_concat)
        df[self.output_column] = TensorArray(concatenated)
        return df

    def __repr__(self):
        return (
            f"Concatenator(output_column={self.output_column}, "
            f"exclude={self.exclude_columns}, dtype={self.dtype})"
        )

    df = pd.DataFrame(
        {
            "a": [1, 2, 3, 4],
            "b": [1, 2, 3, 4],
        }
    )
