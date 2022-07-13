from typing import List, Optional
import numpy as np
import pandas as pd

from ray.data.extensions import TensorArray
from ray.data.preprocessor import Preprocessor


class Tensorizer(Preprocessor):
    """Create tensor columns via concatenation.

    A tensor column is a a column consisting of ndarrays as elements.

    Args:
        columns: A list of column names that should be
            concatenated into a single column. After concatenation,
            these columns will be dropped.
        output_column: output_column is a string that represents the
            name of the outputted, concatenated tensor.
        dtype: Optional. The dtype to convert the output column array to.

    Raises:
        ValueError if any element in `columns` does not exist in the dataset.
    """

    _is_fittable = False

    def __init__(
        self,
        columns: List[str],
        output_column: str,
        dtype: Optional[np.dtype] = None,
    ):
        self.columns = columns
        self.output_column = output_column
        self.dtype = dtype

    def _validate(self, df: pd.DataFrame):
        ds_columns = set(df)
        specified_set = set(self.columns)
        missing_columns = specified_set - ds_columns.intersection(specified_set)
        if missing_columns:
            raise ValueError(f"Missing specified columns from dataset: {missing_columns}")

    def _transform_pandas(self, df: pd.DataFrame):
        self._validate(df)
        concatenated = df[self.columns].to_numpy(dtype=self.dtype)
        df = df.drop(columns=self.columns)
        df[self.output_column] = TensorArray(concatenated)
        return df

    def __repr__(self):
        return (
            f"Tensorizer(columns={self.columns}, "
            f"num_features={self.num_features}, dtype={self.dtype})"
        )
