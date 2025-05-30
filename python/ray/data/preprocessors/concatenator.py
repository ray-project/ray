import logging
from typing import List, Optional

import numpy as np
import pandas as pd

from ray.data.preprocessor import Preprocessor
from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)


@PublicAPI(stability="alpha")
class Concatenator(Preprocessor):
    """Combine numeric columns into a column of type
    :class:`~ray.air.util.tensor_extensions.pandas.TensorDtype`. Only columns
    specified in ``columns`` will be concatenated.

    This preprocessor concatenates numeric columns and stores the result in a new
    column. The new column contains
    :class:`~ray.air.util.tensor_extensions.pandas.TensorArrayElement` objects of
    shape :math:`(m,)`, where :math:`m` is the number of columns concatenated.
    The :math:`m` concatenated columns are dropped after concatenation.
    The preprocessor preserves the order of the columns provided in the ``colummns``
    argument and will use that order when calling ``transform()`` and ``transform_batch()``.

    Examples:
        >>> import numpy as np
        >>> import pandas as pd
        >>> import ray
        >>> from ray.data.preprocessors import Concatenator

        :py:class:`Concatenator` combines numeric columns into a column of
        :py:class:`~ray.air.util.tensor_extensions.pandas.TensorDtype`.

        >>> df = pd.DataFrame({"X0": [0, 3, 1], "X1": [0.5, 0.2, 0.9]})
        >>> ds = ray.data.from_pandas(df)  # doctest: +SKIP
        >>> concatenator = Concatenator(columns=["X0", "X1"])
        >>> concatenator.transform(ds).to_pandas()  # doctest: +SKIP
           concat_out
        0  [0.0, 0.5]
        1  [3.0, 0.2]
        2  [1.0, 0.9]

        By default, the created column is called `"concat_out"`, but you can specify
        a different name.

        >>> concatenator = Concatenator(columns=["X0", "X1"], output_column_name="tensor")
        >>> concatenator.transform(ds).to_pandas()  # doctest: +SKIP
               tensor
        0  [0.0, 0.5]
        1  [3.0, 0.2]
        2  [1.0, 0.9]

        >>> concatenator = Concatenator(columns=["X0", "X1"], dtype=np.float32)
        >>> concatenator.transform(ds)  # doctest: +SKIP
        Dataset(num_rows=3, schema={Y: object, concat_out: TensorDtype(shape=(2,), dtype=float32)})

        When ``flatten=True``, nested vectors in the columns will be flattened during concatenation:

        >>> df = pd.DataFrame({"X0": [[1, 2], [3, 4]], "X1": [0.5, 0.2]})
        >>> ds = ray.data.from_pandas(df)  # doctest: +SKIP
        >>> concatenator = Concatenator(columns=["X0", "X1"], flatten=True)
        >>> concatenator.transform(ds).to_pandas()  # doctest: +SKIP
           concat_out
        0  [1.0, 2.0, 0.5]
        1  [3.0, 4.0, 0.2]

    Args:
        columns: A list of columns to concatenate. The provided order of the columns
             will be retained during concatenation.
        output_column_name: The desired name for the new column.
            Defaults to ``"concat_out"``.
        dtype: The ``dtype`` to convert the output tensors to. If unspecified,
            the ``dtype`` is determined by standard coercion rules.
        raise_if_missing: If ``True``, an error is raised if any
            of the columns in ``columns`` don't exist.
            Defaults to ``False``.
        flatten: If ``True``, nested vectors in the columns will be flattened during
            concatenation. Defaults to ``False``.

    Raises:
        ValueError: if `raise_if_missing` is `True` and a column in `columns` or
            doesn't exist in the dataset.
    """  # noqa: E501

    _is_fittable = False

    def __init__(
        self,
        columns: List[str],
        output_column_name: str = "concat_out",
        dtype: Optional[np.dtype] = None,
        raise_if_missing: bool = False,
        flatten: bool = False,
    ):
        self.columns = columns

        self.output_column_name = output_column_name
        self.dtype = dtype
        self.raise_if_missing = raise_if_missing
        self.flatten = flatten

    def _validate(self, df: pd.DataFrame) -> None:
        missing_columns = set(self.columns) - set(df)
        if missing_columns:
            message = (
                f"Missing columns specified in '{self.columns}': {missing_columns}"
            )
            if self.raise_if_missing:
                raise ValueError(message)
            else:
                logger.warning(message)

    def _transform_pandas(self, df: pd.DataFrame):
        self._validate(df)

        if self.flatten:
            concatenated = df[self.columns].to_numpy()
            concatenated = [
                np.concatenate(
                    [
                        np.atleast_1d(elem)
                        if self.dtype is None
                        else np.atleast_1d(elem).astype(self.dtype)
                        for elem in row
                    ]
                )
                for row in concatenated
            ]
        else:
            concatenated = df[self.columns].to_numpy(dtype=self.dtype)

        df = df.drop(columns=self.columns)
        # Use a Pandas Series for column assignment to get more consistent
        # behavior across Pandas versions.
        df.loc[:, self.output_column_name] = pd.Series(list(concatenated))
        return df

    def __repr__(self):
        default_values = {
            "output_column_name": "concat_out",
            "columns": None,
            "dtype": None,
            "raise_if_missing": False,
            "flatten": False,
        }

        non_default_arguments = []
        for parameter, default_value in default_values.items():
            value = getattr(self, parameter)
            if value != default_value:
                non_default_arguments.append(f"{parameter}={value}")

        return f"{self.__class__.__name__}({', '.join(non_default_arguments)})"
