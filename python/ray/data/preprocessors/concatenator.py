import logging

from typing import List, Optional, Union
import numpy as np
import pandas as pd

from ray.data.preprocessor import Preprocessor
from ray.util.annotations import PublicAPI


logger = logging.getLogger(__name__)


@PublicAPI(stability="alpha")
class Concatenator(Preprocessor):
    """Combine numeric columns into a column of type
    :class:`~ray.air.util.tensor_extensions.pandas.TensorDtype`.

    This preprocessor concatenates numeric columns and stores the result in a new
    column. The new column contains
    :class:`~ray.air.util.tensor_extensions.pandas.TensorArrayElement` objects of
    shape :math:`(m,)`, where :math:`m` is the number of columns concatenated.
    The :math:`m` concatenated columns are dropped after concatenation.

    Examples:
        >>> import numpy as np
        >>> import pandas as pd
        >>> import ray
        >>> from ray.data.preprocessors import Concatenator

        :py:class:`Concatenator` combines numeric columns into a column of
        :py:class:`~ray.air.util.tensor_extensions.pandas.TensorDtype`.

        >>> df = pd.DataFrame({"X0": [0, 3, 1], "X1": [0.5, 0.2, 0.9]})
        >>> ds = ray.data.from_pandas(df)  # doctest: +SKIP
        >>> concatenator = Concatenator()
        >>> concatenator.fit_transform(ds).to_pandas()  # doctest: +SKIP
           concat_out
        0  [0.0, 0.5]
        1  [3.0, 0.2]
        2  [1.0, 0.9]

        By default, the created column is called `"concat_out"`, but you can specify
        a different name.

        >>> concatenator = Concatenator(output_column_name="tensor")
        >>> concatenator.fit_transform(ds).to_pandas()  # doctest: +SKIP
               tensor
        0  [0.0, 0.5]
        1  [3.0, 0.2]
        2  [1.0, 0.9]

        Sometimes, you might not want to concatenate all of of the columns in your
        dataset. In this case, you can exclude columns with the ``exclude`` parameter.

        >>> df = pd.DataFrame({"X0": [0, 3, 1], "X1": [0.5, 0.2, 0.9], "Y": ["blue", "orange", "blue"]})
        >>> ds = ray.data.from_pandas(df)  # doctest: +SKIP
        >>> concatenator = Concatenator(exclude=["Y"])
        >>> concatenator.fit_transform(ds).to_pandas()  # doctest: +SKIP
                Y  concat_out
        0    blue  [0.0, 0.5]
        1  orange  [3.0, 0.2]
        2    blue  [1.0, 0.9]

        Alternatively, you can specify which columns to concatenate with the
        ``include`` parameter.

        >>> concatenator = Concatenator(include=["X0", "X1"])
        >>> concatenator.fit_transform(ds).to_pandas()  # doctest: +SKIP
                Y  concat_out
        0    blue  [0.0, 0.5]
        1  orange  [3.0, 0.2]
        2    blue  [1.0, 0.9]

        Note that if a column is in both ``include`` and ``exclude``, the column is
        excluded.

        >>> concatenator = Concatenator(include=["X0", "X1", "Y"], exclude=["Y"])
        >>> concatenator.fit_transform(ds).to_pandas()  # doctest: +SKIP
                Y  concat_out
        0    blue  [0.0, 0.5]
        1  orange  [3.0, 0.2]
        2    blue  [1.0, 0.9]

        By default, the concatenated tensor is a ``dtype`` common to the input columns.
        However, you can also explicitly set the ``dtype`` with the ``dtype``
        parameter.

        >>> concatenator = Concatenator(include=["X0", "X1"], dtype=np.float32)
        >>> concatenator.fit_transform(ds)  # doctest: +SKIP
        Dataset(num_blocks=1, num_rows=3, schema={Y: object, concat_out: TensorDtype(shape=(2,), dtype=float32)})

    Args:
        output_column_name: The desired name for the new column.
            Defaults to ``"concat_out"``.
        include: A list of columns to concatenate. If ``None``, all columns are
            concatenated.
        exclude: A list of column to exclude from concatenation.
            If a column is in both ``include`` and ``exclude``, the column is excluded
            from concatenation.
        dtype: The ``dtype`` to convert the output tensors to. If unspecified,
            the ``dtype`` is determined by standard coercion rules.
        raise_if_missing: If ``True``, an error is raised if any
            of the columns in ``include`` or ``exclude`` don't exist.
            Defaults to ``False``.

    Raises:
        ValueError: if `raise_if_missing` is `True` and a column in `include` or
            `exclude` doesn't exist in the dataset.
    """  # noqa: E501

    _is_fittable = False

    def __init__(
        self,
        output_column_name: str = "concat_out",
        include: Optional[List[str]] = None,
        exclude: Optional[Union[str, List[str]]] = None,
        dtype: Optional[np.dtype] = None,
        raise_if_missing: bool = False,
    ):
        if isinstance(include, str):
            include = [include]
        if isinstance(exclude, str):
            exclude = [exclude]

        self.output_column_name = output_column_name
        self.include = include
        self.exclude = exclude or []
        self.dtype = dtype
        self.raise_if_missing = raise_if_missing

    def _validate(self, df: pd.DataFrame) -> None:
        for parameter in "include", "exclude":
            columns = getattr(self, parameter)
            if columns is None:
                continue

            missing_columns = set(columns) - set(df)
            if not missing_columns:
                continue

            message = f"Missing columns specified in '{parameter}': {missing_columns}"
            if self.raise_if_missing:
                raise ValueError(message)
            else:
                logger.warning(message)

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
