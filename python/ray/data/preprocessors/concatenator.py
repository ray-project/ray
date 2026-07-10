import logging
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from ray.data.preprocessor import SerializablePreprocessorBase
from ray.data.preprocessors.utils import (
    _PublicField,
    migrate_private_fields,
)
from ray.data.preprocessors.version_support import SerializablePreprocessor
from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)


@PublicAPI(stability="alpha")
@SerializablePreprocessor(version=1, identifier="io.ray.preprocessors.concatenator")
class Concatenator(SerializablePreprocessorBase):
    """Combine numeric columns into a column of type
    :class:`~ray.data._internal.tensor_extensions.pandas.TensorDtype`. Only columns
    specified in ``columns`` will be concatenated.

    This preprocessor concatenates numeric columns and stores the result in a new
    column. The new column contains
    :class:`~ray.data._internal.tensor_extensions.pandas.TensorArrayElement` objects of
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
        :py:class:`~ray.data._internal.tensor_extensions.pandas.TensorDtype`.

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
        super().__init__()
        self._columns = columns
        self._output_column_name = output_column_name
        self._dtype = dtype
        self._raise_if_missing = raise_if_missing
        self._flatten = flatten

    @property
    def columns(self) -> List[str]:
        return self._columns

    @property
    def output_column_name(self) -> str:
        return self._output_column_name

    @property
    def dtype(self) -> Optional[np.dtype]:
        return self._dtype

    @property
    def raise_if_missing(self) -> bool:
        return self._raise_if_missing

    @property
    def flatten(self) -> bool:
        return self._flatten

    def _validate(self, df: pd.DataFrame) -> None:
        missing_columns = set(self._columns) - set(df)
        if missing_columns:
            message = (
                f"Missing columns specified in '{self._columns}': {missing_columns}"
            )
            if self._raise_if_missing:
                raise ValueError(message)
            else:
                logger.warning(message)

    def _transform_pandas(self, df: pd.DataFrame):
        self._validate(df)

        if self._flatten:
            concatenated = df[self._columns].to_numpy()
            concatenated = [
                np.concatenate(
                    [
                        np.atleast_1d(elem)
                        if self._dtype is None
                        else np.atleast_1d(elem).astype(self._dtype)
                        for elem in row
                    ]
                )
                for row in concatenated
            ]
        else:
            concatenated = df[self._columns].to_numpy(dtype=self._dtype)

        df = df.drop(columns=self._columns)
        # Use a Pandas Series for column assignment to get more consistent
        # behavior across Pandas versions.
        df.loc[:, self._output_column_name] = pd.Series(list(concatenated))
        return df

    def get_input_columns(self) -> List[str]:
        return self._columns

    def get_output_columns(self) -> List[str]:
        return [self._output_column_name]

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

    def _get_serializable_fields(self) -> Dict[str, Any]:
        return {
            "columns": self._columns,
            "output_column_name": self._output_column_name,
            "dtype": self._dtype,
            "raise_if_missing": self._raise_if_missing,
            "flatten": getattr(self, "_flatten", False),
        }

    def _set_serializable_fields(self, fields: Dict[str, Any], version: int):
        # required fields
        self._columns = fields["columns"]
        self._output_column_name = fields["output_column_name"]
        self._dtype = fields["dtype"]
        self._raise_if_missing = fields["raise_if_missing"]
        # optional fields (flatten was added later)
        self._flatten = fields.get("flatten", False)

    def __setstate__(self, state: Dict[str, Any]) -> None:
        super().__setstate__(state)
        migrate_private_fields(
            self,
            fields={
                "_columns": _PublicField(public_field="columns"),
                "_output_column_name": _PublicField(
                    public_field="output_column_name", default="concat_out"
                ),
                "_dtype": _PublicField(public_field="dtype", default=None),
                "_raise_if_missing": _PublicField(
                    public_field="raise_if_missing", default=False
                ),
                "_flatten": _PublicField(public_field="flatten", default=False),
            },
        )
