from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from ray.data.preprocessor import Preprocessor
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class PowerTransformer(Preprocessor):
    """Apply a `power transform <https://en.wikipedia.org/wiki/Power_transform>`_ to
    make your data more normally distributed.

    Some models expect data to be normally distributed. By making your data more
    Gaussian-like, you might be able to improve your model's performance.

    This preprocessor supports the following transformations:

    * `Yeo-Johnson <https://en.wikipedia.org/wiki/Power_transform#Yeo%E2%80%93Johnson_transformation>`_
    * `Box-Cox <https://en.wikipedia.org/wiki/Power_transform#Box%E2%80%93Cox_transformation>`_

    Box-Cox requires all data to be positive.

    .. warning::

        You need to manually specify the transform's power parameter. If you
        choose a bad value, the transformation might not work well.

    Args:
        columns: The columns to separately transform.
        power: A parameter that determines how your data is transformed. Practioners
            typically set ``power`` between :math:`-2.5` and :math:`2.5`, although you
            may need to try different values to find one that works well.
        method: A string representing which transformation to apply. Supports
            ``"yeo-johnson"`` and ``"box-cox"``. If you choose ``"box-cox"``, your data
            needs to be positive. Defaults to ``"yeo-johnson"``.
        output_columns: The names of the transformed columns. If None, the transformed
            columns will be the same as the input columns. If not None, the length of
            ``output_columns`` must match the length of ``columns``, othwerwise an error
            will be raised.
    """  # noqa: E501

    _valid_methods = ["yeo-johnson", "box-cox"]
    _is_fittable = False

    def __init__(
        self,
        columns: List[str],
        power: float,
        method: str = "yeo-johnson",
        *,
        output_columns: Optional[List[str]] = None,
    ):
        super().__init__()
        self._columns = columns
        self._method = method
        self._power = power
        self._output_columns = Preprocessor._derive_and_validate_output_columns(
            columns, output_columns
        )

        if method not in self._valid_methods:
            raise ValueError(
                f"Method {method} is not supported."
                f"Supported values are: {self._valid_methods}"
            )

    @property
    def columns(self) -> List[str]:
        return self._columns

    @property
    def method(self) -> str:
        return self._method

    @property
    def power(self) -> float:
        return self._power

    @property
    def output_columns(self) -> List[str]:
        return self._output_columns

    def _transform_pandas(self, df: pd.DataFrame):
        def column_power_transformer(s: pd.Series):
            if self._method == "yeo-johnson":
                result = np.zeros_like(s, dtype=np.float64)
                pos = s >= 0  # binary mask

                if self._power != 0:
                    result[pos] = (np.power(s[pos] + 1, self._power) - 1) / self._power
                else:
                    result[pos] = np.log(s[pos] + 1)

                if self._power != 2:
                    result[~pos] = -(np.power(-s[~pos] + 1, 2 - self._power) - 1) / (
                        2 - self._power
                    )
                else:
                    result[~pos] = -np.log(-s[~pos] + 1)
                return result

            else:  # box-cox
                if self._power != 0:
                    return (np.power(s, self._power) - 1) / self._power
                else:
                    return np.log(s)

        df[self._output_columns] = df[self._columns].transform(column_power_transformer)
        return df

    def __repr__(self):
        return (
            f"{self.__class__.__name__}(columns={self._columns!r}, "
            f"power={self._power!r}, method={self._method!r}, "
            f"output_columns={self._output_columns!r})"
        )

    def __setstate__(self, state: Dict[str, Any]) -> None:
        super().__setstate__(state)
        if "_columns" not in self.__dict__ and "columns" in self.__dict__:
            self._columns = self.__dict__.pop("columns")
        if "_power" not in self.__dict__ and "power" in self.__dict__:
            self._power = self.__dict__.pop("power")
        if "_method" not in self.__dict__ and "method" in self.__dict__:
            self._method = self.__dict__.pop("method")
        if "_output_columns" not in self.__dict__ and "output_columns" in self.__dict__:
            self._output_columns = self.__dict__.pop("output_columns")

        if "_columns" not in self.__dict__:
            self._columns = []
        if "_power" not in self.__dict__:
            raise ValueError(
                "Invalid serialized HashingVectorizer: missing required field 'power'."
            )
        if "_method" not in self.__dict__:
            self._method = "yeo-johnson"
        if "_output_columns" not in self.__dict__:
            self._output_columns = self._columns
