from typing import List, Optional

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
        self.columns = columns
        self.method = method
        self.power = power
        self.output_columns = Preprocessor._derive_and_validate_output_columns(
            columns, output_columns
        )

        if method not in self._valid_methods:
            raise ValueError(
                f"Method {method} is not supported."
                f"Supported values are: {self._valid_methods}"
            )

    def _transform_pandas(self, df: pd.DataFrame):
        def column_power_transformer(s: pd.Series):
            if self.method == "yeo-johnson":
                result = np.zeros_like(s, dtype=np.float64)
                pos = s >= 0  # binary mask

                if self.power != 0:
                    result[pos] = (np.power(s[pos] + 1, self.power) - 1) / self.power
                else:
                    result[pos] = np.log(s[pos] + 1)

                if self.power != 2:
                    result[~pos] = -(np.power(-s[~pos] + 1, 2 - self.power) - 1) / (
                        2 - self.power
                    )
                else:
                    result[~pos] = -np.log(-s[~pos] + 1)
                return result

            else:  # box-cox
                if self.power != 0:
                    return (np.power(s, self.power) - 1) / self.power
                else:
                    return np.log(s)

        df[self.output_columns] = df[self.columns].transform(column_power_transformer)
        return df

    def __repr__(self):
        return (
            f"{self.__class__.__name__}(columns={self.columns!r}, "
            f"power={self.power!r}, method={self.method!r}, "
            f"output_columns={self.output_columns!r})"
        )
