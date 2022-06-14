from typing import List

import numpy as np
import pandas as pd

from ray.data.preprocessor import Preprocessor


class PowerTransformer(Preprocessor):
    """Apply power function to make data more normally distributed.

    See https://en.wikipedia.org/wiki/Power_transform.

    Supports the following methods:
        * Yeo-Johnson (positive and negative numbers)
        * Box-Cox (positive numbers only)

    Currently, this requires the user to specify the ``power`` parameter.
    In the future, an optimal value can be determined in ``fit()``.

    Args:
        columns: The columns that will individually be transformed.
        power: The power parameter which is used as the exponent.
        method: Supports "yeo-johnson" and "box-cox". Defaults to "yeo-johnson".
    """

    _valid_methods = ["yeo-johnson", "box-cox"]
    _is_fittable = False

    def __init__(self, columns: List[str], power: float, method: str = "yeo-johnson"):
        self.columns = columns
        self.method = method
        self.power = power

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

        df.loc[:, self.columns] = df.loc[:, self.columns].transform(
            column_power_transformer
        )
        return df

    def __repr__(self):
        return (
            f"PowerTransformer("
            f"columns={self.columns}, "
            f"method={self.method}, "
            f"power={self.power})"
        )
