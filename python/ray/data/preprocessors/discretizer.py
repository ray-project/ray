from typing import Dict, Iterable, List, Optional, Type, Union

import numpy as np
import pandas as pd

from ray.data import Dataset
from ray.data.aggregate import Max, Min
from ray.data.preprocessor import Preprocessor
from ray.util.annotations import PublicAPI


class _AbstractKBinsDiscretizer(Preprocessor):
    """Abstract base class for all KBinsDiscretizers.

    Essentially a thin wraper around ``pd.cut``.

    Expects either ``self.stats_`` or ``self.bins`` to be set and
    contain {column:list_of_bin_intervals}.
    """

    def _transform_pandas(self, df: pd.DataFrame):
        def bin_values(s: pd.Series) -> pd.Series:
            if s.name not in self.columns:
                return s
            labels = self.dtypes.get(s.name) if self.dtypes else False
            ordered = True
            if labels:
                if isinstance(labels, pd.CategoricalDtype):
                    ordered = labels.ordered
                    labels = list(labels.categories)
                else:
                    labels = False

            bins = self.stats_ if self._is_fittable else self.bins
            return pd.cut(
                s,
                bins[s.name] if isinstance(bins, dict) else bins,
                right=self.right,
                labels=labels,
                ordered=ordered,
                retbins=False,
                include_lowest=self.include_lowest,
                duplicates=self.duplicates,
            )

        return df.apply(bin_values, axis=0)

    def _validate_bins_columns(self):
        if isinstance(self.bins, dict) and not all(
            col in self.bins for col in self.columns
        ):
            raise ValueError(
                "If `bins` is a dictionary, all elements of `columns` must be present "
                "in it."
            )

    def __repr__(self):
        attr_str = ", ".join(
            [
                f"{attr_name}={attr_value!r}"
                for attr_name, attr_value in vars(self).items()
                if not attr_name.startswith("_")
            ]
        )
        return f"{self.__class__.__name__}({attr_str})"


@PublicAPI(stability="alpha")
class CustomKBinsDiscretizer(_AbstractKBinsDiscretizer):
    """Bin values into discrete intervals using custom bin edges.

    Columns must contain numerical values.

    Examples:
        Use :class:`CustomKBinsDiscretizer` to bin continuous features.

        >>> import pandas as pd
        >>> import ray
        >>> from ray.data.preprocessors import CustomKBinsDiscretizer
        >>> df = pd.DataFrame({
        ...     "value_1": [0.2, 1.4, 2.5, 6.2, 9.7, 2.1],
        ...     "value_2": [10, 15, 13, 12, 23, 25],
        ... })
        >>> ds = ray.data.from_pandas(df)
        >>> discretizer = CustomKBinsDiscretizer(
        ...     columns=["value_1", "value_2"],
        ...     bins=[0, 1, 4, 10, 25]
        ... )
        >>> discretizer.transform(ds).to_pandas()
           value_1  value_2
        0        0        2
        1        1        3
        2        1        3
        3        2        3
        4        2        3
        5        1        3

        You can also specify different bin edges per column.

        >>> discretizer = CustomKBinsDiscretizer(
        ...     columns=["value_1", "value_2"],
        ...     bins={"value_1": [0, 1, 4], "value_2": [0, 18, 35, 70]},
        ... )
        >>> discretizer.transform(ds).to_pandas()
           value_1  value_2
        0      0.0        0
        1      1.0        0
        2      1.0        0
        3      NaN        0
        4      NaN        1
        5      1.0        1


    Args:
        columns: The columns to discretize.
        bins: Defines custom bin edges. Can be an iterable of numbers,
            a ``pd.IntervalIndex``, or a dict mapping columns to either of them.
            Note that ``pd.IntervalIndex`` for bins must be non-overlapping.
        right: Indicates whether bins include the rightmost edge.
        include_lowest: Indicates whether the first interval should be left-inclusive.
        duplicates: Can be either 'raise' or 'drop'. If bin edges are not unique,
            raise ``ValueError`` or drop non-uniques.
        dtypes: An optional dictionary that maps columns to ``pd.CategoricalDtype``
            objects or ``np.integer`` types. If you don't include a column in ``dtypes``
            or specify it as an integer dtype, the outputted column will consist of
            ordered integers corresponding to bins. If you use a
            ``pd.CategoricalDtype``, the outputted column will be a
            ``pd.CategoricalDtype`` with the categories being mapped to bins.
            You can use ``pd.CategoricalDtype(categories, ordered=True)`` to
            preserve information about bin order.

    .. seealso::

        :class:`UniformKBinsDiscretizer`
            If you want to bin data into uniform width bins.
    """

    def __init__(
        self,
        columns: List[str],
        bins: Union[
            Iterable[float],
            pd.IntervalIndex,
            Dict[str, Union[Iterable[float], pd.IntervalIndex]],
        ],
        *,
        right: bool = True,
        include_lowest: bool = False,
        duplicates: str = "raise",
        dtypes: Optional[
            Dict[str, Union[pd.CategoricalDtype, Type[np.integer]]]
        ] = None,
    ):
        self.columns = columns
        self.bins = bins
        self.right = right
        self.include_lowest = include_lowest
        self.duplicates = duplicates
        self.dtypes = dtypes

        self._validate_bins_columns()

    _is_fittable = False


@PublicAPI(stability="alpha")
class UniformKBinsDiscretizer(_AbstractKBinsDiscretizer):
    """Bin values into discrete intervals (bins) of uniform width.

    Columns must contain numerical values.

    Examples:
        Use :class:`UniformKBinsDiscretizer` to bin continuous features.

        >>> import pandas as pd
        >>> import ray
        >>> from ray.data.preprocessors import UniformKBinsDiscretizer
        >>> df = pd.DataFrame({
        ...     "value_1": [0.2, 1.4, 2.5, 6.2, 9.7, 2.1],
        ...     "value_2": [10, 15, 13, 12, 23, 25],
        ... })
        >>> ds = ray.data.from_pandas(df)
        >>> discretizer = UniformKBinsDiscretizer(
        ...     columns=["value_1", "value_2"], bins=4
        ... )
        >>> discretizer.fit_transform(ds).to_pandas()
           value_1  value_2
        0        0        0
        1        0        1
        2        0        0
        3        2        0
        4        3        3
        5        0        3

        You can also specify different number of bins per column.

        >>> discretizer = UniformKBinsDiscretizer(
        ...     columns=["value_1", "value_2"], bins={"value_1": 4, "value_2": 3}
        ... )
        >>> discretizer.fit_transform(ds).to_pandas()
           value_1  value_2
        0        0        0
        1        0        0
        2        0        0
        3        2        0
        4        3        2
        5        0        2


    Args:
        columns: The columns to discretize.
        bins: Defines the number of equal-width bins.
            Can be either an integer (which will be applied to all columns),
            or a dict that maps columns to integers.
            The range is extended by .1% on each side to include
            the minimum and maximum values.
        right: Indicates whether bins includes the rightmost edge or not.
        include_lowest: Whether the first interval should be left-inclusive
            or not.
        duplicates: Can be either 'raise' or 'drop'. If bin edges are not unique,
            raise ``ValueError`` or drop non-uniques.
        dtypes: An optional dictionary that maps columns to ``pd.CategoricalDtype``
            objects or ``np.integer`` types. If you don't include a column in ``dtypes``
            or specify it as an integer dtype, the outputted column will consist of
            ordered integers corresponding to bins. If you use a
            ``pd.CategoricalDtype``, the outputted column will be a
            ``pd.CategoricalDtype`` with the categories being mapped to bins.
            You can use ``pd.CategoricalDtype(categories, ordered=True)`` to
            preserve information about bin order.

    .. seealso::

        :class:`CustomKBinsDiscretizer`
            If you want to specify your own bin edges.
    """

    def __init__(
        self,
        columns: List[str],
        bins: Union[int, Dict[str, int]],
        *,
        right: bool = True,
        include_lowest: bool = False,
        duplicates: str = "raise",
        dtypes: Optional[
            Dict[str, Union[pd.CategoricalDtype, Type[np.integer]]]
        ] = None,
    ):
        self.columns = columns
        self.bins = bins
        self.right = right
        self.include_lowest = include_lowest
        self.duplicates = duplicates
        self.dtypes = dtypes

    def _fit(self, dataset: Dataset) -> Preprocessor:
        self._validate_on_fit()
        stats = {}
        aggregates = []
        if isinstance(self.bins, dict):
            columns = self.bins.keys()
        else:
            columns = self.columns

        for column in columns:
            aggregates.extend(
                self._fit_uniform_covert_bin_to_aggregate_if_needed(column)
            )

        aggregate_stats = dataset.aggregate(*aggregates)
        mins = {}
        maxes = {}
        for key, value in aggregate_stats.items():
            column_name = key[4:-1]  # min(column) -> column
            if key.startswith("min"):
                mins[column_name] = value
            if key.startswith("max"):
                maxes[column_name] = value

        for column in mins.keys():
            bins = self.bins[column] if isinstance(self.bins, dict) else self.bins
            stats[column] = _translate_min_max_number_of_bins_to_bin_edges(
                mins[column], maxes[column], bins, self.right
            )

        self.stats_ = stats
        return self

    def _validate_on_fit(self):
        self._validate_bins_columns()

    def _fit_uniform_covert_bin_to_aggregate_if_needed(self, column: str):
        bins = self.bins[column] if isinstance(self.bins, dict) else self.bins
        if isinstance(bins, int):
            return (Min(column), Max(column))
        else:
            raise TypeError(
                f"`bins` must be an integer or a dict of integers, got {bins}"
            )


# Copied from
# https://github.com/pandas-dev/pandas/blob/v1.4.4/pandas/core/reshape/tile.py#L257
# under
# BSD 3-Clause License
#
# Copyright (c) 2008-2011, AQR Capital Management, LLC, Lambda Foundry, Inc.
# and PyData Development Team
# All rights reserved.
#
# Copyright (c) 2011-2022, Open source contributors.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
#
# * Neither the name of the copyright holder nor the names of its
#   contributors may be used to endorse or promote products derived from
#   this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
def _translate_min_max_number_of_bins_to_bin_edges(
    mn: float, mx: float, bins: int, right: bool
) -> List[float]:
    """Translates a range and desired number of bins into list of bin edges."""
    rng = (mn, mx)
    mn, mx = (mi + 0.0 for mi in rng)

    if np.isinf(mn) or np.isinf(mx):
        raise ValueError(
            "Cannot specify integer `bins` when input data contains infinity."
        )
    elif mn == mx:  # adjust end points before binning
        mn -= 0.001 * abs(mn) if mn != 0 else 0.001
        mx += 0.001 * abs(mx) if mx != 0 else 0.001
        bins = np.linspace(mn, mx, bins + 1, endpoint=True)
    else:  # adjust end points after binning
        bins = np.linspace(mn, mx, bins + 1, endpoint=True)
        adj = (mx - mn) * 0.001  # 0.1% of the range
        if right:
            bins[0] -= adj
        else:
            bins[-1] += adj
    return bins


# TODO(ml-team)
# Add QuantileKBinsDiscretizer
