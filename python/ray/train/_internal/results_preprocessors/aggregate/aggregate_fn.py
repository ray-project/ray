import abc
from typing import Dict, List, Union, Optional
import warnings

import numpy as np

from ray.train._internal.results_preprocessors.aggregate.aggregate_utils import (
    VALID_AGGREGATE_TYPES,
    _get_weights_from_results,
)
from ray.util.annotations import Deprecated

_deprecation_msg = (
    "`ray.train.callbacks.results_preprocessors.aggregate` and the `ray.train.Trainer` "
    "API are "
    "deprecated in Ray "
    "2.0, and is replaced by Ray AI Runtime (Ray AIR). Ray AIR "
    "(https://docs.ray.io/en/latest/ray-air/getting-started.html) "
    "will provide greater functionality and a unified API "
    "compared to the current Ray Train API. "
    "This class will be removed in the future."
)


@Deprecated
class AggregateFn(abc.ABC):
    """An abstract class for aggregation function."""

    def __init__(self):
        warnings.warn(
            _deprecation_msg,
            DeprecationWarning,
            stacklevel=2,
        )

    def __call__(
        self, values: List[Union[VALID_AGGREGATE_TYPES]]
    ) -> Union[VALID_AGGREGATE_TYPES]:
        """Perform the aggregation of values when being called.

        Args:
            values (List[Union[VALID_AGGREGATE_TYPES]]): A list of
                values returned from workers. The length of the list
                is expected to be equal to the number of workers.

        Returns:
            A single value that should logically be some form of aggregation
            of the values from each worker in the ``values`` list.
        """
        raise NotImplementedError

    def prepare(self, results: List[Dict]) -> None:
        """Perform some preparation work before aggregation.

        Unlike ``__call__``, this method is not called separately
        for each metric, but is only called once for preparation
        before aggregation begins. Any logic that does not need to
        be called for each metric should be placed in this method.
        """
        pass

    def wrap_key(self, key) -> str:
        """Get a string representation of the aggregation."""
        return str(self) + f"({key})"


@Deprecated
class Average(AggregateFn):
    """Average aggregation class."""

    def __call__(
        self, values: List[Union[VALID_AGGREGATE_TYPES]]
    ) -> Union[VALID_AGGREGATE_TYPES]:
        # A numpy runtime warning will be thrown if values
        # is a list of all ``np.nan``.
        return np.nanmean(values)

    def __repr__(self) -> str:
        return "avg"


@Deprecated
class Max(AggregateFn):
    """Maximum aggregation class."""

    def __call__(
        self, values: List[Union[VALID_AGGREGATE_TYPES]]
    ) -> Union[VALID_AGGREGATE_TYPES]:
        # A numpy runtime warning will be thrown if values
        # is a list of all ``np.nan``.
        return np.nanmax(values)

    def __repr__(self) -> str:
        return "max"


@Deprecated
class WeightedAverage(AggregateFn):
    """Weighted average aggregation class.

    Args:
        weight_key (Optional[str]): A key string that specifies
            the average weight to be used. If it is None, then
            equal weight will be used.
    """

    def __init__(self, weight_key: Optional[str] = None):
        warnings.warn(
            _deprecation_msg,
            DeprecationWarning,
            stacklevel=2,
        )
        self.weight_key = weight_key
        self.weights = None

    def __call__(
        self, values: List[Union[VALID_AGGREGATE_TYPES]]
    ) -> Union[VALID_AGGREGATE_TYPES]:
        return np.nansum(
            np.array(values)
            * self.weights
            / np.nansum(self.weights * (1 - np.isnan(values)))
        )

    def __repr__(self) -> str:
        return f"weight_avg_{self.weight_key}"

    def prepare(self, results: List[Dict]):
        self.weights = _get_weights_from_results(self.weight_key, results)
