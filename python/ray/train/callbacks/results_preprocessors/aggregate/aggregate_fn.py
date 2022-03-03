from typing import Dict, List, Union, Optional

import numpy as np

from ray.train.callbacks.results_preprocessors.aggregate.aggregate_utils import (
    VALID_AGGREGATE_TYPES,
    _get_weights_from_results,
)


class AggregateFn:
    """An abstract class for aggregation function."""

    def __call__(
        self, values: List[Union[VALID_AGGREGATE_TYPES]]
    ) -> Union[VALID_AGGREGATE_TYPES]:
        """
        Args:
            values (List[Union[VALID_AGGREGATE_TYPES]]): A list of
                values returned from workers. The length of the list
                is expected to be equal to the number of workers.

        Returns:
            An aggregated value.
        """
        raise NotImplementedError

    def prepare(self, results: List[Dict]) -> None:
        """Perform some preparation work before aggregation."""
        pass

    def wrap_key(self, key) -> str:
        """Get a string representation of the aggregation."""
        return str(self) + f"({key})"


class Average(AggregateFn):
    """Average aggregation class."""

    def __call__(
        self, values: List[Union[VALID_AGGREGATE_TYPES]]
    ) -> Union[VALID_AGGREGATE_TYPES]:
        return np.nanmean(values)

    def __repr__(self) -> str:
        return "Average"


class Max(AggregateFn):
    """Maximum aggregation class."""

    def __call__(
        self, values: List[Union[VALID_AGGREGATE_TYPES]]
    ) -> Union[VALID_AGGREGATE_TYPES]:
        return np.nanmax(values)

    def __repr__(self) -> str:
        return "Max"


class WeightedAverage(AggregateFn):
    """Weighted average aggregation class.

    Args:
        weight_key (Optional[str]): A string that specifies
            the average weight to be used. If it is None, then
            equal weight will be used.
    """

    def __init__(self, weight_key: Optional[str] = None):
        self.weight_key = weight_key
        self.weights = None

    def __call__(
        self, values: List[Union[VALID_AGGREGATE_TYPES]]
    ) -> Union[VALID_AGGREGATE_TYPES]:
        return np.nansum(np.array(values) * self.weights / np.nansum(self.weights))

    def __repr__(self) -> str:
        return f"Weighted average [by {self.weight_key}]"

    def prepare(self, results: List[Dict]):
        self.weights = _get_weights_from_results(self.weight_key, results)
