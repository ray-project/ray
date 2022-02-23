import logging
from typing import Dict, List, Union, Tuple, Optional

import numpy as np

VALID_AGGREGATE_TYPES: Tuple[type] = (
    int,
    float,
    np.float32,
    np.float64,
    np.int32,
    np.int64,
)

logger = logging.getLogger(__name__)


def _get_values_from_results(key, reported_metrics, results, get_weights=False):
    """Return values in the results list from all workers."""
    values = [result.get(key, np.nan) for result in results]
    warning_message = None
    if key not in reported_metrics:
        warning_message = (
            f"Averaging weight `{key}` is not reported in `train.report()`. "
            if get_weights
            else (
                f"`{key}` is not reported from workers, so it is ignored. "
                "Please make sure that it is saved using `train.report()`."
            )
        )
    else:
        if not all(isinstance(value, VALID_AGGREGATE_TYPES) for value in values):
            warning_message = (
                (
                    f"Averaging weight `{key}` value type "
                    f"(`{type(values[0])}`) is not valid. "
                )
                if get_weights
                else (
                    f"`{key}` value type (`{type(values[0])}`) is not valid, "
                    f"so it is ignored. "
                )
            )

            warning_message += (
                f"Make sure that its type is one of {VALID_AGGREGATE_TYPES}. "
            )

    if warning_message:
        if get_weights:
            logger.warning(warning_message + "Use equal weight instead.")
            values = np.array([1] * len(results))
        else:
            logger.warning(warning_message)
            return None

    return values


class AggregateFn:
    """An abstract class for aggregation function."""

    def __call__(self, *args, **kwargs):
        raise NotImplementedError

    def prepare(self, *args, **kwargs):
        pass

    def wrap_key(self, key):
        return str(self) + f"({key})"


class Average(AggregateFn):
    def __call__(self, values: List[Union[VALID_AGGREGATE_TYPES]]):
        return np.nanmean(values)

    def __repr__(self):
        return "Average"


class Max(AggregateFn):
    def __call__(self, values: List[Union[VALID_AGGREGATE_TYPES]]):
        return np.nanmax(values)

    def __repr__(self):
        return "Max"


class WeightedAverage(AggregateFn):
    def __init__(self, weight_key: Optional[str] = None):
        self.weight_key = weight_key
        self.weights = None

    def __call__(self, values: List[Union[VALID_AGGREGATE_TYPES]]):
        return np.nansum(np.array(values) * self.weights / np.nansum(self.weights))

    def __repr__(self):
        return f"Weighted average [by {self.weight_key}]"

    def prepare(self, results: List[Dict]):
        reported_metrics = set(results[0].keys())
        self.weights = _get_values_from_results(
            self.weight_key, reported_metrics, results, get_weights=True
        )
