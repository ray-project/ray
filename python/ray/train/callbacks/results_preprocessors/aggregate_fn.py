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


class AggregateFn:
    """An abstract class for aggregation function."""

    def __call__(self):
        raise NotImplementedError

    def prepare(self, *args, **kwargs):
        pass


class Average(AggregateFn):
    def __call__(self, values: List[Union[VALID_AGGREGATE_TYPES]]):
        return np.nanmean(values)

    def __repr__(self):
        return "Average:"


class Max(AggregateFn):
    def __call__(self, values: List[Union[VALID_AGGREGATE_TYPES]]):
        return np.nanmax(values)

    def __repr__(self):
        return "Max:"


class WeightedAverage(AggregateFn):
    def __init__(self, weight_key: Optional[str] = None):
        self.weight_key = "" if weight_key is None else weight_key
        self.weights = None

    def __call__(self, values: List[Union[VALID_AGGREGATE_TYPES]]):
        return np.nanmean(np.array(values) * self.weights / np.nansum(self.weights))

    def __repr__(self):
        return "Weighted average:"

    def prepare(self, results: List[Dict]):
        reported_metrics = set(results[0].keys())
        self._get_weights(self.weight_key, reported_metrics, results)

    def _get_weights(self, weight, reported_metrics, results):

        weights_from_workers = np.array(
            [result.get(weight, np.nan) for result in results]
        )

        warning_message = ""
        if weight not in reported_metrics:
            warning_message = (
                f"Averaging weight `{weight}` is not reported in `train.report()`. "
            )
        elif not all(
            isinstance(weight_value, VALID_AGGREGATE_TYPES)
            for weight_value in weights_from_workers
        ):
            warning_message = (
                f"Averaging weight `{weight}` value type "
                f"(`{type(results[0].get(weight, np.nan))}`) is not valid. "
                f"Make sure that its type is one of {VALID_AGGREGATE_TYPES}. "
            )

        # if no weight is provided, equal weight will be used.
        if len(warning_message):
            logger.warning(warning_message + "Use equal weight instead.")
            weights_from_workers = np.array([1] * len(results))

        self.weights = weights_from_workers
