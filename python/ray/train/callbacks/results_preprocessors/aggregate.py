import logging
from typing import Dict, List, Optional

import numpy as np

from ray.train.callbacks.results_preprocessors.preprocessor import ResultsPreprocessor
from ray.train.callbacks.results_preprocessors.aggregate_fn import (
    AggregateFn,
    Average,
    Max,
    WeightedAverage,
    VALID_AGGREGATE_TYPES,
    _get_values_from_results,
)

logger = logging.getLogger(__name__)


class AggregateResultsPreprocessor(ResultsPreprocessor):
    """A preprocessor that aggregates training metrics from all workers.

    Args:
        aggregation_fn (AggregateFn):
            An aggregation method that performs the aggregation on results.
        keys (Optional[List[str]]):
            A list of keys reported in results to be aggregated. Keys should be saved
            using `train.report()`.
    """

    def __init__(self, aggregation_fn: AggregateFn, keys: Optional[List[str]] = None):
        self.aggregate_fn = aggregation_fn
        self.keys = keys

    def preprocess(self, results: List[Dict]) -> List[Dict]:
        """Average results before sending them to callbacks.

        Args:
            results List[Dict]: A list of results from all workers. The metrics
                specified in `keys` will be averaged according by `aggregation_fn`.
                Non-numerical values will be ignored.
        Returns:
            A updated list of results.
        """
        results = [] if results is None else results
        if len(results) == 0:
            return results

        self.aggregate_fn.prepare(results)

        reported_metrics = set(results[0].keys())
        if self.keys is None:
            valid_keys = []
            for metric in reported_metrics:
                if all(
                    isinstance(result.get(metric, np.nan), VALID_AGGREGATE_TYPES)
                    for result in results
                ):
                    valid_keys.append(metric)
            self.keys = valid_keys

        aggregated_results = {}

        for key in self.keys:
            values = _get_values_from_results(key, reported_metrics, results)
            if values is None:
                continue
            aggregated_results[self.aggregate_fn.wrap_key(key)] = self.aggregate_fn(
                values
            )

        for result in results:
            result.update(aggregated_results)

        return results


class AverageResultsPreprocessor(AggregateResultsPreprocessor):
    def __init__(self, keys: Optional[List[str]] = None):
        super().__init__(Average(), keys)


class MaxResultsPreprocessor(AggregateResultsPreprocessor):
    def __init__(self, keys: Optional[List[str]] = None):
        super().__init__(Max(), keys)


class WeightedAverageResultsPreprocessor(AggregateResultsPreprocessor):
    def __init__(
        self, keys: Optional[List[str]] = None, weight_key: Optional[str] = None
    ):
        super().__init__(WeightedAverage(weight_key), keys)
