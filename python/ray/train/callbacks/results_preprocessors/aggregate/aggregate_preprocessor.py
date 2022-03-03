import logging
from typing import Dict, List, Optional

import numpy as np

from ray.util.debug import log_once
from ray.util.annotations import DeveloperAPI
from ray.train.callbacks.results_preprocessors.preprocessor import ResultsPreprocessor
from ray.train.callbacks.results_preprocessors.aggregate.aggregate_fn import (
    AggregateFn,
    Average,
    Max,
    WeightedAverage,
)
from ray.train.callbacks.results_preprocessors.aggregate.aggregate_utils import (
    VALID_AGGREGATE_TYPES,
    _get_metrics_from_results,
)

logger = logging.getLogger(__name__)


@DeveloperAPI
class AggregateResultsPreprocessor(ResultsPreprocessor):
    """A preprocessor that aggregates training metrics from all workers.

    Args:
        aggregation_fn (AggregateFn):
            An aggregation method that performs the aggregation on results.
        keys (Optional[List[str]]):
            A list of keys reported in results to be aggregated. Keys should be saved
            using ``train.report()``.
    """

    def __init__(self, aggregation_fn: AggregateFn, keys: Optional[List[str]] = None):
        self.aggregate_fn = aggregation_fn
        self.keys = keys

    def preprocess(self, results: List[Dict]) -> List[Dict]:
        """Aggregate results before sending them to callbacks.

        Args:
            results List[Dict]: A list of results from all workers. The metrics
                specified in `keys` will be averaged according by `aggregation_fn`.
                Non-numerical values will be ignored.
        Returns:
            An updated results list with aggregated results.
        """
        results = [] if results is None else results
        if len(results) == 0:
            return results

        self.aggregate_fn.prepare(results)
        reported_metrics = {key for result in results for key in result.keys()}

        if self.keys is None:
            valid_keys = []
            for metric in reported_metrics:
                if all(
                    isinstance(result.get(metric, np.nan), VALID_AGGREGATE_TYPES)
                    for result in results
                ):
                    valid_keys.append(metric)
                elif log_once(metric):
                    logger.warning(
                        f"`{metric}` value type is not "
                        f"one of {VALID_AGGREGATE_TYPES}, so it is ignored. "
                    )

            self.keys = valid_keys

        aggregated_results = {}

        for key in self.keys:
            values = _get_metrics_from_results(key, results)
            if values is None:
                continue
            aggregated_results[self.aggregate_fn.wrap_key(key)] = self.aggregate_fn(
                values
            )

        # Currently we directly update each result dict with aggregated results.
        for result in results:
            result.update(aggregated_results)

        return results


class AverageResultsPreprocessor(AggregateResultsPreprocessor):
    """A preprocessor that averages results with equal weight.

    Args:
        keys (Optional[List[str]]): A list of metrics to be averaged.
            If None is specified, then the list will be populated by
            reported keys whose value type is valid, that is, one of
            ``VALID_AGGREGATE_TYPES``.

    Returns:
        An updated results list with average values.
    """

    def __init__(self, keys: Optional[List[str]] = None):
        super().__init__(Average(), keys)


class MaxResultsPreprocessor(AggregateResultsPreprocessor):
    """A preprocessor that averages results with equal weight.

    Args:
        keys (Optional[List[str]]): A list of metrics upon which
            the maximum value will be taken. If None is specified,
            then no maximum value will be reported.

    Returns:
        An updated results list with maximum values.
    """

    def __init__(self, keys: Optional[List[str]] = None):
        super().__init__(Max(), keys)


class WeightedAverageResultsPreprocessor(AggregateResultsPreprocessor):
    """A preprocessor that averages results with equal weight.

    Args:
        keys (Optional[List[str]]): A list of metrics to be averaged.
            If None is specified, then the list will be populated by
            reported keys whose value type is valid, that is, one of
            ``VALID_AGGREGATE_TYPES``.
        weight_key (Optional[str]): A a key from reported metrics that
            is used as the weight in averaging. If None is specified,
            then equal weight will be used.

    Returns:
        An updated results list with weighted average results.
    """

    def __init__(
        self, keys: Optional[List[str]] = None, weight_key: Optional[str] = None
    ):
        super().__init__(WeightedAverage(weight_key), keys)
