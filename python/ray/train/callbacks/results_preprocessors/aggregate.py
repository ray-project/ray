import logging
from typing import Any, Callable, Dict, List, Tuple, Optional, Set, Union

import numpy as np

from ray.train.callbacks.results_preprocessors.preprocessor import ResultsPreprocessor
from ray.train.constants import (
    TIME_THIS_ITER_S,
    TRAINING_ITERATION,
)

logger = logging.getLogger(__name__)


class AggregateResultsPreprocessor(ResultsPreprocessor):
    """A preprocessor that aggregates training metrics from all workers.

    Args:
        aggregators (Optional[Dict[str, Optional[Tuple|Callable[[List], Any]]]]):
            A Dict of aggregation methods. The key is the name of the aggregated
            metric. The value is the aggregation method. If it is a function, the
            aggregation will be performed by calling this function. If the value
            is a tuple (var1, var2), var1 specifies which metrics to be aggregated,
            and var2 is the averaging weights. When var2 is None, the weight will
            be taken to be equal for all workers. var1 and var2 are expected to be
            reported using `train.report()`.
        aggregate_default_metrics (bool): If `aggregate_default_metrics` is True,
            a list of default metrics will be aggregated. Set it to False otherwise.
    """

    VALID_SUMMARY_TYPES: Tuple[type] = (
        int,
        float,
        np.float32,
        np.float64,
        np.int32,
        np.int64,
    )
    DEFAULT_KEYS: Set[str] = {TIME_THIS_ITER_S, TRAINING_ITERATION}

    def __init__(
        self,
        aggregators: Optional[
            Dict[str, Optional[Union[Tuple, Callable[[List], Any]]]]
        ] = None,
        aggregate_default_metrics: bool = True,
    ):
        self.aggregators = {} if aggregators is None else aggregators
        if aggregate_default_metrics:
            self.aggregators.update(
                {
                    "_aggregated" + metrics: (metrics, None)
                    for metrics in self.DEFAULT_KEYS
                }
            )

    def preprocess(self, results: List[Dict] = None) -> List[Dict]:
        """Average results before sending them to callbacks.

        Args:
            results List[Dict]: A list of results from all workers. The metrics
                specified in `metrics_to_average` will be averaged according to
                their weights. Non-numerical values will be ignored.
        Returns:
            A updated list of results.
        """
        results = [] if results is None else results
        if len(results) == 0 or len(self.aggregators) == 0:
            return results

        reported_metrics = set(results[0].keys())
        aggregated_metrics = {}

        for name, aggregator in self.aggregators.items():

            if callable(aggregator):
                aggregated_metrics[name] = aggregator(results)
            else:
                aggregated_metrics[name] = self._aggregate_metrics(
                    *aggregator, results, reported_metrics
                )

        for result in results:
            result.update(aggregated_metrics)

        return results

    def _aggregate_metrics(self, metrics, weight, results, reported_metrics):

        if metrics not in reported_metrics:
            logger.warning(
                f"`{metrics}` is not reported from workers, so it is ignored. "
                "Please make sure that it is saved using `train.report()`."
            )
            return
        elif not all(
            isinstance(result.get(metrics, np.nan), self.VALID_SUMMARY_TYPES)
            for result in results
        ):
            logger.warning(
                f"`{metrics}` value type is not valid, so it is ignored. "
                f"Make sure that its type is one of {self.VALID_SUMMARY_TYPES}."
            )
            return

        metrics_from_workers = np.array(
            [result.get(metrics, np.nan) for result in results]
        )

        if weight in reported_metrics and all(
            isinstance(result.get(weight, np.nan), self.VALID_SUMMARY_TYPES)
            for result in results
        ):
            weights_from_workers = np.array(
                [result.get(weight, np.nan) for result in results]
            )
        else:
            if weight not in reported_metrics:
                message = (
                    f"Averaging weight `{weight}` is not reported in `train.report()`. "
                )
            else:
                message = (
                    f"Averaging weight `{weight}` value type is not valid. "
                    f"Make sure that its type is one of {self.VALID_SUMMARY_TYPES}. "
                )

            # if no weight is provided, equal weight will be used.
            logger.warning(message + "Use equal weight instead.")
            weights_from_workers = np.array([1] * len(metrics_from_workers))

        return np.nanmean(
            metrics_from_workers
            * weights_from_workers
            / np.nansum(weights_from_workers)
        )
