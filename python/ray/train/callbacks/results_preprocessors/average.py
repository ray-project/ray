import logging
from typing import Dict, List, Tuple

import numpy as np

from ray.train.callbacks.results_preprocessors.preprocessor import ResultsPreprocessor


logger = logging.getLogger(__name__)


class AverageResultsPreprocessor(ResultsPreprocessor):
    """A preprocessor that average training metrics from all workers.

    Args:
        metrics_to_average (Dict): A Dict of metrics in results to average.
            The key is the metric to be averaged across all workers. The value
            is the magic key in the results that will be used as weights. If
            the value is None, the weight will be taken to be equal for all
            workers. Both key and value should be reported using `train.report()`.
    """

    VALID_SUMMARY_TYPES: Tuple[type] = (
        int,
        float,
        np.float32,
        np.float64,
        np.int32,
        np.int64,
    )

    def __init__(self, metrics_to_average: Dict = None):
        self.metrics_to_average = (
            {} if metrics_to_average is None else metrics_to_average
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
        if len(results) == 0 or len(self.metrics_to_average) == 0:
            return results

        reported_metrics = set(results[0].keys())
        average_metrics = {}
        for metrics, weight in self.metrics_to_average.items():

            if metrics not in reported_metrics:
                logger.warning(
                    f"`{metrics}` is not reported from workers, so it is ignored. "
                    "Please make sure that it is saved using `train.report()`."
                )
                continue
            elif not all(
                isinstance(result.get(metrics, np.nan), self.VALID_SUMMARY_TYPES)
                for result in results
            ):
                logger.warning(
                    f"`{metrics}` value type is not valid, so it is ignored. "
                    f"Make sure that its type is one of {self.VALID_SUMMARY_TYPES}."
                )
                continue

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
                    message = f"Averaging weight `{weight}` is not reported in `train.report()`. "
                else:
                    message = (
                        f"Averaging weight `{weight}` value type is not valid. "
                        f"Make sure that its type is one of {self.VALID_SUMMARY_TYPES}. "
                    )

                # if no weight is provided, equal weight will be used.
                logger.warning(message + "Use equal weight instead.")
                weights_from_workers = np.array([1] * len(metrics_from_workers))

            average_metrics["_average_" + metrics] = np.nanmean(
                metrics_from_workers
                * weights_from_workers
                / np.nansum(weights_from_workers)
            )

        for result in results:
            result.update(average_metrics)

        return results
