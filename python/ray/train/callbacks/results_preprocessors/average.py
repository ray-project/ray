from typing import Dict, List, Tuple

import numpy as np

from ray.train.callbacks.results_preprocessors.preprocessor import ResultsPreprocessor


class AverageResultsPreprocessor(ResultsPreprocessor):
    """A preprocessor that average training metrics from all workers.

    Args:
        metrics_to_average (Dict): A Dict of metrics in results to average.
            The key is the metric to be averaged across all workers. The value
            is the magic key in the results that will be used as weights. If
            the value is None, the weight will be taken to be equal for all
            workers.
    """

    VALID_SUMMARY_TYPES: Tuple[type] = (
        int,
        float,
        np.float32,
        np.float64,
        np.int32,
        np.int64,
    )

    def __init__(self, metrics_to_average: Dict = {}):
        self.metrics_to_average = metrics_to_average

    def preprocess(self, results: List[Dict] = []) -> List[Dict]:
        """Average results before sending them to callbacks.

        Args:
            results List[Dict]: A list of results from all workers. The metrics
                specified in `metrics_to_average` will be averaged according to
                their weights. Non-numerical values will be ignored.
        Returns:
            A updated list of results.
        """
        if len(results) == 0 or len(self.metrics_to_average) == 0:
            return results

        average_metrics = {}
        for metrics, weight in self.metrics_to_average.items():

            if not isinstance(results[0][metrics], self.VALID_SUMMARY_TYPES):
                continue

            metrics_from_workers = np.array(
                [result[metrics] for result in results if not np.isnan(result[metrics])]
            )
            if weight:
                weights_from_workers = np.array(
                    [
                        result[weight]
                        for result in results
                        if not np.isnan(result[metrics])
                    ]
                )
            else:
                # if no weight is provided, equal weight will be used.
                weights_from_workers = np.array([1] * len(metrics_from_workers))

            average_metrics["_average_" + metrics] = np.nanmean(
                metrics_from_workers
                * weights_from_workers
                / np.sum(weights_from_workers)
            )

        for result in results:
            result.update(average_metrics)

        return results
