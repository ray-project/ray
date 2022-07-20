import logging
from typing import Dict, List, Optional
import warnings

from ray.util.annotations import Deprecated
from ray.train._internal.results_preprocessors.preprocessor import ResultsPreprocessor
from ray.train._internal.results_preprocessors.aggregate.aggregate_fn import (
    AggregateFn,
    Average,
    Max,
    WeightedAverage,
    _deprecation_msg,
)
from ray.train._internal.results_preprocessors.aggregate.aggregate_utils import (
    _get_metrics_from_results,
)

logger = logging.getLogger(__name__)


@Deprecated
class AggregateResultsPreprocessor(ResultsPreprocessor):
    """A preprocessor that aggregates training metrics from all workers.

    Args:
        aggregation_fn (AggregateFn):
            An aggregation method that performs the aggregation on results.
        keys (Optional[List[str]]):
            A list of keys reported in results to be aggregated. Keys should
            be saved using ``train.report()``. If a key is invalid or not
            reported, it will be ignored.
    """

    def __init__(self, aggregation_fn: AggregateFn, keys: Optional[List[str]] = None):
        warnings.warn(
            _deprecation_msg,
            DeprecationWarning,
            stacklevel=2,
        )
        self.aggregate_fn = aggregation_fn
        self.keys = keys

    def preprocess(self, results: Optional[List[Dict]] = None) -> Optional[List[Dict]]:
        """Aggregate results before sending them to callbacks.

        A key will be ignored if one of the following occurs:
        1. No worker reports it.
        2. The values returned from all workers are invalid.
        The aggregation WILL be performed even if some but not all
        workers report the key with valid values. The aggregation
        will only applied to those that report the key.

        Args:
            results (Optional[List[Dict]]): A list of results
                from all workers. The metrics specified in ``keys``
                will be averaged according to ``aggregation_fn``.

        Returns:
            An updated results list that has aggregated results and
            is of the same length as the input list.
        """
        if results is None or len(results) == 0:
            return results

        self.aggregate_fn.prepare(results)

        keys_to_aggregate = (
            self.keys
            if self.keys
            else {key for result in results for key in result.keys()}
        )

        aggregated_results = {}

        for key in keys_to_aggregate:
            values = _get_metrics_from_results(key, results)
            if values:
                aggregated_results[self.aggregate_fn.wrap_key(key)] = self.aggregate_fn(
                    values
                )

        # Currently we directly update each result dict with aggregated results.
        for result in results:
            result.update(aggregated_results)

        return results


@Deprecated
class AverageResultsPreprocessor(AggregateResultsPreprocessor):
    """A preprocessor that averages results with equal weight.

    .. code-block:: python

        preprocessor = AverageResultsPreprocessor(keys=["loss", "accuracy"])
        update_results = preprocessor.preprocess(results)


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


@Deprecated
class MaxResultsPreprocessor(AggregateResultsPreprocessor):
    """A preprocessor that computes maximum values of specified keys.

    .. code-block:: python

        preprocessor = MaxResultsPreprocessor(keys=["loss", "accuracy"])
        update_results = preprocessor.preprocess(results)


    Args:
        keys (Optional[List[str]]): A list of metrics upon which the
            maximum value will be taken. If None is specified, then
            the list will be populated by reported keys whose value type
            is valid, that is, one of ``VALID_AGGREGATE_TYPES``.

    Returns:
        An updated results list with maximum values.
    """

    def __init__(self, keys: Optional[List[str]] = None):
        super().__init__(Max(), keys)


@Deprecated
class WeightedAverageResultsPreprocessor(AggregateResultsPreprocessor):
    """A preprocessor that performs weighted average over metrics.


    .. code-block:: python

        preprocessor = WeightedAverageResultsPreprocessor(keys=["loss", "accuracy"],
                                                          weight_key="batch_size")
        update_results = preprocessor.preprocess(results)

    Args:
        keys (Optional[List[str]]): A list of metrics to be averaged.
            If None is specified, then the list will be populated by
            reported keys whose value type is valid, that is, one of
            ``VALID_AGGREGATE_TYPES``.
        weight_key (Optional[str]): A a key from reported metrics that
            will be used as the weight in averaging. If None is specified,
            then equal weight will be used.

    Returns:
        An updated results list with weighted average results.
    """

    def __init__(
        self, keys: Optional[List[str]] = None, weight_key: Optional[str] = None
    ):
        super().__init__(WeightedAverage(weight_key), keys)
