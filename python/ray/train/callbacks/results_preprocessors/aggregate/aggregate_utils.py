import logging
from typing import Dict, List, Tuple, Union

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


def _get_metrics_from_results(
    key: str, results: List[Dict]
) -> List[Union[VALID_AGGREGATE_TYPES]]:
    """Return metrics values in the results list from all workers.

    Args:
        key (str): A key string. If it doesn't exist in results,
            i.e. it is not reported, the None will be returned.
        results (List[Dict]): The results list returned from workers.

    Returns:
        A list of valid key values from each worker, if key exists.
        Otherwise, None.
    """
    reported_metrics = {key for result in results for key in result.keys()}
    values = [result.get(key, np.nan) for result in results]
    warning_message = None
    if key not in reported_metrics:
        warning_message = (
            f"`{key}` is not reported from workers, so it is ignored. "
            "Please make sure that it is saved using `train.report()`."
        )
    elif not all(isinstance(value, VALID_AGGREGATE_TYPES) for value in values):
        warning_message = (
            f"`{key}` value type (`{type(values[0])}`) is not valid, "
            "so it is ignored. "
            f"Make sure that its type is one of {VALID_AGGREGATE_TYPES}. "
        )

    if warning_message:
        logger.warning(warning_message)
        return None

    return values


def _get_weights_from_results(
    key: str, results: List[Dict]
) -> List[Union[VALID_AGGREGATE_TYPES]]:
    """Return weight values in the results list from all workers.

    Args:
        key (str): A key string specifies the weight metric.
            If it doesn't exist in results, then equal weight
            will be used.
        results (List[Dict]): The results list returned from workers.

    Returns:
        A list of valid weight values from each worker, if key exists.
        Otherwise, a list of all ones, that is, equal weight.
    """
    reported_metrics = {key for result in results for key in result.keys()}
    weights = [result.get(key, np.nan) for result in results]
    warning_message = None
    if key not in reported_metrics:
        warning_message = (
            f"Averaging weight `{key}` is not reported in `train.report()`. "
        )
    elif not all(isinstance(value, VALID_AGGREGATE_TYPES) for value in weights):
        warning_message = (
            f"Averaging weight `{key}` value type (`{type(weights[0])}`) is not valid. "
            f"Make sure that its type is one of {VALID_AGGREGATE_TYPES}. "
        )

    if warning_message:
        logger.warning(warning_message + "Use equal weight instead.")
        weights = np.array([1] * len(results))

    return weights
