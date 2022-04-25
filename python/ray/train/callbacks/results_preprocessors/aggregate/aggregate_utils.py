import logging
from typing import Dict, List, Optional, Tuple, Union

import numpy as np

from ray.util.debug import log_once

VALID_AGGREGATE_TYPES: Tuple[type] = (
    int,
    float,
    np.float32,
    np.float64,
    np.int32,
    np.int64,
)

logger = logging.getLogger(__name__)


def _check_if_key_is_reported(key: str, results: List[Dict]) -> bool:
    """Check if a particular key is reported by some workers.

    Args:
        key (str): A key string.
        results (List[Dict]): The results list returned from workers.

    Returns:
        A boolean value. True if ``key`` exists in some worker's result dict.
        Otherwise, False.
    """
    return key in {key for result in results for key in result.keys()}


def _check_if_any_value_is_valid(key: str, results: List[Dict]) -> bool:
    """Check if some values of ``key`` are valid types.

    Args:
        key (str): A key string.
        results (List[Dict]): The results list returned from workers.

    Returns:
        A boolean value. True if some values of ``key`` are one of
        ``VALID_AGGREGATE_TYPES``. Otherwise, False.
    """
    values = [result.get(key, np.nan) for result in results]
    return any(isinstance(value, VALID_AGGREGATE_TYPES) for value in values)


def _get_valid_values_from_results(
    key: str, results: List[Dict]
) -> List[Union[VALID_AGGREGATE_TYPES]]:
    """Get the list of values specified by ``key``.

    Args:
        key (str): A key string.
        results (List[Dict]): The results list returned from workers.

    Returns:
        A list of values specified by ``key``. Invalid values are
        replaced by ``np.nan``. This should be called after
        ``_check_if_any_value_is_valid()`` returns True.
    """
    return [
        (
            result.get(key, np.nan)
            if isinstance(result.get(key, np.nan), VALID_AGGREGATE_TYPES)
            else np.nan
        )
        for result in results
    ]


def _get_metrics_from_results(
    key: str, results: List[Dict]
) -> Optional[List[Union[VALID_AGGREGATE_TYPES]]]:
    """Return the metric values specified by ``key`` from each worker's result dict.

    Args:
        key (str): A key string specifies the metric.
        results (List[Dict]): The results list returned from workers.

    Returns:
        A list of values for ``key`` from each worker. If ``key`` is
        missing in every result dict, or if ``key`` is not a valid
        type in each result dict, then it will return None. If some
        workers report valid ``key`` values but other don't, a list
        of values will still be returned and invalid values are
        replaced by ``np.nan``.
    """
    warning_message = None
    if not _check_if_key_is_reported(key, results):
        warning_message = (
            f"`{key}` is not reported from workers, so it is ignored. "
            "Please make sure that it is saved using `train.report()`."
        )
    elif not _check_if_any_value_is_valid(key, results):
        warning_message = (
            f"`{key}` value type is not valid, so it is ignored. "
            f"Make sure that its type is one of {VALID_AGGREGATE_TYPES}. "
        )

    if warning_message:
        if log_once(key):
            logger.warning(warning_message)
        return None

    return _get_valid_values_from_results(key, results)


def _get_weights_from_results(
    key: str, results: List[Dict]
) -> List[Union[VALID_AGGREGATE_TYPES]]:
    """Return weight values specified by ``key`` from all workers.

    Args:
        key (str): A key string specifies the weight metric.
        results (List[Dict]): The results list returned from workers.

    Returns:
        A list of valid weight values from each worker, if key exists.
        Invalid values are replaced by ``np.nan`` and will be ignored
        in the subsequent weighted average aggregation. If ``key``
        doesn't exist in every single result or its value from every
        single worker is invalid, then equal weight will be used.
        That is, a list of all ones.
    """
    warning_message = None
    if not _check_if_key_is_reported(key, results):
        warning_message = (
            f"Averaging weight `{key}` is not reported "
            "by all workers in `train.report()`. "
        )
    elif not _check_if_any_value_is_valid(key, results):
        warning_message = (
            f"Averaging weight `{key}` value type is not valid. "
            f"Make sure that its type is one of {VALID_AGGREGATE_TYPES}. "
        )

    if warning_message:
        if log_once(key):
            logger.warning(warning_message + "Use equal weight instead.")
        return [1] * len(results)

    return _get_valid_values_from_results(key, results)
