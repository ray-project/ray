"""The following is set of default rllib reduction methods for ResultDicts"""

from typing import List, Optional, Set

import numpy as np
import tree  # pip install dm-tree

from ray.util.annotations import PublicAPI
from ray.rllib.utils.typing import ResultDict
from ray.rllib.utils.metrics import (
    NUM_AGENT_STEPS_SAMPLED,
    NUM_AGENT_STEPS_TRAINED,
    NUM_ENV_STEPS_SAMPLED,
    NUM_ENV_STEPS_TRAINED,
)


# A set of string keys that must be summed up (instead of averaged over) in the
# `reduce_results` utility function by default.
DEFAULT_KEYS_TO_SUM = {
    NUM_AGENT_STEPS_SAMPLED,
    NUM_AGENT_STEPS_TRAINED,
    NUM_ENV_STEPS_SAMPLED,
    NUM_ENV_STEPS_TRAINED,
}


@PublicAPI(stability="beta")
def reduce_results(
    results: List[ResultDict],
    keys_to_sum: Optional[Set[str]] = None,
) -> ResultDict:
    """Takes the mean (or sum) of all the leaves in the result dict.

    Args:
        results: List of result dicts to reduce mean or -sum.
        keys_to_sum: An optional set of keys for which to reduce sum (instead of reduce
            mean). By default, we'll use `DEFAULT_KEYS_TO_SUM`. Keys not found in
            `keys_to_sum` will be mean reduced.

    Returns:
        Reduced (mean or sum, depending on value of keys) result dict.
    """
    keys_to_sum = keys_to_sum or DEFAULT_KEYS_TO_SUM

    return tree.map_structure_with_path(
        lambda path, *x: np.sum(x) if path[-1] in keys_to_sum else np.mean(x),
        *results,
    )
