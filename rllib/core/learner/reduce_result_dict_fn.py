"""The following is set of default rllib reduction methods for ResultDicts"""

from typing import List

import numpy as np
import tree  # pip install dm-tree

from ray.rllib.utils.typing import ResultDict
from ray.rllib.utils.metrics import (
    NUM_AGENT_STEPS_SAMPLED,
    NUM_AGENT_STEPS_TRAINED,
    NUM_ENV_STEPS_SAMPLED,
    NUM_ENV_STEPS_TRAINED,
)


def _reduce_mean_results(results: List[ResultDict]) -> ResultDict:
    """Takes the average of all the leaves in the result dict

    Args:
        results: list of result dicts to average

    Returns:
        Averaged result dict
    """
    # A list of keys that must be summed up (instead of averaged over).
    KEYS_TO_SUM = set([
        NUM_AGENT_STEPS_SAMPLED,
        NUM_AGENT_STEPS_TRAINED,
        NUM_ENV_STEPS_SAMPLED,
        NUM_ENV_STEPS_TRAINED,
    ])

    return tree.map_structure_with_path(
        lambda path, *x: np.sum(x) if path[-1] in KEYS_TO_SUM else np.mean(x),
        *results,
    )
