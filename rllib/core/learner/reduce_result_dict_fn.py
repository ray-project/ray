"""The following is set of default rllib reduction methods for ResultDicts"""

from typing import List
import numpy as np
import tree  # pip install dm-tree
from ray.rllib.utils.typing import ResultDict


def _reduce_mean_results(results: List[ResultDict]) -> ResultDict:
    """Takes the average of all the leaves in the result dict

    Args:
        results: list of result dicts to average

    Returns:
        Averaged result dict
    """

    return tree.map_structure(lambda *x: np.mean(x), *results)
