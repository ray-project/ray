from typing import Dict
from collections import defaultdict

from ray.util.annotations import PublicAPI
from ray.tune.stopper.stopper import Stopper


@PublicAPI
class MaximumIterationStopper(Stopper):
    """Stop trials after reaching a maximum number of iterations

    Args:
        max_iter: Number of iterations before stopping a trial.
    """

    def __init__(self, max_iter: int):
        self._max_iter = max_iter
        self._iter = defaultdict(lambda: 0)

    def __call__(self, trial_id: str, result: Dict):
        self._iter[trial_id] += 1
        return self._iter[trial_id] >= self._max_iter

    def stop_all(self):
        return False
