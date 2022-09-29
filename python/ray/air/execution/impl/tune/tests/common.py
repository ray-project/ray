from collections import deque, Counter

from ray.tune import Callback
from ray.tune.experiment import Trial
from ray.tune.search import SearchAlgorithm
from typing import List


class SimpleSearchAlgorithm(SearchAlgorithm):
    def __init__(self):
        self._trials = deque()

    def add_trial(self, trial: Trial):
        self._trials.append(trial)

    def next_trial(self):
        if self._trials:
            return self._trials.popleft()

    def is_finished(self) -> bool:
        return not self._trials


class TrialStateCallback(Callback):
    def __init__(self):
        self._trial_status_counts = []

    def on_step_end(self, iteration: int, trials: List["Trial"], **info):
        counter = Counter()
        for trial in trials:
            counter[trial.status] += 1
        self._trial_status_counts.append(counter)

    def max_running_trials(self) -> int:
        return max(status[Trial.RUNNING] for status in self._trial_status_counts)

    def max_pending_trials(self) -> int:
        return max(status[Trial.PENDING] for status in self._trial_status_counts)

    def all_trials_terminated(self) -> bool:
        return (
            len(self._trial_status_counts[-1]) == 1
            and self._trial_status_counts[-1][Trial.TERMINATED] > 0
        )
