from collections import deque, Counter

from ray.air.experimental.execution.impl.tune.tune_controller import TuneController
from ray.air.experimental.execution.resources.fixed import FixedResourceManager
from ray.air.experimental.execution.resources.resource_manager import ResourceManager
from ray.tune import Callback
from ray.tune.experiment import Trial
from ray.tune.schedulers import FIFOScheduler, TrialScheduler
from ray.tune.search import SearchAlgorithm
from typing import List, Optional, Dict


def tune_setup(
    resource_manager: Optional[ResourceManager] = None,
    search_alg: Optional[SearchAlgorithm] = None,
    scheduler: Optional[TrialScheduler] = None,
    callbacks: Optional[List[Callback]] = None,
):
    resource_manager = resource_manager or FixedResourceManager(
        total_resources={"CPU": 4, "GPU": 2}
    )
    search_alg = search_alg or SimpleSearchAlgorithm()
    scheduler = scheduler or SimpleScheduler()
    trial_states = TrialStateCallback()
    callbacks = callbacks or []
    controller = TuneController(
        search_alg=search_alg,
        scheduler=scheduler,
        resource_manager=resource_manager,
        callbacks=callbacks + [trial_states],
    )

    return resource_manager, search_alg, scheduler, trial_states, controller


class SimpleSearchAlgorithm(SearchAlgorithm):
    _max_pending_trials = None

    def __init__(self):
        self._trials = deque()
        self.errored_trials = set()

    def add_trial(self, trial: Trial):
        self._trials.append(trial)

    def next_trial(self):
        if self._trials:
            return self._trials.popleft()

    def is_finished(self) -> bool:
        return not self._trials

    def on_trial_complete(
        self, trial_id: str, result: Optional[Dict] = None, error: bool = False
    ):
        if error:
            self.errored_trials.add(trial_id)


class SimpleScheduler(FIFOScheduler):
    def __init__(self):
        super(SimpleScheduler, self).__init__()
        self.errored_trials = set()

    def on_trial_error(self, trial_runner, trial: Trial):
        self.errored_trials.add(trial.trial_id)


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
