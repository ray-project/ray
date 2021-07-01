import logging
from typing import Dict, Optional, Union, Callable

import pickle

from ray.tune import trial_runner
from ray.tune.schedulers.trial_scheduler import FIFOScheduler, TrialScheduler
from ray.tune.trial import Trial
from ray.tune.utils.placement_groups import PlacementGroupFactory

logger = logging.getLogger(__name__)


class ResourceChangingScheduler(FIFOScheduler):
    def __init__(
            self,
            base_scheduler: Optional[TrialScheduler] = None,
            resources_allocation_function: Optional[Callable] = None,
    ) -> None:
        FIFOScheduler.__init__(self)
        self._resources_allocation_function = resources_allocation_function
        self._base_scheduler = base_scheduler or FIFOScheduler()
        self._base_trial_resources = None
        self._trial_info = {}
        self._trials_to_reallocate = {}

    def set_search_properties(self, metric: Optional[str],
                              mode: Optional[str]) -> bool:
        return self._base_scheduler.set_search_properties(metric, mode)

    def on_trial_add(self, trial_runner: "trial_runner.TrialRunner",
                     trial: Trial, **kwargs):
        if self._base_trial_resources is None:
            self._base_trial_resources = trial.resources
        return self._base_scheduler.on_trial_add(trial_runner, trial, **kwargs)

    def on_trial_error(self, trial_runner: "trial_runner.TrialRunner",
                       trial: Trial, **kwargs):
        return self._base_scheduler.on_trial_error(trial_runner, trial,
                                                   **kwargs)

    def on_trial_complete(self, trial_runner: "trial_runner.TrialRunner",
                          trial: Trial, result: Dict, **kwargs):
        return self._base_scheduler.on_trial_complete(trial_runner, trial,
                                                      result, **kwargs)

    def on_trial_remove(self, trial_runner: "trial_runner.TrialRunner",
                        trial: Trial, **kwargs):
        return self._base_scheduler.on_trial_remove(trial_runner, trial,
                                                    **kwargs)

    def debug_string(self) -> str:
        return self._base_scheduler.debug_string()

    def on_trial_result(self, trial_runner: "trial_runner.TrialRunner",
                        trial: Trial, result: Dict) -> str:
        base_scheduler_decision = self._base_scheduler.on_trial_result(
            trial_runner, trial, result)
        if base_scheduler_decision == TrialScheduler.CONTINUE:
            should_reallocate = self.reallocate_trial_resources_if_needed(
                trial_runner, trial, result)
            if should_reallocate is None:
                return TrialScheduler.STOP
            if should_reallocate:
                # (yard1) this is set here to ensure that
                # the pg is destroyed when trial is paused
                trial.has_new_resources = True
                return TrialScheduler.PAUSE
        return base_scheduler_decision

    def choose_trial_to_run(self, trial_runner: "trial_runner.TrialRunner",
                            **kwargs) -> Optional[Trial]:
        for trial, new_resources in self._trials_to_reallocate.items():
            self.set_trial_resources(trial, new_resources)
        self._trials_to_reallocate.clear()
        return self._base_scheduler.choose_trial_to_run(trial_runner, **kwargs)

    def set_trial_resources(
            self, trial: Trial,
            resources: Union[Dict, Callable, PlacementGroupFactory]):
        if resources:
            logger.info(f"setting trial {trial} resource to {resources}")
            trial.placement_group_factory = None
            trial.update_resources(resources)

    def reallocate_trial_resources_if_needed(
            self, trial_runner: "trial_runner.TrialRunner", trial: Trial,
            result: Dict):
        if self._resources_allocation_function is None:
            return False
        new_resources = self._resources_allocation_function(
            trial_runner, trial, result, self._base_trial_resources)
        if new_resources:
            self._trials_to_reallocate[trial] = new_resources
            return True
        if new_resources is None:
            return None
        return False

    def save(self, checkpoint_path: str):
        save_object = self.__dict__
        with open(checkpoint_path, "wb") as outputFile:
            pickle.dump(save_object, outputFile)

    def restore(self, checkpoint_path: str):
        with open(checkpoint_path, "rb") as inputFile:
            save_object = pickle.load(inputFile)
        self.__dict__.update(save_object)
