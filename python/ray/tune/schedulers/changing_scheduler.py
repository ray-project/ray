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
            base_scheduler: TrialScheduler,
            resources_allocation_function: Optional[Callable] = None,
    ) -> None:
        FIFOScheduler.__init__(self)
        self._resources_allocation_function = resources_allocation_function
        self._base_scheduler = base_scheduler
        self._base_trial_resources = None
        self._trial_info = {}

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

    def choose_trial_to_run(self, trial_runner: "trial_runner.TrialRunner",
                            **kwargs) -> Optional[Trial]:
        return self._base_scheduler.choose_trial_to_run(trial_runner, **kwargs)

    def debug_string(self) -> str:
        return self._base_scheduler.debug_string()

    def on_trial_result(self, trial_runner: "trial_runner.TrialRunner",
                        trial: Trial, result: Dict) -> str:
        base_scheduler_decision = self._base_scheduler.on_trial_result(
            trial_runner, trial, result)
        if (base_scheduler_decision == TrialScheduler.CONTINUE
                and not trial.has_new_resources):
            new_resource = self.should_change_trial_resources(
                trial_runner, trial, result)
            if new_resource is None:
                return TrialScheduler.STOP
            if new_resource is not False:
                self.set_trial_resources(trial_runner, trial, new_resource)
        return base_scheduler_decision

    def set_trial_resources(
            self, trial_runner: "trial_runner.TrialRunner", trial: Trial,
            resources: Union[Dict, Callable, PlacementGroupFactory]):
        print(f"setting trial {trial} resource to {resources}")
        trial_runner.update_trial_resources(trial, resources)

    def should_change_trial_resources(self,
                                      trial_runner: "trial_runner.TrialRunner",
                                      trial: Trial, result: Dict):
        if self._resources_allocation_function is None:
            return False
        return self._resources_allocation_function(trial_runner, trial, result,
                                                   self._base_trial_resources)

    def save(self, checkpoint_path: str):
        save_object = self.__dict__
        with open(checkpoint_path, "wb") as outputFile:
            pickle.dump(save_object, outputFile)

    def restore(self, checkpoint_path: str):
        with open(checkpoint_path, "rb") as inputFile:
            save_object = pickle.load(inputFile)
        self.__dict__.update(save_object)
