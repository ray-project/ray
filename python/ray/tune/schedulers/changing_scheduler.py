import logging
from typing import Dict, Optional, Union, Callable

import pickle

from ray.tune import trial_runner
from ray.tune.resources import Resources
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
        self._base_trial_resources: Optional[Union[
            Resources, PlacementGroupFactory]] = None
        self._trial_info = {}
        self._trials_to_reallocate = {}

    def set_search_properties(self, metric: Optional[str],
                              mode: Optional[str]) -> bool:
        return self._base_scheduler.set_search_properties(metric, mode)

    def on_trial_add(self, trial_runner: "trial_runner.TrialRunner",
                     trial: Trial, **kwargs):
        if self._base_trial_resources is None:
            if trial.uses_placement_groups:
                self._base_trial_resources = trial.placement_group_factory
            else:
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
            new_resources = self.reallocate_trial_resources_if_needed(
                trial_runner, trial, result)
            if new_resources:
                self._trials_to_reallocate[trial] = new_resources
                return TrialScheduler.PAUSE
        return base_scheduler_decision

    def choose_trial_to_run(self, trial_runner: "trial_runner.TrialRunner",
                            **kwargs) -> Optional[Trial]:
        any_resources_changed = False
        for trial, new_resources in self._trials_to_reallocate.items():
            any_resources_changed = self.set_trial_resources(
                trial, new_resources)
        self._trials_to_reallocate.clear()
        if any_resources_changed:
            trial_runner.trial_executor.force_reconcilation_on_next_step_end()
        trial = self._base_scheduler.choose_trial_to_run(
            trial_runner, **kwargs)
        return trial

    def set_trial_resources(
            self, trial: Trial,
            resources: Union[Dict, Callable, PlacementGroupFactory]) -> bool:
        if resources:
            logger.info(f"setting trial {trial} resource to {resources}")
            trial.placement_group_factory = None
            trial.update_resources(resources)
            return True
        return False

    def _check_if_resources_are_the_same(
            self,
            trial: Trial,
            new_resources,
    ) -> bool:
        if trial.uses_placement_groups:
            if (isinstance(new_resources, PlacementGroupFactory)
                    and trial.placement_group_factory == new_resources):
                return True
        return False

    def reallocate_trial_resources_if_needed(
            self, trial_runner: "trial_runner.TrialRunner", trial: Trial,
            result: Dict) -> Union[None, dict, PlacementGroupFactory]:
        if self._resources_allocation_function is None:
            return None

        new_resources = self._resources_allocation_function(
            trial_runner, trial, result, self._base_trial_resources)

        # if we can check if the new resources are the same,
        # we do that here and skip resource allocation
        if new_resources and not self._check_if_resources_are_the_same(
                trial, new_resources):
            return new_resources
        return None

    def save(self, checkpoint_path: str):
        save_object = self.__dict__
        with open(checkpoint_path, "wb") as outputFile:
            pickle.dump(save_object, outputFile)

    def restore(self, checkpoint_path: str):
        with open(checkpoint_path, "rb") as inputFile:
            save_object = pickle.load(inputFile)
        self.__dict__.update(save_object)
