import logging
from typing import Dict, Optional, Union, Callable

import pickle
import warnings

from ray.tune import trial_runner
from ray.tune.resources import Resources
from ray.tune.schedulers.trial_scheduler import FIFOScheduler, TrialScheduler
from ray.tune.trial import Trial
from ray.tune.utils.placement_groups import PlacementGroupFactory

logger = logging.getLogger(__name__)


class ResourceChangingScheduler(TrialScheduler):
    """A utility scheduler to dynamically change resources of live trials.

    Experimental. API may change in future releases.

    The ResourceChangingScheduler works by wrapping around any other
    scheduler and adjusting the resource requirements of live trials
    in response to the decisions of the wrapped scheduler
    through a user-specified `resources_allocation_function`. An example
    of such a function can be found in
    `tune/examples/xgboost_dynamic_resources_example.py`.

    Only supports the Trainable (class) API. If resources of a trial are
    updated with new values, the `update_resources` method in the Trainable
    will be called. This method needs to be overwritten by the user in
    order to let the trained model take advantage of newly allocated resources.

    Args:
        base_scheduler (TrialScheduler): The scheduler to provide decisions
            about trials. If None, a default FIFOScheduler will be used.
        resources_allocation_function (Callable): The function used to change
            live trial resource requiements during tuning. This function
            will be called on each trial as it finishes one step of training.
            The function must take four arguments: `TrialRunner`, current
            `Trial`, current result `dict` and the base trial resource
            `PlacementGroupFactory` or `Resource` (depending on whether
            placement groups are used). The function must return a
            `PlacementGroupFactory`, `Resources`,`dict` or None (signifying
            no need for an update). If `resources_allocation_function` is
            None, no resource requirements will be changed at any time.

    Warning:
        If the `resources_allocation_function` sets trial resource requirements
        to values bigger than possible, the trial will not run. Ensure
        that your function accounts for that possibility by setting upper
        limits. Consult the example file to see how that may be done.

    Example:
        >>> base_scheduler = ASHAScheduler(max_t=16)
        >>> def resource_allocation_function(
        >>>     trial_runner: "trial_runner.TrialRunner",
        >>>     trial: Trial,
        >>>     result: dict,
        >>>     base_trial_resource: Union[PlacementGroupFactory|Resource]
        >>> ) -> Union[None, PlacementGroupFactory, Resource]:
        >>>     # logic here
        >>>     # usage of PlacementGroupFactory is strongly preferred
        >>>     return PlacementGroupFactory(...)
        >>> scheduler = ResourceChangingScheduler(base_scheduler,
        >>>                                     resource_allocation_function)

        See `tune/examples/xgboost_dynamic_resources_example.py` for a
        more detailed example.
    """

    def __init__(
            self,
            base_scheduler: Optional[TrialScheduler] = None,
            resources_allocation_function: Optional[Callable] = None,
    ) -> None:
        super().__init__()
        if resources_allocation_function is None:
            warnings.warn(
                "resources_allocation_function is None. No resource "
                "requirements will be changed at any time. Pass a "
                "correctly defined function to enable functionality.")
        self._resources_allocation_function = resources_allocation_function
        self._base_scheduler = base_scheduler or FIFOScheduler()
        self._base_trial_resources: Optional[Union[
            Resources, PlacementGroupFactory]] = None
        self._trials_to_reallocate: Dict[Trial, Union[
            None, dict, PlacementGroupFactory]] = {}

    @property
    def metric(self):
        return self._base_scheduler._metric

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
        any_resources_changed = False

        for trial, new_resources in self._trials_to_reallocate.items():
            resources_changed = self.set_trial_resources(trial, new_resources)
            any_resources_changed = any_resources_changed or resources_changed
        self._trials_to_reallocate.clear()

        if any_resources_changed:
            # force reconcilation to ensure resource changes
            # are implemented right away
            trial_runner.trial_executor.force_reconcilation_on_next_step_end()

        trial = self._base_scheduler.choose_trial_to_run(
            trial_runner, **kwargs)
        return trial

    def debug_string(self) -> str:
        return ("(ResourceChangingScheduler) "
                f"{self._base_scheduler.debug_string()}")

    def save(self, checkpoint_path: str):
        save_object = self.__dict__
        with open(checkpoint_path, "wb") as outputFile:
            pickle.dump(save_object, outputFile)

    def restore(self, checkpoint_path: str):
        with open(checkpoint_path, "rb") as inputFile:
            save_object = pickle.load(inputFile)
        self.__dict__.update(save_object)

    def set_trial_resources(
            self, trial: Trial,
            new_resources: Union[Dict, Callable, PlacementGroupFactory]
    ) -> bool:
        """Returns True if new_resources were set."""
        if new_resources:
            logger.info(f"setting trial {trial} resource to {new_resources}")
            trial.placement_group_factory = None
            trial.update_resources(new_resources)
            return True
        return False

    def _are_resources_the_same(
            self,
            trial: Trial,
            new_resources,
    ) -> bool:
        """Returns True if trial's resources are value equal to new_resources.

        Only checks for PlacementGroupFactories at this moment.
        """
        if trial.uses_placement_groups:
            if (isinstance(new_resources, PlacementGroupFactory)
                    and trial.placement_group_factory == new_resources):
                logger.debug(
                    f"{trial} PGF "
                    f"{trial.placement_group_factory.required_resources}"
                    f" and {new_resources.required_resources}"
                    f" are the same, skipping")
                return True
        return False

    def reallocate_trial_resources_if_needed(
            self, trial_runner: "trial_runner.TrialRunner", trial: Trial,
            result: Dict) -> Union[None, dict, PlacementGroupFactory]:
        """Calls user defined resources_allocation_function. If the returned
        resources are not none and not the same as currently present, returns
        them. Otherwise, returns None."""
        if self._resources_allocation_function is None:
            return None

        new_resources = self._resources_allocation_function(
            trial_runner, trial, result, self._base_trial_resources)

        # if we can check if the new resources are the same,
        # we do that here and skip resource allocation
        if new_resources and not self._are_resources_the_same(
                trial, new_resources):
            return new_resources
        return None
