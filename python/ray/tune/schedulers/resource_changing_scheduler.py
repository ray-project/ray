import logging
from typing import Dict, Any, Optional, Set, Union, Callable

import pickle
import warnings
import math

from ray.tune import trial_runner
from ray.tune.resources import Resources
from ray.tune.schedulers.trial_scheduler import FIFOScheduler, TrialScheduler
from ray.tune.trial import Trial
from ray.tune.utils.placement_groups import PlacementGroupFactory

logger = logging.getLogger(__name__)


def evenly_distribute_cpus_gpus(
        trial_runner: "trial_runner.TrialRunner", trial: Trial,
        result: Dict[str, Any],
        base_trial_resource: Union[PlacementGroupFactory, Resources]
) -> Union[None, PlacementGroupFactory, Resources]:
    """This is a basic resource allocating function.

    This function is used by default in ``ResourceChangingScheduler``.

    The function naively balances free resources (CPUs and GPUs) between
    trials, giving them all equal priority, ensuring that all resources
    are always being used. If for some reason a trial ends up with
    more resources than there are free ones, it will adjust downwards.

    It will also ensure that trial as at least as many resources as
    it started with (``base_trial_resource``).

    This function returns a new ``PlacementGroupFactory`` with updated
    resource requirements, or None. If the returned
    ``PlacementGroupFactory`` is equal by value to the one the
    trial has currently, the scheduler will skip the update process
    internally (same with None).

    Args:
        trial_runner (TrialRunner): Trial runner for this Tune run.
            Can be used to obtain information about other trials.
        trial (Trial): The trial to allocate new resources to.
        result (Dict[str, Any]): The latest results of trial.
        base_trial_resource (Union[PlacementGroupFactory, Resources]):
            Base trial resources as defined in
            ``tune.run(resources_per_trial)``
    """

    if not isinstance(base_trial_resource, PlacementGroupFactory):
        raise ValueError("evenly_distribute_cpus_gpus only supports"
                         " PlacementGroupFactories.")

    # Don't bother if this is just the first iteration
    if result["training_iteration"] < 1:
        return None

    # default values if resources_per_trial is unspecified
    if base_trial_resource is None:
        base_trial_resource = PlacementGroupFactory([{"CPU": 1, "GPU": 0}])

    # Assume that the number of CPUs and GPUs can't go below
    # what was specified in tune.run
    min_cpu = base_trial_resource.required_resources.get("CPU", 0)
    min_gpu = base_trial_resource.required_resources.get("GPU", 0)

    # Get the number of CPUs and GPUs avaialble in total (not just free)
    total_available_cpus = (trial_runner.trial_executor._avail_resources.cpu)
    total_available_gpus = (trial_runner.trial_executor._avail_resources.gpu)

    # Set upper limits for resources based on number of live trials
    # to ensure that the trial cannot get more resources that it's
    # possible to run
    if min_cpu == 0:
        upper_cpu_limit = 0
    else:
        upper_cpu_limit = math.ceil(total_available_cpus / len(
            trial_runner.get_live_trials()) / min_cpu)

    if min_gpu == 0:
        upper_gpu_limit = 0
    else:
        upper_gpu_limit = math.ceil(total_available_gpus / len(
            trial_runner.get_live_trials()) / min_gpu)

    # Function to check how many CPUs and GPUs a trial is using currently
    def get_used_cpus_and_gpus(t: Trial):
        return (t.placement_group_factory.required_resources.get("CPU", 0),
                t.placement_group_factory.required_resources.get("GPU", 0))

    # Check how many CPUs and GPUs are currently being used by this trial
    trial_used_cpus, trial_used_gpus = get_used_cpus_and_gpus(trial)

    # Check how many CPUs and GPUs are currently being used by live trials
    used_cpus_and_gpus = [
        get_used_cpus_and_gpus(t) for t in trial_runner.get_live_trials()
    ]
    used_cpus, used_gpus = zip(*used_cpus_and_gpus)
    used_cpus = sum(used_cpus)
    used_gpus = sum(used_gpus)

    # Calculate how many free CPUs and GPUs there are
    free_cpus = total_available_cpus - used_cpus
    free_gpus = total_available_gpus - used_gpus

    # Add free CPUs and GPUs enforcing upper and lower limits
    new_cpu = min(upper_cpu_limit, max(trial_used_cpus + free_cpus, min_cpu))
    new_gpu = min(upper_gpu_limit, max(trial_used_gpus + free_gpus, min_gpu))

    # Assign new CPUs and GPUs to the trial in a PlacementGroupFactory
    return PlacementGroupFactory([{"CPU": new_cpu, "GPU": new_gpu}])


class ResourceChangingScheduler(TrialScheduler):
    """A utility scheduler to dynamically change resources of live trials.

    .. versionadded:: 1.5.0

    .. note::
        Experimental. API may change in future releases.

    The ResourceChangingScheduler works by wrapping around any other
    scheduler and adjusting the resource requirements of live trials
    in response to the decisions of the wrapped scheduler
    through a user-specified ``resources_allocation_function``.
    An example of such a function can be found in
    :doc:`/tune/examples/xgboost_dynamic_resources_example`.

    Currently, only supports the Trainable (class) API. If resources of a
    trial are updated with new values, the ``update_resources`` method in
    the Trainable will be called. This method needs to be overwritten by the
    user in order to let the trained model take advantage of newly allocated
    resources.

    Cannot be used if ``reuse_actors`` is True in ``tune.run``. A ValueError
    will be raised in that case.

    Args:
        base_scheduler (TrialScheduler): The scheduler to provide decisions
            about trials. If None, a default FIFOScheduler will be used.
        resources_allocation_function (Callable): The function used to change
            live trial resource requiements during tuning. This function
            will be called on each trial as it finishes one step of training.
            The function must take four arguments: ``TrialRunner``, current
            ``Trial``, current result :class:`dict` and the base trial
            resource ``PlacementGroupFactory`` or ``Resource`` (depending on
            whether placement groups are used). The function must return a
            ``PlacementGroupFactory``, ``Resources``, :class:`dict` or None
            (signifying no need for an update). If
            ``resources_allocation_function`` is None, no resource
            requirements will be changed at any time.
            By default, :func:`evenly_distribute_cpus_gpus` will be used,
            distributing available CPUs and GPUs over all running trials
            in a robust way, without any prioritization.

    Warning:
        If the ``resources_allocation_function`` sets trial resource
        requirements to values bigger than possible, the trial will
        not run. Ensure that your function accounts for that possibility
        by setting upper limits. Consult :func:`evenly_distribute_cpus_gpus`
        to see how that may be done.

    Example:
        .. code-block:: python

            base_scheduler = ASHAScheduler(max_t=16)
            def my_resources_allocation_function(
                trial_runner: "trial_runner.TrialRunner",
                trial: Trial,
                result: Dict[str, Any],
                base_trial_resource: Union[PlacementGroupFactory, Resources]
            ) -> Union[None, PlacementGroupFactory, Resource]:
                # logic here
                # usage of PlacementGroupFactory is strongly preferred
                return PlacementGroupFactory(...)
            scheduler = ResourceChangingScheduler(
                            base_scheduler,
                            my_resources_allocation_function
                        )

        See :doc:`/tune/examples/xgboost_dynamic_resources_example` for a
        more detailed example.
    """

    def __init__(
            self,
            base_scheduler: Optional[TrialScheduler] = None,
            resources_allocation_function: Optional[Callable[[
                "trial_runner.TrialRunner", Trial, Dict[str, Any], Union[
                    PlacementGroupFactory, Resources]
            ], Union[None, PlacementGroupFactory,
                     Resources]]] = evenly_distribute_cpus_gpus,
    ) -> None:
        super().__init__()
        if resources_allocation_function is None:
            warnings.warn(
                "`resources_allocation_function` is None. No resource "
                "requirements will be changed at any time. Pass a "
                "correctly defined function to enable functionality.")
        self._resources_allocation_function = resources_allocation_function
        self._base_scheduler = base_scheduler or FIFOScheduler()
        self._base_trial_resources: Optional[Union[
            Resources, PlacementGroupFactory]] = None
        self._trials_to_reallocate: Dict[Trial, Union[
            None, dict, PlacementGroupFactory]] = {}
        self._reallocated_trial_ids: Set[str] = set()

    @property
    def metric(self):
        return self._base_scheduler._metric

    def set_search_properties(self, metric: Optional[str],
                              mode: Optional[str]) -> bool:
        return self._base_scheduler.set_search_properties(metric, mode)

    def on_trial_add(self, trial_runner: "trial_runner.TrialRunner",
                     trial: Trial, **kwargs):
        # use the first trial resources as the base
        if self._base_trial_resources is None:
            if trial.uses_placement_groups:
                self._base_trial_resources = trial.placement_group_factory
            else:
                self._base_trial_resources = trial.resources
        # Raise error if the resources of a newly added trial don't match
        # base resources, but allow trials that have already had their
        # resources changed by ResourceChangingScheduler
        # (those can be added again during loading from a checkpoint)
        elif trial.trial_id not in self._reallocated_trial_ids:
            if trial.uses_placement_groups:
                trial_resources = trial.placement_group_factory
            else:
                trial_resources = trial.resources
            if trial_resources != self._base_trial_resources:
                raise RuntimeError(
                    "ResourceChangingScheduler doesn't support trials with "
                    "varying base resources. First trial had "
                    f"{self._base_trial_resources}, trial {trial} has "
                    f"{trial_resources}.")

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
        if getattr(trial_runner.trial_executor, "_reuse_actors", False):
            raise ValueError("ResourceChangingScheduler cannot be used with "
                             "`reuse_actors=True`. FIX THIS by setting "
                             "`reuse_actors=False` in `tune.run`.")

        any_resources_changed = False

        for trial, new_resources in self._trials_to_reallocate.items():
            any_resources_changed = (any_resources_changed
                                     or self.set_trial_resources(
                                         trial, new_resources))
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
            logger.info(f"Setting trial {trial} resource to {new_resources}")
            trial.placement_group_factory = None
            trial.update_resources(new_resources)
            # keep track of all trials which had their resources changed
            self._reallocated_trial_ids.add(trial.trial_id)
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
