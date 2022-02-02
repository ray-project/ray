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


class _DistributeResources:
    """Generic functionality for resource allocation functions"""

    def __init__(self, add_bundles: bool = False):
        """If add_bundles is True, create new bundles from free resources.
        Otherwise, spread them among base_trial_resource bundles."""
        self.add_bundles = add_bundles

    def __call__(
        self,
        trial_runner: "trial_runner.TrialRunner",
        trial: Trial,
        result: Dict[str, Any],
        scheduler: "ResourceChangingScheduler",
    ) -> Union[None, PlacementGroupFactory]:
        # Get base trial resources as defined in
        # ``tune.run(resources_per_trial)``
        base_trial_resource = scheduler.base_trial_resources

        if not isinstance(base_trial_resource, PlacementGroupFactory):
            raise ValueError(
                "evenly_distribute_cpus_gpus only supports" " PlacementGroupFactories."
            )

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

        min_cpu_bundle = base_trial_resource.bundles[0].get("CPU", 0)
        min_gpu_bundle = base_trial_resource.bundles[0].get("GPU", 0)

        # Get the number of CPUs and GPUs avaialble in total (not just free)
        total_available_cpus = trial_runner.trial_executor._avail_resources.cpu
        total_available_gpus = trial_runner.trial_executor._avail_resources.gpu

        # Set upper limits for resources based on number of live trials
        # to ensure that the trial cannot get more resources that it's
        # possible to run
        num_running_trials = len(trial_runner.get_live_trials())
        if min_cpu == 0:
            upper_cpu_limit = 0
        else:
            upper_cpu_limit = math.ceil(total_available_cpus / num_running_trials)
            # Round to nearest bundle minimum
            # eg. 8 CPUs between 3 trials with min 2 CPUs per bundle
            #   -> 4, 2, 2
            if self.add_bundles:
                upper_cpu_limit = (
                    math.ceil(upper_cpu_limit / min_cpu_bundle) * min_cpu_bundle
                )
            upper_cpu_limit = max(min_cpu, upper_cpu_limit)

        if min_gpu == 0:
            upper_gpu_limit = 0
        else:
            upper_gpu_limit = math.ceil(total_available_gpus / num_running_trials)
            # Ensure we don't go below per-bundle minimum
            if self.add_bundles:
                upper_gpu_limit = (
                    math.ceil(upper_gpu_limit / min_cpu_bundle) * min_gpu_bundle
                )
            upper_gpu_limit = max(min_gpu, upper_gpu_limit)

        # Function to check how many CPUs and GPUs a trial is using currently
        def get_used_cpus_and_gpus(t: Trial):
            return (
                t.placement_group_factory.required_resources.get("CPU", 0),
                t.placement_group_factory.required_resources.get("GPU", 0),
            )

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

        # If self.add_bundles, make new bundles out of the resources
        if self.add_bundles:
            if min_cpu_bundle and min_gpu_bundle:
                multiplier = min(new_cpu // min_cpu_bundle, new_gpu // min_cpu_bundle)
            elif min_gpu_bundle:
                multiplier = new_gpu // min_cpu_bundle
            else:
                multiplier = new_cpu // min_cpu_bundle
            new_bundles = [{"CPU": min_cpu_bundle, "GPU": min_gpu_bundle}] * int(
                multiplier
            )
        # Otherwise, just put them all in one bundle
        else:
            new_bundles = [{"CPU": new_cpu, "GPU": new_gpu}]
        return PlacementGroupFactory(new_bundles)


def evenly_distribute_cpus_gpus(
    trial_runner: "trial_runner.TrialRunner",
    trial: Trial,
    result: Dict[str, Any],
    scheduler: "ResourceChangingScheduler",
) -> Union[None, PlacementGroupFactory]:
    """This is a basic resource allocating function.

    This function is used by default in ``ResourceChangingScheduler``.

    The function naively balances free resources (CPUs and GPUs) between
    trials, giving them all equal priority, ensuring that all resources
    are always being used. All of the resources will be placed in one bundle.

    If for some reason a trial ends up with
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
        scheduler (ResourceChangingScheduler): The scheduler calling
            the function.
    """

    return _DistributeResources(add_bundles=False)(
        trial_runner, trial, result, scheduler
    )


def evenly_distribute_cpus_gpus_distributed(
    trial_runner: "trial_runner.TrialRunner",
    trial: Trial,
    result: Dict[str, Any],
    scheduler: "ResourceChangingScheduler",
) -> Union[None, PlacementGroupFactory]:
    """This is a basic resource allocating function.

    The function naively balances free resources (CPUs and GPUs) between
    trials, giving them all equal priority, ensuring that all resources
    are always being used. The free resources will be placed in new bundles.
    This function assumes that all bundles are equal (there is no "head"
    bundle).

    If for some reason a trial ends up with
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
        scheduler (ResourceChangingScheduler): The scheduler calling
            the function.
    """

    return _DistributeResources(add_bundles=True)(
        trial_runner, trial, result, scheduler
    )


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

    If the functional API is used, the current trial resources can be obtained
    by calling `tune.get_trial_resources()` inside the training function.
    The function should be able to
    :ref:`load and save checkpoints <tune-checkpoint-syncing>`
    (the latter preferably every iteration).

    If the Trainable (class) API is used, when the resources of a
    trial are updated with new values, the ``update_resources`` method in
    the Trainable will be called. This method needs to be overwritten by the
    user in order to let the trained model take advantage of newly allocated
    resources. You can also obtain the current trial resources by calling
    ``Trainable.trial_resources``.

    Cannot be used if ``reuse_actors`` is True in ``tune.run``. A ValueError
    will be raised in that case.

    Args:
        base_scheduler (TrialScheduler): The scheduler to provide decisions
            about trials. If None, a default FIFOScheduler will be used.
        resources_allocation_function (Callable): The function used to change
            live trial resource requiements during tuning. This function
            will be called on each trial as it finishes one step of training.
            The function must take four arguments: ``TrialRunner``, current
            ``Trial``, current result :class:`dict` and the
            ``ResourceChangingScheduler`` calling it. The function must
            return a ``PlacementGroupFactory``, ``Resources``, :class:`dict`
            or None (signifying no need for an update). If
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
                scheduler: "ResourceChangingScheduler"
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
        resources_allocation_function: Optional[
            Callable[
                [
                    "trial_runner.TrialRunner",
                    Trial,
                    Dict[str, Any],
                    "ResourceChangingScheduler",
                ],
                Union[None, PlacementGroupFactory, Resources],
            ]
        ] = evenly_distribute_cpus_gpus,
    ) -> None:
        super().__init__()
        if resources_allocation_function is None:
            warnings.warn(
                "`resources_allocation_function` is None. No resource "
                "requirements will be changed at any time. Pass a "
                "correctly defined function to enable functionality."
            )
        self._resources_allocation_function = resources_allocation_function
        self._base_scheduler = base_scheduler or FIFOScheduler()
        self._base_trial_resources: Optional[
            Union[Resources, PlacementGroupFactory]
        ] = None
        self._trials_to_reallocate: Dict[
            Trial, Union[None, dict, PlacementGroupFactory]
        ] = {}
        self._reallocated_trial_ids: Set[str] = set()

    @property
    def metric(self):
        return self._base_scheduler._metric

    @property
    def base_trial_resources(self) -> Optional[Union[Resources, PlacementGroupFactory]]:
        return self._base_trial_resources

    def set_search_properties(self, metric: Optional[str], mode: Optional[str]) -> bool:
        return self._base_scheduler.set_search_properties(metric, mode)

    def on_trial_add(
        self, trial_runner: "trial_runner.TrialRunner", trial: Trial, **kwargs
    ):
        # use the first trial resources as the base
        if self._base_trial_resources is None:
            self._base_trial_resources = trial.placement_group_factory
        # Raise error if the resources of a newly added trial don't match
        # base resources, but allow trials that have already had their
        # resources changed by ResourceChangingScheduler
        # (those can be added again during loading from a checkpoint)
        elif trial.trial_id not in self._reallocated_trial_ids:
            trial_resources = trial.placement_group_factory
            if trial_resources != self._base_trial_resources:
                raise RuntimeError(
                    "ResourceChangingScheduler doesn't support trials with "
                    "varying base resources. First trial had "
                    f"{self._base_trial_resources}, trial {trial} has "
                    f"{trial_resources}."
                )

        return self._base_scheduler.on_trial_add(trial_runner, trial, **kwargs)

    def on_trial_error(
        self, trial_runner: "trial_runner.TrialRunner", trial: Trial, **kwargs
    ):
        return self._base_scheduler.on_trial_error(trial_runner, trial, **kwargs)

    def on_trial_result(
        self, trial_runner: "trial_runner.TrialRunner", trial: Trial, result: Dict
    ) -> str:
        base_scheduler_decision = self._base_scheduler.on_trial_result(
            trial_runner, trial, result
        )
        if base_scheduler_decision == TrialScheduler.CONTINUE:
            new_resources = self.reallocate_trial_resources_if_needed(
                trial_runner, trial, result
            )
            if new_resources:
                self._trials_to_reallocate[trial] = new_resources
                return TrialScheduler.PAUSE
        return base_scheduler_decision

    def on_trial_complete(
        self,
        trial_runner: "trial_runner.TrialRunner",
        trial: Trial,
        result: Dict,
        **kwargs,
    ):
        return self._base_scheduler.on_trial_complete(
            trial_runner, trial, result, **kwargs
        )

    def on_trial_remove(
        self, trial_runner: "trial_runner.TrialRunner", trial: Trial, **kwargs
    ):
        return self._base_scheduler.on_trial_remove(trial_runner, trial, **kwargs)

    def choose_trial_to_run(
        self, trial_runner: "trial_runner.TrialRunner", **kwargs
    ) -> Optional[Trial]:
        if getattr(trial_runner.trial_executor, "_reuse_actors", False):
            raise ValueError(
                "ResourceChangingScheduler cannot be used with "
                "`reuse_actors=True`. FIX THIS by setting "
                "`reuse_actors=False` in `tune.run`."
            )

        any_resources_changed = False

        new_trials_to_reallocate = {}
        for trial, new_resources in self._trials_to_reallocate.items():
            if trial.status == Trial.RUNNING:
                new_trials_to_reallocate[trial] = new_resources
                logger.debug(f"{trial} is still running, skipping for now")
                continue
            any_resources_changed = any_resources_changed or self.set_trial_resources(
                trial, new_resources
            )
        self._trials_to_reallocate = new_trials_to_reallocate

        if any_resources_changed:
            # force reconcilation to ensure resource changes
            # are implemented right away
            trial_runner.trial_executor.force_reconcilation_on_next_step_end()

        trial = self._base_scheduler.choose_trial_to_run(trial_runner, **kwargs)
        return trial

    def debug_string(self) -> str:
        return "(ResourceChangingScheduler) " f"{self._base_scheduler.debug_string()}"

    def save(self, checkpoint_path: str):
        save_object = self.__dict__
        with open(checkpoint_path, "wb") as outputFile:
            pickle.dump(save_object, outputFile)

    def restore(self, checkpoint_path: str):
        with open(checkpoint_path, "rb") as inputFile:
            save_object = pickle.load(inputFile)
        self.__dict__.update(save_object)

    def set_trial_resources(
        self, trial: Trial, new_resources: Union[Dict, PlacementGroupFactory]
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
        if (
            isinstance(new_resources, PlacementGroupFactory)
            and trial.placement_group_factory == new_resources
        ):
            logger.debug(
                f"{trial} PGF "
                f"{trial.placement_group_factory.required_resources}"
                f" and {new_resources.required_resources}"
                f" are the same, skipping"
            )
            return True
        else:
            return False

    def reallocate_trial_resources_if_needed(
        self, trial_runner: "trial_runner.TrialRunner", trial: Trial, result: Dict
    ) -> Union[None, dict, PlacementGroupFactory]:
        """Calls user defined resources_allocation_function. If the returned
        resources are not none and not the same as currently present, returns
        them. Otherwise, returns None."""
        if self._resources_allocation_function is None:
            return None

        new_resources = self._resources_allocation_function(
            trial_runner, trial, result, self
        )

        # if we can check if the new resources are the same,
        # we do that here and skip resource allocation
        if new_resources and not self._are_resources_the_same(trial, new_resources):
            return new_resources
        return None
