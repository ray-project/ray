import numpy as np
import logging
from typing import Dict, Any, Optional, Set, Tuple, Union, Callable

import pickle
import warnings
import math

from ray.util import PublicAPI
from ray.tune import trial_runner
from ray.tune.resources import Resources
from ray.tune.schedulers.trial_scheduler import FIFOScheduler, TrialScheduler
from ray.tune.trial import Trial
from ray.tune.utils.placement_groups import PlacementGroupFactory

logger = logging.getLogger(__name__)


@PublicAPI(stability="beta")
class DistributeResources:
    """This class creates a basic uniform resource allocation function.

    The function naively balances free resources (CPUs and GPUs) between
    trials, giving them all equal priority, ensuring that all resources
    are always being used. The free resources will be placed in new bundles.
    The function assumes that all bundles are equal (there is no "head"
    bundle).

    If for some reason a trial ends up with
    more resources than there are free ones, it will adjust downwards.
    It will also ensure that trial as at least as many resources as
    it started with (``base_trial_resource``).

    The function returns a new ``PlacementGroupFactory`` with updated
    resource requirements, or None. If the returned
    ``PlacementGroupFactory`` is equal by value to the one the
    trial has currently, the scheduler will skip the update process
    internally (same with None).

    Args:
        add_bundles (bool): If True, create new bundles from free resources.
            Otherwise, spread them among base_trial_resource bundles.
        increase_by (Optional[Dict[str, float]]): A dict with key-value
            pairs representing an atomic unit of resources (name-amount)
            the trial will be increased by. If not set, the trial will
            increase by 1 CPU/GPU.
        increase_by_times (int): If set to >=1 and ``increase_by`` is set,
            the trial will increase by maximum of
            ``increase_by_times * increase_by`` resources. If set to <1,
            no upper limit is set. Ignored if ``increase_by`` is not set.
        reserve_resources (Optional[Dict[str, float]]): A dict of
            resource_name-amount pairs representing the resources
            that will not be allocated to resized trials.
    """

    def __init__(
        self,
        add_bundles: bool = False,
        increase_by: Optional[Dict[str, float]] = None,
        increase_by_times: int = -1,
        reserve_resources: Optional[Dict[str, float]] = None,
    ):
        self.add_bundles = add_bundles
        self.increase_by = increase_by or {}
        self.increase_by_times = increase_by_times
        self.reserve_resources = reserve_resources or {}

    def _validate(self) -> bool:
        if not isinstance(self.base_trial_resource, PlacementGroupFactory):
            raise ValueError(
                "evenly_distribute_cpus_gpus only supports" " PlacementGroupFactories."
            )

        # Don't bother if this is just the first iteration
        if self.result["training_iteration"] < 1:
            return False

    def _get_total_min_resources(self) -> Tuple[int, int]:
        # Assume that the number of CPUs and GPUs can't go below
        # what was specified in tune.run
        min_cpu = self.base_trial_resource.required_resources.get("CPU", 0)
        min_gpu = self.base_trial_resource.required_resources.get("GPU", 0)
        return min_cpu, min_gpu

    def _get_min_resources_in_bundle(self) -> Tuple[int, int]:
        # Assume that the number of CPUs and GPUs can't go below
        # what was specified in tune.run
        min_cpu_bundle = self.base_trial_resource.bundles[0].get("CPU", 0)
        min_gpu_bundle = self.base_trial_resource.bundles[0].get("GPU", 0)
        return min_cpu_bundle, min_gpu_bundle

    def _get_total_available_resources(self) -> Tuple[int, int]:
        """Get the number of CPUs and GPUs avaialble in total (not just free)"""
        total_available_cpus = (
            self.trial_runner.trial_executor._avail_resources.cpu
            - self.reserve_resources.get("CPU", 0)
        )
        total_available_gpus = (
            self.trial_runner.trial_executor._avail_resources.gpu
            - self.reserve_resources.get("GPU", 0)
        )
        return total_available_cpus, total_available_gpus

    def _get_upper_limits(self) -> Tuple[int, int]:
        num_running_trials = len(self.trial_runner.get_live_trials())
        if self.min_cpu == 0:
            upper_cpu_limit = 0
        else:
            upper_cpu_limit = math.ceil(self.total_available_cpus / num_running_trials)
            # Round to nearest bundle minimum
            # eg. 8 CPUs between 3 trials with min 2 CPUs per bundle
            #   -> 4, 2, 2
            if self.add_bundles:
                upper_cpu_limit = (
                    math.ceil(upper_cpu_limit / self.min_cpu_bundle)
                    * self.min_cpu_bundle
                )
            upper_cpu_limit = max(self.min_cpu, upper_cpu_limit)

        if self.min_gpu == 0:
            upper_gpu_limit = 0
        else:
            upper_gpu_limit = math.ceil(self.total_available_gpus / num_running_trials)
            # Ensure we don't go below per-bundle minimum
            if self.add_bundles:
                upper_gpu_limit = (
                    math.ceil(upper_gpu_limit / self.min_gpu_bundle)
                    * self.min_gpu_bundle
                )
            upper_gpu_limit = max(self.min_gpu, upper_gpu_limit)

        return upper_cpu_limit, upper_gpu_limit

    def _modify_upper_limits_with_increase_by_times(self) -> None:
        if self.increase_by and self.increase_by_times > 0:
            required_cpus = self.increase_by.get("CPU", 0)
            required_gpus = self.increase_by.get("GPU", 0)
            self.upper_cpu_limit = min(
                self.upper_cpu_limit,
                self.min_cpu + required_cpus * self.increase_by_times,
            )
            self.upper_gpu_limit = min(
                self.upper_gpu_limit,
                self.min_gpu + required_gpus * self.increase_by_times,
            )

    def _modify_lower_limits_with_increase_by_times(self) -> None:
        if self.increase_by:
            required_cpus = self.increase_by.get("CPU", 0)
            required_gpus = self.increase_by.get("GPU", 0)
            if required_cpus and required_gpus:
                multiplier = min(
                    self.free_cpus // required_cpus, self.free_gpus // required_gpus
                )
            elif required_gpus:
                multiplier = self.free_gpus // required_gpus
            else:
                multiplier = self.free_cpus // required_cpus
            multiplier = max(multiplier, 0)
            self.free_cpus = multiplier * required_cpus
            self.free_gpus = multiplier * required_gpus

    def _get_used_cpus_and_gpus(self, t: Trial):
        """Function to check how many CPUs and GPUs a trial is using
        currently"""
        return (
            t.placement_group_factory.required_resources.get("CPU", 0),
            t.placement_group_factory.required_resources.get("GPU", 0),
        )

    def __call__(
        self,
        trial_runner: "trial_runner.TrialRunner",
        trial: Trial,
        result: Dict[str, Any],
        scheduler: "ResourceChangingScheduler",
    ) -> Union[None, PlacementGroupFactory]:
        """Run resource allocation logic.

        Returns a new ``PlacementGroupFactory`` with updated
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
        self.trial_runner = trial_runner
        self.trial = trial
        self.result = result
        self.scheduler = scheduler

        # Get base trial resources as defined in
        # ``tune.run(resources_per_trial)``
        self.base_trial_resource = scheduler.base_trial_resources

        if not self._validate():
            return None

        # default values if resources_per_trial is unspecified
        if self.base_trial_resource is None:
            self.base_trial_resource = PlacementGroupFactory([{"CPU": 1, "GPU": 0}])

        self.min_cpu, self.min_gpu = self._get_total_min_resources()

        self.min_cpu_bundle, self.min_gpu_bundle = self._get_min_resources_in_bundle()

        # Get the number of CPUs and GPUs avaialble in total (not just free)
        (
            self.total_available_cpus,
            self.total_available_gpus,
        ) = self._get_total_available_resources()

        # Set upper limits for resources based on number of live trials
        # to ensure that the trial cannot get more resources that it's
        # possible to run
        self.upper_cpu_limit, self.upper_gpu_limit = self._get_upper_limits()

        self._modify_upper_limits_with_increase_by_times()

        # Check how many CPUs and GPUs are currently being used by this trial
        self.trial_used_cpus, self.trial_used_gpus = self._get_used_cpus_and_gpus(trial)

        # Check how many CPUs and GPUs are currently being used by live trials
        used_cpus_and_gpus = [
            self._get_used_cpus_and_gpus(t) for t in trial_runner.get_live_trials()
        ]
        self.used_cpus, self.used_gpus = zip(*used_cpus_and_gpus)
        self.used_cpus = sum(self.used_cpus)
        self.used_gpus = sum(self.used_gpus)

        # Calculate how many free CPUs and GPUs there are
        self.free_cpus = self.total_available_cpus - self.used_cpus
        self.free_gpus = self.total_available_gpus - self.used_gpus

        self._modify_lower_limits_with_increase_by_times()

        # Add free CPUs and GPUs enforcing upper and lower limits
        new_cpu = min(
            self.upper_cpu_limit,
            max(self.trial_used_cpus + self.free_cpus, self.min_cpu),
        )
        new_gpu = min(
            self.upper_gpu_limit,
            max(self.trial_used_gpus + self.free_gpus, self.min_gpu),
        )

        # Assign new CPUs and GPUs to the trial in a PlacementGroupFactory

        # If self.add_bundles, make new bundles out of the resources
        if self.add_bundles:
            if self.min_cpu_bundle and self.min_gpu_bundle:
                multiplier = min(
                    new_cpu // self.min_cpu_bundle, new_gpu // self.min_gpu_bundle
                )
            elif self.min_gpu_bundle:
                multiplier = new_gpu // self.min_gpu_bundle
            else:
                multiplier = new_cpu // self.min_cpu_bundle
            new_bundles = [
                {"CPU": self.min_cpu_bundle, "GPU": self.min_gpu_bundle}
            ] * int(multiplier)
        # Otherwise, just put them all in one bundle
        else:
            new_bundles = [{"CPU": new_cpu, "GPU": new_gpu}]
        return PlacementGroupFactory(new_bundles)


@PublicAPI(stability="beta")
class DistributeResourcesToTopJob(DistributeResources):
    """This class creates a "TopJob" resource allocation function.

    The function will assign all of the free resources to the best
    performing trial (as defined by ``metric`` and ``mode``). The
    previous best trials will not have their resources deallocated,
    unless in the case outlined below.

    If for some reason a trial ends up with
    more resources than there are free ones, it will adjust downwards.
    It will also ensure that trial as at least as many resources as
    it started with (``base_trial_resource``).

    The function returns a new ``PlacementGroupFactory`` with updated
    resource requirements, or None. If the returned
    ``PlacementGroupFactory`` is equal by value to the one the
    trial has currently, the scheduler will skip the update process
    internally (same with None).

    Args:
        add_bundles (bool): If True, create new bundles from free resources.
            Otherwise, spread them among base_trial_resource bundles.
        increase_by (Optional[Dict[str, float]]): A dict with key-value
            pairs representing an atomic unit of resources (name-amount)
            the trial will be increased by. If not set, the trial will
            increase by 1 CPU/GPU.
        increase_by_times (int): If set to >=1 and ``increase_by`` is set,
            the trial will increase by maximum of
            ``increase_by_times * increase_by`` resources. If set to <1,
            no upper limit is set. Ignored if ``increase_by`` is not set.
        reserve_resources (Optional[Dict[str, float]]): A dict of
            resource_name-amount pairs representing the resources
            that will not be allocated to resized trials.
            is that the attribute should increase monotonically.
        metric (Optional[str]): The training result objective value attribute. Stopping
            procedures will use this attribute. If None, will use the metric
            of the scheduler.
        mode (Optional[str]): One of {min, max}. Determines whether objective is
            minimizing or maximizing the metric attribute. If None, will use the metric
            of the scheduler.

    """

    def __init__(
        self,
        add_bundles: bool = False,
        increase_by: Optional[Dict[str, float]] = None,
        increase_by_times: int = -1,
        reserve_resources: Optional[Dict[str, float]] = None,
        metric: Optional[str] = None,
        mode: Optional[str] = None,
    ):
        super().__init__(add_bundles, increase_by, increase_by_times, reserve_resources)
        self.metric = metric
        self.mode = mode

    @property
    def _metric_op(self) -> float:
        if self.mode not in ("min", "max"):
            raise ValueError("The mode parameter can only be" " either min or max.")
        if self.mode == "max":
            return 1.0
        return -1.0

    def _get_upper_limits(self) -> Tuple[int, int]:
        sorted_trials = sorted(
            self.trial_runner.get_live_trials(),
            key=lambda t: -self._metric_op * t.last_result.get(self.metric, np.inf),
        )

        if not sorted_trials:
            return 0, 0

        if self.increase_by and self.increase_by_times > 0:
            required_cpus = self.increase_by.get("CPU", 0)
            required_gpus = self.increase_by.get("GPU", 0)
            upper_cpu_limit = self.min_cpu + required_cpus * self.increase_by_times
            upper_gpu_limit = self.min_gpu + required_gpus * self.increase_by_times

            def is_trial_below_limit(trial: Trial):
                resources = trial.placement_group_factory.required_resources
                ret = (
                    resources.get("CPU", -np.inf) < upper_cpu_limit
                    and resources.get("GPU", -np.inf) < upper_gpu_limit
                )
                return ret

            best_trial = next(
                (t for t in sorted_trials if is_trial_below_limit(t)), sorted_trials[0]
            )
        else:
            best_trial = sorted_trials[0]
        if self.trial.trial_id != best_trial.trial_id:
            return self._get_used_cpus_and_gpus(self.trial)
        return self.total_available_cpus, self.total_available_gpus


def evenly_distribute_cpus_gpus(
    trial_runner: "trial_runner.TrialRunner",
    trial: Trial,
    result: Dict[str, Any],
    scheduler: "ResourceChangingScheduler",
) -> Union[None, PlacementGroupFactory]:
    """This is a basic uniform resource allocating function.

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

    For greater customizability, use ``DistributeResources`` to create
    this function.

    Args:
        trial_runner (TrialRunner): Trial runner for this Tune run.
            Can be used to obtain information about other trials.
        trial (Trial): The trial to allocate new resources to.
        result (Dict[str, Any]): The latest results of trial.
        scheduler (ResourceChangingScheduler): The scheduler calling
            the function.
    """

    return DistributeResources(add_bundles=False)(
        trial_runner, trial, result, scheduler
    )


def evenly_distribute_cpus_gpus_distributed(
    trial_runner: "trial_runner.TrialRunner",
    trial: Trial,
    result: Dict[str, Any],
    scheduler: "ResourceChangingScheduler",
) -> Union[None, PlacementGroupFactory]:
    """This is a basic uniform resource allocating function.

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

    For greater customizability, use ``DistributeResources`` to create
    this function.

    Args:
        trial_runner (TrialRunner): Trial runner for this Tune run.
            Can be used to obtain information about other trials.
        trial (Trial): The trial to allocate new resources to.
        result (Dict[str, Any]): The latest results of trial.
        scheduler (ResourceChangingScheduler): The scheduler calling
            the function.
    """

    return DistributeResources(add_bundles=True)(trial_runner, trial, result, scheduler)


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
        self._metric = None
        self._mode = None

    @property
    def metric(self):
        return self._base_scheduler._metric

    @property
    def base_trial_resources(self) -> Optional[Union[Resources, PlacementGroupFactory]]:
        return self._base_trial_resources

    def set_search_properties(
        self, metric: Optional[str], mode: Optional[str], **spec
    ) -> bool:
        self._metric = metric
        self._mode = mode
        return self._base_scheduler.set_search_properties(metric, mode, **spec)

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

        if not getattr(self._resources_allocation_function, "metric", None):
            self._resources_allocation_function.metric = getattr(
                self._base_scheduler, "_metric", self._metric
            )
        if not getattr(self._resources_allocation_function, "mode", None):
            self._resources_allocation_function.mode = getattr(
                self._base_scheduler, "_mode", self._mode
            )

        new_resources = self._resources_allocation_function(
            trial_runner, trial, result, self
        )

        # if we can check if the new resources are the same,
        # we do that here and skip resource allocation
        if new_resources and not self._are_resources_the_same(trial, new_resources):
            return new_resources
        return None
