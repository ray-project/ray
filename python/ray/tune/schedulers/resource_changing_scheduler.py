from copy import deepcopy
import numpy as np
import logging
from typing import Dict, Any, List, Optional, Set, Tuple, Union, Callable

import pickle
import warnings

from ray.air.execution.resources.request import _sum_bundles
from ray.util.annotations import PublicAPI
from ray.tune.execution import trial_runner
from ray.tune.resources import Resources
from ray.tune.schedulers.trial_scheduler import FIFOScheduler, TrialScheduler
from ray.tune.experiment import Trial
from ray.tune.execution.placement_groups import PlacementGroupFactory

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

    If you wish to implement your own resource distribution logic,
    you can do so by extending this class, as it provides several
    generic methods. You can also implement a function instead.

    Args:
        add_bundles: If True, create new bundles from free resources.
            Otherwise, spread them among base_trial_resource bundles.
        increase_by: A dict with key-value
            pairs representing an atomic unit of resources (name-amount)
            the trial will be increased by. If not set, the trial will
            increase by 1 CPU/GPU.
        increase_by_times: If set to >=1 and ``increase_by`` is set,
            the trial will increase by maximum of
            ``increase_by_times * increase_by`` resources. If set to <1,
            no upper limit is set. Ignored if ``increase_by`` is not set.
        reserve_resources: A dict of
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

    def _validate(
        self, base_trial_resource: PlacementGroupFactory, result: Dict[str, Any]
    ) -> bool:
        """Return False if we should keep the current resources outright."""
        if not isinstance(base_trial_resource, PlacementGroupFactory):
            raise ValueError(
                f"{self.__class__.__name__} only supports PlacementGroupFactories."
            )

        if not self.add_bundles and len(base_trial_resource.bundles) > 1:
            raise ValueError(
                "If `add_bundles` is False, the number of bundles in "
                "`resources_per_trial` must be 1 "
                f"(got {len(base_trial_resource.bundles)})."
            )

        # Don't bother if this is just the first iteration
        if result["training_iteration"] < 1:
            return False
        return True

    def _get_total_available_resources(
        self, trial_runner: "trial_runner.TrialRunner"
    ) -> Tuple[float, float]:
        """Get the number of CPUs and GPUs avaialble in total (not just free)"""
        total_available_cpus = (
            trial_runner.trial_executor._resource_updater.get_num_cpus()
            - self.reserve_resources.get("CPU", 0)
        )
        total_available_gpus = (
            trial_runner.trial_executor._resource_updater.get_num_gpus()
            - self.reserve_resources.get("GPU", 0)
        )
        return total_available_cpus, total_available_gpus

    def _get_used_cpus_and_gpus(self, t: Trial) -> Tuple[float, float]:
        """Check how many CPUs and GPUs a trial is using currently"""
        return (
            t.placement_group_factory.required_resources.get("CPU", 0),
            t.placement_group_factory.required_resources.get("GPU", 0),
        )

    def _get_resources_from_bundles(
        self, bundles: List[Dict[str, float]]
    ) -> Dict[str, float]:
        """Get total sums of resources in bundles"""
        if not bundles:
            return {"CPU": 0, "GPU": 0}
        return _sum_bundles(bundles)

    def _is_bundle_empty(self, bundle: Dict[str, float]) -> bool:
        return not (bundle.get("CPU", 0) or bundle.get("GPU", 0))

    def _add_two_bundles(
        self,
        bundles_a: List[Dict[str, float]],
        bundles_b: List[Dict[str, float]],
        increase_by: Dict[str, float],
        limit_to_increase_by_times: bool,
        max_increase_by_times: int = -1,
    ):
        """Add two bundles together.

        If ``limit_to_increase_by_times`` is True, ``self.increase_by_times`` > 0
        and ``max_increase_by_times`` > 0, ensure that the resulting number of
        bundles is not above ``min(max_increase_by_times, self.increase_by_times)``.

        If ``limit_to_increase_by_times`` is True and ``self.increase_by_times`` > 0,
        ensure that the resulting number of bundles is not above
        `self.increase_by_times``.
        """
        if limit_to_increase_by_times:
            if max_increase_by_times > 0 and self.increase_by_times > 0:
                max_increase_by_times = min(
                    max_increase_by_times, self.increase_by_times
                )
            elif self.increase_by_times > 0:
                max_increase_by_times = self.increase_by_times

        if self.add_bundles:
            bundles = [b for b in bundles_a if not self._is_bundle_empty(b)] + [
                b for b in bundles_b if not self._is_bundle_empty(b)
            ]
            if max_increase_by_times > 0:
                bundles = bundles[:max_increase_by_times]
        else:
            bundles_a = bundles_a or [{}]
            bundles_b = bundles_b or [{}]
            bundles = [
                {
                    "CPU": bundles_a[0].get("CPU", 0) + bundles_b[0].get("CPU", 0),
                    "GPU": bundles_a[0].get("GPU", 0) + bundles_b[0].get("GPU", 0),
                }
            ]
            if max_increase_by_times > 0:
                bundles[0]["CPU"] = min(
                    bundles[0]["CPU"],
                    increase_by.get("CPU", 0) * max_increase_by_times,
                )
                bundles[0]["GPU"] = min(
                    bundles[0]["GPU"],
                    increase_by.get("GPU", 0) * max_increase_by_times,
                )

        return bundles

    def _get_multiplier(
        self,
        increase_by: Dict[str, float],
        cpus: float = 0,
        gpus: float = 0,
        max_multiplier: int = -1,
    ) -> int:
        """Get how many times ``increase_by`` bundles
        occur in ``cpus`` and ``gpus``."""
        if increase_by.get("CPU", 0) and increase_by.get("GPU", 0):
            multiplier = min(
                cpus // increase_by.get("CPU", 0),
                gpus // increase_by.get("GPU", 0),
            )
        elif increase_by.get("GPU", 0):
            multiplier = gpus // increase_by.get("GPU", 0)
        else:
            multiplier = cpus // increase_by.get("CPU", 0)

        if max_multiplier > 0 and multiplier > 0:
            multiplier = min(max_multiplier, multiplier)
        return int(multiplier)

    def _remove_bundles(
        self,
        bundles: List[Dict[str, float]],
        increase_by: Dict[str, float],
        multiplier: int,
    ) -> List[Dict[str, float]]:
        """Remove ``multiplier`` ``increase_by`` bundles from ``bundles``."""
        multiplier = -abs(multiplier)
        if self.add_bundles:
            bundles = bundles[:multiplier]
        else:
            bundles = deepcopy(bundles)
            bundles[0]["CPU"] += increase_by.get("CPU", 0) * multiplier
            bundles[0]["GPU"] += increase_by.get("GPU", 0) * multiplier
            bundles[0]["CPU"] = max(bundles[0]["CPU"], 0)
            bundles[0]["GPU"] = max(bundles[0]["GPU"], 0)
        return bundles

    def _create_new_bundles(
        self,
        increase_by: Dict[str, float],
        multiplier: int,
    ) -> List[Dict[str, float]]:
        """Create a list of new bundles containing ``increase_by`` * ``multiplier``."""
        multiplier = abs(multiplier)

        if self.add_bundles:
            bundles = [increase_by] * int(multiplier)
        else:
            bundles = [{}]
            bundles[0]["CPU"] = increase_by.get("CPU", 0) * multiplier
            bundles[0]["GPU"] = increase_by.get("GPU", 0) * multiplier

        return bundles

    def _modify_bundles_with_free_resources(
        self,
        bundles: List[Dict[str, float]],
        increase_by: Dict[str, float],
        free_cpus: float,
        free_gpus: float,
        *,
        max_multiplier: int = -1,
        max_increase_by_times: int = -1,
    ):
        """Given free resources, increase/decrease the number of bundles in
        ``bundles``."""
        multiplier = self._get_multiplier(
            increase_by, free_cpus, free_gpus, max_multiplier
        )
        if multiplier < 0:
            bundles = self._remove_bundles(bundles, increase_by, multiplier)
        elif multiplier > 0:
            bundles_to_add = self._create_new_bundles(increase_by, multiplier)
            bundles = self._add_two_bundles(
                bundles, bundles_to_add, increase_by, True, max_increase_by_times
            )
        return bundles

    def _get_added_bundles(
        self, bundles: List[Dict[str, float]], base_bundles: List[Dict[str, float]]
    ) -> List[Dict[str, float]]:
        """Return the difference between bundles and base_bundles"""
        if self.add_bundles:
            added_bundles = bundles[len(base_bundles) :]
        else:
            if not bundles:
                bundles = [{"CPU": 0, "GPU": 0}]
            if not base_bundles:
                base_bundles = [{"CPU": 0, "GPU": 0}]
            added_bundles = [
                {
                    "CPU": bundles[0].get("CPU", 0) - base_bundles[0].get("CPU", 0),
                    "GPU": bundles[0].get("GPU", 0) - base_bundles[0].get("GPU", 0),
                }
            ]
        return added_bundles

    def _are_bundles_below_limit(
        self,
        bundles: List[Dict[str, float]],
        base_bundles: Optional[List[Dict[str, float]]] = None,
        max_added_cpus: Optional[float] = None,
        max_added_gpus: Optional[float] = None,
    ):
        if not max_added_cpus:
            if self.increase_by_times > 0:
                max_added_cpus = self.increase_by.get("CPU", 0) * self.increase_by_times
            else:
                max_added_cpus = np.inf
        if not max_added_gpus:
            if self.increase_by_times > 0:
                max_added_gpus = self.increase_by.get("GPU", 0) * self.increase_by_times
            else:
                max_added_gpus = np.inf
        added_resources = self._get_resources_from_bundles(
            self._get_added_bundles(bundles, base_bundles) if base_bundles else bundles
        )
        ret = (
            added_resources.get("CPU", -np.inf) < max_added_cpus
            or added_resources.get("GPU", -np.inf) < max_added_gpus
        )
        return ret

    def _get_new_added_bundles(
        self,
        trial: Trial,
        all_trials: List[Trial],
        base_bundles: List[Dict[str, float]],
        increase_by: Dict[str, float],
        total_available_cpus: float,
        total_available_gpus: float,
        used_cpus: float,
        used_gpus: float,
    ) -> List[Dict[str, float]]:
        """Returns updated added bundles."""
        upper_limit_all_trials_bundles = [list() for _ in range(len(all_trials))]

        free_cpus = total_available_cpus - used_cpus
        free_gpus = total_available_gpus - used_gpus

        base_resources = self._get_resources_from_bundles(base_bundles)
        upper_limit_cpus_to_distribute = total_available_cpus - (
            base_resources.get("CPU", 0) * len(all_trials)
        )
        upper_limit_gpus_to_distribute = total_available_gpus - (
            base_resources.get("GPU", 0) * len(all_trials)
        )
        max_increase_by_times = 0

        # First, calculate upper limits for uniform allocation
        # This is done by simulating a clean slate scenario
        # The loop runs until all resources are allocated or
        # all trials are at their resource limits
        i = 0
        trials_at_limit = set()
        while (
            len(trials_at_limit) < len(all_trials)
            # we have previously asserted that at least one resource has to be
            # bigger than 0
            and upper_limit_cpus_to_distribute >= increase_by.get("CPU", 0)
            and upper_limit_gpus_to_distribute >= increase_by.get("GPU", 0)
        ):
            idx = i % len(upper_limit_all_trials_bundles)
            old_bundles = deepcopy(upper_limit_all_trials_bundles[idx])
            upper_limit_all_trials_bundles[
                idx
            ] = self._modify_bundles_with_free_resources(
                upper_limit_all_trials_bundles[idx],
                increase_by,
                upper_limit_cpus_to_distribute,
                upper_limit_gpus_to_distribute,
                max_multiplier=1,
            )
            added_resources = self._get_resources_from_bundles(
                self._get_added_bundles(
                    upper_limit_all_trials_bundles[idx], old_bundles
                )
            )
            if not added_resources.get("CPU", 0) and not added_resources.get("GPU", 0):
                trials_at_limit.add(idx)
            elif idx == 0:
                max_increase_by_times += 1
            upper_limit_cpus_to_distribute -= added_resources.get("CPU", 0)
            upper_limit_gpus_to_distribute -= added_resources.get("GPU", 0)
            i += 1

        # Add new resourcs, but only up to calculated upper limits
        # (max_increase_by_times)
        return self._modify_bundles_with_free_resources(
            self._get_added_bundles(
                trial.placement_group_factory.bundles, base_bundles
            ),
            increase_by,
            free_cpus,
            free_gpus,
            max_increase_by_times=max_increase_by_times,
        )

    def __call__(
        self,
        trial_runner: "trial_runner.TrialRunner",
        trial: Trial,
        result: Dict[str, Any],
        scheduler: "ResourceChangingScheduler",
    ) -> Optional[PlacementGroupFactory]:
        """Run resource allocation logic.

        Returns a new ``PlacementGroupFactory`` with updated
        resource requirements, or None. If the returned
        ``PlacementGroupFactory`` is equal by value to the one the
        trial has currently, the scheduler will skip the update process
        internally (same with None).

        Args:
            trial_runner: Trial runner for this Tune run.
                Can be used to obtain information about other trials.
            trial: The trial to allocate new resources to.
            result: The latest results of trial.
            scheduler: The scheduler calling
                the function.
        """
        # Get base trial resources as defined in
        # ``tune.run(resources_per_trial)``
        base_trial_resource = scheduler.base_trial_resources

        if not self._validate(base_trial_resource=base_trial_resource, result=result):
            return None

        # default values if resources_per_trial is unspecified
        if base_trial_resource is None:
            base_trial_resource = PlacementGroupFactory([{"CPU": 1, "GPU": 0}])

        if self.increase_by:
            increase_by = self.increase_by
            assert not self._is_bundle_empty(increase_by)
            assert increase_by.get("CPU", 0) >= 0 and increase_by.get("GPU", 0) >= 0
        elif self.add_bundles:
            increase_by = base_trial_resource.bundles[-1]
        elif base_trial_resource.bundles[0].get("GPU", 0):
            increase_by = {"GPU": 1}
        else:
            increase_by = {"CPU": 1}

        base_bundles = deepcopy(base_trial_resource.bundles)

        (
            total_available_cpus,
            total_available_gpus,
        ) = self._get_total_available_resources(trial_runner=trial_runner)

        all_trials = trial_runner.get_live_trials()

        used_cpus_and_gpus = [self._get_used_cpus_and_gpus(t) for t in all_trials]
        used_cpus, used_gpus = zip(*used_cpus_and_gpus)
        used_cpus = sum(used_cpus)
        used_gpus = sum(used_gpus)

        added_bundles = self._get_new_added_bundles(
            trial,
            all_trials,
            base_bundles,
            increase_by,
            total_available_cpus,
            total_available_gpus,
            used_cpus,
            used_gpus,
        )

        new_bundles = self._add_two_bundles(
            base_bundles, added_bundles, increase_by, False
        )

        pgf = PlacementGroupFactory(
            new_bundles,
            strategy=base_trial_resource.strategy,
            *base_trial_resource._args,
            **base_trial_resource._kwargs,
        )
        pgf._head_bundle_is_empty = base_trial_resource._head_bundle_is_empty
        return pgf


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
        add_bundles: If True, create new bundles from free resources.
            Otherwise, spread them among base_trial_resource bundles.
        increase_by: A dict with key-value
            pairs representing an atomic unit of resources (name-amount)
            the trial will be increased by. If not set, the trial will
            increase by 1 CPU/GPU.
        increase_by_times: If set to >=1 and ``increase_by`` is set,
            the trial will increase by maximum of
            ``increase_by_times * increase_by`` resources. If set to <1,
            no upper limit is set. Ignored if ``increase_by`` is not set.
        reserve_resources: A dict of
            resource_name-amount pairs representing the resources
            that will not be allocated to resized trials.
            is that the attribute should increase monotonically.
        metric: The training result objective value attribute. Stopping
            procedures will use this attribute. If None, will use the metric
            of the scheduler.
        mode: One of {min, max}. Determines whether objective is
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
            raise ValueError("The mode parameter can only be either min or max.")
        if self.mode == "max":
            return 1.0
        return -1.0

    def _get_new_added_bundles(
        self,
        trial: Trial,
        all_trials: List[Trial],
        base_bundles: List[Dict[str, float]],
        increase_by: Dict[str, float],
        total_available_cpus: float,
        total_available_gpus: float,
        used_cpus: float,
        used_gpus: float,
    ) -> List[Dict[str, float]]:
        if self.metric is None:
            raise ValueError(
                "The metric parameter cannot be None. The parameter can be set in "
                "either `DistributeResourcesToTopJob`, the base scheduler or in "
                "`tune.TuneConfig()` (highest to lowest priority)."
            )

        free_cpus = total_available_cpus - used_cpus
        free_gpus = total_available_gpus - used_gpus

        sorted_trials = sorted(
            all_trials,
            key=lambda t: -self._metric_op * t.last_result.get(self.metric, np.inf),
        )

        added_bundles = self._get_added_bundles(
            trial.placement_group_factory.bundles, base_bundles
        )

        best_trial = next(
            (
                t
                for t in sorted_trials
                if self._are_bundles_below_limit(
                    t.placement_group_factory.bundles, base_bundles
                )
            ),
            sorted_trials[0],
        )

        if (
            trial.trial_id != best_trial.trial_id
            # Only reduce resources here
            and self._get_multiplier(increase_by, free_cpus, free_gpus) >= 0
        ):
            return added_bundles

        return self._modify_bundles_with_free_resources(
            added_bundles,
            increase_by,
            free_cpus,
            free_gpus,
        )


_DistributeResourcesDefault = DistributeResources(add_bundles=False)
_DistributeResourcesDistributedDefault = DistributeResources(add_bundles=True)


@PublicAPI(stability="beta")
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
    :doc:`/tune/examples/includes/xgboost_dynamic_resources_example`.

    If the functional API is used, the current trial resources can be obtained
    by calling `tune.get_trial_resources()` inside the training function.
    The function should be able to
    :ref:`load and save checkpoints <tune-function-checkpointing>`
    (the latter preferably every iteration).

    If the Trainable (class) API is used, you can obtain the current trial
    resources through the ``Trainable.trial_resources`` property.

    Cannot be used if ``reuse_actors`` is True in ``tune.TuneConfig()``. A ValueError
    will be raised in that case.

    Args:
        base_scheduler: The scheduler to provide decisions
            about trials. If None, a default FIFOScheduler will be used.
        resources_allocation_function: The callable used to change
            live trial resource requiements during tuning. This callable
            will be called on each trial as it finishes one step of training.
            The callable must take four arguments: ``TrialRunner``, current
            ``Trial``, current result :class:`dict` and the
            ``ResourceChangingScheduler`` calling it. The callable must
            return a ``PlacementGroupFactory``, ``Resources``, :class:`dict`
            or None (signifying no need for an update). If
            ``resources_allocation_function`` is None, no resource
            requirements will be changed at any time.
            By default, :class:`DistributeResources` will be used,
            distributing available CPUs and GPUs over all running trials
            in a robust way, without any prioritization.

    Warning:
        If the ``resources_allocation_function`` sets trial resource
        requirements to values bigger than possible, the trial will
        not run. Ensure that your callable accounts for that possibility
        by setting upper limits. Consult :class:`DistributeResources`
        to see how that may be done.

    Example:
        .. code-block:: python

            base_scheduler = ASHAScheduler(max_t=16)
            def my_resources_allocation_function(
                trial_runner: "trial_runner.TrialRunner",
                trial: Trial,
                result: Dict[str, Any],
                scheduler: "ResourceChangingScheduler"
            ) -> Optional[Union[PlacementGroupFactory, Resource]]:
                # logic here
                # usage of PlacementGroupFactory is strongly preferred
                return PlacementGroupFactory(...)
            scheduler = ResourceChangingScheduler(
                            base_scheduler,
                            my_resources_allocation_function
                        )

        See :doc:`/tune/examples/includes/xgboost_dynamic_resources_example` for a
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
                Optional[Union[PlacementGroupFactory, Resources]],
            ]
        ] = _DistributeResourcesDefault,
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
            Trial, Optional[Union[dict, PlacementGroupFactory]]
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
                "`reuse_actors=False` in `tune.TuneConfig()`."
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
            logger.info(
                f"Setting trial {trial} resource to {new_resources} "
                f"with {new_resources._bundles}"
            )
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
    ) -> Optional[Union[dict, PlacementGroupFactory]]:
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
