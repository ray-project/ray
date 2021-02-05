import json
from collections import defaultdict
from inspect import signature
from typing import Dict, List, Optional, Set, TYPE_CHECKING, Tuple
import os
import time

import ray
from ray import ObjectRef
from ray.actor import ActorClass
from ray.tune.resources import Resources
from ray.util.placement_group import PlacementGroup, placement_group

if TYPE_CHECKING:
    from ray.tune.trial import Trial

TUNE_MAX_PENDING_TRIALS_PG = int(os.getenv("TUNE_MAX_PENDING_TRIALS_PG", 1000))


class PlacementGroupFactory:
    """Wrapper class to identify placement group factory methods."""

    def __init__(self,
                 bundles: List[Dict[str, float]],
                 strategy: str = "PACK",
                 *args,
                 **kwargs):
        self._bundles = bundles
        self._strategy = strategy
        self._args = args
        self._kwargs = kwargs

        self._hash = None
        self._bound = None

        self._bind()

    def _bind(self):
        sig = signature(placement_group)
        try:
            self._bound = sig.bind(self._bundles, self._strategy, *self._args,
                                   **self._kwargs)
        except Exception as exc:
            raise RuntimeError(
                "Invalid definition for placement group factory. Please check "
                "that you passed valid arguments to the PlacementGroupFactory "
                "object.") from exc

    def __call__(self, *args, **kwargs):
        # Call with bounded *args and **kwargs
        return placement_group(*self._bound.args, **self._bound.kwargs)

    def __eq__(self, other):
        return self._bound == other._bound

    def __hash__(self):
        if not self._hash:
            # Cache hash
            self._hash = hash(
                json.dumps(
                    {
                        "args": self._bound.args,
                        "kwargs": self._bound.kwargs
                    },
                    sort_keys=True,
                    indent=0,
                    ensure_ascii=True))
        return self._hash

    def __getstate__(self):
        state = self.__dict__.copy()
        state.pop("_hash", None)
        state.pop("_bound", None)
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._hash = None
        self._bound = None
        self._bind()


def resource_dict_to_pg_factory(spec: Optional[Dict[str, float]]):
    spec = spec or {"cpu": 1}

    if isinstance(spec, Resources):
        spec = spec._asdict()

    if any(k.startswith("extra_") and spec[k] for k in spec):
        raise ValueError(
            "Passing `extra_*` resource requirements to `resources_per_trial` "
            "is deprecated. Please use a `PlacementGroupFactory` object "
            "to define your resource requirements instead.")

    cpus = spec.pop("cpu", 0.)
    gpus = spec.pop("gpu", 0.)
    memory = spec.pop("memory", 0.)
    object_store_memory = spec.pop("object_store_memory", 0.)

    bundle = {k: v for k, v in spec.pop("custom_resources", {})}

    bundle.update({
        "CPU": cpus,
        "GPU": gpus,
        "memory": memory,
        "object_store_memory": object_store_memory
    })

    return PlacementGroupFactory([bundle])


class PlacementGroupManager:
    """PlacementGroupManager to stage and manage placement groups.

    This class schedules placement groups for trials, keeps track of
    their state, and can return a fully configured actor class using
    this placement group.

    If two trials share the same placement group factory, both could use
    resulting placement groups from it. Thus this manager associates
    placement groups with their factory methods.
    """

    def __init__(self):
        # Sets of staged placement groups by factory
        self._staging: Dict[PlacementGroupFactory, Set[
            PlacementGroup]] = defaultdict(set)
        # Sets of ready and unused placement groups by factory
        self._ready: Dict[PlacementGroupFactory, Set[
            PlacementGroup]] = defaultdict(set)
        # Ray futures to check if a placement group is ready
        self._staging_futures: Dict[ObjectRef, Tuple[PlacementGroupFactory,
                                                     PlacementGroup]] = {}

        # Placement groups used by trials
        self._in_use_pgs: Dict[PlacementGroup, "Trial"] = {}
        self._in_use_trials: Dict["Trial", PlacementGroup] = {}

        # Latest PG staging time to check if still in grace period.
        self._latest_staging_start_time = time.time()

        # Seconds we wait for a trial to come up before we make blocking calls
        # to process events
        self._grace_period = float(
            os.getenv("TUNE_TRIAL_STARTUP_GRACE_PERIOD", 10.))

    def stage_trial_pg(self, trial: "Trial"):
        """Stage a trial placement group.

        Create the trial placement group if maximum number of pending
        placement groups is not exhausted.

        Args:
            trial (Trial): Trial whose placement group to stage.

        Returns:
            False if placement group has not been staged, True otherwise.

        Creates placement group and moves it to `self._staging`.
        """
        if not self.can_stage():
            return False

        pgf = trial.placement_group_factory
        pg = pgf()  # This creates the placement group

        self._staging[pgf].add(pg)
        self._staging_futures[pg.ready()] = (pgf, pg)

        self._latest_staging_start_time = time.time()

        return True

    def can_stage(self):
        """Return True if we can stage another placement group."""
        return len(self._staging_futures) < TUNE_MAX_PENDING_TRIALS_PG

    def update_status(self):
        """Update placement group status.

        Moves ready placement groups from `self._staging` to
        `self._ready`.
        """
        ready = True
        while ready:
            # Use a loop as `ready` might return futures one by one
            ready, _ = ray.wait(list(self._staging_futures.keys()), timeout=0)

            for ready_fut in ready:
                ready_pgf, ready_pg = self._staging_futures.pop(ready_fut)

                self._staging[ready_pgf].remove(ready_pg)
                self._ready[ready_pgf].add(ready_pg)

    def get_full_actor_cls(self, trial: "Trial",
                           actor_cls: ActorClass) -> Optional[ActorClass]:
        """Get a fully configured actor class.

        Returns the actor handle if the placement group is ready. In this case,
        the placement group is moved to `self._in_use_pgs` and removed from
        `self._ready`.

        Args:
            trial ("Trial"): "Trial" object to start
            actor_cls: Ray actor class.

        Returns:
            Configured ActorClass or None

        """
        pgf = trial.placement_group_factory

        if not self._ready[pgf]:
            return None

        pg = self._ready[pgf].pop()
        self._in_use_pgs[pg] = trial
        self._in_use_trials[trial] = pg

        # We still have to pass resource specs
        # Pass the full resource specs of the first bundle per default
        first_bundle = pg.bundle_specs[0].copy()
        num_cpus = first_bundle.pop("CPU", None)
        num_gpus = first_bundle.pop("GPU", None)

        # Only custom resources remain in `first_bundle`
        resources = first_bundle or None

        return actor_cls.options(
            placement_group=pg,
            placement_group_bundle_index=0,
            num_cpus=num_cpus,
            num_gpus=num_gpus,
            resources=resources)

    def has_ready(self, trial: "Trial") -> bool:
        """Return True if placement group for trial is ready.

        Args:
            trial (Trial): :obj:`Trial` object.

        Returns:
            Boolean.

        """
        return bool(self._ready[trial.placement_group_factory])

    def trial_in_use(self, trial: "Trial"):
        return trial in self._in_use_trials

    def clean_trial_placement_group(
            self, trial: "Trial") -> Optional[PlacementGroup]:
        """Remove reference to placement groups associated with a trial.

        Returns an associated placement group. If the trial was scheduled, this
        is the placement group it was scheduled on. If the trial was not
        scheduled, it will first try to return a staging placement group. If
        there is no staging placement group, it will return a ready placement
        group that is not yet being used by another trial.

        Args:
            trial ("Trial"): :obj:`Trial` object.

        Returns:
            PlacementGroup or None.

        """
        pgf = trial.placement_group_factory

        trial_pg = None

        if trial in self._in_use_trials:
            # "Trial" was in use. Just return its placement group.
            trial_pg = self._in_use_trials.pop(trial)
            self._in_use_pgs.pop(trial_pg)
        else:
            # "Trial" was not in use. If there are pending placement groups
            # in staging, pop a random one.
            if self._staging[pgf]:
                trial_pg = self._staging[pgf].pop()

                # For staging placement groups, we will also need to
                # remove the future.
                trial_future = None
                for future, (pgf, pg) in self._staging_futures.items():
                    if pg == trial_pg:
                        trial_future = future
                        break
                del self._staging_futures[trial_future]

            elif self._ready[pgf]:
                # Otherwise, return an unused ready placement group.
                trial_pg = self._ready[pgf].pop()

        return trial_pg

    def in_staging_grace_period(self):
        return self._staging_futures and self._grace_period and time.time(
        ) <= self._latest_staging_start_time + self._grace_period
