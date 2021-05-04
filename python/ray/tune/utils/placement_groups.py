from collections import defaultdict
from inspect import signature
import json
import os
import time
from typing import Dict, List, Optional, Set, TYPE_CHECKING, Tuple
import uuid

import ray
from ray import ObjectRef, logger
from ray.actor import ActorClass
from ray.tune.resources import Resources
from ray.util.placement_group import PlacementGroup, get_placement_group, \
    placement_group, placement_group_table, remove_placement_group

if TYPE_CHECKING:
    from ray.tune.trial import Trial

TUNE_PLACEMENT_GROUP_REMOVAL_DELAY = 2.

_tune_pg_prefix = None


def get_tune_pg_prefix():
    """Get the tune placement group name prefix.

    This will store the prefix in a global variable so that subsequent runs
    can use this identifier to clean up placement groups before starting their
    run.

    Can be overwritten with the ``TUNE_PLACEMENT_GROUP_PREFIX`` env variable.
    """
    global _tune_pg_prefix

    if _tune_pg_prefix:
        return _tune_pg_prefix

    # Else: check env variable
    env_prefix = os.getenv("TUNE_PLACEMENT_GROUP_PREFIX", "")

    if env_prefix:
        _tune_pg_prefix = env_prefix
        return _tune_pg_prefix

    # Else: create and store unique prefix
    _tune_pg_prefix = f"__tune_{uuid.uuid4().hex[:8]}__"
    return _tune_pg_prefix


class PlacementGroupFactory:
    """Wrapper class that creates placement groups for trials.

    This function should be used to define resource requests for Ray Tune
    trials. It holds the parameters to create placement groups.
    At a minimum, this will hold at least one bundle specifying the
    resource requirements for each trial:

    .. code-block:: python

        from ray import tune

        tune.run(
            train,
            tune.PlacementGroupFactory([
                {"CPU": 1, "GPU": 0.5, "custom_resource": 2}
            ]))

    If the trial itself schedules further remote workers, the resource
    requirements should be specified in additional bundles. You can also
    pass the placement strategy for these bundles, e.g. to enforce
    co-located placement:

    .. code-block:: python

        from ray import tune

        tune.run(
            train,
            resources_per_trial=tune.PlacementGroupFactory([
                {"CPU": 1, "GPU": 0.5, "custom_resource": 2},
                {"CPU": 2},
                {"CPU": 2},
            ], strategy="PACK"))

    The example above will reserve 1 CPU, 0.5 GPUs and 2 custom_resources
    for the trainable itself, and reserve another 2 bundles of 2 CPUs each.
    The trial will only start when all these resources are available. This
    could be used e.g. if you had one learner running in the main trainable
    that schedules two remote workers that need access to 2 CPUs each.

    Args:
        bundles(List[Dict]): A list of bundles which
            represent the resources requirements.
        strategy(str): The strategy to create the placement group.

         - "PACK": Packs Bundles into as few nodes as possible.
         - "SPREAD": Places Bundles across distinct nodes as even as possible.
         - "STRICT_PACK": Packs Bundles into one node. The group is
           not allowed to span multiple nodes.
         - "STRICT_SPREAD": Packs Bundles across distinct nodes.
        *args: Passed to the call of ``placement_group()``
        **kwargs: Passed to the call of ``placement_group()``

    """

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

    @property
    def head_cpus(self):
        return self._bundles[0].get("CPU", None)

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
        kwargs.update(self._bound.kwargs)
        # Call with bounded *args and **kwargs
        return placement_group(*self._bound.args, **kwargs)

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

    spec = spec.copy()
    extra_custom = spec.pop("extra_custom_resources", {}) or {}

    if any(k.startswith("extra_") and spec[k]
           for k in spec) or any(extra_custom[k] for k in extra_custom):
        raise ValueError(
            "Passing `extra_*` resource requirements to `resources_per_trial` "
            "is deprecated. Please use a `PlacementGroupFactory` object "
            "to define your resource requirements instead.")

    cpus = spec.pop("cpu", 0.)
    gpus = spec.pop("gpu", 0.)
    memory = spec.pop("memory", 0.)
    object_store_memory = spec.pop("object_store_memory", 0.)

    bundle = {k: v for k, v in spec.pop("custom_resources", {}).items()}

    bundle.update({
        "CPU": cpus,
        "GPU": gpus,
        "memory": memory,
        "object_store_memory": object_store_memory
    })

    return PlacementGroupFactory([bundle])


class PlacementGroupManager:
    """PlacementGroupManager to stage and manage placement groups.

    .. versionadded:: 1.3.0

    This class schedules placement groups for trials, keeps track of
    their state, and can return a fully configured actor class using
    this placement group.

    If two trials share the same placement group factory, both could use
    resulting placement groups from it. Thus this manager associates
    placement groups with their factory methods.

    Args:
        prefix (str): Prefix for the placement group names that are created.
    """

    def __init__(self, prefix: str = "__tune__", max_staging: int = 1000):
        self._prefix = prefix

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

        # Placement groups used by remote actors but not trials
        # (e.g. for reuse_actors=True)
        self._cached_pgs: Dict[PlacementGroup, PlacementGroupFactory] = {}

        # Placement groups scheduled for delayed removal.
        self._pgs_for_removal: Dict[PlacementGroup, float] = {}
        self._removal_delay = TUNE_PLACEMENT_GROUP_REMOVAL_DELAY

        # Latest PG staging time to check if still in grace period.
        self._latest_staging_start_time = time.time()

        # Seconds we wait for a trial to come up before we make blocking calls
        # to process events
        self._grace_period = float(
            os.getenv("TUNE_TRIAL_STARTUP_GRACE_PERIOD", 10.))

        self._max_staging = max_staging

    def set_max_staging(self, max_staging: int):
        self._max_staging = max_staging

    def remove_pg(self, pg: PlacementGroup):
        """Schedule placement group for (delayed) removal.

        Args:
            pg (PlacementGroup): Placement group object.

        """
        self._pgs_for_removal[pg] = time.time()

    def cleanup(self, force: bool = False):
        """Remove placement groups that are scheduled for removal.

        Currently, this will remove placement groups after they've been
        marked for removal for ``self._removal_delay`` seconds.
        If ``force=True``, this condition is disregarded and all placement
        groups are removed instead.

        Args:
            force (bool): If True, all placement groups scheduled for removal
                will be removed, disregarding any removal conditions.

        """
        # Wrap in list so we can modify the dict
        for pg in list(self._pgs_for_removal):
            if force or (time.time() -
                         self._removal_delay) >= self._pgs_for_removal[pg]:
                self._pgs_for_removal.pop(pg)
                remove_placement_group(pg)

    def cleanup_existing_pg(self, block: bool = False):
        """Clean up (remove) all existing placement groups.

        This scans through the placement_group_table to discover existing
        placement groups and calls remove_placement_group on all that
        match the ``_tune__`` prefix. This method is called at the beginning
        of the tuning run to clean up existing placement groups should the
        experiment be interrupted by a driver failure and resumed in the
        same driver script.

        Args:
            block (bool): If True, will wait until all placement groups are
                shut down.
        """
        should_cleanup = not int(
            os.getenv("TUNE_PLACEMENT_GROUP_CLEANUP_DISABLED", "0"))
        if should_cleanup:
            has_non_removed_pg_left = True
            while has_non_removed_pg_left:
                has_non_removed_pg_left = False
                for pid, info in placement_group_table().items():
                    if not info["name"].startswith(self._prefix):
                        continue
                    if info["state"] == "REMOVED":
                        continue
                    # If block=False, only run once
                    has_non_removed_pg_left = block
                    pg = get_placement_group(info["name"])
                    remove_placement_group(pg)
                time.sleep(0.1)

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
        return self._stage_pgf_pg(pgf)

    def _stage_pgf_pg(self, pgf: PlacementGroupFactory):
        """Create placement group for factory"""
        # This creates the placement group
        pg = pgf(name=f"{self._prefix}{uuid.uuid4().hex[:8]}")

        self._staging[pgf].add(pg)
        self._staging_futures[pg.ready()] = (pgf, pg)

        self._latest_staging_start_time = time.time()

        return True

    def can_stage(self):
        """Return True if we can stage another placement group."""
        return len(self._staging_futures) < self._max_staging

    def update_status(self):
        """Update placement group status.

        Moves ready placement groups from `self._staging` to
        `self._ready`.
        """
        self.cleanup()
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

        if num_cpus is None:
            # If the placement group specifically set the number
            # of CPUs to 0, use this.
            num_cpus = pgf.head_cpus

        logger.debug(f"For trial {trial} use pg {pg.id}")

        return actor_cls.options(
            placement_group=pg,
            placement_group_bundle_index=0,
            num_cpus=num_cpus,
            num_gpus=num_gpus,
            resources=resources)

    def has_ready(self, trial: "Trial", update: bool = False) -> bool:
        """Return True if placement group for trial is ready.

        Args:
            trial (Trial): :obj:`Trial` object.
            update (bool): Update status first.

        Returns:
            Boolean.

        """
        if update:
            self.update_status()
        return bool(self._ready[trial.placement_group_factory])

    def trial_in_use(self, trial: "Trial"):
        return trial in self._in_use_trials

    def cache_trial_pg(self, trial: "Trial") -> Optional[PlacementGroup]:
        """Disassociated placement group from trial object.

        This can be used to move placement groups into a cache so that
        they can be reused by other trials. The difference to just making
        them broadly available again is that they have to be specifically
        re-assigned to a trial via :meth:`assign_cached_pg`. The reason
        for this is that remote actors might already be scheduled on this
        placement group, so it should only be associated to the trial that
        actually re-uses the remote actor (e.g. when using ``reuse_trials``).

        This will replace (unstage) an existing placement group with the same
        factory object. If this is unsuccessful (e.g. because no such
        pending placement group exists), the placement group will *not* be
        cached and None will be returned.

        Args:
            trial (Trial): Trial object with the (currently in use) placement
                group that should be cached.

        Returns:
            PlacementGroup object that was cached or None if
                no placement group was replaced.

        """
        if not trial.uses_placement_groups:
            return None

        pgf = trial.placement_group_factory

        staged_pg = self._unstage_unused_pg(pgf)
        if not staged_pg:
            return None

        self.remove_pg(staged_pg)

        pg = self._in_use_trials.pop(trial)
        self._in_use_pgs.pop(pg)

        self._cached_pgs[pg] = trial.placement_group_factory
        return pg

    def assign_cached_pg(self, pg: PlacementGroup, trial: "Trial") -> bool:
        """Assign a cached pg to a trial."""
        pgf = self._cached_pgs.pop(pg)
        trial_pgf = trial.placement_group_factory

        assert pgf == trial_pgf, f"Cannot assign placement group with a " \
                                 f"non-matching factory to trial {trial}"

        logger.debug(f"For trial {trial} RE-use pg {pg.id}")

        self._in_use_pgs[pg] = trial
        self._in_use_trials[trial] = pg

        return True

    def clean_cached_pg(self, pg: PlacementGroup):
        self._cached_pgs.pop(pg)

    def return_or_clean_cached_pg(self, pg: PlacementGroup):
        """Return cached pg, making it available for other trials to use.

        This will try to replace another staged placement group. If this
        is unsuccessful, destroy the placement group instead.

        Args:
            pg (PlacementGroup): Return this cached placement group.

        Returns:
            Boolean indicating if the placement group was returned (True)
                or destroyed (False)
        """
        pgf = self._cached_pgs.pop(pg)

        # Replace staged placement group
        staged_pg = self._unstage_unused_pg(pgf)

        # Could not replace
        if not staged_pg:
            self.remove_pg(pg)
            return False

        # Replace successful
        self.remove_pg(staged_pg)
        self._ready[pgf].add(pg)
        return True

    def return_pg(self, trial: "Trial"):
        """Return pg, making it available for other trials to use.

        Args:
            trial (Trial): Return placement group of this trial.

        Returns:
            Boolean indicating if the placement group was returned.
        """
        if not trial.uses_placement_groups:
            return True

        pgf = trial.placement_group_factory

        pg = self._in_use_trials.pop(trial)
        self._in_use_pgs.pop(pg)
        self._ready[pgf].add(pg)

        return True

    def _unstage_unused_pg(
            self, pgf: PlacementGroupFactory) -> Optional[PlacementGroup]:
        """Unstage an unsued (i.e. staging or ready) placement group.

        This method will find an unused placement group and remove it from
        the tracked pool of placement groups (including e.g. the
        staging futures). It will *not* call ``remove_placement_group()``
        on the placement group - that is up to the calling method to do.

        (The reason for this is that sometimes we would remove the placement
        group directly, but sometimes we would like to enqueue removal.)

        Args:
            pgf (PlacementGroupFactory): Placement group factory object.
                This method will try to remove a staged PG of this factory
                first, then settle for a ready but unused. If none exist,
                no placement group will be removed and None will be returned.

        Returns:
            Removed placement group object or None.

        """
        trial_pg = None

        # If there are pending placement groups
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

        if trial in self._in_use_trials:
            # "Trial" was in use. Just return its placement group.
            trial_pg = self._in_use_trials.pop(trial)
            self._in_use_pgs.pop(trial_pg)
        else:
            trial_pg = self._unstage_unused_pg(pgf)

        return trial_pg

    def in_staging_grace_period(self):
        return self._staging_futures and self._grace_period and time.time(
        ) <= self._latest_staging_start_time + self._grace_period

    def reconcile_placement_groups(self, trials: List["Trial"]):
        """Reconcile placement groups to match requirements.

        This will loop through all trials and count their statuses by
        placement group factory. This will make sure that only as many
        placement groups are needed as there are trials left to run.

        E.g. if PGF_A has 2 terminated, 1 errored, 2 paused, 1 running,
        and 3 pending trials, a total of 6 placement groups
        (paused+running+pending) should be in staging, use, or the cache.

        Args:
            trials (List[Trial]): List of trials.

        """
        # Keep track of the currently tracked placement groups
        current_counts: Dict[PlacementGroupFactory, int] = defaultdict(int)

        # Count number of expected placement groups
        pgf_expected: Dict[PlacementGroupFactory, int] = defaultdict(int)
        for trial in trials:
            if not trial.uses_placement_groups:
                continue

            # Count in-use placement groups
            if trial in self._in_use_trials:
                current_counts[trial.placement_group_factory] += 1

            pgf_expected[trial.placement_group_factory] += \
                1 if trial.status in ["PAUSED", "PENDING", "RUNNING"] else 0

        # Count cached placement groups
        for pg, pgf in self._cached_pgs.items():
            current_counts[pgf] += 1

        # Compare current with expected
        for pgf, expected in pgf_expected.items():
            # Add staging and ready pgs
            current_counts[pgf] += len(self._staging[pgf])
            current_counts[pgf] += len(self._ready[pgf])

            while current_counts[pgf] > expected:
                pg = self._unstage_unused_pg(pgf)
                if not pg:
                    break
                logger.debug(f"Removing unneeded placement group {pg.id}")
                self.remove_pg(pg)
                current_counts[pgf] -= 1

            while expected > current_counts[pgf]:
                self._stage_pgf_pg(pgf)
                current_counts[pgf] += 1
                logger.debug(f"Adding an expected but previously unstaged "
                             f"placement group for factory {pgf}")

    def occupied_resources(self):
        """Return a dictionary of currently in-use resources."""
        resources = {"CPU": 0, "GPU": 0}
        for pg in self._in_use_pgs:
            for bundle_resources in pg.bundle_specs:
                for key, val in bundle_resources.items():
                    resources[key] = resources.get(key, 0) + val

        return resources

    def total_used_resources(self, committed_resources: Resources) -> dict:
        """Dict of total used resources incl. placement groups

        Args:
            committed_resources (Resources): Additional commited resources
                from (legacy) Ray Tune resource management.
        """
        committed = committed_resources._asdict()

        # Make dict compatible with pg resource dict
        committed.pop("has_placement_group", None)
        committed["CPU"] = committed.pop("cpu", 0) + committed.pop(
            "extra_cpu", 0)
        committed["GPU"] = committed.pop("gpu", 0) + committed.pop(
            "extra_gpu", 0)
        committed["memory"] += committed.pop("extra_memory", 0.)
        committed["object_store_memory"] += committed.pop(
            "extra_object_store_memory", 0.)

        custom = committed.pop("custom_resources", {})
        extra_custom = committed.pop("extra_custom_resources", {})

        for k, v in extra_custom.items():
            custom[k] = custom.get(k, 0.) + v

        committed.update(custom)

        pg_resources = self.occupied_resources()

        for k, v in committed.items():
            pg_resources[k] = pg_resources.get(k, 0.) + v

        return pg_resources
