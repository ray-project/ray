from typing import Dict, List, Optional, Set, TYPE_CHECKING, Tuple, Union
from collections import defaultdict
from inspect import signature
from copy import deepcopy
import json
import os
import time
import uuid

import ray
from ray import ObjectRef, logger
from ray.actor import ActorClass
from ray.tune.resources import Resources
from ray.util.annotations import PublicAPI, DeveloperAPI
from ray.util.placement_group import (
    PlacementGroup,
    get_placement_group,
    placement_group,
    placement_group_table,
    remove_placement_group,
)

if TYPE_CHECKING:
    from ray.tune.experiment import Trial

TUNE_PLACEMENT_GROUP_REMOVAL_DELAY = 2.0

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


@PublicAPI(stability="beta")
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

    If the trainable itself doesn't require resources.
    You can specify it as:

    .. code-block:: python

        from ray import tune

        tune.run(
            train,
            resources_per_trial=tune.PlacementGroupFactory([
                {},
                {"CPU": 2},
                {"CPU": 2},
            ], strategy="PACK"))

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

    def __init__(
        self,
        bundles: List[Dict[str, Union[int, float]]],
        strategy: str = "PACK",
        *args,
        **kwargs,
    ):
        assert (
            len(bundles) > 0
        ), "Cannot initialize a PlacementGroupFactory with zero bundles."

        self._bundles = [
            {k: float(v) for k, v in bundle.items() if v != 0} for bundle in bundles
        ]

        if not self._bundles[0]:
            # This is when trainable itself doesn't need resources.
            self._head_bundle_is_empty = True
            self._bundles.pop(0)
        else:
            self._head_bundle_is_empty = False

        self._strategy = strategy
        self._args = args
        self._kwargs = kwargs

        self._hash = None
        self._bound = None

        self._bind()

    @property
    def head_bundle_is_empty(self):
        """Returns True if head bundle is empty while child bundles
        need resources.

        This is considered an internal API within Tune.
        """
        return self._head_bundle_is_empty

    @property
    @DeveloperAPI
    def head_cpus(self) -> float:
        return 0.0 if self._head_bundle_is_empty else self._bundles[0].get("CPU", 0.0)

    @property
    @DeveloperAPI
    def bundles(self) -> List[Dict[str, float]]:
        """Returns a deep copy of resource bundles"""
        return deepcopy(self._bundles)

    @property
    def required_resources(self) -> Dict[str, float]:
        """Returns a dict containing the sums of all resources"""
        resources = {}
        for bundle in self._bundles:
            for k, v in bundle.items():
                resources[k] = resources.get(k, 0) + v
        return resources

    def _bind(self):
        sig = signature(placement_group)
        try:
            self._bound = sig.bind(
                self._bundles, self._strategy, *self._args, **self._kwargs
            )
        except Exception as exc:
            raise RuntimeError(
                "Invalid definition for placement group factory. Please check "
                "that you passed valid arguments to the PlacementGroupFactory "
                "object."
            ) from exc

    def __call__(self, *args, **kwargs):
        kwargs.update(self._bound.kwargs)
        # Call with bounded *args and **kwargs
        return placement_group(*self._bound.args, **kwargs)

    def __eq__(self, other: "PlacementGroupFactory"):
        return (
            self._bound == other._bound
            and self.head_bundle_is_empty == other.head_bundle_is_empty
        )

    def __hash__(self):
        if not self._hash:
            # Cache hash
            self._hash = hash(
                json.dumps(
                    {"args": self._bound.args, "kwargs": self._bound.kwargs},
                    sort_keys=True,
                    indent=0,
                    ensure_ascii=True,
                )
            )
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

    def __repr__(self) -> str:
        return (
            f"<PlacementGroupFactory (_bound={self._bound}, "
            f"head_bundle_is_empty={self.head_bundle_is_empty})>"
        )


def resource_dict_to_pg_factory(spec: Optional[Dict[str, float]]):
    spec = spec or {"cpu": 1}

    if isinstance(spec, Resources):
        spec = spec._asdict()

    spec = spec.copy()

    cpus = spec.pop("cpu", 0.0)
    gpus = spec.pop("gpu", 0.0)
    memory = spec.pop("memory", 0.0)
    object_store_memory = spec.pop("object_store_memory", 0.0)

    bundle = {k: v for k, v in spec.pop("custom_resources", {}).items()}

    bundle.update(
        {
            "CPU": cpus,
            "GPU": gpus,
            "memory": memory,
            "object_store_memory": object_store_memory,
        }
    )

    return PlacementGroupFactory([bundle])


class _PlacementGroupManager:
    """PlacementGroupManager to stage and manage placement groups.

    .. versionadded:: 1.3.0

    This class schedules placement groups for trials, keeps track of
    their state, and can return a fully configured actor class using
    this placement group.

    If two trials share the same placement group factory, both could use
    resulting placement groups from it. Thus this manager associates
    placement groups with their factory methods.

    Args:
        prefix: Prefix for the placement group names that are created.
    """

    def __init__(self, prefix: str = "__tune__", max_staging: int = 1000):
        self._prefix = prefix

        # Sets of staged placement groups by factory
        self._staging: Dict[PlacementGroupFactory, Set[PlacementGroup]] = defaultdict(
            set
        )
        # Sets of ready and unused placement groups by factory
        self._ready: Dict[PlacementGroupFactory, Set[PlacementGroup]] = defaultdict(set)
        # Ray futures to check if a placement group is ready
        self._staging_futures: Dict[
            ObjectRef, Tuple[PlacementGroupFactory, PlacementGroup]
        ] = {}

        # Cache of unstaged PGs (cleaned after full PG removal)
        self._unstaged_pg_pgf: Dict[PlacementGroup, PlacementGroupFactory] = {}
        self._unstaged_pgf_pg: Dict[
            PlacementGroupFactory, Set[PlacementGroup]
        ] = defaultdict(set)

        # Placement groups used by trials
        self._in_use_pgs: Dict[PlacementGroup, "Trial"] = {}
        self._in_use_trials: Dict["Trial", PlacementGroup] = {}

        # Placement groups used by remote actors but not trials
        # (e.g. for reuse_actors=True)
        self._cached_pgs: Dict[PlacementGroup, PlacementGroupFactory] = {}

        # Placement groups scheduled for delayed removal.
        # This is used as a damper to filter out some high frequency change
        # in resources request.
        # Only PGs that have never been used go here.
        # TODO(xwjiang): `self._pgs_for_removal` and `self._unstaged_xxx`
        #  are really the same now. We should consolidate to using one.
        #  Also `remove_placement_group` method should just be combined with
        #  `unstage_unused_xxx`.
        self._pgs_for_removal: Dict[PlacementGroup, float] = {}
        self._removal_delay = TUNE_PLACEMENT_GROUP_REMOVAL_DELAY

        self._max_staging = max_staging

    def set_max_staging(self, max_staging: int):
        self._max_staging = max_staging

    def remove_pg(self, pg: PlacementGroup):
        """Schedule placement group for (delayed) removal.

        Args:
            pg: Placement group object.

        """
        self._pgs_for_removal[pg] = time.time()

    def cleanup(self, force: bool = False):
        """Remove placement groups that are scheduled for removal.

        Currently, this will remove placement groups after they've been
        marked for removal for ``self._removal_delay`` seconds.
        If ``force=True``, this condition is disregarded and all placement
        groups are removed instead.

        Args:
            force: If True, all placement groups scheduled for removal
                will be removed, disregarding any removal conditions.

        """
        # Wrap in list so we can modify the dict
        for pg in list(self._pgs_for_removal):
            if (
                force
                or (time.time() - self._removal_delay) >= self._pgs_for_removal[pg]
            ):
                self._pgs_for_removal.pop(pg)

                remove_placement_group(pg)

                # Remove from unstaged cache
                if pg in self._unstaged_pg_pgf:
                    pgf = self._unstaged_pg_pgf.pop(pg)
                    self._unstaged_pgf_pg[pgf].discard(pg)

    def cleanup_existing_pg(self, block: bool = False):
        """Clean up (remove) all existing placement groups.

        This scans through the placement_group_table to discover existing
        placement groups and calls remove_placement_group on all that
        match the ``_tune__`` prefix. This method is called at the beginning
        of the tuning run to clean up existing placement groups should the
        experiment be interrupted by a driver failure and resumed in the
        same driver script.

        Args:
            block: If True, will wait until all placement groups are
                shut down.
        """
        should_cleanup = not int(
            os.getenv("TUNE_PLACEMENT_GROUP_CLEANUP_DISABLED", "0")
        )
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

                    # Remove from unstaged cache
                    if pg in self._unstaged_pg_pgf:
                        pgf = self._unstaged_pg_pgf.pop(pg)
                        self._unstaged_pgf_pg[pgf].discard(pg)

                time.sleep(0.1)

    def stage_trial_pg(self, trial: "Trial"):
        """Stage a trial placement group.

        Create the trial placement group if maximum number of pending
        placement groups is not exhausted.

        Args:
            trial: Trial whose placement group to stage.

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
        if len(self._unstaged_pgf_pg[pgf]) > 0:
            # This re-uses a previously unstaged placement group
            pg = self._unstaged_pgf_pg[pgf].pop()
            del self._unstaged_pg_pgf[pg]
            self._pgs_for_removal.pop(pg, None)
        else:
            # This creates the placement group
            pg = pgf(name=f"{self._prefix}{uuid.uuid4().hex[:8]}")

        self._staging[pgf].add(pg)
        self._staging_futures[pg.ready()] = (pgf, pg)

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
                self.handle_ready_future(ready_fut)

    def handle_ready_future(self, ready_fut):
        ready_pgf, ready_pg = self._staging_futures.pop(ready_fut)

        self._staging[ready_pgf].remove(ready_pg)
        self._ready[ready_pgf].add(ready_pg)

    def get_staging_future_list(self):
        return list(self._staging_futures.keys())

    def get_full_actor_cls(
        self, trial: "Trial", actor_cls: ActorClass
    ) -> Optional[ActorClass]:
        """Get a fully configured actor class.

        Returns the actor handle if the placement group is ready. In this case,
        the placement group is moved to `self._in_use_pgs` and removed from
        `self._ready`.

        Args:
            trial: "Trial" object to start
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

        logger.debug(f"For trial {trial} use pg {pg.id}")

        # We still have to pass resource specs
        if not pgf.head_bundle_is_empty:
            # Pass the full resource specs of the first bundle per default
            head_bundle = pg.bundle_specs[0].copy()
            num_cpus = head_bundle.pop("CPU", 0)
            num_gpus = head_bundle.pop("GPU", 0)
            memory = head_bundle.pop("memory", None)
            object_store_memory = head_bundle.pop("object_store_memory", None)

            # Only custom resources remain in `head_bundle`
            resources = head_bundle
            return actor_cls.options(
                placement_group=pg,
                placement_group_bundle_index=0,
                placement_group_capture_child_tasks=True,
                num_cpus=num_cpus,
                num_gpus=num_gpus,
                memory=memory,
                object_store_memory=object_store_memory,
                resources=resources,
            )
        else:
            return actor_cls.options(
                placement_group=pg,
                placement_group_capture_child_tasks=True,
                num_cpus=0,
                num_gpus=0,
                resources={},
            )

    def has_ready(self, trial: "Trial", update: bool = False) -> bool:
        """Return True if placement group for trial is ready.

        Args:
            trial: :obj:`Trial` object.
            update: Update status first.

        Returns:
            Boolean.

        """
        if update:
            self.update_status()
        return bool(self._ready[trial.placement_group_factory])

    def has_staging(self, trial: "Trial", update: bool = False) -> bool:
        """Return True if placement group for trial is staging.

        Args:
            trial: :obj:`Trial` object.
            update: Update status first.

        Returns:
            Boolean.

        """
        if update:
            self.update_status()
        return bool(self._staging[trial.placement_group_factory])

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
            trial: Trial object with the (currently in use) placement
                group that should be cached.

        Returns:
            PlacementGroup object that was cached or None if
                no placement group was replaced.

        """
        pgf = trial.placement_group_factory

        staged_pg = self._unstage_unused_pg(pgf)
        if not staged_pg and not self._unstaged_pgf_pg[pgf]:
            # If we have an unstaged placement group for this factory,
            # this might be the same one we unstaged previously. If so,
            # we should continue with the caching. If not, this will be
            # reconciled later.
            return None

        if staged_pg:
            self.remove_pg(staged_pg)

        pg = self._in_use_trials.pop(trial)
        self._in_use_pgs.pop(pg)

        self._cached_pgs[pg] = trial.placement_group_factory
        return pg

    def assign_cached_pg(self, pg: PlacementGroup, trial: "Trial") -> bool:
        """Assign a cached pg to a trial."""
        pgf = self._cached_pgs.pop(pg)
        trial_pgf = trial.placement_group_factory

        assert pgf == trial_pgf, (
            f"Cannot assign placement group with a "
            f"non-matching factory to trial {trial}"
        )

        logger.debug(f"For trial {trial} RE-use pg {pg.id}")

        self._in_use_pgs[pg] = trial
        self._in_use_trials[trial] = pg

        return True

    def clean_cached_pg(self, pg: PlacementGroup):
        self._cached_pgs.pop(pg)

    def has_cached_pg(self, pgf: PlacementGroupFactory):
        """Check if a placement group for given factory has been cached"""
        return any(cached_pgf == pgf for cached_pgf in self._cached_pgs.values())

    def remove_from_in_use(self, trial: "Trial") -> PlacementGroup:
        """Return pg back to Core scheduling.

        Args:
            trial: Return placement group of this trial.
        """

        pg = self._in_use_trials.pop(trial)
        self._in_use_pgs.pop(pg)

        return pg

    def _unstage_unused_pg(
        self, pgf: PlacementGroupFactory
    ) -> Optional[PlacementGroup]:
        """Unstage an unsued (i.e. staging or ready) placement group.

        This method will find an unused placement group and remove it from
        the tracked pool of placement groups (including e.g. the
        staging futures). It will *not* call ``remove_placement_group()``
        on the placement group - that is up to the calling method to do.

        (The reason for this is that sometimes we would remove the placement
        group directly, but sometimes we would like to enqueue removal.)

        Args:
            pgf: Placement group factory object.
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

            # Track unstaged placement groups for potential reuse
            self._unstaged_pg_pgf[trial_pg] = pgf
            self._unstaged_pgf_pg[pgf].add(trial_pg)

            del self._staging_futures[trial_future]

        elif self._ready[pgf]:
            # Otherwise, return an unused ready placement group.
            trial_pg = self._ready[pgf].pop()

        return trial_pg

    def reconcile_placement_groups(self, trials: List["Trial"]):
        """Reconcile placement groups to match requirements.

        This will loop through all trials and count their statuses by
        placement group factory. This will make sure that only as many
        placement groups are needed as there are trials left to run.

        E.g. if PGF_A has 2 terminated, 1 errored, 2 paused, 1 running,
        and 3 pending trials, a total of 6 placement groups
        (paused+running+pending) should be in staging, use, or the cache.

        Args:
            trials: List of trials.

        """
        # Keep track of the currently tracked placement groups
        current_counts: Dict[PlacementGroupFactory, int] = defaultdict(int)

        # Count number of expected placement groups
        pgf_expected: Dict[PlacementGroupFactory, int] = defaultdict(int)
        for trial in trials:
            # Count in-use placement groups
            if trial in self._in_use_trials:
                current_counts[trial.placement_group_factory] += 1

            pgf_expected[trial.placement_group_factory] += (
                1 if trial.status in ["PAUSED", "PENDING", "RUNNING"] else 0
            )

        # Ensure that unexpected placement groups are accounted for
        for pgf in self._staging:
            if pgf not in pgf_expected:
                pgf_expected[pgf] = 0

        for pgf in self._ready:
            if pgf not in pgf_expected:
                pgf_expected[pgf] = 0

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
                logger.debug(
                    f"Adding an expected but previously unstaged "
                    f"placement group for factory {pgf}"
                )

    def occupied_resources(self):
        """Return a dictionary of currently in-use resources."""
        resources = {"CPU": 0, "GPU": 0}
        for pg in self._in_use_pgs:
            for bundle_resources in pg.bundle_specs:
                for key, val in bundle_resources.items():
                    resources[key] = resources.get(key, 0) + val

        return resources
