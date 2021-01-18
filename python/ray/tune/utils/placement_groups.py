from collections import defaultdict
from typing import Dict, Optional, Set, Tuple
import os
import time

import ray
from ray import ObjectRef
from ray.actor import ActorClass
from ray.tune.resources import PlacementGroupFactory
from ray.tune.trial import Trial
from ray.util.placement_group import PlacementGroup

TUNE_MAX_PENDING_TRIALS_PG = int(os.getenv("TUNE_MAX_PENDING_TRIALS_PG", 1000))
# Seconds we wait for a trial to come up before we make blocking calls
# to process events
TUNE_TRIAL_STARTUP_GRACE_PERIOD = float(
    os.getenv("TUNE_TRIAL_STARTUP_GRACE_PERIOD", 10.))


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
        self._in_use_pgs: Dict[PlacementGroup, Trial] = {}
        self._in_use_trials: Dict[Trial, PlacementGroup] = {}

        # Latest PG staging time to check if still in grace period.
        self._latest_staging_start_time = time.time()

    def stage_trial_pg(self, pgf: PlacementGroupFactory):
        """Stage a trial placement group.

        Create the trial placement group if maximum number of pending
        placement groups is not exhausted.

        Args:
            pgf (PlacementGroupFactory): Placement group factory to stage.

        Returns:
            False if placement group has not been staged, True otherwise.

        Creates placement group and moves it to `self._staging`.
        """
        if not self.can_stage():
            return False

        pg = pgf()  # This creates the placement group

        self._staging[pgf].add(pg)
        self._staging_futures[pg.ready()] = (pgf, pg)

        self._latest_staging_start_time = time.time()

        return True

    def can_stage(self):
        """Return True if we can stage another placement group."""
        return len(self._staging) < TUNE_MAX_PENDING_TRIALS_PG

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

    def get_full_actor_cls(self, trial: Trial,
                           actor_cls: ActorClass) -> Optional[ActorClass]:
        """Get a fully configured actor class.

        Returns the actor handle if the placement group is ready. In this case,
        the placement group is moved to `self._in_use_pgs` and removed from
        `self._ready`.

        Args:
            trial (Trial): Trial object to start
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
        num_gpus = first_bundle.get("GPU", None)

        # Only custom resources remain in `first_bundle`
        resources = first_bundle or None

        return actor_cls.options(
            placement_group=pg,
            placement_group_bundle_index=0,
            num_cpus=num_cpus,
            num_gpus=num_gpus,
            resources=resources)

    def has_ready(self, pgf: PlacementGroupFactory) -> bool:
        """Return True if placement group is ready.

        Args:
            pgf (PlacementGroupFactory): PlacementGroupFactory object.

        Returns:
            Boolean.

        """
        return bool(self._ready[pgf])

    def trial_in_use(self, trial: Trial):
        return trial in self._in_use_trials

    def clean_trial_placement_group(self,
                                    trial: Trial) -> Optional[PlacementGroup]:
        """Remove reference to placement groups associated with a trial.

        Returns an associated placement group. If the trial was scheduled, this
        is the placement group it was scheduled on. If the trial was not
        scheduled, it will first try to return a staging placement group. If
        there is no staging placement group, it will return a ready placement
        group that is not yet being used by another trial.

        Args:
            trial (Trial): Trial object.

        Returns:
            PlacementGroup or None.

        """
        pgf = trial.placement_group_factory

        trial_pg = None

        if trial in self._in_use_trials:
            # Trial was in use. Just return its placement group.
            trial_pg = self._in_use_trials.pop(trial)
            self._in_use_pgs.pop(trial_pg)
        else:
            # Trial was not in use. If there are pending placement groups
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
        return self._staging_futures and time.time(
        ) <= self._latest_staging_start_time + TUNE_TRIAL_STARTUP_GRACE_PERIOD
