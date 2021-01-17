from typing import Dict, Optional, Set
import os
import time

import ray
from ray import ObjectRef
from ray.actor import ActorClass
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

    It also allows switching placement groups associated with each trial,
    so trials that should be run before other trials whose placement
    groups are not ready, yet, can cut the queue.
    """

    def __init__(self):
        self._staging: Dict[Trial, PlacementGroup] = {}
        self._ready: Dict[Trial, PlacementGroup] = {}
        self._staging_futures: Dict[ObjectRef, Trial] = {}
        self._in_use: Set[PlacementGroup] = set()

        self._latest_staging_start_time = time.time()

    @property
    def available_trials(self):
        """Get trials with a ready placement group that have not been started.

        Yields: Trial objects.

        """
        for ready, pg in self._ready.items():
            if pg in self._in_use:
                continue
            yield ready

    def stage_trial(self, trial: Trial):
        """Stage a trial placement group.

        Create the trial placement group if maximum number of pending
        placement groups is not exhausted.

        Args:
            trial (Trial): Trial to stage.

        Returns:
            False if trial has not been staged, True otherwise.

        Creates its placement group and moves it to `self._staging`.
        """
        if trial in self._staging:
            raise RuntimeError(f"Trial {trial} already staged.")

        if not self.can_stage():
            return False

        pg = trial.placement_group_factory()

        self._staging[trial] = pg
        self._staging_futures[pg.ready()] = trial

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
                ready_trial = self._staging_futures.pop(ready_fut)
                ready_pg = self._staging.pop(ready_trial)
                self._ready[ready_trial] = ready_pg

    def get_full_actor_cls(self, trial: Trial,
                           actor_cls: ActorClass) -> Optional[ActorClass]:
        """Get a fully configured actor class.

        Returns the actor handle if the placement group is ready.

        Args:
            trial (Trial): trial object to start
            actor_cls: Ray actor class.

        Returns:
            Configured ActorClass or None

        """
        if trial not in self._ready:
            return None

        pg = self._ready[trial]
        self._in_use.add(pg)

        return actor_cls.options(
            placement_group=pg, placement_group_bundle_index=0)

    def get_trial_pg(self, trial: Trial) -> Optional[PlacementGroup]:
        """Get placement group associated with trial.

        Args:
            trial (Trial): Trial object.

        Returns:
            Placement group or None.

        """
        return self._ready.get(trial) or self._staging.get(trial)

    def switch_trial_pgs(self, first: Trial, second: Trial):
        """Switch placement groups associated with first and second trial.

        Args:
            first (Trial): Trial object.
            second (Trial): Trial object.

        """
        # First, switch staging futures
        # Note that zero, one, or both trials might be currently staging.
        first_fut = second_fut = None
        for future, trial in self._staging_futures.items():
            if trial is first:
                first_fut = future
            if trial is second:
                second_fut = future

        if first_fut:
            self._staging_futures[first_fut] = second

        if second_fut:
            self._staging_futures[second_fut] = first

        # Then switch the references to the placement groups
        if first in self._ready:
            if second in self._ready:
                first_pg = self._ready.pop(first)
                second_pg = self._ready.pop(second)
                self._ready[first] = second_pg
                self._ready[second] = first_pg
                return
            elif second in self._staging:
                first_pg = self._ready.pop(first)
                second_pg = self._staging.pop(second)
                self._staging[first] = second_pg
                self._ready[second] = first_pg
                return
        elif first in self._staging:
            if second in self._ready:
                first_pg = self._staging.pop(first)
                second_pg = self._ready.pop(second)
                self._ready[first] = second_pg
                self._staging[second] = first_pg
                return
            elif second in self._staging:
                first_pg = self._staging.pop(first)
                second_pg = self._staging.pop(second)
                self._staging[first] = second_pg
                self._staging[second] = first_pg
                return
        raise RuntimeError(
            f"Could not switch placement groups of trial {first} and {second}")

    def is_staging(self, trial: Trial) -> bool:
        """Return True if trial placement group is staging.

        Args:
            trial (Trial): Trial object.

        Returns:
            Boolean.

        """
        return trial in self._staging

    def is_ready(self, trial: Trial) -> bool:
        """Return True if trial placement group is ready.

        Args:
            trial (Trial): Trial object.

        Returns:
            Boolean.

        """
        return trial in self._ready

    def clean_trial_placement_group(self, trial: Trial):
        """Remove reference to placement groups associated with a trial.

        Args:
            trial (Trial): Trial object.

        Returns:
            None

        """
        pg = self._ready.pop(trial, None)
        if pg and pg in self._in_use:
            self._staging_futures.pop(pg, None)
            self._in_use.remove(pg)

        pg = self._staging.pop(trial, None)
        if pg and pg in self._in_use:
            self._staging_futures.pop(pg, None)
            self._in_use.remove(pg)

    def in_staging_grace_period(self):
        return self._staging and time.time(
        ) <= self._latest_staging_start_time + TUNE_TRIAL_STARTUP_GRACE_PERIOD
