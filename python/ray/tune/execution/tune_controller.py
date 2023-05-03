import copy
import time
import traceback
from collections import defaultdict, deque
from functools import partial
from typing import Any, Callable, Dict, List, Optional, Union, Tuple, Set

import logging
import os

import ray
from ray.air import Checkpoint, ResourceRequest
from ray.air.config import CheckpointConfig
from ray.air._internal.checkpoint_manager import CheckpointStorage, _TrackedCheckpoint
from ray.air.execution import ResourceManager, PlacementGroupResourceManager
from ray.air.execution._internal import RayActorManager, TrackedActor
from ray.exceptions import RayActorError
from ray.tune.error import _AbortTrialExecution
from ray.tune.execution.ray_trial_executor import _class_cache
from ray.tune.execution.trial_runner import _TuneControllerBase, TrialRunnerWrapper
from ray.tune.experiment.trial import (
    _change_working_directory,
    _noop_logger_creator,
    _TrialInfo,
    _Location,
    _get_trainable_kwargs,
)
from ray.tune.result import TRIAL_INFO, STDOUT_FILE, STDERR_FILE
from ray.tune.trainable import TrainableUtil
from ray.tune import TuneError
from ray.tune.callback import Callback
from ray.tune.schedulers import TrialScheduler
from ray.tune.stopper import Stopper
from ray.tune.search import SearchAlgorithm
from ray.tune.syncer import SyncConfig
from ray.tune.experiment import Trial
from ray.tune.utils import warn_if_slow
from ray.tune.utils.object_cache import _ObjectCache
from ray.tune.utils.resource_updater import _ResourceUpdater
from ray.util.annotations import DeveloperAPI
from ray.util.debug import log_once


logger = logging.getLogger(__name__)


@DeveloperAPI
class TuneController(_TuneControllerBase):
    def __init__(
        self,
        *,
        search_alg: Optional[SearchAlgorithm] = None,
        placeholder_resolvers: Optional[Dict[Tuple, Any]] = None,
        scheduler: Optional[TrialScheduler] = None,
        experiment_path: Optional[str] = None,
        experiment_dir_name: Optional[str] = None,
        sync_config: Optional[SyncConfig] = None,
        stopper: Optional[Stopper] = None,
        resume: Union[str, bool] = False,
        server_port: Optional[int] = None,
        fail_fast: bool = False,
        checkpoint_period: Union[str, int] = None,
        callbacks: Optional[List[Callback]] = None,
        metric: Optional[str] = None,
        trial_checkpoint_config: Optional[CheckpointConfig] = None,
        chdir_to_trial_dir: bool = False,
        reuse_actors: bool = False,
        resource_manager_factory: Optional[Callable[[], ResourceManager]] = None,
    ):
        if resource_manager_factory:
            self._resource_manager = resource_manager_factory()
        else:
            self._resource_manager = PlacementGroupResourceManager()

        self._actor_manager = RayActorManager(resource_manager=self._resource_manager)

        self._class_cache = _class_cache

        # Resource status
        self._resource_updater = _ResourceUpdater(None)

        # Actor <-> Trial mappings
        self._actor_to_trial: Dict[TrackedActor, Trial] = {}
        self._trial_to_actor: Dict[Trial, TrackedActor] = {}

        # Resources <-> Trial
        self._resources_to_pending_trials: Dict[
            ResourceRequest, Set[Trial]
        ] = defaultdict(set)

        # Keep track of actor states
        self._pending_trials: Set[Trial] = set()
        self._pending_trials_list: List[Trial] = []

        self._running_trials: Set[Trial] = set()

        self._paused_trials: Set[Trial] = set()

        self._stopped_trials: Set[Trial] = set()
        self._failed_trials: Set[Trial] = set()

        self._resetting_trials: Set[Trial] = set()
        self._staged_trials: Set[Trial] = set()

        # Removed actors
        self._started_actors: Set[TrackedActor] = set()
        self._stopping_actors: Dict[TrackedActor, float] = {}
        self._earliest_stopping_actor: float = float("inf")
        self._actor_cleanup_timeout: int = int(
            os.environ.get("TUNE_FORCE_TRIAL_CLEANUP_S", "600")
        )
        self._actor_force_cleanup_timeout: int = 10

        # Reuse actors
        self._reuse_actors = reuse_actors  # reuse_actors
        self._actor_cache = _ObjectCache(may_keep_one=True)

        # General trial behavior
        self._chdir_to_trial_dir = chdir_to_trial_dir

        # Trial metadata for experiment checkpoints
        self._trials_to_cache: Set[Trial] = set()
        self._trial_metadata: Dict[str, str] = {}

        # TRAINING
        self._buffer_length = int(os.getenv("TUNE_RESULT_BUFFER_LENGTH", 1))
        self._buffer_min_time_s = float(os.getenv("TUNE_RESULT_BUFFER_MIN_TIME_S", 0.0))
        self._buffer_max_time_s = float(
            os.getenv("TUNE_RESULT_BUFFER_MAX_TIME_S", 100.0)
        )

        super().__init__(
            search_alg=search_alg,
            placeholder_resolvers=placeholder_resolvers,
            scheduler=scheduler,
            experiment_path=experiment_path,
            experiment_dir_name=experiment_dir_name,
            sync_config=sync_config,
            stopper=stopper,
            resume=resume,
            server_port=server_port,
            fail_fast=fail_fast,
            checkpoint_period=checkpoint_period,
            callbacks=callbacks,
            metric=metric,
            trial_checkpoint_config=trial_checkpoint_config,
        )

    def _wrapped(self):
        """Return wrapped tune controller to be passed to scheduler/searchers."""
        return TrialRunnerWrapper(
            self,
            trial_executor=_FakeRayTrialExecutor(self),
            runner_whitelist_attr={"search_alg", "get_trials", "_set_trial_status"},
            executor_whitelist_attr={"has_resources_for_trial", "pause_trial", "save"},
        )

    def _used_resources_string(self) -> str:
        allocated_resources = self._actor_manager.get_live_actors_resources()

        return self._resource_updater.debug_string(allocated_resources)

    def on_step_begin(self):
        pass

    def on_step_end(self):
        self._cleanup_cached_actors(force_all=False)
        self._cleanup_stopping_actors(force_all=False)

    def _cleanup_cached_actors(self, force_all: bool = False):
        if (
            self._search_alg.is_finished()
            and not self._staged_trials
            and self._actor_cache.total_max_objects == 0
        ):
            # If there are no more trials coming in, no trials are pending execution,
            # and we don't explicitly want to cache objects, we can evict the full
            # cache.
            force_all = True

        for tracked_actor in self._actor_cache.flush_cached_objects(
            force_all=force_all
        ):
            logger.debug(f"Cleaning up cached actor: {tracked_actor}")
            # Unset termination callbacks as no trial is associated
            tracked_actor.set_on_stop(None)
            tracked_actor.set_on_error(None)
            self._remove_actor(tracked_actor=tracked_actor)

    def _cleanup_stopping_actors(self, force_all: bool = False):
        now = time.monotonic()

        if (
            not force_all
            and now - self._earliest_stopping_actor > self._actor_cleanup_timeout
        ):
            # If the earliest actor to timeout has not reached the timeout, return
            return

        # This is a bit costly, so we want to avoid running it too often
        times = deque(
            sorted(
                (timestamp, tracked_actor)
                for tracked_actor, timestamp in self._stopping_actors.items()
            )
        )

        while times and (
            force_all or time.monotonic() - times[0][0] > self._actor_cleanup_timeout
        ):
            if (
                time.monotonic() - times[0][0] < self._actor_force_cleanup_timeout
            ) and self._actor_manager.is_actor_started(tracked_actor=times[0][1]):
                # Even if force_all=True, we give the actors time to clean up
                self._actor_manager.next(timeout=1)
                continue

            _, tracked_actor = times.popleft()

            if tracked_actor not in self._stopping_actors:
                # Actor stopping has been handled by the block above
                continue

            if self._actor_manager.is_actor_started(tracked_actor=tracked_actor):
                logger.debug(f"Forcefully killing actor: {tracked_actor}")
                self._actor_manager.remove_actor(tracked_actor=tracked_actor, kill=True)
            self._stopping_actors.pop(tracked_actor)

        if times:
            self._earliest_stopping_actor = times[0][0]
        else:
            self._earliest_stopping_actor = float("inf")

    def step(self):
        if self.is_finished():
            raise TuneError("Called step when all trials finished?")

        with warn_if_slow("on_step_begin"):
            self.on_step_begin()

        with warn_if_slow("callbacks.on_step_begin"):
            self._callbacks.on_step_begin(
                iteration=self._iteration, trials=self._trials
            )

        # Ask searcher for more trials
        self._maybe_update_trial_queue()

        # Start actors for added trials
        self._maybe_add_actors()

        # Handle one event
        if not self._actor_manager.next(timeout=0.1):
            # If there are no actors running, warn about potentially
            # insufficient resources
            if not self._actor_manager.num_live_actors:
                self._insufficient_resources_manager.on_no_available_trials(
                    self.get_trials()
                )

        # Maybe stop whole experiment
        self._stop_experiment_if_needed()

        # Maybe save experiment state
        try:
            self.checkpoint()
        except Exception as e:
            logger.warning(f"Trial controller checkpointing failed: {str(e)}")
            raise e

        self._iteration += 1

        if self._server:
            with warn_if_slow("server"):
                self._process_stop_requests()

            if self.is_finished():
                self._server.shutdown()

        with warn_if_slow("on_step_end"):
            self.on_step_end()
        with warn_if_slow("callbacks.on_step_end"):
            self._callbacks.on_step_end(iteration=self._iteration, trials=self._trials)

    def _set_trial_status(self, trial: Trial, status: str):
        """Set trial to a specific status.

        This will keep track of trials with specific statuses in sets.

        For PENDING and PAUSED trials we also keep a list of trials to be able
        to retain FIFO ordering. See ``_maybe_add_actors`` for details.

        Lastly we also keep a mapping from resources to pending/paused trials
        to be able to efficiently start trials for cached actors.
        """
        current_status = trial.status

        if current_status == status:
            logger.debug(f"Trial {trial} already has status {status}. Skipping update.")
            return

        status_str_map = {
            Trial.PENDING: self._pending_trials,
            Trial.RUNNING: self._running_trials,
            Trial.PAUSED: self._paused_trials,
            Trial.TERMINATED: self._stopped_trials,
            Trial.ERROR: self._failed_trials,
        }

        logger.debug(
            f"Setting status for trial {trial} from {current_status} to {status}"
        )

        assert trial in status_str_map[current_status], (trial, current_status)
        assert trial not in status_str_map[status], (trial, status)

        status_str_map[current_status].remove(trial)
        status_str_map[status].add(trial)

        # We keep a log for pending trials for FIFO scheduling.
        # We do not need to remove from this list as we will just discard
        # items that are in this list but not in the respective set.
        if status == Trial.PENDING:
            self._pending_trials_list.append(trial)
            self._resources_to_pending_trials[trial.placement_group_factory].add(trial)
        else:
            self._resources_to_pending_trials[trial.placement_group_factory].discard(
                trial
            )

        trial.set_status(status)

    def _get_trial_checkpoints(self) -> Dict[str, str]:
        for trial in self._trials_to_cache:
            self._trial_metadata[trial.trial_id] = trial.get_json_state()
        self._trials_to_cache.clear()
        return self._trial_metadata

    def _mark_trial_to_checkpoint(self, trial: Trial):
        self._trials_to_cache.add(trial)

    ###
    # UPDATE TRIALS
    def add_trial(self, trial: Trial):
        """Add a trial to run.

        Like ``_set_trial_status``, this will also update the respective
        trial state sets and mappings.
        """
        super().add_trial(trial)

        logger.debug(f"Adding trial {trial} with status {trial.status}")

        status_str_map = {
            Trial.PENDING: self._pending_trials,
            Trial.RUNNING: self._running_trials,
            Trial.PAUSED: self._paused_trials,
            Trial.TERMINATED: self._stopped_trials,
            Trial.ERROR: self._failed_trials,
        }

        status_str_map[trial.status].add(trial)

        if trial.status == Trial.PENDING:
            self._pending_trials_list.append(trial)
            self._resources_to_pending_trials[trial.placement_group_factory].add(trial)

    def _maybe_update_trial_queue(self):
        """Ask the searcher for more trials."""
        if self._search_alg.is_finished():
            return

        dont_wait_for_trial = (
            self._pending_trials or self._running_trials or self._paused_trials
        )

        while len(self._pending_trials) < self._max_pending_trials:
            if not self._update_trial_queue(blocking=not dont_wait_for_trial):
                break
            dont_wait_for_trial = True

    def _cleanup_trials(self):
        logger.debug("CLEANING UP all trials")

        for tracked_actor in list(self._actor_to_trial):
            trial = self._actor_to_trial[tracked_actor]
            logger.debug(
                f"Scheduling trial stop at end of experiment (trial {trial}): "
                f"{tracked_actor}"
            )
            self._schedule_trial_stop(trial)

        # Clean up cached actors now
        self._cleanup_cached_actors(force_all=True)

        start = time.monotonic()
        while time.monotonic() - start < 5 and self._actor_manager.num_total_actors:
            logger.debug("Waiting for actor manager to clean up final state")
            self._actor_manager.next(timeout=1)

        logger.debug("Force cleanup of remaining actors")
        self._cleanup_stopping_actors(force_all=True)

        self._actor_manager.cleanup()

    def _remove_actor(self, tracked_actor: TrackedActor):
        stop_future = self._actor_manager.schedule_actor_task(
            tracked_actor, "stop", _return_future=True
        )
        now = time.monotonic()
        self._stopping_actors[tracked_actor] = now
        self._earliest_stopping_actor = min(self._earliest_stopping_actor, now)
        self._actor_manager.remove_actor(
            tracked_actor, kill=False, stop_future=stop_future
        )

    ###
    # ADD ACTORS
    def _maybe_add_actors(self) -> None:
        """Add actors for pending and paused trials.

        For actors that have not been staged, yet, we request an actor.

        For actors that have been staged, already, we try to reuse a cached actor.

        First, we handle the trial that the scheduler chooses to run.

        Then, we handle all trials that are pending.

        Lastly, we see if we have cached actors that we can assign to a pending or
        paused trial. This can be the case when a trial has not been staged, yet,
        for instance because the number of staging trials was too large.
        """

        ###
        # 1: Start trial that the scheduler wants to run
        with warn_if_slow("choose_trial_to_run"):
            trial_to_run = self._scheduler_alg.choose_trial_to_run(self._wrapped())

        if trial_to_run:
            logger.debug(f"Chose trial to run from scheduler: {trial_to_run}")
            if (
                trial_to_run not in self._staged_trials
                and trial_to_run not in self._trial_to_actor
            ):
                logger.debug(f"Staging trial to run: {trial_to_run}")
                self._set_trial_status(trial_to_run, Trial.PENDING)
                self._staged_trials.add(trial_to_run)
                self._actor_cache.increase_max(trial_to_run.placement_group_factory)
                # schedule_trial_actor also potentially uses cached actors
                self._schedule_trial_actor(trial_to_run)
            else:
                # Otherwise, only try to use the cached actor
                logger.debug(f"Trying to re-use actor for trial to run: {trial_to_run}")
                self._maybe_reuse_cached_actor(trial_to_run)

        ###
        # 2: Start trials that are PENDING
        def _maybe_add_actors(candidates: List[Trial]):
            new_candidates = []

            while candidates:
                if len(self._staged_trials) >= self._max_pending_trials:
                    break

                trial = candidates.pop(0)

                # If the trial is part of the list, but not of the set,
                # we just ignore it. Removing it from the list on status
                # change is too expensive.
                if trial not in self._pending_trials:
                    continue

                if trial in self._trial_to_actor:
                    new_candidates.append(trial)
                    continue

                if trial in self._staged_trials:
                    self._maybe_reuse_cached_actor(trial)
                    continue

                logger.debug(f"Scheduling actor for enqueued trial: {trial}")
                self._staged_trials.add(trial)
                self._actor_cache.increase_max(trial.placement_group_factory)
                self._schedule_trial_actor(trial)

            return new_candidates + candidates

        self._pending_trials_list = _maybe_add_actors(self._pending_trials_list)

        ###
        # 3: Start any trial that can be started with a cached actor
        if self._actor_cache.num_cached_objects:
            for resource in self._resources_to_pending_trials:
                if not self._resources_to_pending_trials[resource]:
                    continue

                if not self._actor_cache.has_cached_object(resource):
                    continue

                start_trial = self._resources_to_pending_trials[resource].pop()
                logger.debug(
                    f"Trying to re-use actor for enqueued trial: {start_trial}"
                )
                if not self._maybe_reuse_cached_actor(start_trial):
                    self._resources_to_pending_trials[resource].add(start_trial)

    def _maybe_reuse_cached_actor(self, trial: Trial) -> bool:
        """Maybe reuse a cached actor for a trial.

        If an actor has been scheduled for the trial already,
        this will remove the original actor.
        """
        if trial in self._resetting_trials:
            return True

        resource_request = trial.placement_group_factory

        if not self._actor_cache.has_cached_object(resource_request):
            return False

        cached_actor = self._actor_cache.pop_cached_object(resource_request)
        logger.debug(f"Reusing ACTOR for trial {trial}: {cached_actor}")

        if trial in self._trial_to_actor:
            original_actor = self._trial_to_actor.pop(trial)
            self._actor_to_trial.pop(original_actor)

            logger.debug(f"Removing ORIGINAL ACTOR for trial {trial}: {original_actor}")
            self._remove_actor(tracked_actor=original_actor)

        self._trial_to_actor[trial] = cached_actor
        self._actor_to_trial[cached_actor] = trial

        # Todo: get rid of Trial.runner
        ray_actor = self._actor_manager._live_actors_to_ray_actors_resources[
            cached_actor
        ][0]
        trial.set_runner(ray_actor)

        self._schedule_trial_reset(trial, trial.config, trial.experiment_tag)

        return True

    def _schedule_trial_actor(self, trial: Trial):
        """Schedule an actor for a trial.

        If a cached actor is available, use it. Otherwise, request a
        new actor.
        """
        logger.debug(f"Trying to schedule new ACTOR for trial {trial}")

        assert trial.status == Trial.PENDING

        trial.init_logdir()
        # We checkpoint metadata here to try mitigating logdir duplication
        self._mark_trial_to_checkpoint(trial)

        if self._maybe_reuse_cached_actor(trial):
            return

        # Safeguard
        if trial in self._trial_to_actor:
            raise RuntimeError(
                f"Tried to request a new actor for trial {trial}, but an old "
                f"actor still exists. This can lead to leaked resources. The old "
                f"actor should be removed first. "
                f"This is an internal problem in Ray Tune. If you encounter this "
                f"error, please raise an issue on "
                f"https://github.com/ray-project/ray/issues"
            )

        trainable_cls = trial.get_trainable_cls()
        if not trainable_cls:
            raise _AbortTrialExecution(
                f"Invalid trainable: {trial.trainable_name}. If you passed "
                f"a string, make sure the trainable was registered before."
            )
        _actor_cls = self._class_cache.get(trainable_cls)

        trial.set_location(_Location())
        trainable_kwargs = _get_trainable_kwargs(
            trial=trial, should_chdir=self._chdir_to_trial_dir
        )

        with _change_working_directory(trial):
            tracked_actor = self._actor_manager.add_actor(
                cls=_actor_cls,
                resource_request=trial.placement_group_factory,
                kwargs=trainable_kwargs,
                on_start=self._actor_started,
                on_stop=self._actor_stopped,
                on_error=self._actor_failed,
            )
            self._trial_to_actor[trial] = tracked_actor
            self._actor_to_trial[tracked_actor] = trial

        logger.debug(
            f"Scheduled new ACTOR for trial {trial}: {tracked_actor}. "
            f"Resources: {trial.placement_group_factory}"
        )

    def _unstage_trial_with_resources(self, trial: Trial):
        """Unstage trial, or one with the same resources as ``trial``."""
        # Case 1: The trial we started was staged. Just remove it
        if trial in self._staged_trials:
            self._staged_trials.remove(trial)
            self._actor_cache.decrease_max(trial.placement_group_factory)
            return

        # Case 2: We staged a trial "A" with the same resources, but our trial "B"
        # was selected by the scheduler to run. The resource manager does not care
        # about "trials", it just cares about resources being available. Thus we
        # look for a staged trial with the same resource requirements and remove it

        resource_request = trial.placement_group_factory
        # Remove staged trial with same resource requirements
        candidate_trial = None
        for staged_trial in self._staged_trials:
            staged_resources = staged_trial.placement_group_factory
            if staged_resources == resource_request:
                candidate_trial = staged_trial
                break

        if candidate_trial:
            self._staged_trials.remove(candidate_trial)
            self._actor_cache.decrease_max(candidate_trial.placement_group_factory)
            return

        raise RuntimeError(
            "Started a trial with resources requested by a different trial, but "
            "this trial was lost. This is an error in Ray Tune's execution "
            "logic. Please raise a GitHub issue at "
            "https://github.com/ray-project/ray/issues"
        )

    def _maybe_cache_trial_actor(self, trial: Trial) -> bool:
        """Cache trial actor for reuse, if needed.

        We will only cache as many actors as are needed to fulfill any pending
        resource requests for actors with the same resource requirements.
        E.g. if we have 6 running trials and 4 additional staged actors, we will only
        cache up to 4 of the running trial actors when they finish.

        One exception is the case when we have no cached actors, yet. In that case,
        we will always cache the actor in this method.

        Later, in `_cleanup_cached_actors`, we will check again if we need this cached
        actor. That method will keep the actor if we don't have any staged trials,
        because we don't know at that point if the next trial might require the same
        resources. But because there is no staged trial, it is safe to keep the actor
        around, as it won't occupy resources needed by another trial until it's staged.
        """
        if not self._reuse_actors:
            return False

        if self._search_alg.is_finished() and not self._staged_trials:
            logger.debug(
                f"Not caching actor of trial {trial} as the search is over "
                f"and no more trials are staged."
            )
            return False

        tracked_actor = self._trial_to_actor[trial]

        if (
            not self._actor_manager.is_actor_started(tracked_actor)
            or self._actor_manager.is_actor_failed(tracked_actor)
            or tracked_actor not in self._started_actors
        ):
            logger.debug(
                f"Not caching actor of trial {trial} as it has not been started, yet: "
                f"{tracked_actor}"
            )
            return False

        if not self._actor_cache.cache_object(
            trial.placement_group_factory, tracked_actor
        ):
            logger.debug(
                f"Could not cache actor of trial {trial} for "
                "reuse, as there are no pending trials "
                "requiring its resources."
            )
            return False

        logger.debug(f"Caching actor of trial {trial} for re-use: {tracked_actor}")

        tracked_actor = self._trial_to_actor.pop(trial)
        self._actor_to_trial.pop(tracked_actor)

        trial.set_runner(None)

        return True

    def _actor_started(self, tracked_actor: TrackedActor, log: str = "STARTED"):
        self._started_actors.add(tracked_actor)

        trial = self._actor_to_trial[tracked_actor]

        logger.debug(f"Actor {log} for trial {trial}: {tracked_actor}")

        self._unstage_trial_with_resources(trial)

        ray_actor = self._actor_manager._live_actors_to_ray_actors_resources[
            tracked_actor
        ][0]
        trial.set_runner(ray_actor)

        self._callbacks.on_trial_start(
            iteration=self._iteration, trials=self._trials, trial=trial
        )

        self._set_trial_status(trial, Trial.RUNNING)

        self._mark_trial_to_checkpoint(trial)

        if not self._schedule_trial_restore(trial):
            self._schedule_trial_train(trial)

    def _actor_stopped(self, tracked_actor: TrackedActor):
        if tracked_actor in self._actor_to_trial:
            trial = self._actor_to_trial.pop(tracked_actor)
            logger.debug(f"Actor STOPPED for trial {trial}: {tracked_actor}")
            self._trial_to_actor.pop(trial)
            trial.set_runner(None)

        logger.debug(f"Actor STOPPED: {tracked_actor}")

        self._stopping_actors.pop(tracked_actor, None)
        self._started_actors.discard(tracked_actor)

    def _actor_failed(self, tracked_actor: TrackedActor, exception: Exception):
        trial = self._actor_to_trial[tracked_actor]

        logger.debug(
            f"Actor FAILED for trial {trial}: {tracked_actor}. "
            f"Exception: {exception}"
        )

        if trial in (self._pending_trials | self._paused_trials):
            # First, set to running (needed downstream in _process_trial_failure)
            self._set_trial_status(trial, Trial.RUNNING)

            logger.debug(
                f"Trial {trial} failed in its creation task. Unstaging "
                f"to allow it to be re-scheduled."
            )

            self._unstage_trial_with_resources(trial)
            self._trial_task_failure(trial, exception=exception)

        self._actor_manager.clear_actor_task_futures(tracked_actor)

        # Clean up actor
        tracked_actor.set_on_stop(None)
        tracked_actor.set_on_error(None)
        self._actor_manager.remove_actor(tracked_actor, kill=False)

        # Trigger actor stopped callback
        self._actor_stopped(tracked_actor)

    def _schedule_trial_task(
        self,
        trial: Trial,
        method_name: str,
        args: Optional[Tuple] = None,
        kwargs: Optional[Dict] = None,
        on_result: Optional[Callable[[Trial, Any], None]] = None,
        on_error: Optional[Callable[[Trial, Exception], None]] = None,
        _return_future: bool = False,
    ) -> Optional[ray.ObjectRef]:
        """Schedule an actor task future for a trial.

        This is a wrapper around ``ActorManager.schedule_actor_task``. This method
        retrieves the tracked actor for a trial to kick off the task.

        It also wraps around the callbacks, retrieving the trial object given the
        tracked actor.
        """

        tracked_actor = self._trial_to_actor[trial]

        _on_result = None
        _on_error = None

        args = args or tuple()
        kwargs = kwargs or {}

        if on_result:

            def _on_result(tracked_actor: TrackedActor, *args, **kwargs):
                assert trial == self._actor_to_trial[tracked_actor]
                logger.debug(
                    f"Future {method_name.upper()} RESOLVED for trial {trial}: "
                    f"{args}, {kwargs}"
                )
                try:
                    on_result(trial, *args, **kwargs)
                except Exception as e:
                    logger.debug(
                        f"Error handling {method_name.upper()} result "
                        f"for trial {trial}: {e}"
                    )
                    if e is TuneError or self._fail_fast == self.RAISE:
                        raise e
                    else:
                        raise TuneError(traceback.format_exc())

        if on_error:

            def _on_error(tracked_actor: TrackedActor, exception: Exception):
                # If the actor failed, it has already been cleaned up.
                if tracked_actor not in self._actor_to_trial:
                    assert isinstance(exception, RayActorError), type(exception)
                else:
                    assert trial == self._actor_to_trial[tracked_actor]

                logger.debug(
                    f"Future {method_name.upper()} FAILED for trial {trial}: "
                    f"{exception}"
                )
                try:
                    on_error(trial, exception)
                except Exception as e:
                    logger.debug(
                        f"Error handling {method_name.upper()} failure "
                        f"for trial {trial}: {e}"
                    )
                    if e is TuneError or self._fail_fast == self.RAISE:
                        raise e
                    else:
                        raise TuneError(traceback.format_exc())

        logger.debug(f"Future {method_name.upper()} SCHEDULED for trial {trial}")

        with _change_working_directory(trial):
            future = self._actor_manager.schedule_actor_task(
                tracked_actor=tracked_actor,
                method_name=method_name,
                args=args,
                kwargs=kwargs,
                on_result=_on_result,
                on_error=_on_error,
                _return_future=_return_future,
            )
            if _return_future:
                return future

    ###
    # Failure
    def _trial_task_failure(self, trial: Trial, exception: Exception):
        if self._fail_fast == self.RAISE:
            raise exception
        else:
            if self._print_trial_errors:
                logger.error("Trial task failed", exc_info=exception)
            self._process_trial_failure(trial, exception=exception)

    def _schedule_trial_stop(self, trial: Trial, exception: Optional[Exception] = None):
        if trial.status == Trial.ERROR:
            logger.debug(f"Not requesting trial STOP as it is ERROR already: {trial}")
            return

        logger.debug(f"Requesting to STOP actor for trial {trial}")

        trial.saving_to = None
        trial.restoring_from = None

        self._set_trial_status(trial, Trial.ERROR if exception else Trial.TERMINATED)
        trial.set_location(_Location())

        if exception:
            trial.handle_error(exc=exception)

        if trial not in self._trial_to_actor:
            logger.debug(f"Will not STOP trial actor as it is not live: {trial}")
            return

        tracked_actor = self._trial_to_actor[trial]

        self._actor_manager.clear_actor_task_futures(tracked_actor=tracked_actor)

        self._mark_trial_to_checkpoint(trial)

        if not exception and self._maybe_cache_trial_actor(trial):
            # Trial runner has been cached
            return

        logger.debug(f"Terminating actor for trial {trial}: {tracked_actor}")

        tracked_actor = self._trial_to_actor.pop(trial)
        self._actor_to_trial.pop(tracked_actor)

        trial.set_runner(None)

        self._remove_actor(tracked_actor=tracked_actor)

    def _schedule_graceful_trial_stop(self, trial: Trial):
        self._schedule_trial_export(trial)
        if trial.status != "ERROR":
            self._schedule_trial_stop(trial)

    def _schedule_trial_pause(self, trial: Trial, should_checkpoint: bool = True):
        if should_checkpoint:
            self._schedule_trial_save(trial, storage=CheckpointStorage.MEMORY)
        self._schedule_trial_stop(trial)
        self._set_trial_status(trial, Trial.PAUSED)

    ###
    # TRAIN

    def _schedule_trial_train(self, trial: Trial):
        args = ()
        method_name = "train"

        buffer_length, buffer_time_s = self._maybe_buffer_training(trial)

        if buffer_length > 1:
            method_name = "train_buffered"
            args = (buffer_length, buffer_time_s)

        logger.debug(f"Scheduling future {method_name.upper()} for trial {trial}")

        self._schedule_trial_task(
            trial=trial,
            method_name=method_name,
            args=args,
            on_result=self._on_training_result,
            on_error=self._trial_task_failure,
        )

    def _maybe_buffer_training(self, trial: Trial) -> Tuple[int, float]:
        buffer_time_s = max(
            self._buffer_min_time_s,
            min(self._buffer_max_time_s, self._actor_manager.num_actor_tasks // 10),
        )
        buffer_length = self._buffer_length

        if buffer_length > 1 and trial.checkpoint_at_end:
            # If a trial checkpoint can be triggered externally,
            # it is not safe to buffer results.
            if log_once("trial_executor_buffer_checkpoint"):
                logger.warning(
                    "Disabling buffered training as you passed "
                    "`checkpoint_at_end` to `air.CheckpointConfig()`."
                )
            return 1, buffer_time_s

        if buffer_length > 1 and trial.checkpoint_freq > 0:
            return min(buffer_length, trial.checkpoint_freq), buffer_time_s

        return buffer_length, buffer_time_s

    ###
    # SAVE
    def _schedule_trial_save(
        self,
        trial: Trial,
        storage: CheckpointStorage = CheckpointStorage.PERSISTENT,
        result: Optional[Dict] = None,
    ) -> Optional[_TrackedCheckpoint]:
        if trial not in self._trial_to_actor:
            logger.debug(
                f"Trial SAVE requested for trial {trial} but trial is already "
                f"stopping. Ignoring."
            )
            return None

        result = result or trial.last_result

        if storage == CheckpointStorage.MEMORY:
            future = self._schedule_trial_task(
                trial=trial,
                method_name="save_to_object",
                on_result=None,
                on_error=self._trial_task_failure,
                _return_future=True,
            )
            checkpoint = _TrackedCheckpoint(
                dir_or_data=future, storage_mode=storage, metrics=result
            )
            trial.on_checkpoint(checkpoint)
        else:
            future = self._schedule_trial_task(
                trial=trial,
                method_name="save",
                on_result=self._on_saving_result,
                on_error=self._trial_task_failure,
                _return_future=True,
            )
            checkpoint = _TrackedCheckpoint(
                dir_or_data=future,
                storage_mode=storage,
                metrics=result,
            )
            trial.saving_to = checkpoint

        return checkpoint

    ###
    # RESTORE
    def _schedule_trial_restore(self, trial: Trial) -> bool:
        checkpoint = trial.checkpoint

        if checkpoint.dir_or_data is None:
            logger.debug(f"Not restoring trial {trial}: No checkpoint found.")
            return False

        kwargs = {}

        if checkpoint.storage_mode == CheckpointStorage.MEMORY:
            method_name = "restore_from_object"
            args = (checkpoint.dir_or_data,)
        elif (
            trial.uses_cloud_checkpointing
            or not trial.sync_on_checkpoint
            or not os.path.exists(checkpoint.dir_or_data)
        ):
            fallback_to_latest = bool(
                int(os.environ.get("TUNE_FALLBACK_TO_LATEST_CHECKPOINT", "1"))
            )

            method_name = "restore"
            args = (checkpoint.dir_or_data,)
            kwargs = {
                "checkpoint_node_ip": checkpoint.node_ip,
                "fallback_to_latest": fallback_to_latest,
            }
        elif trial.sync_on_checkpoint:
            checkpoint_path = TrainableUtil.find_checkpoint_dir(checkpoint.dir_or_data)
            obj = Checkpoint.from_directory(checkpoint_path).to_bytes()

            method_name = "restore_from_object"
            args = (obj,)
        else:
            raise _AbortTrialExecution(
                "Pass in `sync_on_checkpoint=True` for driver-based trial"
                "restoration. Pass in an `upload_dir` for remote "
                "storage-based restoration"
            )

        trial.restoring_from = checkpoint
        self._schedule_trial_task(
            trial=trial,
            method_name=method_name,
            args=args,
            kwargs=kwargs,
            on_result=self._on_restore_result,
            on_error=self._trial_task_failure,
        )
        return True

    def _on_restore_result(self, trial: Trial, result: Any):
        self._process_trial_restore(trial)

    ###
    # EXPORT
    def _schedule_trial_export(self, trial: Trial):
        if not trial.export_formats or len(trial.export_formats) <= 0:
            return

        # Todo: We are waiting here synchronously until the task resolved.
        # Instead, we should schedule the trial stop after the export resolved.
        # This requires changes in TrialRunner, which we can remove once the
        # legacy execution path has been removed.
        future = self._schedule_trial_task(
            trial=trial,
            method_name="export_model",
            args=(trial.export_formats,),
            on_result=None,
            on_error=self._trial_task_failure,
            _return_future=True,
        )
        self._actor_manager._actor_task_events.resolve_future(future)

    ###
    # RESET
    def _schedule_trial_reset(
        self,
        trial: Trial,
        new_config: Dict,
        new_experiment_tag: str,
    ):
        trial.set_experiment_tag(new_experiment_tag)
        trial.set_config(new_config)

        # Pass magic variables
        extra_config = copy.deepcopy(new_config)
        extra_config[TRIAL_INFO] = _TrialInfo(trial)

        stdout_file, stderr_file = trial.log_to_file
        extra_config[STDOUT_FILE] = stdout_file
        extra_config[STDERR_FILE] = stderr_file

        logger_creator = partial(
            _noop_logger_creator,
            logdir=trial.logdir,
            should_chdir=self._chdir_to_trial_dir,
        )

        self._resetting_trials.add(trial)
        self._schedule_trial_task(
            trial=trial,
            method_name="reset",
            args=(extra_config,),
            kwargs={
                "logger_creator": logger_creator,
                "remote_checkpoint_dir": trial.remote_checkpoint_dir,
            },
            on_result=self._on_trial_reset,
            on_error=self._trial_task_failure,
        )

    def _on_trial_reset(self, trial: Trial, success: bool):
        self._resetting_trials.remove(trial)

        if not success:
            info = (
                "Trainable runner reuse requires reset_config() to be "
                "implemented and return True."
            )

            logger.error(f"Could not re-use actor for trial {trial}: {info}")

            exception = _AbortTrialExecution(info)

            self._schedule_trial_stop(trial, exception=exception)
            return

        tracked_actor = self._trial_to_actor[trial]

        self._actor_started(tracked_actor, log="REUSED")

    def __getstate__(self):
        state = super().__getstate__()
        for exclude in [
            "_resource_manager",
            "_actor_manager",
            "_class_cache",
            "_resource_updater",
            "_trials_to_cache",
            "_trial_metadata",
            "_actor_to_trial",
            "_trial_to_actor",
            "_resources_to_pending_trials",
            "_pending_trials",
            "_pending_trials_list",
            "_running_trials",
            "_paused_trials",
            "_stopped_trials",
            "_failed_trials",
            "_resetting_trials",
            "_started_actors",
            "_stopping_actors",
            "_staged_trials",
            "_actor_cache",
        ]:
            del state[exclude]

        return state


class _FakeRayTrialExecutor:
    """The TuneController does not use a RayTrialExecutor anymore.

    Instead, we pass this fake executor for searchers/schedulers to use
    as an interface.

    In the future, we should have the searchers/schedulers either interact with
    the tune controller, or define a different API for more fine-grained scheduler
    control.
    """

    def __init__(self, tune_controller: TuneController):
        self._tune_controller = tune_controller

    def pause_trial(self, trial: Trial, should_checkpoint: bool = True):
        return self._tune_controller._schedule_trial_pause(
            trial, should_checkpoint=should_checkpoint
        )

    def save(
        self,
        trial: Trial,
        storage: CheckpointStorage = CheckpointStorage.PERSISTENT,
        result: Optional[Dict] = None,
    ) -> _TrackedCheckpoint:
        return self._tune_controller._schedule_trial_save(
            trial=trial, storage=storage, result=result
        )

    def has_resources_for_trial(self, trial: Trial):
        return True

    @property
    def _resource_updater(self):
        return self._tune_controller._resource_updater

    def force_reconcilation_on_next_step_end(self):
        pass
