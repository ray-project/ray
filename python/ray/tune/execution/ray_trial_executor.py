# coding: utf-8
import copy
import logging
import os
import random
import time
import traceback
from collections import deque
from enum import Enum
from typing import Callable, Dict, Iterable, Optional, Set, Union

import ray
from ray.actor import ActorHandle
from ray.air import Checkpoint, AcquiredResources
from ray.air._internal.checkpoint_manager import CheckpointStorage, _TrackedCheckpoint
from ray.air.constants import (
    COPY_DIRECTORY_CHECKPOINTS_INSTEAD_OF_MOVING_ENV,
    DISABLE_LAZY_CHECKPOINTING_ENV,
)
from ray.air.execution import ResourceManager
from ray.air.execution.resources.placement_group import (
    PlacementGroupResourceManager,
)
from ray.exceptions import GetTimeoutError, RayTaskError
from ray.tune.error import (
    TuneError,
    _AbortTrialExecution,
    _TuneNoNextExecutorEventError,
    _TuneStartTrialError,
)
from ray.tune.result import STDERR_FILE, STDOUT_FILE, TRIAL_INFO
from ray.tune.experiment.trial import (
    Trial,
    _Location,
    _TrialInfo,
    _change_working_directory,
    _get_trainable_kwargs,
)
from ray.tune.utils import warn_if_slow
from ray.tune.utils.object_cache import _ObjectCache
from ray.tune.utils.resource_updater import _ResourceUpdater
from ray.tune.trainable.util import TrainableUtil
from ray.util import log_once
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(__name__)

DEFAULT_GET_TIMEOUT = 60.0  # seconds

DEFAULT_ENV_VARS = {
    # https://github.com/ray-project/ray/issues/28197
    "PL_DISABLE_FORK": "1"
}
ENV_VARS_TO_PROPAGATE = {
    DISABLE_LAZY_CHECKPOINTING_ENV,
    COPY_DIRECTORY_CHECKPOINTS_INSTEAD_OF_MOVING_ENV,
    "TUNE_CHECKPOINT_CLOUD_RETRY_NUM",
    "TUNE_CHECKPOINT_CLOUD_RETRY_WAIT_TIME_S",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SECURITY_TOKEN",
    "AWS_SESSION_TOKEN",
}


class _ActorClassCache:
    """Caches actor classes.

    ray.remote is a registration call. It sends the serialized object to the
    key value store (redis), and will be fetched at an arbitrary worker
    later. Registration does not use any Ray scheduling resources.

    Later, class.remote() actually creates the remote actor. The
    actor will be instantiated on some arbitrary machine,
    according to the underlying Ray scheduler.

    Without this cache, you would register the same serialized object
    over and over again. Naturally, since redis doesn’t spill to disk,
    this can easily nuke the redis instance (and basically blow up Ray).
    This cache instead allows us to register once and only once.

    Note that we assume there can be multiple trainables in the
    system at once.
    """

    def __init__(self):
        self._cache = {}

    def get(self, trainable_cls):
        """Gets the wrapped trainable_cls, otherwise calls ray.remote."""
        env_vars = DEFAULT_ENV_VARS.copy()

        for env_var_to_propagate in ENV_VARS_TO_PROPAGATE:
            if env_var_to_propagate in os.environ:
                env_vars[env_var_to_propagate] = os.environ[env_var_to_propagate]

        runtime_env = {"env_vars": env_vars}
        if trainable_cls not in self._cache:
            remote_cls = ray.remote(runtime_env=runtime_env)(trainable_cls)
            self._cache[trainable_cls] = remote_cls
        return self._cache[trainable_cls]


_class_cache = _ActorClassCache()


class _LocalWrapper:
    def __init__(self, result):
        self._result = result

    def unwrap(self):
        """Returns the wrapped result."""
        return self._result


class _TrialCleanup:
    """Responsible for triggering force cleanup of remote actors,
    without waiting for `Trainable.stop()` to finish.

    Only instantiated when `TUNE_FORCE_TRIAL_CLEANUP_S` is set up.
    """

    def __init__(self, force_cleanup):
        assert force_cleanup
        self._force_cleanup = force_cleanup
        self._future_to_insert_time = deque()

    def add(self, future):
        self._future_to_insert_time.append((future, time.time()))

    def get_next(self):
        """Get the next future that is eligible to be cleaned up forcibly."""
        if len(self._future_to_insert_time) > 0 and (
            self._future_to_insert_time[0][1] + self._force_cleanup < time.time()
        ):
            future, _time = self._future_to_insert_time.popleft()
            return future
        else:
            return None

    def is_empty(self):
        return len(self._future_to_insert_time) == 0


class _ExecutorEventType(Enum):
    """The executor event type.

    Some of the events are internal events to executor while others
    are handled by runner."""

    NO_RUNNING_TRIAL_TIMEOUT = 1
    PG_READY = 2
    TRAINING_RESULT = 3
    SAVING_RESULT = 4
    RESTORING_RESULT = 5
    STOP_RESULT = 6  # Internally to executor only.
    YIELD = 7  # Yielding back to TrialRunner's main event loop.


class _ExecutorEvent:
    """A struct that describes the event to be processed by TrialRunner.

    Attributes:
        result: A dict with keys of "future_result" and "exception".
            "future_result" is the corresponding result when future returns
            successfully.
            "exception" is the exception as caught during ``ray.get(future)``.
    """

    KEY_FUTURE_RESULT = "future_result"
    KEY_EXCEPTION = "exception"

    def __init__(
        self,
        event_type: _ExecutorEventType,
        trial: Optional[Trial] = None,
        result: Optional[Dict] = None,
    ):
        self.type = event_type
        self.trial = trial
        self.result = result

    def __repr__(self):
        return f"[{self.type}] for {self.trial}"


@DeveloperAPI
class RayTrialExecutor:
    """An implementation of TrialExecutor based on Ray."""

    def __init__(
        self,
        resource_manager: Optional[ResourceManager] = None,
        reuse_actors: bool = False,
        result_buffer_length: Optional[int] = None,
        refresh_period: Optional[float] = None,
        chdir_to_trial_dir: bool = False,
    ):
        # Trial metadata
        self._cached_trial_state = {}
        self._trials_to_cache = set()

        # future --> (type, trial/pg)
        self._futures = {}
        # Cache futures that are ready to reduce the number times we iterate through
        # all futures (and e.g. shuffle them)
        self._cached_ready_futures = []

        # Cleanup
        force_trial_cleanup = int(os.environ.get("TUNE_FORCE_TRIAL_CLEANUP_S", "600"))
        self._get_next_event_wait = int(
            os.environ.get("TUNE_GET_EXECUTOR_EVENT_WAIT_S", "5")
        )
        if force_trial_cleanup:
            self._trial_cleanup = _TrialCleanup(force_trial_cleanup)
        else:
            self._trial_cleanup = None

        # For printing used resources
        self._resource_updater = _ResourceUpdater(refresh_period)

        # Resource management.
        # For details, see docstring of `_stage_and_update_status()`
        self._max_staged_actors = 1
        self._resource_manager = resource_manager or PlacementGroupResourceManager()

        # Actor re-use.
        # For details, see docstring of `_maybe_cache_trial_actor()`
        self._reuse_actors = reuse_actors
        self._actor_cache = _ObjectCache(may_keep_one=True)

        # Trials for which we requested resources
        self._staged_trials = set()  # Staged trials
        self._trial_to_acquired_resources: Dict[Trial, AcquiredResources] = {}

        # Result buffer
        self._buffer_length = result_buffer_length or int(
            os.getenv("TUNE_RESULT_BUFFER_LENGTH", 1)
        )
        self._buffer_min_time_s = float(os.getenv("TUNE_RESULT_BUFFER_MIN_TIME_S", 0.0))
        self._buffer_max_time_s = float(
            os.getenv("TUNE_RESULT_BUFFER_MAX_TIME_S", 100.0)
        )

        # Default kwargs to pass to trainable
        self._trainable_kwargs = {}

        # Trial dir behavior
        self._chdir_to_trial_dir = chdir_to_trial_dir

    def setup(
        self, max_pending_trials: int, trainable_kwargs: Optional[Dict] = None
    ) -> None:
        if self._actor_cache.num_cached_objects:
            logger.warning(
                "Cannot update maximum number of queued actors for reuse "
                "during a run."
            )
        else:
            self._max_staged_actors = max_pending_trials

        self._trainable_kwargs = trainable_kwargs or {}

    def set_status(self, trial: Trial, status: str) -> None:
        """Sets status and checkpoints metadata if needed.

        Only checkpoints metadata if trial status is a terminal condition.
        PENDING, PAUSED, and RUNNING switches have checkpoints taken care of
        in the TrialRunner.

        Args:
            trial: Trial to checkpoint.
            status: Status to set trial to.
        """
        if trial.status == status:
            logger.debug("Trial %s: Status %s unchanged.", trial, trial.status)
        else:
            logger.debug(
                "Trial %s: Changing status from %s to %s.", trial, trial.status, status
            )
        trial.set_status(status)
        if status in [Trial.TERMINATED, Trial.ERROR]:
            self._trials_to_cache.add(trial)

    def mark_trial_to_checkpoint(self, trial: Trial) -> None:
        self._trials_to_cache.add(trial)

    def get_checkpoints(self) -> Dict[str, str]:
        """Returns a copy of mapping of the trial ID to pickled metadata."""
        for trial in self._trials_to_cache:
            self._cached_trial_state[trial.trial_id] = trial.get_json_state()
        self._trials_to_cache.clear()
        return self._cached_trial_state

    def _stage_and_update_status(self, trials: Iterable[Trial]):
        """Check and update statuses of scheduled placement groups.

        Stages placement groups of all trials.

        We will never request resources for more than `_max_staged_actors` at the same
        time. This does not include running actors.

        Thus, if max_staged_actors=4 and e.g. 8 trials can run at the same time,
        we will occupy resources for up to 8 actors and have requests pending
        for 4 more.
        """
        for trial in trials:
            if len(self._staged_trials) >= self._max_staged_actors:
                break

            if trial.status not in (Trial.PENDING, Trial.PAUSED):
                continue

            if trial in self._staged_trials:
                continue

            resource_request = trial.placement_group_factory

            self._staged_trials.add(trial)
            self._actor_cache.increase_max(resource_request)
            self._resource_manager.request_resources(resource_request=resource_request)

        self._resource_manager.update_state()

    def get_ready_trial(self) -> Optional[Trial]:
        """Get a trial whose resources are ready and that thus can be started.

        Can also return None if no trial is available.

        Returns:
            Trial object or None.

        """
        for trial in self._staged_trials:
            resource_request = trial.placement_group_factory
            # If we have a cached actor for these resources, return
            if self._actor_cache.has_cached_object(resource_request):
                return trial

            # If the resources are available from the resource manager, return
            if self._resource_manager.has_resources_ready(
                resource_request=resource_request
            ):
                return trial

        return None

    def _maybe_use_cached_actor(self, trial, logger_creator) -> Optional[ActorHandle]:
        if not self._reuse_actors:
            return None

        resource_request = trial.placement_group_factory

        if not self._actor_cache.has_cached_object(resource_request):
            return None

        actor, acquired_resources = self._actor_cache.pop_cached_object(
            resource_request
        )

        logger.debug(f"Trial {trial}: Reusing cached actor {actor}")

        trial.set_runner(actor)

        if not self.reset_trial(
            trial, trial.config, trial.experiment_tag, logger_creator
        ):
            raise _AbortTrialExecution(
                "Trainable runner reuse requires reset_config() to be "
                "implemented and return True."
            )

        self._trial_to_acquired_resources[trial] = acquired_resources

        # We are reusing an existing actor (and its resources),
        # so we need to cancel the resource request that we originally scheduled
        # for this trial.
        self._resource_manager.cancel_resource_request(resource_request)

        return actor

    def _setup_remote_runner(self, trial):
        # We checkpoint metadata here to try mitigating logdir duplication
        self._trials_to_cache.add(trial)

        trainable_kwargs = _get_trainable_kwargs(
            trial,
            additional_kwargs=self._trainable_kwargs,
            should_chdir=self._chdir_to_trial_dir,
        )
        logger_creator = trainable_kwargs["logger_creator"]

        existing_runner = self._maybe_use_cached_actor(trial, logger_creator)
        if existing_runner:
            return existing_runner

        trainable_cls = trial.get_trainable_cls()
        if not trainable_cls:
            raise _AbortTrialExecution(
                f"Invalid trainable: {trial.trainable_name}. If you passed "
                f"a string, make sure the trainable was registered before."
            )
        _actor_cls = _class_cache.get(trainable_cls)

        resource_request = trial.placement_group_factory
        acquired_resources = self._resource_manager.acquire_resources(
            resource_request=resource_request
        )

        if not acquired_resources:
            return None

        self._trial_to_acquired_resources[trial] = acquired_resources

        [full_actor_class] = acquired_resources.annotate_remote_entities([_actor_cls])

        # Clear the Trial's location (to be updated later on result)
        # since we don't know where the remote runner is placed.
        trial.set_location(_Location())
        logger.debug("Trial %s: Setting up new remote runner.", trial)

        with _change_working_directory(trial):
            return full_actor_class.remote(**trainable_kwargs)

    def _train(self, trial):
        """Start one iteration of training and save remote id."""

        if self._find_future(trial):
            logging.debug(
                "Trial {} already has a queued future. Skipping this "
                "`train` call. This may occur if a trial has "
                "been unpaused within a scheduler callback.".format(str(trial))
            )
            return

        assert trial.status == Trial.RUNNING, trial.status
        buffer_time_s = max(
            self._buffer_min_time_s,
            min(self._buffer_max_time_s, len(self._futures) // 10),
        )
        with _change_working_directory(trial):
            buffer_length = self._buffer_length
            if buffer_length > 1 and trial.checkpoint_at_end:
                # If a trial checkpoint can be triggered externally,
                # it is not safe to buffer results.
                if log_once("trial_executor_buffer_checkpoint"):
                    logger.warning(
                        "Disabling buffered training as you passed "
                        "`checkpoint_at_end` to `air.CheckpointConfig()`."
                    )
                buffer_length = 1

            if buffer_length > 1:
                if trial.checkpoint_freq > 0:
                    buffer_length = min(buffer_length, trial.checkpoint_freq)
                remote = trial.runner.train_buffered.remote(
                    buffer_time_s, buffer_length
                )
            else:
                remote = trial.runner.train.remote()

        # Local Mode
        if isinstance(remote, dict):
            remote = _LocalWrapper(remote)

        self._futures[remote] = (_ExecutorEventType.TRAINING_RESULT, trial)
        trial_item = self._find_future(trial)
        assert len(trial_item) < 2, trial_item

    def _start_trial(self, trial: Trial) -> bool:
        """Starts trial and restores last result if trial was paused.

        Args:
            trial: The trial to start.

        Returns:
            True if trial was started successfully, False otherwise.

        See `RayTrialExecutor.restore` for possible errors raised.
        """
        self.set_status(trial, Trial.PENDING)
        runner = self._setup_remote_runner(trial)
        if not runner:
            return False
        trial.set_runner(runner)
        self.restore(trial)
        self.set_status(trial, Trial.RUNNING)

        self._unstage_trial_with_resources(trial)

        if not trial.is_restoring:
            self._train(trial)
        return True

    def _unstage_trial_with_resources(self, trial: Trial):
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

        acquired_resources = self._trial_to_acquired_resources[trial]
        cached_resource_request = acquired_resources.resource_request

        if not self._actor_cache.cache_object(
            cached_resource_request, (trial.runner, acquired_resources)
        ):
            logger.debug(
                f"Could not cache actor of trial {trial} for "
                "reuse, as there are no pending trials "
                "requiring its resources."
            )
            return False

        logger.debug(f"Caching actor of trial {trial} for re-use")

        self._trial_to_acquired_resources.pop(trial)

        trial.set_runner(None)

        return True

    def _stop_trial(
        self,
        trial: Trial,
        error: bool = False,
        exc: Optional[Union[TuneError, RayTaskError]] = None,
    ):
        """Stops this trial.

        Stops this trial, releasing all allocating resources. If stopping the
        trial fails, the run will be marked as terminated in error, but no
        exception will be thrown.

        Args:
            error: Whether to mark this trial as terminated in error.
            exc: Optional exception.
        """
        self.set_status(trial, Trial.ERROR if error or exc else Trial.TERMINATED)
        trial.set_location(_Location())

        if not hasattr(trial, "runner") or not trial.runner:
            return

        if exc:
            trial.handle_error(exc=exc)

        if not error and self._maybe_cache_trial_actor(trial):
            # Trial runner has been cached
            return

        try:
            logger.debug("Trial %s: Destroying actor.", trial)

            with _change_working_directory(trial):
                future = trial.runner.stop.remote()

            acquired_resources = self._trial_to_acquired_resources.pop(trial)
            self._futures[future] = (
                _ExecutorEventType.STOP_RESULT,
                acquired_resources,
            )
            if self._trial_cleanup:  # force trial cleanup within a deadline
                self._trial_cleanup.add(future)
        except Exception:
            logger.exception("Trial %s: Error stopping runner.", trial)
            self.set_status(trial, Trial.ERROR)
        finally:
            trial.set_runner(None)

    def start_trial(self, trial: Trial) -> bool:
        """Starts the trial.

        Will not return resources if trial repeatedly fails on start.

        Args:
            trial: Trial to be started.

        Returns:
            True if the remote runner has been started. False if trial was
                not started (e.g. because of lacking resources/pending PG).
        """
        try:
            return self._start_trial(trial)
        except _AbortTrialExecution as e:
            logger.exception("Trial %s: Error starting runner, aborting!", trial)
            time.sleep(2)
            self._stop_trial(trial, exc=e)
            return False
        except Exception as e:
            logger.exception("Trial %s: Unexpected error starting runner.", trial)
            time.sleep(2)
            if isinstance(e, TuneError):
                self._stop_trial(trial, exc=e)
            else:
                self._stop_trial(
                    trial, exc=_TuneStartTrialError(traceback.format_exc())
                )
            # Note that we don't return the resources, since they may
            # have been lost. TODO(ujvl): is this the right thing to do?
            return False

    def _find_future(self, trial):
        out = [rid for rid, t in self._futures.items() if t[1] is trial]
        assert (
            len(out) <= 1
        ), "Expecting one future for any given trial at any given time."
        return out

    def stop_trial(
        self,
        trial: Trial,
        error: bool = False,
        exc: Optional[Union[TuneError, RayTaskError]] = None,
    ) -> None:
        """Stops the trial, releasing held resources and removing futures related to
        this trial from the execution queue.

        Args:
            trial: Trial to stop.
            error: Whether to mark this trial as terminated in error. The trial status
                will be set to either `Trial.ERROR` or `Trial.TERMINATED` based on this.
                Defaults to False.
            exc: Optional exception to log (as a reason for stopping). Defaults to None.
        """
        prior_status = trial.status
        if prior_status == Trial.RUNNING:
            logger.debug("Trial %s: Returning resources.", trial)
            out = self._find_future(trial)
            for result_id in out:
                self._futures.pop(result_id)
        trial.saving_to = None
        trial.restoring_from = None
        self._stop_trial(
            trial,
            error=error or exc,
            exc=exc,
        )

    def continue_training(self, trial: Trial) -> None:
        """Continues the training of this trial."""
        self._train(trial)

    def pause_trial(self, trial: Trial, should_checkpoint: bool = True) -> None:
        """Pauses the trial, releasing resources (specifically GPUs)

        We do this by:
        1. Checkpoint the trial (if `should_checkpoint`) in memory to allow us to resume
        from this state in the future. We may not always  want to checkpoint, if we
        know that the checkpoint will not be used.
        2. Stop the trial and release resources, see `RayTrialExecutor.stop_trial` above
        3. Set the trial status to `Trial.PAUSED`, which is similar to
        `Trial.TERMINATED`, except we have the intention of resuming the trial.

        Args:
            trial: Trial to pause.
            should_checkpoint: Whether to save an in-memory checkpoint before stopping.
        """
        assert trial.status == Trial.RUNNING, trial.status
        try:
            if should_checkpoint:
                self.save(trial, CheckpointStorage.MEMORY)
            self.stop_trial(trial)
            self.set_status(trial, Trial.PAUSED)
        except Exception:
            logger.exception("Error pausing runner.")
            self.set_status(trial, Trial.ERROR)

    def reset_trial(
        self,
        trial: Trial,
        new_config: Dict,
        new_experiment_tag: str,
        logger_creator: Optional[Callable[[Dict], "ray.tune.Logger"]] = None,
    ) -> bool:
        """Tries to invoke `Trainable.reset()` to reset trial.

        Args:
            trial: Trial to be reset.
            new_config: New configuration for Trial trainable.
            new_experiment_tag: New experiment name for trial.
            logger_creator: Function that instantiates a logger on the
                actor process.

        Returns:
            True if `reset_config` is successful else False.
        """
        trial.set_experiment_tag(new_experiment_tag)
        trial.set_config(new_config)
        trainable = trial.runner

        # Pass magic variables
        extra_config = copy.deepcopy(new_config)
        extra_config[TRIAL_INFO] = _TrialInfo(trial)

        stdout_file, stderr_file = trial.log_to_file
        extra_config[STDOUT_FILE] = stdout_file
        extra_config[STDERR_FILE] = stderr_file

        with _change_working_directory(trial):
            with warn_if_slow("reset"):
                try:
                    reset_val = ray.get(
                        trainable.reset.remote(
                            extra_config,
                            logger_creator=logger_creator,
                            remote_checkpoint_dir=trial.remote_path,
                        ),
                        timeout=DEFAULT_GET_TIMEOUT,
                    )
                except GetTimeoutError:
                    logger.exception("Trial %s: reset timed out.", trial)
                    return False
        return reset_val

    def has_resources_for_trial(self, trial: Trial) -> bool:
        """Returns whether there are resources available for this trial.

        This will return True as long as we didn't reach the maximum number
        of pending trials. It will also return True if the trial placement
        group is already staged.

        Args:
            trial: Trial object which should be scheduled.

        Returns:
            boolean

        """
        resource_request = trial.placement_group_factory

        return (
            trial in self._staged_trials
            or self._actor_cache.has_cached_object(resource_request)
            or len(self._staged_trials) < self._max_staged_actors
            or self._resource_manager.has_resources_ready(resource_request)
        )

    def _allocated_resources(self) -> dict:
        total_resources = {"CPU": 0, "GPU": 0}
        for allocated_resource in self._trial_to_acquired_resources.values():
            resource_request = allocated_resource.resource_request
            for bundle_resources in resource_request.bundles:
                for key, val in bundle_resources.items():
                    total_resources[key] = total_resources.get(key, 0) + val
        return total_resources

    def debug_string(self) -> str:
        """Returns a human readable message for printing to the console."""
        allocated_resources = self._allocated_resources()

        return self._resource_updater.debug_string(allocated_resources)

    def on_step_begin(self) -> None:
        """Before step() is called, update the available resources."""
        self._resource_updater.update_avail_resources()

    def on_step_end(self, search_ended: bool = False) -> None:
        self._cleanup_cached_actors(search_ended=search_ended)
        self._do_force_trial_cleanup()

    def _cleanup_cached_actors(
        self, search_ended: bool = False, force_all: bool = False
    ):
        """Clean up unneeded cached actors.

        Ray Tune caches actors for re-use to avoid initialization overhead. This is
        useful in two situations: a) to avoid scheduling overhead of actors when
        trials run for a short time (e.g. < 1 second), and b) to avoid setup overhead
        when trials initialize a heavy environment/dependencies (e.g. rllib).

        Actors are cached when a trial is stopped. However, at that point in time
        we don't always know if we will need the actor later or not. E.g. if all
        subsequent trials have different resource requirements, we wouldn't need to
        cache the actor.

        Tune will generate a new trial in the next iteration of step(). Only once
        this new trial is generated can we know if we will actually use the cached
        actor soon. This is especially the case when we only run one trial at the time
        and don't allow more pending trials: At the point of caching there would be
        _no_ pending trial, so we would never cache the actor.

        To circumvent this problem, we always cache an actor once the trial is
        gracefully stopped (and when `reuse_actors=True`). We only remove this
        cached actor once we have at least one new staged trial, so that we know
        if we need to keep the actor or not. So if we create a new trial in the next
        iteration, we can either reuse the cached actor (if resources match) or
        remove it (if resources don't match and the cached actor is thus not required).

        This method fetches the required resources for all pending trials and the
        resources for all cached actors. If we cached more actors than we need, we
        terminate the excess actors and free the resources.
        """
        if not self._staged_trials and not force_all and not search_ended:
            # If we don't have any staged trials, keep cached actors,
            # unless cleanup is forced or no new trials are going to be generated
            # (if the search ended).
            return

        if (
            search_ended
            and not self._staged_trials
            and self._actor_cache.total_max_objects == 0
        ):
            # If there are no more trials coming in, no trials are pending execution,
            # and we don't explicitly want to cache objects, we can evict the full
            # cache.
            force_all = True

        for actor, acquired_resources in self._actor_cache.flush_cached_objects(
            force_all=force_all
        ):
            future = actor.stop.remote()
            self._futures[future] = (
                _ExecutorEventType.STOP_RESULT,
                acquired_resources,
            )
            if self._trial_cleanup:  # force trial cleanup within a deadline
                self._trial_cleanup.add(future)

    def _resolve_stop_event(
        self,
        future: ray.ObjectRef,
        acquired_resources: AcquiredResources,
        timeout: Optional[float] = None,
    ):
        """Resolve stopping future (Trainable.cleanup() and free resources."""
        try:
            # Let's check one more time if the future resolved. If not,
            # we remove the PG which will terminate the actor.
            ray.get(future, timeout=timeout)
        except GetTimeoutError:
            if log_once("tune_trial_cleanup_timeout"):
                logger.error(
                    "Timed out when trying to stop the Ray actor gracefully. "
                    "Consider making `stop` a faster operation."
                )
        except Exception:
            if log_once("tune_trial_cleanup_exception"):
                logger.error(
                    f"An exception occurred when trying to stop the Ray actor:"
                    f"{traceback.format_exc()}"
                )
        finally:
            self._resource_manager.free_resources(acquired_resources)

    def _do_force_trial_cleanup(self) -> None:
        if self._trial_cleanup:
            while True:
                next_future_to_clean = self._trial_cleanup.get_next()
                if not next_future_to_clean:
                    break
                if next_future_to_clean in self._futures:
                    event_type, acquired_resources = self._futures.pop(
                        next_future_to_clean
                    )

                    assert event_type == _ExecutorEventType.STOP_RESULT

                    # Clean this future
                    self._resolve_stop_event(
                        next_future_to_clean, acquired_resources, timeout=0.01
                    )

                else:
                    # This just means that before the deadline reaches,
                    # the future is already cleaned up.
                    pass

    def force_reconcilation_on_next_step_end(self) -> None:
        self._resource_manager.update_state()

    def save(
        self,
        trial: Trial,
        storage: CheckpointStorage = CheckpointStorage.PERSISTENT,
        result: Optional[Dict] = None,
    ) -> _TrackedCheckpoint:
        """Saves the trial's state to a checkpoint asynchronously.

        Args:
            trial: The trial to be saved.
            storage: Where to store the checkpoint. Defaults to
                PERSISTENT.
            result: The state of this trial as a dictionary to be saved.
                If result is None, the trial's last result will be used.

        Returns:
             Checkpoint object, or None if an Exception occurs.
        """
        logger.debug(f"saving trial {trial}")
        result = result or trial.last_result
        with _change_working_directory(trial):
            if storage == CheckpointStorage.MEMORY:
                value = trial.runner.save_to_object.remote()
                checkpoint = _TrackedCheckpoint(
                    dir_or_data=value, storage_mode=storage, metrics=result
                )
                trial.on_checkpoint(checkpoint)
            else:
                value = trial.runner.save.remote()
                checkpoint = _TrackedCheckpoint(
                    dir_or_data=value,
                    storage_mode=storage,
                    metrics=result,
                )
                trial.saving_to = checkpoint
                self._futures[value] = (_ExecutorEventType.SAVING_RESULT, trial)
        return checkpoint

    def restore(self, trial: Trial) -> None:
        """Restores training state from a given model checkpoint.

        Args:
            trial: The trial to be restored.

        Raises:
            RuntimeError: This error is raised if no runner is found.
            AbortTrialExecution: This error is raised if the trial is
                ineligible for restoration, given the Tune input arguments.
        """
        checkpoint = trial.checkpoint
        if checkpoint.dir_or_data is None:
            return
        if trial.runner is None:
            raise RuntimeError(
                "Trial {}: Unable to restore - no runner found.".format(trial)
            )
        checkpoint_dir = checkpoint.dir_or_data
        node_ip = checkpoint.node_ip
        if checkpoint.storage_mode == CheckpointStorage.MEMORY:
            logger.debug("Trial %s: Attempting restore from object", trial)
            # Note that we don't store the remote since in-memory checkpoints
            # don't guarantee fault tolerance and don't need to be waited on.
            with _change_working_directory(trial):
                trial.runner.restore_from_object.remote(checkpoint_dir)
        else:
            logger.debug("Trial %s: Attempting restore from %s", trial, checkpoint_dir)
            if (
                trial.uses_cloud_checkpointing
                or not trial.sync_on_checkpoint
                or not os.path.exists(checkpoint_dir)
            ):
                # If using cloud checkpointing, trial will get cp from cloud.
                # If not syncing to driver, assume it has access to the cp
                # on the local fs.
                fallback_to_latest = bool(
                    int(os.environ.get("TUNE_FALLBACK_TO_LATEST_CHECKPOINT", "1"))
                )

                with _change_working_directory(trial):
                    remote = trial.runner.restore.remote(
                        checkpoint_dir,
                        checkpoint_node_ip=node_ip,
                        fallback_to_latest=fallback_to_latest,
                    )

            elif trial.sync_on_checkpoint:
                # This provides FT backwards compatibility in the
                # case where no cloud checkpoints are provided.
                logger.debug("Trial %s: Reading checkpoint into memory", trial)
                checkpoint_path = TrainableUtil.find_checkpoint_dir(checkpoint_dir)
                obj = Checkpoint.from_directory(checkpoint_path).to_bytes()
                with _change_working_directory(trial):
                    remote = trial.runner.restore_from_object.remote(obj)
            else:
                raise _AbortTrialExecution(
                    "Pass in `sync_on_checkpoint=True` for driver-based trial"
                    "restoration. Pass in an `upload_dir` for remote "
                    "storage-based restoration"
                )

            self._futures[remote] = (_ExecutorEventType.RESTORING_RESULT, trial)
            trial.restoring_from = checkpoint

    def export_trial_if_needed(self, trial: Trial) -> Dict:
        """Exports model of this trial based on trial.export_formats.

        Return:
            A dict that maps ExportFormats to successfully exported models.
        """
        if trial.export_formats and len(trial.export_formats) > 0:
            with _change_working_directory(trial):
                return ray.get(
                    trial.runner.export_model.remote(trial.export_formats),
                    timeout=DEFAULT_GET_TIMEOUT,
                )
        return {}

    def has_gpus(self) -> bool:
        return self._resource_updater.get_num_gpus() > 0

    def cleanup(self) -> None:
        self._cleanup_cached_actors(force_all=True)

        while self._futures:
            if self._trial_cleanup and self._trial_cleanup.is_empty():
                break

            # Non-blocking trial cleanup futures
            self._do_force_trial_cleanup()

            # Deal with other futures
            ready, _ = ray.wait(list(self._futures.keys()), timeout=0)
            if not ready:
                continue

            event_type, acquired_resources = self._futures.pop(ready[0])

            # It could be STOP future after all, if so, deal with it here.
            if event_type == _ExecutorEventType.STOP_RESULT:
                # Blocking here is ok as the future returned
                self._resolve_stop_event(ready[0], acquired_resources, timeout=None)

        for staged_trial in self._staged_trials:
            resource_request = staged_trial.placement_group_factory
            self._resource_manager.cancel_resource_request(
                resource_request=resource_request
            )

        self._resource_manager.clear()

    def get_next_executor_event(
        self, live_trials: Set[Trial], next_trial_exists: bool
    ) -> _ExecutorEvent:
        """Get the next executor event to be processed in TrialRunner.

        In case there are multiple events available for handling, the next
        event is determined by the following priority:
        1. if there is `next_trial_exists`, and if there is cached resources
        to use, PG_READY is emitted.
        2. if there is `next_trial_exists` and there is no cached resources
        to use, wait on pg future and randomized other futures. If multiple
        futures are ready, pg future will take priority to be handled first.
        3. if there is no `next_trial_exists`, wait on just randomized other
        futures.

        An example of #3 would be synchronous hyperband. Although there are pgs
        ready, the scheduler is holding back scheduling new trials since the
        whole band of trials is waiting for the slowest trial to finish. In
        this case, we prioritize handling training result to avoid deadlock
        situation.

        This is a blocking wait with a timeout (specified with env var).
        The reason for the timeout is
        we still want to print status info periodically in TrialRunner for
        better user experience.

        The handle of `ExecutorEvent.STOP_RESULT` is purely internal to
        RayTrialExecutor itself. All the other future results are handled by
        TrialRunner.

        In the future we may want to do most of the handle of
        `ExecutorEvent.RESTORE_RESULT` and `SAVING_RESULT` in
        RayTrialExecutor itself and only notify TrialRunner to invoke
        corresponding callbacks. This view is more consistent with our goal
        of TrialRunner responsible for external facing Trial state transition,
        while RayTrialExecutor responsible for internal facing transitions,
        namely, `is_saving`, `is_restoring` etc.

        Also you may notice that the boundary between RayTrialExecutor and
        PlacementGroupManager right now is really blurry. This will be
        improved once we move to an ActorPool abstraction.

        `next_trial_exists` means that there is a trial to run - prioritize
        returning PG_READY in this case.
        """
        # First update status of staged placement groups
        self._stage_and_update_status(live_trials)
        while True:
            ###################################################################
            # when next_trial_exists and there are cached resources
            ###################################################################
            # There could be existing PGs from either
            # `self._actor_cache`
            # or from ready trials. If so and if there is indeed
            # a next trial to run, we return `PG_READY` future for trial
            # runner. The next trial can then be scheduled on this PG.
            if next_trial_exists:
                if self._actor_cache.num_cached_objects > 0:
                    return _ExecutorEvent(_ExecutorEventType.PG_READY)
                # TODO(xwjiang): Expose proper API when we decide to do
                #  ActorPool abstraction.
                if any(
                    self._resource_manager.has_resources_ready(
                        trial.placement_group_factory
                    )
                    for trial in self._staged_trials
                ):
                    return _ExecutorEvent(_ExecutorEventType.PG_READY)

            ###################################################################
            # Prepare for futures to wait
            ###################################################################
            if self._cached_ready_futures and not next_trial_exists:
                # If there are cached ready futures, handle the first.
                # But: If next trial exists, we want to prioritize PG_READY events.
                ready_futures = [self._cached_ready_futures.pop(0)]
            else:
                # Otherwise, wait for new futures
                futures_to_wait = list(self._futures.keys())
                random.shuffle(futures_to_wait)
                if next_trial_exists:
                    # Only wait for pg explicitly if there is next trial to run.
                    # In which case, handling PG_READY triumphs handling other events.
                    # Since we want to place pending trial ASAP.
                    futures_to_wait = (
                        self._resource_manager.get_resource_futures() + futures_to_wait
                    )
                logger.debug(
                    f"get_next_executor_event before wait with futures "
                    f"{futures_to_wait} and "
                    f"next_trial_exists={next_trial_exists}"
                )

                # Try to resolve all ready futures that are immediately ready
                ready, _ = ray.wait(
                    futures_to_wait, num_returns=len(futures_to_wait), timeout=0
                )

                if ready:
                    # If at least one future resolved, use that one. Cache the other
                    # ones.
                    ready_futures = [ready.pop(0)]
                    self._cached_ready_futures = ready
                else:
                    # Otherwise, wait for next future with timeout.
                    ready_futures, _ = ray.wait(
                        futures_to_wait,
                        num_returns=1,
                        timeout=self._get_next_event_wait,
                    )

            ###################################################################
            # Dealing with no future returned case.
            ###################################################################
            if len(ready_futures) == 0:
                if len(self._futures) == 0:
                    # No running trial and timing out with wait, could be we may
                    # have insufficient cluster resources that makes tune run
                    # infeasible.
                    # TODO: Move InsufficientResourceManager's logic
                    #  to TrialExecutor. It is not Runner's responsibility!
                    return _ExecutorEvent(_ExecutorEventType.NO_RUNNING_TRIAL_TIMEOUT)
                else:
                    # Training simply takes long time, yield the control back to main
                    # event loop to print progress info etc.
                    return _ExecutorEvent(_ExecutorEventType.YIELD)

            ###################################################################
            # If there is future returned.
            ###################################################################
            assert len(ready_futures) == 1
            ready_future = ready_futures[0]

            ###################################################################
            # If it is a PG_READY event.
            ###################################################################
            if ready_future not in self._futures:
                self._resource_manager.update_state()
                return _ExecutorEvent(_ExecutorEventType.PG_READY)

            ###################################################################
            # non PG_READY event
            ###################################################################
            result_type, trial_or_acquired_resources = self._futures.pop(ready_future)
            if result_type == _ExecutorEventType.STOP_RESULT:
                # This will block, which is ok as the stop future returned
                self._resolve_stop_event(
                    ready_future, trial_or_acquired_resources, timeout=None
                )
            else:
                trial = trial_or_acquired_resources
                assert isinstance(trial, Trial)
                assert result_type in (
                    _ExecutorEventType.TRAINING_RESULT,
                    _ExecutorEventType.SAVING_RESULT,
                    _ExecutorEventType.RESTORING_RESULT,
                )
                try:
                    future_result = ray.get(ready_future)
                    # For local mode
                    if isinstance(future_result, _LocalWrapper):
                        future_result = future_result.unwrap()
                    logger.debug(f"Returning [{result_type}] for trial {trial}")
                    return _ExecutorEvent(
                        result_type,
                        trial,
                        result={_ExecutorEvent.KEY_FUTURE_RESULT: future_result},
                    )
                except Exception as e:
                    return _ExecutorEvent(
                        result_type,
                        trial,
                        result={
                            _ExecutorEvent.KEY_EXCEPTION: e.as_instanceof_cause()
                            if isinstance(e, RayTaskError)
                            else _TuneNoNextExecutorEventError(traceback.format_exc())
                        },
                    )
