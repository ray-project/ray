# coding: utf-8
import copy
import inspect
import random
from collections import deque
from enum import Enum
from functools import partial
import logging
import os
import time
import traceback
from contextlib import contextmanager
from typing import (
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Union,
)

import ray
from ray.exceptions import GetTimeoutError, RayTaskError
from ray.tune.error import (
    AbortTrialExecution,
    TuneError,
    TuneStartTrialError,
    TuneGetNextExecutorEventError,
)
from ray.tune.logger import NoopLogger
from ray.tune.result import TRIAL_INFO, STDOUT_FILE, STDERR_FILE
from ray.tune.utils.placement_groups import PlacementGroupManager, get_tune_pg_prefix
from ray.tune.utils.trainable import TrainableUtil
from ray.tune.trial import Trial, _TuneCheckpoint, Location, TrialInfo
from ray.tune.trial_executor import TrialExecutor
from ray.tune.utils import warn_if_slow
from ray.tune.utils.resource_updater import ResourceUpdater
from ray.util import log_once
from ray.util.annotations import DeveloperAPI
from ray.util.placement_group import remove_placement_group, PlacementGroup

logger = logging.getLogger(__name__)

DEFAULT_GET_TIMEOUT = 60.0  # seconds


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
        runtime_env = {"env_vars": {"TUNE_ORIG_WORKING_DIR": os.getcwd()}}
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


def post_stop_cleanup(future, pg):
    """Things to be done after a trial is stopped."""
    assert isinstance(pg, PlacementGroup)
    try:
        # This should not be blocking as
        # we are only here when triggered.
        ray.get(future, timeout=0)
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
        remove_placement_group(pg)


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
        if (
            len(self._future_to_insert_time) > 0
            and self._future_to_insert_time[0][1] + self._force_cleanup < time.time()
        ):
            return self._future_to_insert_time.popleft()
        else:
            return None

    def is_empty(self):
        return len(self._future_to_insert_time) == 0


def noop_logger_creator(config, logdir):
    # Set the working dir in the remote process, for user file writes
    os.makedirs(logdir, exist_ok=True)
    if not ray.worker._mode() == ray.worker.LOCAL_MODE:
        os.chdir(logdir)
    return NoopLogger(config, logdir)


class ExecutorEventType(Enum):
    """The executor event type.

    Some of the events are internal events to executor while others
    are handled by runner."""

    NO_RUNNING_TRIAL_TIMEOUT = 1
    PG_READY = 2
    TRAINING_RESULT = 3
    SAVING_RESULT = 4
    RESTORING_RESULT = 5
    STOP_RESULT = 6  # Internally to executor only.
    ERROR = 7  # This is to signal to TrialRunner that there is an error.
    YIELD = 8  # Yielding back to TrialRunner's main event loop.


class ExecutorEvent:
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
        event_type: ExecutorEventType,
        trial: Optional[Trial] = None,
        result: Optional[Dict] = None,
    ):
        self.type = event_type
        self.trial = trial
        self.result = result

    def __repr__(self):
        return f"[{self.type}] for {self.trial}"


@DeveloperAPI
class RayTrialExecutor(TrialExecutor):
    """An implementation of TrialExecutor based on Ray."""

    def __init__(
        self,
        reuse_actors: bool = False,
        result_buffer_length: Optional[int] = None,
        refresh_period: Optional[float] = None,
    ):
        super(RayTrialExecutor, self).__init__()
        # future --> (type, trial/pg)
        self._futures = {}

        force_trial_cleanup = int(os.environ.get("TUNE_FORCE_TRIAL_CLEANUP_S", "0"))
        self._get_next_event_wait = int(
            os.environ.get("TUNE_GET_EXECUTOR_EVENT_WAIT_S", "5")
        )
        if force_trial_cleanup:
            self._trial_cleanup = _TrialCleanup(force_trial_cleanup)
        else:
            self._trial_cleanup = None

        self._resource_updater = ResourceUpdater(refresh_period)

        self._has_cleaned_up_pgs = False
        self._reuse_actors = reuse_actors
        # The maxlen will be updated when `set_max_pending_trials()` is called
        self._cached_actor_pg = deque(maxlen=1)
        self._pg_manager = PlacementGroupManager(prefix=get_tune_pg_prefix())
        self._staged_trials = set()
        self._trial_just_finished = False
        self._trial_just_finished_before = False
        self.last_pg_recon = 0
        self.pg_recon_interval = float(
            os.environ.get("TUNE_PLACEMENT_GROUP_RECON_INTERVAL", "5")
        )

        self._buffer_length = result_buffer_length or int(
            os.getenv("TUNE_RESULT_BUFFER_LENGTH", 1)
        )
        self._buffer_min_time_s = float(os.getenv("TUNE_RESULT_BUFFER_MIN_TIME_S", 0.0))
        self._buffer_max_time_s = float(
            os.getenv("TUNE_RESULT_BUFFER_MAX_TIME_S", 100.0)
        )

    def set_max_pending_trials(self, max_pending: int) -> None:
        if len(self._cached_actor_pg) > 0:
            logger.warning(
                "Cannot update maximum number of queued actors for reuse "
                "during a run."
            )
        else:
            self._cached_actor_pg = deque(maxlen=max_pending)
        self._pg_manager.set_max_staging(max_pending)

    def _stage_and_update_status(self, trials: Iterable[Trial]):
        """Check and update statuses of scheduled placement groups.

        Stages placement groups of all trials.
        """
        if not self._has_cleaned_up_pgs:
            # Clean up existing placement groups after trigger the tuning
            # run step() method for the first time
            self._pg_manager.cleanup_existing_pg()
            self._has_cleaned_up_pgs = True

        for trial in trials:
            if trial.status not in (Trial.PENDING, Trial.PAUSED):
                continue
            if trial in self._staged_trials:
                continue
            if self._pg_manager.trial_in_use(trial):
                continue

            if not self._pg_manager.stage_trial_pg(trial):
                # Break if we reached the limit of pending placement groups.
                break
            self._staged_trials.add(trial)

        self._pg_manager.update_status()

    def get_staged_trial(self):
        """Get a trial whose placement group was successfully staged.

        Can also return None if no trial is available.

        Returns:
            Trial object or None.

        """
        # TODO(xwjiang): This method should consider `self._cached_actor_pg`.
        for trial in self._staged_trials:
            if self._pg_manager.has_ready(trial):
                return trial

        return None

    def _setup_remote_runner(self, trial):
        trial.init_logdir()
        # We checkpoint metadata here to try mitigating logdir duplication
        self._trials_to_cache.add(trial)
        logger_creator = partial(noop_logger_creator, logdir=trial.logdir)

        if len(self._cached_actor_pg) > 0:
            assert self._reuse_actors
            existing_runner, pg = self._cached_actor_pg.popleft()
            logger.debug(f"Trial {trial}: Reusing cached runner " f"{existing_runner}")

            trial.set_runner(existing_runner)
            if pg:
                self._pg_manager.assign_cached_pg(pg, trial)

            if not self.reset_trial(
                trial, trial.config, trial.experiment_tag, logger_creator
            ):
                raise AbortTrialExecution(
                    "Trainable runner reuse requires reset_config() to be "
                    "implemented and return True."
                )
            return existing_runner

        trainable_cls = trial.get_trainable_cls()
        if not trainable_cls:
            raise AbortTrialExecution(
                f"Invalid trainable: {trial.trainable_name}. If you passed "
                f"a string, make sure the trainable was registered before."
            )
        _actor_cls = _class_cache.get(trainable_cls)

        if not self._pg_manager.has_ready(trial):
            return None

        full_actor_class = self._pg_manager.get_full_actor_cls(trial, _actor_cls)
        # Clear the Trial's location (to be updated later on result)
        # since we don't know where the remote runner is placed.
        trial.set_location(Location())
        logger.debug("Trial %s: Setting up new remote runner.", trial)
        # Logging for trials is handled centrally by TrialRunner, so
        # configure the remote runner to use a noop-logger.
        trial_config = copy.deepcopy(trial.config)
        trial_config[TRIAL_INFO] = TrialInfo(trial)

        stdout_file, stderr_file = trial.log_to_file
        trial_config[STDOUT_FILE] = stdout_file
        trial_config[STDERR_FILE] = stderr_file
        kwargs = {
            "config": trial_config,
            "logger_creator": logger_creator,
        }
        if trial.uses_cloud_checkpointing:
            # We keep these kwargs separate for backwards compatibility
            # with trainables that don't provide these keyword arguments
            kwargs["remote_checkpoint_dir"] = trial.remote_checkpoint_dir
            kwargs["sync_function_tpl"] = trial.sync_function_tpl

            # Throw a meaningful error if trainable does not use the
            # new API
            sig = inspect.signature(trial.get_trainable_cls())
            try:
                sig.bind_partial(**kwargs)
            except Exception as e:
                raise RuntimeError(
                    "Your trainable class does not accept a "
                    "`remote_checkpoint_dir` or `sync_function_tpl` argument "
                    "in its constructor, but you've passed a "
                    "`upload_dir` to your SyncConfig. Without accepting "
                    "these parameters and passing them to the base trainable "
                    "constructor in the init call, cloud checkpointing is "
                    "effectively disabled. To resolve this issue, add the "
                    "parameters to your trainable class constructor or "
                    "disable cloud checkpointing by setting `upload_dir=None`."
                ) from e

        with self._change_working_directory(trial):
            return full_actor_class.remote(**kwargs)

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
        with self._change_working_directory(trial):
            buffer_length = self._buffer_length
            if buffer_length > 1 and trial.checkpoint_at_end:
                # If a trial checkpoint can be triggered externally,
                # it is not safe to buffer results.
                if log_once("trial_executor_buffer_checkpoint"):
                    logger.warning(
                        "Disabling buffered training as you passed "
                        "`checkpoint_at_end` to `tune.run()`."
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

        self._futures[remote] = (ExecutorEventType.TRAINING_RESULT, trial)
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

        if trial in self._staged_trials:
            self._staged_trials.remove(trial)

        if not trial.is_restoring:
            self._train(trial)
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
        self._trial_just_finished = True
        trial.set_location(Location())

        try:
            trial.write_error_log(exc=exc)
            if hasattr(trial, "runner") and trial.runner:
                if (
                    not error
                    and self._reuse_actors
                    and (
                        len(self._cached_actor_pg)
                        < (self._cached_actor_pg.maxlen or float("inf"))
                    )
                ):
                    logger.debug("Reusing actor for %s", trial.runner)
                    # Move PG into cache (disassociate from trial)
                    pg = self._pg_manager.cache_trial_pg(trial)
                    if pg:
                        # True if a placement group was replaced
                        self._cached_actor_pg.append((trial.runner, pg))
                        should_destroy_actor = False
                    else:
                        # False if no placement group was replaced. This should
                        # only be the case if there are no more trials with
                        # this placement group factory to run
                        logger.debug(
                            f"Could not cache actor of trial {trial} for "
                            "reuse, as there are no pending trials "
                            "requiring its resources."
                        )
                        should_destroy_actor = True
                else:
                    should_destroy_actor = True

                if should_destroy_actor:
                    logger.debug("Trial %s: Destroying actor.", trial)

                    with self._change_working_directory(trial):
                        future = trial.runner.stop.remote()

                    pg = self._pg_manager.remove_from_in_use(trial)
                    self._futures[future] = (ExecutorEventType.STOP_RESULT, pg)
                    if self._trial_cleanup:  # force trial cleanup within a deadline
                        self._trial_cleanup.add(future)

                if trial in self._staged_trials:
                    self._staged_trials.remove(trial)

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
        except AbortTrialExecution as e:
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
                self._stop_trial(trial, exc=TuneStartTrialError(traceback.format_exc()))
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
        prior_status = trial.status
        self._stop_trial(trial, error=error or exc, exc=exc)
        if prior_status == Trial.RUNNING:
            logger.debug("Trial %s: Returning resources.", trial)
            out = self._find_future(trial)
            for result_id in out:
                self._futures.pop(result_id)

    def continue_training(self, trial: Trial) -> None:
        """Continues the training of this trial."""
        self._train(trial)

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
        extra_config[TRIAL_INFO] = TrialInfo(trial)

        stdout_file, stderr_file = trial.log_to_file
        extra_config[STDOUT_FILE] = stdout_file
        extra_config[STDERR_FILE] = stderr_file

        with self._change_working_directory(trial):
            with warn_if_slow("reset"):
                try:
                    reset_val = ray.get(
                        trainable.reset.remote(extra_config, logger_creator),
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
        return (
            trial in self._staged_trials
            or self._pg_manager.can_stage()
            or self._pg_manager.has_ready(trial, update=True)
        )

    def debug_string(self) -> str:
        """Returns a human readable message for printing to the console."""
        total_resources = self._pg_manager.occupied_resources()
        return self._resource_updater.debug_string(total_resources)

    def on_step_begin(self, trials: List[Trial]) -> None:
        """Before step() is called, update the available resources."""
        self._resource_updater.update_avail_resources()
        self._trial_just_finished_before = self._trial_just_finished
        self._trial_just_finished = False

    def on_step_end(self, trials: List[Trial]) -> None:
        self._do_force_trial_cleanup()
        if time.time() > self.last_pg_recon + self.pg_recon_interval:
            # Only do this every now and then - usually the placement groups
            # should not get out of sync, and calling this often is inefficient
            self._pg_manager.reconcile_placement_groups(trials)
            self.last_pg_recon = time.time()

        self._pg_manager.cleanup()

    def _do_force_trial_cleanup(self) -> None:
        if self._trial_cleanup:
            while True:
                next_future_to_clean = self._trial_cleanup.get_next()
                if not next_future_to_clean:
                    break
                if next_future_to_clean in self._futures.keys():
                    _, pg = self._futures.pop(next_future_to_clean)
                    post_stop_cleanup(next_future_to_clean, pg)
                else:
                    # This just means that before the deadline reaches,
                    # the future is already cleaned up.
                    pass

    def force_reconcilation_on_next_step_end(self) -> None:
        self.last_pg_recon = -float("inf")

    def save(
        self,
        trial: Trial,
        storage: str = _TuneCheckpoint.PERSISTENT,
        result: Optional[Dict] = None,
    ) -> _TuneCheckpoint:
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
        with self._change_working_directory(trial):
            if storage == _TuneCheckpoint.MEMORY:
                value = trial.runner.save_to_object.remote()
                checkpoint = _TuneCheckpoint(storage, value, result)
                trial.on_checkpoint(checkpoint)
            else:
                value = trial.runner.save.remote()
                checkpoint = _TuneCheckpoint(storage, value, result)
                trial.saving_to = checkpoint
                self._futures[value] = (ExecutorEventType.SAVING_RESULT, trial)
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
        if checkpoint.value is None:
            return
        if trial.runner is None:
            raise RuntimeError(
                "Trial {}: Unable to restore - no runner found.".format(trial)
            )
        value = checkpoint.value
        node_ip = checkpoint.node_ip
        if checkpoint.storage == _TuneCheckpoint.MEMORY:
            logger.debug("Trial %s: Attempting restore from object", trial)
            # Note that we don't store the remote since in-memory checkpoints
            # don't guarantee fault tolerance and don't need to be waited on.
            with self._change_working_directory(trial):
                trial.runner.restore_from_object.remote(value)
        else:
            logger.debug("Trial %s: Attempting restore from %s", trial, value)
            if trial.uses_cloud_checkpointing or not trial.sync_on_checkpoint:
                # If using cloud checkpointing, trial will get cp from cloud.
                # If not syncing to driver, assume it has access to the cp
                # on the local fs.
                with self._change_working_directory(trial):
                    remote = trial.runner.restore.remote(value, node_ip)
            elif trial.sync_on_checkpoint:
                # This provides FT backwards compatibility in the
                # case where no cloud checkpoints are provided.
                logger.debug("Trial %s: Reading checkpoint into memory", trial)
                obj = TrainableUtil.checkpoint_to_object(value)
                with self._change_working_directory(trial):
                    remote = trial.runner.restore_from_object.remote(obj)
            else:
                raise AbortTrialExecution(
                    "Pass in `sync_on_checkpoint=True` for driver-based trial"
                    "restoration. Pass in an `upload_dir` for remote "
                    "storage-based restoration"
                )

            self._futures[remote] = (ExecutorEventType.RESTORING_RESULT, trial)
            trial.restoring_from = checkpoint

    def export_trial_if_needed(self, trial: Trial) -> Dict:
        """Exports model of this trial based on trial.export_formats.

        Return:
            A dict that maps ExportFormats to successfully exported models.
        """
        if trial.export_formats and len(trial.export_formats) > 0:
            with self._change_working_directory(trial):
                return ray.get(
                    trial.runner.export_model.remote(trial.export_formats),
                    timeout=DEFAULT_GET_TIMEOUT,
                )
        return {}

    def has_gpus(self) -> bool:
        return self._resource_updater.get_num_gpus() > 0

    def cleanup(self, trials: List[Trial]) -> None:
        while True:
            if self._trial_cleanup and self._trial_cleanup.is_empty():
                break
            elif not self._trial_cleanup and len(self._futures) == 0:
                break
            self._do_force_trial_cleanup()
            ready, _ = ray.wait(list(self._futures.keys()), timeout=0)
            if not ready:
                continue
            event_type, trial_or_pg = self._futures.pop(ready[0])
            if event_type == ExecutorEventType.STOP_RESULT:
                post_stop_cleanup(ready[0], trial_or_pg)

        self._pg_manager.reconcile_placement_groups(trials)
        self._pg_manager.cleanup(force=True)
        self._pg_manager.cleanup_existing_pg(block=True)

    @contextmanager
    def _change_working_directory(self, trial):
        """Context manager changing working directory to trial logdir.
        Used in local mode.

        For non-local mode it is no-op.
        """
        if ray.worker._mode() == ray.worker.LOCAL_MODE:
            old_dir = os.getcwd()
            try:
                os.chdir(trial.logdir)
                yield
            finally:
                os.chdir(old_dir)
        else:
            yield

    def get_next_executor_event(
        self, live_trials: Set[Trial], next_trial_exists: bool
    ) -> ExecutorEvent:
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
            # There could be existing PGs from either `self._cached_actor_pg`
            # or from `self._pg_manager._ready`. If so and if there is indeed
            # a next trial to run, we return `PG_READY` future for trial
            # runner. The next trial can then be scheduled on this PG.
            if next_trial_exists:
                if len(self._cached_actor_pg) > 0:
                    return ExecutorEvent(ExecutorEventType.PG_READY)
                # TODO(xwjiang): Expose proper API when we decide to do
                #  ActorPool abstraction.
                if any(len(r) > 0 for r in self._pg_manager._ready.values()):
                    return ExecutorEvent(ExecutorEventType.PG_READY)

            ###################################################################
            # Prepare for futures to wait
            ###################################################################
            futures_to_wait = list(self._futures.keys())
            random.shuffle(futures_to_wait)
            if next_trial_exists:
                # Only wait for pg explicitly if there is next trial to run.
                # In which case, handling PG_READY triumphs handling other events.
                # Since we want to place pending trial ASAP.
                futures_to_wait = (
                    self._pg_manager.get_staging_future_list() + futures_to_wait
                )
            logger.debug(
                f"get_next_executor_event before wait with futures "
                f"{futures_to_wait} and "
                f"next_trial_exists={next_trial_exists}"
            )

            ready_futures, _ = ray.wait(
                futures_to_wait, num_returns=1, timeout=self._get_next_event_wait
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
                    return ExecutorEvent(ExecutorEventType.NO_RUNNING_TRIAL_TIMEOUT)
                else:
                    # Training simply takes long time, yield the control back to main
                    # event loop to print progress info etc.
                    return ExecutorEvent(ExecutorEventType.YIELD)

            ###################################################################
            # If there is future returned.
            ###################################################################
            assert len(ready_futures) == 1
            ready_future = ready_futures[0]

            ###################################################################
            # If it is a PG_READY event.
            ###################################################################
            if ready_future not in self._futures.keys():
                self._pg_manager.handle_ready_future(ready_future)
                return ExecutorEvent(ExecutorEventType.PG_READY)

            ###################################################################
            # non PG_READY event
            ###################################################################
            result_type, trial_or_pg = self._futures.pop(ready_future)
            if result_type == ExecutorEventType.STOP_RESULT:
                pg = trial_or_pg
                post_stop_cleanup(ready_future, pg)
            else:
                trial = trial_or_pg
                assert isinstance(trial, Trial)
                try:
                    future_result = ray.get(ready_future)
                    # For local mode
                    if isinstance(future_result, _LocalWrapper):
                        future_result = future_result.unwrap()
                    if result_type in (
                        ExecutorEventType.TRAINING_RESULT,
                        ExecutorEventType.SAVING_RESULT,
                        ExecutorEventType.RESTORING_RESULT,
                    ):
                        logger.debug(f"Returning [{result_type}] for trial {trial}")
                        return ExecutorEvent(
                            result_type,
                            trial,
                            result={ExecutorEvent.KEY_FUTURE_RESULT: future_result},
                        )
                    else:
                        raise TuneError(f"Unexpected future type - [{result_type}]")
                except RayTaskError as e:
                    return ExecutorEvent(
                        ExecutorEventType.ERROR,
                        trial,
                        result={ExecutorEvent.KEY_EXCEPTION: e.as_instanceof_cause()},
                    )
                except Exception:
                    return ExecutorEvent(
                        ExecutorEventType.ERROR,
                        trial,
                        result={
                            ExecutorEvent.KEY_EXCEPTION: TuneGetNextExecutorEventError(
                                traceback.format_exc()
                            )
                        },
                    )
