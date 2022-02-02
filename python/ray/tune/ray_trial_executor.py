# coding: utf-8
import copy
import inspect
from collections import deque
from functools import partial
import logging
import os
import random
import time
import traceback
from contextlib import contextmanager
from typing import (
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
)

import ray
from ray.actor import ActorHandle
from ray.exceptions import GetTimeoutError
from ray import ray_constants
from ray._private.resource_spec import NODE_ID_PREFIX
from ray.tune.error import AbortTrialExecution
from ray.tune.logger import NoopLogger
from ray.tune.result import TRIAL_INFO, STDOUT_FILE, STDERR_FILE
from ray.tune.resources import Resources
from ray.tune.utils.placement_groups import PlacementGroupManager, get_tune_pg_prefix
from ray.tune.utils.trainable import TrainableUtil
from ray.tune.trial import Trial, Checkpoint, Location, TrialInfo
from ray.tune.trial_executor import TrialExecutor
from ray.tune.utils import warn_if_slow
from ray.util import log_once
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(__name__)

TUNE_STATE_REFRESH_PERIOD = 10  # Refresh resources every 10 s
BOTTLENECK_WARN_PERIOD_S = 60
NONTRIVIAL_WAIT_TIME_THRESHOLD_S = 1e-3
DEFAULT_GET_TIMEOUT = 60.0  # seconds
TRIAL_CLEANUP_THRESHOLD = 100


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
        if trainable_cls not in self._cache:
            remote_cls = ray.remote(trainable_cls)
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
    """Mechanism for ensuring trial stop futures are cleaned up.

    Args:
        threshold (int): Number of futures to hold at once. If the threshold
            is passed, cleanup will kick in and remove futures.
        force_cleanup (int): Grace periods for forceful actor termination.
            If 0, actors will not be forcefully terminated.
    """

    def __init__(
        self, threshold: int = TRIAL_CLEANUP_THRESHOLD, force_cleanup: int = 0
    ):
        self.threshold = threshold
        self._cleanup_map = {}
        if force_cleanup < 0:
            force_cleanup = 0
        self._force_cleanup = force_cleanup

    def add(self, trial: Trial, actor: ActorHandle):
        """Adds a trial actor to be stopped.

        If the number of futures exceeds the threshold, the cleanup mechanism
        will kick in.

        Args:
            trial (Trial): The trial corresponding to the future.
            actor (ActorHandle): Handle to the trainable to be stopped.
        """
        future = actor.stop.remote()

        del actor

        self._cleanup_map[future] = trial
        if len(self._cleanup_map) > self.threshold:
            self.cleanup(partial=True)

    def cleanup(self, partial: bool = True):
        """Waits for cleanup to finish.

        If partial=False, all futures are expected to return. If a future
        does not return within the timeout period, the cleanup terminates.
        """
        # At this point, self._cleanup_map holds the last references
        # to actors. Removing those references either one-by-one
        # (graceful termination case) or all at once, by reinstantiating
        # self._cleanup_map (forceful termination case) will cause Ray
        # to kill the actors during garbage collection.
        logger.debug("Cleaning up futures")
        num_to_keep = int(self.threshold) / 2 if partial else 0
        while len(self._cleanup_map) > num_to_keep:
            dones, _ = ray.wait(
                list(self._cleanup_map),
                timeout=DEFAULT_GET_TIMEOUT
                if not self._force_cleanup
                else self._force_cleanup,
            )
            if not dones:
                logger.warning(
                    "Skipping cleanup - trainable.stop did not return in "
                    "time. Consider making `stop` a faster operation."
                )
                if not partial and self._force_cleanup:
                    logger.warning("Forcing trainable cleanup by terminating actors.")
                    self._cleanup_map = {}
                    return
            else:
                done = dones[0]
                del self._cleanup_map[done]


def noop_logger_creator(config, logdir):
    # Set the working dir in the remote process, for user file writes
    os.makedirs(logdir, exist_ok=True)
    if not ray.worker._mode() == ray.worker.LOCAL_MODE:
        os.chdir(logdir)
    return NoopLogger(config, logdir)


@DeveloperAPI
class RayTrialExecutor(TrialExecutor):
    """An implementation of TrialExecutor based on Ray."""

    def __init__(
        self,
        reuse_actors: bool = False,
        result_buffer_length: Optional[int] = None,
        refresh_period: Optional[float] = None,
        wait_for_placement_group: Optional[float] = None,
    ):
        super(RayTrialExecutor, self).__init__()
        self._running = {}

        force_trial_cleanup = int(os.environ.get("TUNE_FORCE_TRIAL_CLEANUP_S", "0"))
        self._trial_cleanup = _TrialCleanup(force_cleanup=force_trial_cleanup)
        self._has_cleaned_up_pgs = False
        self._reuse_actors = reuse_actors
        # The maxlen will be updated when `set_max_pending_trials()` is called
        self._cached_actor_pg = deque(maxlen=1)

        self._avail_resources = Resources(cpu=0, gpu=0)
        self._pg_manager = PlacementGroupManager(prefix=get_tune_pg_prefix())
        self._staged_trials = set()
        self._just_staged_trials = set()
        self._trial_just_finished = False
        self._trial_just_finished_before = False

        self._resources_initialized = False

        if refresh_period is None:
            refresh_period = float(
                os.environ.get("TUNE_STATE_REFRESH_PERIOD", TUNE_STATE_REFRESH_PERIOD)
            )
        self._refresh_period = refresh_period

        self._wait_for_pg = wait_for_placement_group or float(
            os.environ.get("TUNE_PLACEMENT_GROUP_WAIT_S", "-1")
        )
        if self._wait_for_pg < 0:
            self._wait_for_pg = None

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

        self._last_resource_refresh = float("-inf")
        self._last_ip_refresh = float("-inf")
        self._last_ip_addresses = set()
        self._last_nontrivial_wait = time.time()

        if ray.is_initialized():
            self._update_avail_resources()

    def in_staging_grace_period(self) -> bool:
        """Returns True if trials have recently been staged."""
        return self._pg_manager.in_staging_grace_period()

    def set_max_pending_trials(self, max_pending: int) -> None:
        if len(self._cached_actor_pg) > 0:
            logger.warning(
                "Cannot update maximum number of queued actors for reuse "
                "during a run."
            )
        else:
            self._cached_actor_pg = deque(maxlen=max_pending)
        self._pg_manager.set_max_staging(max_pending)

    def stage_and_update_status(self, trials: Iterable[Trial]):
        """Check and update statuses of scheduled placement groups.

        Stages placement groups of all trials.
        """
        if not self._has_cleaned_up_pgs:
            # Clean up existing placement groups after trigger the tuning
            # run step() method for the first time
            self._pg_manager.cleanup_existing_pg()
            self._has_cleaned_up_pgs = True

        for trial in trials:
            if trial.status != Trial.PENDING:
                continue
            if trial in self._staged_trials:
                continue
            if self._pg_manager.trial_in_use(trial):
                continue

            if not self._pg_manager.stage_trial_pg(trial):
                # Break if we reached the limit of pending placement groups.
                break
            self._staged_trials.add(trial)
            self._just_staged_trials.add(trial)

        self._pg_manager.update_status()

    def get_staged_trial(self):
        """Get a trial whose placement group was successfully staged.

        Can also return None if no trial is available.

        Returns:
            Trial object or None.

        """
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

        if not self._pg_manager.has_ready(trial, update=True):
            if trial not in self._staged_trials:
                if self._pg_manager.stage_trial_pg(trial):
                    self._staged_trials.add(trial)
                    self._just_staged_trials.add(trial)

            just_staged = trial in self._just_staged_trials

            # This part of the code is mostly here for testing
            # purposes. If self._wait_for_pg is set, we will wait here
            # for that many seconds until the placement group is ready.
            # This ensures that the trial can be started right away and
            # not just in the next step() of the trial runner.
            # We only do this if we have reason to believe that resources
            # will be ready, soon, i.e. when a) we just staged the PG,
            # b) another trial just exited, freeing resources, or c)
            # when there are no currently running trials.
            if self._wait_for_pg is not None and (
                just_staged
                or self._trial_just_finished_before
                or not self.get_running_trials()
            ):
                logger.debug(
                    f"Waiting up to {self._wait_for_pg} seconds for "
                    f"placement group of trial {trial} to become ready."
                )
                wait_end = time.monotonic() + self._wait_for_pg
                while time.monotonic() < wait_end:
                    self._pg_manager.update_status()
                    if self._pg_manager.has_ready(trial):
                        break
                    time.sleep(0.1)
            else:
                return None

        if not self._pg_manager.has_ready(trial):
            # PG may have become ready during waiting period
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

        if self._find_item(self._running, trial):
            logging.debug(
                "Trial {} already has a queued future. Skipping this "
                "`train` call. This may occur if a trial has "
                "been unpaused within a scheduler callback.".format(str(trial))
            )
            return

        assert trial.status == Trial.RUNNING, trial.status
        buffer_time_s = max(
            self._buffer_min_time_s,
            min(self._buffer_max_time_s, len(self._running) // 10),
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

        self._running[remote] = trial
        trial_item = self._find_item(self._running, trial)
        assert len(trial_item) < 2, trial_item

    def _start_trial(self, trial) -> bool:
        """Starts trial and restores last result if trial was paused.

        Args:
            trial (Trial): The trial to start.

        Returns:
            True if trial was started successfully, False otherwise.

        See `RayTrialExecutor.restore` for possible errors raised.
        """
        self.set_status(trial, Trial.PENDING)
        runner = self._setup_remote_runner(trial)
        if not runner:
            return False
        trial.set_runner(runner)
        self._notify_trainable_of_new_resources_if_needed(trial)
        self.restore(trial)
        self.set_status(trial, Trial.RUNNING)

        if trial in self._staged_trials:
            self._staged_trials.remove(trial)

        if not trial.is_restoring:
            self._train(trial)
        return True

    def _notify_trainable_of_new_resources_if_needed(self, trial: Trial):
        if trial.has_new_resources:
            trainable = trial.runner
            trial.has_new_resources = False
            with self._change_working_directory(trial):
                with warn_if_slow("update_resources"):
                    try:
                        ray.get(
                            trainable._update_resources.remote(
                                trial.placement_group_factory
                            ),
                            timeout=DEFAULT_GET_TIMEOUT,
                        )
                    except GetTimeoutError:
                        logger.exception(
                            "Trial %s: updating resources timed out.", trial
                        )

    def _stop_trial(self, trial: Trial, error=False, error_msg=None):
        """Stops this trial.

        Stops this trial, releasing all allocating resources. If stopping the
        trial fails, the run will be marked as terminated in error, but no
        exception will be thrown.

        Args:
            error (bool): Whether to mark this trial as terminated in error.
            error_msg (str): Optional error message.

        """
        self.set_status(trial, Trial.ERROR if error else Trial.TERMINATED)
        self._trial_just_finished = True
        trial.set_location(Location())

        try:
            trial.write_error_log(error_msg)
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
                            "Could not cache of trial {trial} actor for "
                            "reuse, as there are no pending trials "
                            "requiring its resources."
                        )
                        should_destroy_actor = True
                else:
                    should_destroy_actor = True

                if should_destroy_actor:
                    logger.debug("Trial %s: Destroying actor.", trial)

                    # Try to return the placement group for other trials to use
                    self._pg_manager.return_pg(trial)

                    with self._change_working_directory(trial):
                        self._trial_cleanup.add(trial, actor=trial.runner)

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
            trial (Trial): Trial to be started.

        Returns:
            True if the remote runner has been started. False if trial was
                not started (e.g. because of lacking resources/pending PG).
        """
        try:
            return self._start_trial(trial)
        except AbortTrialExecution:
            logger.exception("Trial %s: Error starting runner, aborting!", trial)
            time.sleep(2)
            error_msg = traceback.format_exc()
            self._stop_trial(trial, error=True, error_msg=error_msg)
            return False
        except Exception:
            logger.exception("Trial %s: Unexpected error starting runner.", trial)
            time.sleep(2)
            error_msg = traceback.format_exc()
            self._stop_trial(trial, error=True, error_msg=error_msg)
            # Note that we don't return the resources, since they may
            # have been lost. TODO(ujvl): is this the right thing to do?
            return False

    def _find_item(self, dictionary, item):
        out = [rid for rid, t in dictionary.items() if t is item]
        assert (
            len(out) <= 1
        ), "Expecting one future for any given trial at any given time."
        return out

    def stop_trial(
        self, trial: Trial, error: bool = False, error_msg: Optional[str] = None
    ) -> None:
        prior_status = trial.status
        self._stop_trial(trial, error=error, error_msg=error_msg)
        if prior_status == Trial.RUNNING:
            logger.debug("Trial %s: Returning resources.", trial)
            out = self._find_item(self._running, trial)
            for result_id in out:
                self._running.pop(result_id)

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
            trial (Trial): Trial to be reset.
            new_config (dict): New configuration for Trial trainable.
            new_experiment_tag (str): New experiment name for trial.
            logger_creator (Optional[Callable[[Dict], Logger]]): Function
                that instantiates a logger on the actor process.

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

    def get_running_trials(self) -> List[Trial]:
        """Returns the running trials."""
        return list(self._running.values())

    def get_next_available_trial(
        self, timeout: Optional[float] = None
    ) -> Optional[Trial]:
        if not self._running:
            return None
        shuffled_results = list(self._running.keys())
        random.shuffle(shuffled_results)

        # Note: We shuffle the results because `ray.wait` by default returns
        # the first available result, and we want to guarantee that slower
        # trials (i.e. trials that run remotely) also get fairly reported.
        # See https://github.com/ray-project/ray/issues/4211 for details.
        start = time.time()
        ready, _ = ray.wait(shuffled_results, timeout=timeout)
        if not ready:
            return None
        result_id = ready[0]
        wait_time = time.time() - start
        if wait_time > NONTRIVIAL_WAIT_TIME_THRESHOLD_S:
            self._last_nontrivial_wait = time.time()
        if time.time() - self._last_nontrivial_wait > BOTTLENECK_WARN_PERIOD_S:
            logger.warning(
                "Over the last {} seconds, the Tune event loop has been "
                "backlogged processing new results. Consider increasing your "
                "period of result reporting to improve performance.".format(
                    BOTTLENECK_WARN_PERIOD_S
                )
            )

            self._last_nontrivial_wait = time.time()
        return self._running[result_id]

    def fetch_result(self, trial) -> List[Dict]:
        """Fetches result list of the running trials.

        Returns:
            Result of the most recent trial training run.
        """
        trial_future = self._find_item(self._running, trial)
        if not trial_future:
            raise ValueError("Trial was not running.")
        self._running.pop(trial_future[0])
        with warn_if_slow("fetch_result"):
            result = ray.get(trial_future[0], timeout=DEFAULT_GET_TIMEOUT)

        # For local mode
        if isinstance(result, _LocalWrapper):
            result = result.unwrap()

        if not isinstance(result, list):
            return [result]
        return result

    def _update_avail_resources(self, num_retries=5):
        if time.time() - self._last_resource_refresh < self._refresh_period:
            return
        logger.debug("Checking Ray cluster resources.")
        resources = None
        for i in range(num_retries):
            if i > 0:
                logger.warning(
                    "Cluster resources not detected or are 0. Attempt #" "%s...", i + 1
                )
                time.sleep(0.5)
            resources = ray.cluster_resources()
            if resources:
                break

        if not resources:
            # NOTE: This hides the possibility that Ray may be waiting for
            # clients to connect.
            resources.setdefault("CPU", 0)
            resources.setdefault("GPU", 0)
            logger.warning(
                "Cluster resources cannot be detected or are 0. "
                "You can resume this experiment by passing in "
                "`resume=True` to `run`."
            )

        resources = resources.copy()
        num_cpus = resources.pop("CPU", 0)
        num_gpus = resources.pop("GPU", 0)
        memory = ray_constants.from_memory_units(resources.pop("memory", 0))
        object_store_memory = ray_constants.from_memory_units(
            resources.pop("object_store_memory", 0)
        )
        custom_resources = resources

        self._avail_resources = Resources(
            int(num_cpus),
            int(num_gpus),
            memory=int(memory),
            object_store_memory=int(object_store_memory),
            custom_resources=custom_resources,
        )
        self._last_resource_refresh = time.time()
        self._resources_initialized = True

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

        if self._resources_initialized:
            status = (
                "Resources requested: {}/{} CPUs, {}/{} GPUs, "
                "{}/{} GiB heap, {}/{} GiB objects".format(
                    total_resources.pop("CPU", 0),
                    self._avail_resources.cpu,
                    total_resources.pop("GPU", 0),
                    self._avail_resources.gpu,
                    _to_gb(total_resources.pop("memory", 0.0)),
                    _to_gb(self._avail_resources.memory),
                    _to_gb(total_resources.pop("object_store_memory", 0.0)),
                    _to_gb(self._avail_resources.object_store_memory),
                )
            )
            customs = ", ".join(
                [
                    "{}/{} {}".format(
                        total_resources.get(name, 0.0),
                        self._avail_resources.get_res_total(name),
                        name,
                    )
                    for name in self._avail_resources.custom_resources
                    if not name.startswith(NODE_ID_PREFIX)
                    and (total_resources.get(name, 0.0) > 0 or "_group_" not in name)
                ]
            )
            if customs:
                status += " ({})".format(customs)
            return status
        else:
            return "Resources requested: ?"

    def on_step_begin(self, trials: List[Trial]) -> None:
        """Before step() is called, update the available resources."""
        self._update_avail_resources()
        self._trial_just_finished_before = self._trial_just_finished
        self._trial_just_finished = False

    def on_step_end(self, trials: List[Trial]) -> None:
        self._just_staged_trials.clear()

        if time.time() > self.last_pg_recon + self.pg_recon_interval:
            # Only do this every now and then - usually the placement groups
            # should not get out of sync, and calling this often is inefficient
            self._pg_manager.reconcile_placement_groups(trials)
            self.last_pg_recon = time.time()

        self._pg_manager.cleanup()

    def force_reconcilation_on_next_step_end(self) -> None:
        self.last_pg_recon = -float("inf")

    def save(
        self, trial, storage=Checkpoint.PERSISTENT, result: Optional[Dict] = None
    ) -> Checkpoint:
        """Saves the trial's state to a checkpoint asynchronously.

        Args:
            trial (Trial): The trial to be saved.
            storage (str): Where to store the checkpoint. Defaults to
                PERSISTENT.
            result (dict): The state of this trial as a dictionary to be saved.
                If result is None, the trial's last result will be used.

        Returns:
             Checkpoint object, or None if an Exception occurs.
        """
        result = result or trial.last_result
        with self._change_working_directory(trial):
            if storage == Checkpoint.MEMORY:
                value = trial.runner.save_to_object.remote()
                checkpoint = Checkpoint(storage, value, result)
                trial.on_checkpoint(checkpoint)
            else:
                value = trial.runner.save.remote()
                checkpoint = Checkpoint(storage, value, result)
                trial.saving_to = checkpoint
                self._running[value] = trial
        return checkpoint

    def restore(self, trial) -> None:
        """Restores training state from a given model checkpoint.

        Args:
            trial (Trial): The trial to be restored.

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
        if checkpoint.storage == Checkpoint.MEMORY:
            logger.debug("Trial %s: Attempting restore from object", trial)
            # Note that we don't store the remote since in-memory checkpoints
            # don't guarantee fault tolerance and don't need to be waited on.
            with self._change_working_directory(trial):
                trial.runner.restore_from_object.remote(value)
        else:
            logger.debug("Trial %s: Attempting restore from %s", trial, value)
            if trial.uses_cloud_checkpointing or not trial.sync_on_checkpoint:
                with self._change_working_directory(trial):
                    remote = trial.runner.restore.remote(value)
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

            self._running[remote] = trial
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
        if self._resources_initialized:
            self._update_avail_resources()
            return self._avail_resources.gpu > 0

    def cleanup(self, trials: List[Trial]) -> None:
        self._trial_cleanup.cleanup(partial=False)
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


def _to_gb(n_bytes):
    return round(n_bytes / (1024 ** 3), 2)
