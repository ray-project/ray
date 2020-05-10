# coding: utf-8
import copy
import logging
import os
import random
import time
import traceback
from contextlib import contextmanager

import ray
from ray.exceptions import RayTimeoutError
from ray import ray_constants
from ray.resource_spec import ResourceSpec
from ray.tune.durable_trainable import DurableTrainable
from ray.tune.error import AbortTrialExecution, TuneError
from ray.tune.logger import NoopLogger
from ray.tune.result import TRIAL_INFO
from ray.tune.resources import Resources
from ray.tune.trainable import TrainableUtil
from ray.tune.trial import Trial, Checkpoint, Location, TrialInfo
from ray.tune.trial_executor import TrialExecutor
from ray.tune.utils import warn_if_slow

logger = logging.getLogger(__name__)

RESOURCE_REFRESH_PERIOD = 0.5  # Refresh resources every 500 ms
BOTTLENECK_WARN_PERIOD_S = 60
NONTRIVIAL_WAIT_TIME_THRESHOLD_S = 1e-3
DEFAULT_GET_TIMEOUT = 60.0  # seconds
TRIAL_CLEANUP_THRESHOLD = 100


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
    """

    def __init__(self, threshold=TRIAL_CLEANUP_THRESHOLD):
        self.threshold = threshold
        self._cleanup_map = {}

    def add(self, trial, actor):
        """Adds a trial actor to be stopped.

        If the number of futures exceeds the threshold, the cleanup mechanism
        will kick in.

        Args:
            trial (Trial): The trial corresponding to the future.
            actor (ActorHandle): Handle to the trainable to be stopped.
        """
        future = actor.stop.remote()
        actor.__ray_terminate__.remote()

        self._cleanup_map[future] = trial
        if len(self._cleanup_map) > self.threshold:
            self.cleanup(partial=True)

    def cleanup(self, partial=True):
        """Waits for cleanup to finish.

        If partial=False, all futures are expected to return. If a future
        does not return within the timeout period, the cleanup terminates.
        """
        logger.debug("Cleaning up futures")
        num_to_keep = int(self.threshold) / 2 if partial else 0
        while len(self._cleanup_map) > num_to_keep:
            dones, _ = ray.wait(
                list(self._cleanup_map), timeout=DEFAULT_GET_TIMEOUT)
            if not dones:
                logger.warning(
                    "Skipping cleanup - trainable.stop did not return in "
                    "time. Consider making `stop` a faster operation.")
            else:
                done = dones[0]
                del self._cleanup_map[done]


class RayTrialExecutor(TrialExecutor):
    """An implementation of TrialExecutor based on Ray."""

    def __init__(self,
                 queue_trials=False,
                 reuse_actors=False,
                 ray_auto_init=False,
                 refresh_period=RESOURCE_REFRESH_PERIOD):
        super(RayTrialExecutor, self).__init__(queue_trials)
        # Check for if we are launching a trial without resources in kick off
        # autoscaler.
        self._trial_queued = False
        self._running = {}
        # Since trial resume after paused should not run
        # trial.train.remote(), thus no more new remote object id generated.
        # We use self._paused to store paused trials here.
        self._paused = {}

        self._trial_cleanup = _TrialCleanup()
        self._reuse_actors = reuse_actors
        self._cached_actor = None

        self._avail_resources = Resources(cpu=0, gpu=0)
        self._committed_resources = Resources(cpu=0, gpu=0)
        self._resources_initialized = False
        self._refresh_period = refresh_period
        self._last_resource_refresh = float("-inf")
        self._last_nontrivial_wait = time.time()
        if not ray.is_initialized() and ray_auto_init:
            logger.info("Initializing Ray automatically."
                        "For cluster usage or custom Ray initialization, "
                        "call `ray.init(...)` before `tune.run`.")
            ray.init()

        if ray.is_initialized():
            self._update_avail_resources()

    def _setup_remote_runner(self, trial, reuse_allowed):
        trial.init_logger()
        # We checkpoint metadata here to try mitigating logdir duplication
        self.try_checkpoint_metadata(trial)
        remote_logdir = trial.logdir

        if (self._reuse_actors and reuse_allowed
                and self._cached_actor is not None):
            logger.debug("Trial %s: Reusing cached runner %s", trial,
                         self._cached_actor)
            existing_runner = self._cached_actor
            self._cached_actor = None
            trial.set_runner(existing_runner)
            if not self.reset_trial(trial, trial.config, trial.experiment_tag):
                raise AbortTrialExecution(
                    "Trainable runner reuse requires reset_config() to be "
                    "implemented and return True.")
            return existing_runner

        if self._cached_actor:
            logger.debug("Cannot reuse cached runner {} for new trial".format(
                self._cached_actor))
            with self._change_working_directory(trial):
                self._trial_cleanup.add(trial, actor=self._cached_actor)
            self._cached_actor = None

        cls = ray.remote(
            num_cpus=trial.resources.cpu,
            num_gpus=trial.resources.gpu,
            memory=trial.resources.memory,
            object_store_memory=trial.resources.object_store_memory,
            resources=trial.resources.custom_resources)(
                trial.get_trainable_cls())

        def logger_creator(config):
            # Set the working dir in the remote process, for user file writes
            os.makedirs(remote_logdir, exist_ok=True)
            if not ray.worker._mode() == ray.worker.LOCAL_MODE:
                os.chdir(remote_logdir)
            return NoopLogger(config, remote_logdir)

        # Clear the Trial's location (to be updated later on result)
        # since we don't know where the remote runner is placed.
        trial.set_location(Location())
        logger.debug("Trial %s: Setting up new remote runner.", trial)
        # Logging for trials is handled centrally by TrialRunner, so
        # configure the remote runner to use a noop-logger.
        trial_config = copy.deepcopy(trial.config)
        trial_config[TRIAL_INFO] = TrialInfo(trial)
        kwargs = {
            "config": trial_config,
            "logger_creator": logger_creator,
        }
        if issubclass(trial.get_trainable_cls(), DurableTrainable):
            kwargs["remote_checkpoint_dir"] = trial.remote_checkpoint_dir

        with self._change_working_directory(trial):
            return cls.remote(**kwargs)

    def _train(self, trial):
        """Start one iteration of training and save remote id."""
        if self._find_item(self._paused, trial):
            raise TuneError(
                "Should not call `train` on PAUSED trial {}. "
                "This is an internal error - please file an issue "
                "on https://github.com/ray-project/ray/issues/.".format(
                    str(trial)))

        if self._find_item(self._running, trial):
            logging.debug(
                "Trial {} already has a queued future. Skipping this "
                "`train` call. This may occur if a trial has "
                "been unpaused within a scheduler callback.".format(
                    str(trial)))
            return

        assert trial.status == Trial.RUNNING, trial.status
        with self._change_working_directory(trial):
            remote = trial.runner.train.remote()

        # Local Mode
        if isinstance(remote, dict):
            remote = _LocalWrapper(remote)

        self._running[remote] = trial
        trial_item = self._find_item(self._running, trial)
        assert len(trial_item) < 2, trial_item

    def _start_trial(self, trial, checkpoint=None, runner=None, train=True):
        """Starts trial and restores last result if trial was paused.

        Args:
            trial (Trial): The trial to start.
            checkpoint (Optional[Checkpoint]): The checkpoint to restore from.
                If None, and no trial checkpoint exists, the trial is started
                from the beginning.
            runner (Trainable): The remote runner to use. This can be the
                cached actor. If None, a new runner is created.
            train (bool): Whether or not to start training.

        See `RayTrialExecutor.restore` for possible errors raised.
        """
        prior_status = trial.status
        if runner is None:
            reuse_allowed = checkpoint is not None or trial.has_checkpoint()
            runner = self._setup_remote_runner(trial, reuse_allowed)
        trial.set_runner(runner)
        self.restore(trial, checkpoint)
        self.set_status(trial, Trial.RUNNING)

        previous_run = self._find_item(self._paused, trial)
        if prior_status == Trial.PAUSED and previous_run:
            # If Trial was in flight when paused, self._paused stores result.
            self._paused.pop(previous_run[0])
            self._running[previous_run[0]] = trial
        elif train and not trial.is_restoring:
            self._train(trial)

    def _stop_trial(self, trial, error=False, error_msg=None,
                    stop_logger=True):
        """Stops this trial.

        Stops this trial, releasing all allocating resources. If stopping the
        trial fails, the run will be marked as terminated in error, but no
        exception will be thrown.

        Args:
            error (bool): Whether to mark this trial as terminated in error.
            error_msg (str): Optional error message.
            stop_logger (bool): Whether to shut down the trial logger.
        """
        self.set_status(trial, Trial.ERROR if error else Trial.TERMINATED)
        trial.set_location(Location())

        try:
            trial.write_error_log(error_msg)
            if hasattr(trial, "runner") and trial.runner:
                if (not error and self._reuse_actors
                        and self._cached_actor is None):
                    logger.debug("Reusing actor for %s", trial.runner)
                    self._cached_actor = trial.runner
                else:
                    logger.debug("Trial %s: Destroying actor.", trial)
                    with self._change_working_directory(trial):
                        self._trial_cleanup.add(trial, actor=trial.runner)
        except Exception:
            logger.exception("Trial %s: Error stopping runner.", trial)
            self.set_status(trial, Trial.ERROR)
        finally:
            trial.set_runner(None)
            if stop_logger:
                trial.close_logger()

    def start_trial(self, trial, checkpoint=None, train=True):
        """Starts the trial.

        Will not return resources if trial repeatedly fails on start.

        Args:
            trial (Trial): Trial to be started.
            checkpoint (Checkpoint): A Python object or path storing the state
                of trial.
            train (bool): Whether or not to start training.
        """
        self._commit_resources(trial.resources)
        try:
            self._start_trial(trial, checkpoint, train=train)
        except AbortTrialExecution:
            logger.exception("Trial %s: Error starting runner, aborting!",
                             trial)
            time.sleep(2)
            error_msg = traceback.format_exc()
            self._stop_trial(trial, error=True, error_msg=error_msg)
        except Exception:
            logger.exception("Trial %s: Unexpected error starting runner.",
                             trial)
            time.sleep(2)
            error_msg = traceback.format_exc()
            self._stop_trial(trial, error=True, error_msg=error_msg)
            # Note that we don't return the resources, since they may
            # have been lost. TODO(ujvl): is this the right thing to do?

    def _find_item(self, dictionary, item):
        out = [rid for rid, t in dictionary.items() if t is item]
        return out

    def stop_trial(self, trial, error=False, error_msg=None, stop_logger=True):
        """Only returns resources if resources allocated."""
        prior_status = trial.status
        self._stop_trial(
            trial, error=error, error_msg=error_msg, stop_logger=stop_logger)
        if prior_status == Trial.RUNNING:
            logger.debug("Trial %s: Returning resources.", trial)
            self._return_resources(trial.resources)
            out = self._find_item(self._running, trial)
            for result_id in out:
                self._running.pop(result_id)

    def continue_training(self, trial):
        """Continues the training of this trial."""
        self._train(trial)

    def pause_trial(self, trial):
        """Pauses the trial.

        If trial is in-flight, preserves return value in separate queue
        before pausing, which is restored when Trial is resumed.
        """
        trial_future = self._find_item(self._running, trial)
        if trial_future:
            self._paused[trial_future[0]] = trial
        super(RayTrialExecutor, self).pause_trial(trial)

    def reset_trial(self, trial, new_config, new_experiment_tag):
        """Tries to invoke `Trainable.reset_config()` to reset trial.

        Args:
            trial (Trial): Trial to be reset.
            new_config (dict): New configuration for Trial trainable.
            new_experiment_tag (str): New experiment name for trial.

        Returns:
            True if `reset_config` is successful else False.
        """
        trial.experiment_tag = new_experiment_tag
        trial.config = new_config
        trainable = trial.runner
        with self._change_working_directory(trial):
            with warn_if_slow("reset_config"):
                try:
                    reset_val = ray.get(
                        trainable.reset_config.remote(new_config),
                        DEFAULT_GET_TIMEOUT)
                except RayTimeoutError:
                    logger.exception("Trial %s: reset_config timed out.",
                                     trial)
                    return False
        return reset_val

    def get_running_trials(self):
        """Returns the running trials."""
        return list(self._running.values())

    def get_alive_node_ips(self):
        nodes = ray.state.nodes()
        ip_addresses = set()
        for node in nodes:
            if node["alive"]:
                ip_addresses.add(node["NodeManagerAddress"])
        return ip_addresses

    def get_current_trial_ips(self):
        return {t.node_ip for t in self.get_running_trials()}

    def get_next_failed_trial(self):
        """Gets the first trial found to be running on a node presumed dead.

        Returns:
            A Trial object that is ready for failure processing. None if
            no failure detected.
        """
        if ray.worker._mode() != ray.worker.LOCAL_MODE:
            live_cluster_ips = self.get_alive_node_ips()
            if live_cluster_ips - self.get_current_trial_ips():
                for trial in self.get_running_trials():
                    if trial.node_ip and trial.node_ip not in live_cluster_ips:
                        return trial
        return None

    def get_next_available_trial(self):
        shuffled_results = list(self._running.keys())
        random.shuffle(shuffled_results)
        # Note: We shuffle the results because `ray.wait` by default returns
        # the first available result, and we want to guarantee that slower
        # trials (i.e. trials that run remotely) also get fairly reported.
        # See https://github.com/ray-project/ray/issues/4211 for details.
        start = time.time()
        [result_id], _ = ray.wait(shuffled_results)
        wait_time = time.time() - start
        if wait_time > NONTRIVIAL_WAIT_TIME_THRESHOLD_S:
            self._last_nontrivial_wait = time.time()
        if time.time() - self._last_nontrivial_wait > BOTTLENECK_WARN_PERIOD_S:
            logger.warning(
                "Over the last {} seconds, the Tune event loop has been "
                "backlogged processing new results. Consider increasing your "
                "period of result reporting to improve performance.".format(
                    BOTTLENECK_WARN_PERIOD_S))

            self._last_nontrivial_wait = time.time()
        return self._running[result_id]

    def fetch_result(self, trial):
        """Fetches one result of the running trials.

        Returns:
            Result of the most recent trial training run.
        """
        trial_future = self._find_item(self._running, trial)
        if not trial_future:
            raise ValueError("Trial was not running.")
        self._running.pop(trial_future[0])
        with warn_if_slow("fetch_result"):
            result = ray.get(trial_future[0], DEFAULT_GET_TIMEOUT)

        # For local mode
        if isinstance(result, _LocalWrapper):
            result = result.unwrap()
        return result

    def _commit_resources(self, resources):
        committed = self._committed_resources
        all_keys = set(resources.custom_resources).union(
            set(committed.custom_resources))

        custom_resources = {
            k: committed.get(k) + resources.get_res_total(k)
            for k in all_keys
        }

        self._committed_resources = Resources(
            committed.cpu + resources.cpu_total(),
            committed.gpu + resources.gpu_total(),
            committed.memory + resources.memory_total(),
            committed.object_store_memory +
            resources.object_store_memory_total(),
            custom_resources=custom_resources)

    def _return_resources(self, resources):
        committed = self._committed_resources

        all_keys = set(resources.custom_resources).union(
            set(committed.custom_resources))

        custom_resources = {
            k: committed.get(k) - resources.get_res_total(k)
            for k in all_keys
        }
        self._committed_resources = Resources(
            committed.cpu - resources.cpu_total(),
            committed.gpu - resources.gpu_total(),
            custom_resources=custom_resources)

        assert self._committed_resources.is_nonnegative(), (
            "Resource invalid: {}".format(resources))

    def _update_avail_resources(self, num_retries=5):
        resources = None
        for i in range(num_retries):
            if i > 0:
                logger.warning(
                    "Cluster resources not detected or are 0. Attempt #"
                    "%s...", i + 1)
                time.sleep(0.5)
            try:
                resources = ray.cluster_resources()
            except Exception:
                # TODO(rliaw): Remove this when local mode is fixed.
                # https://github.com/ray-project/ray/issues/4147
                logger.debug("Using resources for local machine.")
                resources = ResourceSpec().resolve(True).to_resource_dict()
            if resources:
                break

        if not resources:
            # NOTE: This hides the possibility that Ray may be waiting for
            # clients to connect.
            resources.setdefault("CPU", 0)
            resources.setdefault("GPU", 0)
            logger.warning("Cluster resources cannot be detected or are 0. "
                           "You can resume this experiment by passing in "
                           "`resume=True` to `run`.")

        resources = resources.copy()
        num_cpus = resources.pop("CPU", 0)
        num_gpus = resources.pop("GPU", 0)
        memory = ray_constants.from_memory_units(resources.pop("memory", 0))
        object_store_memory = ray_constants.from_memory_units(
            resources.pop("object_store_memory", 0))
        custom_resources = resources

        self._avail_resources = Resources(
            int(num_cpus),
            int(num_gpus),
            memory=int(memory),
            object_store_memory=int(object_store_memory),
            custom_resources=custom_resources)
        self._last_resource_refresh = time.time()
        self._resources_initialized = True

    def has_resources(self, resources):
        """Returns whether this runner has at least the specified resources.

        This refreshes the Ray cluster resources if the time since last update
        has exceeded self._refresh_period. This also assumes that the
        cluster is not resizing very frequently.
        """
        if time.time() - self._last_resource_refresh > self._refresh_period:
            self._update_avail_resources()

        currently_available = Resources.subtract(self._avail_resources,
                                                 self._committed_resources)

        have_space = (
            resources.cpu_total() <= currently_available.cpu
            and resources.gpu_total() <= currently_available.gpu
            and resources.memory_total() <= currently_available.memory
            and resources.object_store_memory_total() <=
            currently_available.object_store_memory and all(
                resources.get_res_total(res) <= currently_available.get(res)
                for res in resources.custom_resources))

        if have_space:
            # The assumption right now is that we block all trials if one
            # trial is queued.
            self._trial_queued = False
            return True

        can_overcommit = self._queue_trials and not self._trial_queued
        if can_overcommit:
            self._trial_queued = True
            logger.warning(
                "Allowing trial to start even though the "
                "cluster does not have enough free resources. Trial actors "
                "may appear to hang until enough resources are added to the "
                "cluster (e.g., via autoscaling). You can disable this "
                "behavior by specifying `queue_trials=False` in "
                "ray.tune.run().")
            return True

        return False

    def debug_string(self):
        """Returns a human readable message for printing to the console."""
        if self._resources_initialized:
            status = ("Resources requested: {}/{} CPUs, {}/{} GPUs, "
                      "{}/{} GiB heap, {}/{} GiB objects".format(
                          self._committed_resources.cpu,
                          self._avail_resources.cpu,
                          self._committed_resources.gpu,
                          self._avail_resources.gpu,
                          _to_gb(self._committed_resources.memory),
                          _to_gb(self._avail_resources.memory),
                          _to_gb(
                              self._committed_resources.object_store_memory),
                          _to_gb(self._avail_resources.object_store_memory)))
            customs = ", ".join([
                "{}/{} {}".format(
                    self._committed_resources.get_res_total(name),
                    self._avail_resources.get_res_total(name), name)
                for name in self._avail_resources.custom_resources
                if not name.startswith(ray.resource_spec.NODE_ID_PREFIX)
            ])
            if customs:
                status += " ({})".format(customs)
            return status
        else:
            return "Resources requested: ?"

    def resource_string(self):
        """Returns a string describing the total resources available."""
        if self._resources_initialized:
            res_str = ("{} CPUs, {} GPUs, "
                       "{} GiB heap, {} GiB objects".format(
                           self._avail_resources.cpu,
                           self._avail_resources.gpu,
                           _to_gb(self._avail_resources.memory),
                           _to_gb(self._avail_resources.object_store_memory)))
            if self._avail_resources.custom_resources:
                custom = ", ".join(
                    "{} {}".format(
                        self._avail_resources.get_res_total(name), name)
                    for name in self._avail_resources.custom_resources)
                res_str += " ({})".format(custom)
            return res_str
        else:
            return "? CPUs, ? GPUs"

    def on_step_begin(self, trial_runner):
        """Before step() called, update the available resources."""
        self._update_avail_resources()

    def save(self, trial, storage=Checkpoint.PERSISTENT, result=None):
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

    def restore(self, trial, checkpoint=None, block=False):
        """Restores training state from a given model checkpoint.

        Args:
            trial (Trial): The trial to be restored.
            checkpoint (Checkpoint): The checkpoint to restore from. If None,
                the most recent PERSISTENT checkpoint is used. Defaults to
                None.
            block (bool): Whether or not to block on restore before returning.

        Raises:
            RuntimeError: This error is raised if no runner is found.
            AbortTrialExecution: This error is raised if the trial is
                ineligible for restoration, given the Tune input arguments.
        """
        if checkpoint is None or checkpoint.value is None:
            checkpoint = trial.checkpoint
        if checkpoint.value is None:
            return
        if trial.runner is None:
            raise RuntimeError(
                "Trial {}: Unable to restore - no runner found.".format(trial))
        value = checkpoint.value
        if checkpoint.storage == Checkpoint.MEMORY:
            logger.debug("Trial %s: Attempting restore from object", trial)
            # Note that we don't store the remote since in-memory checkpoints
            # don't guarantee fault tolerance and don't need to be waited on.
            with self._change_working_directory(trial):
                trial.runner.restore_from_object.remote(value)
        else:
            logger.debug("Trial %s: Attempting restore from %s", trial, value)
            if issubclass(trial.get_trainable_cls(), DurableTrainable):
                with self._change_working_directory(trial):
                    remote = trial.runner.restore.remote(value)
            elif trial.sync_on_checkpoint:
                # This provides FT backwards compatibility in the
                # case where a DurableTrainable is not provided.
                logger.warning("Trial %s: Reading checkpoint into memory.",
                               trial)
                data_dict = TrainableUtil.pickle_checkpoint(value)
                with self._change_working_directory(trial):
                    remote = trial.runner.restore_from_object.remote(data_dict)
            else:
                raise AbortTrialExecution(
                    "Pass in `sync_on_checkpoint=True` for driver-based trial"
                    "restoration. Pass in an `upload_dir` and a Trainable "
                    "extending `DurableTrainable` for remote storage-based "
                    "restoration")

            if block:
                ray.get(remote)
            else:
                self._running[remote] = trial
                trial.restoring_from = checkpoint

    def export_trial_if_needed(self, trial):
        """Exports model of this trial based on trial.export_formats.

        Return:
            A dict that maps ExportFormats to successfully exported models.
        """
        if trial.export_formats and len(trial.export_formats) > 0:
            with self._change_working_directory(trial):
                return ray.get(
                    trial.runner.export_model.remote(trial.export_formats),
                    DEFAULT_GET_TIMEOUT)
        return {}

    def has_gpus(self):
        if self._resources_initialized:
            self._update_avail_resources()
            return self._avail_resources.gpu > 0

    def cleanup(self):
        self._trial_cleanup.cleanup(partial=False)

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
    return round(n_bytes / (1024**3), 2)
