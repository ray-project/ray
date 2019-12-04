# coding: utf-8
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import os
import random
import time
import traceback

import ray
from ray.exceptions import RayTimeoutError
from ray import ray_constants
from ray.resource_spec import ResourceSpec
from ray.tune.error import AbortTrialExecution
from ray.tune.logger import NoopLogger
from ray.tune.trial import Trial, Checkpoint, Location
from ray.tune.resources import Resources
from ray.tune.trial_executor import TrialExecutor
from ray.tune.util import warn_if_slow

logger = logging.getLogger(__name__)

RESOURCE_REFRESH_PERIOD = 0.5  # Refresh resources every 500 ms
BOTTLENECK_WARN_PERIOD_S = 60
NONTRIVIAL_WAIT_TIME_THRESHOLD_S = 1e-3
DEFAULT_GET_TIMEOUT = 30.0  # seconds
TRIAL_START_ATTEMPTS = 3


class _LocalWrapper(object):
    def __init__(self, result):
        self._result = result

    def unwrap(self):
        """Returns the wrapped result."""
        return self._result


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
            trial.runner = existing_runner
            if not self.reset_trial(trial, trial.config, trial.experiment_tag):
                raise AbortTrialExecution(
                    "Trainable runner reuse requires reset_config() to be "
                    "implemented and return True.")
            return existing_runner

        if self._cached_actor:
            logger.debug("Cannot reuse cached runner {} for new trial".format(
                self._cached_actor))
            self._cached_actor.stop.remote()
            self._cached_actor.__ray_terminate__.remote()
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
            if not os.path.exists(remote_logdir):
                os.makedirs(remote_logdir)
            if not ray.worker._mode() == ray.worker.LOCAL_MODE:
                os.chdir(remote_logdir)
            return NoopLogger(config, remote_logdir)

        # Clear the Trial's location (to be updated later on result)
        # since we don't know where the remote runner is placed.
        trial.set_location(Location())
        logger.info("Trial %s: Setting up new remote runner.", trial)
        # Logging for trials is handled centrally by TrialRunner, so
        # configure the remote runner to use a noop-logger.
        return cls.remote(config=trial.config, logger_creator=logger_creator)

    def _train(self, trial):
        """Start one iteration of training and save remote id."""

        assert trial.status == Trial.RUNNING, trial.status
        remote = trial.runner.train.remote()

        # Local Mode
        if isinstance(remote, dict):
            remote = _LocalWrapper(remote)

        self._running[remote] = trial

    def _start_trial(self, trial, checkpoint=None, runner=None):
        """Starts trial and restores last result if trial was paused.

        Args:
            trial (Trial): The trial to start.
            checkpoint (Optional[Checkpoint]): The checkpoint to restore from.
                If None, and no trial checkpoint exists, the trial is started
                from the beginning.
            runner (Trainable): The remote runner to use. This can be the
                cached actor. If None, a new runner is created.

        See `RayTrialExecutor.restore` for possible errors raised.
        """
        prior_status = trial.status
        self.set_status(trial, Trial.RUNNING)
        trial.runner = runner or self._setup_remote_runner(
            trial,
            reuse_allowed=checkpoint is not None or trial.has_checkpoint())
        self.restore(trial, checkpoint)

        previous_run = self._find_item(self._paused, trial)
        if prior_status == Trial.PAUSED and previous_run:
            # If Trial was in flight when paused, self._paused stores result.
            self._paused.pop(previous_run[0])
            self._running[previous_run[0]] = trial
        else:
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

        if stop_logger:
            trial.close_logger()

        self.set_status(trial, Trial.ERROR if error else Trial.TERMINATED)
        trial.set_location(Location())

        try:
            trial.write_error_log(error_msg)
            if hasattr(trial, "runner") and trial.runner:
                if (not error and self._reuse_actors
                        and self._cached_actor is None):
                    logger.debug("Reusing actor for {}".format(trial.runner))
                    self._cached_actor = trial.runner
                else:
                    logger.debug("Trial %s: Destroying actor.", trial)
                    trial.runner.stop.remote()
                    trial.runner.__ray_terminate__.remote()
        except Exception:
            logger.exception("Trial %s: Error stopping runner.", trial)
            self.set_status(trial, Trial.ERROR)
        finally:
            trial.runner = None

    def start_trial(self, trial, checkpoint=None):
        """Starts the trial.

        Will not return resources if trial repeatedly fails on start.

        Args:
            trial (Trial): Trial to be started.
            checkpoint (Checkpoint): A Python object or path storing the state
                of trial.
        """
        self._commit_resources(trial.resources)
        remote_runner = None
        attempts = 0
        while attempts < TRIAL_START_ATTEMPTS:
            attempts += 1
            if attempts > 1:
                logger.warning("Trial %s: Start attempt #%s...", trial,
                               attempts)
            try:
                self._start_trial(trial, checkpoint, remote_runner)
                break
            except AbortTrialExecution:
                logger.exception("Trial %s: Error starting runner, aborting!",
                                 trial)
                time.sleep(2)
                error_msg = traceback.format_exc()
                self._stop_trial(trial, error=True, error_msg=error_msg)
                break  # don't retry fatal Tune errors
            except RayTimeoutError:
                # Reuse the existing runner on retries.
                remote_runner = trial.runner
                warning = ("Runner task timed out. This could be due to "
                           "slow worker startup.")
                if attempts == TRIAL_START_ATTEMPTS:
                    error_msg = traceback.format_exc()
                    self._stop_trial(trial, error=True, error_msg=error_msg)
                else:
                    warning += " Reusing the same runner."
                logger.warning("Trial %s: %s", trial, warning)
            except Exception:
                logger.exception("Trial %s: Error starting runner.", trial)
                time.sleep(2)
                error_msg = traceback.format_exc()
                self._stop_trial(trial, error=True, error_msg=error_msg)
                remote_runner = None
                # This forces the trial to not start from checkpoint.
                checkpoint = None
                trial.clear_checkpoint()
                # Note that we don't return the resources, since they may
                # have been lost. TODO(ujvl): is this the right thing to do?
        else:
            logger.exception(
                "Trial %s: Aborting trial after %s start "
                "attempts!", trial, TRIAL_START_ATTEMPTS)

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
            new_config (dict): New configuration for Trial
                trainable.
            new_experiment_tag (str): New experiment name
                for trial.

        Returns:
            True if `reset_config` is successful else False.
        """
        trial.experiment_tag = new_experiment_tag
        trial.config = new_config
        trainable = trial.runner
        with warn_if_slow("reset_config"):
            try:
                reset_val = ray.get(
                    trainable.reset_config.remote(new_config),
                    DEFAULT_GET_TIMEOUT)
            except RayTimeoutError:
                logger.exception("Trial %s: reset_config timed out.")
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
            Result of the most recent trial training run."""
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
        for i in range(num_retries):
            try:
                resources = ray.cluster_resources()
            except Exception:
                # TODO(rliaw): Remove this when local mode is fixed.
                # https://github.com/ray-project/ray/issues/4147
                logger.debug("Using resources for local machine.")
                resources = ResourceSpec().resolve(True).to_resource_dict()
            if not resources:
                logger.warning(
                    "Cluster resources not detected or are 0. Retrying...")
                time.sleep(0.5)

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

    def save(self, trial, storage=Checkpoint.DISK, result=None):
        """Saves the trial's state to a checkpoint."""
        result = result or trial.last_result

        if storage == Checkpoint.MEMORY:
            value = trial.runner.save_to_object.remote()
            checkpoint = Checkpoint(storage, value, result)
        else:
            with warn_if_slow("save_checkpoint_to_disk"):
                value = ray.get(trial.runner.save.remote())
                checkpoint = Checkpoint(storage, value, result)

        with warn_if_slow("on_checkpoint", DEFAULT_GET_TIMEOUT) as profile:
            try:
                trial.on_checkpoint(checkpoint)
            except Exception:
                logger.exception("Trial %s: Error handling checkpoint %s",
                                 trial, checkpoint.value)
                return None
        if profile.too_slow and trial.sync_on_checkpoint:
            logger.warning(
                "Consider turning off forced head-worker trial checkpoint "
                "syncs by setting sync_on_checkpoint=False. Note that this "
                "might result in faulty trial restoration for some worker "
                "failure modes.")
        return checkpoint.value

    def restore(self, trial, checkpoint=None):
        """Restores training state from a given model checkpoint.

        This will also sync the trial results to a new location
        if restoring on a different node.

        Raises:
            RuntimeError: This error is raised if no runner is found.
            RayTimeoutError: This error is raised if a remote call to the
                runner times out.
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
            assert not isinstance(value, Checkpoint), type(value)
            trial.runner.restore_from_object.remote(value)
        else:
            logger.info("Trial %s: Attempting restore from %s", trial, value)
            with warn_if_slow("get_current_ip"):
                worker_ip = ray.get(trial.runner.current_ip.remote(),
                                    DEFAULT_GET_TIMEOUT)
            with warn_if_slow("sync_to_new_location"):
                trial.sync_logger_to_new_location(worker_ip)
            with warn_if_slow("restore_from_disk"):
                # TODO(ujvl): Take blocking restores out of the control loop.
                ray.get(trial.runner.restore.remote(value))
        trial.last_result = checkpoint.result

    def export_trial_if_needed(self, trial):
        """Exports model of this trial based on trial.export_formats.

        Return:
            A dict that maps ExportFormats to successfully exported models.
        """
        if trial.export_formats and len(trial.export_formats) > 0:
            return ray.get(
                trial.runner.export_model.remote(trial.export_formats),
                DEFAULT_GET_TIMEOUT)
        return {}

    def has_gpus(self):
        if self._resources_initialized:
            self._update_avail_resources()
            return self._avail_resources.gpu > 0


def _to_gb(n_bytes):
    return round(n_bytes / (1024**3), 2)
