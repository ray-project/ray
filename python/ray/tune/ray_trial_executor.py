# coding: utf-8
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import math
import os
import random
import time
import traceback

import ray
from ray.tune.error import AbortTrialExecution
from ray.tune.logger import NoopLogger
from ray.tune.trial import Trial, Checkpoint
from ray.tune.resources import Resources
from ray.tune.trial_executor import TrialExecutor
from ray.tune.util import warn_if_slow

logger = logging.getLogger(__name__)

RESOURCE_REFRESH_PERIOD = 0.5  # Refresh resources every 500 ms
BOTTLENECK_WARN_PERIOD_S = 60
NONTRIVIAL_WAIT_TIME_THRESHOLD_S = 1e-3


class _LocalWrapper(object):
    def __init__(self, result):
        self._result = result

    def unwrap(self):
        """Returns the wrapped result."""
        return self._result


class RayTrialExecutor(TrialExecutor):
    """An implemention of TrialExecutor based on Ray."""

    def __init__(self,
                 queue_trials=False,
                 reuse_actors=False,
                 ray_auto_init=False,
                 refresh_period=RESOURCE_REFRESH_PERIOD):
        super(RayTrialExecutor, self).__init__(queue_trials)
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
            ray.init(object_store_memory=int(1e8))

        if ray.is_initialized():
            self._update_avail_resources()

    def _setup_runner(self, trial, reuse_allowed):
        if (self._reuse_actors and reuse_allowed
                and self._cached_actor is not None):
            logger.debug("Reusing cached runner {} for {}".format(
                self._cached_actor, trial.trial_id))
            existing_runner = self._cached_actor
            self._cached_actor = None
        else:
            if self._cached_actor:
                logger.debug(
                    "Cannot reuse cached runner {} for new trial".format(
                        self._cached_actor))
                self._cached_actor.stop.remote()
                self._cached_actor.__ray_terminate__.remote()
                self._cached_actor = None
            existing_runner = None
            cls = ray.remote(
                num_cpus=trial.resources.cpu,
                num_gpus=trial.resources.gpu,
                resources=trial.resources.custom_resources)(
                    trial._get_trainable_cls())

        trial.init_logger()
        # We checkpoint metadata here to try mitigating logdir duplication
        self.try_checkpoint_metadata(trial)
        remote_logdir = trial.logdir

        if existing_runner:
            trial.runner = existing_runner
            if not self.reset_trial(trial, trial.config, trial.experiment_tag):
                raise AbortTrialExecution(
                    "Trial runner reuse requires reset_trial() to be "
                    "implemented and return True.")
            return existing_runner

        def logger_creator(config):
            # Set the working dir in the remote process, for user file writes
            if not os.path.exists(remote_logdir):
                os.makedirs(remote_logdir)
            if not ray.worker._mode() == ray.worker.LOCAL_MODE:
                os.chdir(remote_logdir)
            return NoopLogger(config, remote_logdir)

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

    def _start_trial(self, trial, checkpoint=None):
        """Starts trial and restores last result if trial was paused.

        Raises:
            ValueError if restoring from checkpoint fails.
        """
        prior_status = trial.status
        self.set_status(trial, Trial.RUNNING)
        trial.runner = self._setup_runner(
            trial,
            reuse_allowed=checkpoint is not None
            or trial._checkpoint.value is not None)
        if not self.restore(trial, checkpoint):
            if trial.status == Trial.ERROR:
                raise RuntimeError(
                    "Restore from checkpoint failed for Trial {}.".format(
                        str(trial)))

        previous_run = self._find_item(self._paused, trial)
        if (prior_status == Trial.PAUSED and previous_run):
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

        if error:
            self.set_status(trial, Trial.ERROR)
        else:
            self.set_status(trial, Trial.TERMINATED)

        try:
            trial.write_error_log(error_msg)
            if hasattr(trial, "runner") and trial.runner:
                if (not error and self._reuse_actors
                        and self._cached_actor is None):
                    logger.debug("Reusing actor for {}".format(trial.runner))
                    self._cached_actor = trial.runner
                else:
                    logger.debug(
                        "Destroying actor for trial {}.".format(trial))
                    trial.runner.stop.remote()
                    trial.runner.__ray_terminate__.remote()
        except Exception:
            logger.exception("Error stopping runner for Trial %s", str(trial))
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
        try:
            self._start_trial(trial, checkpoint)
        except Exception as e:
            logger.exception("Error starting runner for Trial %s", str(trial))
            error_msg = traceback.format_exc()
            time.sleep(2)
            self._stop_trial(trial, error=True, error_msg=error_msg)
            if isinstance(e, AbortTrialExecution):
                return  # don't retry fatal Tune errors
            try:
                # This forces the trial to not start from checkpoint.
                trial.clear_checkpoint()
                logger.info(
                    "Trying to start runner for Trial %s without checkpoint.",
                    str(trial))
                self._start_trial(trial)
            except Exception:
                logger.exception(
                    "Error starting runner for Trial %s, aborting!",
                    str(trial))
                error_msg = traceback.format_exc()
                self._stop_trial(trial, error=True, error_msg=error_msg)
                # note that we don't return the resources, since they may
                # have been lost

    def _find_item(self, dictionary, item):
        out = [rid for rid, t in dictionary.items() if t is item]
        return out

    def stop_trial(self, trial, error=False, error_msg=None, stop_logger=True):
        """Only returns resources if resources allocated."""
        prior_status = trial.status
        self._stop_trial(
            trial, error=error, error_msg=error_msg, stop_logger=stop_logger)
        if prior_status == Trial.RUNNING:
            logger.debug("Returning resources for Trial %s.", str(trial))
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
            reset_val = ray.get(trainable.reset_config.remote(new_config))
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

    def get_next_available_trial(self):
        if ray.worker._mode() != ray.worker.LOCAL_MODE:
            live_cluster_ips = self.get_alive_node_ips()
            if live_cluster_ips - self.get_current_trial_ips():
                for trial in self.get_running_trials():
                    if trial.node_ip and trial.node_ip not in live_cluster_ips:
                        logger.warning(
                            "{} (ip: {}) detected as stale. This is likely "
                            "because the node was lost. Processing this "
                            "trial first.".format(trial, trial.node_ip))
                        return trial

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
            logger.warn(
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
            result = ray.get(trial_future[0])

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
                resources = ray.services.check_and_update_resources(
                    None, None, None)
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
        custom_resources = resources

        self._avail_resources = Resources(
            int(num_cpus), int(num_gpus), custom_resources=custom_resources)
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
            and resources.gpu_total() <= currently_available.gpu and all(
                resources.get_res_total(res) <= currently_available.get(res)
                for res in resources.custom_resources))

        if have_space:
            return True

        can_overcommit = self._queue_trials

        if (resources.cpu_total() > 0 and currently_available.cpu <= 0) or \
           (resources.gpu_total() > 0 and currently_available.gpu <= 0) or \
           any((resources.get_res_total(res_name) > 0
                and currently_available.get(res_name) <= 0)
               for res_name in resources.custom_resources):
            can_overcommit = False  # requested resource is already saturated

        if can_overcommit:
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
            status = "Resources requested: {}/{} CPUs, {}/{} GPUs".format(
                self._committed_resources.cpu, self._avail_resources.cpu,
                self._committed_resources.gpu, self._avail_resources.gpu)
            customs = ", ".join([
                "{}/{} {}".format(
                    self._committed_resources.get_res_total(name),
                    self._avail_resources.get_res_total(name), name)
                for name in self._avail_resources.custom_resources
            ])
            if customs:
                status += " ({})".format(customs)
            return status
        else:
            return "Resources requested: ?"

    def resource_string(self):
        """Returns a string describing the total resources available."""

        if self._resources_initialized:
            res_str = "{} CPUs, {} GPUs".format(self._avail_resources.cpu,
                                                self._avail_resources.gpu)
            if self._avail_resources.custom_resources:
                custom = ", ".join(
                    "{} {}".format(
                        self._avail_resources.get_res_total(name), name)
                    for name in self._avail_resources.custom_resources)
                res_str += " ({})".format(custom)
            return res_str
        else:
            return "? CPUs, ? GPUs"

    def on_step_begin(self):
        """Before step() called, update the available resources."""
        self._update_avail_resources()

    def save(self, trial, storage=Checkpoint.DISK):
        """Saves the trial's state to a checkpoint."""
        trial._checkpoint.storage = storage
        trial._checkpoint.last_result = trial.last_result
        if storage == Checkpoint.MEMORY:
            trial._checkpoint.value = trial.runner.save_to_object.remote()
        else:
            # Keeps only highest performing checkpoints if enabled
            if trial.keep_checkpoints_num:
                try:
                    last_attr_val = trial.last_result[
                        trial.checkpoint_score_attr]
                    if (trial.compare_checkpoints(last_attr_val)
                            and not math.isnan(last_attr_val)):
                        trial.best_checkpoint_attr_value = last_attr_val
                        self._checkpoint_and_erase(trial)
                except KeyError:
                    logger.warning(
                        "Result dict has no key: {}. keep"
                        "_checkpoints_num flag will not work".format(
                            trial.checkpoint_score_attr))
            else:
                with warn_if_slow("save_to_disk"):
                    trial._checkpoint.value = ray.get(
                        trial.runner.save.remote())

        return trial._checkpoint.value

    def _checkpoint_and_erase(self, trial):
        """Checkpoints the model and erases old checkpoints
            if needed.
        Parameters
        ----------
            trial : trial to save
        """

        with warn_if_slow("save_to_disk"):
            trial._checkpoint.value = ray.get(trial.runner.save.remote())

        if len(trial.history) >= trial.keep_checkpoints_num:
            ray.get(trial.runner.delete_checkpoint.remote(trial.history[-1]))
            trial.history.pop()

        trial.history.insert(0, trial._checkpoint.value)

    def restore(self, trial, checkpoint=None):
        """Restores training state from a given model checkpoint.

        This will also sync the trial results to a new location
        if restoring on a different node.
        """
        if checkpoint is None or checkpoint.value is None:
            checkpoint = trial._checkpoint
        if checkpoint is None or checkpoint.value is None:
            return True
        if trial.runner is None:
            logger.error("Unable to restore - no runner.")
            self.set_status(trial, Trial.ERROR)
            return False
        try:
            value = checkpoint.value
            if checkpoint.storage == Checkpoint.MEMORY:
                assert type(value) != Checkpoint, type(value)
                trial.runner.restore_from_object.remote(value)
            else:
                # TODO: Somehow, the call to get the current IP on the
                # remote actor can be very slow - a better fix would
                # be to use an actor table to detect the IP of the Trainable
                # and rsync the files there.
                # See https://github.com/ray-project/ray/issues/5168
                with warn_if_slow("get_current_ip"):
                    worker_ip = ray.get(trial.runner.current_ip.remote())
                with warn_if_slow("sync_to_new_location"):
                    trial.sync_logger_to_new_location(worker_ip)
                with warn_if_slow("restore_from_disk"):
                    ray.get(trial.runner.restore.remote(value))
            trial.last_result = checkpoint.last_result
            return True
        except Exception:
            logger.exception("Error restoring runner for Trial %s.", trial)
            self.set_status(trial, Trial.ERROR)
            return False

    def export_trial_if_needed(self, trial):
        """Exports model of this trial based on trial.export_formats.

        Return:
            A dict that maps ExportFormats to successfully exported models.
        """
        if trial.export_formats and len(trial.export_formats) > 0:
            return ray.get(
                trial.runner.export_model.remote(trial.export_formats))
        return {}
