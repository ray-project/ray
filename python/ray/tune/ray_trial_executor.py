# coding: utf-8
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import os
import time
import traceback

import ray
from ray.tune.logger import NoopLogger
from ray.tune.trial import Trial, Resources, Checkpoint
from ray.tune.trial_executor import TrialExecutor

logger = logging.getLogger(__name__)


class RayTrialExecutor(TrialExecutor):
    """An implemention of TrialExecutor based on Ray."""

    def __init__(self, queue_trials=False):
        super(RayTrialExecutor, self).__init__(queue_trials)
        self._running = {}
        # Since trial resume after paused should not run
        # trial.train.remote(), thus no more new remote object id generated.
        # We use self._paused to store paused trials here.
        self._paused = {}
        self._avail_resources = Resources(cpu=0, gpu=0)
        self._committed_resources = Resources(cpu=0, gpu=0)
        self._resources_initialized = False
        if ray.is_initialized():
            self._update_avail_resources()

    def _setup_runner(self, trial):
        cls = ray.remote(
            num_cpus=trial.resources.cpu,
            num_gpus=trial.resources.gpu)(trial._get_trainable_cls())

        trial.init_logger()
        remote_logdir = trial.logdir

        def logger_creator(config):
            # Set the working dir in the remote process, for user file writes
            if not os.path.exists(remote_logdir):
                os.makedirs(remote_logdir)
            os.chdir(remote_logdir)
            return NoopLogger(config, remote_logdir)

        # Logging for trials is handled centrally by TrialRunner, so
        # configure the remote runner to use a noop-logger.
        return cls.remote(config=trial.config, logger_creator=logger_creator)

    def _train(self, trial):
        """Start one iteration of training and save remote id."""

        assert trial.status == Trial.RUNNING, trial.status
        remote = trial.runner.train.remote()
        self._running[remote] = trial

    def _start_trial(self, trial, checkpoint=None):
        prior_status = trial.status
        trial.status = Trial.RUNNING
        trial.runner = self._setup_runner(trial)
        if not self.restore(trial, checkpoint):
            return

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

        if error:
            trial.status = Trial.ERROR
        else:
            trial.status = Trial.TERMINATED

        try:
            trial.write_error_log(error_msg)
            if hasattr(trial, 'runner') and trial.runner:
                stop_tasks = []
                stop_tasks.append(trial.runner.stop.remote())
                stop_tasks.append(trial.runner.__ray_terminate__.remote())
                # TODO(ekl)  seems like wait hangs when killing actors
                _, unfinished = ray.wait(
                    stop_tasks, num_returns=2, timeout=250)
        except Exception:
            logger.exception("Error stopping runner.")
            trial.status = Trial.ERROR
        finally:
            trial.runner = None

        if stop_logger:
            trial.close_logger()

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
        except Exception:
            logger.exception("Error starting runner. "
                             "Trying again without checkpoint.")
            error_msg = traceback.format_exc()
            time.sleep(2)
            self._stop_trial(trial, error=True, error_msg=error_msg)
            try:
                # This forces the trial to not start from checkpoint.
                self._start_trial(trial, checkpoint=False)
            except Exception:
                logger.exception("Error starting runner, aborting!")
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
            logger.debug("Returning resources for this trial.")
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
        reset_val = ray.get(trainable.reset_config.remote(new_config))
        return reset_val

    def get_running_trials(self):
        """Returns the running trials."""

        return list(self._running.values())

    def get_next_available_trial(self):
        [result_id], _ = ray.wait(list(self._running))
        return self._running[result_id]

    def fetch_result(self, trial):
        """Fetches one result of the running trials.

        Returns:
            Result of the most recent trial training run."""
        trial_future = self._find_item(self._running, trial)
        if not trial_future:
            raise ValueError("Trial was not running.")
        self._running.pop(trial_future[0])
        result = ray.get(trial_future[0])
        return result

    def _commit_resources(self, resources):
        self._committed_resources = Resources(
            self._committed_resources.cpu + resources.cpu_total(),
            self._committed_resources.gpu + resources.gpu_total())

    def _return_resources(self, resources):
        self._committed_resources = Resources(
            self._committed_resources.cpu - resources.cpu_total(),
            self._committed_resources.gpu - resources.gpu_total())
        assert self._committed_resources.cpu >= 0
        assert self._committed_resources.gpu >= 0

    def _update_avail_resources(self):
        resources = ray.global_state.cluster_resources()
        num_cpus = resources["CPU"]
        num_gpus = resources["GPU"]
        self._avail_resources = Resources(int(num_cpus), int(num_gpus))
        self._resources_initialized = True

    def has_resources(self, resources):
        """Returns whether this runner has at least the specified resources."""

        cpu_avail = self._avail_resources.cpu - self._committed_resources.cpu
        gpu_avail = self._avail_resources.gpu - self._committed_resources.gpu

        have_space = (resources.cpu_total() <= cpu_avail
                      and resources.gpu_total() <= gpu_avail)

        if have_space:
            return True

        can_overcommit = self._queue_trials

        if (resources.cpu_total() > 0 and cpu_avail <= 0) or \
           (resources.gpu_total() > 0 and gpu_avail <= 0):
            can_overcommit = False  # requested resource is already saturated

        if can_overcommit:
            logger.warning(
                "Allowing trial to start even though the "
                "cluster does not have enough free resources. Trial actors "
                "may appear to hang until enough resources are added to the "
                "cluster (e.g., via autoscaling). You can disable this "
                "behavior by specifying `queue_trials=False` in "
                "ray.tune.run_experiments().")
            return True

        return False

    def debug_string(self):
        """Returns a human readable message for printing to the console."""

        if self._resources_initialized:
            return "Resources requested: {}/{} CPUs, {}/{} GPUs".format(
                self._committed_resources.cpu, self._avail_resources.cpu,
                self._committed_resources.gpu, self._avail_resources.gpu)
        else:
            return "Resources requested: ?"

    def resource_string(self):
        """Returns a string describing the total resources available."""

        if self._resources_initialized:
            return "{} CPUs, {} GPUs".format(self._avail_resources.cpu,
                                             self._avail_resources.gpu)
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
            trial._checkpoint.value = ray.get(trial.runner.save.remote())
        return trial._checkpoint.value

    def restore(self, trial, checkpoint=None):
        """Restores training state from a given model checkpoint."""
        if checkpoint is None or checkpoint.value is None:
            checkpoint = trial._checkpoint
        if checkpoint is None or checkpoint.value is None:
            return True
        if trial.runner is None:
            logger.error("Unable to restore - no runner.")
            trial.status = Trial.ERROR
            return False
        try:
            value = checkpoint.value
            if checkpoint.storage == Checkpoint.MEMORY:
                assert type(value) != Checkpoint, type(value)
                ray.get(trial.runner.restore_from_object.remote(value))
            else:
                ray.get(trial.runner.restore.remote(value))
            trial.last_result = checkpoint.last_result

            return True
        except Exception:
            logger.exception("Error restoring runner.")
            trial.status = Trial.ERROR
            return False
