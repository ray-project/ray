# coding: utf-8
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import time
import traceback
import ray
from ray.tune.logger import NoopLogger
from ray.tune.trial import Trial, Resources, Checkpoint
from ray.tune.trial_executor import TrialExecutor


class RayTrialExecutor(TrialExecutor):
    """An implemention of TrialExecutor based on Ray."""

    def __init__(self, queue_trials=False):
        super(RayTrialExecutor, self).__init__(queue_trials)
        self._running = {}  # TODO
        # Since trial resume after paused should not run
        # trial.train.remote(), thus no more new remote object id generated.
        # We use self._paused to store paused trials here.
        self._paused = {}
        self._avail_resources = Resources(cpu=0, gpu=0)
        self._committed_resources = Resources(cpu=0, gpu=0)
        self._resources_initialized = False

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
        if prior_status == Trial.PAUSED:
            # If prev status is PAUSED, self._paused stores its remote_id.
            remote_id = self._find_item(self._paused, trial)[0]
            self._paused.pop(remote_id)
            self._running[remote_id] = trial
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
            print("Error stopping runner:", traceback.format_exc())
            trial.status = Trial.ERROR
        finally:
            trial.runner = None

        if stop_logger:
            trial.close_logger()

    def start_trial(self, trial, checkpoint_obj=None):
        """Starts the trial."""

        self._commit_resources(trial.resources)
        try:
            self._start_trial(trial, checkpoint_obj)
        except Exception:
            error_msg = traceback.format_exc()
            print("Error starting runner, retrying:", error_msg)
            time.sleep(2)
            self._stop_trial(trial, error=True, error_msg=error_msg)
            try:
                self._start_trial(trial)
            except Exception:
                error_msg = traceback.format_exc()
                print("Error starting runner, abort:", error_msg)
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
            self._return_resources(trial.resources)
            out = self._find_item(self._running, trial)
            for result_id in out:
                self._running.pop(result_id)

    def continue_training(self, trial):
        """Continues the training of this trial."""

        self._train(trial)

    def pause_trial(self, trial):
        """Pauses the trial."""

        remote_id = self._find_item(self._running, trial)[0]
        self._paused[remote_id] = trial
        super(RayTrialExecutor, self).pause_trial(trial)

    def get_running_trials(self):
        """Returns the running trials."""

        return list(self._running.values())

    def fetch_one_result(self):
        """Fetches one result of the running trials."""

        [result_id], _ = ray.wait(list(self._running))
        trial = self._running.pop(result_id)
        result = None
        try:
            result = ray.get(result_id)
        except Exception:
            print("fetch_one_result failed:", traceback.format_exc())

        return trial, result

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
        clients = ray.global_state.client_table()
        if ray.worker.global_worker.use_raylet:
            # TODO(rliaw): Remove once raylet flag is swapped
            num_cpus = sum(cl['Resources']['CPU'] for cl in clients)
            num_gpus = sum(cl['Resources'].get('GPU', 0) for cl in clients)
        else:
            local_schedulers = [
                entry for client in clients.values() for entry in client
                if (entry['ClientType'] == 'local_scheduler'
                    and not entry['Deleted'])
            ]
            num_cpus = sum(ls['CPU'] for ls in local_schedulers)
            num_gpus = sum(ls.get('GPU', 0) for ls in local_schedulers)
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
            print("WARNING:tune:allowing trial to start even though the "
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
            return ""

    def on_step_begin(self):
        """Before step() called, update the available resources."""

        self._update_avail_resources()

    def save(self, trial, storage=Checkpoint.DISK):
        """Saves the trial's state to a checkpoint."""
        trial._checkpoint.storage = storage
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
            print("Unable to restore - no runner")
            trial.status = Trial.ERROR
            return False
        try:
            value = checkpoint.value
            if checkpoint.storage == Checkpoint.MEMORY:
                assert type(value) != Checkpoint, type(value)
                ray.get(trial.runner.restore_from_object.remote(value))
            else:
                ray.get(trial.runner.restore.remote(value))
            return True
        except Exception:
            print("Error restoring runner:", traceback.format_exc())
            trial.status = Trial.ERROR
            return False
