# coding: utf-8
import os
import tempfile
import traceback

import time

import ray
from ray.tune import TuneError
from ray.tune.logger import NoopLogger, UnifiedLogger, pretty_print
from ray.tune.result import DEFAULT_RESULTS_DIR

from ray.tune.trial import Trial, register_trial_impl, Resources, date_str
from ray.tune.trial_executor import TrialExecutor

MAX_LEN_IDENTIFIER = 130

class RayTrial(Trial):
    def __init__(self, trainable_name,
                 config=None,
                 trial_id=None,
                 local_dir=DEFAULT_RESULTS_DIR,
                 experiment_tag="",
                 resources=None,
                 stopping_criterion=None,
                 checkpoint_freq=0,
                 restore_path=None,
                 upload_dir=None,
                 max_failures=0):
        super(RayTrial, self).__init__(
            trainable_name=trainable_name, config=config, trial_id=trial_id,
            local_dir=local_dir, experiment_tag=experiment_tag, resources=resources,
            stopping_criterion=stopping_criterion, checkpoint_freq=checkpoint_freq,
            restore_path=restore_path, upload_dir=upload_dir, max_failures=max_failures
        )

    def start(self, checkpoint_obj=None):
        """Starts this trial.

        If an error is encountered when starting the trial, an exception will
        be thrown.

        Args:
            checkpoint_obj (obj): Optional checkpoint to resume from.
        """

        self._setup_runner()
        if checkpoint_obj:
            self.restore_from_obj(checkpoint_obj)
        elif self._checkpoint_path:
            self.restore_from_path(self._checkpoint_path)
        elif self._checkpoint_obj:
            self.restore_from_obj(self._checkpoint_obj)

    def stop(self, error=False, error_msg=None, stop_logger=True):
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
            self.status = Trial.ERROR
        else:
            self.status = Trial.TERMINATED

        try:
            if error_msg and self.logdir:
                self.num_failures += 1
                error_file = os.path.join(self.logdir,
                                          "error_{}.txt".format(date_str()))
                with open(error_file, "w") as f:
                    f.write(error_msg)
                self.error_file = error_file
            if self.runner:
                stop_tasks = []
                stop_tasks.append(self.runner.stop.remote())
                stop_tasks.append(self.runner.__ray_terminate__.remote())
                # TODO(ekl)  seems like wait hangs when killing actors
                _, unfinished = ray.wait(
                    stop_tasks, num_returns=2, timeout=250)
        except Exception:
            print("Error stopping runner:", traceback.format_exc())
            self.status = Trial.ERROR
        finally:
            self.runner = None

        if stop_logger and self.result_logger:
            self.result_logger.close()
            self.result_logger = None

    def train_remote(self):
        """Returns Ray future for one iteration of training."""

        assert self.status == Trial.RUNNING, self.status
        return self.runner.train.remote()

    def save_to_object(self):
        return self.runner.save_to_object.remote()

    def save_to_path(self):
        ray.get(self.runner.save.remote())

    def _setup_runner(self):
        self.status = Trial.RUNNING
        cls = ray.remote(
            num_cpus=self.resources.cpu,
            num_gpus=self.resources.gpu)(self._get_trainable_cls())
        if not self.result_logger:
            if not os.path.exists(self.local_dir):
                os.makedirs(self.local_dir)
            self.logdir = tempfile.mkdtemp(
                prefix="{}_{}".format(
                    str(self)[:MAX_LEN_IDENTIFIER], date_str()),
                dir=self.local_dir)
            self.result_logger = UnifiedLogger(self.config, self.logdir,
                                               self.upload_dir)
        remote_logdir = self.logdir

        def logger_creator(config):
            # Set the working dir in the remote process, for user file writes
            if not os.path.exists(remote_logdir):
                os.makedirs(remote_logdir)
            os.chdir(remote_logdir)
            return NoopLogger(config, remote_logdir)

        # Logging for trials is handled centrally by TrialRunner, so
        # configure the remote runner to use a noop-logger.
        self.runner = cls.remote(
            config=self.config, logger_creator=logger_creator)

    def restore_from_path(self, path):
        """Restores runner state from specified path.

        Args:
            path (str): A path where state will be restored.
        """

        if self.runner is None:
            print("Unable to restore - no runner")
        else:
            try:
                ray.get(self.runner.restore.remote(path))
            except Exception:
                print("Error restoring runner:", traceback.format_exc())
                self.status = Trial.ERROR

    def restore_from_obj(self, obj):
        """Restores runner state from the specified object."""

        if self.runner is None:
            print("Unable to restore - no runner")
        else:
            try:
                ray.get(self.runner.restore_from_object.remote(obj))
            except Exception:
                print("Error restoring runner:", traceback.format_exc())
                self.status = Trial.ERROR

register_trial_impl(RayTrial) # any call to Trial() will create RayTrial instead of Trial.

class RayTrialExecutor(TrialExecutor):
    def __init__(self, queue_trials = False):
        super(RayTrialExecutor, self).__init__(queue_trials)
        self._running = {}
        self._avail_resources = Resources(cpu=0, gpu=0)
        self._committed_resources = Resources(cpu=0, gpu=0)
        self._resources_initialized = False

    def start_trial(self, trial, checkpoint_obj=None):
        self._commit_resources(trial.resources)
        try:
            trial.start(checkpoint_obj=checkpoint_obj)
            self._running[trial.train_remote()] = trial
        except Exception:
            error_msg = traceback.format_exc()
            print("Error starting runner, retrying:", error_msg)
            time.sleep(2)
            trial.stop(error=True, error_msg=error_msg)
            try:
                trial.start()
                self._running[trial.train_remote()] = trial
            except Exception:
                error_msg = traceback.format_exc()
                print("Error starting runner, abort:", error_msg)
                trial.stop(error=True, error_msg=error_msg)
                # note that we don't return the resources, since they may
                # have been lost

    def stop_trial(self, trial, error=False, error_msg=None, stop_logger=True):
        """Only returns resources if resources allocated."""
        prior_status = trial.status
        trial.stop(error=error, error_msg=error_msg)
        if prior_status == Trial.RUNNING:
            self._return_resources(trial.resources)

    def continue_trial(self, trial):
        self._running[trial.train_remote()] = trial

    def pause_trial(self, trial):
        assert trial.status == Trial.RUNNING, trial.status
        super(RayTrialExecutor, self).pause_trial(trial)
        self._return_resources(trial.resources)

    def get_running_trials(self):
        return list(self._running.values())

    def fetch_one_result(self):
        # NOTE: There should only be one...
        [result_id], _ = ray.wait(list(self._running))
        trial = self._running.pop(result_id)
        result = ray.get(result_id)
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

        if ((resources.cpu_total() > 0 and cpu_avail <= 0)
            or (resources.gpu_total() > 0 and gpu_avail <= 0)):
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
        if self._resources_initialized:
            return "Resources requested: {}/{} CPUs, {}/{} GPUs".format(
                self._committed_resources.cpu, self._avail_resources.cpu,
                self._committed_resources.gpu, self._avail_resources.gpu)
        else:
            return ""

    def on_step_begin(self):
        self._update_avail_resources()