from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import namedtuple
from datetime import datetime
import tempfile
import time
import traceback
import ray
import os

from ray.tune import TuneError
from ray.tune.logger import NoopLogger, UnifiedLogger, pretty_print
# NOTE(rkn): We import ray.tune.registry here instead of importing the names we
# need because there are cyclic imports that may cause specific names to not
# have been defined yet. See https://github.com/ray-project/ray/issues/1716.
import ray.tune.registry
from ray.tune.result import TrainingResult, DEFAULT_RESULTS_DIR
from ray.utils import random_string, binary_to_hex

DEBUG_PRINT_INTERVAL = 5
MAX_LEN_IDENTIFIER = 130


def date_str():
    return datetime.today().strftime("%Y-%m-%d_%H-%M-%S")


class Resources(
        namedtuple("Resources", [
            "cpu", "gpu", "driver_cpu_limit", "driver_gpu_limit"])):
    """Ray resources required to schedule a trial.

    Attributes:
        cpu (int): Number of CPUs required for the trial total.
        gpu (int): Number of GPUs required for the trial total.
        driver_cpu_limit (int): Max CPUs allocated to the driver.
            Defaults to all of the required CPUs.
        driver_gpu_limit (int): Max GPUs allocated to the driver.
            Defaults to all of the required GPUs.
    """
    __slots__ = ()

    def __new__(cls, cpu, gpu, driver_cpu_limit=None, driver_gpu_limit=None):
        if driver_cpu_limit is not None:
            assert driver_cpu_limit <= cpu
        else:
            driver_cpu_limit = cpu
        if driver_gpu_limit is not None:
            assert driver_gpu_limit <= gpu
        else:
            driver_gpu_limit = gpu
        return super(Resources, cls).__new__(
            cls, cpu, gpu, driver_cpu_limit, driver_gpu_limit)

    def summary_string(self):
        return "{} CPUs, {} GPUs".format(self.cpu, self.gpu)


class Trial(object):
    """A trial object holds the state for one model training run.

    Trials are themselves managed by the TrialRunner class, which implements
    the event loop for submitting trial runs to a Ray cluster.

    Trials start in the PENDING state, and transition to RUNNING once started.
    On error it transitions to ERROR, otherwise TERMINATED on success.

    The driver for the trial will be allocated at most `driver_cpu_limit` and
    `driver_gpu_limit` CPUs and GPUs.
    """

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    TERMINATED = "TERMINATED"
    ERROR = "ERROR"

    def __init__(
            self, trainable_name, config=None, local_dir=DEFAULT_RESULTS_DIR,
            experiment_tag=None, resources=Resources(cpu=1, gpu=0),
            stopping_criterion=None, checkpoint_freq=0,
            restore_path=None, upload_dir=None, max_failures=0):
        """Initialize a new trial.

        The args here take the same meaning as the command line flags defined
        in ray.tune.config_parser.
        """

        if not ray.tune.registry._default_registry.contains(
                ray.tune.registry.TRAINABLE_CLASS, trainable_name):
            raise TuneError("Unknown trainable: " + trainable_name)

        if stopping_criterion:
            for k in stopping_criterion:
                if k not in TrainingResult._fields:
                    raise TuneError(
                        "Stopping condition key `{}` must be one of {}".format(
                            k, TrainingResult._fields))

        # Trial config
        self.trainable_name = trainable_name
        self.config = config or {}
        self.local_dir = local_dir
        self.experiment_tag = experiment_tag
        self.resources = resources
        self.stopping_criterion = stopping_criterion or {}
        self.checkpoint_freq = checkpoint_freq
        self.upload_dir = upload_dir
        self.verbose = True
        self.max_failures = max_failures

        # Local trial state that is updated during the run
        self.last_result = None
        self._checkpoint_path = restore_path
        self._checkpoint_obj = None
        self.runner = None
        self.status = Trial.PENDING
        self.location = None
        self.logdir = None
        self.result_logger = None
        self.last_debug = 0
        self.trial_id = binary_to_hex(random_string())[:8]
        self.error_file = None
        self.num_failures = 0

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
                error_file = os.path.join(
                    self.logdir, "error_{}.txt".format(date_str()))
                with open(error_file, "w") as f:
                    f.write(error_msg)
                self.error_file = error_file
            if self.runner:
                stop_tasks = []
                stop_tasks.append(self.runner.stop.remote())
                stop_tasks.append(self.runner.__ray_terminate__.remote(
                    self.runner._ray_actor_id.id()))
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

    def pause(self):
        """We want to release resources (specifically GPUs) when pausing an
        experiment. This results in a state similar to TERMINATED."""

        assert self.status == Trial.RUNNING, self.status
        try:
            self.checkpoint(to_object_store=True)
            self.stop(stop_logger=False)
            self.status = Trial.PAUSED
        except Exception:
            print("Error pausing runner:", traceback.format_exc())
            self.status = Trial.ERROR

    def unpause(self):
        """Sets PAUSED trial to pending to allow scheduler to start."""
        assert self.status == Trial.PAUSED, self.status
        self.status = Trial.PENDING

    def resume(self):
        """Resume PAUSED trials. This is a blocking call."""

        assert self.status == Trial.PAUSED, self.status
        self.start()

    def train_remote(self):
        """Returns Ray future for one iteration of training."""

        assert self.status == Trial.RUNNING, self.status
        return self.runner.train.remote()

    def should_stop(self, result):
        """Whether the given result meets this trial's stopping criteria."""

        if result.done:
            return True

        for criteria, stop_value in self.stopping_criterion.items():
            if getattr(result, criteria) >= stop_value:
                return True

        return False

    def should_checkpoint(self):
        """Whether this trial is due for checkpointing."""

        if not self.checkpoint_freq:
            return False

        return self.last_result.training_iteration % self.checkpoint_freq == 0

    def progress_string(self):
        """Returns a progress message for printing out to the console."""

        if self.last_result is None:
            return self._status_string()

        def location_string(hostname, pid):
            if hostname == os.uname()[1]:
                return 'pid={}'.format(pid)
            else:
                return '{} pid={}'.format(hostname, pid)

        pieces = [
            '{} [{}]'.format(
                self._status_string(),
                location_string(
                    self.last_result.hostname, self.last_result.pid)),
            '{} s'.format(int(self.last_result.time_total_s)),
            '{} ts'.format(int(self.last_result.timesteps_total))]

        if self.last_result.episode_reward_mean is not None:
            pieces.append('{} rew'.format(
                format(self.last_result.episode_reward_mean, '.3g')))

        if self.last_result.mean_loss is not None:
            pieces.append('{} loss'.format(
                format(self.last_result.mean_loss, '.3g')))

        if self.last_result.mean_accuracy is not None:
            pieces.append('{} acc'.format(
                format(self.last_result.mean_accuracy, '.3g')))

        return ', '.join(pieces)

    def _status_string(self):
        return "{}{}".format(
            self.status,
            ", {} failures: {}".format(self.num_failures, self.error_file)
            if self.error_file else "")

    def has_checkpoint(self):
        return self._checkpoint_path is not None or \
            self._checkpoint_obj is not None

    def checkpoint(self, to_object_store=False):
        """Checkpoints the state of this trial.

        Args:
            to_object_store (bool): Whether to save to the Ray object store
                (async) vs a path on local disk (sync).
        """

        obj = None
        path = None
        if to_object_store:
            obj = self.runner.save_to_object.remote()
        else:
            path = ray.get(self.runner.save.remote())
        self._checkpoint_path = path
        self._checkpoint_obj = obj

        if self.verbose:
            print("Saved checkpoint for {} to {}".format(self, path or obj))
        return path or obj

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

    def update_last_result(self, result, terminate=False):
        if terminate:
            result = result._replace(done=True)
        if terminate or (
                self.verbose and
                time.time() - self.last_debug > DEBUG_PRINT_INTERVAL):
            print("TrainingResult for {}:".format(self))
            print("  {}".format(pretty_print(result).replace("\n", "\n  ")))
            self.last_debug = time.time()
        self.last_result = result
        self.result_logger.on_result(self.last_result)

    def _setup_runner(self):
        self.status = Trial.RUNNING
        trainable_cls = ray.tune.registry.get_registry().get(
            ray.tune.registry.TRAINABLE_CLASS, self.trainable_name)
        cls = ray.remote(
            num_cpus=self.resources.driver_cpu_limit,
            num_gpus=self.resources.driver_gpu_limit)(trainable_cls)
        if not self.result_logger:
            if not os.path.exists(self.local_dir):
                os.makedirs(self.local_dir)
            self.logdir = tempfile.mkdtemp(
                prefix="{}_{}".format(
                    str(self)[:MAX_LEN_IDENTIFIER], date_str()),
                dir=self.local_dir)
            self.result_logger = UnifiedLogger(
                self.config, self.logdir, self.upload_dir)
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
            config=self.config, registry=ray.tune.registry.get_registry(),
            logger_creator=logger_creator)

    def set_verbose(self, verbose):
        self.verbose = verbose

    def is_finished(self):
        return self.status in [Trial.TERMINATED, Trial.ERROR]

    def __repr__(self):
        return str(self)

    def __str__(self):
        """Combines ``env`` with ``trainable_name`` and ``experiment_tag``."""
        if "env" in self.config:
            identifier = "{}_{}".format(
                self.trainable_name, self.config["env"])
        else:
            identifier = self.trainable_name
        if self.experiment_tag:
            identifier += "_" + self.experiment_tag
        return identifier
