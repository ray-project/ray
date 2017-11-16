from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tempfile
import traceback
import ray
import os

from collections import namedtuple
from ray.rllib.agent import get_agent_class
from ray.tune.logger import NoopLogger, UnifiedLogger


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
            self, env_creator, alg, config={}, local_dir='/tmp/ray',
            experiment_tag=None, resources=Resources(cpu=1, gpu=0),
            stopping_criterion={}, checkpoint_freq=0,
            restore_path=None, upload_dir=None):
        """Initialize a new trial.

        The args here take the same meaning as the command line flags defined
        in ray.tune.config_parser.
        """

        # Immutable config
        self.env_creator = env_creator
        if type(env_creator) is str:
            self.env_name = env_creator
        else:
            if hasattr(env_creator, "env_name"):
                self.env_name = env_creator.env_name
            else:
                self.env_name = "custom"
        self.alg = alg
        self.config = config
        self.local_dir = local_dir
        self.experiment_tag = experiment_tag
        self.resources = resources
        self.stopping_criterion = stopping_criterion
        self.checkpoint_freq = checkpoint_freq
        self.upload_dir = upload_dir

        # Local trial state that is updated during the run
        self.last_result = None
        self._checkpoint_path = restore_path
        self.agent = None
        self.status = Trial.PENDING
        self.location = None
        self.logdir = None
        self.result_logger = None

    def start(self):
        """Starts this trial.

        If an error is encountered when starting the trial, an exception will
        be thrown.
        """

        self._setup_agent()
        if self._checkpoint_path:
            self.restore_from_path(path=self._checkpoint_path)

    def stop(self, error=False, stop_logger=True):
        """Stops this trial.

        Stops this trial, releasing all allocating resources. If stopping the
        trial fails, the run will be marked as terminated in error, but no
        exception will be thrown.

        Args:
            error (bool): Whether to mark this trial as terminated in error.
        """

        if error:
            self.status = Trial.ERROR
        else:
            self.status = Trial.TERMINATED

        try:
            if self.agent:
                stop_tasks = []
                stop_tasks.append(self.agent.stop.remote())
                stop_tasks.append(self.agent.__ray_terminate__.remote(
                    self.agent._ray_actor_id.id()))
                # TODO(ekl)  seems like wait hangs when killing actors
                _, unfinished = ray.wait(
                        stop_tasks, num_returns=2, timeout=250)
                if unfinished:
                    print(("Stopping %s Actor timed out, "
                           "but moving on...") % self)
        except Exception:
            print("Error stopping agent:", traceback.format_exc())
            self.status = Trial.ERROR
        finally:
            self.agent = None

        if stop_logger and self.result_logger:
            self.result_logger.close()
            self.result_logger = None

    def pause(self):
        """We want to release resources (specifically GPUs) when pausing an
        experiment. This results in a state similar to TERMINATED."""

        assert self.status == Trial.RUNNING, self.status
        try:
            self.checkpoint()
            self.stop(stop_logger=False)
            self.status = Trial.PAUSED
        except Exception:
            print("Error pausing agent:", traceback.format_exc())
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
        return self.agent.train.remote()

    def should_stop(self, result):
        """Whether the given result meets this trial's stopping criteria."""

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
            return self.status

        def location_string(hostname, pid):
            if hostname == os.uname()[1]:
                return 'pid={}'.format(pid)
            else:
                return '{} pid={}'.format(hostname, pid)

        pieces = [
            '{} [{}]'.format(
                self.status, location_string(
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

    def checkpoint(self):
        """Synchronously checkpoints the state of this trial.

        TODO(ekl): we should support a PAUSED state based on checkpointing.
        """

        path = ray.get(self.agent.save.remote())
        self._checkpoint_path = path
        print("Saved checkpoint to:", path)
        return path

    def restore_from_path(self, path):
        """Restores agent state from specified path.

        Args:
            path (str): A path where state will be restored.
        """

        if self.agent is None:
            print("Unable to restore - no agent")
        else:
            try:
                ray.get(self.agent.restore.remote(path))
            except Exception:
                print("Error restoring agent:", traceback.format_exc())
                self.status = Trial.ERROR

    def _setup_agent(self):
        self.status = Trial.RUNNING
        agent_cls = get_agent_class(self.alg)
        cls = ray.remote(
            num_cpus=self.resources.driver_cpu_limit,
            num_gpus=self.resources.driver_gpu_limit)(agent_cls)
        if not self.result_logger:
            if not os.path.exists(self.local_dir):
                os.makedirs(self.local_dir)
            self.logdir = tempfile.mkdtemp(
                prefix=str(self), dir=self.local_dir)
            self.result_logger = UnifiedLogger(
                self.config, self.logdir, self.upload_dir)
        remote_logdir = self.logdir
        # Logging for trials is handled centrally by TrialRunner, so
        # configure the remote agent to use a noop-logger.
        self.agent = cls.remote(
            self.env_creator, self.config,
            lambda config: NoopLogger(config, remote_logdir))

    def __str__(self):
        identifier = '{}_{}'.format(self.alg, self.env_name)
        if self.experiment_tag:
            identifier += '_' + self.experiment_tag
        return identifier

    def __eq__(self, other):
        return str(self) == str(other)

    def __hash__(self):
        return hash(str(self))
