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
from ray.tune.result import (DEFAULT_RESULTS_DIR, DONE, HOSTNAME, PID,
                             TIME_TOTAL_S, TRAINING_ITERATION)
from ray.utils import random_string, binary_to_hex

DEBUG_PRINT_INTERVAL = 5


def date_str():
    return datetime.today().strftime("%Y-%m-%d_%H-%M-%S")


class Resources(
        namedtuple("Resources", ["cpu", "gpu", "extra_cpu", "extra_gpu"])):
    """Ray resources required to schedule a trial.

    Attributes:
        cpu (int): Number of CPUs to allocate to the trial.
        gpu (int): Number of GPUs to allocate to the trial.
        extra_cpu (int): Extra CPUs to reserve in case the trial needs to
            launch additional Ray actors that use CPUs.
        extra_gpu (int): Extra GPUs to reserve in case the trial needs to
            launch additional Ray actors that use GPUs.
    """
    __slots__ = ()

    def __new__(cls, cpu, gpu, extra_cpu=0, extra_gpu=0):
        return super(Resources, cls).__new__(cls, cpu, gpu, extra_cpu,
                                             extra_gpu)

    def summary_string(self):
        return "{} CPUs, {} GPUs".format(self.cpu + self.extra_cpu,
                                         self.gpu + self.extra_gpu)

    def cpu_total(self):
        return self.cpu + self.extra_cpu

    def gpu_total(self):
        return self.gpu + self.extra_gpu


def has_trainable(trainable_name):
    return ray.tune.registry._global_registry.contains(
        ray.tune.registry.TRAINABLE_CLASS, trainable_name)


class Trial(object):
    """A trial object holds the state for one model training run.

    Trials are themselves managed by the TrialRunner class, which implements
    the event loop for submitting trial runs to a Ray cluster.

    Trials start in the PENDING state, and transition to RUNNING once started.
    On error it transitions to ERROR, otherwise TERMINATED on success.
    """

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    TERMINATED = "TERMINATED"
    ERROR = "ERROR"

    def __init__(self,
                 trainable_name,
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
        """Initialize a new trial.

        The args here take the same meaning as the command line flags defined
        in ray.tune.config_parser.
        """
        if not has_trainable(trainable_name):
            # Make sure rllib agents are registered
            from ray import rllib  # noqa: F401
            if not has_trainable(trainable_name):
                raise TuneError("Unknown trainable: " + trainable_name)

        # Trial config
        self.trainable_name = trainable_name
        self.config = config or {}
        self.local_dir = os.path.expanduser(local_dir)
        self.experiment_tag = experiment_tag
        self.resources = (
            resources
            or self._get_trainable_cls().default_resource_request(self.config))
        self.stopping_criterion = stopping_criterion or {}
        self.checkpoint_freq = checkpoint_freq
        self.upload_dir = upload_dir
        self.verbose = True
        self.max_failures = max_failures

        # Local trial state that is updated during the run
        self.last_result = None
        self._checkpoint_path = restore_path
        self._checkpoint_obj = None
        self.status = Trial.PENDING
        self.location = None
        self.logdir = None
        self.result_logger = None
        self.last_debug = 0
        if trial_id is not None:
            self.trial_id = trial_id
        else:
            self.trial_id = Trial.generate_id()
        self.error_file = None
        self.num_failures = 0

    @classmethod
    def generate_id(cls):
        return binary_to_hex(random_string())[:8]

    def should_stop(self, result):
        """Whether the given result meets this trial's stopping criteria."""

        if result.get(DONE):
            return True

        for criteria, stop_value in self.stopping_criterion.items():
            if criteria not in result:
                raise TuneError("Stopping Criteria not provided in result.")
            if result[criteria] >= stop_value:
                return True

        return False


    def should_checkpoint(self):
        """Whether this trial is due for checkpointing."""

        if not self.checkpoint_freq:
            return False

        return self.last_result[TRAINING_ITERATION] % self.checkpoint_freq == 0

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
                    self.last_result.get(HOSTNAME),
                    self.last_result.get(PID))), '{} s'.format(
                        int(self.last_result.get(TIME_TOTAL_S)))
        ]

        if self.last_result.get("episode_reward_mean") is not None:
            pieces.append('{} rew'.format(
                format(self.last_result["episode_reward_mean"], '.3g')))

        if self.last_result.get("mean_loss") is not None:
            pieces.append('{} loss'.format(
                format(self.last_result["mean_loss"], '.3g')))

        if self.last_result.get("mean_accuracy") is not None:
            pieces.append('{} acc'.format(
                format(self.last_result["mean_accuracy"], '.3g')))

        return ', '.join(pieces)

    def _status_string(self):
        return "{}{}".format(
            self.status, ", {} failures: {}".format(self.num_failures,
                                                    self.error_file)
            if self.error_file else "")

    def has_checkpoint(self):
        return self._checkpoint_path is not None or \
            self._checkpoint_obj is not None

    def save_to_object(self):
        """save the state of this trial to object

        :return: object to store the state.
        """
        return None

    def save_to_path(self):
        """ save the state of trial to path

        :return: path to store the state.
        """
        return None

    def checkpoint(self, to_object_store=False):
        """Checkpoints the state of this trial.

        Args:
            to_object_store (bool): Whether to save to the Ray object store
                (async) vs a path on local disk (sync).
        """
        obj = None
        path = None
        if to_object_store:
            obj = self.save_to_object()
        else:
            path = self.save_to_path()
        self._checkpoint_path = path
        self._checkpoint_obj = obj

        if self.verbose:
            print("Saved checkpoint for {} to {}".format(self, path or obj))

        return path or obj

    def update_last_result(self, result, terminate=False):
        if terminate:
            result.update(done=True)
        if self.verbose and (terminate or time.time() - self.last_debug >
                             DEBUG_PRINT_INTERVAL):
            print("Result for {}:".format(self))
            print("  {}".format(pretty_print(result).replace("\n", "\n  ")))
            self.last_debug = time.time()
        self.last_result = result
        self.result_logger.on_result(self.last_result)

    def _get_trainable_cls(self):
        return ray.tune.registry._global_registry.get(
            ray.tune.registry.TRAINABLE_CLASS, self.trainable_name)

    def set_verbose(self, verbose):
        self.verbose = verbose

    def is_finished(self):
        return self.status in [Trial.TERMINATED, Trial.ERROR]

    def __repr__(self):
        return str(self)

    def __str__(self):
        """Combines ``env`` with ``trainable_name`` and ``experiment_tag``."""
        if "env" in self.config:
            identifier = "{}_{}".format(self.trainable_name,
                                        self.config["env"])
        else:
            identifier = self.trainable_name
        if self.experiment_tag:
            identifier += "_" + self.experiment_tag
        return identifier

    class Holder():
        IMPL_CLS = None # refer to the subclass of Trial.

    def __new__(cls, *args, **kwargs):
        return object.__new__(Trial.Holder.IMPL_CLS)

def register_trial_impl(cls):
    """register Trial type

    any call to Trial() create a cls object

    Args:
        cls: cls should be a subclass of Trial
    """
    Trial.Holder.IMPL_CLS = cls
