from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import namedtuple
import ray.cloudpickle as cloudpickle
import copy
from datetime import datetime
import logging
import json
import time
import tempfile
import os

# For compatibility under py2 to consider unicode as str
from six import string_types
from numbers import Number

import ray
from ray.tune import TuneError
from ray.tune.log_sync import validate_sync_function
from ray.tune.logger import pretty_print, UnifiedLogger
# NOTE(rkn): We import ray.tune.registry here instead of importing the names we
# need because there are cyclic imports that may cause specific names to not
# have been defined yet. See https://github.com/ray-project/ray/issues/1716.
import ray.tune.registry
from ray.tune.result import (DEFAULT_RESULTS_DIR, DONE, HOSTNAME, PID,
                             TIME_TOTAL_S, TRAINING_ITERATION, TIMESTEPS_TOTAL)
from ray.utils import random_string, binary_to_hex, hex_to_binary

DEBUG_PRINT_INTERVAL = 5
MAX_LEN_IDENTIFIER = 130
logger = logging.getLogger(__name__)


def date_str():
    return datetime.today().strftime("%Y-%m-%d_%H-%M-%S")


class Resources(
        namedtuple("Resources", ["cpu", "gpu", "extra_cpu", "extra_gpu"])):
    """Ray resources required to schedule a trial.

    TODO: Custom resources.

    Attributes:
        cpu (float): Number of CPUs to allocate to the trial.
        gpu (float): Number of GPUs to allocate to the trial.
        extra_cpu (float): Extra CPUs to reserve in case the trial needs to
            launch additional Ray actors that use CPUs.
        extra_gpu (float): Extra GPUs to reserve in case the trial needs to
            launch additional Ray actors that use GPUs.

    """

    __slots__ = ()

    def __new__(cls, cpu, gpu, extra_cpu=0, extra_gpu=0):
        for entry in [cpu, gpu, extra_cpu, extra_gpu]:
            assert isinstance(entry, Number), "Improper resource value."
            assert entry >= 0, "Resource cannot be negative."
        return super(Resources, cls).__new__(cls, cpu, gpu, extra_cpu,
                                             extra_gpu)

    def summary_string(self):
        return "{} CPUs, {} GPUs".format(self.cpu + self.extra_cpu,
                                         self.gpu + self.extra_gpu)

    def cpu_total(self):
        return self.cpu + self.extra_cpu

    def gpu_total(self):
        return self.gpu + self.extra_gpu


def json_to_resources(data):
    if data is None or data == "null":
        return None
    if isinstance(data, string_types):
        data = json.loads(data)
    for k in data:
        if k in ["driver_cpu_limit", "driver_gpu_limit"]:
            raise TuneError(
                "The field `{}` is no longer supported. Use `extra_cpu` "
                "or `extra_gpu` instead.".format(k))
        if k not in Resources._fields:
            raise TuneError(
                "Unknown resource type {}, must be one of {}".format(
                    k, Resources._fields))
    return Resources(
        data.get("cpu", 1), data.get("gpu", 0), data.get("extra_cpu", 0),
        data.get("extra_gpu", 0))


def resources_to_json(resources):
    if resources is None:
        return None
    return {
        "cpu": resources.cpu,
        "gpu": resources.gpu,
        "extra_cpu": resources.extra_cpu,
        "extra_gpu": resources.extra_gpu,
    }


def has_trainable(trainable_name):
    return ray.tune.registry._global_registry.contains(
        ray.tune.registry.TRAINABLE_CLASS, trainable_name)


class Checkpoint(object):
    """Describes a checkpoint of trial state.

    Checkpoint may be saved in different storage.

    Attributes:
        storage (str): Storage type.
        value (str): If storage==MEMORY,value is a Python object.
            If storage==DISK,value is a path points to the checkpoint in disk.
    """

    MEMORY = "memory"
    DISK = "disk"

    def __init__(self, storage, value, last_result=None):
        self.storage = storage
        self.value = value
        self.last_result = last_result

    @staticmethod
    def from_object(value=None):
        """Creates a checkpoint from a Python object."""
        return Checkpoint(Checkpoint.MEMORY, value)


class ExportFormat(object):
    """Describes the format to export the trial Trainable.

    This may correspond to different file formats based on the
    Trainable implementation.
    """
    CHECKPOINT = "checkpoint"
    MODEL = "model"

    @staticmethod
    def validate(export_formats):
        """Validates export_formats.

        Raises:
            ValueError if the format is unknown.
        """
        for export_format in export_formats:
            if export_format not in [
                    ExportFormat.CHECKPOINT, ExportFormat.MODEL
            ]:
                raise TuneError("Unsupported export format: " + export_format)


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
                 checkpoint_at_end=False,
                 export_formats=None,
                 restore_path=None,
                 upload_dir=None,
                 trial_name_creator=None,
                 custom_loggers=None,
                 sync_function=None,
                 max_failures=0):
        """Initialize a new trial.

        The args here take the same meaning as the command line flags defined
        in ray.tune.config_parser.
        """

        Trial._registration_check(trainable_name)
        # Trial config
        self.trainable_name = trainable_name
        self.config = config or {}
        self.local_dir = os.path.expanduser(local_dir)
        self.experiment_tag = experiment_tag
        self.resources = (
            resources
            or self._get_trainable_cls().default_resource_request(self.config))
        self.stopping_criterion = stopping_criterion or {}
        self.upload_dir = upload_dir
        self.custom_loggers = custom_loggers
        self.sync_function = sync_function
        validate_sync_function(sync_function)
        self.verbose = True
        self.max_failures = max_failures

        # Local trial state that is updated during the run
        self.last_result = None
        self.last_update_time = -float("inf")
        self.checkpoint_freq = checkpoint_freq
        self.checkpoint_at_end = checkpoint_at_end
        self._checkpoint = Checkpoint(
            storage=Checkpoint.DISK, value=restore_path)
        self.export_formats = export_formats
        self.status = Trial.PENDING
        self.logdir = None
        self.runner = None
        self.result_logger = None
        self.last_debug = 0
        self.trial_id = Trial.generate_id() if trial_id is None else trial_id
        self.error_file = None
        self.num_failures = 0

        self.trial_name = None
        if trial_name_creator:
            self.trial_name = trial_name_creator(self)

    @classmethod
    def _registration_check(cls, trainable_name):
        if not has_trainable(trainable_name):
            # Make sure rllib agents are registered
            from ray import rllib  # noqa: F401
            if not has_trainable(trainable_name):
                raise TuneError("Unknown trainable: " + trainable_name)

    @classmethod
    def generate_id(cls):
        return binary_to_hex(random_string())[:8]

    def init_logger(self):
        """Init logger."""

        if not self.result_logger:
            if not os.path.exists(self.local_dir):
                os.makedirs(self.local_dir)
            if not self.logdir:
                self.logdir = tempfile.mkdtemp(
                    prefix="{}_{}".format(
                        str(self)[:MAX_LEN_IDENTIFIER], date_str()),
                    dir=self.local_dir)
            elif not os.path.exists(self.logdir):
                os.makedirs(self.logdir)

            self.result_logger = UnifiedLogger(
                self.config,
                self.logdir,
                upload_uri=self.upload_dir,
                custom_loggers=self.custom_loggers,
                sync_function=self.sync_function)

    def sync_logger_to_new_location(self, worker_ip):
        """Updates the logger location.

        Also pushes logdir to worker_ip, allowing for cross-node recovery.
        """
        if self.result_logger:
            self.result_logger.sync_results_to_new_location(worker_ip)

    def close_logger(self):
        """Close logger."""

        if self.result_logger:
            self.result_logger.close()
            self.result_logger = None

    def write_error_log(self, error_msg):
        if error_msg and self.logdir:
            self.num_failures += 1  # may be moved to outer scope?
            error_file = os.path.join(self.logdir,
                                      "error_{}.txt".format(date_str()))
            with open(error_file, "w") as f:
                f.write(error_msg)
            self.error_file = error_file

    def should_stop(self, result):
        """Whether the given result meets this trial's stopping criteria."""

        if result.get(DONE):
            return True

        for criteria, stop_value in self.stopping_criterion.items():
            if criteria not in result:
                raise TuneError(
                    "Stopping criteria {} not provided in result {}.".format(
                        criteria, result))
            if result[criteria] >= stop_value:
                return True

        return False

    def should_checkpoint(self):
        """Whether this trial is due for checkpointing."""
        result = self.last_result or {}

        if result.get(DONE) and self.checkpoint_at_end:
            return True

        if self.checkpoint_freq:
            return result.get(TRAINING_ITERATION,
                              0) % self.checkpoint_freq == 0
        else:
            return False

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

        if self.last_result.get(TRAINING_ITERATION) is not None:
            pieces.append('{} iter'.format(
                self.last_result[TRAINING_ITERATION]))

        if self.last_result.get(TIMESTEPS_TOTAL) is not None:
            pieces.append('{} ts'.format(self.last_result[TIMESTEPS_TOTAL]))

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
        return self._checkpoint.value is not None

    def clear_checkpoint(self):
        self._checkpoint.value = None

    def should_recover(self):
        """Returns whether the trial qualifies for restoring.

        This is if a checkpoint frequency is set and has not failed more than
        max_failures. This may return true even when there may not yet
        be a checkpoint.
        """
        return (self.checkpoint_freq > 0
                and (self.num_failures < self.max_failures
                     or self.max_failures < 0))

    def update_last_result(self, result, terminate=False):
        if terminate:
            result.update(done=True)
        if self.verbose and (terminate or time.time() - self.last_debug >
                             DEBUG_PRINT_INTERVAL):
            print("Result for {}:".format(self))
            print("  {}".format(pretty_print(result).replace("\n", "\n  ")))
            self.last_debug = time.time()
        self.last_result = result
        self.last_update_time = time.time()
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
        """Combines ``env`` with ``trainable_name`` and ``experiment_tag``.

        Can be overriden with a custom string creator.
        """
        if self.trial_name:
            return self.trial_name

        if "env" in self.config:
            env = self.config["env"]
            if isinstance(env, type):
                env = env.__name__
            identifier = "{}_{}".format(self.trainable_name, env)
        else:
            identifier = self.trainable_name
        if self.experiment_tag:
            identifier += "_" + self.experiment_tag
        return identifier.replace("/", "_")

    def __getstate__(self):
        """Memento generator for Trial.

        Sets RUNNING trials to PENDING, and flushes the result logger.
        Note this can only occur if the trial holds a DISK checkpoint.
        """
        assert self._checkpoint.storage == Checkpoint.DISK, (
            "Checkpoint must not be in-memory.")
        state = self.__dict__.copy()
        state["resources"] = resources_to_json(self.resources)

        pickle_data = {
            "_checkpoint": self._checkpoint,
            "config": self.config,
            "custom_loggers": self.custom_loggers,
            "sync_function": self.sync_function,
            "last_result": self.last_result
        }

        for key, value in pickle_data.items():
            state[key] = binary_to_hex(cloudpickle.dumps(value))

        state["runner"] = None
        state["result_logger"] = None
        if self.status == Trial.RUNNING:
            state["status"] = Trial.PENDING
        if self.result_logger:
            self.result_logger.flush()
            state["__logger_started__"] = True
        else:
            state["__logger_started__"] = False
        return copy.deepcopy(state)

    def __setstate__(self, state):
        logger_started = state.pop("__logger_started__")
        state["resources"] = json_to_resources(state["resources"])
        for key in [
                "_checkpoint", "config", "custom_loggers", "sync_function",
                "last_result"
        ]:
            state[key] = cloudpickle.loads(hex_to_binary(state[key]))

        self.__dict__.update(state)
        Trial._registration_check(self.trainable_name)
        if logger_started:
            self.init_logger()
