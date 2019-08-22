from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray.cloudpickle as cloudpickle
import copy
from datetime import datetime
import logging
import uuid
import time
import tempfile
import os
import ray
from ray.tune import TuneError
from ray.tune.logger import pretty_print, UnifiedLogger
# NOTE(rkn): We import ray.tune.registry here instead of importing the names we
# need because there are cyclic imports that may cause specific names to not
# have been defined yet. See https://github.com/ray-project/ray/issues/1716.
import ray.tune.registry
from ray.tune.result import (DEFAULT_RESULTS_DIR, DONE, HOSTNAME, PID,
                             TIME_TOTAL_S, TRAINING_ITERATION, TIMESTEPS_TOTAL,
                             EPISODE_REWARD_MEAN, MEAN_LOSS, MEAN_ACCURACY)
from ray.utils import binary_to_hex, hex_to_binary
from ray.tune.resources import Resources, json_to_resources, resources_to_json

DEBUG_PRINT_INTERVAL = 5
MAX_LEN_IDENTIFIER = int(os.environ.get("MAX_LEN_IDENTIFIER", 130))
logger = logging.getLogger(__name__)


def date_str():
    return datetime.today().strftime("%Y-%m-%d_%H-%M-%S")


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
        self.last_result = last_result or {}

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
        for i in range(len(export_formats)):
            export_formats[i] = export_formats[i].strip().lower()
            if export_formats[i] not in [
                    ExportFormat.CHECKPOINT, ExportFormat.MODEL
            ]:
                raise TuneError("Unsupported export format: " +
                                export_formats[i])


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
                 keep_checkpoints_num=None,
                 checkpoint_score_attr="",
                 export_formats=None,
                 restore_path=None,
                 trial_name_creator=None,
                 loggers=None,
                 sync_to_driver_fn=None,
                 max_failures=0):
        """Initialize a new trial.

        The args here take the same meaning as the command line flags defined
        in ray.tune.config_parser.
        """

        Trial._registration_check(trainable_name)
        # Trial config
        self.trainable_name = trainable_name
        self.trial_id = Trial.generate_id() if trial_id is None else trial_id
        self.config = config or {}
        self.local_dir = local_dir  # This remains unexpanded for syncing.
        self.experiment_tag = experiment_tag
        trainable_cls = self._get_trainable_cls()
        if trainable_cls and hasattr(trainable_cls,
                                     "default_resource_request"):
            default_resources = trainable_cls.default_resource_request(
                self.config)
            if default_resources:
                if resources:
                    raise ValueError(
                        "Resources for {} have been automatically set to {} "
                        "by its `default_resource_request()` method. Please "
                        "clear the `resources_per_trial` option.".format(
                            trainable_cls, default_resources))
                resources = default_resources
        self.resources = resources or Resources(cpu=1, gpu=0)
        self.stopping_criterion = stopping_criterion or {}
        self.loggers = loggers
        self.sync_to_driver_fn = sync_to_driver_fn
        self.verbose = True
        self.max_failures = max_failures

        # Local trial state that is updated during the run
        self.last_result = {}
        self.last_update_time = -float("inf")
        self.checkpoint_freq = checkpoint_freq
        self.checkpoint_at_end = checkpoint_at_end

        self.history = []
        self.keep_checkpoints_num = keep_checkpoints_num
        self._cmp_greater = not checkpoint_score_attr.startswith("min-")
        self.best_checkpoint_attr_value = -float("inf") \
            if self._cmp_greater else float("inf")
        # Strip off "min-" from checkpoint attribute
        self.checkpoint_score_attr = checkpoint_score_attr \
            if self._cmp_greater else checkpoint_score_attr[4:]

        self._checkpoint = Checkpoint(
            storage=Checkpoint.DISK, value=restore_path)
        self.export_formats = export_formats
        self.status = Trial.PENDING
        self.logdir = None
        self.runner = None
        self.result_logger = None
        self.last_debug = 0
        self.error_file = None
        self.error_msg = None
        self.num_failures = 0
        self.custom_trial_name = None

        # AutoML fields
        self.results = None
        self.best_result = None
        self.param_config = None
        self.extra_arg = None

        self._nonjson_fields = [
            "_checkpoint",
            "loggers",
            "sync_to_driver_fn",
            "results",
            "best_result",
            "param_config",
            "extra_arg",
        ]
        if trial_name_creator:
            self.custom_trial_name = trial_name_creator(self)

    @classmethod
    def _registration_check(cls, trainable_name):
        if not has_trainable(trainable_name):
            # Make sure rllib agents are registered
            from ray import rllib  # noqa: F401
            if not has_trainable(trainable_name):
                raise TuneError("Unknown trainable: " + trainable_name)

    @classmethod
    def generate_id(cls):
        return str(uuid.uuid1().hex)[:8]

    @classmethod
    def create_logdir(cls, identifier, local_dir):
        local_dir = os.path.expanduser(local_dir)
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)
        return tempfile.mkdtemp(
            prefix="{}_{}".format(identifier[:MAX_LEN_IDENTIFIER], date_str()),
            dir=local_dir)

    def init_logger(self):
        """Init logger."""

        if not self.result_logger:
            if not self.logdir:
                self.logdir = Trial.create_logdir(str(self), self.local_dir)
            elif not os.path.exists(self.logdir):
                os.makedirs(self.logdir)

            self.result_logger = UnifiedLogger(
                self.config,
                self.logdir,
                loggers=self.loggers,
                sync_function=self.sync_to_driver_fn)

    def update_resources(self, cpu, gpu, **kwargs):
        """EXPERIMENTAL: Updates the resource requirements.

        Should only be called when the trial is not running.

        Raises:
            ValueError if trial status is running.
        """
        if self.status is Trial.RUNNING:
            raise ValueError("Cannot update resources while Trial is running.")
        self.resources = Resources(cpu, gpu, **kwargs)

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
            self.error_msg = error_msg

    def should_stop(self, result):
        """Whether the given result meets this trial's stopping criteria."""

        if result.get(DONE):
            return True

        for criteria, stop_value in self.stopping_criterion.items():
            if criteria not in result:
                raise TuneError(
                    "Stopping criteria {} not provided in result {}.".format(
                        criteria, result))
            elif isinstance(criteria, dict):
                raise ValueError(
                    "Stopping criteria is now flattened by default. "
                    "Use forward slashes to nest values `key1/key2/key3`.")
            elif result[criteria] >= stop_value:
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

        if not self.last_result:
            return self._status_string()

        def location_string(hostname, pid):
            if hostname == os.uname()[1]:
                return "pid={}".format(pid)
            else:
                return "{} pid={}".format(hostname, pid)

        pieces = [
            "{}".format(self._status_string()), "[{}]".format(
                self.resources.summary_string()), "[{}]".format(
                    location_string(
                        self.last_result.get(HOSTNAME),
                        self.last_result.get(PID))), "{} s".format(
                            int(self.last_result.get(TIME_TOTAL_S, 0)))
        ]

        if self.last_result.get(TRAINING_ITERATION) is not None:
            pieces.append("{} iter".format(
                self.last_result[TRAINING_ITERATION]))

        if self.last_result.get(TIMESTEPS_TOTAL) is not None:
            pieces.append("{} ts".format(self.last_result[TIMESTEPS_TOTAL]))

        if self.last_result.get(EPISODE_REWARD_MEAN) is not None:
            pieces.append("{} rew".format(
                format(self.last_result[EPISODE_REWARD_MEAN], ".3g")))

        if self.last_result.get(MEAN_LOSS) is not None:
            pieces.append("{} loss".format(
                format(self.last_result[MEAN_LOSS], ".3g")))

        if self.last_result.get(MEAN_ACCURACY) is not None:
            pieces.append("{} acc".format(
                format(self.last_result[MEAN_ACCURACY], ".3g")))

        return ", ".join(pieces)

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
        result.update(trial_id=self.trial_id, done=terminate)
        if self.verbose and (terminate or time.time() - self.last_debug >
                             DEBUG_PRINT_INTERVAL):
            print("Result for {}:".format(self))
            print("  {}".format(pretty_print(result).replace("\n", "\n  ")))
            self.last_debug = time.time()
        self.last_result = result
        self.last_update_time = time.time()
        self.result_logger.on_result(self.last_result)

    def compare_checkpoints(self, attr_mean):
        """Compares two checkpoints based on the attribute attr_mean param.
        Greater than is used by default. If  command-line parameter
        checkpoint_score_attr starts with "min-" less than is used.

        Arguments:
            attr_mean: mean of attribute value for the current checkpoint

        Returns:
            True: when attr_mean is greater than previous checkpoint attr_mean
                  and greater than function is selected
                  when attr_mean is less than previous checkpoint attr_mean and
                  less than function is selected
            False: when attr_mean is not in alignment with selected cmp fn
        """
        if self._cmp_greater and attr_mean > self.best_checkpoint_attr_value:
            return True
        elif (not self._cmp_greater
              and attr_mean < self.best_checkpoint_attr_value):
            return True
        return False

    def _get_trainable_cls(self):
        return ray.tune.registry._global_registry.get(
            ray.tune.registry.TRAINABLE_CLASS, self.trainable_name)

    def set_verbose(self, verbose):
        self.verbose = verbose

    def is_finished(self):
        return self.status in [Trial.TERMINATED, Trial.ERROR]

    @property
    def node_ip(self):
        return self.last_result.get("node_ip")

    def __repr__(self):
        return str(self)

    def __str__(self):
        """Combines ``env`` with ``trainable_name`` and ``experiment_tag``.

        Can be overriden with a custom string creator.
        """
        if self.custom_trial_name:
            return self.custom_trial_name

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

        for key in self._nonjson_fields:
            state[key] = binary_to_hex(cloudpickle.dumps(state.get(key)))

        state["runner"] = None
        state["result_logger"] = None
        if self.result_logger:
            self.result_logger.flush()
            state["__logger_started__"] = True
        else:
            state["__logger_started__"] = False
        return copy.deepcopy(state)

    def __setstate__(self, state):
        logger_started = state.pop("__logger_started__")
        state["resources"] = json_to_resources(state["resources"])
        if state["status"] == Trial.RUNNING:
            state["status"] = Trial.PENDING
        for key in self._nonjson_fields:
            state[key] = cloudpickle.loads(hex_to_binary(state[key]))

        self.__dict__.update(state)
        Trial._registration_check(self.trainable_name)
        if logger_started:
            self.init_logger()
