from typing import Callable, Dict, Sequence, Union
import json

import ray.cloudpickle as cloudpickle
from collections import deque
import copy
import logging
import platform
import shutil
import uuid
import time
import os
from numbers import Number
from ray.tune import TuneError
from ray.tune.checkpoint_manager import Checkpoint, CheckpointManager
# NOTE(rkn): We import ray.tune.registry here instead of importing the names we
# need because there are cyclic imports that may cause specific names to not
# have been defined yet. See https://github.com/ray-project/ray/issues/1716.
from ray.tune.registry import get_trainable_cls, validate_trainable
from ray.tune.result import DEFAULT_RESULTS_DIR, DONE, TRAINING_ITERATION
from ray.tune.resources import PlacementGroupFactory, Resources, \
    json_to_resources, resources_to_json
from ray.tune.utils.serialization import TuneFunctionEncoder
from ray.tune.utils.trainable import TrainableUtil
from ray.tune.utils import date_str, flatten_dict
from ray.utils import binary_to_hex, hex_to_binary

DEBUG_PRINT_INTERVAL = 5
logger = logging.getLogger(__name__)
if "MAX_LEN_IDENTIFIER" in os.environ:
    logger.error(
        "The MAX_LEN_IDENTIFIER environment variable is deprecated and will "
        "be removed in the future. Use TUNE_MAX_LEN_IDENTIFIER instead.")
MAX_LEN_IDENTIFIER = int(
    os.environ.get("TUNE_MAX_LEN_IDENTIFIER",
                   os.environ.get("MAX_LEN_IDENTIFIER", 130)))


class Location:
    """Describes the location at which Trial is placed to run."""

    def __init__(self, hostname=None, pid=None):
        self.hostname = hostname
        self.pid = pid

    def __str__(self):
        if not self.pid:
            return ""
        elif self.hostname == platform.node():
            return "pid={}".format(self.pid)
        else:
            return "{}:{}".format(self.hostname, self.pid)


class ExportFormat:
    """Describes the format to import/export the trial Trainable.

    This may correspond to different file formats based on the
    Trainable implementation.
    """
    CHECKPOINT = "checkpoint"
    MODEL = "model"
    H5 = "h5"

    @staticmethod
    def validate(formats):
        """Validates formats.

        Raises:
            ValueError if the format is unknown.
        """
        for i in range(len(formats)):
            formats[i] = formats[i].strip().lower()
            if formats[i] not in [
                    ExportFormat.CHECKPOINT, ExportFormat.MODEL,
                    ExportFormat.H5
            ]:
                raise TuneError("Unsupported import/export format: " +
                                formats[i])


def checkpoint_deleter(trial_id, runner):
    """Returns a checkpoint deleter callback for a runner."""
    if not runner:
        return lambda checkpoint: None

    def delete(checkpoint):
        """Requests checkpoint deletion asynchronously.

        Args:
            checkpoint (Checkpoint): Checkpoint to delete.
        """
        if checkpoint.storage == Checkpoint.PERSISTENT and checkpoint.value:
            logger.debug("Trial %s: Deleting checkpoint %s", trial_id,
                         checkpoint.value)
            checkpoint_path = checkpoint.value
            # Delete local copy, if any exists.
            if os.path.exists(checkpoint_path):
                try:
                    checkpoint_dir = TrainableUtil.find_checkpoint_dir(
                        checkpoint_path)
                    shutil.rmtree(checkpoint_dir)
                except FileNotFoundError:
                    logger.warning("Checkpoint dir not found during deletion.")

            # TODO(ujvl): Batch remote deletes.
            runner.delete_checkpoint.remote(checkpoint.value)

    return delete


class TrialInfo:
    """Serializable struct for holding information for a Trial.

    Attributes:
        trial_name (str): String name of the current trial.
        trial_id (str): trial_id of the trial
    """

    def __init__(self, trial):
        self._trial_name = str(trial)
        self._trial_id = trial.trial_id

    @property
    def trial_name(self):
        return self._trial_name

    @property
    def trial_id(self):
        return self._trial_id


def create_logdir(dirname, local_dir):
    local_dir = os.path.expanduser(local_dir)
    logdir = os.path.join(local_dir, dirname)
    if os.path.exists(logdir):
        old_dirname = dirname
        dirname += "_" + uuid.uuid4().hex[:4]
        logger.info(f"Creating a new dirname {dirname} because "
                    f"trial dirname '{old_dirname}' already exists.")
        logdir = os.path.join(local_dir, dirname)
    os.makedirs(logdir, exist_ok=True)
    return logdir


class Trial:
    """A trial object holds the state for one model training run.

    Trials are themselves managed by the TrialRunner class, which implements
    the event loop for submitting trial runs to a Ray cluster.

    Trials start in the PENDING state, and transition to RUNNING once started.
    On error it transitions to ERROR, otherwise TERMINATED on success.

    Attributes:
        trainable_name (str): Name of the trainable object to be executed.
        config (dict): Provided configuration dictionary with evaluated params.
        trial_id (str): Unique identifier for the trial.
        local_dir (str): Local_dir as passed to tune.run.
        logdir (str): Directory where the trial logs are saved.
        evaluated_params (dict): Evaluated parameters by search algorithm,
        experiment_tag (str): Identifying trial name to show in the console.
        resources (Resources): Amount of resources that this trial will use.
        status (str): One of PENDING, RUNNING, PAUSED, TERMINATED, ERROR/
        error_file (str): Path to the errors that this trial has raised.

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
                 evaluated_params=None,
                 experiment_tag="",
                 resources=None,
                 placement_group_factory=None,
                 stopping_criterion=None,
                 remote_checkpoint_dir=None,
                 checkpoint_freq=0,
                 checkpoint_at_end=False,
                 sync_on_checkpoint=True,
                 keep_checkpoints_num=None,
                 checkpoint_score_attr=TRAINING_ITERATION,
                 export_formats=None,
                 restore_path=None,
                 trial_name_creator=None,
                 trial_dirname_creator=None,
                 log_to_file=None,
                 max_failures=0):
        """Initialize a new trial.

        The args here take the same meaning as the command line flags defined
        in ray.tune.config_parser.
        """
        validate_trainable(trainable_name)
        # Trial config
        self.trainable_name = trainable_name
        self.trial_id = Trial.generate_id() if trial_id is None else trial_id
        self.config = config or {}
        self.local_dir = local_dir  # This remains unexpanded for syncing.

        #: Parameters that Tune varies across searches.
        self.evaluated_params = evaluated_params or {}
        self.experiment_tag = experiment_tag
        trainable_cls = self.get_trainable_cls()
        if trainable_cls:
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
        self.location = Location()
        self.resources = resources or Resources(cpu=1, gpu=0)
        self.placement_group_factory = placement_group_factory
        if self.placement_group_factory:
            resource_kwargs = self.resources._asdict()
            resource_kwargs["has_placement_group"] = True
            self.resources = Resources(**resource_kwargs)

        self.stopping_criterion = stopping_criterion or {}

        self.log_to_file = log_to_file
        # Make sure `stdout_file, stderr_file = Trial.log_to_file` works
        if not self.log_to_file or not isinstance(self.log_to_file, Sequence) \
           or not len(self.log_to_file) == 2:
            self.log_to_file = (None, None)

        self.max_failures = max_failures

        # Local trial state that is updated during the run
        self.last_result = {}
        self.last_update_time = -float("inf")

        # stores in memory max/min/avg/last-n-avg/last result for each
        # metric by trial
        self.metric_analysis = {}

        # keep a moving average over these last n steps
        self.n_steps = [5, 10]
        self.metric_n_steps = {}

        self.export_formats = export_formats
        self.status = Trial.PENDING
        self.start_time = None
        self.logdir = None
        self.runner = None
        self.last_debug = 0
        self.error_file = None
        self.error_msg = None
        self.trial_name_creator = trial_name_creator
        self.custom_trial_name = None
        self.custom_dirname = None

        # Checkpointing fields
        self.saving_to = None
        if remote_checkpoint_dir:
            self.remote_checkpoint_dir_prefix = remote_checkpoint_dir
        else:
            self.remote_checkpoint_dir_prefix = None
        self.checkpoint_freq = checkpoint_freq
        self.checkpoint_at_end = checkpoint_at_end
        self.keep_checkpoints_num = keep_checkpoints_num
        self.checkpoint_score_attr = checkpoint_score_attr
        self.sync_on_checkpoint = sync_on_checkpoint
        self.checkpoint_manager = CheckpointManager(
            keep_checkpoints_num, checkpoint_score_attr,
            checkpoint_deleter(self._trainable_name(), self.runner))

        # Restoration fields
        self.restore_path = restore_path
        self.restoring_from = None
        self.num_failures = 0

        # AutoML fields
        self.results = None
        self.best_result = None
        self.param_config = None
        self.extra_arg = None

        self._nonjson_fields = [
            "results",
            "best_result",
            "param_config",
            "extra_arg",
        ]
        if trial_name_creator:
            self.custom_trial_name = trial_name_creator(self)

        if trial_dirname_creator:
            self.custom_dirname = trial_dirname_creator(self)
            if os.path.sep in self.custom_dirname:
                raise ValueError(f"Trial dirname must not contain '/'. "
                                 "Got {self.custom_dirname}")

        self._state_json = None
        self._state_valid = False

    @property
    def node_ip(self):
        return self.location.hostname

    @property
    def checkpoint(self):
        """Returns the most recent checkpoint.

        If the trial is in ERROR state, the most recent PERSISTENT checkpoint
        is returned.
        """
        if self.status == Trial.ERROR:
            checkpoint = self.checkpoint_manager.newest_persistent_checkpoint
        else:
            checkpoint = self.checkpoint_manager.newest_checkpoint
        if checkpoint.value is None:
            checkpoint = Checkpoint(Checkpoint.PERSISTENT, self.restore_path)
        return checkpoint

    @classmethod
    def generate_id(cls):
        return str(uuid.uuid1().hex)[:8]

    @property
    def remote_checkpoint_dir(self):
        assert self.logdir, "Trial {}: logdir not initialized.".format(self)
        if not self.remote_checkpoint_dir_prefix:
            return None
        logdir_name = os.path.basename(self.logdir)
        return os.path.join(self.remote_checkpoint_dir_prefix, logdir_name)

    @property
    def uses_placement_groups(self):
        return bool(self.placement_group_factory)

    def reset(self):
        return Trial(
            self.trainable_name,
            config=self.config,
            trial_id=None,
            local_dir=self.local_dir,
            evaluated_params=self.evaluated_params,
            experiment_tag=self.experiment_tag,
            resources=self.resources,
            placement_group_factory=self.placement_group_factory,
            stopping_criterion=self.stopping_criterion,
            remote_checkpoint_dir=self.remote_checkpoint_dir,
            checkpoint_freq=self.checkpoint_freq,
            checkpoint_at_end=self.checkpoint_at_end,
            sync_on_checkpoint=self.sync_on_checkpoint,
            keep_checkpoints_num=self.keep_checkpoints_num,
            checkpoint_score_attr=self.checkpoint_score_attr,
            export_formats=self.export_formats,
            restore_path=self.restore_path,
            trial_name_creator=self.trial_name_creator,
            log_to_file=self.log_to_file,
            max_failures=self.max_failures,
        )

    def init_logdir(self):
        """Init logdir."""
        if not self.logdir:
            self.logdir = create_logdir(self._generate_dirname(),
                                        self.local_dir)
        else:
            os.makedirs(self.logdir, exist_ok=True)
        self.invalidate_json_state()

    def update_resources(
            self, resources: Union[Dict, Callable, PlacementGroupFactory]):
        """EXPERIMENTAL: Updates the resource requirements.

        Should only be called when the trial is not running.

        Raises:
            ValueError if trial status is running.
        """
        if self.status is Trial.RUNNING:
            raise ValueError("Cannot update resources while Trial is running.")
        if isinstance(resources, PlacementGroupFactory):
            self.placement_group_factory = resources
        elif callable(resources):
            self.placement_group_factory = PlacementGroupFactory(resources)
        else:
            self.resources = Resources(**resources)
            self.placement_group_factory = None

        if self.placement_group_factory and \
           not self.resources.has_placement_group:
            resource_kwargs = self.resources._asdict()
            resource_kwargs["has_placement_group"] = True
            self.resources = Resources(**resource_kwargs)

        self.invalidate_json_state()

    def set_runner(self, runner):
        self.runner = runner
        self.checkpoint_manager.delete = checkpoint_deleter(
            self._trainable_name(), runner)
        # No need to invalidate state cache: runner is not stored in json
        # self.invalidate_json_state()

    def set_location(self, location):
        """Sets the location of the trial."""
        self.location = location
        # No need to invalidate state cache: location is not stored in json
        # self.invalidate_json_state()

    def set_status(self, status):
        """Sets the status of the trial."""
        self.status = status
        if status == Trial.RUNNING:
            if self.start_time is None:
                self.start_time = time.time()
        self.invalidate_json_state()

    def set_config(self, config):
        self.config = config
        self.invalidate_json_state()

    def set_experiment_tag(self, experiment_tag):
        self.experiment_tag = experiment_tag
        self.invalidate_json_state()

    def write_error_log(self, error_msg):
        if error_msg and self.logdir:
            self.num_failures += 1
            self.error_file = os.path.join(self.logdir, "error.txt")
            with open(self.error_file, "a+") as f:
                f.write("Failure # {} (occurred at {})\n".format(
                    self.num_failures, date_str()))
                f.write(error_msg + "\n")
            self.error_msg = error_msg
        self.invalidate_json_state()

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
        return (self.checkpoint_freq and
                result.get(TRAINING_ITERATION, 0) % self.checkpoint_freq == 0)

    def has_checkpoint(self):
        return self.checkpoint.value is not None

    def clear_checkpoint(self):
        self.checkpoint.value = None
        self.restoring_from = None
        self.invalidate_json_state()

    def on_checkpoint(self, checkpoint):
        """Hook for handling checkpoints taken by the Trainable.

        Args:
            checkpoint (Checkpoint): Checkpoint taken.
        """
        self.checkpoint_manager.on_checkpoint(checkpoint)
        self.invalidate_json_state()

    def on_restore(self):
        """Handles restoration completion."""
        assert self.is_restoring
        self.last_result = self.restoring_from.result
        self.restoring_from = None
        self.invalidate_json_state()

    def should_recover(self):
        """Returns whether the trial qualifies for retrying.

        This is if the trial has not failed more than max_failures. Note this
        may return true even when there is no checkpoint, either because
        `self.checkpoint_freq` is `0` or because the trial failed before
        a checkpoint has been made.
        """
        return self.num_failures < self.max_failures or self.max_failures < 0

    def update_last_result(self, result, terminate=False):
        if self.experiment_tag:
            result.update(experiment_tag=self.experiment_tag)

        self.set_location(Location(result.get("node_ip"), result.get("pid")))
        self.last_result = result
        self.last_update_time = time.time()

        for metric, value in flatten_dict(result).items():
            if isinstance(value, Number):
                if metric not in self.metric_analysis:
                    self.metric_analysis[metric] = {
                        "max": value,
                        "min": value,
                        "avg": value,
                        "last": value
                    }
                    self.metric_n_steps[metric] = {}
                    for n in self.n_steps:
                        key = "last-{:d}-avg".format(n)
                        self.metric_analysis[metric][key] = value
                        # Store n as string for correct restore.
                        self.metric_n_steps[metric][str(n)] = deque(
                            [value], maxlen=n)
                else:
                    step = result["training_iteration"] or 1
                    self.metric_analysis[metric]["max"] = max(
                        value, self.metric_analysis[metric]["max"])
                    self.metric_analysis[metric]["min"] = min(
                        value, self.metric_analysis[metric]["min"])
                    self.metric_analysis[metric]["avg"] = 1 / step * (
                        value +
                        (step - 1) * self.metric_analysis[metric]["avg"])
                    self.metric_analysis[metric]["last"] = value

                    for n in self.n_steps:
                        key = "last-{:d}-avg".format(n)
                        self.metric_n_steps[metric][str(n)].append(value)
                        self.metric_analysis[metric][key] = sum(
                            self.metric_n_steps[metric][str(n)]) / len(
                                self.metric_n_steps[metric][str(n)])
        self.invalidate_json_state()

    def get_trainable_cls(self):
        return get_trainable_cls(self.trainable_name)

    def is_finished(self):
        return self.status in [Trial.ERROR, Trial.TERMINATED]

    @property
    def is_restoring(self):
        return self.restoring_from is not None

    @property
    def is_saving(self):
        return self.saving_to is not None

    def __repr__(self):
        return self._trainable_name(include_trial_id=True)

    def __str__(self):
        return self._trainable_name(include_trial_id=True)

    def _trainable_name(self, include_trial_id=False):
        """Combines ``env`` with ``trainable_name`` and ``trial_id``.

        Can be overridden with a custom string creator.
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
        if include_trial_id:
            identifier += "_" + self.trial_id
        return identifier.replace("/", "_")

    def _generate_dirname(self):
        if self.custom_dirname:
            generated_dirname = self.custom_dirname
        else:
            generated_dirname = f"{str(self)}_{self.experiment_tag}"
            generated_dirname = generated_dirname[:MAX_LEN_IDENTIFIER]
            generated_dirname += f"_{date_str()}"
        return generated_dirname.replace("/", "_")

    def invalidate_json_state(self):
        self._state_valid = False

    def get_json_state(self) -> str:
        if not self._state_json or not self._state_valid:
            json_state = json.dumps(
                self.__getstate__(), indent=2, cls=TuneFunctionEncoder)
            self._state_json = json_state
            self._state_valid = True
        return self._state_json

    def __getstate__(self):
        """Memento generator for Trial.

        Sets RUNNING trials to PENDING.
        Note this can only occur if the trial holds a PERSISTENT checkpoint.
        """
        assert self.checkpoint.storage == Checkpoint.PERSISTENT, (
            "Checkpoint must not be in-memory.")
        state = self.__dict__.copy()
        state["resources"] = resources_to_json(self.resources)

        for key in self._nonjson_fields:
            state[key] = binary_to_hex(cloudpickle.dumps(state.get(key)))

        state["runner"] = None
        state["location"] = Location()
        # Avoid waiting for events that will never occur on resume.
        state["restoring_from"] = None
        state["saving_to"] = None

        state["_state_json"] = None
        state["_state_valid"] = False

        return copy.deepcopy(state)

    def __setstate__(self, state):
        state["resources"] = json_to_resources(state["resources"])

        if state["status"] == Trial.RUNNING:
            state["status"] = Trial.PENDING
        for key in self._nonjson_fields:
            state[key] = cloudpickle.loads(hex_to_binary(state[key]))

        self.__dict__.update(state)
        validate_trainable(self.trainable_name)
        self.init_logdir()  # Create logdir if it does not exist
