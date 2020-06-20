import ray.cloudpickle as cloudpickle
from collections import deque
import copy
from datetime import datetime
import logging
import platform
import shutil
import uuid
import time
import tempfile
import os
from numbers import Number
from ray.tune import TuneError
from ray.tune.checkpoint_manager import Checkpoint, CheckpointManager
from ray.tune.durable_trainable import DurableTrainable
from ray.tune.logger import pretty_print, UnifiedLogger
# NOTE(rkn): We import ray.tune.registry here instead of importing the names we
# need because there are cyclic imports that may cause specific names to not
# have been defined yet. See https://github.com/ray-project/ray/issues/1716.
from ray.tune.registry import get_trainable_cls, validate_trainable
from ray.tune.result import DEFAULT_RESULTS_DIR, DONE, TRAINING_ITERATION
from ray.tune.resources import Resources, json_to_resources, resources_to_json
from ray.tune.trainable import TrainableUtil
from ray.tune.utils import flatten_dict
from ray.utils import binary_to_hex, hex_to_binary

DEBUG_PRINT_INTERVAL = 5
MAX_LEN_IDENTIFIER = int(os.environ.get("MAX_LEN_IDENTIFIER", 130))
logger = logging.getLogger(__name__)


def date_str():
    return datetime.today().strftime("%Y-%m-%d_%H-%M-%S")


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
                 loggers=None,
                 sync_to_driver_fn=None,
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
        self.stopping_criterion = stopping_criterion or {}
        self.loggers = loggers
        self.sync_to_driver_fn = sync_to_driver_fn
        self.verbose = True
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
        self.result_logger = None
        self.last_debug = 0
        self.error_file = None
        self.error_msg = None
        self.custom_trial_name = None

        # Checkpointing fields
        self.saving_to = None
        if remote_checkpoint_dir:
            self.remote_checkpoint_dir_prefix = remote_checkpoint_dir
        else:
            self.remote_checkpoint_dir_prefix = None
        self.checkpoint_freq = checkpoint_freq
        self.checkpoint_at_end = checkpoint_at_end
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
            "loggers",
            "sync_to_driver_fn",
            "results",
            "best_result",
            "param_config",
            "extra_arg",
        ]
        if trial_name_creator:
            self.custom_trial_name = trial_name_creator(self)

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

    @classmethod
    def create_logdir(cls, identifier, local_dir):
        local_dir = os.path.expanduser(local_dir)
        os.makedirs(local_dir, exist_ok=True)
        return tempfile.mkdtemp(
            prefix="{}_{}".format(identifier[:MAX_LEN_IDENTIFIER], date_str()),
            dir=local_dir)

    def init_logger(self):
        """Init logger."""
        if not self.result_logger:
            if not self.logdir:
                self.logdir = Trial.create_logdir(
                    self._trainable_name() + "_" + self.experiment_tag,
                    self.local_dir)
            else:
                os.makedirs(self.logdir, exist_ok=True)

            self.result_logger = UnifiedLogger(
                self.config,
                self.logdir,
                trial=self,
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

    def set_runner(self, runner):
        self.runner = runner
        self.checkpoint_manager.delete = checkpoint_deleter(
            self._trainable_name(), runner)

    def set_location(self, location):
        """Sets the location of the trial."""
        self.location = location

    def set_status(self, status):
        """Sets the status of the trial."""
        self.status = status
        if status == Trial.RUNNING:
            if self.start_time is None:
                self.start_time = time.time()

    def close_logger(self):
        """Closes logger."""
        if self.result_logger:
            self.result_logger.close()
            self.result_logger = None

    def write_error_log(self, error_msg):
        if error_msg and self.logdir:
            self.num_failures += 1
            self.error_file = os.path.join(self.logdir, "error.txt")
            with open(self.error_file, "a+") as f:
                f.write("Failure # {} (occurred at {})\n".format(
                    self.num_failures, date_str()))
                f.write(error_msg + "\n")
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
        return (self.checkpoint_freq and
                result.get(TRAINING_ITERATION, 0) % self.checkpoint_freq == 0)

    def has_checkpoint(self):
        return self.checkpoint.value is not None

    def clear_checkpoint(self):
        self.checkpoint.value = None
        self.restoring_from = None

    def on_checkpoint(self, checkpoint):
        """Hook for handling checkpoints taken by the Trainable.

        Args:
            checkpoint (Checkpoint): Checkpoint taken.
        """
        if checkpoint.storage == Checkpoint.MEMORY:
            self.checkpoint_manager.on_checkpoint(checkpoint)
            return
        if self.sync_on_checkpoint:
            try:
                # Wait for any other syncs to finish. We need to sync again
                # after this to handle checkpoints taken mid-sync.
                self.result_logger.wait()
            except TuneError as e:
                # Errors occurring during this wait are not fatal for this
                # checkpoint, so it should just be logged.
                logger.error(
                    "Trial %s: An error occurred during the "
                    "checkpoint pre-sync wait - %s", self, str(e))
            # Force sync down and wait before tracking the new checkpoint.
            try:
                if self.result_logger.sync_down():
                    self.result_logger.wait()
                else:
                    logger.error(
                        "Trial %s: Checkpoint sync skipped. "
                        "This should not happen.", self)
            except TuneError as e:
                if issubclass(self.get_trainable_cls(), DurableTrainable):
                    # Even though rsync failed the trainable can restore
                    # from remote durable storage.
                    logger.error("Trial %s: Sync error - %s", self, str(e))
                else:
                    # If the trainable didn't have remote storage to upload
                    # to then this checkpoint may have been lost, so we
                    # shouldn't track it with the checkpoint_manager.
                    raise e
            if not issubclass(self.get_trainable_cls(), DurableTrainable):
                if not os.path.exists(checkpoint.value):
                    raise TuneError("Trial {}: Checkpoint path {} not "
                                    "found after successful sync down.".format(
                                        self, checkpoint.value))
        self.checkpoint_manager.on_checkpoint(checkpoint)

    def on_restore(self):
        """Handles restoration completion."""
        assert self.is_restoring
        self.last_result = self.restoring_from.result
        self.restoring_from = None

    def should_recover(self):
        """Returns whether the trial qualifies for retrying.

        This is if the trial has not failed more than max_failures. Note this
        may return true even when there is no checkpoint, either because
        `self.checkpoint_freq` is `0` or because the trial failed before
        a checkpoint has been made.
        """
        return self.num_failures < self.max_failures or self.max_failures < 0

    def update_last_result(self, result, terminate=False):
        result.update(trial_id=self.trial_id, done=terminate)
        if self.experiment_tag:
            result.update(experiment_tag=self.experiment_tag)
        if self.verbose and (terminate or time.time() - self.last_debug >
                             DEBUG_PRINT_INTERVAL):
            print("Result for {}:".format(self))
            print("  {}".format(pretty_print(result).replace("\n", "\n  ")))
            self.last_debug = time.time()
        self.set_location(Location(result.get("node_ip"), result.get("pid")))
        self.last_result = result
        self.last_update_time = time.time()
        self.result_logger.on_result(self.last_result)

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

    def get_trainable_cls(self):
        return get_trainable_cls(self.trainable_name)

    def set_verbose(self, verbose):
        self.verbose = verbose

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

    def __getstate__(self):
        """Memento generator for Trial.

        Sets RUNNING trials to PENDING, and flushes the result logger.
        Note this can only occur if the trial holds a PERSISTENT checkpoint.
        """
        assert self.checkpoint.storage == Checkpoint.PERSISTENT, (
            "Checkpoint must not be in-memory.")
        state = self.__dict__.copy()
        state["resources"] = resources_to_json(self.resources)

        for key in self._nonjson_fields:
            state[key] = binary_to_hex(cloudpickle.dumps(state.get(key)))

        state["runner"] = None
        state["result_logger"] = None
        # Avoid waiting for events that will never occur on resume.
        state["resuming_from"] = None
        state["saving_to"] = None
        if self.result_logger:
            self.result_logger.flush(sync_down=False)
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
        validate_trainable(self.trainable_name)
        if logger_started:
            self.init_logger()
