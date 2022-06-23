from collections import deque
import copy
import json
import logging
from numbers import Number
import os
from pathlib import Path
import platform
import re
import shutil
import time
from typing import Dict, Optional, Sequence, Union, Callable, List
import uuid

import ray
import ray.cloudpickle as cloudpickle
from ray.exceptions import RayActorError, RayTaskError
from ray.tune import TuneError
from ray.tune.execution.checkpoint_manager import _CheckpointManager

# NOTE(rkn): We import ray.tune.registry here instead of importing the names we
# need because there are cyclic imports that may cause specific names to not
# have been defined yet. See https://github.com/ray-project/ray/issues/1716.
from ray.tune.registry import get_trainable_cls, validate_trainable
from ray.tune.result import (
    DEFAULT_RESULTS_DIR,
    DONE,
    NODE_IP,
    PID,
    TRAINING_ITERATION,
    TRIAL_ID,
    DEBUG_METRICS,
)
from ray.tune.resources import Resources
from ray.tune.syncer import Syncer
from ray.tune.execution.placement_groups import (
    PlacementGroupFactory,
    resource_dict_to_pg_factory,
)
from ray.tune.utils.serialization import TuneFunctionEncoder
from ray.tune.trainable.util import TrainableUtil
from ray.tune.utils import date_str, flatten_dict
from ray.util.annotations import DeveloperAPI
from ray.util.debug import log_once
from ray._private.utils import binary_to_hex, hex_to_binary
from ray.util.ml_utils.checkpoint_manager import _TrackedCheckpoint, CheckpointStorage

DEBUG_PRINT_INTERVAL = 5
logger = logging.getLogger(__name__)


class _Location:
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


@DeveloperAPI
class ExportFormat:
    """Describes the format to import/export the trial Trainable.

    This may correspond to different file formats based on the
    Trainable implementation.
    """

    CHECKPOINT = "checkpoint"
    MODEL = "model"
    ONNX = "onnx"
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
                ExportFormat.CHECKPOINT,
                ExportFormat.MODEL,
                ExportFormat.ONNX,
                ExportFormat.H5,
            ]:
                raise TuneError("Unsupported import/export format: " + formats[i])


class _CheckpointDeleter:
    """Checkpoint deleter callback for a runner."""

    def __init__(self, trial_id, runner):
        self.trial_id = trial_id
        self.runner = runner

    def __call__(self, checkpoint: _TrackedCheckpoint):
        """Requests checkpoint deletion asynchronously.

        Args:
            checkpoint: Checkpoint to delete.
        """
        if not self.runner:
            return

        if (
            checkpoint.storage_mode == CheckpointStorage.PERSISTENT
            and checkpoint.dir_or_data
        ):
            checkpoint_path = checkpoint.dir_or_data

            logger.debug(
                "Trial %s: Deleting checkpoint %s", self.trial_id, checkpoint_path
            )

            # TODO(ujvl): Batch remote deletes.
            # We first delete the remote checkpoint. If it is on the same
            # node as the driver, it will also remove the local copy.
            ray.get(self.runner.delete_checkpoint.remote(checkpoint_path))

            # Delete local copy, if any exists.
            if os.path.exists(checkpoint_path):
                try:
                    checkpoint_dir = TrainableUtil.find_checkpoint_dir(checkpoint_path)
                    shutil.rmtree(checkpoint_dir)
                except FileNotFoundError:
                    logger.debug("Local checkpoint dir not found during deletion.")


class _TrialInfo:
    """Serializable struct for holding information for a Trial.

    Attributes:
        trial_name: String name of the current trial.
        trial_id: trial_id of the trial
        trial_resources: resources used by trial.
    """

    def __init__(self, trial: "Trial"):
        self._trial_name = str(trial)
        self._trial_id = trial.trial_id
        self._trial_resources = trial.placement_group_factory

    @property
    def trial_name(self):
        return self._trial_name

    @property
    def trial_id(self):
        return self._trial_id

    @property
    def trial_resources(self) -> Union[Resources, PlacementGroupFactory]:
        return self._trial_resources

    @trial_resources.setter
    def trial_resources(self, new_resources: Union[Resources, PlacementGroupFactory]):
        self._trial_resources = new_resources


def create_unique_logdir_name(root: str, relative_logdir: str) -> str:
    candidate = Path(root).expanduser().joinpath(relative_logdir)
    if candidate.exists():
        relative_logdir_old = relative_logdir
        relative_logdir += "_" + uuid.uuid4().hex[:4]
        logger.info(
            f"Creating a new dirname {relative_logdir} because "
            f"trial dirname '{relative_logdir_old}' already exists."
        )
    return relative_logdir


def _to_pg_factory(
    resources: Optional[Resources],
    placement_group_factory: Optional[PlacementGroupFactory],
) -> PlacementGroupFactory:
    """Outputs resources requirement in the form of PGF.

    In case that `placement_group_factory` is None, `resources` will be
    converted to PGF. If this is unsuccessful, an error will be raised.

    """
    if not placement_group_factory:
        if not resources:
            resources = Resources(cpu=1, gpu=0)
        placement_group_factory = resource_dict_to_pg_factory(resources)
    return placement_group_factory


@DeveloperAPI
class Trial:
    """A trial object holds the state for one model training run.

    Trials are themselves managed by the TrialRunner class, which implements
    the event loop for submitting trial runs to a Ray cluster.

    Trials start in the PENDING state, and transition to RUNNING once started.
    On error it transitions to ERROR, otherwise TERMINATED on success.

    There are resources allocated to each trial. These should be specified
    using ``PlacementGroupFactory``.

    Attributes:
        trainable_name: Name of the trainable object to be executed.
        config: Provided configuration dictionary with evaluated params.
        trial_id: Unique identifier for the trial.
        local_dir: ``local_dir`` as passed to ``tune.run`` joined
            with the name of the experiment.
        logdir: Directory where the trial logs are saved.
        relative_logdir: Same as ``logdir``, but relative to the parent of
            the ``local_dir`` (equal to ``local_dir`` argument passed
            to ``tune.run``).
        evaluated_params: Evaluated parameters by search algorithm,
        experiment_tag: Identifying trial name to show in the console
        status: One of PENDING, RUNNING, PAUSED, TERMINATED, ERROR/
        error_file: Path to the errors that this trial has raised.

    """

    _nonjson_fields = [
        "results",
        "best_result",
        "param_config",
        "extra_arg",
        "placement_group_factory",
    ]

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    TERMINATED = "TERMINATED"
    ERROR = "ERROR"

    def __init__(
        self,
        trainable_name: str,
        config: Optional[Dict] = None,
        trial_id: Optional[str] = None,
        local_dir: Optional[str] = DEFAULT_RESULTS_DIR,
        evaluated_params: Optional[Dict] = None,
        experiment_tag: str = "",
        resources: Optional[Resources] = None,
        placement_group_factory: Optional[PlacementGroupFactory] = None,
        stopping_criterion: Optional[Dict[str, float]] = None,
        remote_checkpoint_dir: Optional[str] = None,
        custom_syncer: Optional[Syncer] = None,
        checkpoint_freq: int = 0,
        checkpoint_at_end: bool = False,
        sync_on_checkpoint: bool = True,
        keep_checkpoints_num: Optional[int] = None,
        checkpoint_score_attr: str = TRAINING_ITERATION,
        export_formats: Optional[List[str]] = None,
        restore_path: Optional[str] = None,
        trial_name_creator: Optional[Callable[["Trial"], str]] = None,
        trial_dirname_creator: Optional[Callable[["Trial"], str]] = None,
        log_to_file: Optional[str] = None,
        max_failures: int = 0,
        stub: bool = False,
        _setup_default_resource: bool = True,
    ):
        """Initialize a new trial.

        The args here take the same meaning as the command line flags defined
        in ray.tune.experiment.config_parser.

        Args:
            _setup_default_resource: Whether to set up default resources.
                When initializing trials from checkpoints, this field is set to false,
                so that setting up default resources can be delayed till after
                ``trial.config`` is loaded from checkpoints.
        """
        # If this is set, trainables are not validated or looked up.
        # This can be used e.g. to initialize Trial objects from checkpoints
        # without loading the trainable first.
        self.stub = stub

        if not self.stub:
            validate_trainable(trainable_name)
        # Trial config
        self.trainable_name = trainable_name
        self.trial_id = Trial.generate_id() if trial_id is None else trial_id
        self.config = config or {}
        self.local_dir = local_dir  # This remains unexpanded for syncing.

        # Parameters that Tune varies across searches.
        self.evaluated_params = evaluated_params or {}
        self.experiment_tag = experiment_tag
        self.location = _Location()
        trainable_cls = self.get_trainable_cls()
        if trainable_cls and _setup_default_resource:
            default_resources = trainable_cls.default_resource_request(self.config)

            # If Trainable returns resources, do not allow manual override via
            # `resources_per_trial` by the user.
            if default_resources:
                if resources or placement_group_factory:
                    raise ValueError(
                        "Resources for {} have been automatically set to {} "
                        "by its `default_resource_request()` method. Please "
                        "clear the `resources_per_trial` option.".format(
                            trainable_cls, default_resources
                        )
                    )

                if isinstance(default_resources, PlacementGroupFactory):
                    placement_group_factory = default_resources
                    resources = None
                else:
                    placement_group_factory = None
                    resources = default_resources

        self.placement_group_factory = _to_pg_factory(
            resources, placement_group_factory
        )

        self.stopping_criterion = stopping_criterion or {}

        self.log_to_file = log_to_file
        # Make sure `stdout_file, stderr_file = Trial.log_to_file` works
        if (
            not self.log_to_file
            or not isinstance(self.log_to_file, Sequence)
            or not len(self.log_to_file) == 2
        ):
            self.log_to_file = (None, None)

        self.max_failures = max_failures

        # Local trial state that is updated during the run
        self._last_result = {}
        self._default_result_or_future: Union[ray.ObjectRef, dict, None] = None
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
        self.relative_logdir = None
        self.runner = None
        self.last_debug = 0
        self.error_file = None
        self.pickled_error_file = None
        self.trial_name_creator = trial_name_creator
        self.trial_dirname_creator = trial_dirname_creator
        self.custom_trial_name = None
        self.custom_dirname = None

        # Checkpointing fields
        self.saving_to = None
        if remote_checkpoint_dir:
            self.remote_checkpoint_dir_prefix = remote_checkpoint_dir
        else:
            self.remote_checkpoint_dir_prefix = None

        if custom_syncer == "auto" or not isinstance(custom_syncer, Syncer):
            custom_syncer = None
        self.custom_syncer = custom_syncer

        self.checkpoint_freq = checkpoint_freq
        self.checkpoint_at_end = checkpoint_at_end
        self.keep_checkpoints_num = keep_checkpoints_num
        self.checkpoint_score_attr = checkpoint_score_attr
        self.sync_on_checkpoint = sync_on_checkpoint
        self.checkpoint_manager = _CheckpointManager(
            keep_checkpoints_num,
            checkpoint_score_attr,
            delete_fn=_CheckpointDeleter(self._trainable_name(), self.runner),
        )

        # Restoration fields
        self.restore_path = restore_path
        self.restoring_from = None
        self.num_failures = 0

        # AutoML fields
        self.results = None
        self.best_result = None
        self.param_config = None
        self.extra_arg = None

        if trial_name_creator:
            self.custom_trial_name = trial_name_creator(self)

        if trial_dirname_creator:
            self.custom_dirname = trial_dirname_creator(self)
            if os.path.sep in self.custom_dirname:
                raise ValueError(
                    f"Trial dirname must not contain '/'. Got {self.custom_dirname}"
                )

        self._state_json = None
        self._state_valid = False

    def _get_default_result_or_future(self) -> Optional[dict]:
        """Calls ray.get on self._default_result_or_future and assigns back.

        Returns None in case of exceptions.
        Will also set the trial location if runner is set.
        """
        if self._default_result_or_future and isinstance(
            self._default_result_or_future, ray.ObjectRef
        ):
            try:
                self._default_result_or_future = ray.get(self._default_result_or_future)
            except RayActorError:  # error during initialization
                self._default_result_or_future = None
        if self._default_result_or_future and self.runner:
            self.set_location(
                _Location(
                    self._default_result_or_future.get(NODE_IP),
                    self._default_result_or_future.get(PID),
                )
            )
        return self._default_result_or_future

    @property
    def last_result(self) -> dict:
        # The logic in here is as follows:
        # 1. If the trial has reported at least once, last_result would have
        #    been set and therefore would not be empty. We can just return it.
        # 2. If the trial has not reported at least once but we have the
        #    future for the default results dict, (obtained through
        #    Trainable.get_auto_filled_metrics), we get that future
        #    and return it.
        # 3. In the worst case where we have nothing, we just set the
        #    trial_id and return that.
        result = self._last_result
        if not {k for k in result if k != TRIAL_ID}:
            self._get_default_result_or_future()
            result = self._default_result_or_future or result
        result.setdefault(TRIAL_ID, self.trial_id)
        return result

    @last_result.setter
    def last_result(self, val: dict):
        self._last_result = val

    @property
    def logdir(self):
        if not self.relative_logdir:
            return None
        return str(Path(self.local_dir).joinpath(self.relative_logdir))

    @logdir.setter
    def logdir(self, logdir):
        relative_logdir = Path(logdir).relative_to(self.local_dir)
        if ".." in str(relative_logdir):
            raise ValueError(
                f"The `logdir` points to a directory outside the trial's `local_dir` "
                f"({self.local_dir}), which is unsupported. Use a logdir within the "
                f"local directory instead. Got: {logdir}"
            )
        if log_once("logdir_setter"):
            logger.warning(
                "Deprecated. In future versions only the relative logdir "
                "will be used and calling logdir will raise an error."
            )
        self.relative_logdir = relative_logdir

    @property
    def has_reported_at_least_once(self) -> bool:
        return bool(self._last_result)

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
        if checkpoint.dir_or_data is None:
            checkpoint = _TrackedCheckpoint(
                dir_or_data=self.restore_path,
                storage_mode=CheckpointStorage.PERSISTENT,
            )
        return checkpoint

    @classmethod
    def generate_id(cls):
        return str(uuid.uuid1().hex)[:8]

    @property
    def remote_checkpoint_dir(self):
        """This is the **per trial** remote checkpoint dir.

        This is different from **per experiment** remote checkpoint dir.
        """
        assert self.logdir, "Trial {}: logdir not initialized.".format(self)
        if not self.remote_checkpoint_dir_prefix:
            return None
        return os.path.join(self.remote_checkpoint_dir_prefix, self.relative_logdir)

    @property
    def uses_cloud_checkpointing(self):
        return bool(self.remote_checkpoint_dir)

    def reset(self):
        # If there is `default_resource_request` associated with the trainable,
        # clear `resources` and `placement_group_factory`.
        # This is mainly relevant for RLlib tuning jobs, where we save users
        # of the trouble to specify the resources themselves by having some
        # default resources for popular RLlib algorithms.
        trainable_cls = self.get_trainable_cls()
        clear_resources = trainable_cls and trainable_cls.default_resource_request(
            self.config
        )
        placement_group_factory = (
            self.placement_group_factory if not clear_resources else None
        )

        return Trial(
            self.trainable_name,
            config=self.config,
            trial_id=None,
            local_dir=self.local_dir,
            evaluated_params=self.evaluated_params,
            experiment_tag=self.experiment_tag,
            resources=None,
            placement_group_factory=placement_group_factory,
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
            trial_dirname_creator=self.trial_dirname_creator,
            log_to_file=self.log_to_file,
            max_failures=self.max_failures,
        )

    def init_logdir(self):
        """Init logdir."""
        if not self.relative_logdir:
            self.relative_logdir = create_unique_logdir_name(
                self.local_dir, self._generate_dirname()
            )
        assert self.logdir
        logdir_path = Path(self.logdir)
        logdir_path.mkdir(parents=True, exist_ok=True)

        self.invalidate_json_state()

    def update_resources(self, resources: Union[Dict, PlacementGroupFactory]):
        """EXPERIMENTAL: Updates the resource requirements.

        Should only be called when the trial is not running.

        Raises:
            ValueError if trial status is running.
        """
        if self.status is Trial.RUNNING:
            raise ValueError("Cannot update resources while Trial is running.")

        placement_group_factory = None
        if isinstance(resources, PlacementGroupFactory):
            placement_group_factory = resources
        else:
            resources = Resources(**resources)

        self.placement_group_factory = _to_pg_factory(
            resources, placement_group_factory
        )

        self.invalidate_json_state()

    def set_runner(self, runner):
        self.runner = runner
        if runner:
            # Do not block here, the result will be gotten when last_result
            # property is accessed
            self._default_result_or_future = runner.get_auto_filled_metrics.remote(
                debug_metrics_only=True
            )
        self.checkpoint_manager.set_delete_fn(
            _CheckpointDeleter(self._trainable_name(), runner)
        )
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

    def write_error_log(self, exc: Optional[Union[TuneError, RayTaskError]] = None):
        if exc and self.logdir:
            self.num_failures += 1
            self.error_file = os.path.join(self.logdir, "error.txt")
            if exc and isinstance(exc, RayTaskError):
                # Piping through the actual error to result grid.
                self.pickled_error_file = os.path.join(self.logdir, "error.pkl")
                with open(self.pickled_error_file, "wb") as f:
                    cloudpickle.dump(exc, f)
            with open(self.error_file, "a+") as f:
                f.write(
                    "Failure # {} (occurred at {})\n".format(
                        self.num_failures, date_str()
                    )
                )
                f.write(str(exc) + "\n")
        self.invalidate_json_state()

    def should_stop(self, result):
        """Whether the given result meets this trial's stopping criteria."""
        if result.get(DONE):
            return True

        for criteria, stop_value in self.stopping_criterion.items():
            if criteria not in result:
                raise TuneError(
                    "Stopping criteria {} not provided in result dict. Keys "
                    "are {}.".format(criteria, list(result.keys()))
                )
            elif isinstance(criteria, dict):
                raise ValueError(
                    "Stopping criteria is now flattened by default. "
                    "Use forward slashes to nest values `key1/key2/key3`."
                )
            elif result[criteria] >= stop_value:
                return True
        return False

    def should_checkpoint(self):
        """Whether this trial is due for checkpointing."""
        result = self.last_result or {}
        if result.get(DONE) and self.checkpoint_at_end:
            return True
        return (
            self.checkpoint_freq
            and result.get(TRAINING_ITERATION, 0) % self.checkpoint_freq == 0
        )

    def has_checkpoint(self):
        return self.checkpoint.dir_or_data is not None

    def clear_checkpoint(self):
        self.checkpoint.dir_or_data = None
        self.restoring_from = None
        self.invalidate_json_state()

    def on_checkpoint(self, checkpoint: _TrackedCheckpoint):
        """Hook for handling checkpoints taken by the Trainable.

        Args:
            checkpoint: Checkpoint taken.
        """
        self.checkpoint_manager.on_checkpoint(checkpoint)
        self.invalidate_json_state()

    def on_restore(self):
        """Handles restoration completion."""
        assert self.is_restoring
        self.last_result = self.restoring_from.metrics
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

    def update_last_result(self, result):
        if self.experiment_tag:
            result.update(experiment_tag=self.experiment_tag)

        self.set_location(_Location(result.get(NODE_IP), result.get(PID)))
        self.last_result = result
        self.last_update_time = time.time()

        metric_result = self.last_result.copy()
        for remove_metric in DEBUG_METRICS:
            metric_result.pop(remove_metric, None)

        for metric, value in flatten_dict(metric_result).items():
            if isinstance(value, Number):
                if metric not in self.metric_analysis:
                    self.metric_analysis[metric] = {
                        "max": value,
                        "min": value,
                        "avg": value,
                        "last": value,
                    }
                    self.metric_n_steps[metric] = {}
                    for n in self.n_steps:
                        key = "last-{:d}-avg".format(n)
                        self.metric_analysis[metric][key] = value
                        # Store n as string for correct restore.
                        self.metric_n_steps[metric][str(n)] = deque([value], maxlen=n)
                else:
                    step = result["training_iteration"] or 1
                    self.metric_analysis[metric]["max"] = max(
                        value, self.metric_analysis[metric]["max"]
                    )
                    self.metric_analysis[metric]["min"] = min(
                        value, self.metric_analysis[metric]["min"]
                    )
                    self.metric_analysis[metric]["avg"] = (
                        1
                        / step
                        * (value + (step - 1) * self.metric_analysis[metric]["avg"])
                    )
                    self.metric_analysis[metric]["last"] = value

                    for n in self.n_steps:
                        key = "last-{:d}-avg".format(n)
                        self.metric_n_steps[metric][str(n)].append(value)
                        self.metric_analysis[metric][key] = sum(
                            self.metric_n_steps[metric][str(n)]
                        ) / len(self.metric_n_steps[metric][str(n)])
        self.invalidate_json_state()

    def get_trainable_cls(self):
        if self.stub:
            return None
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
            MAX_LEN_IDENTIFIER = int(os.environ.get("TUNE_MAX_LEN_IDENTIFIER", "130"))
            generated_dirname = f"{str(self)}_{self.experiment_tag}"
            generated_dirname = generated_dirname[:MAX_LEN_IDENTIFIER]
            generated_dirname += f"_{date_str()}"
        # This is the file path used by rsync. ['/', '(', ')'] are not allowed.
        return re.sub("[/()]", "_", generated_dirname)

    def invalidate_json_state(self):
        self._state_valid = False

    def get_json_state(self) -> str:
        if not self._state_json or not self._state_valid:
            json_state = json.dumps(
                self.__getstate__(), indent=2, cls=TuneFunctionEncoder
            )
            self._state_json = json_state
            self._state_valid = True
        return self._state_json

    def __getstate__(self):
        """Memento generator for Trial.

        Sets RUNNING trials to PENDING.
        Note this can only occur if the trial holds a PERSISTENT checkpoint.
        """
        state = self.__dict__.copy()

        for key in self._nonjson_fields:
            state[key] = binary_to_hex(cloudpickle.dumps(state.get(key)))

        state["runner"] = None
        state["location"] = _Location()
        # Avoid waiting for events that will never occur on resume.
        state["restoring_from"] = None
        state["saving_to"] = None

        state["_state_json"] = None
        state["_state_valid"] = False
        state["_default_result_or_future"] = None

        return copy.deepcopy(state)

    def __setstate__(self, state):

        if state["status"] == Trial.RUNNING:
            state["status"] = Trial.PENDING
        for key in self._nonjson_fields:
            if key in state:
                state[key] = cloudpickle.loads(hex_to_binary(state[key]))

        # Ensure that stub doesn't get overriden
        stub = state.pop("stub", True)
        self.__dict__.update(state)
        self.stub = stub or getattr(self, "stub", False)

        if not self.stub:
            validate_trainable(self.trainable_name)

        assert self.placement_group_factory

        # Avoid creating logdir in client mode for returned trial results,
        # since the dir might not be creatable locally.
        # TODO(ekl) this is kind of a hack.
        if not ray.util.client.ray.is_connected():
            self.init_logdir()  # Create logdir if it does not exist
