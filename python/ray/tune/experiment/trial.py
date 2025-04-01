import copy
import json
import logging
import os
import platform
import re
import time
import uuid
from contextlib import contextmanager
from functools import partial
from numbers import Number
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

import ray
import ray.cloudpickle as cloudpickle
from ray._private.utils import binary_to_hex, hex_to_binary
from ray.air.constants import (
    EXPR_ERROR_FILE,
    EXPR_ERROR_PICKLE_FILE,
    TRAINING_ITERATION,
)
from ray.exceptions import RayActorError, RayTaskError
from ray.tune import Checkpoint, CheckpointConfig
from ray.train._internal.checkpoint_manager import _CheckpointManager
from ray.train._internal.session import _FutureTrainingResult, _TrainingResult
from ray.train._internal.storage import StorageContext, _exists_at_fs_path
from ray.train.constants import (
    RAY_CHDIR_TO_TRIAL_DIR,
    RAY_TRAIN_COUNT_PREEMPTION_AS_FAILURE,
)
from ray.tune.error import TuneError
from ray.tune.execution.placement_groups import (
    PlacementGroupFactory,
    resource_dict_to_pg_factory,
)
from ray.tune.logger import NoopLogger

# NOTE(rkn): We import ray.tune.registry here instead of importing the names we
# need because there are cyclic imports that may cause specific names to not
# have been defined yet. See https://github.com/ray-project/ray/issues/1716.
from ray.tune.registry import get_trainable_cls, validate_trainable
from ray.tune.result import (
    DEBUG_METRICS,
    DONE,
    NODE_IP,
    PID,
    STDERR_FILE,
    STDOUT_FILE,
    TRIAL_ID,
    TRIAL_INFO,
)
from ray.tune.trainable.metadata import _TrainingRunMetadata
from ray.tune.utils import date_str, flatten_dict
from ray.tune.utils.serialization import TuneFunctionDecoder, TuneFunctionEncoder
from ray.util import log_once
from ray.util.annotations import Deprecated, DeveloperAPI

DEBUG_PRINT_INTERVAL = 5
_DEFAULT_WIN_MAX_PATH_LENGTH = 260
TRIAL_STATE_FILENAME = "trial_metadata.json"


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
            ValueError: if the format is unknown.
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
        self._experiment_name = trial.experiment_dir_name

    @property
    def experiment_name(self):
        return self._experiment_name

    @property
    def trial_name(self):
        return self._trial_name

    @property
    def trial_id(self):
        return self._trial_id

    @property
    def trial_resources(self) -> PlacementGroupFactory:
        return self._trial_resources

    @trial_resources.setter
    def trial_resources(self, new_resources: PlacementGroupFactory):
        self._trial_resources = new_resources


class _TemporaryTrialState:
    """Temporary trial state.

    Values saved here should not be restored on resume.
    """

    def __init__(self):
        self.location = _Location()

        self.ray_actor: Optional[ray.actor.ActorHandle] = None

        self.saving_to: Optional[_FutureTrainingResult] = None
        self.restoring_from: Optional[_TrainingResult] = None

        self.num_restore_failures: int = 0

    def __getstate__(self):
        return {}


def _get_max_path_length() -> int:
    if hasattr(os, "pathconf"):
        return os.pathconf("/", "PC_PATH_MAX")
    # Windows
    return _DEFAULT_WIN_MAX_PATH_LENGTH


def _create_unique_logdir_name(root: str, relative_logdir: str) -> str:
    candidate = Path(root).expanduser().joinpath(relative_logdir)
    if candidate.exists():
        relative_logdir_old = relative_logdir
        relative_logdir += "_" + uuid.uuid4().hex[:4]
        logger.info(
            f"Creating a new dirname {relative_logdir} because "
            f"trial dirname '{relative_logdir_old}' already exists."
        )
    return relative_logdir


def _noop_logger_creator(config: Dict[str, Any], logdir: str):
    # Upon remote process setup, record the actor's original working dir before
    # changing to the Tune logdir
    os.environ.setdefault("TUNE_ORIG_WORKING_DIR", os.getcwd())

    os.makedirs(logdir, exist_ok=True)

    if bool(int(os.environ.get(RAY_CHDIR_TO_TRIAL_DIR, "1"))):
        # Set the working dir to the trial directory in the remote process,
        # for user file writes
        if not ray._private.worker._mode() == ray._private.worker.LOCAL_MODE:
            os.chdir(logdir)

    return NoopLogger(config, logdir)


def _get_trainable_kwargs(trial: "Trial") -> Dict[str, Any]:
    trial.init_local_path()

    logger_creator = partial(
        _noop_logger_creator, logdir=trial.storage.trial_working_directory
    )

    trial_config = copy.deepcopy(trial.config)
    trial_config[TRIAL_INFO] = _TrialInfo(trial)
    stdout_file, stderr_file = trial.log_to_file
    trial_config[STDOUT_FILE] = stdout_file
    trial_config[STDERR_FILE] = stderr_file

    assert trial.storage.trial_dir_name

    kwargs = {
        "config": trial_config,
        "logger_creator": logger_creator,
        "storage": trial.storage,
    }

    return kwargs


@contextmanager
def _change_working_directory(trial):
    """Context manager changing working directory to trial logdir.
    Used in local mode.

    For non-local mode it is no-op.
    """
    if ray._private.worker._mode() == ray._private.worker.LOCAL_MODE:
        old_dir = os.getcwd()
        try:
            os.chdir(trial.local_path)
            yield
        finally:
            os.chdir(old_dir)
    else:
        yield


@DeveloperAPI
class Trial:
    """A trial object holds the state for one model training run.

    Trials are themselves managed by the TrialRunner class, which implements
    the event loop for submitting trial runs to a Ray cluster.

    Trials start in the PENDING state, and transition to RUNNING once started.
    On error, it transitions to ERROR, otherwise TERMINATED on success.

    There are resources allocated to each trial. These should be specified
    using ``PlacementGroupFactory``.

    Attributes:
        trainable_name: Name of the trainable object to be executed.
        config: Provided configuration dictionary with evaluated params.
        trial_id: Unique identifier for the trial.
        path: Path where results for this trial are stored. Can be on
            the local node or on cloud storage.
        local_path: Path on the local disk where results are stored.
        remote_path: Path on cloud storage where results are stored,
            or None if not set.
        relative_logdir: Directory of the trial relative to its
            experiment directory.
        evaluated_params: Evaluated parameters by search algorithm,
        experiment_tag: Identifying trial name to show in the console
        status: One of PENDING, RUNNING, PAUSED, TERMINATED, ERROR/
        error_file: Path to the errors that this trial has raised.

    """

    _nonjson_fields = [
        "results",
        "extra_arg",
        "placement_group_factory",
        "_resources",
        "_default_placement_group_factory",
    ]

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    TERMINATED = "TERMINATED"
    ERROR = "ERROR"

    def __init__(
        self,
        trainable_name: str,
        *,
        config: Optional[Dict] = None,
        trial_id: Optional[str] = None,
        storage: Optional[StorageContext] = None,
        evaluated_params: Optional[Dict] = None,
        experiment_tag: str = "",
        placement_group_factory: Optional[PlacementGroupFactory] = None,
        stopping_criterion: Optional[Dict[str, float]] = None,
        checkpoint_config: Optional[CheckpointConfig] = None,
        export_formats: Optional[List[str]] = None,
        restore_path: Optional[str] = None,
        trial_name_creator: Optional[Callable[["Trial"], str]] = None,
        trial_dirname_creator: Optional[Callable[["Trial"], str]] = None,
        log_to_file: Union[Optional[str], Tuple[Optional[str], Optional[str]]] = None,
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

        self.temporary_state = _TemporaryTrialState()
        self.run_metadata = _TrainingRunMetadata()

        # Create a copy, since `init_local_path` updates the context with the
        # generated trial dirname.
        self.storage = copy.copy(storage)

        self.config = config or {}
        # Save a copy of the original unresolved config so that we can swap
        # out and update any reference config values after restoration.
        self.__unresolved_config = self.config

        # Parameters that Tune varies across searches.
        self.evaluated_params = evaluated_params or {}
        self.experiment_tag = experiment_tag
        self.stopping_criterion = stopping_criterion or {}

        self._setup_default_resource = _setup_default_resource

        if placement_group_factory and not isinstance(
            placement_group_factory, PlacementGroupFactory
        ):
            placement_group_factory = resource_dict_to_pg_factory(
                placement_group_factory
            )

        self._default_placement_group_factory = placement_group_factory
        # Will be created in create_placement_group_factory().
        self.placement_group_factory = None

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
        self._default_result_or_future: Union[ray.ObjectRef, dict, None] = None

        self.export_formats = export_formats
        self.status = Trial.PENDING
        self.relative_logdir = None

        self.trial_name_creator = trial_name_creator
        self.trial_dirname_creator = trial_dirname_creator
        self.custom_trial_name = None
        self.custom_dirname = None

        # Checkpoint config
        checkpoint_config = checkpoint_config or CheckpointConfig()

        self.run_metadata.checkpoint_manager = _CheckpointManager(
            checkpoint_config=checkpoint_config
        )

        # Restoration fields
        self.restore_path = restore_path
        self._restore_checkpoint_result: Optional[_TrainingResult] = None
        if restore_path:
            # tune.run(restore) passes in a path without metrics.
            self._restore_checkpoint_result = _TrainingResult(
                checkpoint=Checkpoint.from_directory(restore_path), metrics={}
            )

        if trial_name_creator:
            self.custom_trial_name = trial_name_creator(self)

        if trial_dirname_creator:
            self.custom_dirname = trial_dirname_creator(self)
            if os.path.sep in self.custom_dirname:
                raise ValueError(
                    f"Trial dirname must not contain '/'. Got {self.custom_dirname}"
                )

        self._state_json = None

    def create_placement_group_factory(self):
        """Compute placement group factory if needed.

        Note: this must be called after all the placeholders in
        self.config are resolved.
        """
        trainable_cls = self.get_trainable_cls()
        if not trainable_cls or not self._setup_default_resource:
            # Create placement group factory using default resources.
            self.placement_group_factory = (
                self._default_placement_group_factory or resource_dict_to_pg_factory()
            )
            return

        default_resources = trainable_cls.default_resource_request(self.config)

        # If Trainable returns resources, do not allow manual override via
        # `resources_per_trial` by the user.
        if default_resources and self._default_placement_group_factory:
            raise TuneError(
                "Resources for {} have been automatically set to {} "
                "by its `default_resource_request()` method. Please "
                "clear the `resources_per_trial` option.".format(
                    trainable_cls, default_resources
                )
            )

        if default_resources and not isinstance(
            default_resources, PlacementGroupFactory
        ):
            default_resources = resource_dict_to_pg_factory(default_resources)

        self.placement_group_factory = (
            # default_resource_request
            default_resources
            # resources_per_trial
            or self._default_placement_group_factory
            # cpu=1
            or resource_dict_to_pg_factory()
        )

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
        if self._default_result_or_future and self.temporary_state.ray_actor:
            self.set_location(
                _Location(
                    self._default_result_or_future.get(NODE_IP),
                    self._default_result_or_future.get(PID),
                )
            )
        return self._default_result_or_future

    def resolve_config_placeholders(self, placeholder_resolvers: Dict[Tuple, Any]):
        from ray.tune.impl.placeholder import resolve_placeholders

        # Make a copy of the unresolved config before resolve it.
        self.config = copy.deepcopy(self.__unresolved_config)
        resolve_placeholders(self.config, placeholder_resolvers)

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
        result = self.run_metadata.last_result
        if not {k for k in result if k != TRIAL_ID}:
            self._get_default_result_or_future()
            result = self._default_result_or_future or result
        result.setdefault(TRIAL_ID, self.trial_id)
        return result

    @property
    def metric_analysis(self):
        return self.run_metadata.metric_analysis

    @property
    def metric_n_steps(self):
        return self.run_metadata.metric_n_steps

    def get_ray_actor_ip(self) -> Optional[str]:
        if self.temporary_state.location.hostname:
            return self.temporary_state.location.hostname

        if not self.temporary_state.ray_actor:
            return None

        hostname, pid = ray.get(
            self.temporary_state.ray_actor.get_current_ip_pid.remote()
        )
        self.temporary_state.location = _Location(hostname, pid)
        return self.temporary_state.location.hostname

    @property
    @Deprecated("Replaced by `local_experiment_path`")
    def local_dir(self):
        return self.local_experiment_path

    @property
    def experiment_dir_name(self):
        return self.storage.experiment_dir_name

    @property
    def remote_experiment_path(self) -> str:
        return self.storage.experiment_fs_path

    @property
    def local_experiment_path(self) -> str:
        return self.storage.experiment_driver_staging_path

    @property
    @Deprecated("Replaced by `local_path`")
    def logdir(self) -> Optional[str]:
        # TODO(justinvyu): [Deprecated] Remove in 2.11.
        raise DeprecationWarning("Use `local_path` instead of `logdir`.")

    @property
    def local_path(self) -> Optional[str]:
        return self.storage.trial_driver_staging_path

    @property
    def path(self) -> Optional[str]:
        return self.storage.trial_fs_path

    @property
    def has_reported_at_least_once(self) -> bool:
        return bool(self.run_metadata.last_result)

    @property
    def node_ip(self):
        return self.temporary_state.location.hostname

    @property
    def checkpoint_at_end(self):
        config = self.run_metadata.checkpoint_manager.checkpoint_config
        return config.checkpoint_at_end

    @property
    def checkpoint_freq(self):
        config = self.run_metadata.checkpoint_manager.checkpoint_config
        return config.checkpoint_frequency

    @property
    def latest_checkpoint_result(self) -> Optional[_TrainingResult]:
        # NOTE: Fallback to the checkpoint passed in from `tune.run(restore)`
        # if the trial hasn't saved any checkpoints itself yet.
        return (
            self.run_metadata.checkpoint_manager.latest_checkpoint_result
            or self._restore_checkpoint_result
        )

    @property
    def checkpoint(self) -> Optional[Checkpoint]:
        """Returns the most recent checkpoint if one has been saved."""
        return (
            self.latest_checkpoint_result.checkpoint
            if self.latest_checkpoint_result
            else None
        )

    @classmethod
    def generate_id(cls):
        return str(uuid.uuid4().hex)[:8]

    def reset(self) -> "Trial":
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

        checkpoint_config = self.run_metadata.checkpoint_manager.checkpoint_config
        return Trial(
            self.trainable_name,
            config=self.config,
            trial_id=None,
            evaluated_params=self.evaluated_params,
            experiment_tag=self.experiment_tag,
            placement_group_factory=placement_group_factory,
            stopping_criterion=self.stopping_criterion,
            checkpoint_config=checkpoint_config,
            export_formats=self.export_formats,
            restore_path=self.restore_path,
            trial_name_creator=self.trial_name_creator,
            trial_dirname_creator=self.trial_dirname_creator,
            log_to_file=self.log_to_file,
            max_failures=self.max_failures,
            storage=self.storage,
        )

    @Deprecated("Replaced by `init_local_path()`")
    def init_logdir(self):
        # TODO(justinvyu): [Deprecated] Remove in 2.11.
        raise DeprecationWarning("Use `init_local_path` instead of `init_logdir`.")

    def init_local_path(self):
        """Init logdir."""
        if not self.relative_logdir:
            self.relative_logdir = _create_unique_logdir_name(
                str(self.local_experiment_path), self._generate_dirname()
            )
            # Populate the storage context with the trial dir name we just generated.
            self.storage.trial_dir_name = self.relative_logdir

        assert self.local_path
        logdir_path = Path(self.local_path)
        max_path_length = _get_max_path_length()
        if len(str(logdir_path)) >= max_path_length:
            logger.warning(
                f"The path to the trial log directory is too long "
                f"(max length: {max_path_length}. "
                f"Consider using `trial_dirname_creator` to shorten the path. "
                f"Path: {logdir_path}"
            )
        logdir_path.mkdir(parents=True, exist_ok=True)

        self.invalidate_json_state()

    def update_resources(self, resources: Union[dict, PlacementGroupFactory]):
        """EXPERIMENTAL: Updates the resource requirements.

        Should only be called when the trial is not running.

        Raises:
            ValueError: if trial status is running.
        """
        if self.status is Trial.RUNNING:
            raise ValueError("Cannot update resources while Trial is running.")

        placement_group_factory = resources
        if isinstance(resources, dict):
            placement_group_factory = resource_dict_to_pg_factory(resources)

        self.placement_group_factory = placement_group_factory

        self.invalidate_json_state()

    def set_ray_actor(self, ray_actor):
        self.temporary_state.ray_actor = ray_actor
        if ray_actor:
            # Do not block here, the result will be gotten when last_result
            # property is accessed
            self._default_result_or_future = ray_actor.get_auto_filled_metrics.remote(
                debug_metrics_only=True
            )

    def set_location(self, location):
        """Sets the location of the trial."""
        self.temporary_state.location = location

    def set_status(self, status):
        """Sets the status of the trial."""
        self.status = status
        if status == Trial.RUNNING:
            if self.run_metadata.start_time is None:
                self.run_metadata.start_time = time.time()
        self.invalidate_json_state()

    def set_config(self, config):
        self.config = config
        self.invalidate_json_state()

    def set_experiment_tag(self, experiment_tag):
        self.experiment_tag = experiment_tag
        self.invalidate_json_state()

    def set_storage(self, new_storage: StorageContext):
        """Updates the storage context of the trial.

        If the `storage_path` or `experiment_dir_name` has changed, then this setter
        also updates the paths of all checkpoints tracked by the checkpoint manager.
        This enables restoration from a checkpoint if the user moves the directory.
        """
        original_storage = self.storage

        checkpoint_manager = self.run_metadata.checkpoint_manager

        for checkpoint_result in checkpoint_manager.best_checkpoint_results:
            checkpoint_result.checkpoint = Checkpoint(
                path=checkpoint_result.checkpoint.path.replace(
                    original_storage.trial_fs_path, new_storage.trial_fs_path, 1
                ),
                filesystem=new_storage.storage_filesystem,
            )
        latest_checkpoint_result = checkpoint_manager.latest_checkpoint_result
        if latest_checkpoint_result:
            latest_checkpoint_result.checkpoint = Checkpoint(
                path=latest_checkpoint_result.checkpoint.path.replace(
                    original_storage.trial_fs_path, new_storage.trial_fs_path, 1
                ),
                filesystem=new_storage.storage_filesystem,
            )

        self.storage = new_storage
        self.invalidate_json_state()

    @property
    def num_failures(self):
        return self.run_metadata.num_failures

    @property
    def num_failures_after_restore(self):
        return self.run_metadata.num_failures_after_restore

    @property
    def error_file(self):
        if not self.local_path or not self.run_metadata.error_filename:
            return None
        return Path(self.local_path, self.run_metadata.error_filename).as_posix()

    @property
    def pickled_error_file(self):
        if not self.local_path or not self.run_metadata.pickled_error_filename:
            return None
        return Path(
            self.local_path, self.run_metadata.pickled_error_filename
        ).as_posix()

    def get_pickled_error(self) -> Optional[Exception]:
        """Returns the pickled error object if it exists in storage.

        This is a pickled version of the latest error that the trial encountered.
        """
        error_filename = self.run_metadata.pickled_error_filename
        if error_filename is None:
            return None

        fs = self.storage.storage_filesystem
        pickled_error_fs_path = Path(
            self.storage.trial_fs_path, error_filename
        ).as_posix()

        if _exists_at_fs_path(fs=fs, fs_path=pickled_error_fs_path):
            with fs.open_input_stream(pickled_error_fs_path) as f:
                return cloudpickle.loads(f.readall())
        return None

    def get_error(self) -> Optional[TuneError]:
        """Returns the error text file trace as a TuneError object
        if it exists in storage.

        This is a text trace of the latest error that the trial encountered,
        which is used in the case that the error is not picklable.
        """
        error_filename = self.run_metadata.error_filename
        if error_filename is None:
            return None

        fs = self.storage.storage_filesystem
        txt_error_fs_path = Path(self.storage.trial_fs_path, error_filename).as_posix()

        if _exists_at_fs_path(fs=fs, fs_path=txt_error_fs_path):
            with fs.open_input_stream(txt_error_fs_path) as f:
                return f.readall().decode()
        return None

    def _handle_restore_error(self, exc: Exception):
        # For Restoration errors, we only increment the restore failure count
        # if the number of failures exceeds the restore retry limit.
        if self.temporary_state.num_restore_failures >= int(
            os.environ.get("TUNE_RESTORE_RETRY_NUM", 0)
        ):
            self.run_metadata.num_failures += 1
        else:
            self.temporary_state.num_restore_failures += 1

    def _handle_ray_actor_error(self, exc: RayActorError):
        count_preemption_errors = bool(
            int(os.environ.get(RAY_TRAIN_COUNT_PREEMPTION_AS_FAILURE, "0"))
        )
        if not exc.preempted or count_preemption_errors:
            # Only count non-preempted actor errors as failures.
            self.run_metadata.num_failures += 1

    def _handle_ray_task_error(self, exc: RayTaskError):
        cause = exc.as_instanceof_cause()
        if isinstance(cause, RayActorError):
            # Handle the RayActorError directly (ex: Ray Train worker actor errors)
            return self._handle_ray_actor_error(cause)

        # Increment failures for all user errors (which get raised as RayTaskError)
        self.run_metadata.num_failures += 1

    def handle_error(
        self, exc: Optional[Union[TuneError, RayTaskError, RayActorError]] = None
    ):
        if self.is_restoring:
            self._handle_restore_error(exc)
        elif isinstance(exc, RayActorError):
            self._handle_ray_actor_error(exc)
        elif isinstance(exc, RayTaskError):
            self._handle_ray_task_error(exc)
        else:
            self.run_metadata.num_failures += 1

        if self.local_path:
            self.run_metadata.error_filename = EXPR_ERROR_FILE
            if isinstance(exc, (RayTaskError, RayActorError)):
                # Piping through the actual error to result grid.
                self.run_metadata.pickled_error_filename = EXPR_ERROR_PICKLE_FILE
                with open(self.pickled_error_file, "wb") as f:
                    cloudpickle.dump(exc, f)
            with open(self.error_file, "a+") as f:
                f.write(
                    "Failure # {} (occurred at {})\n".format(
                        self.run_metadata.num_failures, date_str()
                    )
                )
                f.write(str(exc) + "\n")
        self.run_metadata.invalidate_cache()

    def should_stop(self, result):
        """Whether the given result meets this trial's stopping criteria."""
        if result.get(DONE):
            return True

        for criterion, stop_value in self.stopping_criterion.items():
            if isinstance(criterion, dict):
                raise ValueError(
                    "Stopping criteria is now flattened by default. "
                    "Use forward slashes to nest values `key1/key2/key3`."
                )
            elif criterion not in result:
                if log_once("tune_trial_stop_criterion_not_found"):
                    logger.warning(
                        f"Stopping criterion '{criterion}' not found in result dict! "
                        f"Available keys are {list(result.keys())}. If '{criterion}' is"
                        " never reported, the run will continue until training is "
                        "finished."
                    )
            elif result[criterion] >= stop_value:
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

    def has_checkpoint(self) -> bool:
        return self.checkpoint is not None

    def on_checkpoint(self, checkpoint_result: _TrainingResult):
        """Hook for handling checkpoints taken by the Trainable.

        Args:
            checkpoint: Checkpoint taken.
        """
        self.run_metadata.checkpoint_manager.register_checkpoint(checkpoint_result)
        # Update the checkpoint index to keep the checkpoint index in sync.
        # This index will get restored when the trial is restored and will
        # be passed to the Trainable as the starting checkpoint index.
        self.storage._update_checkpoint_index(checkpoint_result.metrics)

        self.invalidate_json_state()
        self.run_metadata.invalidate_cache()

    def on_restore(self):
        """Handles restoration completion."""
        assert self.is_restoring
        self.run_metadata.last_result = self.temporary_state.restoring_from.metrics
        self.run_metadata.last_result.setdefault("config", self.config)
        self.temporary_state.restoring_from = None
        self.temporary_state.num_restore_failures = 0

    def should_recover(self):
        """Returns whether the trial qualifies for retrying.

        `num_failures` should represent the number of times the trial has
        failed *up to the moment this method is called.* If we've failed
        5 times and `max_failures=5`, then we should recover, since
        we only pass the limit on the 6th failure.

        Note this may return true even when there is no checkpoint, either because
        `self.checkpoint_freq` is `0` or because the trial failed before
        a checkpoint has been made.
        """
        return (
            self.run_metadata.num_failures <= self.max_failures or self.max_failures < 0
        )

    def update_last_result(self, result):
        if self.experiment_tag:
            result.update(experiment_tag=self.experiment_tag)

        self.set_location(_Location(result.get(NODE_IP), result.get(PID)))
        self.run_metadata.last_result = result
        self.run_metadata.last_result_time = time.time()

        metric_result = self.last_result.copy()
        for remove_metric in DEBUG_METRICS:
            metric_result.pop(remove_metric, None)

        for metric, value in flatten_dict(metric_result).items():
            if isinstance(value, Number):
                self.run_metadata.update_metric(
                    metric, value, step=result.get("training_iteration")
                )

    def get_trainable_cls(self):
        if self.stub:
            return None
        return get_trainable_cls(self.trainable_name)

    def is_finished(self):
        return self.status in [Trial.ERROR, Trial.TERMINATED]

    @property
    def is_restoring(self):
        return self.temporary_state.restoring_from is not None

    @property
    def is_saving(self):
        return self.temporary_state.saving_to is not None

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
        self._state_json = None

    def get_json_state(self) -> Tuple[str, str]:
        if self._state_json is None:
            state = self.__getstate__()
            state.pop("run_metadata", None)
            self._state_json = json.dumps(state, indent=2, cls=TuneFunctionEncoder)

        runtime_metadata_json = self.run_metadata.get_json_state()

        return self._state_json, runtime_metadata_json

    @classmethod
    def from_json_state(cls, json_state: str, stub: bool = False) -> "Trial":
        state = json.loads(json_state, cls=TuneFunctionDecoder)

        new_trial = Trial(
            state["trainable_name"],
            stub=stub,
            _setup_default_resource=False,
        )

        new_trial.__setstate__(state)

        return new_trial

    def restore_run_metadata(self, run_metadata: str):
        self.run_metadata = _TrainingRunMetadata.from_json_state(run_metadata)

    @classmethod
    def from_directory(
        cls, path: Union[str, os.PathLike], stub: bool = False
    ) -> "Trial":
        metadata_path = Path(path, TRIAL_STATE_FILENAME)
        if not metadata_path.exists():
            raise FileNotFoundError(
                f"Can't restore trial from path: File `{metadata_path}` not found."
            )

        json_state = metadata_path.read_text()
        return cls.from_json_state(json_state, stub=stub)

    def __getstate__(self):
        """Memento generator for Trial.

        Sets RUNNING trials to PENDING.
        Note this can only occur if the trial holds a PERSISTENT checkpoint.
        """
        state = self.__dict__.copy()

        for key in self._nonjson_fields:
            state[key] = binary_to_hex(cloudpickle.dumps(state.get(key)))

        state.pop("temporary_state", None)

        state["_state_json"] = None
        state["_default_result_or_future"] = None

        return state

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

        self.temporary_state = _TemporaryTrialState()

        assert self.placement_group_factory
