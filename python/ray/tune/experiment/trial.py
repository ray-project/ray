import warnings
import copy
import json
import logging
from contextlib import contextmanager
from functools import partial
from numbers import Number
import os
from pathlib import Path
import platform
import re
import shutil
import time
from typing import Any, Dict, Optional, Sequence, Union, Callable, List, Tuple
import uuid

import ray
from ray.air import CheckpointConfig
from ray.air._internal.uri_utils import URI
from ray.air._internal.checkpoint_manager import _TrackedCheckpoint, CheckpointStorage
from ray.air.constants import (
    EXPR_ERROR_PICKLE_FILE,
    EXPR_ERROR_FILE,
    TRAINING_ITERATION,
)

import ray.cloudpickle as cloudpickle
from ray.exceptions import RayActorError, RayTaskError
from ray.train import Checkpoint
from ray.train.constants import RAY_CHDIR_TO_TRIAL_DIR
from ray.train._internal.checkpoint_manager import (
    _CheckpointManager as _NewCheckpointManager,
)
from ray.train._internal.session import _FutureTrainingResult, _TrainingResult
from ray.train._internal.storage import _use_storage_context, StorageContext
from ray.tune import TuneError
from ray.tune.error import _TuneRestoreError
from ray.tune.execution.checkpoint_manager import _CheckpointManager
from ray.tune.logger import NoopLogger

# NOTE(rkn): We import ray.tune.registry here instead of importing the names we
# need because there are cyclic imports that may cause specific names to not
# have been defined yet. See https://github.com/ray-project/ray/issues/1716.
from ray.tune.registry import get_trainable_cls, validate_trainable
from ray.tune.result import (
    DONE,
    NODE_IP,
    PID,
    TRIAL_ID,
    DEBUG_METRICS,
    TRIAL_INFO,
    STDOUT_FILE,
    STDERR_FILE,
    DEFAULT_EXPERIMENT_NAME,
    _get_defaults_results_dir,
)
from ray.train import SyncConfig
from ray.tune.execution.placement_groups import (
    PlacementGroupFactory,
    resource_dict_to_pg_factory,
)
from ray.tune.trainable.metadata import _TrainingRunMetadata
from ray.tune.utils.serialization import TuneFunctionDecoder, TuneFunctionEncoder
from ray.tune.trainable.util import TrainableUtil
from ray.tune.utils import date_str, flatten_dict
from ray.tune.utils.util import _split_remote_local_path
from ray.util.annotations import DeveloperAPI, Deprecated
from ray.util.debug import log_once
from ray._private.utils import binary_to_hex, hex_to_binary


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

    def __init__(self, trial_id, ray_actor):
        self.trial_id = trial_id
        self.ray_actor = ray_actor

    def __call__(self, checkpoint: _TrackedCheckpoint):
        """Requests checkpoint deletion asynchronously.

        Args:
            checkpoint: Checkpoint to delete.
        """
        if not self.ray_actor:
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
            ray.get(self.ray_actor.delete_checkpoint.remote(checkpoint_path))

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

    logger_creator = partial(_noop_logger_creator, logdir=trial.local_path)

    trial_config = copy.deepcopy(trial.config)
    trial_config[TRIAL_INFO] = _TrialInfo(trial)
    stdout_file, stderr_file = trial.log_to_file
    trial_config[STDOUT_FILE] = stdout_file
    trial_config[STDERR_FILE] = stderr_file

    kwargs = {
        "config": trial_config,
        "logger_creator": logger_creator,
    }

    if _use_storage_context():
        assert trial.storage
        assert trial.storage.trial_dir_name
        kwargs["storage"] = trial.storage

    if trial.uses_cloud_checkpointing:
        # We keep these kwargs separate for backwards compatibility
        # with trainables that don't provide these keyword arguments
        kwargs["remote_checkpoint_dir"] = trial.remote_path
        kwargs["sync_config"] = trial.legacy_sync_config

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
        experiment_path: Optional[str] = None,
        experiment_dir_name: Optional[str] = None,
        evaluated_params: Optional[Dict] = None,
        experiment_tag: str = "",
        placement_group_factory: Optional[PlacementGroupFactory] = None,
        stopping_criterion: Optional[Dict[str, float]] = None,
        sync_config: Optional[SyncConfig] = None,
        checkpoint_config: Optional[CheckpointConfig] = None,
        export_formats: Optional[List[str]] = None,
        restore_path: Optional[str] = None,
        trial_name_creator: Optional[Callable[["Trial"], str]] = None,
        trial_dirname_creator: Optional[Callable[["Trial"], str]] = None,
        log_to_file: Union[Optional[str], Tuple[Optional[str], Optional[str]]] = None,
        max_failures: int = 0,
        stub: bool = False,
        _setup_default_resource: bool = True,
        # Deprecated
        local_dir: Optional[str] = None,
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

        if _use_storage_context():
            self._legacy_orig_experiment_path = None
            self._legacy_orig_experiment_dir_name = None
            self._legacy_local_experiment_path = None
            self._legacy_remote_experiment_path = None
            self._legacy_experiment_dir_name = None
            self.legacy_sync_config = None
        else:
            # Set to pass through on `Trial.reset()`
            self._legacy_orig_experiment_path = experiment_path
            self._legacy_orig_experiment_dir_name = experiment_dir_name

            self._legacy_experiment_dir_name = experiment_dir_name

            # Sync config
            self.legacy_sync_config = sync_config or SyncConfig()

            local_experiment_path, remote_experiment_path = _split_remote_local_path(
                experiment_path, None
            )

            # Backwards compatibility for `local_dir`
            if local_dir:
                if local_experiment_path:
                    raise ValueError(
                        "Only one of `local_dir` or `experiment_path` "
                        "can be passed to `Trial()`."
                    )
                local_experiment_path = local_dir

            # Derive experiment dir name from local path
            if not experiment_dir_name and local_experiment_path:
                # Maybe derive experiment dir name from local storage dir
                experiment_dir_name = Path(local_experiment_path).name
            elif not experiment_dir_name:
                experiment_dir_name = DEFAULT_EXPERIMENT_NAME

            # Set default experiment dir name
            if not local_experiment_path:
                local_experiment_path = str(
                    Path(_get_defaults_results_dir()) / experiment_dir_name
                )
                os.makedirs(local_experiment_path, exist_ok=True)

            # Set remote experiment path if upload_dir is set
            if self.legacy_sync_config.upload_dir:
                if remote_experiment_path:
                    if not remote_experiment_path.startswith(
                        self.legacy_sync_config.upload_dir
                    ):
                        raise ValueError(
                            f"Both a `SyncConfig.upload_dir` and an `experiment_path` "
                            f"pointing to remote storage were passed, but they do not "
                            f"point to the same location. Got: "
                            f"`experiment_path={experiment_path}` and "
                            "`SyncConfig.upload_dir="
                            f"{self.legacy_sync_config.upload_dir}`. "
                        )
                    warnings.warn(
                        "If `experiment_path` points to a remote storage location, "
                        "do not set `SyncConfig.upload_dir`. ",
                        DeprecationWarning,
                    )
                else:
                    remote_experiment_path = str(
                        URI(self.legacy_sync_config.upload_dir) / experiment_dir_name
                    )

            # Finally, set properties
            self._legacy_local_experiment_path = local_experiment_path
            self._legacy_remote_experiment_path = remote_experiment_path

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
        if not _use_storage_context():
            # TODO(justinvyu): Why is this needed?
            checkpoint_config.checkpoint_score_attribute = (
                checkpoint_config.checkpoint_score_attribute or TRAINING_ITERATION
            )

        if _use_storage_context():
            self.run_metadata.checkpoint_manager = _NewCheckpointManager(
                checkpoint_config=checkpoint_config
            )
        else:
            self.run_metadata.checkpoint_manager = _CheckpointManager(
                checkpoint_config=checkpoint_config,
                delete_fn=_CheckpointDeleter(str(self), self.temporary_state.ray_actor),
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
        if _use_storage_context():
            return self.storage.experiment_dir_name

        return self._legacy_experiment_dir_name

    @experiment_dir_name.setter
    def experiment_dir_name(self, name: str):
        if _use_storage_context():
            raise RuntimeError("Set storage.experiment_dir_name instead.")

        self._legacy_experiment_dir_name = name

    @property
    def remote_experiment_path(self) -> str:
        if _use_storage_context():
            return self.storage.experiment_fs_path

        return str(self._legacy_remote_experiment_path)

    @remote_experiment_path.setter
    def remote_experiment_path(self, remote_path: str):
        if _use_storage_context():
            raise RuntimeError("Set storage.experiment_dir_name instead.")

        self._legacy_remote_experiment_path = remote_path

    @property
    def local_experiment_path(self) -> str:
        if _use_storage_context():
            return self.storage.experiment_local_path

        return str(self._legacy_local_experiment_path)

    @local_experiment_path.setter
    def local_experiment_path(self, local_path: str):
        if _use_storage_context():
            raise RuntimeError("Set storage.experiment_dir_name instead.")

        relative_checkpoint_dirs = []
        if self.local_path:
            # Save the relative paths of persistent trial checkpoints, which are saved
            # relative to the old `local_dir`/`logdir`
            for checkpoint in self.get_trial_checkpoints():
                checkpoint_dir = checkpoint.dir_or_data
                if not isinstance(checkpoint_dir, str):
                    logger.warning(
                        f"No data found in checkpoint for trial {self} and metrics "
                        f"{checkpoint.metrics} (type: {type(checkpoint_dir)}). "
                        f"Skipping."
                    )
                    continue

                relative_checkpoint_dirs.append(
                    os.path.relpath(checkpoint_dir, self.local_path)
                )

        # Update the underlying `_legacy_local_experiment_path`,
        # which also updates the trial `local_path`
        self._legacy_local_experiment_path = local_path

        if self.local_path:
            for checkpoint, relative_checkpoint_dir in zip(
                self.get_trial_checkpoints(), relative_checkpoint_dirs
            ):
                # Reconstruct the checkpoint dir using the (possibly updated)
                # trial logdir and the relative checkpoint directory.
                checkpoint.dir_or_data = os.path.join(
                    self.local_path, relative_checkpoint_dir
                )

    @property
    @Deprecated("Replaced by `local_path`")
    def logdir(self) -> Optional[str]:
        # Deprecate: Raise in 2.5, Remove in 2.6
        return self.local_path

    @property
    def local_path(self) -> Optional[str]:
        if _use_storage_context():
            return self.storage.trial_local_path

        if not self.local_experiment_path or not self.relative_logdir:
            return None
        return str(Path(self.local_experiment_path).joinpath(self.relative_logdir))

    @local_path.setter
    def local_path(self, logdir):
        if _use_storage_context():
            raise RuntimeError("Set storage.trial_dir_name instead.")

        relative_logdir = Path(logdir).relative_to(self.local_experiment_path)
        if ".." in str(relative_logdir):
            raise ValueError(
                f"The `local_path` points to a directory outside the trial's "
                f"`local_experiment_path` ({self.local_experiment_path}), "
                f"which is unsupported. Use a logdir within the "
                f"local directory instead. Got: {logdir}"
            )
        if log_once("logdir_setter"):
            logger.warning(
                "Deprecated. In future versions only the relative logdir "
                "will be used and calling logdir will raise an error."
            )
        self.relative_logdir = relative_logdir

    @property
    @Deprecated("Replaced by `remote_path`")
    def remote_checkpoint_dir(self) -> Optional[str]:
        # Deprecate: Raise in 2.5, Remove in 2.6
        return self.remote_path

    @property
    def remote_path(self) -> Optional[str]:
        # TODO(justinvyu): Remove remote_path. It's just path vs local_path now.
        if _use_storage_context():
            return self.path

        if not self._legacy_remote_experiment_path or not self.relative_logdir:
            return None
        uri = URI(self._legacy_remote_experiment_path)
        return str(uri / self.relative_logdir)

    @property
    def path(self) -> Optional[str]:
        if _use_storage_context():
            return self.storage.trial_fs_path

        return self.remote_path or self.local_path

    @property
    def has_reported_at_least_once(self) -> bool:
        return bool(self.run_metadata.last_result)

    @property
    def node_ip(self):
        return self.temporary_state.location.hostname

    @property
    def sync_on_checkpoint(self):
        if _use_storage_context():
            return self.storage.sync_config.sync_on_checkpoint

        return self.legacy_sync_config.sync_on_checkpoint

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
        if _use_storage_context():
            return (
                self.latest_checkpoint_result.checkpoint
                if self.latest_checkpoint_result
                else None
            )

        if self.status == Trial.ERROR:
            checkpoint = (
                self.run_metadata.checkpoint_manager.newest_persistent_checkpoint
            )
        else:
            checkpoint = self.run_metadata.checkpoint_manager.newest_checkpoint
        if checkpoint.dir_or_data is None:
            checkpoint = _TrackedCheckpoint(
                dir_or_data=self.restore_path,
                storage_mode=CheckpointStorage.PERSISTENT,
            )
        return checkpoint

    @classmethod
    def generate_id(cls):
        return str(uuid.uuid4().hex)[:8]

    @property
    def uses_cloud_checkpointing(self):
        # TODO(justinvyu): This is entangled in the old restore codepaths.
        # Remove this once those are gone.
        if _use_storage_context():
            return False

        return bool(self.remote_path)

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

        checkpoint_config = self.run_metadata.checkpoint_manager.checkpoint_config
        return Trial(
            self.trainable_name,
            config=self.config,
            trial_id=None,
            experiment_path=self._legacy_orig_experiment_path,
            experiment_dir_name=self._legacy_orig_experiment_dir_name,
            evaluated_params=self.evaluated_params,
            experiment_tag=self.experiment_tag,
            placement_group_factory=placement_group_factory,
            stopping_criterion=self.stopping_criterion,
            sync_config=self.legacy_sync_config,
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
        # Deprecate: Raise in 2.5, Remove in 2.6
        self.init_local_path()

    def init_local_path(self):
        """Init logdir."""
        if not self.relative_logdir:
            self.relative_logdir = _create_unique_logdir_name(
                str(self.local_experiment_path), self._generate_dirname()
            )

        if _use_storage_context():
            # Populate the storage context with the trial dir name we just generated.
            assert self.storage
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
            ValueError if trial status is running.
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
        if not _use_storage_context():
            self.run_metadata.checkpoint_manager.set_delete_fn(
                _CheckpointDeleter(str(self), ray_actor)
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
        return os.path.join(self.local_path, self.run_metadata.error_filename)

    @property
    def pickled_error_file(self):
        if not self.local_path or not self.run_metadata.pickled_error_filename:
            return None
        return os.path.join(self.local_path, self.run_metadata.pickled_error_filename)

    def handle_error(self, exc: Optional[Union[TuneError, RayTaskError]] = None):
        if isinstance(exc, _TuneRestoreError):
            exc = exc.exc
            if self.temporary_state.num_restore_failures >= int(
                os.environ.get("TUNE_RESTORE_RETRY_NUM", 0)
            ):
                # Restore was unsuccessful, try again without checkpoint.
                self.clear_checkpoint()
                self.run_metadata.num_failures += 1
            else:
                self.temporary_state.num_restore_failures += 1
        else:
            self.run_metadata.num_failures += 1

        if self.local_path:
            self.run_metadata.error_filename = EXPR_ERROR_FILE
            if isinstance(exc, RayTaskError):
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
        if _use_storage_context():
            return self.checkpoint is not None
        return self.checkpoint.dir_or_data is not None

    def clear_checkpoint(self):
        if _use_storage_context():
            if self.latest_checkpoint_result:
                self.latest_checkpoint_result.checkpoint = None
            self.temporary_state.restoring_from = None
            self.run_metadata.invalidate_cache()
            return

        self.checkpoint.dir_or_data = None
        self.temporary_state.restoring_from = None
        self.run_metadata.invalidate_cache()

    def on_checkpoint(self, checkpoint: Union[_TrackedCheckpoint, _TrainingResult]):
        """Hook for handling checkpoints taken by the Trainable.

        Args:
            checkpoint: Checkpoint taken.
        """
        if _use_storage_context():
            checkpoint_result = checkpoint
            assert isinstance(checkpoint_result, _TrainingResult)
            self.run_metadata.checkpoint_manager.register_checkpoint(checkpoint_result)
            # Increment the checkpoint index to keep the checkpoint index in sync.
            # This index will get restored when the trial is restored and will
            # be passed to the Trainable as the starting checkpoint index.
            self.storage.current_checkpoint_index += 1
        else:
            self.run_metadata.checkpoint_manager.on_checkpoint(checkpoint)
        self.invalidate_json_state()
        self.run_metadata.invalidate_cache()

    def on_restore(self):
        """Handles restoration completion."""
        assert self.is_restoring

        if _use_storage_context():
            assert isinstance(self.temporary_state.restoring_from, _TrainingResult)

        self.run_metadata.last_result = self.temporary_state.restoring_from.metrics
        self.run_metadata.last_result.setdefault("config", self.config)
        self.temporary_state.restoring_from = None
        self.temporary_state.num_restore_failures = 0

    def should_recover(self):
        """Returns whether the trial qualifies for retrying.

        This is if the trial has not failed more than max_failures. Note this
        may return true even when there is no checkpoint, either because
        `self.checkpoint_freq` is `0` or because the trial failed before
        a checkpoint has been made.
        """
        return (
            self.run_metadata.num_failures < self.max_failures
            or self.max_failures < 0
            or (
                self.run_metadata.num_failures == self.max_failures
                and self.temporary_state.num_restore_failures
                < int(os.environ.get("TUNE_RESTORE_RETRY_NUM", 0))
            )
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

    def get_trial_checkpoints(self) -> List[_TrackedCheckpoint]:
        return self.run_metadata.checkpoint_manager.best_checkpoints()

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
        metadata_path = os.path.join(path, TRIAL_STATE_FILENAME)
        if not os.path.exists(metadata_path):
            raise FileNotFoundError(
                f"Can't restore trial from path: File `{metadata_path}` not found."
            )

        json_state = Path(metadata_path).read_text()
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
