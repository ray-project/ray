import copy
import logging
import os
import platform
import shutil
import sys
import tempfile
import time
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Union

import ray
import ray.cloudpickle as ray_pickle
from ray._common.utils import try_to_create_directory
from ray.air._internal.util import exception_cause, skip_exceptions
from ray.air.constants import TIME_THIS_ITER_S, TIMESTAMP, TRAINING_ITERATION
from ray.train._internal.checkpoint_manager import _TrainingResult
from ray.train._internal.storage import StorageContext, _exists_at_fs_path
from ray.train.constants import DEFAULT_STORAGE_PATH
from ray.tune.execution.placement_groups import PlacementGroupFactory
from ray.tune.result import (
    DEBUG_METRICS,
    DONE,
    EPISODES_THIS_ITER,
    EPISODES_TOTAL,
    HOSTNAME,
    NODE_IP,
    PID,
    RESULT_DUPLICATE,
    SHOULD_CHECKPOINT,
    STDERR_FILE,
    STDOUT_FILE,
    TIME_TOTAL_S,
    TIMESTEPS_THIS_ITER,
    TIMESTEPS_TOTAL,
    TRIAL_ID,
    TRIAL_INFO,
)
from ray.tune.utils import UtilMonitor
from ray.tune.utils.log import disable_ipython
from ray.tune.utils.util import Tee
from ray.util.annotations import DeveloperAPI, PublicAPI

if TYPE_CHECKING:
    from ray.tune.logger import Logger

logger = logging.getLogger(__name__)

SETUP_TIME_THRESHOLD = 10

# File containing dict data returned by user from `Trainable.save_checkpoint`
_DICT_CHECKPOINT_FILE_NAME = "_dict_checkpoint.pkl"


@PublicAPI
class Trainable:
    """Abstract class for trainable models, functions, etc.

    A call to ``train()`` on a trainable will execute one logical iteration of
    training. As a rule of thumb, the execution time of one train call should
    be large enough to avoid overheads (i.e. more than a few seconds), but
    short enough to report progress periodically (i.e. at most a few minutes).

    Calling ``save()`` should save the training state of a trainable to disk,
    and ``restore(path)`` should restore a trainable to the given state.

    Generally you only need to implement ``setup``, ``step``,
    ``save_checkpoint``, and ``load_checkpoint`` when subclassing Trainable.

    Other implementation methods that may be helpful to override are
    ``log_result``, ``reset_config``, ``cleanup``, and ``_export_model``.

    Tune will convert this class into a Ray actor, which runs on a separate process.
    By default, Tune will also change the current working directory of this process to
    its corresponding trial-level log directory ``self.logdir``.
    This is designed so that different trials that run on the same physical node won't
    accidentally write to the same location and overstep each other.

    The behavior of changing the working directory can be disabled by setting the
    `RAY_CHDIR_TO_TRIAL_DIR=0` environment variable. This allows access to files
    in the original working directory, but relative paths should be used for read only
    purposes, and you must make sure that the directory is synced on all nodes if
    running on multiple machines.

    The `TUNE_ORIG_WORKING_DIR` environment variable was the original workaround for
    accessing paths relative to the original working directory. This environment
    variable is deprecated, and the `RAY_CHDIR_TO_TRIAL_DIR` environment variable
    described above should be used instead.

    This class supports checkpointing to and restoring from remote storage.
    """

    def __init__(
        self,
        config: Dict[str, Any] = None,
        logger_creator: Callable[[Dict[str, Any]], "Logger"] = None,  # Deprecated (2.7)
        storage: Optional[StorageContext] = None,
    ):
        """Initialize a Trainable.

        Sets up logging and points ``self.logdir`` to a directory in which
        training outputs should be placed.

        Subclasses should prefer defining ``setup()`` instead of overriding
        ``__init__()`` directly.

        Args:
            config: Trainable-specific configuration data. By default
                will be saved as ``self.config``.
            logger_creator: (Deprecated) Function that creates a ray.tune.Logger
                object. If unspecified, a default logger is created.
            storage: StorageContext object that contains persistent storage paths
        """

        self.config = config or {}
        trial_info = self.config.pop(TRIAL_INFO, None)

        if self.is_actor():
            disable_ipython()

        # TODO(ml-team): Remove `logger_creator` in 2.7.
        # TODO(justinvyu): Rename/remove logdir.
        self._result_logger = self._logdir = None
        self._create_logger(self.config, logger_creator)

        self._stdout_context = self._stdout_fp = self._stdout_stream = None
        self._stderr_context = self._stderr_fp = self._stderr_stream = None
        self._stderr_logging_handler = None

        stdout_file = self.config.pop(STDOUT_FILE, None)
        stderr_file = self.config.pop(STDERR_FILE, None)

        self._iteration = 0
        self._time_total = 0.0
        self._timesteps_total = None
        self._episodes_total = None
        self._time_since_restore = 0.0
        self._timesteps_since_restore = 0
        self._iterations_since_restore = 0
        self._last_result = None
        self._restored = False
        self._trial_info = trial_info
        self._stdout_file = stdout_file
        self._stderr_file = stderr_file

        self._start_time = time.time()
        self._local_ip = ray.util.get_node_ip_address()

        self._storage = storage
        if storage:
            assert storage.trial_fs_path
            logger.debug(f"StorageContext on the TRAINABLE:\n{storage}")

        self._open_logfiles(stdout_file, stderr_file)

        self.setup(copy.deepcopy(self.config))
        setup_time = time.time() - self._start_time
        if setup_time > SETUP_TIME_THRESHOLD:
            logger.info(
                "Trainable.setup took {:.3f} seconds. If your "
                "trainable is slow to initialize, consider setting "
                "reuse_actors=True to reduce actor creation "
                "overheads.".format(setup_time)
            )
        log_sys_usage = self.config.get("log_sys_usage", False)
        self._monitor = UtilMonitor(start=log_sys_usage)

    @classmethod
    def default_resource_request(
        cls, config: Dict[str, Any]
    ) -> Optional[PlacementGroupFactory]:
        """Provides a static resource requirement for the given configuration.

        This can be overridden by sub-classes to set the correct trial resource
        allocation, so the user does not need to.

        .. testcode::

            @classmethod
            def default_resource_request(cls, config):
                return PlacementGroupFactory([{"CPU": 1}, {"CPU": 1}])


        Args:
            config[Dict[str, Any]]: The Trainable's config dict.

        Returns:
            PlacementGroupFactory: A PlacementGroupFactory consumed by Tune
                for queueing.
        """
        return None

    @classmethod
    def resource_help(cls, config: Dict):
        """Returns a help string for configuring this trainable's resources.

        Args:
            config: The Trainer's config dict.
        """
        return ""

    def get_current_ip_pid(self):
        return self._local_ip, os.getpid()

    def get_auto_filled_metrics(
        self,
        now: Optional[datetime] = None,
        time_this_iter: Optional[float] = None,
        timestamp: Optional[int] = None,
        debug_metrics_only: bool = False,
    ) -> dict:
        """Return a dict with metrics auto-filled by the trainable.

        If ``debug_metrics_only`` is True, only metrics that don't
        require at least one iteration will be returned
        (``ray.tune.result.DEBUG_METRICS``).
        """
        if now is None:
            now = datetime.today()
        autofilled = {
            TRIAL_ID: self.trial_id,
            "date": now.strftime("%Y-%m-%d_%H-%M-%S"),
            "timestamp": timestamp if timestamp else int(time.mktime(now.timetuple())),
            TIME_THIS_ITER_S: time_this_iter,
            TIME_TOTAL_S: self._time_total,
            PID: os.getpid(),
            HOSTNAME: platform.node(),
            NODE_IP: self._local_ip,
            "config": self.config,
            "time_since_restore": self._time_since_restore,
            "iterations_since_restore": self._iterations_since_restore,
        }
        if self._timesteps_since_restore:
            autofilled["timesteps_since_restore"] = self._timesteps_since_restore

        if debug_metrics_only:
            autofilled = {k: v for k, v in autofilled.items() if k in DEBUG_METRICS}
        return autofilled

    def is_actor(self):
        try:
            actor_id = ray._private.worker.global_worker.actor_id
            return actor_id != actor_id.nil()
        except Exception:
            # If global_worker is not instantiated, we're not in an actor
            return False

    def train_buffered(self, buffer_time_s: float, max_buffer_length: int = 1000):
        """Runs multiple iterations of training.

        Calls ``train()`` internally. Collects and combines multiple results.
        This function will run ``self.train()`` repeatedly until one of
        the following conditions is met: 1) the maximum buffer length is
        reached, 2) the maximum buffer time is reached, or 3) a checkpoint
        was created. Even if the maximum time is reached, it will always
        block until at least one result is received.

        Args:
            buffer_time_s: Maximum time to buffer. The next result
                received after this amount of time has passed will return
                the whole buffer.
            max_buffer_length: Maximum number of results to buffer.

        """
        results = []

        now = time.time()
        send_buffer_at = now + buffer_time_s
        while now < send_buffer_at or not results:  # At least one result
            result = self.train()
            results.append(result)
            if result.get(DONE, False):
                # If the trial is done, return
                break
            elif result.get(SHOULD_CHECKPOINT, False):
                # If a checkpoint was created, return
                break
            elif result.get(RESULT_DUPLICATE):
                # If the function API trainable completed, return
                break
            elif len(results) >= max_buffer_length:
                # If the buffer is full, return
                break
            now = time.time()

        return results

    def train(self):
        """Runs one logical iteration of training.

        Calls ``step()`` internally. Subclasses should override ``step()``
        instead to return results.
        This method automatically fills the following fields in the result:

            `done` (bool): training is terminated. Filled only if not provided.

            `time_this_iter_s` (float): Time in seconds this iteration
            took to run. This may be overridden in order to override the
            system-computed time difference.

            `time_total_s` (float): Accumulated time in seconds for this
            entire experiment.

            `training_iteration` (int): The index of this
            training iteration, e.g. call to train(). This is incremented
            after `step()` is called.

            `pid` (str): The pid of the training process.

            `date` (str): A formatted date of when the result was processed.

            `timestamp` (str): A UNIX timestamp of when the result
            was processed. This may be overridden.

            `hostname` (str): Hostname of the machine hosting the training
            process.

            `node_ip` (str): Node ip of the machine hosting the training
            process.

        Returns:
            A dict that describes training progress.
        """
        start = time.time()
        try:
            result = self.step()
        except Exception as e:
            skipped = skip_exceptions(e)
            raise skipped from exception_cause(skipped)

        assert isinstance(result, dict), "step() needs to return a dict."

        # We do not modify internal state nor update this result if duplicate.
        if RESULT_DUPLICATE in result:
            return result

        result = result.copy()

        self._iteration += 1
        self._iterations_since_restore += 1

        if result.get(TIME_THIS_ITER_S) is not None:
            time_this_iter = result[TIME_THIS_ITER_S]
        else:
            time_this_iter = time.time() - start
        self._time_total += time_this_iter
        self._time_since_restore += time_this_iter

        result_timestamp = result.get(TIMESTAMP, None)

        result.setdefault(DONE, False)

        # self._timesteps_total should only be tracked if increments are provided
        if result.get(TIMESTEPS_THIS_ITER) is not None:
            if self._timesteps_total is None:
                self._timesteps_total = 0
            self._timesteps_total += result[TIMESTEPS_THIS_ITER]
            self._timesteps_since_restore += result[TIMESTEPS_THIS_ITER]

        # self._episodes_total should only be tracked if increments provided
        if result.get(EPISODES_THIS_ITER) is not None:
            if self._episodes_total is None:
                self._episodes_total = 0
            self._episodes_total += result[EPISODES_THIS_ITER]

        # self._timesteps_total should not override user-provided total
        if self._timesteps_total is not None:
            result.setdefault(TIMESTEPS_TOTAL, self._timesteps_total)
        if self._episodes_total is not None:
            result.setdefault(EPISODES_TOTAL, self._episodes_total)
        result.setdefault(TRAINING_ITERATION, self._iteration)

        now = datetime.today()
        result.update(
            self.get_auto_filled_metrics(
                now=now, time_this_iter=time_this_iter, timestamp=result_timestamp
            )
        )

        monitor_data = self._monitor.get_data()
        if monitor_data:
            result.update(monitor_data)

        self.log_result(result)

        if self._stdout_context:
            self._stdout_stream.flush()
        if self._stderr_context:
            self._stderr_stream.flush()

        self._last_result = result

        if self._storage:
            # Launch background tasks to sync artifacts at some specified frequency.
            self._storage.persist_artifacts()

        return result

    def get_state(self):
        return {
            "iteration": self._iteration,
            "timesteps_total": self._timesteps_total,
            "time_total": self._time_total,
            "episodes_total": self._episodes_total,
            "last_result": self._last_result,
            "ray_version": ray.__version__,
        }

    def _report_class_trainable_checkpoint(
        self, checkpoint_dir: str, checkpoint_dict_or_path: Union[str, Dict]
    ) -> _TrainingResult:
        """Report a checkpoint saved via Trainable.save_checkpoint.

        Need to handle both dict or path checkpoint returned by the user's
        `save_checkpoint` method.

        This is to get class trainables to work with storage backend used by
        function trainables.
        This basically re-implements `tune.report` for class trainables,
        making sure to persist the checkpoint to storage.
        """
        if isinstance(checkpoint_dict_or_path, dict):
            with Path(checkpoint_dir, _DICT_CHECKPOINT_FILE_NAME).open("wb") as f:
                ray_pickle.dump(checkpoint_dict_or_path, f)
        elif isinstance(checkpoint_dict_or_path, str):
            if checkpoint_dict_or_path != checkpoint_dir:
                raise ValueError(
                    "The returned checkpoint path from `save_checkpoint` "
                    "must be None or the same as the provided path argument."
                    f"Got {checkpoint_dict_or_path} != {checkpoint_dir}"
                )

        local_checkpoint = ray.tune.Checkpoint.from_directory(checkpoint_dir)

        metrics = self._last_result.copy() if self._last_result else {}

        if self._storage:
            # The checkpoint index is updated with the current result.
            # NOTE: This is no longer using "iteration" as the folder indexing
            # to be consistent with fn trainables.
            self._storage._update_checkpoint_index(metrics)

            persisted_checkpoint = self._storage.persist_current_checkpoint(
                local_checkpoint
            )

            checkpoint_result = _TrainingResult(
                checkpoint=persisted_checkpoint, metrics=metrics
            )
            # Persist trial artifacts to storage.
            self._storage.persist_artifacts(
                force=self._storage.sync_config.sync_artifacts_on_checkpoint
            )
        else:
            # `storage=None` only happens when initializing the
            # Trainable manually, outside of Tune/Train.
            # In this case, no storage is set, so the default behavior
            # is to just not upload anything and report a local checkpoint.
            # This is fine for the main use case of local debugging.
            checkpoint_result = _TrainingResult(
                checkpoint=local_checkpoint, metrics=metrics
            )
        return checkpoint_result

    @DeveloperAPI
    def save(self, checkpoint_dir: Optional[str] = None) -> _TrainingResult:
        """Saves the current model state to a checkpoint.

        Subclasses should override ``save_checkpoint()`` instead to save state.

        Args:
            checkpoint_dir: Optional dir to place the checkpoint.

        Returns:
            The given or created checkpoint directory.

        Note the return value matches up with what is expected of `restore()`.
        """
        if not isinstance(self, ray.tune.trainable.FunctionTrainable):
            # Use a temporary directory if no checkpoint_dir is provided.
            use_temp_dir = not checkpoint_dir
            checkpoint_dir = checkpoint_dir or tempfile.mkdtemp()
            os.makedirs(checkpoint_dir, exist_ok=True)

            checkpoint_dict_or_path = self.save_checkpoint(checkpoint_dir)
            checkpoint_result = self._report_class_trainable_checkpoint(
                checkpoint_dir, checkpoint_dict_or_path
            )

            # Clean up the temporary directory, since it's already been
            # reported + persisted to storage. If no storage is set, the user is
            # running the Trainable locally and is responsible for cleaning
            # up the checkpoint directory themselves.
            if use_temp_dir and self._storage:
                shutil.rmtree(checkpoint_dir, ignore_errors=True)
        else:
            checkpoint_result: _TrainingResult = self.save_checkpoint(None)
            assert isinstance(checkpoint_result, _TrainingResult)
            assert self._last_result
            # Update the checkpoint result to include auto-filled metrics.
            checkpoint_result.metrics.update(self._last_result)

        return checkpoint_result

    @DeveloperAPI
    def restore(
        self, checkpoint_path: Union[str, "ray.tune.Checkpoint", _TrainingResult]
    ):
        """Restores training state from a given model checkpoint.

        These checkpoints are returned from calls to save().

        Subclasses should override ``load_checkpoint()`` instead to
        restore state.
        This method restores additional metadata saved with the checkpoint.

        `checkpoint_path` should match with the return from ``save()``.

        Args:
            checkpoint_path: training result that was returned by a
                previous call to `save()`.
        """
        # TODO(justinvyu): This also supports restoring from a Checkpoint object
        # or a path, which are legacy APIs that RLlib depends on.
        # RLlib should remove this dependency since `restore` is a DeveloperAPI.
        if isinstance(checkpoint_path, str):
            checkpoint_path = ray.tune.Checkpoint.from_directory(checkpoint_path)
        if isinstance(checkpoint_path, ray.tune.Checkpoint):
            checkpoint_result = _TrainingResult(checkpoint=checkpoint_path, metrics={})
        else:
            checkpoint_result: _TrainingResult = checkpoint_path

        assert isinstance(checkpoint_result, _TrainingResult), type(checkpoint_result)
        checkpoint = checkpoint_result.checkpoint
        checkpoint_metrics = checkpoint_result.metrics
        self._iteration = checkpoint_metrics.get(TRAINING_ITERATION, 0)
        self._time_total = checkpoint_metrics.get(TIME_TOTAL_S, 0)
        self._time_since_restore = 0.0
        self._iterations_since_restore = 0

        # TODO(justinvyu): This stuff should be moved to rllib.
        self._timesteps_total = checkpoint_metrics.get(TIMESTEPS_TOTAL)
        self._timesteps_since_restore = 0
        self._episodes_total = checkpoint_metrics.get(EPISODES_TOTAL)

        if not _exists_at_fs_path(checkpoint.filesystem, checkpoint.path):
            raise ValueError(
                f"Could not recover from checkpoint as it does not exist on "
                f"storage anymore. "
                f"Got storage fs type `{checkpoint.filesystem.type_name}` and "
                f"path: {checkpoint.path}"
            )

        # TODO(justinvyu): [cls_trainable_support]
        # This is to conform to the public class Trainable `load_checkpoint` API.
        if not isinstance(self, ray.tune.trainable.FunctionTrainable):
            # Need to convert Checkpoint -> local path or dict
            # (depending on what the output of save_checkpoint was)
            with checkpoint.as_directory() as checkpoint_dir:
                checkpoint_path = Path(checkpoint_dir)
                dict_checkpoint_file = checkpoint_path / _DICT_CHECKPOINT_FILE_NAME
                if dict_checkpoint_file.exists():
                    # If this was a dict checkpoint, load it as a dict
                    with open(dict_checkpoint_file, "rb") as f:
                        checkpoint_dict = ray_pickle.load(f)
                    self.load_checkpoint(checkpoint_dict)
                else:
                    self.load_checkpoint(checkpoint_dir)
        else:
            # TODO(justinvyu): The Function Trainable case doesn't conform
            # to the load_checkpoint API at the moment.
            self.load_checkpoint(checkpoint_result)

        self._restored = True

        logger.info(f"Restored on {self._local_ip} from checkpoint: {checkpoint}")

    def export_model(
        self, export_formats: Union[List[str], str], export_dir: Optional[str] = None
    ):
        """Exports model based on export_formats.

        Subclasses should override _export_model() to actually
        export model to local directory.

        Args:
            export_formats: Format or list of (str) formats
                that should be exported.
            export_dir: Optional dir to place the exported model.
                Defaults to self.logdir.

        Returns:
            A dict that maps ExportFormats to successfully exported models.
        """
        if isinstance(export_formats, str):
            export_formats = [export_formats]
        export_dir = export_dir or self.logdir
        return self._export_model(export_formats, export_dir)

    def reset(self, new_config, logger_creator=None, storage=None):
        """Resets trial for use with new config.

        Subclasses should override reset_config() to actually
        reset actor behavior for the new config."""
        self.config = new_config

        self._storage = storage

        trial_info = new_config.pop(TRIAL_INFO, None)
        if trial_info:
            self._trial_info = trial_info

        self._result_logger.flush()
        self._result_logger.close()

        if logger_creator:
            logger.debug("Logger reset.")
            self._create_logger(new_config.copy(), logger_creator)
        else:
            logger.debug(
                "Did not reset logger. Got: "
                f"trainable.reset(logger_creator={logger_creator})."
            )

        stdout_file = new_config.pop(STDOUT_FILE, None)
        stderr_file = new_config.pop(STDERR_FILE, None)

        self._close_logfiles()
        self._open_logfiles(stdout_file, stderr_file)

        success = self.reset_config(new_config)
        if not success:
            return False

        # Reset attributes. Will be overwritten by `restore` if a checkpoint
        # is provided.
        self._iteration = 0
        self._time_total = 0.0
        self._timesteps_total = None
        self._episodes_total = None
        self._time_since_restore = 0.0
        self._timesteps_since_restore = 0
        self._iterations_since_restore = 0
        self._restored = False

        return True

    def reset_config(self, new_config: Dict) -> bool:
        """Resets configuration without restarting the trial.

        This method is optional, but can be implemented to speed up algorithms
        such as PBT, and to allow performance optimizations such as running
        experiments with reuse_actors=True.

        Args:
            new_config: Updated hyperparameter configuration
                for the trainable.

        Returns:
            True if reset was successful else False.
        """
        return False

    def _create_logger(
        self,
        config: Dict[str, Any],
        logger_creator: Callable[[Dict[str, Any]], "Logger"] = None,
    ):
        """Create logger from logger creator.

        Sets _logdir and _result_logger.

        `_logdir` is the **per trial** directory for the Trainable.
        """
        if logger_creator:
            self._result_logger = logger_creator(config)
            self._logdir = self._result_logger.logdir
        else:
            from ray.tune.logger import UnifiedLogger

            logdir_prefix = datetime.today().strftime("%Y-%m-%d_%H-%M-%S")
            try_to_create_directory(DEFAULT_STORAGE_PATH)
            self._logdir = tempfile.mkdtemp(
                prefix=logdir_prefix, dir=DEFAULT_STORAGE_PATH
            )
            self._result_logger = UnifiedLogger(config, self._logdir, loggers=None)

    def _open_logfiles(self, stdout_file, stderr_file):
        """Create loggers. Open stdout and stderr logfiles."""
        if stdout_file:
            stdout_path = (Path(self._logdir) / stdout_file).expanduser().as_posix()
            self._stdout_fp = open(stdout_path, "a+")
            self._stdout_stream = Tee(sys.stdout, self._stdout_fp)
            self._stdout_context = redirect_stdout(self._stdout_stream)
            self._stdout_context.__enter__()

        if stderr_file:
            stderr_path = (Path(self._logdir) / stderr_file).expanduser().as_posix()
            self._stderr_fp = open(stderr_path, "a+")
            self._stderr_stream = Tee(sys.stderr, self._stderr_fp)
            self._stderr_context = redirect_stderr(self._stderr_stream)
            self._stderr_context.__enter__()

            # Add logging handler to root ray logger
            formatter = logging.Formatter(
                "[%(levelname)s %(asctime)s] "
                "%(filename)s: %(lineno)d  "
                "%(message)s"
            )
            self._stderr_logging_handler = logging.StreamHandler(self._stderr_fp)
            self._stderr_logging_handler.setFormatter(formatter)
            ray.logger.addHandler(self._stderr_logging_handler)

    def _close_logfiles(self):
        """Close stdout and stderr logfiles."""
        if self._stderr_logging_handler:
            ray.logger.removeHandler(self._stderr_logging_handler)

        if self._stdout_context:
            self._stdout_stream.flush()
            self._stdout_context.__exit__(None, None, None)
            self._stdout_fp.close()
            self._stdout_context = None
        if self._stderr_context:
            self._stderr_stream.flush()
            self._stderr_context.__exit__(None, None, None)
            self._stderr_fp.close()
            self._stderr_context = None

    def stop(self):
        """Releases all resources used by this trainable.

        Calls ``Trainable.cleanup`` internally. Subclasses should override
        ``Trainable.cleanup`` for custom cleanup procedures.
        """
        self._result_logger.flush()
        self._result_logger.close()
        if self._monitor.is_alive():
            self._monitor.stop()
            self._monitor.join()
        self.cleanup()

        self._close_logfiles()

    @property
    def logdir(self):
        """Directory of the results and checkpoints for this Trainable.

        Note that the current working directory will also be changed to this.
        """
        return self._logdir

    @property
    def trial_name(self):
        """Trial name for the corresponding trial of this Trainable.

        This is not set if not using Tune.

        .. testcode::

            from ray.tune import Trainable

            name = Trainable().trial_name
        """
        if self._trial_info:
            return self._trial_info.trial_name
        else:
            return "default"

    @property
    def trial_id(self):
        """Trial ID for the corresponding trial of this Trainable.

        This is not set if not using Tune.

        .. testcode::

            from ray.tune import Trainable

            trial_id = Trainable().trial_id
        """
        if self._trial_info:
            return self._trial_info.trial_id
        else:
            return "default"

    @property
    def trial_resources(self) -> Optional[PlacementGroupFactory]:
        """Resources currently assigned to the trial of this Trainable.

        This is not set if not using Tune.

        .. testcode::

            from ray.tune import Trainable

            trial_resources = Trainable().trial_resources
        """
        if self._trial_info:
            return self._trial_info.trial_resources
        else:
            return None

    @property
    def iteration(self):
        """Current training iteration.

        This value is automatically incremented every time `train()` is called
        and is automatically inserted into the training result dict.

        """
        return self._iteration

    @property
    def training_iteration(self):
        """Current training iteration (same as `self.iteration`).

        This value is automatically incremented every time `train()` is called
        and is automatically inserted into the training result dict.

        """
        return self._iteration

    def get_config(self):
        """Returns configuration passed in by Tune."""
        return self.config

    def step(self):
        """Subclasses should override this to implement train().

        The return value will be automatically passed to the loggers. Users
        can also return `tune.result.DONE` or `tune.result.SHOULD_CHECKPOINT`
        as a key to manually trigger termination or checkpointing of this
        trial. Note that manual checkpointing only works when subclassing
        Trainables.

        .. versionadded:: 0.8.7

        Returns:
            A dict that describes training progress.

        """
        raise NotImplementedError

    def save_checkpoint(self, checkpoint_dir: str) -> Optional[Dict]:
        """Subclasses should override this to implement ``save()``.

        Warning:
            Do not rely on absolute paths in the implementation of
            ``Trainable.save_checkpoint`` and ``Trainable.load_checkpoint``.

        Use ``validate_save_restore`` to catch ``Trainable.save_checkpoint``/
        ``Trainable.load_checkpoint`` errors before execution.

        >>> from ray.tune.utils import validate_save_restore
        >>> MyTrainableClass = ... # doctest: +SKIP
        >>> validate_save_restore(MyTrainableClass) # doctest: +SKIP

        .. versionadded:: 0.8.7

        Args:
            checkpoint_dir: The directory where the checkpoint
                file must be stored. In a Tune run, if the trial is paused,
                the provided path may be temporary and moved.

        Returns:
            A dict or None. If dict, the return value will
            be automatically serialized by Tune. In that case,
            ``Trainable.load_checkpoint()`` will receive the dict upon restore.

        Example:
            >>> trainable, trainable1, trainable2 = ... # doctest: +SKIP
            >>> print(trainable1.save_checkpoint("/tmp/checkpoint_1")) # doctest: +SKIP
            "/tmp/checkpoint_1"
            >>> print(trainable2.save_checkpoint("/tmp/checkpoint_2")) # doctest: +SKIP
            {"some": "data"}
            >>> trainable.save_checkpoint("/tmp/bad_example") # doctest: +SKIP
            "/tmp/NEW_CHECKPOINT_PATH/my_checkpoint_file" # This will error.
        """
        raise NotImplementedError

    def load_checkpoint(self, checkpoint: Optional[Dict]):
        """Subclasses should override this to implement restore().

        Warning:
            In this method, do not rely on absolute paths. The absolute
            path of the checkpoint_dir used in ``Trainable.save_checkpoint``
            may be changed.

        If ``Trainable.save_checkpoint`` returned a prefixed string, the
        prefix of the checkpoint string returned by
        ``Trainable.save_checkpoint`` may be changed.
        This is because trial pausing depends on temporary directories.

        The directory structure under the checkpoint_dir provided to
        ``Trainable.save_checkpoint`` is preserved.

        See the examples below.

        Example:
            >>> import os
            >>> from ray.tune.trainable import Trainable
            >>> class Example(Trainable):
            ...    def save_checkpoint(self, checkpoint_path):
            ...        my_checkpoint_path = os.path.join(checkpoint_path, "my/path")
            ...        return my_checkpoint_path
            ...    def load_checkpoint(self, my_checkpoint_path):
            ...        print(my_checkpoint_path)
            >>> trainer = Example()
            >>> # This is used when PAUSED.
            >>> checkpoint_result = trainer.save() # doctest: +SKIP
            >>> trainer.restore(checkpoint_result) # doctest: +SKIP

        If `Trainable.save_checkpoint` returned a dict, then Tune will directly pass
        the dict data as the argument to this method.

        Example:
            >>> from ray.tune.trainable import Trainable
            >>> class Example(Trainable):
            ...    def save_checkpoint(self, checkpoint_path):
            ...        return {"my_data": 1}
            ...    def load_checkpoint(self, checkpoint_dict):
            ...        print(checkpoint_dict["my_data"])

        .. versionadded:: 0.8.7

        Args:
            checkpoint: If dict, the return value is as
                returned by ``save_checkpoint``. Otherwise, the directory
                the checkpoint was stored in.
        """
        raise NotImplementedError

    def setup(self, config: Dict):
        """Subclasses should override this for custom initialization.

        .. versionadded:: 0.8.7

        Args:
            config: Hyperparameters and other configs given.
                Copy of `self.config`.

        """
        pass

    def log_result(self, result: Dict):
        """Subclasses can optionally override this to customize logging.

        The logging here is done on the worker process rather than
        the driver.

        .. versionadded:: 0.8.7

        Args:
            result: Training result returned by step().
        """
        self._result_logger.on_result(result)

    def cleanup(self):
        """Subclasses should override this for any cleanup on stop.

        If any Ray actors are launched in the Trainable (i.e., with a RLlib
        trainer), be sure to kill the Ray actor process here.

        This process should be lightweight. Per default,

        You can kill a Ray actor by calling `ray.kill(actor)`
        on the actor or removing all references to it and waiting for garbage
        collection

        .. versionadded:: 0.8.7
        """
        pass

    def _export_model(self, export_formats: List[str], export_dir: str):
        """Subclasses should override this to export model.

        Args:
            export_formats: List of formats that should be exported.
            export_dir: Directory to place exported models.

        Return:
            A dict that maps ExportFormats to successfully exported models.
        """
        return {}

    def _implements_method(self, key):
        return hasattr(self, key) and callable(getattr(self, key))
