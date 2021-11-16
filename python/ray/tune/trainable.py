from contextlib import redirect_stdout, redirect_stderr
import copy
from datetime import datetime
import logging
import os
import platform
import shutil
import sys
import tempfile
import time
from typing import Any, Dict, Optional, Union, Callable
import uuid

import ray
import ray.cloudpickle as pickle
from ray.tune.logger import Logger
from ray.tune.resources import Resources
from ray.tune.result import (
    DEBUG_METRICS, DEFAULT_RESULTS_DIR, HOSTNAME, NODE_IP, PID,
    SHOULD_CHECKPOINT, TIME_THIS_ITER_S, TIME_TOTAL_S, TIMESTEPS_THIS_ITER,
    DONE, TIMESTEPS_TOTAL, EPISODES_THIS_ITER, EPISODES_TOTAL,
    TRAINING_ITERATION, RESULT_DUPLICATE, TRIAL_ID, TRIAL_INFO, STDOUT_FILE,
    STDERR_FILE)
from ray.tune.sync_client import get_sync_client, get_cloud_sync_client
from ray.tune.utils import UtilMonitor
from ray.tune.utils.placement_groups import PlacementGroupFactory
from ray.tune.utils.trainable import TrainableUtil
from ray.tune.utils.log import disable_ipython
from ray.tune.utils.util import Tee
from ray.util.debug import log_once
from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)

SETUP_TIME_THRESHOLD = 10


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

    When using Tune, Tune will convert this class into a Ray actor, which
    runs on a separate process. Tune will also change the current working
    directory of this process to ``self.logdir``.

    This class supports checkpointing to and restoring from remote storage.


    """
    _sync_function_tpl = None

    def __init__(self,
                 config: Dict[str, Any] = None,
                 logger_creator: Callable[[Dict[str, Any]], Logger] = None,
                 remote_checkpoint_dir: Optional[str] = None,
                 sync_function_tpl: Optional[str] = None):
        """Initialize an Trainable.

        Sets up logging and points ``self.logdir`` to a directory in which
        training outputs should be placed.

        Subclasses should prefer defining ``setup()`` instead of overriding
        ``__init__()`` directly.

        Args:
            config (dict): Trainable-specific configuration data. By default
                will be saved as ``self.config``.
            logger_creator (func): Function that creates a ray.tune.Logger
                object. If unspecified, a default logger is created.
            remote_checkpoint_dir (str): Upload directory (S3 or GS path).
            sync_function_tpl (str): Sync function template to use. Defaults
              to `cls._sync_function` (which defaults to `None`).
        """

        self._experiment_id = uuid.uuid4().hex
        self.config = config or {}
        trial_info = self.config.pop(TRIAL_INFO, None)

        if self.is_actor():
            disable_ipython()

        self._result_logger = self._logdir = None
        self._create_logger(self.config, logger_creator)

        self._stdout_context = self._stdout_fp = self._stdout_stream = None
        self._stderr_context = self._stderr_fp = self._stderr_stream = None
        self._stderr_logging_handler = None

        stdout_file = self.config.pop(STDOUT_FILE, None)
        stderr_file = self.config.pop(STDERR_FILE, None)
        self._open_logfiles(stdout_file, stderr_file)

        self._iteration = 0
        self._time_total = 0.0
        self._timesteps_total = None
        self._episodes_total = None
        self._time_since_restore = 0.0
        self._timesteps_since_restore = 0
        self._iterations_since_restore = 0
        self._restored = False
        self._trial_info = trial_info
        self._stdout_file = stdout_file
        self._stderr_file = stderr_file

        start_time = time.time()
        self._local_ip = self.get_current_ip()
        self.setup(copy.deepcopy(self.config))
        setup_time = time.time() - start_time
        if setup_time > SETUP_TIME_THRESHOLD:
            logger.info("Trainable.setup took {:.3f} seconds. If your "
                        "trainable is slow to initialize, consider setting "
                        "reuse_actors=True to reduce actor creation "
                        "overheads.".format(setup_time))
        log_sys_usage = self.config.get("log_sys_usage", False)
        self._monitor = UtilMonitor(start=log_sys_usage)

        self.remote_checkpoint_dir = remote_checkpoint_dir
        self.sync_function_tpl = sync_function_tpl or self._sync_function_tpl
        self.storage_client = None

        if self.uses_cloud_checkpointing:
            self.storage_client = self._create_storage_client()

    @property
    def uses_cloud_checkpointing(self):
        return bool(self.remote_checkpoint_dir)

    def _create_storage_client(self):
        """Returns a storage client."""
        return get_sync_client(
            self.sync_function_tpl) or get_cloud_sync_client(
                self.remote_checkpoint_dir)

    def _storage_path(self, local_path):
        rel_local_path = os.path.relpath(local_path, self.logdir)
        return os.path.join(self.remote_checkpoint_dir, rel_local_path)

    @classmethod
    def default_resource_request(cls, config: Dict[str, Any]) -> \
            Union[Resources, PlacementGroupFactory]:
        """Provides a static resource requirement for the given configuration.

        This can be overridden by sub-classes to set the correct trial resource
        allocation, so the user does not need to.

        .. code-block:: python

            @classmethod
            def default_resource_request(cls, config):
                return PlacementGroupFactory([{"CPU": 1}, {"CPU": 1}]])


        Args:
            config[Dict[str, Any]]: The Trainable's config dict.

        Returns:
            Union[Resources, PlacementGroupFactory]: A Resources object or
                PlacementGroupFactory consumed by Tune for queueing.
        """
        return None

    @classmethod
    def resource_help(cls, config):
        """Returns a help string for configuring this trainable's resources.

        Args:
            config (dict): The Trainer's config dict.
        """
        return ""

    def get_current_ip(self):
        self._local_ip = ray.util.get_node_ip_address()
        return self._local_ip

    def get_auto_filled_metrics(self,
                                now: Optional[datetime] = None,
                                time_this_iter: Optional[float] = None,
                                debug_metrics_only: bool = False) -> dict:
        """Return a dict with metrics auto-filled by the trainable.

        If ``debug_metrics_only`` is True, only metrics that don't
        require at least one iteration will be returned
        (``ray.tune.result.DEBUG_METRICS``).
        """
        if now is None:
            now = datetime.today()
        autofilled = {
            TRIAL_ID: self.trial_id,
            "experiment_id": self._experiment_id,
            "date": now.strftime("%Y-%m-%d_%H-%M-%S"),
            "timestamp": int(time.mktime(now.timetuple())),
            TIME_THIS_ITER_S: time_this_iter,
            TIME_TOTAL_S: self._time_total,
            PID: os.getpid(),
            HOSTNAME: platform.node(),
            NODE_IP: self._local_ip,
            "config": self.config,
            "time_since_restore": self._time_since_restore,
            "timesteps_since_restore": self._timesteps_since_restore,
            "iterations_since_restore": self._iterations_since_restore
        }
        if debug_metrics_only:
            autofilled = {
                k: v
                for k, v in autofilled.items() if k in DEBUG_METRICS
            }
        return autofilled

    def is_actor(self):
        try:
            actor_id = ray.worker.global_worker.actor_id
            return actor_id != actor_id.nil()
        except Exception:
            # If global_worker is not instantiated, we're not in an actor
            return False

    def train_buffered(self,
                       buffer_time_s: float,
                       max_buffer_length: int = 1000):
        """Runs multiple iterations of training.

        Calls ``train()`` internally. Collects and combines multiple results.
        This function will run ``self.train()`` repeatedly until one of
        the following conditions is met: 1) the maximum buffer length is
        reached, 2) the maximum buffer time is reached, or 3) a checkpoint
        was created. Even if the maximum time is reached, it will always
        block until at least one result is received.

        Args:
            buffer_time_s (float): Maximum time to buffer. The next result
                received after this amount of time has passed will return
                the whole buffer.
            max_buffer_length (int): Maximum number of results to buffer.

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

            `experiment_id` (str): Unique string identifier
            for this experiment. This id is preserved
            across checkpoint / restore calls.

            `training_iteration` (int): The index of this
            training iteration, e.g. call to train(). This is incremented
            after `step()` is called.

            `pid` (str): The pid of the training process.

            `date` (str): A formatted date of when the result was processed.

            `timestamp` (str): A UNIX timestamp of when the result
            was processed.

            `hostname` (str): Hostname of the machine hosting the training
            process.

            `node_ip` (str): Node ip of the machine hosting the training
            process.

        Returns:
            A dict that describes training progress.
        """
        start = time.time()
        result = self.step()
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

        result.setdefault(DONE, False)

        # self._timesteps_total should only be tracked if increments provided
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
        result.setdefault(TIMESTEPS_TOTAL, self._timesteps_total)
        result.setdefault(EPISODES_TOTAL, self._episodes_total)
        result.setdefault(TRAINING_ITERATION, self._iteration)

        # Provides auto-filled neg_mean_loss for avoiding regressions
        if result.get("mean_loss"):
            result.setdefault("neg_mean_loss", -result["mean_loss"])

        now = datetime.today()
        result.update(self.get_auto_filled_metrics(now, time_this_iter))

        monitor_data = self._monitor.get_data()
        if monitor_data:
            result.update(monitor_data)

        self.log_result(result)

        if self._stdout_context:
            self._stdout_stream.flush()
        if self._stderr_context:
            self._stderr_stream.flush()

        return result

    def get_state(self):
        return {
            "experiment_id": self._experiment_id,
            "iteration": self._iteration,
            "timesteps_total": self._timesteps_total,
            "time_total": self._time_total,
            "episodes_total": self._episodes_total,
            "ray_version": ray.__version__,
        }

    def save(self, checkpoint_dir=None):
        """Saves the current model state to a checkpoint.

        Subclasses should override ``save_checkpoint()`` instead to save state.
        This method dumps additional metadata alongside the saved path.

        If a remote checkpoint dir is given, this will also sync up to remote
        storage.

        Args:
            checkpoint_dir (str): Optional dir to place the checkpoint.

        Returns:
            str: Checkpoint path or prefix that may be passed to restore().
        """
        checkpoint_dir = TrainableUtil.make_checkpoint_dir(
            checkpoint_dir or self.logdir, index=self.iteration)
        checkpoint = self.save_checkpoint(checkpoint_dir)
        trainable_state = self.get_state()
        checkpoint_path = TrainableUtil.process_checkpoint(
            checkpoint,
            parent_dir=checkpoint_dir,
            trainable_state=trainable_state)

        # Maybe sync to cloud
        self._maybe_save_to_cloud()

        return checkpoint_path

    def _maybe_save_to_cloud(self):
        # Derived classes like the FunctionRunner might call this
        if self.uses_cloud_checkpointing:
            self.storage_client.sync_up(self.logdir,
                                        self.remote_checkpoint_dir)
            self.storage_client.wait()

    def save_to_object(self):
        """Saves the current model state to a Python object.

        It also saves to disk but does not return the checkpoint path.

        Returns:
            Object holding checkpoint data.
        """
        tmpdir = tempfile.mkdtemp("save_to_object", dir=self.logdir)
        checkpoint_path = self.save(tmpdir)
        # Save all files in subtree and delete the tmpdir.
        obj = TrainableUtil.checkpoint_to_object(checkpoint_path)
        shutil.rmtree(tmpdir)
        return obj

    def restore(self, checkpoint_path):
        """Restores training state from a given model checkpoint.

        These checkpoints are returned from calls to save().

        Subclasses should override ``_restore()`` instead to restore state.
        This method restores additional metadata saved with the checkpoint.
        """
        # Maybe sync from cloud
        if self.uses_cloud_checkpointing:
            self.storage_client.sync_down(self.remote_checkpoint_dir,
                                          self.logdir)
            self.storage_client.wait()

        with open(checkpoint_path + ".tune_metadata", "rb") as f:
            metadata = pickle.load(f)
        self._experiment_id = metadata["experiment_id"]
        self._iteration = metadata["iteration"]
        self._timesteps_total = metadata["timesteps_total"]
        self._time_total = metadata["time_total"]
        self._episodes_total = metadata["episodes_total"]
        saved_as_dict = metadata["saved_as_dict"]
        if saved_as_dict:
            with open(checkpoint_path, "rb") as loaded_state:
                checkpoint_dict = pickle.load(loaded_state)
            checkpoint_dict.update(tune_checkpoint_path=checkpoint_path)
            self.load_checkpoint(checkpoint_dict)
        else:
            self.load_checkpoint(checkpoint_path)
        self._time_since_restore = 0.0
        self._timesteps_since_restore = 0
        self._iterations_since_restore = 0
        self._restored = True
        logger.info("Restored on %s from checkpoint: %s",
                    self.get_current_ip(), checkpoint_path)
        state = {
            "_iteration": self._iteration,
            "_timesteps_total": self._timesteps_total,
            "_time_total": self._time_total,
            "_episodes_total": self._episodes_total,
        }
        logger.info("Current state after restoring: %s", state)

    def restore_from_object(self, obj):
        """Restores training state from a checkpoint object.

        These checkpoints are returned from calls to save_to_object().
        """
        tmpdir = tempfile.mkdtemp("restore_from_object", dir=self.logdir)
        checkpoint_path = TrainableUtil.create_from_pickle(obj, tmpdir)
        self.restore(checkpoint_path)
        shutil.rmtree(tmpdir)

    def delete_checkpoint(self, checkpoint_path):
        """Deletes local copy of checkpoint.

        Args:
            checkpoint_path (str): Path to checkpoint.
        """
        try:
            checkpoint_dir = TrainableUtil.find_checkpoint_dir(checkpoint_path)
        except FileNotFoundError:
            # The checkpoint won't exist locally if the
            # trial was rescheduled to another worker.
            logger.debug(
                f"Local checkpoint not found during garbage collection: "
                f"{self.trial_id} - {checkpoint_path}")
            return
        else:
            if self.uses_cloud_checkpointing:
                self.storage_client.delete(self._storage_path(checkpoint_dir))

        if os.path.exists(checkpoint_dir):
            shutil.rmtree(checkpoint_dir)

    def export_model(self, export_formats, export_dir=None):
        """Exports model based on export_formats.

        Subclasses should override _export_model() to actually
        export model to local directory.

        Args:
            export_formats (Union[list,str]): Format or list of (str) formats
                that should be exported.
            export_dir (str): Optional dir to place the exported model.
                Defaults to self.logdir.

        Returns:
            A dict that maps ExportFormats to successfully exported models.
        """
        if isinstance(export_formats, str):
            export_formats = [export_formats]
        export_dir = export_dir or self.logdir
        return self._export_model(export_formats, export_dir)

    def reset(self, new_config, logger_creator=None):
        """Resets trial for use with new config.

        Subclasses should override reset_config() to actually
        reset actor behavior for the new config."""
        self.config = new_config

        trial_info = new_config.pop(TRIAL_INFO, None)
        if trial_info:
            self._trial_info = trial_info

        self._result_logger.flush()
        self._result_logger.close()

        if logger_creator:
            logger.debug("Logger reset.")
            self._create_logger(new_config.copy(), logger_creator)
        else:
            logger.debug("Did not reset logger. Got: "
                         f"trainable.reset(logger_creator={logger_creator}).")

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

    def reset_config(self, new_config):
        """Resets configuration without restarting the trial.

        This method is optional, but can be implemented to speed up algorithms
        such as PBT, and to allow performance optimizations such as running
        experiments with reuse_actors=True.

        Args:
            new_config (dict): Updated hyperparameter configuration
                for the trainable.

        Returns:
            True if reset was successful else False.
        """
        return False

    def _update_resources(
            self, new_resources: Union[PlacementGroupFactory, Resources]):
        """Internal version of ``update_resources``."""
        self._trial_info.trial_resources = new_resources
        return self.update_resources(new_resources)

    def update_resources(
            self, new_resources: Union[PlacementGroupFactory, Resources]):
        """Fires whenever Trainable resources are changed.

        This method will be called before the checkpoint is loaded.

        The current trial resources can also be obtained through
        ``self.trial_resources``.

        Args:
            new_resources (PlacementGroupFactory|Resources):
                Updated resources. Will be a PlacementGroupFactory if
                trial uses placement groups and Resources otherwise.
        """
        return

    def _create_logger(
            self,
            config: Dict[str, Any],
            logger_creator: Callable[[Dict[str, Any]], Logger] = None):
        """Create logger from logger creator.

        Sets _logdir and _result_logger.
        """
        if logger_creator:
            self._result_logger = logger_creator(config)
            self._logdir = self._result_logger.logdir
        else:
            from ray.tune.logger import UnifiedLogger

            logdir_prefix = datetime.today().strftime("%Y-%m-%d_%H-%M-%S")
            ray._private.utils.try_to_create_directory(DEFAULT_RESULTS_DIR)
            self._logdir = tempfile.mkdtemp(
                prefix=logdir_prefix, dir=DEFAULT_RESULTS_DIR)
            self._result_logger = UnifiedLogger(
                config, self._logdir, loggers=None)

    def _open_logfiles(self, stdout_file, stderr_file):
        """Create loggers. Open stdout and stderr logfiles."""
        if stdout_file:
            stdout_path = os.path.expanduser(
                os.path.join(self._logdir, stdout_file))
            self._stdout_fp = open(stdout_path, "a+")
            self._stdout_stream = Tee(sys.stdout, self._stdout_fp)
            self._stdout_context = redirect_stdout(self._stdout_stream)
            self._stdout_context.__enter__()

        if stderr_file:
            stderr_path = os.path.expanduser(
                os.path.join(self._logdir, stderr_file))
            self._stderr_fp = open(stderr_path, "a+")
            self._stderr_stream = Tee(sys.stderr, self._stderr_fp)
            self._stderr_context = redirect_stderr(self._stderr_stream)
            self._stderr_context.__enter__()

            # Add logging handler to root ray logger
            formatter = logging.Formatter("[%(levelname)s %(asctime)s] "
                                          "%(filename)s: %(lineno)d  "
                                          "%(message)s")
            self._stderr_logging_handler = logging.StreamHandler(
                self._stderr_fp)
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

        Tune will automatically sync this folder with the driver if execution
        is distributed.

        Note that the current working directory will also be changed to this.

        """
        return os.path.join(self._logdir, "")

    @property
    def trial_name(self):
        """Trial name for the corresponding trial of this Trainable.

        This is not set if not using Tune.

        .. code-block:: python

            name = self.trial_name
        """
        if self._trial_info:
            return self._trial_info.trial_name
        else:
            return "default"

    @property
    def trial_id(self):
        """Trial ID for the corresponding trial of this Trainable.

        This is not set if not using Tune.

        .. code-block:: python

            trial_id = self.trial_id
        """
        if self._trial_info:
            return self._trial_info.trial_id
        else:
            return "default"

    @property
    def trial_resources(self) -> Union[Resources, PlacementGroupFactory]:
        """Resources currently assigned to the trial of this Trainable.

        This is not set if not using Tune.

        .. code-block:: python

            trial_resources = self.trial_resources
        """
        if self._trial_info:
            return self._trial_info.trial_resources
        else:
            return "default"

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
        if self._implements_method("_train") and log_once("_train"):
            raise DeprecationWarning(
                "Trainable._train is deprecated and is now removed. Override "
                "Trainable.step instead.")
        raise NotImplementedError

    def save_checkpoint(self, tmp_checkpoint_dir):
        """Subclasses should override this to implement ``save()``.

        Warning:
            Do not rely on absolute paths in the implementation of
            ``Trainable.save_checkpoint`` and ``Trainable.load_checkpoint``.

        Use ``validate_save_restore`` to catch ``Trainable.save_checkpoint``/
        ``Trainable.load_checkpoint`` errors before execution.

        >>> from ray.tune.utils import validate_save_restore
        >>> validate_save_restore(MyTrainableClass)
        >>> validate_save_restore(MyTrainableClass, use_object_store=True)

        .. versionadded:: 0.8.7

        Args:
            tmp_checkpoint_dir (str): The directory where the checkpoint
                file must be stored. In a Tune run, if the trial is paused,
                the provided path may be temporary and moved.

        Returns:
            A dict or string. If string, the return value is expected to be
            prefixed by `tmp_checkpoint_dir`. If dict, the return value will
            be automatically serialized by Tune and
            passed to ``Trainable.load_checkpoint()``.

        Examples:
            >>> print(trainable1.save_checkpoint("/tmp/checkpoint_1"))
            "/tmp/checkpoint_1/my_checkpoint_file"
            >>> print(trainable2.save_checkpoint("/tmp/checkpoint_2"))
            {"some": "data"}

            >>> trainable.save_checkpoint("/tmp/bad_example")
            "/tmp/NEW_CHECKPOINT_PATH/my_checkpoint_file" # This will error.
        """
        if self._implements_method("_save") and log_once("_save"):
            raise DeprecationWarning(
                "Trainable._save is deprecated and is now removed. Override "
                "Trainable.save_checkpoint instead.")
        raise NotImplementedError

    def load_checkpoint(self, checkpoint):
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

        See the example below.

        .. code-block:: python

            class Example(Trainable):
                def save_checkpoint(self, checkpoint_path):
                    print(checkpoint_path)
                    return os.path.join(checkpoint_path, "my/check/point")

                def load_checkpoint(self, checkpoint):
                    print(checkpoint)

            >>> trainer = Example()
            >>> obj = trainer.save_to_object()  # This is used when PAUSED.
            <logdir>/tmpc8k_c_6hsave_to_object/checkpoint_0/my/check/point
            >>> trainer.restore_from_object(obj)  # Note the different prefix.
            <logdir>/tmpb87b5axfrestore_from_object/checkpoint_0/my/check/point

        .. versionadded:: 0.8.7

        Args:
            checkpoint (str|dict): If dict, the return value is as
                returned by `save_checkpoint`. If a string, then it is
                a checkpoint path that may have a different prefix than that
                returned by `save_checkpoint`. The directory structure
                underneath the `checkpoint_dir` `save_checkpoint` is preserved.
        """
        if self._implements_method("_restore") and log_once("_restore"):
            raise DeprecationWarning(
                "Trainable._restore is deprecated and is now removed. "
                "Override Trainable.load_checkpoint instead.")
        raise NotImplementedError

    def setup(self, config):
        """Subclasses should override this for custom initialization.

        .. versionadded:: 0.8.7

        Args:
            config (dict): Hyperparameters and other configs given.
                Copy of `self.config`.

        """
        if self._implements_method("_setup") and log_once("_setup"):
            raise DeprecationWarning(
                "Trainable._setup is deprecated and is now removed. Override "
                "Trainable.setup instead.")
        pass

    def log_result(self, result):
        """Subclasses can optionally override this to customize logging.

        The logging here is done on the worker process rather than
        the driver. You may want to turn off driver logging via the
        ``loggers`` parameter in ``tune.run`` when overriding this function.

        .. versionadded:: 0.8.7

        Args:
            result (dict): Training result returned by step().
        """
        if self._implements_method("_log_result") and log_once("_log_result"):
            raise DeprecationWarning(
                "Trainable._log_result is deprecated and is now removed. "
                "Override Trainable.log_result instead.")
        self._result_logger.on_result(result)

    def cleanup(self):
        """Subclasses should override this for any cleanup on stop.

        If any Ray actors are launched in the Trainable (i.e., with a RLlib
        trainer), be sure to kill the Ray actor process here.

        You can kill a Ray actor by calling `actor.__ray_terminate__.remote()`
        on the actor.

        .. versionadded:: 0.8.7
        """
        if self._implements_method("_stop") and log_once("_stop"):
            raise DeprecationWarning(
                "Trainable._stop is deprecated and is now removed. Override "
                "Trainable.cleanup instead.")
        pass

    def _export_model(self, export_formats, export_dir):
        """Subclasses should override this to export model.

        Args:
            export_formats (list): List of formats that should be exported.
            export_dir (str): Directory to place exported models.

        Return:
            A dict that maps ExportFormats to successfully exported models.
        """
        return {}

    def _implements_method(self, key):
        return hasattr(self, key) and callable(getattr(self, key))


@PublicAPI
class DistributedTrainable(Trainable):
    """Common Trainable class for distributed training."""

    def build_config(self, config: Dict):
        """Builds config for distributed training.

        Builds a deep copy of the input config and populates it with
        metadata from this Trainable.

        Useful for passing this Trainable's configs to each distributed
        Trainable instance.
        """
        new_config = copy.deepcopy(config)
        new_config[TRIAL_INFO] = self._trial_info
        new_config[STDOUT_FILE] = self._stdout_file
        new_config[STDERR_FILE] = self._stderr_file
        return new_config
