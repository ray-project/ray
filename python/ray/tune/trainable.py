from datetime import datetime

import copy
import io
import logging
import glob
import os
import pickle
import platform
import pandas as pd
from six import string_types
import shutil
import tempfile
import time
import uuid

import ray
from ray.tune.logger import UnifiedLogger
from ray.tune.result import (DEFAULT_RESULTS_DIR, TIME_THIS_ITER_S,
                             TIMESTEPS_THIS_ITER, DONE, TIMESTEPS_TOTAL,
                             EPISODES_THIS_ITER, EPISODES_TOTAL,
                             TRAINING_ITERATION, RESULT_DUPLICATE, TRIAL_INFO)
from ray.tune.utils import UtilMonitor

logger = logging.getLogger(__name__)

SETUP_TIME_THRESHOLD = 10


class TrainableUtil:
    @staticmethod
    def pickle_checkpoint(checkpoint_path):
        """Pickles checkpoint data."""
        checkpoint_dir = TrainableUtil.find_checkpoint_dir(checkpoint_path)
        data = {}
        for basedir, _, file_names in os.walk(checkpoint_dir):
            for file_name in file_names:
                path = os.path.join(basedir, file_name)
                with open(path, "rb") as f:
                    data[os.path.relpath(path, checkpoint_dir)] = f.read()
        # Use normpath so that a directory path isn't mapped to empty string.
        name = os.path.basename(os.path.normpath(checkpoint_path))
        name += os.path.sep if os.path.isdir(checkpoint_path) else ""
        data_dict = pickle.dumps({
            "checkpoint_name": name,
            "data": data,
        })
        return data_dict

    @staticmethod
    def find_checkpoint_dir(checkpoint_path):
        """Returns the directory containing the checkpoint path.

        Raises:
            FileNotFoundError if the directory is not found.
        """
        if not os.path.exists(checkpoint_path):
            raise FileNotFoundError("Path does not exist", checkpoint_path)
        if os.path.isdir(checkpoint_path):
            checkpoint_dir = checkpoint_path
        else:
            checkpoint_dir = os.path.dirname(checkpoint_path)
        while checkpoint_dir != os.path.dirname(checkpoint_dir):
            if os.path.exists(os.path.join(checkpoint_dir, ".is_checkpoint")):
                break
            checkpoint_dir = os.path.dirname(checkpoint_dir)
        else:
            raise FileNotFoundError("Checkpoint directory not found for {}"
                                    .format(checkpoint_path))
        return checkpoint_dir

    @staticmethod
    def make_checkpoint_dir(checkpoint_dir):
        """Creates a checkpoint directory at the provided path."""
        os.makedirs(checkpoint_dir, exist_ok=True)
        # Drop marker in directory to identify it as a checkpoint dir.
        open(os.path.join(checkpoint_dir, ".is_checkpoint"), "a").close()

    @staticmethod
    def get_checkpoints_paths(logdir):
        """ Finds the checkpoints within a specific folder.

        Returns a pandas DataFrame of training iterations and checkpoint
        paths within a specific folder.

        Raises:
            FileNotFoundError if the directory is not found.
        """
        marker_paths = glob.glob(
            os.path.join(logdir, "checkpoint_*/.is_checkpoint"))
        iter_chkpt_pairs = []
        for marker_path in marker_paths:
            chkpt_dir = os.path.dirname(marker_path)
            metadata_file = glob.glob(
                os.path.join(chkpt_dir, "*.tune_metadata"))
            if len(metadata_file) != 1:
                raise ValueError(
                    "{} has zero or more than one tune_metadata.".format(
                        chkpt_dir))

            chkpt_path = metadata_file[0][:-len(".tune_metadata")]
            chkpt_iter = int(chkpt_dir[chkpt_dir.rfind("_") + 1:])
            iter_chkpt_pairs.append([chkpt_iter, chkpt_path])

        chkpt_df = pd.DataFrame(
            iter_chkpt_pairs, columns=["training_iteration", "chkpt_path"])
        return chkpt_df


class Trainable:
    """Abstract class for trainable models, functions, etc.

    A call to ``train()`` on a trainable will execute one logical iteration of
    training. As a rule of thumb, the execution time of one train call should
    be large enough to avoid overheads (i.e. more than a few seconds), but
    short enough to report progress periodically (i.e. at most a few minutes).

    Calling ``save()`` should save the training state of a trainable to disk,
    and ``restore(path)`` should restore a trainable to the given state.

    Generally you only need to implement ``_setup``, ``_train``,
    ``_save``, and ``_restore`` when subclassing Trainable.

    Other implementation methods that may be helpful to override are
    ``_log_result``, ``reset_config``, ``_stop``, and ``_export_model``.

    When using Tune, Tune will convert this class into a Ray actor, which
    runs on a separate process. Tune will also change the current working
    directory of this process to ``self.logdir``.

    """

    def __init__(self, config=None, logger_creator=None):
        """Initialize an Trainable.

        Sets up logging and points ``self.logdir`` to a directory in which
        training outputs should be placed.

        Subclasses should prefer defining ``_setup()`` instead of overriding
        ``__init__()`` directly.

        Args:
            config (dict): Trainable-specific configuration data. By default
                will be saved as ``self.config``.
            logger_creator (func): Function that creates a ray.tune.Logger
                object. If unspecified, a default logger is created.
        """

        self._experiment_id = uuid.uuid4().hex
        self.config = config or {}
        trial_info = self.config.pop(TRIAL_INFO, None)

        if logger_creator:
            self._result_logger = logger_creator(self.config)
            self._logdir = self._result_logger.logdir
        else:
            logdir_prefix = datetime.today().strftime("%Y-%m-%d_%H-%M-%S")
            ray.utils.try_to_create_directory(DEFAULT_RESULTS_DIR)
            self._logdir = tempfile.mkdtemp(
                prefix=logdir_prefix, dir=DEFAULT_RESULTS_DIR)
            self._result_logger = UnifiedLogger(
                self.config, self._logdir, loggers=None)

        self._iteration = 0
        self._time_total = 0.0
        self._timesteps_total = None
        self._episodes_total = None
        self._time_since_restore = 0.0
        self._timesteps_since_restore = 0
        self._iterations_since_restore = 0
        self._restored = False
        self._trial_info = trial_info

        start_time = time.time()
        self._setup(copy.deepcopy(self.config))
        setup_time = time.time() - start_time
        if setup_time > SETUP_TIME_THRESHOLD:
            logger.info("_setup took {:.3f} seconds. If your trainable is "
                        "slow to initialize, consider setting "
                        "reuse_actors=True to reduce actor creation "
                        "overheads.".format(setup_time))
        self._local_ip = self.get_current_ip()
        log_sys_usage = self.config.get("log_sys_usage", False)
        self._monitor = UtilMonitor(start=log_sys_usage)

    @classmethod
    def default_resource_request(cls, config):
        """Provides a static resource requirement for the given configuration.

        This can be overridden by sub-classes to set the correct trial resource
        allocation, so the user does not need to.

        .. code-block:: python

            @classmethod
            def default_resource_request(cls, config):
                return Resources(
                    cpu=0,
                    gpu=0,
                    extra_cpu=config["workers"],
                    extra_gpu=int(config["use_gpu"]) * config["workers"])

        Returns:
            Resources: A Resources object consumed by Tune for queueing.
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
        self._local_ip = ray.services.get_node_ip_address()
        return self._local_ip

    def train(self):
        """Runs one logical iteration of training.

        Subclasses should override ``_train()`` instead to return results.
        This class automatically fills the following fields in the result:

            `done` (bool): training is terminated. Filled only if not provided.

            `time_this_iter_s` (float): Time in seconds this iteration
            took to run. This may be overriden in order to override the
            system-computed time difference.

            `time_total_s` (float): Accumulated time in seconds for this
            entire experiment.

            `experiment_id` (str): Unique string identifier
            for this experiment. This id is preserved
            across checkpoint / restore calls.

            `training_iteration` (int): The index of this
            training iteration, e.g. call to train(). This is incremented
            after `_train()` is called.

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
        result = self._train()
        assert isinstance(result, dict), "_train() needs to return a dict."

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
        result.update(
            experiment_id=self._experiment_id,
            date=now.strftime("%Y-%m-%d_%H-%M-%S"),
            timestamp=int(time.mktime(now.timetuple())),
            time_this_iter_s=time_this_iter,
            time_total_s=self._time_total,
            pid=os.getpid(),
            hostname=platform.node(),
            node_ip=self._local_ip,
            config=self.config,
            time_since_restore=self._time_since_restore,
            timesteps_since_restore=self._timesteps_since_restore,
            iterations_since_restore=self._iterations_since_restore)

        monitor_data = self._monitor.get_data()
        if monitor_data:
            result.update(monitor_data)

        self._log_result(result)

        return result

    def save(self, checkpoint_dir=None):
        """Saves the current model state to a checkpoint.

        Subclasses should override ``_save()`` instead to save state.
        This method dumps additional metadata alongside the saved path.

        Args:
            checkpoint_dir (str): Optional dir to place the checkpoint.

        Returns:
            str: Checkpoint path or prefix that may be passed to restore().
        """
        checkpoint_dir = os.path.join(checkpoint_dir or self.logdir,
                                      "checkpoint_{}".format(self._iteration))
        TrainableUtil.make_checkpoint_dir(checkpoint_dir)
        checkpoint = self._save(checkpoint_dir)
        saved_as_dict = False
        if isinstance(checkpoint, string_types):
            if not checkpoint.startswith(checkpoint_dir):
                raise ValueError(
                    "The returned checkpoint path must be within the "
                    "given checkpoint dir {}: {}".format(
                        checkpoint_dir, checkpoint))
            checkpoint_path = checkpoint
            if os.path.isdir(checkpoint_path):
                # Add trailing slash to prevent tune metadata from
                # being written outside the directory.
                checkpoint_path = os.path.join(checkpoint_path, "")
        elif isinstance(checkpoint, dict):
            saved_as_dict = True
            checkpoint_path = os.path.join(checkpoint_dir, "checkpoint")
            with open(checkpoint_path, "wb") as f:
                pickle.dump(checkpoint, f)
        else:
            raise ValueError("Returned unexpected type {}. "
                             "Expected str or dict.".format(type(checkpoint)))

        with open(checkpoint_path + ".tune_metadata", "wb") as f:
            pickle.dump({
                "experiment_id": self._experiment_id,
                "iteration": self._iteration,
                "timesteps_total": self._timesteps_total,
                "time_total": self._time_total,
                "episodes_total": self._episodes_total,
                "saved_as_dict": saved_as_dict,
                "ray_version": ray.__version__,
            }, f)
        return checkpoint_path

    def save_to_object(self):
        """Saves the current model state to a Python object.

        It also saves to disk but does not return the checkpoint path.

        Returns:
            Object holding checkpoint data.
        """
        tmpdir = tempfile.mkdtemp("save_to_object", dir=self.logdir)
        checkpoint_path = self.save(tmpdir)
        # Save all files in subtree.
        data_dict = TrainableUtil.pickle_checkpoint(checkpoint_path)
        out = io.BytesIO()
        if len(data_dict) > 10e6:  # getting pretty large
            logger.info("Checkpoint size is {} bytes".format(len(data_dict)))
        out.write(data_dict)
        shutil.rmtree(tmpdir)
        return out.getvalue()

    def restore(self, checkpoint_path):
        """Restores training state from a given model checkpoint.

        These checkpoints are returned from calls to save().

        Subclasses should override ``_restore()`` instead to restore state.
        This method restores additional metadata saved with the checkpoint.
        """
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
            self._restore(checkpoint_dict)
        else:
            self._restore(checkpoint_path)
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
        info = pickle.loads(obj)
        data = info["data"]
        tmpdir = tempfile.mkdtemp("restore_from_object", dir=self.logdir)
        checkpoint_path = os.path.join(tmpdir, info["checkpoint_name"])

        for relpath_name, file_contents in data.items():
            path = os.path.join(tmpdir, relpath_name)

            # This may be a subdirectory, hence not just using tmpdir
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "wb") as f:
                f.write(file_contents)

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
            logger.debug("Checkpoint not found during garbage collection.")
            return
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

    def reset_config(self, new_config):
        """Resets configuration without restarting the trial.

        This method is optional, but can be implemented to speed up algorithms
        such as PBT, and to allow performance optimizations such as running
        experiments with reuse_actors=True. Note that self.config need to
        be updated to reflect the latest parameter information in Ray logs.

        Args:
            new_config (dir): Updated hyperparameter configuration
                for the trainable.

        Returns:
            True if reset was successful else False.
        """
        return False

    def stop(self):
        """Releases all resources used by this trainable."""
        self._result_logger.flush()
        self._result_logger.close()
        self._stop()

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
        return self._trial_info.trial_name

    @property
    def trial_id(self):
        """Trial ID for the corresponding trial of this Trainable.

        This is not set if not using Tune.

        .. code-block:: python

            trial_id = self.trial_id
        """
        return self._trial_info.trial_id

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

    def _train(self):
        """Subclasses should override this to implement train().

        The return value will be automatically passed to the loggers. Users
        can also return `tune.result.DONE` or `tune.result.SHOULD_CHECKPOINT`
        as a key to manually trigger termination or checkpointing of this
        trial. Note that manual checkpointing only works when subclassing
        Trainables.

        Returns:
            A dict that describes training progress.

        """

        raise NotImplementedError

    def _save(self, tmp_checkpoint_dir):
        """Subclasses should override this to implement ``save()``.

        Warning:
            Do not rely on absolute paths in the implementation of ``_save``
            and ``_restore``.

        Use ``validate_save_restore`` to catch ``_save``/``_restore`` errors
        before execution.

        >>> from ray.tune.utils import validate_save_restore
        >>> validate_save_restore(MyTrainableClass)
        >>> validate_save_restore(MyTrainableClass, use_object_store=True)

        Args:
            tmp_checkpoint_dir (str): The directory where the checkpoint
                file must be stored. In a Tune run, if the trial is paused,
                the provided path may be temporary and moved.

        Returns:
            A dict or string. If string, the return value is expected to be
            prefixed by `tmp_checkpoint_dir`. If dict, the return value will
            be automatically serialized by Tune and passed to `_restore()`.

        Examples:
            >>> print(trainable1._save("/tmp/checkpoint_1"))
            "/tmp/checkpoint_1/my_checkpoint_file"
            >>> print(trainable2._save("/tmp/checkpoint_2"))
            {"some": "data"}

            >>> trainable._save("/tmp/bad_example")
            "/tmp/NEW_CHECKPOINT_PATH/my_checkpoint_file" # This will error.
        """

        raise NotImplementedError

    def _restore(self, checkpoint):
        """Subclasses should override this to implement restore().

        Warning:
            In this method, do not rely on absolute paths. The absolute
            path of the checkpoint_dir used in ``_save`` may be changed.

        If ``_save`` returned a prefixed string, the prefix of the checkpoint
        string returned by ``_save`` may be changed. This is because trial
        pausing depends on temporary directories.

        The directory structure under the checkpoint_dir provided to ``_save``
        is preserved.

        See the example below.

        .. code-block:: python

            class Example(Trainable):
                def _save(self, checkpoint_path):
                    print(checkpoint_path)
                    return os.path.join(checkpoint_path, "my/check/point")

                def _restore(self, checkpoint):
                    print(checkpoint)

            >>> trainer = Example()
            >>> obj = trainer.save_to_object()  # This is used when PAUSED.
            <logdir>/tmpc8k_c_6hsave_to_object/checkpoint_0/my/check/point
            >>> trainer.restore_from_object(obj)  # Note the different prefix.
            <logdir>/tmpb87b5axfrestore_from_object/checkpoint_0/my/check/point


        Args:
            checkpoint (str|dict): If dict, the return value is as
                returned by `_save`. If a string, then it is a checkpoint path
                that may have a different prefix than that returned by `_save`.
                The directory structure underneath the `checkpoint_dir`
                `_save` is preserved.
        """

        raise NotImplementedError

    def _setup(self, config):
        """Subclasses should override this for custom initialization.

        Args:
            config (dict): Hyperparameters and other configs given.
                Copy of `self.config`.
        """
        pass

    def _log_result(self, result):
        """Subclasses can optionally override this to customize logging.

        The logging here is done on the worker process rather than
        the driver. You may want to turn off driver logging via the
        ``loggers`` parameter in ``tune.run`` when overriding this function.

        Args:
            result (dict): Training result returned by _train().
        """
        self._result_logger.on_result(result)

    def _stop(self):
        """Subclasses should override this for any cleanup on stop.

        If any Ray actors are launched in the Trainable (i.e., with a RLlib
        trainer), be sure to kill the Ray actor process here.

        You can kill a Ray actor by calling `actor.__ray_terminate__.remote()`
        on the actor.
        """
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
