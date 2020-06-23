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
    def process_checkpoint(checkpoint, parent_dir, trainable_state):
        saved_as_dict = False
        if isinstance(checkpoint, string_types):
            if not checkpoint.startswith(parent_dir):
                raise ValueError(
                    "The returned checkpoint path must be within the "
                    "given checkpoint dir {}: {}".format(
                        parent_dir, checkpoint))
            checkpoint_path = checkpoint
            if os.path.isdir(checkpoint_path):
                # Add trailing slash to prevent tune metadata from
                # being written outside the directory.
                checkpoint_path = os.path.join(checkpoint_path, "")
        elif isinstance(checkpoint, dict):
            saved_as_dict = True
            checkpoint_path = os.path.join(parent_dir, "checkpoint")
            with open(checkpoint_path, "wb") as f:
                pickle.dump(checkpoint, f)
        else:
            raise ValueError("Returned unexpected type {}. "
                             "Expected str or dict.".format(type(checkpoint)))

        with open(checkpoint_path + ".tune_metadata", "wb") as f:
            trainable_state["saved_as_dict"] = saved_as_dict
            pickle.dump(trainable_state, f)
        return checkpoint_path

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
        name = os.path.relpath(
            os.path.normpath(checkpoint_path), checkpoint_dir)
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
    def make_checkpoint_dir(checkpoint_dir, index):
        """Creates a checkpoint directory within the provided path.

        Args:
            checkpoint_dir (str): Path to checkpoint directory.
            index (str): A subdirectory will be created
                at the checkpoint directory named 'checkpoint_{index}'.
        """
        suffix = "checkpoint"
        if index is not None:
            suffix += "_{}".format(index)
        checkpoint_dir = os.path.join(checkpoint_dir, suffix)

        os.makedirs(checkpoint_dir, exist_ok=True)
        # Drop marker in directory to identify it as a checkpoint dir.
        open(os.path.join(checkpoint_dir, ".is_checkpoint"), "a").close()
        return checkpoint_dir

    @staticmethod
    def create_from_pickle(obj, tmpdir):
        info = pickle.loads(obj)
        data = info["data"]
        checkpoint_path = os.path.join(tmpdir, info["checkpoint_name"])

        for relpath_name, file_contents in data.items():
            path = os.path.join(tmpdir, relpath_name)

            # This may be a subdirectory, hence not just using tmpdir
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "wb") as f:
                f.write(file_contents)
        return checkpoint_path

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


class TrainWorker:
    def __init__(self, trainable_cls, config=None, logger_creator=None):
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
        self._iteration = 0
        self._time_total = 0.0
        self._timesteps_total = None
        self._episodes_total = None
        self._time_since_restore = 0.0
        self._timesteps_since_restore = 0
        self._iterations_since_restore = 0
        self._restored = False

        start_time = time.time()

        self.trainable = trainable_cls(
            copy.deepcopy(self.config), logger_creator)
        self.logdir = self.trainable.logdir

        setup_time = time.time() - start_time
        if setup_time > SETUP_TIME_THRESHOLD:
            logger.info("_setup took {:.3f} seconds. If your trainable is "
                        "slow to initialize, consider setting "
                        "reuse_actors=True to reduce actor creation "
                        "overheads.".format(setup_time))
        self._local_ip = self.get_current_ip()
        log_sys_usage = self.config.get("log_sys_usage", False)
        self._monitor = UtilMonitor(start=log_sys_usage)

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
        result = self.trainable.step()
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

        Subclasses should override ``_save()`` instead to save state.
        This method dumps additional metadata alongside the saved path.

        Args:
            checkpoint_dir (str): Optional dir to place the checkpoint.

        Returns:
            str: Checkpoint path or prefix that may be passed to restore().
        """
        checkpoint_dir = TrainableUtil.make_checkpoint_dir(
            checkpoint_dir, index=self.iteration)
        checkpoint = self.trainable.save(checkpoint_dir)
        trainable_state = self.get_state()
        checkpoint_path = TrainableUtil.process_checkpoint(
            checkpoint,
            parent_dir=checkpoint_dir,
            trainable_state=trainable_state)
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
            self.trainable.restore(checkpoint_dict)
        else:
            self.trainable.restore(checkpoint_path)
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
        return self.trainable.reset_config(new_config)

    def _export_model(self, export_formats, export_dir):
        """Subclasses should override this to export model.

        Args:
            export_formats (list): List of formats that should be exported.
            export_dir (str): Directory to place exported models.

        Return:
            A dict that maps ExportFormats to successfully exported models.
        """
        return {}
