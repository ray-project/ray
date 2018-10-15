from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from datetime import datetime

import copy
import gzip
import io
import logging
import os
import pickle
import shutil
import tempfile
import time
import uuid

import ray
from ray.tune.logger import UnifiedLogger
from ray.tune.result import (DEFAULT_RESULTS_DIR, TIME_THIS_ITER_S,
                             TIMESTEPS_THIS_ITER, DONE, TIMESTEPS_TOTAL,
                             EPISODES_THIS_ITER, EPISODES_TOTAL)
from ray.tune.trial import Resources

logger = logging.getLogger(__name__)


class Trainable(object):
    """Abstract class for trainable models, functions, etc.

    A call to ``train()`` on a trainable will execute one logical iteration of
    training. As a rule of thumb, the execution time of one train call should
    be large enough to avoid overheads (i.e. more than a few seconds), but
    short enough to report progress periodically (i.e. at most a few minutes).

    Calling ``save()`` should save the training state of a trainable to disk,
    and ``restore(path)`` should restore a trainable to the given state.

    Generally you only need to implement ``_train``, ``_save``, and
    ``_restore`` here when subclassing Trainable.

    Note that, if you don't require checkpoint/restore functionality, then
    instead of implementing this class you can also get away with supplying
    just a ``my_train(config, reporter)`` function to the config.
    The function will be automatically converted to this interface
    (sans checkpoint functionality).
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

        if logger_creator:
            self._result_logger = logger_creator(self.config)
            self.logdir = self._result_logger.logdir
        else:
            logdir_prefix = datetime.today().strftime("%Y-%m-%d_%H-%M-%S")
            if not os.path.exists(DEFAULT_RESULTS_DIR):
                os.makedirs(DEFAULT_RESULTS_DIR)
            self.logdir = tempfile.mkdtemp(
                prefix=logdir_prefix, dir=DEFAULT_RESULTS_DIR)
            self._result_logger = UnifiedLogger(self.config, self.logdir, None)

        self._iteration = 0
        self._time_total = 0.0
        self._timesteps_total = None
        self._episodes_total = None
        self._time_since_restore = 0.0
        self._timesteps_since_restore = 0
        self._iterations_since_restore = 0
        self._restored = False
        self._setup(copy.deepcopy(self.config))
        self._local_ip = ray.services.get_node_ip_address()

    @classmethod
    def default_resource_request(cls, config):
        """Returns the resource requirement for the given configuration.

        This can be overriden by sub-classes to set the correct trial resource
        allocation, so the user does not need to.
        """

        return Resources(cpu=1, gpu=0)

    @classmethod
    def resource_help(cls, config):
        """Returns a help string for configuring this trainable's resources."""

        return ""

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
            training iteration, e.g. call to train().

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
        if result.get(TIMESTEPS_THIS_ITER):
            if self._timesteps_total is None:
                self._timesteps_total = 0
            self._timesteps_total += result[TIMESTEPS_THIS_ITER]
            self._timesteps_since_restore += result[TIMESTEPS_THIS_ITER]

        # self._timesteps_total should only be tracked if increments provided
        if result.get(EPISODES_THIS_ITER):
            if self._episodes_total is None:
                self._episodes_total = 0
            self._episodes_total += result[EPISODES_THIS_ITER]

        # self._timesteps_total should not override user-provided total
        result.setdefault(TIMESTEPS_TOTAL, self._timesteps_total)
        result.setdefault(EPISODES_TOTAL, self._episodes_total)

        # Provides auto-filled neg_mean_loss for avoiding regressions
        if result.get("mean_loss"):
            result.setdefault("neg_mean_loss", -result["mean_loss"])

        now = datetime.today()
        result.update(
            experiment_id=self._experiment_id,
            date=now.strftime("%Y-%m-%d_%H-%M-%S"),
            timestamp=int(time.mktime(now.timetuple())),
            training_iteration=self._iteration,
            time_this_iter_s=time_this_iter,
            time_total_s=self._time_total,
            pid=os.getpid(),
            hostname=os.uname()[1],
            node_ip=self._local_ip,
            config=self.config,
            time_since_restore=self._time_since_restore,
            timesteps_since_restore=self._timesteps_since_restore,
            iterations_since_restore=self._iterations_since_restore)

        self._result_logger.on_result(result)

        return result

    def save(self, checkpoint_dir=None):
        """Saves the current model state to a checkpoint.

        Subclasses should override ``_save()`` instead to save state.
        This method dumps additional metadata alongside the saved path.

        Args:
            checkpoint_dir (str): Optional dir to place the checkpoint.

        Returns:
            Checkpoint path that may be passed to restore().
        """

        checkpoint_path = tempfile.mkdtemp(
            prefix="checkpoint_{}".format(self._iteration),
            dir=checkpoint_dir or self.logdir)
        checkpoint = self._save(checkpoint_path)
        saved_as_dict = False
        if isinstance(checkpoint, str):
            checkpoint_path = checkpoint
        elif isinstance(checkpoint, dict):
            saved_as_dict = True
            pickle.dump(checkpoint, open(checkpoint_path + ".tune_state",
                                         "wb"))
        else:
            raise ValueError("Return value from `_save` must be dict or str.")
        pickle.dump({
            "experiment_id": self._experiment_id,
            "iteration": self._iteration,
            "timesteps_total": self._timesteps_total,
            "time_total": self._time_total,
            "episodes_total": self._episodes_total,
            "saved_as_dict": saved_as_dict
        }, open(checkpoint_path + ".tune_metadata", "wb"))
        return checkpoint_path

    def save_to_object(self):
        """Saves the current model state to a Python object. It also
        saves to disk but does not return the checkpoint path.

        Returns:
            Object holding checkpoint data.
        """

        tmpdir = tempfile.mkdtemp("save_to_object", dir=self.logdir)
        checkpoint_prefix = self.save(tmpdir)

        data = {}
        base_dir = os.path.dirname(checkpoint_prefix)
        for path in os.listdir(base_dir):
            path = os.path.join(base_dir, path)
            if path.startswith(checkpoint_prefix):
                data[os.path.basename(path)] = open(path, "rb").read()

        out = io.BytesIO()
        with gzip.GzipFile(fileobj=out, mode="wb") as f:
            compressed = pickle.dumps({
                "checkpoint_name": os.path.basename(checkpoint_prefix),
                "data": data,
            })
            if len(compressed) > 10e6:  # getting pretty large
                logger.info("Checkpoint size is {} bytes".format(
                    len(compressed)))
            f.write(compressed)

        shutil.rmtree(tmpdir)
        return out.getvalue()

    def restore(self, checkpoint_path):
        """Restores training state from a given model checkpoint.

        These checkpoints are returned from calls to save().

        Subclasses should override ``_restore()`` instead to restore state.
        This method restores additional metadata saved with the checkpoint.
        """

        metadata = pickle.load(open(checkpoint_path + ".tune_metadata", "rb"))
        self._experiment_id = metadata["experiment_id"]
        self._iteration = metadata["iteration"]
        self._timesteps_total = metadata["timesteps_total"]
        self._time_total = metadata["time_total"]
        self._episodes_total = metadata["episodes_total"]
        saved_as_dict = metadata["saved_as_dict"]
        if saved_as_dict:
            with open(checkpoint_path + ".tune_state", "rb") as loaded_state:
                checkpoint_dict = pickle.load(loaded_state)
            self._restore(checkpoint_dict)
        else:
            self._restore(checkpoint_path)
        self._restored = True

    def restore_from_object(self, obj):
        """Restores training state from a checkpoint object.

        These checkpoints are returned from calls to save_to_object().
        """

        out = io.BytesIO(obj)
        info = pickle.loads(gzip.GzipFile(fileobj=out, mode="rb").read())
        data = info["data"]
        tmpdir = tempfile.mkdtemp("restore_from_object", dir=self.logdir)
        checkpoint_path = os.path.join(tmpdir, info["checkpoint_name"])

        for file_name, file_contents in data.items():
            with open(os.path.join(tmpdir, file_name), "wb") as f:
                f.write(file_contents)

        self.restore(checkpoint_path)
        shutil.rmtree(tmpdir)

    def reset_config(self, new_config):
        """Resets configuration without restarting the trial.

        Args:
            new_config (dir): Updated hyperparameter configuration
                for the trainable.

        Returns:
            True if configuration reset successfully else False.
        """
        return False

    def stop(self):
        """Releases all resources used by this trainable."""

        self._result_logger.close()
        self._stop()

    def _train(self):
        """Subclasses should override this to implement train().

        Returns:
            A dict that describes training progress."""

        raise NotImplementedError

    def _save(self, checkpoint_dir):
        """Subclasses should override this to implement save().

        Args:
            checkpoint_dir (str): The directory where the checkpoint
                can be stored.

        Returns:
            checkpoint (str | dict): If string, the return value is
                expected to be the checkpoint path that will be passed to
                `_restore()`. If dict, the return value will be automatically
                serialized by Tune and passed to `_restore()`.

        Examples:
            >>> checkpoint_data = trainable._save(checkpoint_dir)
            >>> trainable2._restore(checkpoint_data)
        """

        raise NotImplementedError

    def _restore(self, checkpoint):
        """Subclasses should override this to implement restore().

        Args:
            checkpoint (str | dict): Value as returned by `_save`.
                If a string, then it is the checkpoint path.
        """

        raise NotImplementedError

    def _setup(self, config):
        """Subclasses should override this for custom initialization.

        Args:
            config (dict): Hyperparameters and other configs given.
                Copy of `self.config`.
        """
        pass

    def _stop(self):
        """Subclasses should override this for any cleanup on stop."""
        pass


def wrap_function(train_func):
    from ray.tune.function_runner import FunctionRunner

    class WrappedFunc(FunctionRunner):
        def _trainable_func(self):
            return train_func

    return WrappedFunc
