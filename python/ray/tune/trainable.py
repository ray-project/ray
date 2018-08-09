from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from datetime import datetime

import gzip
import io
import os
import pickle
import shutil
import tempfile
import time
import uuid

import ray
from ray.tune.logger import UnifiedLogger
from ray.tune.result import (DEFAULT_RESULTS_DIR, TIME_THIS_ITER_S,
                             TIMESTEPS_THIS_ITER, DONE, TIMESTEPS_TOTAL)
from ray.tune.trial import Resources


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
    just a `my_train(config, reporter)` function and calling:

    ``register_trainable("my_func", train)``

    to register it for use with Tune. The function will be automatically
    converted to this interface (sans checkpoint functionality).

    Attributes:
        config (obj): The hyperparam configuration for this trial.
        logdir (str): Directory in which training outputs should be placed.
    """

    def __init__(self, config=None, logger_creator=None):
        """Initialize an Trainable.

        Subclasses should prefer defining ``_setup()`` instead of overriding
        ``__init__()`` directly.

        Args:
            config (dict): Trainable-specific configuration data.
            logger_creator (func): Function that creates a ray.tune.Logger
                object. If unspecified, a default logger is created.
        """

        self._initialize_ok = False
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
        self._timesteps_total = 0
        self._setup()
        self._initialize_ok = True
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
            done (bool): training is terminated. Filled only if not provided.
            time_this_iter_s (float): Time in seconds
                this iteration took to run. This may be overriden in order to
                override the system-computed time difference.
            time_total_s (float): Accumulated time in seconds
                for this entire experiment.
            experiment_id (str): Unique string identifier
                for this experiment. This id is preserved
                across checkpoint / restore calls.
            training_iteration (int): The index of this
                training iteration, e.g. call to train().
            pid (str): The pid of the training process.
            date (str): A formatted date of
                when the result was processed.
            timestamp (str): A UNIX timestamp of
                when the result was processed.
            hostname (str): The hostname of the machine
                hosting the training process.
            node_ip (str): The node ip of the machine
                hosting the training process.

        Returns:
            A dict that describes training progress.
        """

        if not self._initialize_ok:
            raise ValueError(
                "Trainable initialization failed, see previous errors")

        start = time.time()
        result = self._train()
        result = result.copy()

        self._iteration += 1

        if result.get(TIME_THIS_ITER_S) is not None:
            time_this_iter = result[TIME_THIS_ITER_S]
        else:
            time_this_iter = time.time() - start
        self._time_total += time_this_iter

        self._timesteps_total += result.get(TIMESTEPS_THIS_ITER, 0)

        result.setdefault(DONE, False)
        result.setdefault(TIMESTEPS_TOTAL, self._timesteps_total)

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
            config=self.config)

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

        checkpoint_path = self._save(checkpoint_dir or self.logdir)
        pickle.dump([
            self._experiment_id, self._iteration, self._timesteps_total,
            self._time_total
        ], open(checkpoint_path + ".tune_metadata", "wb"))
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
                print("Checkpoint size is {} bytes".format(len(compressed)))
            f.write(compressed)

        shutil.rmtree(tmpdir)
        return out.getvalue()

    def restore(self, checkpoint_path):
        """Restores training state from a given model checkpoint.

        These checkpoints are returned from calls to save().

        Subclasses should override ``_restore()`` instead to restore state.
        This method restores additional metadata saved with the checkpoint.
        """

        self._restore(checkpoint_path)
        metadata = pickle.load(open(checkpoint_path + ".tune_metadata", "rb"))
        self._experiment_id = metadata[0]
        self._iteration = metadata[1]
        self._timesteps_total = metadata[2]
        self._time_total = metadata[3]

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

    def stop(self):
        """Releases all resources used by this trainable."""

        if self._initialize_ok:
            self._result_logger.close()
            self._stop()

    def _train(self):
        """Subclasses should override this to implement train().

        Returns:
            A dict that describes training progress."""

        raise NotImplementedError

    def _save(self, checkpoint_dir):
        """Subclasses should override this to implement save()."""

        raise NotImplementedError

    def _restore(self, checkpoint_path):
        """Subclasses should override this to implement restore()."""

        raise NotImplementedError

    def _setup(self):
        """Subclasses should override this for custom initialization."""
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
