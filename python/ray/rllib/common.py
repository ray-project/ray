from collections import namedtuple
from datetime import datetime
import json
import logging
import numpy as np
import os
import pickle
import sys
import tempfile
import time
import uuid
import smart_open

if sys.version_info[0] == 2:
    import cStringIO as StringIO
elif sys.version_info[0] == 3:
    import io as StringIO

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class RLLibEncoder(json.JSONEncoder):
    def default(self, value):
        if isinstance(value, np.float32) or isinstance(value, np.float64):
            if np.isnan(value):
                return None
            else:
                return float(value)


class RLLibLogger(object):
    """Writing small amounts of data to S3 with real-time updates.
    """

    def __init__(self, uri):
        self.result_buffer = StringIO.StringIO()
        self.uri = uri

    def write(self, b):
        # TODO(pcm): At the moment we are writing the whole results output from
        # the beginning in each iteration. This will write O(n^2) bytes where n
        # is the number of bytes printed so far. Fix this! This should at least
        # only write the last 5MBs (S3 chunksize).
        with smart_open.smart_open(self.uri, "w") as f:
            self.result_buffer.write(b)
            f.write(self.result_buffer.getvalue())


TrainingResult = namedtuple("TrainingResult", [
    # Unique string identifier for this experiment. This id is preserved
    # across checkpoint / restore calls.
    "experiment_id",

    # The index of this training iteration, e.g. call to train().
    "training_iteration",

    # The mean episode reward reported during this iteration.
    "episode_reward_mean",

    # The mean episode length reported during this iteration.
    "episode_len_mean",

    # Agent-specific metadata to report for this iteration.
    "info",

    # Number of timesteps in the simulator in this iteration.
    "timesteps_this_iter",

    # Accumulated timesteps for this entire experiment.
    "timesteps_total",

    # Time in seconds this iteration took to run.
    "time_this_iter_s",

    # Accumulated time in seconds for this entire experiment.
    "time_total_s",
])

TrainingResult.__new__.__defaults__ = (None,) * len(TrainingResult._fields)


class Agent(object):
    """All RLlib agents extend this base class.

    Agent objects retain internal model state between calls to train(), so
    you should create a new agent instance for each training session.

    Attributes:
        env_name (str): Name of the OpenAI gym environment to train against.
        config (obj): Algorithm-specific configuration data.
        logdir (str): Directory in which training outputs should be placed.
    """

    def __init__(self, env_name, config, upload_dir=None):
        """Initialize an RLLib agent.

        Args:
            env_name (str): The name of the OpenAI gym environment to use.
            config (obj): Algorithm-specific configuration data.
            upload_dir (str): Root directory into which the output directory
                should be placed. Can be local like file:///tmp/ray/ or on S3
                like s3://bucketname/.
        """
        upload_dir = "file:///tmp/ray" if upload_dir is None else upload_dir
        self.experiment_id = uuid.uuid4().hex
        self.env_name = env_name

        self.config = config
        self.config.update({"experiment_id": self.experiment_id})
        self.config.update({"env_name": env_name})
        prefix = "{}_{}_{}".format(
            env_name,
            self.__class__.__name__,
            datetime.today().strftime("%Y-%m-%d_%H-%M-%S"))
        if upload_dir.startswith("file"):
            local_dir = upload_dir[len("file://"):]
            if not os.path.exists(local_dir):
                os.makedirs(local_dir)
            self.logdir = tempfile.mkdtemp(prefix=prefix, dir=local_dir)
        else:
            self.logdir = os.path.join(upload_dir, prefix)

        # TODO(ekl) consider inlining config into the result jsons
        log_path = os.path.join(self.logdir, "config.json")
        with smart_open.smart_open(log_path, "w") as f:
            json.dump(self.config, f, sort_keys=True, cls=RLLibEncoder)
        logger.info(
            "%s algorithm created with logdir '%s'",
            self.__class__.__name__, self.logdir)

        self.iteration = 0
        self.time_total = 0.0
        self.timesteps_total = 0

    def train(self):
        """Runs one logical iteration of training.

        Returns:
            A TrainingResult that describes training progress.
        """

        start = time.time()
        result = self._train()
        self.iteration += 1
        time_this_iter = time.time() - start

        self.time_total += time_this_iter
        self.timesteps_total += result.timesteps_this_iter

        result = result._replace(
            experiment_id=self.experiment_id,
            training_iteration=self.iteration,
            timesteps_total=self.timesteps_total,
            time_this_iter_s=time_this_iter,
            time_total_s=self.time_total)

        for field in result:
            assert field is not None, result

        return result

    def save(self):
        """Saves the current model state to a checkpoint.

        Returns:
            Checkpoint path that may be passed to restore().
        """

        checkpoint_path = self._save()
        pickle.dump(
            [self.experiment_id, self.iteration, self.timesteps_total,
             self.time_total],
            open(checkpoint_path + ".rllib_metadata", "wb"))
        return checkpoint_path

    def restore(self, checkpoint_path):
        """Restores training state from a given model checkpoint.

        These checkpoints are returned from calls to save().
        """

        self._restore(checkpoint_path)
        metadata = pickle.load(open(checkpoint_path + ".rllib_metadata", "rb"))
        self.experiment_id = metadata[0]
        self.iteration = metadata[1]
        self.timesteps_total = metadata[2]
        self.time_total = metadata[3]

    def compute_action(self, observation):
        """Computes an action using the current trained policy."""

        raise NotImplementedError

    def _train(self):
        """Subclasses should override this to implement train()."""

        raise NotImplementedError

    def _save(self):
        """Subclasses should override this to implement save()."""

        raise NotImplementedError

    def _restore(self):
        """Subclasses should override this to implement restore()."""

        raise NotImplementedError
