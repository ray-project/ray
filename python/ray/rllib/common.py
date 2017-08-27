from collections import namedtuple
from datetime import datetime
import json
import logging
import numpy as np
import os
import sys
import tempfile
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
    "experiment_id",
    "training_iteration",
    "episode_reward_mean",
    "episode_len_mean",
    "info"
])


class Algorithm(object):
    """All RLlib algorithms extend this base class.

    Algorithm objects retain internal model state between calls to train(), so
    you should create a new algorithm instance for each training session.

    Attributes:
        env_name (str): Name of the OpenAI gym environment to train against.
        config (obj): Algorithm-specific configuration data.
        logdir (str): Directory in which training outputs should be placed.

    TODO(ekl): support checkpoint / restore of training state.
    """

    def __init__(self, env_name, config, upload_dir=None):
        """Initialize an RLLib algorithm.

        Args:
            env_name (str): The name of the OpenAI gym environment to use.
            config (obj): Algorithm-specific configuration data.
            upload_dir (str): Root directory into which the output directory
                should be placed. Can be local like file:///tmp/ray/ or on S3
                like s3://bucketname/.
        """
        upload_dir = "file:///tmp/ray" if upload_dir is None else upload_dir
        self.experiment_id = uuid.uuid4()
        self.env_name = env_name
        self.config = config
        self.config.update({"experiment_id": self.experiment_id.hex})
        self.config.update({"env_name": env_name})
        prefix = "{}_{}_{}".format(
            env_name,
            self.__class__.__name__,
            datetime.today().strftime("%Y-%m-%d_%H-%M-%S"))
        if upload_dir.startswith("file"):
            self.logdir = tempfile.mkdtemp(prefix=prefix, dir="/tmp/ray")
        else:
            self.logdir = os.path.join(upload_dir, prefix)
        log_path = os.path.join(self.logdir, "config.json")
        with smart_open.smart_open(log_path, "w") as f:
            json.dump(self.config, f, sort_keys=True, cls=RLLibEncoder)
        logger.info(
            "%s algorithm created with logdir '%s'",
            self.__class__.__name__, self.logdir)

    def train(self):
        """Runs one logical iteration of training.

        Returns:
            A TrainingResult that describes training progress.
        """

        raise NotImplementedError

    def save(self):
        """Saves the current model state to a checkpoint.

        Returns:
            Checkpoint path that may be passed to restore().
        """

        raise NotImplementedError

    def restore(self, checkpoint_path):
        """Restores training state from a given model checkpoint.

        These checkpoints are returned from calls to save().
        """

        raise NotImplementedError

    def compute_action(self, observation):
        """Computes an action using the current trained policy."""

        raise NotImplementedError
