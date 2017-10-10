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

import gym
import smart_open
import tensorflow as tf

if sys.version_info[0] == 2:
    import cStringIO as StringIO
elif sys.version_info[0] == 3:
    import io as StringIO

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def get_tensorflow_log_dir(logdir):
    if logdir.startswith("s3"):
        print("WARNING: TensorFlow logging to S3 not supported by"
              "TensorFlow, logging to /tmp/ray/ instead")
        logdir = "/tmp/ray/"
        if not os.path.exists(logdir):
            os.makedirs(logdir)
    return logdir


class RLLibEncoder(json.JSONEncoder):

    def __init__(self, nan_str="null", **kwargs):
        super(RLLibEncoder, self).__init__(**kwargs)
        self.nan_str = nan_str

    def iterencode(self, o, _one_shot=False):
        if self.ensure_ascii:
            _encoder = json.encoder.encode_basestring_ascii
        else:
            _encoder = json.encoder.encode_basestring

        def floatstr(o, allow_nan=self.allow_nan, nan_str=self.nan_str):
            return repr(o) if not np.isnan(o) else nan_str

        _iterencode = json.encoder._make_iterencode(
                None, self.default, _encoder, self.indent, floatstr,
                self.key_separator, self.item_separator, self.sort_keys,
                self.skipkeys, _one_shot)
        return _iterencode(o, 0)

    def default(self, value):
        if np.isnan(value):
            return None
        if np.issubdtype(value, float):
            return float(value)
        if np.issubdtype(value, int):
            return int(value)


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
        env_creator (func): Function that creates a new training env.
        config (obj): Algorithm-specific configuration data.
        logdir (str): Directory in which training outputs should be placed.
    """

    def __init__(
            self, env_creator, config, upload_dir='/tmp/ray', agent_id=''):
        """Initialize an RLLib agent.

        Args:
            env_creator (str|func): Name of the OpenAI gym environment to train
                against, or a function that creates such an env.
            config (obj): Algorithm-specific configuration data.
            upload_dir (str): Root directory into which the output directory
                should be placed. Can be local like /tmp/ray/ or on S3
                like s3://bucketname/.
            agent_id (str): Optional unique identifier for this agent, used
                to determine where to store results.
        """
        self._experiment_id = uuid.uuid4().hex
        if type(env_creator) is str:
            env_name = env_creator
            self.env_creator = lambda: gym.make(env_name)
        else:
            env_name = "custom"
            self.env_creator = env_creator

        self.config = config
        self.config.update({"experiment_id": self._experiment_id})
        self.config.update({"env_name": env_name})
        self.config.update({"alg": self._agent_name})
        self.config.update({"agent_id": agent_id})
        logdir_prefix = "{}_{}_{}".format(
            env_name,
            self.__class__.__name__,
            agent_id or datetime.today().strftime("%Y-%m-%d_%H-%M-%S"))

        if upload_dir.startswith("/"):
            if not os.path.exists(upload_dir):
                os.makedirs(upload_dir)
            self.logdir = tempfile.mkdtemp(
                prefix=logdir_prefix, dir=local_dir)
        else:
            self.logdir = os.path.join(upload_dir, logdir_prefix)

        if not os.path.exists(self.logdir):
            os.makedirs(self.logdir)

        # TODO(ekl) consider inlining config into the result jsons
        config_out = os.path.join(self.logdir, "config.json")
        with open(config_out, "w") as f:
            json.dump(self.config, f, sort_keys=True, cls=RLLibEncoder)
        logger.info(
            "%s algorithm created with logdir '%s'",
            self.__class__.__name__, self.logdir)

        self._result_logger = RLLibLogger(
            os.path.join(self.logdir, "result.json"))
        self._file_writer = tf.summary.FileWriter(self.logdir)

        self._iteration = 0
        self._time_total = 0.0
        self._timesteps_total = 0

        with tf.Graph().as_default():
            self._init()

    def _init(self, config, env_creator):
        """Subclasses should override this for custom initialization."""

        raise NotImplementedError

    def train(self):
        """Runs one logical iteration of training.

        Returns:
            A TrainingResult that describes training progress.
        """

        start = time.time()
        result = self._train()
        self._iteration += 1
        time_this_iter = time.time() - start

        self._time_total += time_this_iter
        self._timesteps_total += result.timesteps_this_iter

        result = result._replace(
            experiment_id=self._experiment_id,
            training_iteration=self._iteration,
            timesteps_total=self._timesteps_total,
            time_this_iter_s=time_this_iter,
            time_total_s=self._time_total)

        for field in result:
            assert field is not None, result

        self._log_result(result)

        return result

    def _log_result(self, result):
        """Appends the given result to this agent's log dir."""

        # We need to use a custom json serializer class so that NaNs get
        # encoded as null as required by Athena.
        json.dump(result._asdict(), self._result_logger, cls=RLLibEncoder)
        self._result_logger.write("\n")
        train_stats = tf.Summary(value=[
            tf.Summary.Value(
                tag="rllib/time_this_iter_s",
                simple_value=result.time_this_iter_s),
            tf.Summary.Value(
                tag="rllib/episode_reward_mean",
                simple_value=result.episode_reward_mean),
            tf.Summary.Value(
                tag="rllib/episode_len_mean",
                simple_value=result.episode_len_mean)])
        self._file_writer.add_summary(train_stats, result.training_iteration)

    def save(self):
        """Saves the current model state to a checkpoint.

        Returns:
            Checkpoint path that may be passed to restore().
        """

        checkpoint_path = self._save()
        pickle.dump(
            [self._experiment_id, self._iteration, self._timesteps_total,
             self._time_total],
            open(checkpoint_path + ".rllib_metadata", "wb"))
        return checkpoint_path

    def restore(self, checkpoint_path):
        """Restores training state from a given model checkpoint.

        These checkpoints are returned from calls to save().
        """

        self._restore(checkpoint_path)
        metadata = pickle.load(open(checkpoint_path + ".rllib_metadata", "rb"))
        self._experiment_id = metadata[0]
        self._iteration = metadata[1]
        self._timesteps_total = metadata[2]
        self._time_total = metadata[3]

    def stop(self):
        """Releases all resources used by this agent."""

        self._file_writer.close()

    def compute_action(self, observation):
        """Computes an action using the current trained policy."""

        raise NotImplementedError

    @property
    def iteration(self):
        """Current training iter, auto-incremented with each train() call."""

        return self._iteration

    @property
    def _agent_name(self):
        """Subclasses should override this to declare their name."""

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
