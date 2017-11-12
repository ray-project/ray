from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

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

import tensorflow as tf
from ray.tune.result import TrainingResult

if sys.version_info[0] == 2:
    import cStringIO as StringIO
elif sys.version_info[0] == 3:
    import io as StringIO

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class Agent(object):
    """All RLlib agents extend this base class.

    Agent objects retain internal model state between calls to train(), so
    you should create a new agent instance for each training session.

    Attributes:
        env_creator (func): Function that creates a new training env.
        config (obj): Algorithm-specific configuration data.
        logdir (str): Directory in which training outputs should be placed.
    """

    _allow_unknown_configs = False

    def __init__(
            self, env_creator, config, local_dir='/tmp/ray',
            upload_dir=None, experiment_tag=None):
        """Initialize an RLLib agent.

        Args:
            env_creator (str|func): Name of the OpenAI gym environment to train
                against, or a function that creates such an env.
            config (obj): Algorithm-specific configuration data.
            local_dir (str): Directory where results and temporary files will
                be placed.
            upload_dir (str): Optional remote URI like s3://bucketname/ where
                results will be uploaded.
            experiment_tag (str): Optional string containing extra metadata
                about the experiment, e.g. a summary of parameters. This string
                will be included in the logdir path and when displaying agent
                progress.
        """
        self._initialize_ok = False
        self._experiment_id = uuid.uuid4().hex
        if type(env_creator) is str:
            import gym
            env_name = env_creator
            self.env_creator = lambda: gym.make(env_name)
        else:
            if hasattr(env_creator, "env_name"):
                env_name = env_creator.env_name
            else:
                env_name = "custom"
            self.env_creator = env_creator

        self.config = self._default_config.copy()
        if not self._allow_unknown_configs:
            for k in config.keys():
                if k not in self.config:
                    raise Exception(
                        "Unknown agent config `{}`, "
                        "all agent configs: {}".format(k, self.config.keys()))
        self.config.update(config)
        self.config.update({
            "experiment_tag": experiment_tag,
            "alg": self._agent_name,
            "env_name": env_name,
            "experiment_id": self._experiment_id,
        })

        logdir_suffix = "{}_{}_{}".format(
            env_name,
            self._agent_name,
            experiment_tag or datetime.today().strftime("%Y-%m-%d_%H-%M-%S"))

        if not os.path.exists(local_dir):
            os.makedirs(local_dir)

        self.logdir = tempfile.mkdtemp(prefix=logdir_suffix, dir=local_dir)

        if upload_dir:
            log_upload_uri = os.path.join(upload_dir, logdir_suffix)
        else:
            log_upload_uri = None

        # TODO(ekl) consider inlining config into the result jsons
        config_out = os.path.join(self.logdir, "config.json")
        with open(config_out, "w") as f:
            json.dump(self.config, f, sort_keys=True, cls=_Encoder)
        logger.info(
            "%s agent created with logdir '%s' and upload uri '%s'",
            self.__class__.__name__, self.logdir, log_upload_uri)

        self._result_logger = _Logger(
            os.path.join(self.logdir, "result.json"),
            log_upload_uri and os.path.join(log_upload_uri, "result.json"))
        self._file_writer = tf.summary.FileWriter(self.logdir)

        self._iteration = 0
        self._time_total = 0.0
        self._timesteps_total = 0

        with tf.Graph().as_default():
            self._init()

        self._initialize_ok = True

    def _init(self, config, env_creator):
        """Subclasses should override this for custom initialization."""

        raise NotImplementedError

    def train(self):
        """Runs one logical iteration of training.

        Returns:
            A TrainingResult that describes training progress.
        """

        if not self._initialize_ok:
            raise ValueError(
                "Agent initialization failed, see previous errors")

        start = time.time()
        result = self._train()
        self._iteration += 1
        if result.time_this_iter_s is not None:
            time_this_iter = result.time_this_iter_s
        else:
            time_this_iter = time.time() - start

        assert result.timesteps_this_iter is not None

        self._time_total += time_this_iter
        self._timesteps_total += result.timesteps_this_iter

        result = result._replace(
            experiment_id=self._experiment_id,
            training_iteration=self._iteration,
            timesteps_total=self._timesteps_total,
            time_this_iter_s=time_this_iter,
            time_total_s=self._time_total,
            pid=os.getpid(),
            hostname=os.uname()[1])

        self._log_result(result)

        return result

    def _log_result(self, result):
        """Appends the given result to this agent's log dir."""

        # We need to use a custom json serializer class so that NaNs get
        # encoded as null as required by Athena.
        json.dump(result._asdict(), self._result_logger, cls=_Encoder)
        self._result_logger.write("\n")
        attrs_to_log = [
            "time_this_iter_s", "mean_loss", "mean_accuracy",
            "episode_reward_mean", "episode_len_mean"]
        values = []
        for attr in attrs_to_log:
            if getattr(result, attr) is not None:
                values.append(tf.Summary.Value(
                    tag="ray/tune/{}".format(attr),
                    simple_value=getattr(result, attr)))
        train_stats = tf.Summary(value=values)
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

    @property
    def _default_config(self):
        """Subclasses should override this to declare their default config."""

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


class _Encoder(json.JSONEncoder):

    def __init__(self, nan_str="null", **kwargs):
        super(_Encoder, self).__init__(**kwargs)
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


class _Logger(object):
    """Writing small amounts of data to S3 with real-time updates.
    """

    def __init__(self, local_file, uri=None):
        self.local_out = open(local_file, "w")
        self.result_buffer = StringIO.StringIO()
        self.uri = uri
        if self.uri:
            import smart_open
            self.smart_open = smart_open.smart_open

    def write(self, b):
        self.local_out.write(b)
        self.local_out.flush()
        # TODO(pcm): At the moment we are writing the whole results output from
        # the beginning in each iteration. This will write O(n^2) bytes where n
        # is the number of bytes printed so far. Fix this! This should at least
        # only write the last 5MBs (S3 chunksize).
        if self.uri:
            with self.smart_open(self.uri, "w") as f:
                self.result_buffer.write(b)
                f.write(self.result_buffer.getvalue())


class _MockAgent(Agent):
    """Mock agent for use in tests"""

    _agent_name = "MockAgent"
    _default_config = {}

    def _init(self):
        self.info = None

    def _train(self):
        return TrainingResult(
            episode_reward_mean=10, episode_len_mean=10,
            timesteps_this_iter=10, info={})

    def _save(self):
        path = os.path.join(self.logdir, "mock_agent.pkl")
        with open(path, 'wb') as f:
            pickle.dump(self.info, f)
        return path

    def _restore(self, checkpoint_path):
        with open(checkpoint_path, 'rb') as f:
            info = pickle.load(f)
        self.info = info

    def set_info(self, info):
        self.info = info
        return info

    def get_info(self):
        return self.info


class _SigmoidFakeData(_MockAgent):
    """Agent that returns sigmoid learning curves.

    This can be helpful for evaluating early stopping algorithms."""

    _agent_name = "SigmoidFakeData"
    _default_config = {
        "width": 100,
        "height": 100,
        "offset": 0,
        "iter_time": 10,
        "iter_timesteps": 1,
    }

    def _train(self):
        i = max(0, self.iteration - self.config["offset"])
        v = np.tanh(float(i) / self.config["width"])
        v *= self.config["height"]
        return TrainingResult(
            episode_reward_mean=v, episode_len_mean=v,
            timesteps_this_iter=self.config["iter_timesteps"],
            time_this_iter_s=self.config["iter_time"], info={})


def get_agent_class(alg):
    """Returns the class of an known agent given its name."""

    if alg == "PPO":
        from ray.rllib import ppo
        return ppo.PPOAgent
    elif alg == "ES":
        from ray.rllib import es
        return es.ESAgent
    elif alg == "DQN":
        from ray.rllib import dqn
        return dqn.DQNAgent
    elif alg == "A3C":
        from ray.rllib import a3c
        return a3c.A3CAgent
    elif alg == "script":
        from ray.tune import script_runner
        return script_runner.ScriptRunner
    elif alg == "__fake":
        return _MockAgent
    elif alg == "__sigmoid_fake_data":
        return _SigmoidFakeData
    else:
        raise Exception(
            ("Unknown algorithm {}, check --alg argument. Valid choices " +
             "are PPO, ES, DQN, and A3C.").format(alg))
