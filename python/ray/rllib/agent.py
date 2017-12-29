from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from datetime import datetime

import logging
import numpy as np
import io
import os
import gzip
import pickle
import shutil
import tempfile
import time
import uuid

# Note: avoid introducing unnecessary library dependencies here, e.g. gym
# until https://github.com/ray-project/ray/issues/1144 is resolved
import tensorflow as tf
from ray.tune.logger import UnifiedLogger
from ray.tune.registry import ENV_CREATOR, get_registry
from ray.tune.result import DEFAULT_RESULTS_DIR, TrainingResult
from ray.tune.trainable import Trainable

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def _deep_update(original, new_dict, new_keys_allowed, whitelist):
    """Updates original dict with values from new_dict recursively.
    If new key is introduced in new_dict, then if new_keys_allowed is not
    True, an error will be thrown. Further, for sub-dicts, if the key is
    in the whitelist, then new subkeys can be introduced.

    Args:
        original (dict): Dictionary with default values.
        new_dict (dict): Dictionary with values to be updated
        new_keys_allowed (bool): Whether new keys are allowed.
        whitelist (list): List of keys that correspond to dict values
            where new subkeys can be introduced. This is only at
            the top level.
    """
    for k, value in new_dict.items():
        if k not in original and k != "env":
            if not new_keys_allowed:
                raise Exception(
                    "Unknown config parameter `{}` ".format(k))
            else:
                logger.warn("`{}` not in default configuration...".format(k))
        if type(original.get(k)) is dict:
            if k in whitelist:
                _deep_update(original[k], value, True, [])
            else:
                _deep_update(original[k], value, new_keys_allowed, [])
        else:
            original[k] = value
    return original


class Agent(Trainable):
    """All RLlib agents extend this base class.

    Agent objects retain internal model state between calls to train(), so
    you should create a new agent instance for each training session.

    Attributes:
        env_creator (func): Function that creates a new training env.
        config (obj): Algorithm-specific configuration data.
        logdir (str): Directory in which training outputs should be placed.
        registry (obj): Tune object registry, for registering user-defined
            classes and objects by name.
    """

    _allow_unknown_configs = False
    _allow_unknown_subkeys = []

    def __init__(
            self, config={}, env=None, registry=get_registry(),
            logger_creator=None):
        """Initialize an RLLib agent.

        Args:
            config (dict): Algorithm-specific configuration data.
            env (str): Name of the environment to use. Note that this can also
                be specified as the `env` key in config.
            registry (obj): Object registry for user-defined envs, models, etc.
                If unspecified, it will be assumed empty.
            logger_creator (func): Function that creates a ray.tune.Logger
                object. If unspecified, a default logger is created.
        """
        self._initialize_ok = False
        self._experiment_id = uuid.uuid4().hex
        env = env or config.get("env")
        if env:
            config["env"] = env
            if registry and registry.contains(ENV_CREATOR, env):
                self.env_creator = registry.get(ENV_CREATOR, env)
            else:
                import gym  # soft dependency
                self.env_creator = lambda: gym.make(env)
        else:
            self.env_creator = lambda: None
        self.config = self._default_config.copy()
        self.registry = registry

        self.config = _deep_update(self.config, config,
                                   self._allow_unknown_configs,
                                   self._allow_unknown_subkeys)

        if logger_creator:
            self._result_logger = logger_creator(self.config)
            self.logdir = self._result_logger.logdir
        else:
            logdir_suffix = "{}_{}_{}".format(
                env, self._agent_name,
                datetime.today().strftime("%Y-%m-%d_%H-%M-%S"))
            if not os.path.exists(DEFAULT_RESULTS_DIR):
                os.makedirs(DEFAULT_RESULTS_DIR)
            self.logdir = tempfile.mkdtemp(
                prefix=logdir_suffix, dir=DEFAULT_RESULTS_DIR)
            self._result_logger = UnifiedLogger(self.config, self.logdir, None)

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

        now = datetime.today()
        result = result._replace(
            experiment_id=self._experiment_id,
            date=now.strftime("%Y-%m-%d_%H-%M-%S"),
            timestamp=int(time.mktime(now.timetuple())),
            training_iteration=self._iteration,
            timesteps_total=self._timesteps_total,
            time_this_iter_s=time_this_iter,
            time_total_s=self._time_total,
            pid=os.getpid(),
            hostname=os.uname()[1])

        self._result_logger.on_result(result)

        return result

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

    def save_to_object(self):
        """Saves the current model state to a Python object. It also
        saves to disk but does not return the checkpoint path.

        Returns:
            Object holding checkpoint data.
        """

        checkpoint_prefix = self.save()

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
            print("Saving checkpoint to object store, {} bytes".format(
                len(compressed)))
            f.write(compressed)

        return out.getvalue()

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
        """Releases all resources used by this agent."""

        if self._initialize_ok:
            self._result_logger.close()
            self._stop()

    def _stop(self):
        """Subclasses should override this for custom stopping."""

        pass

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
            ("Unknown algorithm {}.").format(alg))
