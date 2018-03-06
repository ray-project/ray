from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import numpy as np
import os
import pickle

import tensorflow as tf
from ray.tune.registry import ENV_CREATOR
from ray.tune.result import TrainingResult
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
        registry (obj): Tune object registry which holds user-registered
            classes and objects by name.
    """

    _allow_unknown_configs = False
    _allow_unknown_subkeys = []

    def __init__(
            self, config=None, env=None, registry=None,
            logger_creator=None):
        """Initialize an RLLib agent.

        Args:
            config (dict): Algorithm-specific configuration data.
            env (str): Name of the environment to use. Note that this can also
                be specified as the `env` key in config.
            registry (obj): Object registry for user-defined envs, models, etc.
                If unspecified, the default registry will be used.
            logger_creator (func): Function that creates a ray.tune.Logger
                object. If unspecified, a default logger is created.
        """

        config = config or {}

        # Agents allow env ids to be passed directly to the constructor.
        self._env_id = env or config.get("env")
        Trainable.__init__(self, config, registry, logger_creator)

    def _setup(self):
        env = self._env_id
        if env:
            self.config["env"] = env
            if self.registry and self.registry.contains(ENV_CREATOR, env):
                self.env_creator = self.registry.get(ENV_CREATOR, env)
            else:
                import gym  # soft dependency
                self.env_creator = lambda env_config: gym.make(env)
        else:
            self.env_creator = lambda env_config: None

        # Merge the supplied config with the class default
        merged_config = self._default_config.copy()
        merged_config = _deep_update(merged_config, self.config,
                                     self._allow_unknown_configs,
                                     self._allow_unknown_subkeys)
        self.config = merged_config

        # TODO(ekl) setting the graph is unnecessary for PyTorch agents
        with tf.Graph().as_default():
            self._init()

    def _init(self):
        """Subclasses should override this for custom initialization."""

        raise NotImplementedError

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


class _MockAgent(Agent):
    """Mock agent for use in tests"""

    _agent_name = "MockAgent"
    _default_config = {
        "mock_error": False,
        "persistent_error": False,
    }

    def _init(self):
        self.info = None
        self.restored = False

    def _train(self):
        if self.config["mock_error"] and self.iteration == 1 \
                and (self.config["persistent_error"] or not self.restored):
            raise Exception("mock error")
        return TrainingResult(
            episode_reward_mean=10, episode_len_mean=10,
            timesteps_this_iter=10, info={})

    def _save(self, checkpoint_dir):
        path = os.path.join(checkpoint_dir, "mock_agent.pkl")
        with open(path, 'wb') as f:
            pickle.dump(self.info, f)
        return path

    def _restore(self, checkpoint_path):
        with open(checkpoint_path, 'rb') as f:
            info = pickle.load(f)
        self.info = info
        self.restored = True

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


class _ParameterTuningAgent(_MockAgent):

    _agent_name = "ParameterTuningAgent"
    _default_config = {
        "reward_amt": 10,
        "dummy_param": 10,
        "dummy_param2": 15,
        "iter_time": 10,
        "iter_timesteps": 1
    }

    def _train(self):
        return TrainingResult(
            episode_reward_mean=self.config["reward_amt"] * self.iteration,
            episode_len_mean=self.config["reward_amt"],
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
    elif alg == "APEX":
        from ray.rllib import dqn
        return dqn.ApexAgent
    elif alg == "A3C":
        from ray.rllib import a3c
        return a3c.A3CAgent
    elif alg == "BC":
        from ray.rllib import bc
        return bc.BCAgent
    elif alg == "PG":
        from ray.rllib import pg
        return pg.PGAgent
    elif alg == "script":
        from ray.tune import script_runner
        return script_runner.ScriptRunner
    elif alg == "__fake":
        return _MockAgent
    elif alg == "__sigmoid_fake_data":
        return _SigmoidFakeData
    elif alg == "__parameter_tuning":
        return _ParameterTuningAgent
    else:
        raise Exception(
            ("Unknown algorithm {}.").format(alg))
