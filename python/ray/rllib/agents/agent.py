from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import copy
import json
import numpy as np
import os
import pickle

import tensorflow as tf
from ray.rllib.evaluation.policy_evaluator import PolicyEvaluator
from ray.tune.registry import ENV_CREATOR, _global_registry
from ray.tune.result import TrainingResult
from ray.tune.trainable import Trainable

COMMON_CONFIG = {
    # Discount factor of the MDP
    "gamma": 0.99,
    # Number of steps after which the rollout gets cut
    "horizon": None,
    # Number of environments to evaluate vectorwise per worker.
    "num_envs_per_worker": 1,
    # Number of actors used for parallelism
    "num_workers": 2,
    # Default sample batch size
    "sample_batch_size": 200,
    # Whether to rollout "complete_episodes" or "truncate_episodes"
    "batch_mode": "truncate_episodes",
    # Whether to use a background thread for sampling (slightly off-policy)
    "sample_async": False,
    # Which observation filter to apply to the observation
    "observation_filter": "NoFilter",
    # Whether to use rllib or deepmind preprocessors
    "preprocessor_pref": "rllib",
    # Arguments to pass to the env creator
    "env_config": {},
    # Arguments to pass to model
    "model": {
        "use_lstm": False,
        "max_seq_len": 20,
    },
    # Arguments to pass to the rllib optimizer
    "optimizer": {},
    # Configure TF for single-process operation by default
    "tf_session_args": {
        "intra_op_parallelism_threads": 1,
        "inter_op_parallelism_threads": 1,
        "gpu_options": {
            "allow_growth": True,
        },
        "log_device_placement": False,
        "device_count": {
            "CPU": 1
        },
        "allow_soft_placement": True,  # required by PPO multi-gpu
    },
    # Whether to LZ4 compress observations
    "compress_observations": False,

    # === Multiagent ===
    "multiagent": {
        # Map from policy ids to tuples of (policy_graph_cls, obs_space,
        # act_space, config). See policy_evaluator.py for more info.
        "policy_graphs": {},
        # Function mapping agent ids to policy ids.
        "policy_mapping_fn": None,
        # Optional whitelist of policies to train, or None for all policies.
        "policies_to_train": None,
    },
}


def with_common_config(extra_config):
    """Returns the given config dict merged with common agent confs."""

    config = copy.deepcopy(COMMON_CONFIG)
    config.update(extra_config)
    return config


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
                raise Exception("Unknown config parameter `{}` ".format(k))
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
    """

    _allow_unknown_configs = False
    _allow_unknown_subkeys = [
        "tf_session_args", "env_config", "model", "optimizer", "multiagent"
    ]

    def make_local_evaluator(self, env_creator, policy_graph):
        """Convenience method to return configured local evaluator."""

        return self._make_evaluator(PolicyEvaluator, env_creator, policy_graph,
                                    0)

    def make_remote_evaluators(self, env_creator, policy_graph, count,
                               remote_args):
        """Convenience method to return a number of remote evaluators."""

        cls = PolicyEvaluator.as_remote(**remote_args).remote
        return [
            self._make_evaluator(cls, env_creator, policy_graph, i + 1)
            for i in range(count)
        ]

    def _make_evaluator(self, cls, env_creator, policy_graph, worker_index):
        config = self.config

        def session_creator():
            return tf.Session(
                config=tf.ConfigProto(**config["tf_session_args"]))

        return cls(
            env_creator,
            self.config["multiagent"]["policy_graphs"] or policy_graph,
            policy_mapping_fn=self.config["multiagent"]["policy_mapping_fn"],
            policies_to_train=self.config["multiagent"]["policies_to_train"],
            tf_session_creator=(session_creator
                                if config["tf_session_args"] else None),
            batch_steps=config["sample_batch_size"],
            batch_mode=config["batch_mode"],
            episode_horizon=config["horizon"],
            preprocessor_pref=config["preprocessor_pref"],
            sample_async=config["sample_async"],
            compress_observations=config["compress_observations"],
            num_envs=config["num_envs_per_worker"],
            observation_filter=config["observation_filter"],
            env_config=config["env_config"],
            model_config=config["model"],
            policy_config=config,
            worker_index=worker_index)

    @classmethod
    def resource_help(cls, config):
        return ("\n\nYou can adjust the resource requests of RLlib agents by "
                "setting `num_workers` and other configs. See the "
                "DEFAULT_CONFIG defined by each agent for more info.\n\n"
                "The config of this agent is: " + json.dumps(config))

    def __init__(self, config=None, env=None, logger_creator=None):
        """Initialize an RLLib agent.

        Args:
            config (dict): Algorithm-specific configuration data.
            env (str): Name of the environment to use. Note that this can also
                be specified as the `env` key in config.
            logger_creator (func): Function that creates a ray.tune.Logger
                object. If unspecified, a default logger is created.
        """

        config = config or {}

        # Agents allow env ids to be passed directly to the constructor.
        self._env_id = env or config.get("env")
        Trainable.__init__(self, config, logger_creator)

    def _setup(self):
        env = self._env_id
        if env:
            self.config["env"] = env
            if _global_registry.contains(ENV_CREATOR, env):
                self.env_creator = _global_registry.get(ENV_CREATOR, env)
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

    def compute_action(self, observation, state=None):
        """Computes an action using the current trained policy."""

        if state is None:
            state = []
        obs = self.local_evaluator.filters["default"](
            observation, update=False)
        return self.local_evaluator.for_policy(
            lambda p: p.compute_single_action(obs, state, is_training=False)[0]
        )

    def get_weights(self, policies=None):
        """Return a dictionary of policy ids to weights.

        Arguments:
            policies (list): Optional list of policies to return weights for,
                or None for all policies.
        """
        return self.local_evaluator.get_weights(policies)

    def set_weights(self, weights):
        """Set policy weights by policy id.

        Arguments:
            weights (dict): Map of policy ids to weights to set.
        """
        self.local_evaluator.set_weights(weights)


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
            episode_reward_mean=10,
            episode_len_mean=10,
            timesteps_this_iter=10,
            info={})

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
            episode_reward_mean=v,
            episode_len_mean=v,
            timesteps_this_iter=self.config["iter_timesteps"],
            time_this_iter_s=self.config["iter_time"],
            info={})


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
            time_this_iter_s=self.config["iter_time"],
            info={})


def get_agent_class(alg):
    """Returns the class of a known agent given its name."""

    if alg == "DDPG":
        from ray.rllib.agents import ddpg
        return ddpg.DDPGAgent
    elif alg == "APEX_DDPG":
        from ray.rllib.agents import ddpg
        return ddpg.ApexDDPGAgent
    elif alg == "PPO":
        from ray.rllib.agents import ppo
        return ppo.PPOAgent
    elif alg == "ES":
        from ray.rllib.agents import es
        return es.ESAgent
    elif alg == "DQN":
        from ray.rllib.agents import dqn
        return dqn.DQNAgent
    elif alg == "APEX":
        from ray.rllib.agents import dqn
        return dqn.ApexAgent
    elif alg == "A3C":
        from ray.rllib.agents import a3c
        return a3c.A3CAgent
    elif alg == "BC":
        from ray.rllib.agents import bc
        return bc.BCAgent
    elif alg == "PG":
        from ray.rllib.agents import pg
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
        raise Exception(("Unknown algorithm {}.").format(alg))
