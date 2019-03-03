from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import pickle
import numpy as np

from ray.rllib.agents.agent import Agent, with_common_config


class _MockAgent(Agent):
    """Mock agent for use in tests"""

    _agent_name = "MockAgent"
    _default_config = with_common_config({
        "mock_error": False,
        "persistent_error": False,
        "test_variable": 1,
        "num_workers": 0,
    })

    def _init(self):
        self.info = None
        self.restored = False

    def _train(self):
        if self.config["mock_error"] and self.iteration == 1 \
                and (self.config["persistent_error"] or not self.restored):
            raise Exception("mock error")
        return dict(
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

    def _register_if_needed(self, env_object):
        pass

    def set_info(self, info):
        self.info = info
        return info

    def get_info(self):
        return self.info


class _SigmoidFakeData(_MockAgent):
    """Agent that returns sigmoid learning curves.

    This can be helpful for evaluating early stopping algorithms."""

    _agent_name = "SigmoidFakeData"
    _default_config = with_common_config({
        "width": 100,
        "height": 100,
        "offset": 0,
        "iter_time": 10,
        "iter_timesteps": 1,
        "num_workers": 0,
    })

    def _train(self):
        i = max(0, self.iteration - self.config["offset"])
        v = np.tanh(float(i) / self.config["width"])
        v *= self.config["height"]
        return dict(
            episode_reward_mean=v,
            episode_len_mean=v,
            timesteps_this_iter=self.config["iter_timesteps"],
            time_this_iter_s=self.config["iter_time"],
            info={})


class _ParameterTuningAgent(_MockAgent):

    _agent_name = "ParameterTuningAgent"
    _default_config = with_common_config({
        "reward_amt": 10,
        "dummy_param": 10,
        "dummy_param2": 15,
        "iter_time": 10,
        "iter_timesteps": 1,
        "num_workers": 0,
    })

    def _train(self):
        return dict(
            episode_reward_mean=self.config["reward_amt"] * self.iteration,
            episode_len_mean=self.config["reward_amt"],
            timesteps_this_iter=self.config["iter_timesteps"],
            time_this_iter_s=self.config["iter_time"],
            info={})


def _agent_import_failed(trace):
    """Returns dummy agent class for if PyTorch etc. is not installed."""

    class _AgentImportFailed(Agent):
        _agent_name = "AgentImportFailed"
        _default_config = with_common_config({})

        def _setup(self, config):
            raise ImportError(trace)

    return _AgentImportFailed
