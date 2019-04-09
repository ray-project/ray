from __future__ import absolute_import

import torch.nn as nn
from torch.optim import RMSprop

from ray.rllib.agents.dqn.dqn import DEFAULT_CONFIG as DQN_DEFAULT_CONFIG
from ray.rllib.evaluation.policy_graph import PolicyGraph
from ray.rllib.utils.annotations import override


class DQNTorchLoss(nn.Module):
    pass


class DQNTorchPolicyGraph(PolicyGraph):
    def __init__(self, observation_space, action_space, config):
        _validate(config)
        config = dict(DQN_DEFAULT_CONFIG, **config)
        self.config = config
        self.observation_space = observation_space
        self.action_space = action_space

        # todo: initialize the remaining attributes of this class

        self.optimiser = RMSprop(
            params=self.params,
            lr=config["lr"],
            alpha=config["optim_alpha"],
            eps=config["optim_eps"])

    @override(PolicyGraph)
    def compute_actions(self,
                        obs_batch,
                        state_batches,
                        prev_action_batch=None,
                        prev_reward_batch=None,
                        info_batch=None,
                        episodes=None,
                        **kwargs):
        pass

    @override(PolicyGraph)
    def learn_on_batch(self, samples):
        pass

    @override(PolicyGraph)
    def get_initial_state(self):
        pass

    @override(PolicyGraph)
    def get_weights(self):
        pass

    @override(PolicyGraph)
    def get_state(self):
        pass

    @override(PolicyGraph)
    def set_state(self, state):
        pass


def _validate(config):
    if not hasattr(config, "optim_alpha"):
        raise ValueError("Need to specify \"optim_alpha\" attribute of config, got: {}".format(config))
    if not hasattr(config, "optim_eps"):
        raise ValueError("Need to specify \"optim_eps\" attribute of config, got: {}".format(config))
