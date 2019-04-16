from __future__ import absolute_import

from gym.spaces import Dict
import torch.nn as nn
from torch.optim import RMSprop

from ray.rllib.agents.dqn.dqn import DEFAULT_CONFIG as DQN_DEFAULT_CONFIG
from ray.rllib.agents.qmix.model import _get_size
from ray.rllib.evaluation.policy_graph import PolicyGraph
from ray.rllib.utils.annotations import override
from ray.rllib.models.catalog import ModelCatalog


class RainbowTorchLoss(nn.Module):
    def __init__(self,
                 model,
                 n_actions,
                 gamma=0.99):
        nn.Module.__init__(self)
        self.model = model
        self.n_actions = n_actions
        self.gamma = gamma

        def forward():
            pass  # todo


class RainbowTorchPolicyGraph(PolicyGraph):
    def __init__(self, observation_space, action_space, config):
        _validate(config)
        config = dict(DQN_DEFAULT_CONFIG, **config)
        self.config = config
        self.observation_space = observation_space
        self.action_space = action_space
        self.n_actions = action_space.spaces[0].n
        self.cur_epsilon = 1.0

        agent_obs_space = observation_space.original_space.spaces[0]
        if isinstance(agent_obs_space, Dict):
            space_keys = set(agent_obs_space.spaces.keys())
            if space_keys != {"obs", "action_mask"}:
                raise ValueError(
                    "Dict obs space for agent must have keyset "
                    "['obs', 'action_mask'], got {}".format(space_keys))
            mask_shape = tuple(agent_obs_space.spaces["action_mask"].shape)
            if mask_shape != (self.n_actions, ):
                raise ValueError("Action mask shape must be {}, got {}".format(
                    (self.n_actions, ), mask_shape))
            self.has_action_mask = True
            self.obs_size = _get_size(agent_obs_space.spaces["obs"])
            # The real agent obs space is nested inside the dict
            agent_obs_space = agent_obs_space.spaces["obs"]
        else:
            self.has_action_mask = False
            self.obs_size = _get_size(agent_obs_space)

        self.model = ModelCatalog.get_torch_model(
            agent_obs_space,
            self.n_actions,
            config["model"])

        # Setup optimiser
        self.params = list(self.model.parameters())
        self.loss - RainbowTorchLoss(self.model, self.n_actions)
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
        pass  # todo

    @override(PolicyGraph)
    def learn_on_batch(self, samples):
        pass  # todo

    @override(PolicyGraph)
    def get_weights(self):
        return {"model": self.model.state_dict()}

    @override(PolicyGraph)
    def set_weights(self, weights):
        self.model.load_state_dict(weights["model"])

    @override(PolicyGraph)
    def get_state(self):
        return {
            "model": self.model.state_dict(),
            "target_model": self.target_model.state_dict(),
            "cur_epsilon": self.cur_epsilon
        }

    @override(PolicyGraph)
    def set_state(self, state):
        pass  # todo


def _validate(config):
    if not hasattr(config, "optim_alpha"):
        raise ValueError("Need to specify \"optim_alpha\" attribute of config, got: {}".format(config))
    if not hasattr(config, "optim_eps"):
        raise ValueError("Need to specify \"optim_eps\" attribute of config, got: {}".format(config))
    if not hasattr(config, "model"):
        raise ValueError("Need to specify \"model\" attribute of config, got: {}".format(config))
