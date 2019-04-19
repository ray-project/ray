from __future__ import absolute_import

from gym.spaces import Dict
import math
import torch as th
import torch.nn as nn
import torch.nn.functional as F
from torch.optim import RMSprop

from ray.rllib.agents.dqn.dqn import DEFAULT_CONFIG as DQN_DEFAULT_CONFIG
from ray.rllib.agents.qmix.model import _get_size
from ray.rllib.evaluation.policy_graph import PolicyGraph
from ray.rllib.utils.annotations import override
from ray.rllib.models.catalog import ModelCatalog


class NoisyLinear(nn.Module):
    """
        todo: add reference to Kaixhin's Rainbow etc.
    """
    def __init__(self, in_features, out_features, std_init=0.5):
        super(NoisyLinear, self).__init__()
        self.in_features = in_features
        self.out_features = out_features
        self.std_init = std_init
        self.weight_mu = nn.Parameter(th.empty(out_features, in_features))
        self.weight_sigma = nn.Parameter(th.empty(out_features, in_features))
        self.register_buffer('weight_epsilon', th.empty(out_features, in_features))
        self.bias_mu = nn.Parameter(th.empty(out_features))
        self.bias_sigma = nn.Parameter(th.empty(out_features))
        self.register_buffer('bias_epsilon', th.empty(out_features))
        self.reset_parameters()
        self.reset_noise()

    def reset_parameters(self):
        mu_range = 1 / math.sqrt(self.in_features)
        self.weight_mu.data.uniform_(-mu_range, mu_range)
        self.weight_sigma.data.fill_(self.std_init / math.sqrt(self.in_features))
        self.bias_mu.data.uniform_(-mu_range, mu_range)
        self.bias_sigma.data.fill_(self.std_init / math.sqrt(self.out_features))

    def _scale_noise(self, size):
        x = th.randn(size)
        return x.sign().mul_(x.abs().sqrt_())

    def reset_noise(self):
        epsilon_in = self._scale_noise(self.in_features)
        epsilon_out = self._scale_noise(self.out_features)
        self.weight_epsilon.copy_(epsilon_out.ger(epsilon_in))
        self.bias_epsilon.copy_(epsilon_out)

    def forward(self, input_):
        if self.training:
            return F.linear(input_,
                            self.weight_mu + self.weight_sigma * self.weight_epsilon,
                            self.bias_mu + self.bias_sigma * self.bias_epsilon)
        else:
            return F.linear(input_, self.weight_mu, self.bias_mu)


class RainbowTorchLoss(nn.Module):
    """
    todo: add reference to Kaixhin's Rainbow etc.
    """
    def __init__(self, args, action_space):
        super().__init__()
        self.atoms = args.atoms
        self.action_space = action_space

        self.conv1 = nn.Conv2d(args.history_length, 32, 8, stride=4, padding=1)
        self.conv2 = nn.Conv2d(32, 64, 4, stride=2)
        self.conv3 = nn.Conv2d(64, 64, 3)
        self.fc_h_v = NoisyLinear(3136, args.hidden_size, std_init=args.noisy_std)
        self.fc_h_a = NoisyLinear(3136, args.hidden_size, std_init=args.noisy_std)
        self.fc_z_v = NoisyLinear(args.hidden_size, self.atoms, std_init=args.noisy_std)
        self.fc_z_a = NoisyLinear(args.hidden_size, action_space * self.atoms, std_init=args.noisy_std)

    def forward(self, x, log=False):
        x = F.relu(self.conv1(x))
        x = F.relu(self.conv2(x))
        x = F.relu(self.conv3(x))
        x = x.view(-1, 3136)
        v = self.fc_z_v(F.relu(self.fc_h_v(x)))  # Value stream
        a = self.fc_z_a(F.relu(self.fc_h_a(x)))  # Advantage stream
        v, a = v.view(-1, 1, self.atoms), a.view(-1, self.action_space, self.atoms)
        q = v + a - a.mean(1, keepdim=True)  # Combine streams
        if log:  # Use log softmax for numerical stability
          q = F.log_softmax(q, dim=2)  # Log probabilities with action over second dimension
        else:
          q = F.softmax(q, dim=2)  # Probabilities with action over second dimension
        return q

    def reset_noise(self):
        for name, module in self.named_children():
            if 'fc' in name:
                module.reset_noise()


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
        self.loss = RainbowTorchLoss(self.model, self.n_actions)
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


def _validate(config):
    if not hasattr(config, "optim_alpha"):
        raise ValueError("Need to specify \"optim_alpha\" attribute of config, got: {}".format(config))
    if not hasattr(config, "optim_eps"):
        raise ValueError("Need to specify \"optim_eps\" attribute of config, got: {}".format(config))
    if not hasattr(config, "model"):
        raise ValueError("Need to specify \"model\" attribute of config, got: {}".format(config))
