from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from gym.spaces import Tuple, Discrete
import logging
import numpy as np
import torch as th
import torch.nn as nn
from torch.optim import RMSprop
from torch.distributions import Categorical

import ray
from ray.rllib.agents.qmix.mixers import VDNMixer, QMixer
from ray.rllib.agents.qmix.model import RNNModel
from ray.rllib.evaluation.policy_graph import PolicyGraph
from ray.rllib.models.action_dist import TupleActions
from ray.rllib.models.pytorch.misc import var_to_np
from ray.rllib.models.lstm import chop_into_sequences
from ray.rllib.models.preprocessors import get_preprocessor, \
    TupleFlatteningPreprocessor
from ray.rllib.env.constants import GROUP_REWARDS_KEY, GROUP_INFO_KEY, \
    AVAIL_ACTIONS_KEY
from ray.rllib.utils.annotations import override

logger = logging.getLogger(__name__)


class QMixLoss(nn.Module):
    def __init__(self,
                 model,
                 target_model,
                 mixer,
                 target_mixer,
                 n_agents,
                 n_actions,
                 double_q=True,
                 gamma=0.99):
        nn.Module.__init__(self)
        self.model = model
        self.target_model = target_model
        self.mixer = mixer
        self.target_mixer = target_mixer
        self.n_agents = n_agents
        self.n_actions = n_actions
        self.double_q = double_q
        self.gamma = gamma

    def forward(self, rewards, actions, terminated, mask, obs, avail_actions):
        """Forward pass of the loss.

        Arguments:
            rewards: Tensor of shape [B, T-1, n_agents]
            actions: Tensor of shape [B, T-1, n_agents]
            terminated: Tensor of shape [B, T-1, n_agents]
            mask: Tensor of shape [B, T-1, n_agents]
            obs: Tensor of shape [B, T, n_agents, obs_size]
            avail_actions: Tensor of shape [B, T, n_agents, n_actions]
        """

        B, T = obs.size(0), obs.size(1)

        # Calculate estimated Q-Values
        mac_out = []
        h = self.model.init_hidden().expand([B, self.n_agents, -1])
        for t in range(T):
            q, h = _mac(self.model, obs[:, t], h)
            mac_out.append(q)
        mac_out = th.stack(mac_out, dim=1)  # Concat over time

        # Pick the Q-Values for the actions taken -> [B * n_agents, T-1]
        chosen_action_qvals = th.gather(
            mac_out[:, :-1], dim=3, index=actions.unsqueeze(3)).squeeze(3)

        # Calculate the Q-Values necessary for the target
        target_mac_out = []
        target_h = self.target_model.init_hidden().expand(
            [B, self.n_agents, -1])
        for t in range(T):
            target_q, target_h = _mac(self.target_model, obs[:, t], target_h)
            target_mac_out.append(target_q)

        # We don't need the first timesteps Q-Value estimate for targets
        target_mac_out = th.stack(
            target_mac_out[1:], dim=1)  # Concat across time

        # Mask out unavailable actions
        target_mac_out[avail_actions[:, 1:] == 0] = -9999999

        # Max over target Q-Values
        if self.double_q:
            # Get actions that maximise live Q (for double q-learning)
            mac_out[avail_actions == 0] = -9999999
            cur_max_actions = mac_out[:, 1:].max(dim=3, keepdim=True)[1]
            target_max_qvals = th.gather(target_mac_out, 3,
                                         cur_max_actions).squeeze(3)
        else:
            target_max_qvals = target_mac_out.max(dim=3)[0]

        # Mix
        if self.mixer is not None:
            # TODO(ekl) add support for handling global state. This is just
            # treating the stacked agent obs as the state.
            chosen_action_qvals = self.mixer(chosen_action_qvals, obs[:, :-1])
            target_max_qvals = self.target_mixer(target_max_qvals, obs[:, 1:])

        # Calculate 1-step Q-Learning targets
        targets = rewards + self.gamma * (1 - terminated) * target_max_qvals

        # Td-error
        td_error = (chosen_action_qvals - targets.detach())

        mask = mask.expand_as(td_error)

        # 0-out the targets that came from padded data
        masked_td_error = td_error * mask

        # Normal L2 loss, take mean over actual data
        loss = (masked_td_error**2).sum() / mask.sum()
        return loss, mask, masked_td_error, chosen_action_qvals, targets


class QMixPolicyGraph(PolicyGraph):
    """QMix impl. Assumes homogeneous agents for now.

    You must use MultiAgentEnv.with_agent_groups() to group agents
    together for QMix. This creates the proper Tuple obs/action spaces and
    populates the '_group_rewards' info field.
    """

    def __init__(self, obs_space, action_space, config):
        _validate(obs_space, action_space)
        config = dict(ray.rllib.agents.qmix.qmix.DEFAULT_CONFIG, **config)
        self.config = config
        self.observation_space = obs_space
        self.action_space = action_space

        # Punt on dealing with complex observations. Instead, we will just
        # unflatten individual agent observations into flat vectors.
        self.unflat_obs_space = _get_unflattened_shape(obs_space)
        self.obs_size = self.unflat_obs_space.spaces[0].shape[0]
        self.n_agents = len(self.unflat_obs_space.spaces)
        self.n_actions = action_space.spaces[0].n
        self.h_size = config["model"]["lstm_cell_size"]
        self.model = RNNModel(self.obs_size, self.h_size, self.n_actions)
        self.target_model = RNNModel(self.obs_size, self.h_size,
                                     self.n_actions)

        # Setup the mixer network.
        # The global state is just the stacked agent observations for now.
        self.state_shape = (
            list(self.unflat_obs_space.spaces[0].shape) + [self.n_agents])
        if config["mixer"] is None:
            self.mixer = None
            self.target_mixer = None
        elif config["mixer"] == "qmix":
            self.mixer = QMixer(self.n_agents, self.state_shape,
                                config["mixing_embed_dim"])
            self.target_mixer = QMixer(self.n_agents, self.state_shape,
                                       config["mixing_embed_dim"])
        elif config["mixer"] == "vdn":
            self.mixer = VDNMixer()
            self.target_mixer = VDNMixer()
        else:
            raise ValueError("Unknown mixer type {}".format(config["mixer"]))

        self.cur_epsilon = 1.0
        self.update_target()  # initial sync

        # Setup optimizer
        self.params = list(self.model.parameters())
        self.loss = QMixLoss(self.model, self.target_model, self.mixer,
                             self.target_mixer, self.n_agents, self.n_actions,
                             self.config["double_q"], self.config["gamma"])
        self.optimiser = RMSprop(
            params=self.params,
            lr=config["lr"],
            alpha=config["optim_alpha"],
            eps=config["optim_eps"])

    @override(PolicyGraph)
    def compute_actions(self,
                        obs_batch,
                        state_batches=None,
                        prev_action_batch=None,
                        prev_reward_batch=None,
                        info_batch=None,
                        episodes=None):
        assert len(state_batches) == self.n_agents, state_batches
        _, avail_actions = self._get_multiagent_info(info_batch)
        state_batches = np.stack(state_batches, axis=1)
        obs_batch = np.array(obs_batch).reshape(
            [len(obs_batch), self.n_agents, self.obs_size])

        # Compute actions
        with th.no_grad():
            q_values, hiddens = _mac(self.model, th.from_numpy(obs_batch),
                                     th.from_numpy(state_batches))
            avail = th.from_numpy(avail_actions).float()
            masked_q_values = q_values.clone()
            masked_q_values[avail == 0.0] = -float("inf")
            # epsilon-greedy action selector
            random_numbers = th.rand_like(q_values[:, 0])
            pick_random = (random_numbers < self.cur_epsilon).long()
            random_actions = Categorical(avail).sample().long()
            actions = (pick_random * random_actions +
                       (1 - pick_random) * masked_q_values.max(dim=1)[1])
            actions = var_to_np(actions)
            hiddens = var_to_np(hiddens)

        return (TupleActions(list(actions.transpose([1, 0]))),
                hiddens.transpose([1, 0, 2]), {})

    @override(PolicyGraph)
    def compute_apply(self, samples):
        group_rewards, avail_actions = self._get_multiagent_info(
            samples["infos"])

        # These will be padded to shape [B * T, ...]
        [rew, avail_actions, act, dones, obs], initial_states, seq_lens = \
            chop_into_sequences(
                samples["eps_id"],
                samples["agent_index"], [
                    group_rewards, avail_actions, samples["actions"],
                    samples["dones"], samples["obs"]
                ],
                [samples["state_in_{}".format(k)]
                 for k in range(self.n_agents)],
                max_seq_len=self.config["model"]["max_seq_len"],
                dynamic_max=True)
        B, T = len(seq_lens), max(seq_lens) + 1

        def to_batches(arr):
            new_shape = [B, T] + list(arr.shape[1:])
            return th.from_numpy(np.reshape(arr, new_shape))

        rewards = to_batches(rew)[:, :-1].float()
        actions = to_batches(act)[:, :-1].long()
        obs = to_batches(obs).reshape([B, T, self.n_agents,
                                       self.obs_size]).float()
        avail_actions = to_batches(avail_actions)

        # TODO(ekl) this treats group termination as individual termination
        terminated = to_batches(dones.astype(np.float32)).unsqueeze(2).expand(
            B, T, self.n_agents)[:, :-1]
        filled = (np.reshape(np.tile(np.arange(T), B), [B, T]) <
                  np.expand_dims(seq_lens, 1)).astype(np.float32)
        mask = th.from_numpy(filled).unsqueeze(2).expand(B, T,
                                                         self.n_agents)[:, :-1]
        mask[:, 1:] = mask[:, 1:] * (1 - terminated[:, :-1])

        # Compute loss
        loss_out, mask, masked_td_error, chosen_action_qvals, targets = \
            self.loss(rewards, actions, terminated, mask, obs, avail_actions)

        # Optimise
        self.optimiser.zero_grad()
        loss_out.backward()
        grad_norm = th.nn.utils.clip_grad_norm_(
            self.params, self.config["grad_norm_clipping"])
        self.optimiser.step()

        mask_elems = mask.sum().item()
        stats = {
            "loss": loss_out.item(),
            "grad_norm": grad_norm.item(),
            "td_error_abs": masked_td_error.abs().sum().item() / mask_elems,
            "q_taken_mean": (chosen_action_qvals * mask).sum().item() /
            (mask_elems * self.n_agents),
            "target_mean": (targets * mask).sum().item() /
            (mask_elems * self.n_agents),
        }
        return {"stats": stats}, {}

    @override(PolicyGraph)
    def get_initial_state(self):
        return [
            self.model.init_hidden().numpy().squeeze()
            for _ in range(self.n_agents)
        ]

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
            "mixer": self.mixer.state_dict() if self.mixer else None,
            "target_mixer": self.target_mixer.state_dict()
            if self.mixer else None,
            "cur_epsilon": self.cur_epsilon,
        }

    @override(PolicyGraph)
    def set_state(self, state):
        self.model.load_state_dict(state["model"])
        self.target_model.load_state_dict(state["target_model"])
        if state["mixer"] is not None:
            self.mixer.load_state_dict(state["mixer"])
            self.target_mixer.load_state_dict(state["target_mixer"])
        self.set_epsilon(state["cur_epsilon"])
        self.update_target()

    def update_target(self):
        self.target_model.load_state_dict(self.model.state_dict())
        if self.mixer is not None:
            self.target_mixer.load_state_dict(self.mixer.state_dict())
        logger.debug("Updated target networks")

    def set_epsilon(self, epsilon):
        self.cur_epsilon = epsilon

    def _get_multiagent_info(self, info_batch):
        group_rewards = np.array([
            info.get(GROUP_REWARDS_KEY, [0.0] * self.n_agents)
            for info in info_batch
        ])

        def get_avail_actions(info):
            group_infos = info.get(GROUP_INFO_KEY)
            all_avail = [1.0] * self.n_actions
            if group_infos:
                avail_actions = [
                    m.get(AVAIL_ACTIONS_KEY, all_avail) for m in group_infos
                ]
            else:
                avail_actions = [all_avail] * self.n_agents
            return avail_actions

        avail_actions = np.array(
            [get_avail_actions(info) for info in info_batch])
        return group_rewards, avail_actions


def _validate(obs_space, action_space):
    if not hasattr(obs_space, "original_space") or \
            not isinstance(obs_space.original_space, Tuple):
        raise ValueError("Obs space must be a Tuple, got {}. Use ".format(
            obs_space) + "MultiAgentEnv.with_agent_groups() to group related "
                         "agents for QMix.")
    if not isinstance(action_space, Tuple):
        raise ValueError(
            "Action space must be a Tuple, got {}. ".format(action_space) +
            "Use MultiAgentEnv.with_agent_groups() to group related "
            "agents for QMix.")
    if not isinstance(action_space.spaces[0], Discrete):
        raise ValueError(
            "QMix requires a discrete action space, got {}".format(
                action_space.spaces[0]))
    if len(set([str(x) for x in obs_space.original_space.spaces])) > 1:
        raise ValueError(
            "Implementation limitation: observations of grouped agents "
            "must be homogeneous, got {}".format(
                obs_space.original_space.spaces))
    if len(set([str(x) for x in action_space.spaces])) > 1:
        raise ValueError(
            "Implementation limitation: action space of grouped agents "
            "must be homogeneous, got {}".format(action_space.spaces))


def _get_unflattened_shape(obs_space):
    space = obs_space.original_space
    prep = get_preprocessor(space)(space)
    assert isinstance(prep, TupleFlatteningPreprocessor), prep
    spaces = [p.observation_space for p in prep.preprocessors]
    return Tuple(spaces)


def _mac(model, obs, h):
    """Forward pass of the multi-agent controller.

    Arguments:
        model: Model that produces q-values for a 1d agent batch
        obs: Tensor of shape [B, n_agents, obs_size]
        h: Tensor of shape [B, n_agents, h_size]

    Returns:
        q_vals: Tensor of shape [B, n_agents, n_actions]
        h: Tensor of shape [B, n_agents, h_size]
    """
    B, n_agents = obs.size(0), obs.size(1)
    obs_flat = obs.reshape([B * n_agents, -1])
    h_flat = h.reshape([B * n_agents, -1])
    q_flat, h_flat = model.forward(obs_flat, h_flat)
    return q_flat.reshape([B, n_agents, -1]), h_flat.reshape([B, n_agents, -1])
