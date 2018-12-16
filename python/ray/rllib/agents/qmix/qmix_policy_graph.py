from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

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
from ray.rllib.models.pytorch.misc import var_to_np
from ray.rllib.utils.annotations import override

logger = logging.getLogger(__name__)


class QMixLoss(nn.Module):
    def __init__(self,
                 model,
                 target_model,
                 mixer,
                 target_mixer,
                 double_q=True,
                 gamma=0.99):
        nn.Module.__init__(self)
        self.model = model
        self.target_model = target_model
        self.mixer = mixer
        self.target_mixer = target_mixer
        self.double_q = double_q
        self.gamma = gamma

    def forward(self, rewards, actions, terminated, obs, avail_actions):
        B = obs.size(0)
        T = obs.size(1)

        # TODO(ekl) support batching
        # mask = batch["filled"][:, :-1].float()
        mask = th.ones_like(terminated)
        mask[:, 1:] = mask[:, 1:] * (1 - terminated[:, :-1])

        # Calculate estimated Q-Values
        q_out = []
        h = self.model.init_hidden().expand([B, -1])  # shape [B, H]
        for t in range(T):
            q, h = self.model.forward(obs[:, t], h)
            q_out.append(q)
        q_out = th.stack(q_out, dim=1)  # Concat over time

        # Pick the Q-Values for the actions taken by each agent
        chosen_action_qvals = th.gather(
            q_out[:, :-1], dim=2, index=actions.unsqueeze(2)).squeeze(2)

        # Calculate the Q-Values necessary for the target
        target_q_out = []
        target_h = self.target_model.init_hidden().expand([B, -1])
        for t in range(T):
            target_q, target_h = self.target_model.forward(obs[:, t], target_h)
            target_q_out.append(target_q)

        # We don't need the first timesteps Q-Value estimate for targets
        target_q_out = th.stack(target_q_out[1:], dim=1)  # Concat across time

        # Mask out unavailable actions
        target_q_out[avail_actions[:, 1:] == 0] = -9999999

        # Max over target Q-Values
        if self.double_q:
            # Get actions that maximise live Q (for double q-learning)
            q_out[avail_actions == 0] = -9999999
            cur_max_actions = q_out[:, 1:].max(dim=2, keepdim=True)[1]
            target_max_qvals = th.gather(target_q_out, 2,
                                         cur_max_actions).squeeze(2)
        else:
            target_max_qvals = target_q_out.max(dim=2)[0]

        # Mix
        if self.mixer is not None:
            # TODO(ekl) add support for handling global state. This is just
            # stacking the agent obs.
            # Transpose shape from [B, T, state_size] => [T, B, state_size]
            chosen_action_qvals = self.mixer(chosen_action_qvals,
                                             th.transpose(obs[:, :-1], 0, 1))
            target_max_qvals = self.target_mixer(
                target_max_qvals, th.transpose(obs[:, 1:], 0, 1))

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
    """QMix impl. Assumes homogeneous agents for now."""

    def __init__(self, obs_space, action_space, config):
        config = dict(ray.rllib.agents.qmix.qmix.DEFAULT_CONFIG, **config)
        self.observation_space = obs_space
        self.action_space = action_space
        self.config = config
        # TODO(ekl) don't hard-code these shapes
        self.model = RNNModel(self.observation_space.shape[0], 64,
                              action_space.n)
        self.target_model = RNNModel(self.observation_space.shape[0], 64,
                                     action_space.n)
        self.n_agents = 2
        self.state_shape = list(self.observation_space.shape) + [self.n_agents]
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

        self.params = list(self.model.parameters())
        self.loss = QMixLoss(self.model, self.target_model, self.mixer,
                             self.target_mixer)
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
                        episodes=None):
        assert len(state_batches) == 1

        with th.no_grad():
            actions, hiddens = self.model.forward(
                th.from_numpy(np.array(obs_batch)),
                th.from_numpy(np.array(state_batches[0])))
            masked_q_values = actions.clone()
            # TODO(ekl) support action masks
            # masked_q_values[avail_actions == 0.0] = -float("inf")
            # epsilon-greedy action selector
            random_numbers = th.rand_like(actions[:, 0])
            pick_random = (random_numbers < self.cur_epsilon).long()
            avail_actions = th.ones_like(actions)
            random_actions = Categorical(avail_actions).sample().long()
            picked_actions = (
                pick_random * random_actions +
                (1 - pick_random) * masked_q_values.max(dim=1)[1])

            return (var_to_np(picked_actions), [var_to_np(hiddens)], {
                "avail_actions": var_to_np(avail_actions)
            })

    @override(PolicyGraph)
    def compute_apply(self, samples):
        num_agents = self.n_agents
        B = num_agents
        T = samples.count // B

        # TODO: reshape to [B, T, n_agents, shape]

        def add_time_dim(arr):
            new_shape = [B, T] + list(arr.shape[1:])
            return th.from_numpy(np.reshape(arr, new_shape))

        rewards = add_time_dim(samples["rewards"])[:, :-1]
        actions = add_time_dim(samples["actions"])[:, :-1]
        terminated = add_time_dim(samples["dones"].astype(np.float32))[:, :-1]
        obs = add_time_dim(samples["obs"])
        avail_actions = add_time_dim(samples["avail_actions"])

        # Compute loss
        loss_out, mask, masked_td_error, chosen_action_qvals, targets = \
            self.loss(rewards, actions, terminated, obs, avail_actions)

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
            (mask_elems * num_agents),
            "target_mean": (targets * mask).sum().item() /
            (mask_elems * num_agents),
        }
        return {"stats": stats}, {}

    @override(PolicyGraph)
    def get_initial_state(self):
        return [self.model.init_hidden().numpy()]

    @override(PolicyGraph)
    def get_weights(self):
        return {"model": self.model.state_dict()}

    @override(PolicyGraph)
    def set_weights(self, weights):
        self.model.load_state_dict(weights["model"])

    def update_target(self):
        self.target_model.load_state_dict(self.model.state_dict())
        if self.mixer is not None:
            self.target_mixer.load_state_dict(self.mixer.state_dict())
        logger.debug("Updated target networks")

    def set_epsilon(self, epsilon):
        self.cur_epsilon = epsilon
