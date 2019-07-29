from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from gym.spaces import Tuple, Discrete, Dict
import logging
import numpy as np
import torch as th
import torch.nn as nn
from torch.optim import RMSprop
from torch.distributions import Categorical

import ray
from ray.rllib.agents.qmix.mixers import VDNMixer, QMixer
from ray.rllib.agents.qmix.model import RNNModel, _get_size
from ray.rllib.evaluation.metrics import LEARNER_STATS_KEY
from ray.rllib.policy.policy import Policy, TupleActions
from ray.rllib.policy.rnn_sequencing import chop_into_sequences
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.model import _unpack_obs
from ray.rllib.env.constants import GROUP_REWARDS
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

    def forward(self, rewards, actions, terminated, mask, obs, next_obs,
                action_mask, next_action_mask):
        """Forward pass of the loss.

        Arguments:
            rewards: Tensor of shape [B, T, n_agents]
            actions: Tensor of shape [B, T, n_agents]
            terminated: Tensor of shape [B, T, n_agents]
            mask: Tensor of shape [B, T, n_agents]
            obs: Tensor of shape [B, T, n_agents, obs_size]
            next_obs: Tensor of shape [B, T, n_agents, obs_size]
            action_mask: Tensor of shape [B, T, n_agents, n_actions]
            next_action_mask: Tensor of shape [B, T, n_agents, n_actions]
        """

        B, T = obs.size(0), obs.size(1)

        # Calculate estimated Q-Values
        mac_out = []
        h = [
            s.expand([B, self.n_agents, -1])
            for s in self.model.get_initial_state()
        ]
        for t in range(T):
            q, h = _mac(self.model, obs[:, t], h)
            mac_out.append(q)
        mac_out = th.stack(mac_out, dim=1)  # Concat over time

        # Pick the Q-Values for the actions taken -> [B * n_agents, T]
        chosen_action_qvals = th.gather(
            mac_out, dim=3, index=actions.unsqueeze(3)).squeeze(3)

        # Calculate the Q-Values necessary for the target
        target_mac_out = []
        target_h = [
            s.expand([B, self.n_agents, -1])
            for s in self.target_model.get_initial_state()
        ]
        for t in range(T):
            target_q, target_h = _mac(self.target_model, next_obs[:, t],
                                      target_h)
            target_mac_out.append(target_q)
        target_mac_out = th.stack(target_mac_out, dim=1)  # Concat across time

        # Mask out unavailable actions
        ignore_action = (next_action_mask == 0) & (mask == 1).unsqueeze(-1)
        target_mac_out[ignore_action] = -np.inf

        # Max over target Q-Values
        if self.double_q:
            # Get actions that maximise live Q (for double q-learning)
            ignore_action = (action_mask == 0) & (mask == 1).unsqueeze(-1)
            mac_out = mac_out.clone()  # issue 4742
            mac_out[ignore_action] = -np.inf
            cur_max_actions = mac_out.max(dim=3, keepdim=True)[1]
            target_max_qvals = th.gather(target_mac_out, 3,
                                         cur_max_actions).squeeze(3)
        else:
            target_max_qvals = target_mac_out.max(dim=3)[0]

        assert target_max_qvals.min().item() != -np.inf, \
            "target_max_qvals contains a masked action; \
            there may be a state with no valid actions."

        # Mix
        if self.mixer is not None:
            # TODO(ekl) add support for handling global state? This is just
            # treating the stacked agent obs as the state.
            chosen_action_qvals = self.mixer(chosen_action_qvals, obs)
            target_max_qvals = self.target_mixer(target_max_qvals, next_obs)

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


class QMixTorchPolicy(Policy):
    """QMix impl. Assumes homogeneous agents for now.

    You must use MultiAgentEnv.with_agent_groups() to group agents
    together for QMix. This creates the proper Tuple obs/action spaces and
    populates the '_group_rewards' info field.

    Action masking: to specify an action mask for individual agents, use a
    dict space with an action_mask key, e.g. {"obs": ob, "action_mask": mask}.
    The mask space must be `Box(0, 1, (n_actions,))`.
    """

    def __init__(self, obs_space, action_space, config):
        _validate(obs_space, action_space)
        config = dict(ray.rllib.agents.qmix.qmix.DEFAULT_CONFIG, **config)
        self.config = config
        self.observation_space = obs_space
        self.action_space = action_space
        self.n_agents = len(obs_space.original_space.spaces)
        self.n_actions = action_space.spaces[0].n
        self.h_size = config["model"]["lstm_cell_size"]

        agent_obs_space = obs_space.original_space.spaces[0]
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

        self.model = ModelCatalog.get_model_v2(
            agent_obs_space,
            action_space.spaces[0],
            self.n_actions,
            config["model"],
            framework="torch",
            name="model",
            default_model=RNNModel)

        self.target_model = ModelCatalog.get_model_v2(
            agent_obs_space,
            action_space.spaces[0],
            self.n_actions,
            config["model"],
            framework="torch",
            name="target_model",
            default_model=RNNModel)

        # Setup the mixer network.
        # The global state is just the stacked agent observations for now.
        self.state_shape = [self.obs_size, self.n_agents]
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
        if self.mixer:
            self.params += list(self.mixer.parameters())
        self.loss = QMixLoss(self.model, self.target_model, self.mixer,
                             self.target_mixer, self.n_agents, self.n_actions,
                             self.config["double_q"], self.config["gamma"])
        self.optimiser = RMSprop(
            params=self.params,
            lr=config["lr"],
            alpha=config["optim_alpha"],
            eps=config["optim_eps"])

    @override(Policy)
    def compute_actions(self,
                        obs_batch,
                        state_batches=None,
                        prev_action_batch=None,
                        prev_reward_batch=None,
                        info_batch=None,
                        episodes=None,
                        **kwargs):
        obs_batch, action_mask = self._unpack_observation(obs_batch)

        # Compute actions
        with th.no_grad():
            q_values, hiddens = _mac(
                self.model, th.from_numpy(obs_batch),
                [th.from_numpy(np.array(s)) for s in state_batches])
            avail = th.from_numpy(action_mask).float()
            masked_q_values = q_values.clone()
            masked_q_values[avail == 0.0] = -float("inf")
            # epsilon-greedy action selector
            random_numbers = th.rand_like(q_values[:, :, 0])
            pick_random = (random_numbers < self.cur_epsilon).long()
            random_actions = Categorical(avail).sample().long()
            actions = (pick_random * random_actions +
                       (1 - pick_random) * masked_q_values.max(dim=2)[1])
            actions = actions.numpy()
            hiddens = [s.numpy() for s in hiddens]

        return TupleActions(list(actions.transpose([1, 0]))), hiddens, {}

    @override(Policy)
    def learn_on_batch(self, samples):
        obs_batch, action_mask = self._unpack_observation(
            samples[SampleBatch.CUR_OBS])
        next_obs_batch, next_action_mask = self._unpack_observation(
            samples[SampleBatch.NEXT_OBS])
        group_rewards = self._get_group_rewards(samples[SampleBatch.INFOS])

        # These will be padded to shape [B * T, ...]
        [rew, action_mask, next_action_mask, act, dones, obs, next_obs], \
            initial_states, seq_lens = \
            chop_into_sequences(
                samples[SampleBatch.EPS_ID],
                samples[SampleBatch.UNROLL_ID],
                samples[SampleBatch.AGENT_INDEX], [
                    group_rewards, action_mask, next_action_mask,
                    samples[SampleBatch.ACTIONS], samples[SampleBatch.DONES],
                    obs_batch, next_obs_batch
                ],
                [samples["state_in_{}".format(k)]
                 for k in range(len(self.get_initial_state()))],
                max_seq_len=self.config["model"]["max_seq_len"],
                dynamic_max=True)
        B, T = len(seq_lens), max(seq_lens)

        def to_batches(arr):
            new_shape = [B, T] + list(arr.shape[1:])
            return th.from_numpy(np.reshape(arr, new_shape))

        rewards = to_batches(rew).float()
        actions = to_batches(act).long()
        obs = to_batches(obs).reshape([B, T, self.n_agents,
                                       self.obs_size]).float()
        action_mask = to_batches(action_mask)
        next_obs = to_batches(next_obs).reshape(
            [B, T, self.n_agents, self.obs_size]).float()
        next_action_mask = to_batches(next_action_mask)

        # TODO(ekl) this treats group termination as individual termination
        terminated = to_batches(dones.astype(np.float32)).unsqueeze(2).expand(
            B, T, self.n_agents)

        # Create mask for where index is < unpadded sequence length
        filled = (np.reshape(np.tile(np.arange(T), B), [B, T]) <
                  np.expand_dims(seq_lens, 1)).astype(np.float32)
        mask = th.from_numpy(filled).unsqueeze(2).expand(B, T, self.n_agents)

        # Compute loss
        loss_out, mask, masked_td_error, chosen_action_qvals, targets = \
            self.loss(rewards, actions, terminated, mask, obs,
                      next_obs, action_mask, next_action_mask)

        # Optimise
        self.optimiser.zero_grad()
        loss_out.backward()
        grad_norm = th.nn.utils.clip_grad_norm_(
            self.params, self.config["grad_norm_clipping"])
        self.optimiser.step()

        mask_elems = mask.sum().item()
        stats = {
            "loss": loss_out.item(),
            "grad_norm": grad_norm
            if isinstance(grad_norm, float) else grad_norm.item(),
            "td_error_abs": masked_td_error.abs().sum().item() / mask_elems,
            "q_taken_mean": (chosen_action_qvals * mask).sum().item() /
            mask_elems,
            "target_mean": (targets * mask).sum().item() / mask_elems,
        }
        return {LEARNER_STATS_KEY: stats}

    @override(Policy)
    def get_initial_state(self):
        return [
            s.expand([self.n_agents, -1]).numpy()
            for s in self.model.get_initial_state()
        ]

    @override(Policy)
    def get_weights(self):
        return {"model": self.model.state_dict()}

    @override(Policy)
    def set_weights(self, weights):
        self.model.load_state_dict(weights["model"])

    @override(Policy)
    def get_state(self):
        return {
            "model": self.model.state_dict(),
            "target_model": self.target_model.state_dict(),
            "mixer": self.mixer.state_dict() if self.mixer else None,
            "target_mixer": self.target_mixer.state_dict()
            if self.mixer else None,
            "cur_epsilon": self.cur_epsilon,
        }

    @override(Policy)
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

    def _get_group_rewards(self, info_batch):
        group_rewards = np.array([
            info.get(GROUP_REWARDS, [0.0] * self.n_agents)
            for info in info_batch
        ])
        return group_rewards

    def _unpack_observation(self, obs_batch):
        """Unpacks the action mask / tuple obs from agent grouping.

        Returns:
            obs (Tensor): flattened obs tensor of shape [B, n_agents, obs_size]
            mask (Tensor): action mask, if any
        """
        unpacked = _unpack_obs(
            np.array(obs_batch),
            self.observation_space.original_space,
            tensorlib=np)
        if self.has_action_mask:
            obs = np.concatenate(
                [o["obs"] for o in unpacked],
                axis=1).reshape([len(obs_batch), self.n_agents, self.obs_size])
            action_mask = np.concatenate(
                [o["action_mask"] for o in unpacked], axis=1).reshape(
                    [len(obs_batch), self.n_agents, self.n_actions])
        else:
            obs = np.concatenate(
                unpacked,
                axis=1).reshape([len(obs_batch), self.n_agents, self.obs_size])
            action_mask = np.ones(
                [len(obs_batch), self.n_agents, self.n_actions])
        return obs, action_mask


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
    if len({str(x) for x in obs_space.original_space.spaces}) > 1:
        raise ValueError(
            "Implementation limitation: observations of grouped agents "
            "must be homogeneous, got {}".format(
                obs_space.original_space.spaces))
    if len({str(x) for x in action_space.spaces}) > 1:
        raise ValueError(
            "Implementation limitation: action space of grouped agents "
            "must be homogeneous, got {}".format(action_space.spaces))


def _mac(model, obs, h):
    """Forward pass of the multi-agent controller.

    Arguments:
        model: TorchModelV2 class
        obs: Tensor of shape [B, n_agents, obs_size]
        h: List of tensors of shape [B, n_agents, h_size]

    Returns:
        q_vals: Tensor of shape [B, n_agents, n_actions]
        h: Tensor of shape [B, n_agents, h_size]
    """
    B, n_agents = obs.size(0), obs.size(1)
    obs_flat = obs.reshape([B * n_agents, -1])
    h_flat = [s.reshape([B * n_agents, -1]) for s in h]
    q_flat, h_flat = model({"obs": obs_flat}, h_flat, None)
    return q_flat.reshape(
        [B, n_agents, -1]), [s.reshape([B, n_agents, -1]) for s in h_flat]
