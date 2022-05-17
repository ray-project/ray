import gym
import logging
import numpy as np
import tree  # pip install dm_tree
from typing import Dict, List, Optional, Tuple

import ray
from ray.rllib.algorithms.qmix.mixers import VDNMixer, QMixer
from ray.rllib.algorithms.qmix.model import RNNModel, _get_size
from ray.rllib.env.multi_agent_env import ENV_STATE
from ray.rllib.env.wrappers.group_agents_wrapper import GROUP_REWARDS
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.modelv2 import _unpack_obs
from ray.rllib.models.torch.torch_action_dist import TorchCategorical
from ray.rllib.policy.rnn_sequencing import chop_into_sequences
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.torch_policy import TorchPolicy
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.metrics.learner_info import LEARNER_STATS_KEY
from ray.rllib.utils.typing import TensorType

# Torch must be installed.
torch, nn = try_import_torch(error=True)

logger = logging.getLogger(__name__)


class QMixLoss(nn.Module):
    def __init__(
        self,
        model,
        target_model,
        mixer,
        target_mixer,
        n_agents,
        n_actions,
        double_q=True,
        gamma=0.99,
    ):
        nn.Module.__init__(self)
        self.model = model
        self.target_model = target_model
        self.mixer = mixer
        self.target_mixer = target_mixer
        self.n_agents = n_agents
        self.n_actions = n_actions
        self.double_q = double_q
        self.gamma = gamma

    def forward(
        self,
        rewards,
        actions,
        terminated,
        mask,
        obs,
        next_obs,
        action_mask,
        next_action_mask,
        state=None,
        next_state=None,
    ):
        """Forward pass of the loss.

        Args:
            rewards: Tensor of shape [B, T, n_agents]
            actions: Tensor of shape [B, T, n_agents]
            terminated: Tensor of shape [B, T, n_agents]
            mask: Tensor of shape [B, T, n_agents]
            obs: Tensor of shape [B, T, n_agents, obs_size]
            next_obs: Tensor of shape [B, T, n_agents, obs_size]
            action_mask: Tensor of shape [B, T, n_agents, n_actions]
            next_action_mask: Tensor of shape [B, T, n_agents, n_actions]
            state: Tensor of shape [B, T, state_dim] (optional)
            next_state: Tensor of shape [B, T, state_dim] (optional)
        """

        # Assert either none or both of state and next_state are given
        if state is None and next_state is None:
            state = obs  # default to state being all agents' observations
            next_state = next_obs
        elif (state is None) != (next_state is None):
            raise ValueError(
                "Expected either neither or both of `state` and "
                "`next_state` to be given. Got: "
                "\n`state` = {}\n`next_state` = {}".format(state, next_state)
            )

        # Calculate estimated Q-Values
        mac_out = _unroll_mac(self.model, obs)

        # Pick the Q-Values for the actions taken -> [B * n_agents, T]
        chosen_action_qvals = torch.gather(
            mac_out, dim=3, index=actions.unsqueeze(3)
        ).squeeze(3)

        # Calculate the Q-Values necessary for the target
        target_mac_out = _unroll_mac(self.target_model, next_obs)

        # Mask out unavailable actions for the t+1 step
        ignore_action_tp1 = (next_action_mask == 0) & (mask == 1).unsqueeze(-1)
        target_mac_out[ignore_action_tp1] = -np.inf

        # Max over target Q-Values
        if self.double_q:
            # Double Q learning computes the target Q values by selecting the
            # t+1 timestep action according to the "policy" neural network and
            # then estimating the Q-value of that action with the "target"
            # neural network

            # Compute the t+1 Q-values to be used in action selection
            # using next_obs
            mac_out_tp1 = _unroll_mac(self.model, next_obs)

            # mask out unallowed actions
            mac_out_tp1[ignore_action_tp1] = -np.inf

            # obtain best actions at t+1 according to policy NN
            cur_max_actions = mac_out_tp1.argmax(dim=3, keepdim=True)

            # use the target network to estimate the Q-values of policy
            # network's selected actions
            target_max_qvals = torch.gather(target_mac_out, 3, cur_max_actions).squeeze(
                3
            )
        else:
            target_max_qvals = target_mac_out.max(dim=3)[0]

        assert (
            target_max_qvals.min().item() != -np.inf
        ), "target_max_qvals contains a masked action; \
            there may be a state with no valid actions."

        # Mix
        if self.mixer is not None:
            chosen_action_qvals = self.mixer(chosen_action_qvals, state)
            target_max_qvals = self.target_mixer(target_max_qvals, next_state)

        # Calculate 1-step Q-Learning targets
        targets = rewards + self.gamma * (1 - terminated) * target_max_qvals

        # Td-error
        td_error = chosen_action_qvals - targets.detach()

        mask = mask.expand_as(td_error)

        # 0-out the targets that came from padded data
        masked_td_error = td_error * mask

        # Normal L2 loss, take mean over actual data
        loss = (masked_td_error ** 2).sum() / mask.sum()
        return loss, mask, masked_td_error, chosen_action_qvals, targets


class QMixTorchPolicy(TorchPolicy):
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
        config = dict(ray.rllib.algorithms.qmix.qmix.DEFAULT_CONFIG, **config)
        self.framework = "torch"

        self.n_agents = len(obs_space.original_space.spaces)
        config["model"]["n_agents"] = self.n_agents
        self.n_actions = action_space.spaces[0].n
        self.h_size = config["model"]["lstm_cell_size"]
        self.has_env_global_state = False
        self.has_action_mask = False

        agent_obs_space = obs_space.original_space.spaces[0]
        if isinstance(agent_obs_space, gym.spaces.Dict):
            space_keys = set(agent_obs_space.spaces.keys())
            if "obs" not in space_keys:
                raise ValueError("Dict obs space must have subspace labeled `obs`")
            self.obs_size = _get_size(agent_obs_space.spaces["obs"])
            if "action_mask" in space_keys:
                mask_shape = tuple(agent_obs_space.spaces["action_mask"].shape)
                if mask_shape != (self.n_actions,):
                    raise ValueError(
                        "Action mask shape must be {}, got {}".format(
                            (self.n_actions,), mask_shape
                        )
                    )
                self.has_action_mask = True
            if ENV_STATE in space_keys:
                self.env_global_state_shape = _get_size(
                    agent_obs_space.spaces[ENV_STATE]
                )
                self.has_env_global_state = True
            else:
                self.env_global_state_shape = (self.obs_size, self.n_agents)
            # The real agent obs space is nested inside the dict
            config["model"]["full_obs_space"] = agent_obs_space
            agent_obs_space = agent_obs_space.spaces["obs"]
        else:
            self.obs_size = _get_size(agent_obs_space)
            self.env_global_state_shape = (self.obs_size, self.n_agents)

        self.model = ModelCatalog.get_model_v2(
            agent_obs_space,
            action_space.spaces[0],
            self.n_actions,
            config["model"],
            framework="torch",
            name="model",
            default_model=RNNModel,
        )

        super().__init__(obs_space, action_space, config, model=self.model)

        self.target_model = ModelCatalog.get_model_v2(
            agent_obs_space,
            action_space.spaces[0],
            self.n_actions,
            config["model"],
            framework="torch",
            name="target_model",
            default_model=RNNModel,
        ).to(self.device)

        self.exploration = self._create_exploration()

        # Setup the mixer network.
        if config["mixer"] is None:
            self.mixer = None
            self.target_mixer = None
        elif config["mixer"] == "qmix":
            self.mixer = QMixer(
                self.n_agents, self.env_global_state_shape, config["mixing_embed_dim"]
            ).to(self.device)
            self.target_mixer = QMixer(
                self.n_agents, self.env_global_state_shape, config["mixing_embed_dim"]
            ).to(self.device)
        elif config["mixer"] == "vdn":
            self.mixer = VDNMixer().to(self.device)
            self.target_mixer = VDNMixer().to(self.device)
        else:
            raise ValueError("Unknown mixer type {}".format(config["mixer"]))

        self.cur_epsilon = 1.0
        self.update_target()  # initial sync

        # Setup optimizer
        self.params = list(self.model.parameters())
        if self.mixer:
            self.params += list(self.mixer.parameters())
        self.loss = QMixLoss(
            self.model,
            self.target_model,
            self.mixer,
            self.target_mixer,
            self.n_agents,
            self.n_actions,
            self.config["double_q"],
            self.config["gamma"],
        )
        from torch.optim import RMSprop

        self.optimiser = RMSprop(
            params=self.params,
            lr=config["lr"],
            alpha=config["optim_alpha"],
            eps=config["optim_eps"],
        )

    @override(TorchPolicy)
    def compute_actions_from_input_dict(
        self,
        input_dict: Dict[str, TensorType],
        explore: bool = None,
        timestep: Optional[int] = None,
        **kwargs,
    ) -> Tuple[TensorType, List[TensorType], Dict[str, TensorType]]:

        obs_batch = input_dict[SampleBatch.OBS]
        state_batches = []
        i = 0
        while f"state_in_{i}" in input_dict:
            state_batches.append(input_dict[f"state_in_{i}"])
            i += 1

        explore = explore if explore is not None else self.config["explore"]
        obs_batch, action_mask, _ = self._unpack_observation(obs_batch)
        # We need to ensure we do not use the env global state
        # to compute actions

        # Compute actions
        with torch.no_grad():
            q_values, hiddens = _mac(
                self.model,
                torch.as_tensor(obs_batch, dtype=torch.float, device=self.device),
                [
                    torch.as_tensor(np.array(s), dtype=torch.float, device=self.device)
                    for s in state_batches
                ],
            )
            avail = torch.as_tensor(action_mask, dtype=torch.float, device=self.device)
            masked_q_values = q_values.clone()
            masked_q_values[avail == 0.0] = -float("inf")
            masked_q_values_folded = torch.reshape(
                masked_q_values, [-1] + list(masked_q_values.shape)[2:]
            )
            actions, _ = self.exploration.get_exploration_action(
                action_distribution=TorchCategorical(masked_q_values_folded),
                timestep=timestep,
                explore=explore,
            )
            actions = (
                torch.reshape(actions, list(masked_q_values.shape)[:-1]).cpu().numpy()
            )
            hiddens = [s.cpu().numpy() for s in hiddens]

        return tuple(actions.transpose([1, 0])), hiddens, {}

    @override(TorchPolicy)
    def compute_actions(self, *args, **kwargs):
        return self.compute_actions_from_input_dict(*args, **kwargs)

    @override(TorchPolicy)
    def compute_log_likelihoods(
        self,
        actions,
        obs_batch,
        state_batches=None,
        prev_action_batch=None,
        prev_reward_batch=None,
    ):
        obs_batch, action_mask, _ = self._unpack_observation(obs_batch)
        return np.zeros(obs_batch.size()[0])

    @override(TorchPolicy)
    def learn_on_batch(self, samples):
        obs_batch, action_mask, env_global_state = self._unpack_observation(
            samples[SampleBatch.CUR_OBS]
        )
        (
            next_obs_batch,
            next_action_mask,
            next_env_global_state,
        ) = self._unpack_observation(samples[SampleBatch.NEXT_OBS])
        group_rewards = self._get_group_rewards(samples[SampleBatch.INFOS])

        input_list = [
            group_rewards,
            action_mask,
            next_action_mask,
            samples[SampleBatch.ACTIONS],
            samples[SampleBatch.DONES],
            obs_batch,
            next_obs_batch,
        ]
        if self.has_env_global_state:
            input_list.extend([env_global_state, next_env_global_state])

        output_list, _, seq_lens = chop_into_sequences(
            episode_ids=samples[SampleBatch.EPS_ID],
            unroll_ids=samples[SampleBatch.UNROLL_ID],
            agent_indices=samples[SampleBatch.AGENT_INDEX],
            feature_columns=input_list,
            state_columns=[],  # RNN states not used here
            max_seq_len=self.config["model"]["max_seq_len"],
            dynamic_max=True,
        )
        # These will be padded to shape [B * T, ...]
        if self.has_env_global_state:
            (
                rew,
                action_mask,
                next_action_mask,
                act,
                dones,
                obs,
                next_obs,
                env_global_state,
                next_env_global_state,
            ) = output_list
        else:
            (
                rew,
                action_mask,
                next_action_mask,
                act,
                dones,
                obs,
                next_obs,
            ) = output_list
        B, T = len(seq_lens), max(seq_lens)

        def to_batches(arr, dtype):
            new_shape = [B, T] + list(arr.shape[1:])
            return torch.as_tensor(
                np.reshape(arr, new_shape), dtype=dtype, device=self.device
            )

        rewards = to_batches(rew, torch.float)
        actions = to_batches(act, torch.long)
        obs = to_batches(obs, torch.float).reshape([B, T, self.n_agents, self.obs_size])
        action_mask = to_batches(action_mask, torch.float)
        next_obs = to_batches(next_obs, torch.float).reshape(
            [B, T, self.n_agents, self.obs_size]
        )
        next_action_mask = to_batches(next_action_mask, torch.float)
        if self.has_env_global_state:
            env_global_state = to_batches(env_global_state, torch.float)
            next_env_global_state = to_batches(next_env_global_state, torch.float)

        # TODO(ekl) this treats group termination as individual termination
        terminated = (
            to_batches(dones, torch.float).unsqueeze(2).expand(B, T, self.n_agents)
        )

        # Create mask for where index is < unpadded sequence length
        filled = np.reshape(
            np.tile(np.arange(T, dtype=np.float32), B), [B, T]
        ) < np.expand_dims(seq_lens, 1)
        mask = (
            torch.as_tensor(filled, dtype=torch.float, device=self.device)
            .unsqueeze(2)
            .expand(B, T, self.n_agents)
        )

        # Compute loss
        loss_out, mask, masked_td_error, chosen_action_qvals, targets = self.loss(
            rewards,
            actions,
            terminated,
            mask,
            obs,
            next_obs,
            action_mask,
            next_action_mask,
            env_global_state,
            next_env_global_state,
        )

        # Optimise
        self.optimiser.zero_grad()
        loss_out.backward()
        grad_norm = torch.nn.utils.clip_grad_norm_(
            self.params, self.config["grad_norm_clipping"]
        )
        self.optimiser.step()

        mask_elems = mask.sum().item()
        stats = {
            "loss": loss_out.item(),
            "grad_norm": grad_norm
            if isinstance(grad_norm, float)
            else grad_norm.item(),
            "td_error_abs": masked_td_error.abs().sum().item() / mask_elems,
            "q_taken_mean": (chosen_action_qvals * mask).sum().item() / mask_elems,
            "target_mean": (targets * mask).sum().item() / mask_elems,
        }
        return {LEARNER_STATS_KEY: stats}

    @override(TorchPolicy)
    def get_initial_state(self):  # initial RNN state
        return [
            s.expand([self.n_agents, -1]).cpu().numpy()
            for s in self.model.get_initial_state()
        ]

    @override(TorchPolicy)
    def get_weights(self):
        return {
            "model": self._cpu_dict(self.model.state_dict()),
            "target_model": self._cpu_dict(self.target_model.state_dict()),
            "mixer": self._cpu_dict(self.mixer.state_dict()) if self.mixer else None,
            "target_mixer": self._cpu_dict(self.target_mixer.state_dict())
            if self.mixer
            else None,
        }

    @override(TorchPolicy)
    def set_weights(self, weights):
        self.model.load_state_dict(self._device_dict(weights["model"]))
        self.target_model.load_state_dict(self._device_dict(weights["target_model"]))
        if weights["mixer"] is not None:
            self.mixer.load_state_dict(self._device_dict(weights["mixer"]))
            self.target_mixer.load_state_dict(
                self._device_dict(weights["target_mixer"])
            )

    @override(TorchPolicy)
    def get_state(self):
        state = self.get_weights()
        state["cur_epsilon"] = self.cur_epsilon
        return state

    @override(TorchPolicy)
    def set_state(self, state):
        self.set_weights(state)
        self.set_epsilon(state["cur_epsilon"])

    def update_target(self):
        self.target_model.load_state_dict(self.model.state_dict())
        if self.mixer is not None:
            self.target_mixer.load_state_dict(self.mixer.state_dict())
        logger.debug("Updated target networks")

    def set_epsilon(self, epsilon):
        self.cur_epsilon = epsilon

    def _get_group_rewards(self, info_batch):
        group_rewards = np.array(
            [info.get(GROUP_REWARDS, [0.0] * self.n_agents) for info in info_batch]
        )
        return group_rewards

    def _device_dict(self, state_dict):
        return {
            k: torch.as_tensor(v, device=self.device) for k, v in state_dict.items()
        }

    @staticmethod
    def _cpu_dict(state_dict):
        return {k: v.cpu().detach().numpy() for k, v in state_dict.items()}

    def _unpack_observation(self, obs_batch):
        """Unpacks the observation, action mask, and state (if present)
        from agent grouping.

        Returns:
            obs (np.ndarray): obs tensor of shape [B, n_agents, obs_size]
            mask (np.ndarray): action mask, if any
            state (np.ndarray or None): state tensor of shape [B, state_size]
                or None if it is not in the batch
        """

        unpacked = _unpack_obs(
            np.array(obs_batch, dtype=np.float32),
            self.observation_space.original_space,
            tensorlib=np,
        )

        if isinstance(unpacked[0], dict):
            assert "obs" in unpacked[0]
            unpacked_obs = [np.concatenate(tree.flatten(u["obs"]), 1) for u in unpacked]
        else:
            unpacked_obs = unpacked

        obs = np.concatenate(unpacked_obs, axis=1).reshape(
            [len(obs_batch), self.n_agents, self.obs_size]
        )

        if self.has_action_mask:
            action_mask = np.concatenate(
                [o["action_mask"] for o in unpacked], axis=1
            ).reshape([len(obs_batch), self.n_agents, self.n_actions])
        else:
            action_mask = np.ones(
                [len(obs_batch), self.n_agents, self.n_actions], dtype=np.float32
            )

        if self.has_env_global_state:
            state = np.concatenate(tree.flatten(unpacked[0][ENV_STATE]), 1)
        else:
            state = None
        return obs, action_mask, state


def _validate(obs_space, action_space):
    if not hasattr(obs_space, "original_space") or not isinstance(
        obs_space.original_space, gym.spaces.Tuple
    ):
        raise ValueError(
            "Obs space must be a Tuple, got {}. Use ".format(obs_space)
            + "MultiAgentEnv.with_agent_groups() to group related "
            "agents for QMix."
        )
    if not isinstance(action_space, gym.spaces.Tuple):
        raise ValueError(
            "Action space must be a Tuple, got {}. ".format(action_space)
            + "Use MultiAgentEnv.with_agent_groups() to group related "
            "agents for QMix."
        )
    if not isinstance(action_space.spaces[0], gym.spaces.Discrete):
        raise ValueError(
            "QMix requires a discrete action space, got {}".format(
                action_space.spaces[0]
            )
        )
    if len({str(x) for x in obs_space.original_space.spaces}) > 1:
        raise ValueError(
            "Implementation limitation: observations of grouped agents "
            "must be homogeneous, got {}".format(obs_space.original_space.spaces)
        )
    if len({str(x) for x in action_space.spaces}) > 1:
        raise ValueError(
            "Implementation limitation: action space of grouped agents "
            "must be homogeneous, got {}".format(action_space.spaces)
        )


def _mac(model, obs, h):
    """Forward pass of the multi-agent controller.

    Args:
        model: TorchModelV2 class
        obs: Tensor of shape [B, n_agents, obs_size]
        h: List of tensors of shape [B, n_agents, h_size]

    Returns:
        q_vals: Tensor of shape [B, n_agents, n_actions]
        h: Tensor of shape [B, n_agents, h_size]
    """
    B, n_agents = obs.size(0), obs.size(1)
    if not isinstance(obs, dict):
        obs = {"obs": obs}
    obs_agents_as_batches = {k: _drop_agent_dim(v) for k, v in obs.items()}
    h_flat = [s.reshape([B * n_agents, -1]) for s in h]
    q_flat, h_flat = model(obs_agents_as_batches, h_flat, None)
    return q_flat.reshape([B, n_agents, -1]), [
        s.reshape([B, n_agents, -1]) for s in h_flat
    ]


def _unroll_mac(model, obs_tensor):
    """Computes the estimated Q values for an entire trajectory batch"""
    B = obs_tensor.size(0)
    T = obs_tensor.size(1)
    n_agents = obs_tensor.size(2)

    mac_out = []
    h = [s.expand([B, n_agents, -1]) for s in model.get_initial_state()]
    for t in range(T):
        q, h = _mac(model, obs_tensor[:, t], h)
        mac_out.append(q)
    mac_out = torch.stack(mac_out, dim=1)  # Concat over time

    return mac_out


def _drop_agent_dim(T):
    shape = list(T.shape)
    B, n_agents = shape[0], shape[1]
    return T.reshape([B * n_agents] + shape[2:])


def _add_agent_dim(T, n_agents):
    shape = list(T.shape)
    B = shape[0] // n_agents
    assert shape[0] % n_agents == 0
    return T.reshape([B, n_agents] + shape[1:])
