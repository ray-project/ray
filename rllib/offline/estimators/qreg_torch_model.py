from ray.rllib.models.utils import get_initializer
from ray.rllib.policy import Policy
from typing import Dict, List, Union
import numpy as np

from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import TensorType

torch, nn = try_import_torch()


class QRegTorchModel(TorchModelV2):
    def __init__(self, policy: Policy, gamma: float, config: Dict) -> None:
        self.policy = policy
        self.gamma = gamma
        self.observation_space = policy.observation_space
        self.action_space = policy.action_space

        self.q_model: TorchModelV2 = ModelCatalog.get_model_v2(
            self.observation_space,
            self.action_space,
            self.action_space.n,
            config["model"],
            framework="torch",
            name="TorchQModel",
        )
        self.device = self.q_model.device
        self.n_iters = config.get("n_iters", 80)
        self.lr = config.get("lr", 1e-3)
        self.delta = config.get("delta", 1e-4)
        self.optimizer = torch.optim.Adam(self.q_model.variables(), self.lr).to(
            self.device
        )
        self.initializer = get_initializer("xavier_uniform", framework="torch")

    def reset(self) -> None:
        self.q_model.apply(self.initializer)

    def train_q(self, batch: SampleBatch) -> TensorType:
        batch_obs = []
        batch_actions = []
        batch_ps = []
        batch_returns = []
        batch_discounts = []
        for episode in batch.split_by_episode():
            rewards, old_prob = episode["rewards"], episode["action_prob"]
            new_prob = self.action_prob(episode)
            # calculate importance ratios and returns
            p = np.zeros_like(rewards)
            returns = np.zeros_like(rewards)
            discounts = np.zeros_like(rewards)
            for t in range(episode.count):
                discounts[t] = self.gamma ** t
                if t == 0:
                    pt_prev = 1.0
                    pt_next = 1.0
                else:
                    pt_prev = p[t - 1]
                    pt_next = pt_next * new_prob[-t] / old_prob[-t]
                p[t] = pt_prev * new_prob[t] / old_prob[t]
                # Trick: returns[0] is already 0 when t = T
                returns[-t - 1] = rewards[-t - 1] + self.gamma * pt_next * returns[-t]
            batch_obs.extend(episode[SampleBatch.OBS])
            batch_actions.extend(episode[SampleBatch.ACTIONS])
            batch_ps.extend(p)
            batch_returns.extend(returns)
            batch_discounts.extend(discounts)

        obs = torch.tensor(batch_obs, device=self.device)
        actions = torch.tensor(batch_actions, device=self.device)
        ps = torch.tensor(batch_ps, device=self.device)
        returns = torch.tensor(batch_returns, device=self.device)
        discounts = torch.tensor(batch_discounts, device=self.device)
        losses = []
        for _ in range(self.n_iters):
            q_values = self.q_model({"obs": obs}, [], None)
            q_acts = torch.gather(q_values, actions, dim=-1)
            loss = discounts * ps * (returns - q_acts) ** 2
            loss = torch.mean(loss)
            self.optimizer.zero_grad()
            self.optimizer.step()
            losses.append(loss.item())
            if loss < self.delta:
                break
        return np.array(losses, dtype=float)

    def estimate_q(
        self,
        obs: Union[TensorType, List[TensorType]],
        actions: Union[TensorType, List[TensorType]] = None,
    ) -> TensorType:
        obs = torch.tensor(obs, device=self.device)
        actions = torch.tensor(actions, device=self.device)
        q_values = self.q_model.forward({"obs": obs}, [], None)
        q_acts = torch.gather(q_values, actions, dim=-1) if actions else q_values
        return q_acts

    def estimate_v(self, obs: Union[TensorType, List[TensorType]]) -> TensorType:
        obs = torch.tensor(obs, device=self.device)
        q_values = self.estimate_q(obs)
        actions = torch.zeros_like(q_values, device=self.device)
        actions[:] = torch.arange(0, self.action_space.n)
        act_probs = self.compute_log_likelihoods(actions)
        v_values = torch.sum(q_values * act_probs, axis=-1)
        return v_values
