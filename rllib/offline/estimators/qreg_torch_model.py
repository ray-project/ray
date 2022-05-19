from ray.rllib.models.utils import get_initializer
from ray.rllib.policy import Policy
from typing import Dict, List
import gym
from gym.spaces import Box, Discrete
import numpy as np

from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.typing import SampleBatchType, TensorType, TensorStructType

torch, nn = try_import_torch()


class QRegTorchModel:
    def __init__(self,
        observation_space: gym.Space,
        action_space: gym.Space,
        config: Dict) -> None:
        self.observation_space = observation_space
        self.action_space = action_space

        self.q_model: TorchModelV2 = ModelCatalog.get_model_v2(
            self.observation_space,
            self.action_space,
            self.action_space.n,
            config["model"],
            framework="torch",
            name="QRegTorchModel",
        )
        self.device = self.q_model.device
        self.n_iters = config.get("n_iters", 80)
        self.lr = config.get("lr", 1e-3)
        self.delta = config.get("delta", 1e-4)
        self.optimizer = torch.optim.Adam(self.q_model.variables(), self.lr).to(self.device)
        self.initializer = get_initializer("xavier_uniform", framework="torch")
    
    def reset(self) -> None:
        self.q_model.apply(self.initializer)

    def train_q(self,
        obs: List[TensorType],
        actions: List[TensorType],
        ps: List[TensorType],
        returns: List[TensorType],
        discounts: List[TensorType],
        ) -> List[float]:
        obs = torch.Tensor(obs, device=self.device)
        actions = torch.Tensor(actions, device=self.device)
        ps = torch.Tensor(ps, device=self.device)
        returns = torch.Tensor(returns, device=self.device)
        discounts = torch.Tensor(discounts, device=self.device)
        losses = []
        for _ in range(self.n_iters):
            q_values = self.q_model.forward({"obs": obs}, [], None)
            q_acts = torch.gather(q_values, actions)
            loss = discounts * ps * (returns - q_acts) ** 2
            loss = torch.mean(loss)
            self.optimizer.zero_grad()
            self.optimizer.step()
            losses.append(loss.item())
            if loss < self.delta:
                break
        return losses


    def estimate_q(self, obs: List[TensorType]) -> TensorType:
        obs = torch.Tensor(obs, device=self.device)
        q_values = self.q_model.forward({"obs": obs}, [], None)
        return convert_to_numpy(q_values)