import gymnasium as gym
from typing import Any, Mapping, Union

import torch.nn as nn
import torch
from torch.distributions import Categorical

from ray.rllib.core.rl_module import RLModule
from ray.rllib.core.rl_module.torch.torch_rl_module import TorchRLModule
from ray.rllib.models.specs.specs_dict import ModelSpec
from ray.rllib.models.specs.specs_torch import TorchTensorSpec
from ray.rllib.utils.annotations import override
from ray.rllib.utils.nested_dict import NestedDict


class DiscreteBCTorchModule(TorchRLModule):
    def __init__(
        self,
        input_dim: int,
        hidden_dim: int,
        output_dim: int,
    ) -> None:
        super().__init__()
        self.policy = nn.Sequential(
            nn.Linear(input_dim, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, output_dim),
        )

        self.input_dim = input_dim

    @override(RLModule)
    def input_specs_exploration(self) -> ModelSpec:
        return ModelSpec(self._default_inputs())

    @override(RLModule)
    def input_specs_inference(self) -> ModelSpec:
        return ModelSpec(self._default_inputs())

    @override(RLModule)
    def input_specs_train(self) -> ModelSpec:
        return ModelSpec(self._default_inputs())

    @override(RLModule)
    def output_specs_exploration(self) -> ModelSpec:
        return ModelSpec(self._default_outputs())

    @override(RLModule)
    def output_specs_inference(self) -> ModelSpec:
        return ModelSpec(self._default_outputs())

    @override(RLModule)
    def output_specs_train(self) -> ModelSpec:
        return ModelSpec(self._default_outputs())

    @override(RLModule)
    def _forward_inference(self, batch: NestedDict) -> Mapping[str, Any]:
        with torch.no_grad():
            return self._forward_train(batch)

    @override(RLModule)
    def _forward_exploration(self, batch: NestedDict) -> Mapping[str, Any]:
        with torch.no_grad():
            return self._forward_train(batch)

    @override(RLModule)
    def _forward_train(self, batch: NestedDict) -> Mapping[str, Any]:
        action_logits = self.policy(batch["obs"])
        return {"action_dist": Categorical(logits=action_logits)}

    @classmethod
    @override(RLModule)
    def from_model_config(
        cls,
        observation_space: "gym.Space",
        action_space: "gym.Space",
        model_config: Mapping[str, Any],
    ) -> Union["RLModule", Mapping[str, Any]]:

        config = {
            "input_dim": observation_space.shape[0],
            "hidden_dim": model_config["hidden_dim"],
            "output_dim": action_space.n,
        }

        return cls(**config)

    def _default_inputs(self) -> dict:
        return {
            "obs": TorchTensorSpec("b, do", do=self.input_dim),
        }

    def _default_outputs(self) -> dict:
        return {"action_dist": torch.distributions.Categorical}
