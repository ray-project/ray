from dataclasses import dataclass
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.models.specs.specs_dict import ModelSpec
import torch
from torch import nn
import gym
from rllib.models.torch.model import TorchModel
from rllib.catalog.torch.encoders.vector import get_feature_size


class TorchActor(TorchModel):
    pass


class TorchContinuousActor(TorchActor):
    pass


class TorchDiscreteActor(TorchActor):
    pass


class TorchMultiBinaryActor(TorchDiscreteActor):
    pass


class TorchMultiDiscreteActor(TorchDiscreteActor):
    pass


class TorchDiagGaussianActor(TorchContinuousActor):
    def __init__(self, in_spec: ModelSpec, action_dim: int, config: "TorchActorConfig"):
        self.config = config
        # TODO: Concat across feature dim allowing multiple inputs
        # logits -> mu, sigma
        self._in_spec = in_spec
        self.action_dim = action_dim
        if config.std == "elementwise":
            self.map = nn.Linear(get_feature_size(in_spec), 2 * self.action_dim)
        elif config.std == "shared":
            self.map = nn.Linear(get_feature_size(in_spec), 1 + self.action_dim)
        else:
            raise NotImplementedError()

    @property
    def input_spec(self) -> ModelSpec:
        return self._in_spec

    def output_spec(self) -> ModelSpec:
        return self.config.out_spec

    def forward(self, inputs: NestedDict) -> NestedDict:
        logits = self.map(inputs.values()[0])
        # TODO: Do not assume final dim is feature dim
        if self.config.std == "elementwise":
            mu_logits, sigma_logits = self.map(inputs.values()[0]).chunk(2, dim=-1)
        else:
            logits = self.map(inputs.values()[0])
            mu_logits = logits[..., :-1]
            sigma_logits = logits[..., -1]
        inputs[self.out_spec.keys()[0]] = torch.distributions.Normal(
            mu_logits, sigma_logits
        )
        return inputs


class TorchCategoricalActor(TorchDiscreteActor):
    def __init__(self, in_spec: ModelSpec, action_dim: int, config: "TorchActorConfig"):
        self.config = config
        # TODO: Concat across feature dim allowing multiple inputs
        self._in_spec = in_spec
        # self.action_dim = gym.spaces.util.flatdim(action_space)
        self.action_dim = action_dim
        self.map = nn.Linear(get_feature_size(in_spec), self.action_dim)

    @property
    def input_spec(self) -> ModelSpec:
        return self._in_spec

    def output_spec(self) -> ModelSpec:
        return self.config.out_spec

    def forward(self, inputs: NestedDict) -> NestedDict:
        logits = self.map(inputs.values()[0])
        inputs[self.out_spec.keys()[0]] = torch.distributions.Categorical(logits)
        return inputs


@dataclass
class TorchActorConfig:
    pass


class TorchDiscreteActorConfig(TorchActorConfig):
    pass


class TorchContinuousActorConfig(TorchActorConfig):
    pass


@dataclass
class TorchDiagGaussianActorConfig(TorchContinuousActorConfig):
    std: str = "elementwise"

    def build(self, in_spec: ModelSpec, action_space: gym.Space) -> TorchActor:
        return TorchDiagGaussianActor(in_spec, action_space, self)


@dataclass
class TorchCategoricalActorConfig(TorchDiscreteActorConfig):
    def build(self, in_spec: ModelSpec, action_space: gym.Space) -> TorchActor:
        return TorchCategoricalActor(in_spec, action_space, self)
