from dataclasses import dataclass
from typing import List, Optional
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.models.specs.specs_dict import ModelSpec
import torch
from torch import nn
import gym
import tree
import numpy as np
from rllib.models.torch.model import TorchModel
from rllib.catalog.torch.encoders.vector import get_feature_size


class TorchActorConfig:
    pass


class TorchContinuousActor(TorchActorConfig):
    pass


class TorchDiscreteActor(TorchActorConfig):
    pass


class TorchDiagGaussianActor(TorchContinuousActor):
    def __init__(
        self, in_spec: ModelSpec, action_dim: int, config: "TorchContinuousActorConfig"
    ):
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
        dist_input = self.map(torch.cat(inputs.values(), dim=-1))
        # TODO: Do not assume final dim is feature dim
        if self.config.std == "elementwise":
            mu_logits, sigma_logits = self.map(dist_input).chunk(2, dim=-1)
        else:
            logits = self.map(dist_input)
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


class TorchActor(TorchModel):
    pass


class TorchDiscreteActorConfig:
    pass


class TorchContinuousActorConfig:
    pass


def is_continuous(space: gym.spaces.Space) -> bool:
    if isinstance(space, gym.spaces.Box):
        if np.issubdtype(space.dtype, np.floating):
            return True
        else:
            return False
    elif (
        isinstance(space, gym.spaces.Discrete)
        or isinstance(space, gym.spaces.MultiDiscrete)
        or isinstance(space, gym.spaces.Binary)
    ):
        return False


@dataclass
class TorchDiagGaussianActorConfig(TorchContinuousActorConfig):
    std: str = "elementwise"

    def build(self, in_spec: ModelSpec, action_space: gym.Space) -> TorchActor:
        return TorchDiagGaussianActor(in_spec, action_space, self)


@dataclass
class TorchCategoricalActorConfig(TorchDiscreteActorConfig):
    def build(self, in_spec: ModelSpec, action_space: gym.Space) -> TorchActor:
        return TorchCategoricalActor(in_spec, action_space, self)


class TorchMixedDistribution(torch.distributions.Distribution):
    def __init__(self, distributions: List[torch.distributions.Distribution]):
        self.distributions = distributions

    def sample(self) -> torch.tensor:
        return torch.cat([s.sample() for s in self.distributions], dim=-1)

    def sample_n(self, n: int) -> torch.tensor:
        return torch.cat([s.sample_n(n) for s in self.distributions], dim=-1)

    def log_prob(self) -> torch.tensor:
        return torch.cat([s.log_prob() for s in self.distributions], dim=-1)

    def entropy(self) -> torch.tensor:
        return torch.cat([s.entropy() for s in self.distributions], dim=-1)


class TorchListActor(TorchActor):
    def __init__(self, actors: List[TorchActor]):
        self.actors = actors

    def forward(self, inputs: NestedDict) -> NestedDict:
        distributions = TorchMixedDistribution([act(inputs) for act in self.actors])
        return NestedDict({"action_dist": distributions})


@dataclass
class TorchMultiActorConfig:
    discrete_actor: TorchDiscreteActorConfig
    continuous_actor: TorchContinuousActorConfig
    # Override discrete/continuous defaults with a custom list of actor configs
    child_configs: Optional[List[TorchActorConfig]] = None
    # Set by build()
    out_spec: ModelSpec = None

    def get_default_actor_configs(
        self, in_spec: ModelSpec, space: gym.spaces.Space
    ) -> List[gym.spaces.Space]:
        flat = tree.flatten(space)
        sizes = [gym.spaces.utils.flatdim(s) for s in flat]
        actor_configs = [
            self.continuous_actor if is_continuous(s) else self.discrete_actor
            for s in flat
        ]
        return [cfg(in_spec, sz) for sz, cfg in zip(sizes, actor_configs)]

    def build(self, in_spec: ModelSpec, action_space: gym.Space) -> TorchListActor:
        # TODO: For autorecurrent actions, convert in_spec distribution objects
        # to TensorSpec using torch.distributions.event_shape
        if self.child_configs is not None:
            self.child_configs = self.get_default_actor_configs(action_space)
        actor_instance = TorchListActor(
            [actor_cfg.build(in_spec) for actor_cfg in self.child_configs]
        )
        self.out_spec = ModelSpec(
            {
                "action_dist": [
                    actor.output_spec() for i, actor in enumerate(actor_instance.actors)
                ]
            }
        )
        return actor_instance
