import math
import gym
import numpy as np
from dataclasses import dataclass
from typing import TYPE_CHECKING, List, Tuple, Type

import tree as dm_tree
from rllib.models.specs.specs_dict import ModelSpec

from rllib.models.torch.torch_distributions import TorchCategorical, TorchDiagGaussian
from rllib.utils.annotations import override
from ray.rllib.models.torch.heads.pi import TorchPi

if TYPE_CHECKING:
    from rllib.models.distributions import Distribution


def is_continuous(space: gym.spaces.Space) -> bool:
    """Determines whether the given non-nested space
    uses continuous or discrete actions"""
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
    else:
        raise NotImplementedError(f"Unsupported space {space}")


class DistributionMetadata:
    """Additional metadata associated with each Distribution object.

    DistributionMetadata provides input and output shapes of each distribution,
    and are used by various PiConfigs to construct Pis.

    Attributes:
        distribution_class: The class of distribution to model
        action_space: A primitive (unnested) action space
        input_shape: Automatically set. The feature dimensions of incoming shape
            required by the distribution. E.g. Box(shape=(3,)) and DiagGaussian
            would produce input_shape = (3, 2).
        input_shape_flat: The input_shape flattened to a single dimension
        output_shape: Automatically set. The feature dimensions of the outgoing shape
            required by the action space. E.g. Discrete(3) will produce
            output_shape=(3,).
    """

    distribution_class: Type[Distribution]
    action_space: gym.Space
    input_shape: Tuple[int, ...]
    input_shape_flat: Tuple[int]
    output_shape: Tuple[int]

    def __init__(self, distribution_class: Type[Distribution], action_space: gym.Space):
        self.distribution_class = distribution_class
        self.action_space = action_space
        self.input_shape = distribution_class.required_model_output_shape(action_space)
        self.input_shape_flat = math.prod(self.input_shape)
        self.output_shape = gym.spaces.util.flatdim(action_space)


@dataclass
class PiConfig:
    framework_str: str = "torch"
    output_key: str = "action_dist"

    def decompose_space(self, action_space: gym.Space) -> Distribution:
        """Decompose an arbitrarily-nested gym.space into distributions and shapes.

        This method decomposes an action space into constituent parts. This method
        is to be called by build(), as well as providing an easy way to debug
        space to distribution mappings.

        Args:
            action_space: A (possibly-nested) gym.Space to be mapped to
                action distributions

        Returns:
            A DistributionConfig object, providing the distribution type and shape
                characteristics
        """

    def build(
        self, input_spec: ModelSpec, action_space: gym.Space, *args, **kwargs
    ) -> TorchPi:
        pass


@dataclass
class PolicyGradientPiConfig(PiConfig):
    """PiConfig for policy gradient based Pi, like REINFORCE, PPO, IMPALA, etc.

    Uses a DiagGaussian distribution for continuous action spaces and a
    CategoricalDistribution for discrete spaces.
    """

    @override(PiConfig)
    def decompose_space(self, action_space: gym.Space) -> List[DistributionMetadata]:
        if self.framework_str == "torch":
            cont_cls = TorchDiagGaussian
            disc_cls = TorchCategorical
        else:
            raise NotImplementedError(
                f"Not implemented for framework_str: {self.framework_str}"
            )

        flat_space = dm_tree.flatten(action_space)
        dist_configs = []
        for subspace in flat_space:
            dist_cls = cont_cls if is_continuous(subspace) else disc_cls
            dist_configs.append(DistributionMetadata(dist_cls, subspace))
        return dist_configs

    @override(PiConfig)
    def build(self, input_spec: ModelSpec, action_space: gym.Space) -> TorchPi:
        """Builds the PolicyGradientPiConfing in a Pi"""
        dist_configs = self.decompose_space(action_space)
        return TorchPi(input_spec, dist_configs, self)
