from abc import abstractmethod
import math
import gym
import numpy as np
from dataclasses import dataclass
from typing import List, Optional, Tuple, Type
from ray.rllib.utils.typing import TensorType
import torch

import tree as dm_tree
from ray.rllib.models.specs.specs_dict import ModelSpec

from ray.rllib.models.torch.torch_distributions import (
    TorchCategorical,
    TorchDistribution,
    TorchSquashedDiagGaussian,
    TorchBernoulli,
)
from ray.rllib.utils.annotations import override
from ray.rllib.models.torch.heads.pi import TorchPi
from ray.rllib.models.distributions import Distribution


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
        or isinstance(space, gym.spaces.MultiBinary)
    ):
        return False
    else:
        raise NotImplementedError(f"Unsupported space {space}")


class DistributionConfig:
    """Config for building Distribution objects used in Pi's.

    DistributionConfig provides input and output shapes of each distribution,
    and are used by various PiConfigs when constructing Pis.

    Args:
        distribution_class: The class of distribution to model
        action_space: A primitive (unnested) action space

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
        low: An optional lower bound on the support of the distribution
        high: An optional upper bound on the support of the distribution
    """

    framework_str: str = "torch"
    distribution_class: Type[Distribution]
    action_space: gym.Space
    # The following will be automatically be set in init
    input_shape: Tuple[int, ...]
    input_shape_flat: Tuple[int]
    output_shape: Tuple[int]
    low: Optional[TensorType]
    high: Optional[TensorType]

    def __repr__(self):
        return (
            f"{self.__class__.__name__}(\n"
            + "\n   ".join([f"{k}={v}" for k, v in vars(self).items()])
            + "\n)"
        )

    def __init__(self, distribution_class: Type[Distribution], action_space: gym.Space):
        self.distribution_class = distribution_class
        self.action_space = action_space

        self.input_shape = distribution_class.required_model_output_shape(
            action_space, None
        )
        self.input_shape_flat = math.prod(self.input_shape)
        self.output_shape = gym.spaces.utils.flatdim(action_space)
        if is_continuous(action_space):
            if self.framework_str == "torch":
                self.low = torch.from_numpy(self.action_space.low)
                self.high = torch.from_numpy(self.action_space.high)
            else:
                raise NotImplementedError()

    def build(self, distribution_input: TensorType) -> Distribution:
        """Build the config into a Distribution object.

        Note that build() is called for each Pi forward pass, so keep
        this lightweight.

        Args:
            distribution_input: A tensor of inputs to the distribution,
                sometimes called logits
        Returns:
            A Distribution instance, ready to be sampled from
        """
        if is_continuous(self.action_space):
            if self.framework_str == "torch":
                self.low = self.low.to(distribution_input.device)
                self.high = self.high.to(distribution_input.device)
            else:
                raise NotImplementedError()
            return self.distribution_class(
                distribution_input, low=self.low, high=self.high
            )
        else:
            return self.distribution_class(logits=distribution_input)


@dataclass
class PiConfig:
    """A config for Pi's, where a ~ Pi(s)

    PiConfig determines how to construct a Pi, which maps some latent
    state to an action space.
    """

    framework_str: str = "torch"
    output_key: str = "action_dist"

    @abstractmethod
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

    @abstractmethod
    def build(
        self, input_spec: ModelSpec, action_space: gym.Space, *args, **kwargs
    ) -> TorchPi:
        """Build the PiConfig into an instance of Pi

        Args:
            input_spec: The spec describing the inputs of this module
            action_space: The (possibly nested) action space this Pi
                will output actions for

        Returns:
            An instance of TorchPi
        """


@dataclass
class PolicyGradientPiConfig(PiConfig):
    """PiConfig for policy gradient based Pi, like REINFORCE, PPO, IMPALA, etc.

    Uses a DiagGaussian distribution for continuous action spaces and a
    CategoricalDistribution for discrete spaces.
    """

    def get_torch_distribution(self, space: gym.Space) -> TorchDistribution:
        """Return the distribution associated with each primitive gym action space.

        Args:
            space: Action space to get the distribution for

        Returns:
            The associated TorchDistribution
        """
        if isinstance(space, gym.spaces.MultiBinary):
            return TorchBernoulli
        elif isinstance(space, gym.spaces.Discrete):
            return TorchCategorical
        elif isinstance(space, gym.spaces.Box):
            if is_continuous(space):
                return TorchSquashedDiagGaussian
            else:
                raise NotImplementedError(
                    "Use MultiDiscrete or Discrete instead of Box(dtype=int)"
                )
        else:
            raise NotImplementedError(f"Unsupported action space {space}")

    @override(PiConfig)
    def decompose_space(self, action_space: gym.Space) -> List[DistributionConfig]:
        """Decompose an action space into a list of DistributionConfigs

        Args:
            action_space: A (possibly nested) action space that we want to
                decompose into individual DistributionConfigs

        Returns:
            A list of DistributionConfigs that will be built in Distributions
                within the Pi.
        """
        flat_space = dm_tree.flatten(action_space)
        # Constructing a Tuple from MultiDiscrete
        # results in a Tuple of Discrete
        flat_space = [
            gym.spaces.Tuple(s) if isinstance(s, gym.spaces.MultiDiscrete) else s
            for s in flat_space
        ]
        # Reflatten added tuples
        flat_space = dm_tree.flatten(flat_space)

        if self.framework_str == "torch":
            get_distribution = self.get_torch_distribution
        else:
            raise NotImplementedError(
                f"Not implemented for framework_str: {self.framework_str}"
            )
        dist_configs = []
        for subspace in flat_space:
            dist_cls = get_distribution(subspace)
            dist_configs.append(DistributionConfig(dist_cls, subspace))
        return dist_configs

    @override(PiConfig)
    def build(self, input_spec: ModelSpec, action_space: gym.Space) -> TorchPi:
        """Builds the PolicyGradientPiConfing in a Pi"""
        dist_configs = self.decompose_space(action_space)
        return TorchPi(input_spec, dist_configs, self)
