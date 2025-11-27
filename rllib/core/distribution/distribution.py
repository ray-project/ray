"""This is the next version of action distribution base class."""
import abc
from typing import Tuple

import gymnasium as gym

from ray.rllib.utils.annotations import ExperimentalAPI, override
from ray.rllib.utils.typing import TensorType, Union


@ExperimentalAPI
class Distribution(abc.ABC):
    """The base class for distribution over a random variable.

    Examples:

    .. testcode::

        import torch
        from ray.rllib.core.models.configs import MLPHeadConfig
        from ray.rllib.core.distribution.torch.torch_distribution import (
            TorchCategorical
        )

        model = MLPHeadConfig(input_dims=[1]).build(framework="torch")

        # Create an action distribution from model logits
        action_logits = model(torch.Tensor([[1]]))
        action_dist = TorchCategorical.from_logits(action_logits)
        action = action_dist.sample()

        # Create another distribution from a dummy Tensor
        action_dist2 = TorchCategorical.from_logits(torch.Tensor([0]))

        # Compute some common metrics
        logp = action_dist.logp(action)
        kl = action_dist.kl(action_dist2)
        entropy = action_dist.entropy()
    """

    @abc.abstractmethod
    def sample(
        self,
        *,
        sample_shape: Tuple[int, ...] = None,
        return_logp: bool = False,
        **kwargs,
    ) -> Union[TensorType, Tuple[TensorType, TensorType]]:
        """Draw a sample from the distribution.

        Args:
            sample_shape: The shape of the sample to draw.
            return_logp: Whether to return the logp of the sampled values.
            **kwargs: Forward compatibility placeholder.

        Returns:
            The sampled values. If return_logp is True, returns a tuple of the
            sampled values and its logp.
        """

    @abc.abstractmethod
    def rsample(
        self,
        *,
        sample_shape: Tuple[int, ...] = None,
        return_logp: bool = False,
        **kwargs,
    ) -> Union[TensorType, Tuple[TensorType, TensorType]]:
        """Draw a re-parameterized sample from the action distribution.

        If this method is implemented, we can take gradients of samples w.r.t. the
        distribution parameters.

        Args:
            sample_shape: The shape of the sample to draw.
            return_logp: Whether to return the logp of the sampled values.
            **kwargs: Forward compatibility placeholder.

        Returns:
            The sampled values. If return_logp is True, returns a tuple of the
            sampled values and its logp.
        """

    @abc.abstractmethod
    def logp(self, value: TensorType, **kwargs) -> TensorType:
        """The log-likelihood of the distribution computed at `value`

        Args:
            value: The value to compute the log-likelihood at.
            **kwargs: Forward compatibility placeholder.

        Returns:
            The log-likelihood of the value.
        """

    @abc.abstractmethod
    def kl(self, other: "Distribution", **kwargs) -> TensorType:
        """The KL-divergence between two distributions.

        Args:
            other: The other distribution.
            **kwargs: Forward compatibility placeholder.

        Returns:
            The KL-divergence between the two distributions.
        """

    @abc.abstractmethod
    def entropy(self, **kwargs) -> TensorType:
        """The entropy of the distribution.

        Args:
            **kwargs: Forward compatibility placeholder.

        Returns:
            The entropy of the distribution.
        """

    @staticmethod
    @abc.abstractmethod
    def required_input_dim(space: gym.Space, **kwargs) -> int:
        """Returns the required length of an input parameter tensor.

        Args:
            space: The space this distribution will be used for,
                whose shape attributes will be used to determine the required shape of
                the input parameter tensor.
            **kwargs: Forward compatibility placeholder.

        Returns:
            size of the required input vector (minus leading batch dimension).
        """

    @classmethod
    def from_logits(cls, logits: TensorType, **kwargs) -> "Distribution":
        """Creates a Distribution from logits.

        The caller does not need to have knowledge of the distribution class in order
        to create it and sample from it. The passed batched logits vectors might be
        split up and are passed to the distribution class' constructor as kwargs.

        Args:
            logits: The logits to create the distribution from.
            **kwargs: Forward compatibility placeholder.

        Returns:
            The created distribution.

        .. testcode::

            import numpy as np
            from ray.rllib.core.distribution.distribution import Distribution

            class Uniform(Distribution):
                def __init__(self, lower, upper):
                    self.lower = lower
                    self.upper = upper

                def sample(self):
                    return self.lower + (self.upper - self.lower) * np.random.rand()

                def logp(self, x):
                    ...

                def kl(self, other):
                    ...

                def entropy(self):
                    ...

                @staticmethod
                def required_input_dim(space):
                    ...

                def rsample(self):
                    ...

                @classmethod
                def from_logits(cls, logits, **kwargs):
                    return Uniform(logits[:, 0], logits[:, 1])

            logits = np.array([[0.0, 1.0], [2.0, 3.0]])
            my_dist = Uniform.from_logits(logits)
            sample = my_dist.sample()
        """
        raise NotImplementedError

    @classmethod
    def get_partial_dist_cls(
        parent_cls: "Distribution", **partial_kwargs
    ) -> "Distribution":
        """Returns a partial child of TorchMultiActionDistribution.

        This is useful if inputs needed to instantiate the Distribution from logits
        are available, but the logits are not.
        """

        class DistributionPartial(parent_cls):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)

            @staticmethod
            def _merge_kwargs(**kwargs):
                """Checks if keys in kwargs don't clash with partial_kwargs."""
                overlap = set(kwargs) & set(partial_kwargs)
                if overlap:
                    raise ValueError(
                        f"Cannot override the following kwargs: {overlap}.\n"
                        f"This is because they were already set at the time this "
                        f"partial class was defined."
                    )
                merged_kwargs = {**partial_kwargs, **kwargs}
                return merged_kwargs

            @classmethod
            @override(parent_cls)
            def required_input_dim(cls, space: gym.Space, **kwargs) -> int:
                merged_kwargs = cls._merge_kwargs(**kwargs)
                assert space == merged_kwargs["space"]
                return parent_cls.required_input_dim(**merged_kwargs)

            @classmethod
            @override(parent_cls)
            def from_logits(
                cls,
                logits: TensorType,
                **kwargs,
            ) -> "DistributionPartial":
                merged_kwargs = cls._merge_kwargs(**kwargs)
                distribution = parent_cls.from_logits(logits, **merged_kwargs)
                # Replace the class of the returned distribution with this partial
                # This makes it so that we can use type() on this distribution and
                # get back the partial class.
                distribution.__class__ = cls
                return distribution

        # Substitute name of this partial class to match the original class.
        DistributionPartial.__name__ = f"{parent_cls}Partial"

        return DistributionPartial

    def to_deterministic(self) -> "Distribution":
        """Returns a deterministic equivalent for this distribution.

        Specifically, the deterministic equivalent for a Categorical distribution is a
        Deterministic distribution that selects the action with maximum logit value.
        Generally, the choice of the deterministic replacement is informed by
        established conventions.
        """
        return self
