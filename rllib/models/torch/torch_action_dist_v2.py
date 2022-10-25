"""The main difference between this and the old action distribution is that this one
has more explicit input args. So that the input format does not have to be guessed from
the code. This matches the design pattern of torch distribution which developers may
already be familiar with.
"""
import gym
import numpy as np
from typing import Optional
import abc


from ray.rllib.models.action_dist_v2 import ActionDistributionV2
from ray.rllib.utils.annotations import override, DeveloperAPI
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import TensorType, Union, Tuple, ModelConfigDict

torch, nn = try_import_torch()


@DeveloperAPI
class TorchDistribution(ActionDistributionV2, abc.ABC):
    """Wrapper class for torch.distributions."""

    def __init__(self, *args, **kwargs):
        super().__init__()
        self.dist = self._get_distribution(*args, **kwargs)

    @abc.abstractmethod
    def _get_distribution(self, *args, **kwargs) -> torch.distributions.Distribution:
        """Returns the torch.distributions.Distribution object to use."""

    @override(ActionDistributionV2)
    def logp(self, actions: TensorType, **kwargs) -> TensorType:
        return self.dist.log_prob(actions, **kwargs)

    @override(ActionDistributionV2)
    def entropy(self) -> TensorType:
        return self.dist.entropy()

    @override(ActionDistributionV2)
    def kl(self, other: "ActionDistributionV2") -> TensorType:
        return torch.distributions.kl.kl_divergence(self.dist, other.dist)

    @override(ActionDistributionV2)
    def sample(
        self, *, sample_shape=torch.Size(), return_logp: bool = False
    ) -> Union[TensorType, Tuple[TensorType, TensorType]]:
        sample = self.dist.sample(sample_shape)
        if return_logp:
            return sample, self.logp(sample)
        return sample

    @override(ActionDistributionV2)
    def rsample(
        self, *, sample_shape=torch.Size(), return_logp: bool = False
    ) -> Union[TensorType, Tuple[TensorType, TensorType]]:
        sample = self.dist.rsample(sample_shape)
        if return_logp:
            return sample, self.logp(sample)
        return sample


@DeveloperAPI
class TorchCategorical(TorchDistribution):
    """Wrapper class for PyTorch Categorical distribution."""

    def __init__(
        self,
        probs: torch.Tensor = None,
        logits: torch.Tensor = None,
        temperature: float = 1.0,
    ) -> None:
        super().__init__(probs=probs, logits=logits, temperature=temperature)

    @override(TorchDistribution)
    def _get_distribution(
        self,
        probs: torch.Tensor = None,
        logits: torch.Tensor = None,
        temperature: float = 1.0,
    ) -> torch.distributions.Distribution:
        if logits is not None:
            assert temperature > 0.0, "Categorical `temperature` must be > 0.0!"
            logits /= temperature
        return torch.distributions.categorical.Categorical(probs, logits)

    @staticmethod
    @override(ActionDistributionV2)
    def required_model_output_shape(
        action_space: gym.Space, model_config: ModelConfigDict
    ) -> Tuple[int, ...]:
        return (action_space.n,)


@DeveloperAPI
class TorchDiagGaussian(TorchDistribution):
    """Wrapper class for PyTorch Normal distribution."""

    @override(ActionDistributionV2)
    def __init__(
        self,
        loc: torch.Tensor,
        scale: Optional[torch.Tensor] = None,
    ):
        super().__init__(loc=loc, scale=scale)

    def _get_distribution(self, loc, scale=None) -> torch.distributions.Distribution:
        if scale is None:
            loc, log_std = torch.chunk(self.inputs, 2, dim=1)
            scale = torch.exp(log_std)
        return torch.distributions.normal.Normal(loc, scale)

    @override(TorchDistribution)
    def logp(self, actions: TensorType) -> TensorType:
        return super().logp(actions).sum(-1)

    @override(TorchDistribution)
    def entropy(self) -> TensorType:
        return super().entropy().sum(-1)

    @override(TorchDistribution)
    def kl(self, other: "TorchDistribution") -> TensorType:
        return super().kl(other).sum(-1)

    @staticmethod
    @override(ActionDistributionV2)
    def required_model_output_shape(
        action_space: gym.Space, model_config: ModelConfigDict
    ) -> Tuple[int, ...]:
        return tuple(np.prod(action_space.shape, dtype=np.int32) * 2)


@DeveloperAPI
class TorchDeterministic(ActionDistributionV2):
    """Action distribution that returns the input values directly.

    This is similar to DiagGaussian with standard deviation zero (thus only
    requiring the "mean" values as NN output).
    """

    def __init__(self, loc: torch.Tensor) -> None:
        super().__init__()
        self.loc = loc

    @override(ActionDistributionV2)
    def sample(
        self,
        *,
        sample_shape: Tuple[int, ...] = None,
        return_logp: bool = False,
        **kwargs
    ) -> Union[TensorType, Tuple[TensorType, TensorType]]:
        if return_logp:
            raise ValueError("Cannot return logp for TorchDeterministic.")

        if sample_shape is None:
            sample_shape = torch.Size()
        loc_shape = self.loc.shape
        return (
            torch.ones(
                sample_shape + loc_shape, device=self.loc.device, dtype=self.loc.dtype
            )
            * self.loc
        )

    def rsample(
        self,
        *,
        sample_shape: Tuple[int, ...] = None,
        return_logp: bool = False,
        **kwargs
    ) -> Union[TensorType, Tuple[TensorType, TensorType]]:
        raise NotImplementedError

    @override(ActionDistributionV2)
    def logp(self, action: TensorType, **kwargs) -> TensorType:
        raise NotImplementedError

    @override(ActionDistributionV2)
    def entropy(self, **kwargs) -> TensorType:
        raise torch.zeros_like(self.loc)

    @override(ActionDistributionV2)
    def kl(self, other: "ActionDistributionV2", **kwargs) -> TensorType:
        raise ValueError("Cannot return kl for TorchDeterministic.")

    @staticmethod
    @override(ActionDistributionV2)
    def required_model_output_shape(
        action_space: gym.Space, model_config: ModelConfigDict
    ) -> Tuple[int, ...]:
        # TODO: This was copied from previous code. Is this correct? add unit test.
        return tuple(np.prod(action_space.shape, dtype=np.int32))
