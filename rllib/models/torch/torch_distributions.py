"""The main difference between this and the old ActionDistribution is that this one
has more explicit input args. So that the input format does not have to be guessed from
the code. This matches the design pattern of torch distribution which developers may
already be familiar with.
"""
import gym
import numpy as np
from typing import Optional
import abc


from ray.rllib.models.distributions import Distribution
from ray.rllib.utils.annotations import override, DeveloperAPI
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import TensorType, Union, Tuple, ModelConfigDict

torch, nn = try_import_torch()


@DeveloperAPI
class TorchDistribution(Distribution, abc.ABC):
    """Wrapper class for torch.distributions."""

    def __init__(self, *args, **kwargs):
        super().__init__()
        self.dist = self._get_torch_distribution(*args, **kwargs)

    @abc.abstractmethod
    def _get_torch_distribution(
        self, *args, **kwargs
    ) -> torch.distributions.Distribution:
        """Returns the torch.distributions.Distribution object to use."""

    @override(Distribution)
    def logp(self, value: TensorType, **kwargs) -> TensorType:
        return self.dist.log_prob(value, **kwargs)

    @override(Distribution)
    def entropy(self) -> TensorType:
        return self.dist.entropy()

    @override(Distribution)
    def kl(self, other: "Distribution") -> TensorType:
        return torch.distributions.kl.kl_divergence(self.dist, other.dist)

    @override(Distribution)
    def sample(
        self, *, sample_shape=torch.Size(), return_logp: bool = False
    ) -> Union[TensorType, Tuple[TensorType, TensorType]]:
        sample = self.dist.sample(sample_shape)
        if return_logp:
            return sample, self.logp(sample)
        return sample

    @override(Distribution)
    def rsample(
        self, *, sample_shape=torch.Size(), return_logp: bool = False
    ) -> Union[TensorType, Tuple[TensorType, TensorType]]:
        rsample = self.dist.rsample(sample_shape)
        if return_logp:
            return rsample, self.logp(rsample)
        return rsample


@DeveloperAPI
class TorchCategorical(TorchDistribution):
    """Wrapper class for PyTorch Categorical distribution.

    Creates a categorical distribution parameterized by either :attr:`probs` or
    :attr:`logits` (but not both).

    Samples are integers from :math:`\{0, \ldots, K-1\}` where `K` is
    ``probs.size(-1)``.

    If `probs` is 1-dimensional with length-`K`, each element is the relative
    probability of sampling the class at that index.

    If `probs` is N-dimensional, the first N-1 dimensions are treated as a batch of
    relative probability vectors.

    Example::
        >>> m = TorchCategorical(torch.tensor([ 0.25, 0.25, 0.25, 0.25 ]))
        >>> m.sample(sample_shape=(2,))  # equal probability of 0, 1, 2, 3
        tensor([3, 4])

    Args:
        probs: The probablities of each event.
        logits: Event log probabilities (unnormalized)
        temperature: In case of using logits, this parameter can be used to determine
            the sharpness of the distribution. i.e.
            ``probs = softmax(logits / temperature)``. The temperature must be strictly
            positive. A low value (e.g. 1e-10) will result in argmax sampling while a
            larger value will result in uniform sampling.
    """

    def __init__(
        self,
        probs: torch.Tensor = None,
        logits: torch.Tensor = None,
        temperature: float = 1.0,
    ) -> None:
        super().__init__(probs=probs, logits=logits, temperature=temperature)

    @override(TorchDistribution)
    def _get_torch_distribution(
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
    @override(Distribution)
    def required_model_output_shape(
        space: gym.Space, model_config: ModelConfigDict
    ) -> Tuple[int, ...]:
        return (space.n,)


@DeveloperAPI
class TorchDiagGaussian(TorchDistribution):
    """Wrapper class for PyTorch Normal distribution.

    Creates a normal distribution parameterized by :attr:`loc` and :attr:`scale`. In
    case of multi-dimensional distribution, the variance is assumed to be diagonal.

    Example::

        >>> m = Normal(loc=torch.tensor([0.0, 0.0]), scale=torch.tensor([1.0, 1.0]))
        >>> m.sample(sample_shape=(2,))  # 2d normal dist with loc=0 and scale=1
        tensor([[ 0.1046, -0.6120], [ 0.234, 0.556]])

        >>> # scale is None
        >>> m = Normal(loc=torch.tensor([0.0, 1.0]))
        >>> m.sample(sample_shape=(2,))  # normally distributed with loc=0 and scale=1
        tensor([0.1046, 0.6120])


    Args:
        loc: mean of the distribution (often referred to as mu). If scale is None, the
            second half of the `loc` will be used as the log of scale.
        scale: standard deviation of the distribution (often referred to as sigma).
            Has to be positive.
    """

    @override(Distribution)
    def __init__(
        self,
        loc: Union[float, torch.Tensor],
        scale: Optional[Union[float, torch.Tensor]] = None,
    ):
        super().__init__(loc=loc, scale=scale)

    def _get_torch_distribution(
        self, loc, scale=None
    ) -> torch.distributions.Distribution:
        if scale is None:
            loc, log_std = torch.chunk(self.inputs, 2, dim=1)
            scale = torch.exp(log_std)
        return torch.distributions.normal.Normal(loc, scale)

    @override(TorchDistribution)
    def logp(self, value: TensorType) -> TensorType:
        return super().logp(value).sum(-1)

    @override(TorchDistribution)
    def entropy(self) -> TensorType:
        return super().entropy().sum(-1)

    @override(TorchDistribution)
    def kl(self, other: "TorchDistribution") -> TensorType:
        return super().kl(other).sum(-1)

    @staticmethod
    @override(Distribution)
    def required_model_output_shape(
        space: gym.Space, model_config: ModelConfigDict
    ) -> Tuple[int, ...]:
        return tuple(np.prod(space.shape, dtype=np.int32) * 2)


@DeveloperAPI
class TorchDeterministic(Distribution):
    """The distribution that returns the input values directly.

    This is similar to DiagGaussian with standard deviation zero (thus only
    requiring the "mean" values as NN output).

    Note: entropy is always zero, ang logp and kl are not implemented.

    Example::

        >>> m = TorchDeterministic(loc=torch.tensor([0.0, 0.0]))
        >>> m.sample(sample_shape=(2,))
        tensor([[ 0.0, 0.0], [ 0.0, 0.0]])

    Args:
        loc: the determinsitic value to return
    """

    def __init__(self, loc: torch.Tensor) -> None:
        super().__init__()
        self.loc = loc

    @override(Distribution)
    def sample(
        self,
        *,
        sample_shape: Tuple[int, ...] = None,
        return_logp: bool = False,
        **kwargs,
    ) -> Union[TensorType, Tuple[TensorType, TensorType]]:
        if return_logp:
            raise ValueError(f"Cannot return logp for {self.__class__.__name__}.")

        if sample_shape is None:
            sample_shape = torch.Size()

        device = self.loc.device
        dtype = self.loc.dtype
        shape = sample_shape + self.loc.shape
        return torch.ones(shape, device=device, dtype=dtype) * self.loc

    def rsample(
        self,
        *,
        sample_shape: Tuple[int, ...] = None,
        return_logp: bool = False,
        **kwargs,
    ) -> Union[TensorType, Tuple[TensorType, TensorType]]:
        raise NotImplementedError

    @override(Distribution)
    def logp(self, value: TensorType, **kwargs) -> TensorType:
        raise ValueError(f"Cannot return logp for {self.__class__.__name__}.")

    @override(Distribution)
    def entropy(self, **kwargs) -> TensorType:
        raise torch.zeros_like(self.loc)

    @override(Distribution)
    def kl(self, other: "Distribution", **kwargs) -> TensorType:
        raise ValueError(f"Cannot return kl for {self.__class__.__name__}.")

    @staticmethod
    @override(Distribution)
    def required_model_output_shape(
        space: gym.Space, model_config: ModelConfigDict
    ) -> Tuple[int, ...]:
        # TODO: This was copied from previous code. Is this correct? add unit test.
        return tuple(np.prod(space.shape, dtype=np.int32))
