"""The main difference between this and the old ActionDistribution is that this one
has more explicit input args. So that the input format does not have to be guessed from
the code. This matches the design pattern of torch distribution which developers may
already be familiar with.
"""
import gym
import numpy as np
from typing import List, Optional
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
    def max_likelihood(
        self, *, sample_shape=torch.Size(), return_logp: bool = False
    ) -> Union[TensorType, Tuple[TensorType, TensorType]]:
        raise NotImplementedError()

    @override(Distribution)
    def rsample(
        self, *, sample_shape=torch.Size(), return_logp: bool = False
    ) -> Union[TensorType, Tuple[TensorType, TensorType]]:
        rsample = self.dist.rsample(sample_shape)
        if return_logp:
            return rsample, self.logp(rsample)
        return rsample

    def __repr__(self):
        return f"{self.__class__.__name__}({self.dist})"


@DeveloperAPI
class TorchMultiDistribution(TorchDistribution):
    """A distribution class that contains multiple disparate distributions.

    For example, a TorchMultiDistribution instance could contain two categorical
    distributions and a normal distribution.

    Public methods will recursively call the methods for each subdistribution, and
    where necessary, concatenate the results over the last dimension. Depending on the
    function (e.g. entropy()), the result will be reduced across the last dimension
    (e.g. sum of entropies).

    Example::
    >>> d0 = TorchCategorical(torch.tensor([ 0.5, 0.5 ]))
    >>> d1 = TorchDiagGaussian(torch.tensor([[0.5, 0.3], 0.01, 0.01]))
    >>> combined = TorchMultiDistribution([d0, d1], [(2,), (2,2)])
    >>> combined.sample().shape = (4,)
    >>> combined.entropy().shape = (1,)
    """

    def __init__(self, distributions: List[TorchDistribution]):
        self.distributions = distributions

    def _get_torch_distribution(
        self, *args, **kwargs
    ) -> List[torch.distributions.Distribution]:
        raise NotImplementedError()

    @override(Distribution)
    def logp(self, value: TensorType, **kwargs):
        logps = [d.logp(value, **kwargs) for d in self.distributions]
        return torch.cat(logps, dim=-1)

    def _check_dists(self, other: "TorchMultiDistribution") -> None:
        """Ensures that we are doing a like-for-like comparison across distributions.

        Args:
            other: the other distribution we are comparing to

        Raises:
            NotImplementedError: if the other distribution is not a
                TorchMultiDistribution
            AssertionError: if the other MultiDistribution has different
                subdistributions than our subdistributions
        """

        if not isinstance(other, TorchMultiDistribution):
            raise NotImplementedError(
                "Cannot compare MultiDistribution to non-MultiDistribution"
            )

        assert len(other) == len(
            self.distributions
        ), f"Distribution size mismatch: {self.distributions} and {other}"
        for my_dist, other_dist in zip(self.distributions, other.distributions):
            assert type(my_dist) == type(
                other_dist
            ), f"Distribution mismatch: {self.distributions} and {other}"

    @override(Distribution)
    def entropy(self) -> TensorType:
        entropies = [d.entropy() for d in self.distributions]
        # Total entropy is sum of entropies
        return torch.cat(entropies, dim=-1).sum()

    @override(Distribution)
    def kl(self, other: Distribution) -> TensorType:
        # Match each subdistribution to each other subdistribution
        self._check_dists(other)
        kls = []
        for my_dist, other_dist in zip(self.distributions, other.distributions):
            kls.append(my_dist.kl(other_dist))
        # The combined KL divergence is the sum of divergences
        return torch.cat(kls, dim=-1).sum()

    @override(Distribution)
    def sample(
        self, *, sample_shape=torch.Size(), return_logp: bool = False
    ) -> Union[TensorType, Tuple[TensorType, TensorType]]:
        samples = [
            d.sample(sample_shape=sample_shape, return_logp=return_logp)
            for d in self.distributions
        ]
        if return_logp:
            samples, logps = zip(*samples)

        samples = torch.cat(samples, dim=-1)
        if not return_logp:
            return samples

        logps = torch.cat(logps, dim=-1)
        return samples, logps

    @override(Distribution)
    def rsample(
        self, *, sample_shape=torch.Size(), return_logp: bool = False
    ) -> Union[TensorType, Tuple[TensorType, TensorType]]:
        rsamples = [d.dist.rsample(sample_shape) for d in self.distributions]
        if return_logp:
            logps = [d.logp(s) for s, d in zip(rsamples, self.distributions)]
            logps = torch.cat(logps, dim=-1)
        rsamples = torch.cat(rsamples, dim=-1)

        if return_logp:
            return rsamples, logps
        else:
            return rsamples

    @override(Distribution)
    def max_likelihood(
        self, *, sample_shape=torch.Size(), return_logp: bool = False
    ) -> Union[TensorType, Tuple[TensorType, TensorType]]:
        max_likelihoods = [
            d.dist.max_likelihood(sample_shape) for d in self.distributions
        ]
        if return_logp:
            logps = [d.logp(s) for s, d in zip(max_likelihoods, self.distributions)]
            logps = torch.cat(logps, dim=-1)
        max_likelihoods = torch.cat(max_likelihoods, dim=-1)

        if return_logp:
            return max_likelihoods, logps
        else:
            return max_likelihoods

    @staticmethod
    @override(Distribution)
    def required_model_output_shape(
        space: gym.Space, model_config: ModelConfigDict
    ) -> Tuple[int, ...]:
        # TODO: We should implement this to use DistributionConfig
        # that the user can pass to PiConfig
        raise NotImplementedError()

    def __repr__(self):
        substrs = ",".join([d.__repr__() for d in self.distributions])
        return f"{self.__class__.__name__}({substrs})"


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
            logits = logits / temperature
        return torch.distributions.Categorical(probs, logits)

    @override(TorchDistribution)
    def sample(
        self, *, sample_shape=torch.Size(), return_logp: bool = False
    ) -> Union[TensorType, Tuple[TensorType, TensorType]]:
        sample = super().sample()
        if return_logp:
            sample, logp = super().sample(
                sample_shape=sample_shape, return_logp=return_logp
            )
            # Do not collapse the batch dimension into the feature dimension
            return sample.unsqueeze(-1), logp.unsqueeze(-1)
        sample = super().sample(sample_shape=sample_shape, return_logp=return_logp)
        return sample.unsqueeze(-1)

    def max_likelihood(
        self, *, sample_shape=torch.Size(), return_logp: bool = False
    ) -> Union[TensorType, Tuple[TensorType, TensorType]]:
        max_likelihood = self.dist.mode
        if return_logp:
            return max_likelihood, self.logp(max_likelihood)
        return max_likelihood

    @staticmethod
    @override(Distribution)
    def required_model_output_shape(
        space: gym.Space, model_config: ModelConfigDict
    ) -> Tuple[int, ...]:
        # return (space.n,)
        return (gym.spaces.utils.flatdim(space),)


@DeveloperAPI
class TorchOneHotCategorical(TorchCategorical):
    @override(TorchCategorical)
    def _get_torch_distribution(
        self,
        probs: torch.Tensor = None,
        logits: torch.Tensor = None,
        temperature: float = 1.0,
    ) -> torch.distributions.Distribution:
        if logits is not None:
            assert temperature > 0.0, "Categorical `temperature` must be > 0.0!"
            logits = logits / temperature
        return torch.distributions.Categorical(probs, logits)


class TorchBernoulli(TorchDistribution):
    @override(TorchDistribution)
    def _get_torch_distribution(
        self,
        probs: torch.Tensor = None,
        logits: torch.Tensor = None,
        temperature: Union[float, TensorType] = 1.0,
    ) -> torch.distributions.Distribution:
        if logits is not None:
            assert temperature > 0.0, "Categorical `temperature` must be > 0.0!"
            logits = logits / temperature
        return torch.distributions.Bernoulli(probs, logits)

    def max_likelihood(
        self, *, sample_shape=torch.Size(), return_logp: bool = False
    ) -> Union[TensorType, Tuple[TensorType, TensorType]]:
        max_likelihood = self.dist.mode
        if return_logp:
            return max_likelihood, self.logp(max_likelihood)
        return max_likelihood

    @staticmethod
    @override(Distribution)
    def required_model_output_shape(
        space: gym.Space, model_config: ModelConfigDict
    ) -> Tuple[int, ...]:
        # return (space.n,)
        return (gym.spaces.utils.flatdim(space),)


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
            loc, log_std = torch.chunk(loc, 2, dim=-1)
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

    @override(TorchDistribution)
    def max_likelihood(
        self, *, sample_shape=torch.Size(), return_logp: bool = False
    ) -> Union[TensorType, Tuple[TensorType, TensorType]]:
        max_likelihood = self.dist.mean
        if return_logp:
            return max_likelihood, self.logp(max_likelihood)
        return max_likelihood

    @staticmethod
    @override(Distribution)
    def required_model_output_shape(
        space: gym.Space, model_config: ModelConfigDict
    ) -> Tuple[int, ...]:
        # return tuple(np.prod(space.shape, dtype=np.int32) * 2)
        return (2 * gym.spaces.utils.flatdim(space),)


class TorchSquashedDiagGaussian(TorchDiagGaussian):
    @override(Distribution)
    def __init__(
        self,
        loc: Union[float, torch.Tensor],
        scale: Optional[Union[float, torch.Tensor]] = None,
        low: Union[float, TensorType] = -1.0,
        high: Union[float, TensorType] = 1.0,
    ):
        super().__init__(loc=loc, scale=scale)
        self.support = (low, high)
        self.shift = (self.support[1] + self.support[0]) / 2.0
        self.scale = (self.support[1] - self.support[0]) / 2.0

    def logp(self, value: TensorType) -> TensorType:
        # Computing logp after sample is error-prone.
        # arctanh() or inverse sigmoid will return inf/nan for
        # sufficiently large log probs.
        # Force the users to hold onto the accurate logp via
        # calls to sample()
        raise NotImplementedError("Call sample(logp=True) instead")

    def _squash(self, x):
        return self.scale * x.tanh() + self.shift

    @override(TorchDistribution)
    def sample(
        self, *, sample_shape=torch.Size(), return_logp: bool = False
    ) -> Union[TensorType, Tuple[TensorType, TensorType]]:
        sample = self.dist.sample(sample_shape)
        if return_logp:
            # This is more accurate than calling sample and logp separately
            return self._squash(sample), self.dist.log_prob(sample)
        return self._squash(sample)

    @override(TorchDistribution)
    def max_likelihood(
        self, *, sample_shape=torch.Size(), return_logp: bool = False
    ) -> Union[TensorType, Tuple[TensorType, TensorType]]:
        max_likelihood = self.dist.mean
        if return_logp:
            return self._squash(max_likelihood), self.logp(max_likelihood)
        return max_likelihood

    @override(TorchDistribution)
    def rsample(
        self, *, sample_shape=torch.Size(), return_logp: bool = False
    ) -> Union[TensorType, Tuple[TensorType, TensorType]]:
        rsample = self.dist.rsample(sample_shape)
        if return_logp:
            return self._squash(rsample), self.logp(rsample)
        return self._squash(rsample)


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

    def max_likelihood(
        self,
        *,
        sample_shape: Tuple[int, ...] = None,
        return_logp: bool = False,
        **kwargs,
    ) -> Union[TensorType, Tuple[TensorType, TensorType]]:
        raise NotImplementedError

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
