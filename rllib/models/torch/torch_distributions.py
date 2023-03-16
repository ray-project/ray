"""The main difference between this and the old ActionDistribution is that this one
has more explicit input args. So that the input format does not have to be guessed from
the code. This matches the design pattern of torch distribution which developers may
already be familiar with.
"""
import gymnasium as gym
import numpy as np
from typing import Optional, List, Mapping, Iterable
import tree
import functools
import abc


from ray.rllib.models.distributions import Distribution
from ray.rllib.utils.annotations import override, DeveloperAPI
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import TensorType, Union, Tuple, ModelConfigDict
from ray.rllib.utils.spaces.space_utils import get_base_struct_from_space
from ray.rllib.utils.spaces.space_utils import flatten_space

torch, nn = try_import_torch()


@DeveloperAPI
class TorchDistribution(Distribution, abc.ABC):
    """Wrapper class for torch.distributions."""

    def __init__(self, *args, **kwargs):
        super().__init__()
        self._dist = self._get_torch_distribution(*args, **kwargs)

    @abc.abstractmethod
    def _get_torch_distribution(
        self, *args, **kwargs
    ) -> "torch.distributions.Distribution":
        """Returns the torch.distributions.Distribution object to use."""

    @override(Distribution)
    def logp(self, value: TensorType, **kwargs) -> TensorType:
        return self._dist.log_prob(value, **kwargs)

    @override(Distribution)
    def entropy(self) -> TensorType:
        return self._dist.entropy()

    @override(Distribution)
    def kl(self, other: "Distribution") -> TensorType:
        return torch.distributions.kl.kl_divergence(self._dist, other._dist)

    @override(Distribution)
    def sample(
        self, *, sample_shape=torch.Size(), return_logp: bool = False
    ) -> Union[TensorType, Tuple[TensorType, TensorType]]:
        sample = self._dist.sample(sample_shape)
        if return_logp:
            return sample, self.logp(sample)
        return sample

    @override(Distribution)
    def rsample(
        self, *, sample_shape=torch.Size(), return_logp: bool = False
    ) -> Union[TensorType, Tuple[TensorType, TensorType]]:
        rsample = self._dist.rsample(sample_shape)
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

    @override(TorchDistribution)
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
    ) -> "torch.distributions.Distribution":
        if logits is not None:
            assert temperature > 0.0, "Categorical `temperature` must be > 0.0!"
            logits /= temperature
        return torch.distributions.categorical.Categorical(probs, logits)

    @staticmethod
    @override(Distribution)
    def required_model_output_shape(space: gym.Space) -> Tuple[int, ...]:
        return (int(space.n),)

    @classmethod
    @override(Distribution)
    def from_logits(
        cls, logits: TensorType, temperature: float = 1.0, **kwargs
    ) -> "TorchCategorical":
        return TorchCategorical(logits=logits, temperature=temperature, **kwargs)


@DeveloperAPI
class TorchDiagGaussian(TorchDistribution):
    """Wrapper class for PyTorch Normal distribution.

    Creates a normal distribution parameterized by :attr:`loc` and :attr:`scale`. In
    case of multi-dimensional distribution, the variance is assumed to be diagonal.

    Example::
        >>> loc, scale = torch.tensor([0.0, 0.0]), torch.tensor([1.0, 1.0])
        >>> m = TorchDiagGaussian(loc=loc, scale=scale)
        >>> m.sample(sample_shape=(2,))  # 2d normal dist with loc=0 and scale=1
        tensor([[ 0.1046, -0.6120], [ 0.234, 0.556]])

        >>> # scale is None
        >>> m = TorchDiagGaussian(loc=torch.tensor([0.0, 1.0]))
        >>> m.sample(sample_shape=(2,))  # normally distributed with loc=0 and scale=1
        tensor([0.1046, 0.6120])


    Args:
        loc: mean of the distribution (often referred to as mu). If scale is None, the
            second half of the `loc` will be used as the log of scale.
        scale: standard deviation of the distribution (often referred to as sigma).
            Has to be positive.
    """

    @override(TorchDistribution)
    def __init__(
        self,
        loc: Union[float, torch.Tensor],
        scale: Optional[Union[float, torch.Tensor]] = None,
    ):
        super().__init__(loc=loc, scale=scale)

    def _get_torch_distribution(
        self, loc, scale=None
    ) -> "torch.distributions.Distribution":
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
    def required_model_output_shape(space: gym.Space) -> Tuple[int, ...]:
        return (int(np.prod(space.shape, dtype=np.int32) * 2),)

    @classmethod
    @override(Distribution)
    def from_logits(cls, logits: TensorType, **kwargs) -> "TorchDiagGaussian":
        loc, log_std = logits.chunk(2, dim=-1)
        scale = log_std.exp()
        return TorchDiagGaussian(loc=loc, scale=scale)


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

    @override(Distribution)
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
    def required_model_output_shape(space: gym.Space) -> Tuple[int, ...]:
        # TODO: This was copied from previous code. Is this correct? add unit test.
        return (int(np.prod(space.shape, dtype=np.int32)),)

    @classmethod
    @override(Distribution)
    def from_logits(cls, logits: TensorType, **kwargs) -> "TorchDeterministic":
        return TorchDeterministic(loc=logits)


@DeveloperAPI
class TorchMultiCategorical(Distribution):
    """MultiCategorical distribution for MultiDiscrete action spaces."""

    @override(Distribution)
    def __init__(
        self,
        inputs: TensorType,
        input_lens: Union[List[int], np.ndarray, Tuple[int, ...]],
    ):
        super().__init__()
        # If input_lens is np.ndarray or list, force-make it a tuple.
        inputs_split = inputs.split(tuple(input_lens), dim=1)
        self.cats = [
            torch.distributions.categorical.Categorical(logits=input_)
            for input_ in inputs_split
        ]

    @override(Distribution)
    def sample(self) -> TensorType:
        arr = [cat.sample() for cat in self.cats]
        sample_ = torch.stack(arr, dim=1)
        if isinstance(self.action_space, gym.spaces.Box):
            sample_ = torch.reshape(sample_, [-1] + list(self.action_space.shape))
        return sample_

    @override(Distribution)
    def deterministic_sample(self) -> TensorType:
        arr = [torch.argmax(cat.probs, -1) for cat in self.cats]
        sample_ = torch.stack(arr, dim=1)
        if isinstance(self.action_space, gym.spaces.Box):
            sample_ = torch.reshape(sample_, [-1] + list(self.action_space.shape))
        return sample_

    @override(Distribution)
    def logp(self, value: torch.Tensor) -> TensorType:
        value = torch.unbind(value, dim=1)
        logps = torch.stack([cat.log_prob(act) for cat, act in zip(self.cats, value)])
        return torch.sum(logps, dim=0)

    @override(Distribution)
    def multi_entropy(self) -> TensorType:
        return torch.stack([cat.entropy() for cat in self.cats], dim=1)

    @override(Distribution)
    def entropy(self) -> TensorType:
        return torch.sum(self.multi_entropy(), dim=1)

    @override(Distribution)
    def multi_kl(self, other: Distribution) -> TensorType:
        return torch.stack(
            [
                torch.distributions.kl.kl_divergence(cat, oth_cat)
                for cat, oth_cat in zip(self.cats, other.cats)
            ],
            dim=1,
        )

    @override(Distribution)
    def kl(self, other: Distribution) -> TensorType:
        return torch.sum(self.multi_kl(other), dim=1)

    @staticmethod
    @override(Distribution)
    def required_model_output_shape(space: gym.Space) -> Tuple[int, ...]:
        return (int(np.sum(space.nvec)),)

    @classmethod
    @override(Distribution)
    def from_logits(
        cls,
        inputs: TensorType,
        input_lens: List[int],
        **kwargs,
    ) -> "TorchMultiCategorical":
        return TorchMultiCategorical(
            inputs=inputs,
            input_lens=input_lens,
        )

    @staticmethod
    def get_partial_dist_cls(input_lens: List[int]):
        """Returns a partial child of TorchMultiCategorical.

        This is useful if the input lengths are already known, but the logits are not
        yet available.
        """

        class TorchMultiCategoricalPartial(TorchMultiCategorical):
            def __init__(self, inputs: torch.Tensor):
                super().__init__(
                    inputs=inputs,
                    input_lens=input_lens,
                )

            def from_logits(
                cls,
                logits: TensorType,
                **kwargs,
            ) -> "TorchMultiCategoricalPartial":
                return TorchMultiCategoricalPartial(logits)

        return TorchMultiCategoricalPartial


@DeveloperAPI
class TorchMultiActionDistribution(Distribution):
    """Action distribution that operates on multiple, possibly nested actions."""

    def __init__(
        self,
        inputs: torch.Tensor,
        child_distribution_cls_struct: Union[Mapping, Iterable],
        input_lens: List[int],
        space: gym.Space,
    ):
        """Initializes a TorchMultiActionDistribution object.

        Args:
            inputs: A single tensor of shape [BATCH, size].
            child_distribution_cls_struct: Any struct
                that contains the child distribution classes to use to
                instantiate the child distributions from `inputs`. This could
                be an already flattened list or a struct according to
                `action_space`.
            input_lens (any[int]): A flat list or a nested struct of input
                split lengths used to split `inputs`.
            space (Union[gym.spaces.Dict,gym.spaces.Tuple]): The complex
                and possibly nested output space.
        """
        super().__init__()
        self.space_struct = get_base_struct_from_space(space)

        self.input_lens = tree.flatten(input_lens)
        child_distribution_cls_list = tree.flatten(child_distribution_cls_struct)
        split_inputs = torch.split(inputs, self.input_lens, dim=1)
        self.child_distribution_list = tree.map_structure(
            lambda dist, input_: dist.from_logits(input_),
            child_distribution_cls_list,
            list(split_inputs),
        )

    @override(Distribution)
    def logp(self, value: TensorType) -> TensorType:
        # Single tensor input (all merged).
        split_indices = []
        for dist in self.flat_child_distributions:
            if isinstance(dist, TorchCategorical):
                split_indices.append(1)
            elif isinstance(dist, MultiCategorical) and dist.action_space is not None:
                split_indices.append(int(np.prod(dist.action_space.shape)))
            else:
                sample = dist.sample()
                # Cover Box(shape=()) case.
                if len(sample.shape) == 1:
                    split_indices.append(1)
                else:
                    split_indices.append(sample.size()[1])
        split_x = list(torch.split(value, split_indices, dim=1))

        def map_(val, dist):
            # Remove extra categorical dimension.
            if isinstance(dist, TorchCategorical):
                val = (torch.squeeze(val, dim=-1) if len(val.shape) > 1 else val).int()
            return dist.logp(val)

        # Remove extra categorical dimension and take the logp of each
        # component.
        flat_logps = tree.map_structure(map_, split_x, self.flat_child_distributions)

        return functools.reduce(lambda a, b: a + b, flat_logps)

    @override(Distribution)
    def kl(self, other: Distribution) -> TensorType:
        kl_list = [
            d.kl(o)
            for d, o in zip(
                self.flat_child_distributions, other.flat_child_distributions
            )
        ]
        return functools.reduce(lambda a, b: a + b, kl_list)

    @override(Distribution)
    def entropy(self):
        entropy_list = [d.entropy() for d in self.flat_child_distributions]
        return functools.reduce(lambda a, b: a + b, entropy_list)

    @override(Distribution)
    def sample(self):
        child_distributions = tree.unflatten_as(
            self.space_struct, self.flat_child_distributions
        )
        return tree.map_structure(lambda s: s.sample(), child_distributions)

    @override(Distribution)
    def deterministic_sample(self):
        child_distributions = tree.unflatten_as(
            self.space_struct, self.flat_child_distributions
        )
        return tree.map_structure(
            lambda s: s.deterministic_sample(), child_distributions
        )

    @override(Distribution)
    def sampled_action_logp(self):
        p = self.flat_child_distributions[0].sampled_action_logp()
        for c in self.flat_child_distributions[1:]:
            p += c.sampled_action_logp()
        return p

    @staticmethod
    @override(Distribution)
    def required_model_output_shape(self, space: gym.Space) -> Tuple[int, ...]:
        return (np.sum(self.input_lens, dtype=np.int32),)

    @classmethod
    @override(Distribution)
    def from_logits(
        cls,
        logits: TensorType,
        child_distribution_cls_struct: Union[Mapping, Iterable],
        input_lens: List[int],
        space: gym.Space,
        **kwargs,
    ) -> "TorchMultiActionDistribution":
        return TorchMultiActionDistribution(
            logits=logits,
            child_distribution_cls_struct=child_distribution_cls_struct,
            input_lens=input_lens,
            space=space,
        )

    @staticmethod
    def get_partial_dist_cls(
        space: gym.Space,
        child_distribution_cls_struct: Union[Mapping, Iterable],
        input_lens: List[int],
    ):
        """Returns a partial child of TorchMultiActionDistribution.

        This is useful if the space, child distribution classes and input lengths are
        already known, but the logits are not yet available.
        """

        class TorchMultiActionDistributionPartial(TorchMultiActionDistribution):
            def __init__(self, inputs: torch.Tensor):
                super().__init__(
                    inputs=inputs,
                    child_distribution_cls_struct=child_distribution_cls_struct,
                    input_lens=input_lens,
                    space=space,
                )

            def from_logits(
                cls,
                logits: TensorType,
                **kwargs,
            ) -> "TorchMultiActionDistributionPartial":
                return TorchMultiActionDistributionPartial(logits)

        return TorchMultiActionDistributionPartial
