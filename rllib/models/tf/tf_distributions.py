"""The main difference between this and the old ActionDistribution is that this one
has more explicit input args. So that the input format does not have to be guessed from
the code. This matches the design pattern of torch distribution which developers may
already be familiar with.
"""
import gymnasium as gym
import functools
from ray.rllib.utils.spaces.space_utils import get_base_struct_from_space
import tree
import numpy as np
from typing import Optional, List, Mapping, Iterable
import abc


from ray.rllib.models.distributions import Distribution
from ray.rllib.utils.annotations import override, DeveloperAPI
from ray.rllib.utils.framework import try_import_tf, try_import_tfp
from ray.rllib.utils.typing import TensorType, Union, Tuple
from ray.rllib.utils.annotations import OverrideToImplementCustomLogic


_, tf, _ = try_import_tf()
tfp = try_import_tfp()

# TODO (Kourosh) Write unittest for this class similar to torch distirbutions.


@DeveloperAPI
class TfDistribution(Distribution, abc.ABC):
    """Wrapper class for tfp.distributions."""

    def __init__(self, *args, **kwargs):
        super().__init__()
        self._dist = self._get_tf_distribution(*args, **kwargs)

    @abc.abstractmethod
    def _get_tf_distribution(self, *args, **kwargs) -> "tfp.distributions.Distribution":
        """Returns the tfp.distributions.Distribution object to use."""

    @override(Distribution)
    def logp(self, value: TensorType, **kwargs) -> TensorType:
        return self._dist.log_prob(value, **kwargs)

    @override(Distribution)
    def entropy(self) -> TensorType:
        return self._dist.entropy()

    @override(Distribution)
    def kl(self, other: "Distribution") -> TensorType:
        return self._dist.kl_divergence(other._dist)

    @override(Distribution)
    def sample(
        self, *, sample_shape=(), return_logp: bool = False
    ) -> Union[TensorType, Tuple[TensorType, TensorType]]:
        sample = self._dist.sample(sample_shape)
        if return_logp:
            return sample, self.logp(sample)
        return sample

    @override(Distribution)
    def rsample(
        self, *, sample_shape=(), return_logp: bool = False
    ) -> Union[TensorType, Tuple[TensorType, TensorType]]:

        # rsample is not implemented in tfp. So we need to make a placeholder for it.
        rsample = self._rsample(sample_shape)
        if return_logp:
            return rsample, self.logp(rsample)
        return rsample

    @OverrideToImplementCustomLogic
    def _rsample(self, sample_shape=()):
        """Returns a sample from the distribution, while maintaining the gradient."""
        raise NotImplementedError


@DeveloperAPI
class TfCategorical(TfDistribution):
    """Wrapper class for Categorical distribution.

    Creates a categorical distribution parameterized by either :attr:`probs` or
    :attr:`logits` (but not both).

    Samples are integers from :math:`\{0, \ldots, K-1\}` where `K` is
    ``probs.size(-1)``.

    If `probs` is 1-dimensional with length-`K`, each element is the relative
    probability of sampling the class at that index.

    If `probs` is N-dimensional, the first N-1 dimensions are treated as a batch of
    relative probability vectors.

    Example::
        >>> m = TfCategorical([ 0.25, 0.25, 0.25, 0.25 ])
        >>> m.sample(sample_shape=(2,))  # equal probability of 0, 1, 2, 3
        tf.Tensor([2 3], shape=(2,), dtype=int32)

    Args:
        probs: The probablities of each event.
        logits: Event log probabilities (unnormalized)
        temperature: In case of using logits, this parameter can be used to determine
            the sharpness of the distribution. i.e.
            ``probs = softmax(logits / temperature)``. The temperature must be strictly
            positive. A low value (e.g. 1e-10) will result in argmax sampling while a
            larger value will result in uniform sampling.
    """

    @override(TfDistribution)
    def __init__(
        self,
        probs: tf.Tensor = None,
        logits: tf.Tensor = None,
        temperature: float = 1.0,
    ) -> None:
        super().__init__(probs=probs, logits=logits, temperature=temperature)

    @override(TfDistribution)
    def _get_tf_distribution(
        self,
        probs: tf.Tensor = None,
        logits: tf.Tensor = None,
        temperature: float = 1.0,
    ) -> "tfp.distributions.Distribution":
        if logits is not None:
            assert temperature > 0.0, "Categorical `temperature` must be > 0.0!"
            logits /= temperature
        return tfp.distributions.Categorical(probs=probs, logits=logits)

    @staticmethod
    @override(Distribution)
    def required_input_dim(space: gym.Space, **kwargs) -> int:
        return int(space.n)

    @override(TfDistribution)
    def _rsample(self, sample_shape=()):
        # TODO (Kourosh) Implement Categorical sampling using grad-passthrough trick.
        raise NotImplementedError

    @classmethod
    @override(Distribution)
    def from_logits(
        cls, logits: TensorType, temperature: float = 1.0, **kwargs
    ) -> "TfCategorical":
        return TfCategorical(logits=logits, temperature=temperature, **kwargs)


@DeveloperAPI
class TfDiagGaussian(TfDistribution):
    """Wrapper class for Normal distribution.

    Creates a normal distribution parameterized by :attr:`loc` and :attr:`scale`. In
    case of multi-dimensional distribution, the variance is assumed to be diagonal.

    Example::

        >>> m = TfDiagGaussian(loc=[0.0, 0.0], scale=[1.0, 1.0])
        >>> m.sample(sample_shape=(2,))  # 2d normal dist with loc=0 and scale=1
        tensor([[ 0.1046, -0.6120], [ 0.234, 0.556]])

        >>> # scale is None
        >>> m = TfDiagGaussian(loc=[0.0, 1.0])
        >>> m.sample(sample_shape=(2,))  # normally distributed with loc=0 and scale=1
        tensor([0.1046, 0.6120])


    Args:
        loc: mean of the distribution (often referred to as mu). If scale is None, the
            second half of the `loc` will be used as the log of scale.
        scale: standard deviation of the distribution (often referred to as sigma).
            Has to be positive.
    """

    @override(TfDistribution)
    def __init__(
        self,
        loc: Union[float, tf.Tensor],
        scale: Optional[Union[float, tf.Tensor]] = None,
    ):
        super().__init__(loc=loc, scale=scale)

    @override(TfDistribution)
    def _get_tf_distribution(self, loc, scale=None) -> "tfp.distributions.Distribution":
        if scale is None:
            loc, log_scale = tf.split(loc, num_or_size_splits=2, axis=-1)
            scale = tf.exp(log_scale)
        return tfp.distributions.Normal(loc=loc, scale=scale)

    @override(TfDistribution)
    def logp(self, value: TensorType) -> TensorType:
        return tf.math.reduce_sum(super().logp(value), axis=-1)

    @override(TfDistribution)
    def entropy(self) -> TensorType:
        return tf.math.reduce_sum(super().entropy(), axis=-1)

    @override(TfDistribution)
    def kl(self, other: "TfDistribution") -> TensorType:
        return tf.math.reduce_sum(super().kl(other), axis=-1)

    @staticmethod
    @override(Distribution)
    def required_input_dim(space: gym.Space, **kwargs) -> int:
        return int(np.prod(space.shape, dtype=np.int32) * 2)

    @override(TfDistribution)
    def _rsample(self, sample_shape=()):
        """Implements reparameterization trick."""
        eps = tf.random.normal(sample_shape)
        return self._dist.loc + eps * self._dist.scale

    @classmethod
    @override(Distribution)
    def from_logits(cls, logits: TensorType, **kwargs) -> "TfDiagGaussian":
        loc, log_std = tf.split(logits, num_or_size_splits=2, axis=1)
        scale = tf.math.exp(log_std)
        return TfDiagGaussian(loc=loc, scale=scale)


@DeveloperAPI
class TfDeterministic(Distribution):
    """The distribution that returns the input values directly.

    This is similar to DiagGaussian with standard deviation zero (thus only
    requiring the "mean" values as NN output).

    Note: entropy is always zero, ang logp and kl are not implemented.

    Example::

        >>> m = TfDeterministic(loc=tf.constant([0.0, 0.0]))
        >>> m.sample(sample_shape=(2,))
        Tensor([[ 0.0, 0.0], [ 0.0, 0.0]])

    Args:
        loc: the determinsitic value to return
    """

    @override(Distribution)
    def __init__(self, loc: tf.Tensor) -> None:
        super().__init__()
        self.loc = loc

    @override(Distribution)
    def sample(
        self,
        *,
        sample_shape: Tuple[int, ...] = (),
        return_logp: bool = False,
        **kwargs,
    ) -> Union[TensorType, Tuple[TensorType, TensorType]]:
        if return_logp:
            raise ValueError(f"Cannot return logp for {self.__class__.__name__}.")

        shape = sample_shape + self.loc.shape
        return tf.ones(shape, dtype=self.loc.dtype) * self.loc

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
        raise tf.zeros_like(self.loc)

    @override(Distribution)
    def kl(self, other: "Distribution", **kwargs) -> TensorType:
        raise ValueError(f"Cannot return kl for {self.__class__.__name__}.")

    @staticmethod
    @override(Distribution)
    def required_input_dim(space: gym.Space, **kwargs) -> int:
        return int(np.prod(space.shape, dtype=np.int32))

    @classmethod
    @override(Distribution)
    def from_logits(cls, logits: TensorType, **kwargs) -> "TfDeterministic":
        return TfDeterministic(loc=logits)


@DeveloperAPI
class TfMultiCategorical(Distribution):
    """MultiCategorical distribution for MultiDiscrete action spaces."""

    def __init__(
        self,
        inputs: TensorType,
        input_lens: List[int],
        temperatures: List[float] = None,
    ):
        super().__init__()
        if not temperatures:
            # If temperatures are not provided, use 1.0 for all actions.
            temperatures = [1.0] * len(input_lens)

        self.cats = [
            TfCategorical(logits=logits, temperature=temperature)
            for logits, temperature in zip(
                tf.split(inputs, input_lens, axis=1), temperatures
            )
        ]

    @override(Distribution)
    def sample(self) -> TensorType:
        arr = [cat.sample() for cat in self.cats]
        sample_ = tf.stack(arr, dim=1)
        return sample_

    @override(Distribution)
    def logp(self, value: tf.Tensor) -> TensorType:
        actions = tf.unstack(tf.cast(value, tf.int32), axis=1)
        logps = tf.stack([cat.logp(act) for cat, act in zip(self.cats, actions)])
        return tf.reduce_sum(logps, axis=0)

    @override(Distribution)
    def entropy(self) -> TensorType:
        return tf.reduce_sum(
            tf.stack([cat.entropy() for cat in self.cats], axis=1), axis=1
        )

    @override(Distribution)
    def kl(self, other: Distribution) -> TensorType:
        kls = tf.stack(
            [cat.kl(oth_cat) for cat, oth_cat in zip(self.cats, other.cats)], axis=1
        )
        return tf.reduce_sum(kls, axis=1)

    @staticmethod
    @override(Distribution)
    def required_input_dim(space: gym.Space, **kwargs) -> int:
        return int(np.sum(space.nvec))

    @classmethod
    @override(Distribution)
    def from_logits(
        cls,
        inputs: TensorType,
        input_lens: List[int],
        **kwargs,
    ) -> "TfMultiCategorical":
        """Creates this Distribution from logits (and additional arguments).

        If you wish to create this distribution from logits only, please refer to
        `Distribution.get_partial_dist_cls()`.
        """
        return TfMultiCategorical(
            inputs=inputs,
            input_lens=input_lens,
        )


@DeveloperAPI
class TfMultiActionDistribution(Distribution):
    """Action distribution that operates on multiple, possibly nested actions."""

    def __init__(
        self,
        logits: tf.Tensor,
        child_distribution_cls_struct: Union[Mapping, Iterable],
        input_lens: List[int],
        space: gym.Space,
    ):
        """Initializes a TfMultiActionDistribution object.

        Args:
            logits: A single tensor of shape [BATCH, size].
            child_distribution_cls_struct: Any struct
                that contains the child distribution classes to use to
                instantiate the child distributions from `logits`. This could
                be an already flattened list or a struct according to
                `action_space`.
            input_lens (any[int]): A flat list or a nested struct of input
                split lengths used to split `logits`.
            space (Union[gym.spaces.Dict,gym.spaces.Tuple]): The complex
                and possibly nested output space.
        """
        super().__init__()
        self.space_struct = get_base_struct_from_space(space)

        self.input_lens = tree.flatten(input_lens)
        child_distribution_cls_list = tree.flatten(child_distribution_cls_struct)
        split_logits = tf.split(logits, self.input_lens, axis=1)
        self.child_distribution_list = tree.map_structure(
            lambda dist, input_: dist.from_logits(input_),
            child_distribution_cls_list,
            list(split_logits),
        )

    @override(Distribution)
    def rsample(
        self,
        *,
        sample_shape: Tuple[int, ...] = None,
        return_logp: bool = False,
        **kwargs,
    ) -> Union[TensorType, Tuple[TensorType, TensorType]]:
        rsamples = []
        logps = []
        for dist in self.flat_child_distributions:
            rsample_logp = dist.rsample(
                sample_shape=sample_shape, return_logp=return_logp, **kwargs
            )
            if return_logp:
                rsample, logp = rsample_logp
                logps.append(logp)
                rsamples.append(rsample)
            else:
                rsample = rsample_logp
                rsamples.append(rsample)

        rsamples = tree.unflatten_as(self.action_space_struct, rsamples)

        if return_logp:
            logps = tree.unflatten_as(self.action_space_struct, logps)
            return rsamples, logps
        else:
            return rsamples

    @override(Distribution)
    def logp(self, x):
        # Single tensor input (all merged).
        if isinstance(x, (tf.Tensor, np.ndarray)):
            split_indices = []
            for dist in self.flat_child_distributions:
                if isinstance(dist, TfCategorical):
                    split_indices.append(1)
                elif (
                    isinstance(dist, TfMultiCategorical)
                    and dist.action_space is not None
                ):
                    split_indices.append(np.prod(dist.action_space.shape))
                else:
                    sample = dist.sample()
                    # Cover Box(shape=()) case.
                    if len(sample.shape) == 1:
                        split_indices.append(1)
                    else:
                        split_indices.append(tf.shape(sample)[1])
            split_x = tf.split(x, split_indices, axis=1)
        # Structured or flattened (by single action component) input.
        else:
            split_x = tree.flatten(x)

        def map_(val, dist):
            # Remove extra categorical dimension.
            if isinstance(dist, TfCategorical):
                val = tf.cast(
                    tf.squeeze(val, axis=-1) if len(val.shape) > 1 else val, tf.int32
                )
            return dist.logp(val)

        # Remove extra categorical dimension and take the logp of each
        # component.
        flat_logps = tree.map_structure(map_, split_x, self.flat_child_distributions)

        return functools.reduce(lambda a, b: a + b, flat_logps)

    @override(Distribution)
    def kl(self, other):
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
        child_distributions_struct = tree.unflatten_as(
            self.space_struct, self.child_distribution_list
        )
        return tree.map_structure(lambda s: s.sample(), child_distributions_struct)

    @staticmethod
    @override(Distribution)
    def required_input_dim(space: gym.Space, input_lens: List[int], **kwargs) -> int:
        return np.sum(input_lens, dtype=np.int32)

    @classmethod
    @override(Distribution)
    def from_logits(
        cls,
        logits: TensorType,
        child_distribution_cls_struct: Union[Mapping, Iterable],
        input_lens: List[int],
        space: gym.Space,
        **kwargs,
    ) -> "TfMultiActionDistribution":
        """Creates this Distribution from logits (and additional arguments).

        If you wish to create this distribution from logits only, please refer to
        `Distribution.get_partial_dist_cls()`.
        """

        return TfMultiActionDistribution(
            logits=logits,
            child_distribution_cls_struct=child_distribution_cls_struct,
            input_lens=input_lens,
            space=space,
        )
