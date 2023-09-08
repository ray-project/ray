"""The main difference between this and the old ActionDistribution is that this one
has more explicit input args. So that the input format does not have to be guessed from
the code. This matches the design pattern of torch distribution which developers may
already be familiar with.
"""
import gymnasium as gym
import tree
import numpy as np
from typing import Optional, List, Mapping, Iterable, Dict
import abc


from ray.rllib.models.distributions import Distribution
from ray.rllib.utils.annotations import override, DeveloperAPI
from ray.rllib.utils.framework import try_import_tf, try_import_tfp
from ray.rllib.utils.typing import TensorType, Union, Tuple


_, tf, _ = try_import_tf()
tfp = try_import_tfp()

# TODO (Kourosh) Write unittest for this class similar to torch distributions.


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
        self, *, sample_shape=()
    ) -> Union[TensorType, Tuple[TensorType, TensorType]]:
        sample = self._dist.sample(sample_shape)
        return sample

    @override(Distribution)
    def rsample(
        self, *, sample_shape=()
    ) -> Union[TensorType, Tuple[TensorType, TensorType]]:
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
        probs: "tf.Tensor" = None,
        logits: "tf.Tensor" = None,
    ) -> None:
        # We assert this here because to_deterministic makes this assumption.
        assert (probs is None) != (
            logits is None
        ), "Exactly one out of `probs` and `logits` must be set!"

        self.probs = probs
        self.logits = logits
        self.one_hot = tfp.distributions.OneHotCategorical(logits=logits, probs=probs)
        super().__init__(logits=logits, probs=probs)

    @override(Distribution)
    def logp(self, value: TensorType, **kwargs) -> TensorType:
        # This prevents an error in which float values at the boundaries of the range
        # of the distribution are passed to this function.
        return -tf.nn.sparse_softmax_cross_entropy_with_logits(
            logits=self.logits if self.logits is not None else tf.log(self.probs),
            labels=tf.cast(value, tf.int32),
        )

    @override(TfDistribution)
    def _get_tf_distribution(
        self,
        probs: "tf.Tensor" = None,
        logits: "tf.Tensor" = None,
    ) -> "tfp.distributions.Distribution":
        return tfp.distributions.Categorical(probs=probs, logits=logits)

    @staticmethod
    @override(Distribution)
    def required_input_dim(space: gym.Space, **kwargs) -> int:
        assert isinstance(space, gym.spaces.Discrete)
        return int(space.n)

    @override(Distribution)
    def rsample(self, sample_shape=()):
        one_hot_sample = self.one_hot.sample(sample_shape)
        return tf.stop_gradients(one_hot_sample - self.probs) + self.probs

    @classmethod
    @override(Distribution)
    def from_logits(cls, logits: TensorType, **kwargs) -> "TfCategorical":
        return TfCategorical(logits=logits, **kwargs)

    def to_deterministic(self) -> "TfDeterministic":
        if self.probs is not None:
            probs_or_logits = self.probs
        else:
            probs_or_logits = self.logits

        return TfDeterministic(loc=tf.math.argmax(probs_or_logits, axis=-1))


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
        loc: Union[float, TensorType],
        scale: Optional[Union[float, TensorType]] = None,
    ):
        self.loc = loc
        super().__init__(loc=loc, scale=scale)

    @override(TfDistribution)
    def _get_tf_distribution(self, loc, scale) -> "tfp.distributions.Distribution":
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
        assert isinstance(space, gym.spaces.Box)
        return int(np.prod(space.shape, dtype=np.int32) * 2)

    @override(Distribution)
    def rsample(self, sample_shape=()):
        eps = tf.random.normal(sample_shape)
        return self._dist.loc + eps * self._dist.scale

    @classmethod
    @override(Distribution)
    def from_logits(cls, logits: TensorType, **kwargs) -> "TfDiagGaussian":
        loc, log_std = tf.split(logits, num_or_size_splits=2, axis=-1)
        scale = tf.math.exp(log_std)
        return TfDiagGaussian(loc=loc, scale=scale)

    def to_deterministic(self) -> "TfDeterministic":
        return TfDeterministic(loc=self.loc)


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
    def __init__(self, loc: "tf.Tensor") -> None:
        super().__init__()
        self.loc = loc

    @override(Distribution)
    def sample(
        self,
        *,
        sample_shape: Tuple[int, ...] = (),
        **kwargs,
    ) -> Union[TensorType, Tuple[TensorType, TensorType]]:
        shape = sample_shape + self.loc.shape
        return tf.ones(shape, dtype=self.loc.dtype) * self.loc

    @override(Distribution)
    def rsample(
        self,
        *,
        sample_shape: Tuple[int, ...] = None,
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
        assert isinstance(space, gym.spaces.Box)
        return int(np.prod(space.shape, dtype=np.int32))

    @classmethod
    @override(Distribution)
    def from_logits(cls, logits: TensorType, **kwargs) -> "TfDeterministic":
        return TfDeterministic(loc=logits)

    def to_deterministic(self) -> "TfDeterministic":
        return self


@DeveloperAPI
class TfMultiCategorical(Distribution):
    """MultiCategorical distribution for MultiDiscrete action spaces."""

    @override(Distribution)
    def __init__(
        self,
        categoricals: List[TfCategorical],
    ):
        super().__init__()
        self._cats = categoricals

    @override(Distribution)
    def sample(self) -> TensorType:
        arr = [cat.sample() for cat in self._cats]
        sample_ = tf.stack(arr, axis=1)
        return sample_

    @override(Distribution)
    def rsample(self, sample_shape=()):
        arr = [cat.rsample() for cat in self._cats]
        sample_ = tf.stack(arr, axis=1)
        return sample_

    @override(Distribution)
    def logp(self, value: tf.Tensor) -> TensorType:
        actions = tf.unstack(tf.cast(value, tf.int32), axis=1)
        logps = tf.stack([cat.logp(act) for cat, act in zip(self._cats, actions)])
        return tf.reduce_sum(logps, axis=0)

    @override(Distribution)
    def entropy(self) -> TensorType:
        return tf.reduce_sum(
            tf.stack([cat.entropy() for cat in self._cats], axis=1), axis=1
        )

    @override(Distribution)
    def kl(self, other: Distribution) -> TensorType:
        kls = tf.stack(
            [cat.kl(oth_cat) for cat, oth_cat in zip(self._cats, other.cats)], axis=1
        )
        return tf.reduce_sum(kls, axis=1)

    @staticmethod
    @override(Distribution)
    def required_input_dim(space: gym.Space, **kwargs) -> int:
        assert isinstance(space, gym.spaces.MultiDiscrete)
        return int(np.sum(space.nvec))

    @classmethod
    @override(Distribution)
    def from_logits(
        cls,
        logits: tf.Tensor,
        input_lens: List[int],
        temperatures: List[float] = None,
        **kwargs,
    ) -> "TfMultiCategorical":
        """Creates this Distribution from logits (and additional arguments).

        If you wish to create this distribution from logits only, please refer to
        `Distribution.get_partial_dist_cls()`.

        Args:
            logits: The tensor containing logits to be separated by logit_lens.
                child_distribution_cls_struct: A struct of Distribution classes that can
                be instantiated from the given logits.
            input_lens: A list of integers that indicate the length of the logits
                vectors to be passed into each child distribution.
            temperatures: A list of floats representing the temperature to use for
                each Categorical distribution. If not provided, 1.0 is used for all.
            **kwargs: Forward compatibility kwargs.
        """
        if not temperatures:
            # If temperatures are not provided, use 1.0 for all actions.
            temperatures = [1.0] * len(input_lens)

        categoricals = [
            TfCategorical(logits=logits)
            for logits in tf.split(logits, input_lens, axis=1)
        ]

        return TfMultiCategorical(categoricals=categoricals)

    def to_deterministic(self) -> "TfMultiDistribution":
        return TfMultiDistribution([cat.to_deterministic() for cat in self._cats])


@DeveloperAPI
class TfMultiDistribution(Distribution):
    """Action distribution that operates on multiple, possibly nested actions."""

    def __init__(
        self,
        child_distribution_struct: Union[Tuple, List, Dict],
    ):
        """Initializes a TfMultiDistribution object.

        Args:
            child_distribution_struct: Any struct
                that contains the child distribution classes to use to
                instantiate the child distributions from `logits`.
        """
        super().__init__()
        self._original_struct = child_distribution_struct
        self._flat_child_distributions = tree.flatten(child_distribution_struct)

    @override(Distribution)
    def rsample(
        self,
        *,
        sample_shape: Tuple[int, ...] = None,
        **kwargs,
    ) -> Union[TensorType, Tuple[TensorType, TensorType]]:
        rsamples = []
        for dist in self._flat_child_distributions:
            rsample = dist.rsample(sample_shape=sample_shape, **kwargs)
            rsamples.append(rsample)

        rsamples = tree.unflatten_as(self._original_struct, rsamples)
        return rsamples

    @override(Distribution)
    def logp(self, value):
        # Single tensor input (all merged).
        if isinstance(value, (tf.Tensor, np.ndarray)):
            split_indices = []
            for dist in self._flat_child_distributions:
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
            split_value = tf.split(value, split_indices, axis=1)
        # Structured or flattened (by single action component) input.
        else:
            split_value = tree.flatten(value)

        def map_(val, dist):
            # Remove extra dimension if present.
            if (
                isinstance(dist, TfCategorical)
                and len(val.shape) > 1
                and val.shape[-1] == 1
            ):
                val = tf.squeeze(val, axis=-1)

            return dist.logp(val)

        # Remove extra categorical dimension and take the logp of each
        # component.
        flat_logps = tree.map_structure(
            map_, split_value, self._flat_child_distributions
        )

        return sum(flat_logps)

    @override(Distribution)
    def kl(self, other):
        kl_list = [
            d.kl(o)
            for d, o in zip(
                self._flat_child_distributions, other._flat_child_distributions
            )
        ]
        return sum(kl_list)

    @override(Distribution)
    def entropy(self):
        entropy_list = [d.entropy() for d in self._flat_child_distributions]
        return sum(entropy_list)

    @override(Distribution)
    def sample(self):
        child_distributions_struct = tree.unflatten_as(
            self._original_struct, self._flat_child_distributions
        )
        return tree.map_structure(lambda s: s.sample(), child_distributions_struct)

    @staticmethod
    @override(Distribution)
    def required_input_dim(space: gym.Space, input_lens: List[int], **kwargs) -> int:
        return sum(input_lens)

    @classmethod
    @override(Distribution)
    def from_logits(
        cls,
        logits: tf.Tensor,
        child_distribution_cls_struct: Union[Mapping, Iterable],
        input_lens: Union[Dict, List[int]],
        space: gym.Space,
        **kwargs,
    ) -> "TfMultiDistribution":
        """Creates this Distribution from logits (and additional arguments).

        If you wish to create this distribution from logits only, please refer to
        `Distribution.get_partial_dist_cls()`.

        Args:
            logits: The tensor containing logits to be separated by `input_lens`.
                child_distribution_cls_struct: A struct of Distribution classes that can
                be instantiated from the given logits.
            child_distribution_cls_struct: A struct of Distribution classes that can
                be instantiated from the given logits.
            input_lens: A list or dict of integers that indicate the length of each
                logit. If this is given as a dict, the structure should match the
                structure of child_distribution_cls_struct.
            space: The possibly nested output space.
            **kwargs: Forward compatibility kwargs.

        Returns:
            A TfMultiDistribution object.
        """
        logit_lens = tree.flatten(input_lens)
        child_distribution_cls_list = tree.flatten(child_distribution_cls_struct)
        split_logits = tf.split(logits, logit_lens, axis=1)

        child_distribution_list = tree.map_structure(
            lambda dist, input_: dist.from_logits(input_),
            child_distribution_cls_list,
            list(split_logits),
        )

        child_distribution_struct = tree.unflatten_as(
            child_distribution_cls_struct, child_distribution_list
        )

        return TfMultiDistribution(
            child_distribution_struct=child_distribution_struct,
        )

    def to_deterministic(self) -> "TfMultiDistribution":
        flat_deterministic_dists = [
            dist.to_deterministic for dist in self._flat_child_distributions
        ]
        deterministic_dists = tree.unflatten_as(
            self._original_struct, flat_deterministic_dists
        )
        return TfMultiDistribution(deterministic_dists)
