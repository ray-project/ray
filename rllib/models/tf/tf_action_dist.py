import functools
import gym
from math import log
import numpy as np
import tree  # pip install dm_tree
from typing import Optional

from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.utils import MIN_LOG_NN_OUTPUT, MAX_LOG_NN_OUTPUT, SMALL_NUMBER
from ray.rllib.utils.annotations import override, DeveloperAPI, ExperimentalAPI
from ray.rllib.utils.framework import try_import_tf, try_import_tfp
from ray.rllib.utils.spaces.space_utils import get_base_struct_from_space
from ray.rllib.utils.typing import TensorType, List, Union, Tuple, ModelConfigDict

tf1, tf, tfv = try_import_tf()
tfp = try_import_tfp()


@DeveloperAPI
class TFActionDistribution(ActionDistribution):
    """TF-specific extensions for building action distributions."""

    @override(ActionDistribution)
    def __init__(self, inputs: List[TensorType], model: ModelV2):
        super().__init__(inputs, model)
        self.sample_op = self._build_sample_op()
        self.sampled_action_logp_op = self.logp(self.sample_op)

    def _build_sample_op(self) -> TensorType:
        """Implement this instead of sample(), to enable op reuse.

        This is needed since the sample op is non-deterministic and is shared
        between sample() and sampled_action_logp().
        """
        raise NotImplementedError

    @override(ActionDistribution)
    def sample(self) -> TensorType:
        """Draw a sample from the action distribution."""
        return self.sample_op

    @override(ActionDistribution)
    def sampled_action_logp(self) -> TensorType:
        """Returns the log probability of the sampled action."""
        return self.sampled_action_logp_op


class Categorical(TFActionDistribution):
    """Categorical distribution for discrete action spaces."""

    def __init__(
        self, inputs: List[TensorType], model: ModelV2 = None, temperature: float = 1.0
    ):
        assert temperature > 0.0, "Categorical `temperature` must be > 0.0!"
        # Allow softmax formula w/ temperature != 1.0:
        # Divide inputs by temperature.
        super().__init__(inputs / temperature, model)

    @override(ActionDistribution)
    def deterministic_sample(self) -> TensorType:
        return tf.math.argmax(self.inputs, axis=1)

    @override(ActionDistribution)
    def logp(self, x: TensorType) -> TensorType:
        return -tf.nn.sparse_softmax_cross_entropy_with_logits(
            logits=self.inputs, labels=tf.cast(x, tf.int32)
        )

    @override(ActionDistribution)
    def entropy(self) -> TensorType:
        a0 = self.inputs - tf.reduce_max(self.inputs, axis=1, keepdims=True)
        ea0 = tf.exp(a0)
        z0 = tf.reduce_sum(ea0, axis=1, keepdims=True)
        p0 = ea0 / z0
        return tf.reduce_sum(p0 * (tf.math.log(z0) - a0), axis=1)

    @override(ActionDistribution)
    def kl(self, other: ActionDistribution) -> TensorType:
        a0 = self.inputs - tf.reduce_max(self.inputs, axis=1, keepdims=True)
        a1 = other.inputs - tf.reduce_max(other.inputs, axis=1, keepdims=True)
        ea0 = tf.exp(a0)
        ea1 = tf.exp(a1)
        z0 = tf.reduce_sum(ea0, axis=1, keepdims=True)
        z1 = tf.reduce_sum(ea1, axis=1, keepdims=True)
        p0 = ea0 / z0
        return tf.reduce_sum(p0 * (a0 - tf.math.log(z0) - a1 + tf.math.log(z1)), axis=1)

    @override(TFActionDistribution)
    def _build_sample_op(self) -> TensorType:
        return tf.squeeze(tf.random.categorical(self.inputs, 1), axis=1)

    @staticmethod
    @override(ActionDistribution)
    def required_model_output_shape(action_space, model_config):
        return action_space.n


@DeveloperAPI
class MultiCategorical(TFActionDistribution):
    """MultiCategorical distribution for MultiDiscrete action spaces."""

    def __init__(
        self,
        inputs: List[TensorType],
        model: ModelV2,
        input_lens: Union[List[int], np.ndarray, Tuple[int, ...]],
        action_space=None,
    ):
        # skip TFActionDistribution init
        ActionDistribution.__init__(self, inputs, model)
        self.cats = [
            Categorical(input_, model)
            for input_ in tf.split(inputs, input_lens, axis=1)
        ]
        self.action_space = action_space
        if self.action_space is None:
            self.action_space = gym.spaces.MultiDiscrete(
                [c.inputs.shape[1] for c in self.cats]
            )
        self.sample_op = self._build_sample_op()
        self.sampled_action_logp_op = self.logp(self.sample_op)

    @override(ActionDistribution)
    def deterministic_sample(self) -> TensorType:
        sample_ = tf.stack([cat.deterministic_sample() for cat in self.cats], axis=1)
        if isinstance(self.action_space, gym.spaces.Box):
            return tf.cast(
                tf.reshape(sample_, [-1] + list(self.action_space.shape)),
                self.action_space.dtype,
            )
        return sample_

    @override(ActionDistribution)
    def logp(self, actions: TensorType) -> TensorType:
        # If tensor is provided, unstack it into list.
        if isinstance(actions, tf.Tensor):
            if isinstance(self.action_space, gym.spaces.Box):
                actions = tf.reshape(
                    actions, [-1, int(np.prod(self.action_space.shape))]
                )
            elif isinstance(self.action_space, gym.spaces.MultiDiscrete):
                actions.set_shape((None, len(self.cats)))
            actions = tf.unstack(tf.cast(actions, tf.int32), axis=1)
        logps = tf.stack([cat.logp(act) for cat, act in zip(self.cats, actions)])
        return tf.reduce_sum(logps, axis=0)

    @override(ActionDistribution)
    def multi_entropy(self) -> TensorType:
        return tf.stack([cat.entropy() for cat in self.cats], axis=1)

    @override(ActionDistribution)
    def entropy(self) -> TensorType:
        return tf.reduce_sum(self.multi_entropy(), axis=1)

    @override(ActionDistribution)
    def multi_kl(self, other: ActionDistribution) -> TensorType:
        return tf.stack(
            [cat.kl(oth_cat) for cat, oth_cat in zip(self.cats, other.cats)], axis=1
        )

    @override(ActionDistribution)
    def kl(self, other: ActionDistribution) -> TensorType:
        return tf.reduce_sum(self.multi_kl(other), axis=1)

    @override(TFActionDistribution)
    def _build_sample_op(self) -> TensorType:
        sample_op = tf.stack([cat.sample() for cat in self.cats], axis=1)
        if isinstance(self.action_space, gym.spaces.Box):
            return tf.cast(
                tf.reshape(sample_op, [-1] + list(self.action_space.shape)),
                dtype=self.action_space.dtype,
            )
        return sample_op

    @staticmethod
    @override(ActionDistribution)
    def required_model_output_shape(
        action_space: gym.Space, model_config: ModelConfigDict
    ) -> Union[int, np.ndarray]:
        # Int Box.
        if isinstance(action_space, gym.spaces.Box):
            assert action_space.dtype.name.startswith("int")
            low_ = np.min(action_space.low)
            high_ = np.max(action_space.high)
            assert np.all(action_space.low == low_)
            assert np.all(action_space.high == high_)
            np.prod(action_space.shape, dtype=np.int32) * (high_ - low_ + 1)
        # MultiDiscrete space.
        else:
            # nvec is already integer, so no casting needed.
            return np.sum(action_space.nvec)


@ExperimentalAPI
class SlateMultiCategorical(Categorical):
    """MultiCategorical distribution for MultiDiscrete action spaces.

    The action space must be uniform, meaning all nvec items have the same size, e.g.
    MultiDiscrete([10, 10, 10]), where 10 is the number of candidates to pick from
    and 3 is the slate size (pick 3 out of 10). When picking candidates, no candidate
    must be picked more than once.
    """

    def __init__(
        self,
        inputs: List[TensorType],
        model: ModelV2 = None,
        temperature: float = 1.0,
        action_space: Optional[gym.spaces.MultiDiscrete] = None,
        all_slates=None,
    ):
        assert temperature > 0.0, "Categorical `temperature` must be > 0.0!"
        # Allow softmax formula w/ temperature != 1.0:
        # Divide inputs by temperature.
        super().__init__(inputs / temperature, model)
        self.action_space = action_space
        # Assert uniformness of the action space (all discrete buckets have the same
        # size).
        assert isinstance(self.action_space, gym.spaces.MultiDiscrete) and all(
            n == self.action_space.nvec[0] for n in self.action_space.nvec
        )
        self.all_slates = all_slates

    @override(ActionDistribution)
    def deterministic_sample(self) -> TensorType:
        # Get a sample from the underlying Categorical (batch of ints).
        sample = super().deterministic_sample()
        # Use the sampled ints to pick the actual slates.
        return tf.gather(self.all_slates, sample)

    @override(ActionDistribution)
    def logp(self, x: TensorType) -> TensorType:
        # TODO: Implement.
        return tf.ones_like(self.inputs[:, 0])


@DeveloperAPI
class GumbelSoftmax(TFActionDistribution):
    """GumbelSoftmax distr. (for differentiable sampling in discr. actions

    The Gumbel Softmax distribution [1] (also known as the Concrete [2]
    distribution) is a close cousin of the relaxed one-hot categorical
    distribution, whose tfp implementation we will use here plus
    adjusted `sample_...` and `log_prob` methods. See discussion at [0].

    [0] https://stackoverflow.com/questions/56226133/
    soft-actor-critic-with-discrete-action-space

    [1] Categorical Reparametrization with Gumbel-Softmax (Jang et al, 2017):
    https://arxiv.org/abs/1611.01144
    [2] The Concrete Distribution: A Continuous Relaxation of Discrete Random
    Variables (Maddison et al, 2017) https://arxiv.org/abs/1611.00712
    """

    def __init__(
        self, inputs: List[TensorType], model: ModelV2 = None, temperature: float = 1.0
    ):
        """Initializes a GumbelSoftmax distribution.

        Args:
            temperature (float): Temperature parameter. For low temperatures,
                the expected value approaches a categorical random variable.
                For high temperatures, the expected value approaches a uniform
                distribution.
        """
        assert temperature >= 0.0
        self.dist = tfp.distributions.RelaxedOneHotCategorical(
            temperature=temperature, logits=inputs
        )
        self.probs = tf.nn.softmax(self.dist._distribution.logits)
        super().__init__(inputs, model)

    @override(ActionDistribution)
    def deterministic_sample(self) -> TensorType:
        # Return the dist object's prob values.
        return self.probs

    @override(ActionDistribution)
    def logp(self, x: TensorType) -> TensorType:
        # Override since the implementation of tfp.RelaxedOneHotCategorical
        # yields positive values.
        if x.shape != self.dist.logits.shape:
            values = tf.one_hot(
                x, self.dist.logits.shape.as_list()[-1], dtype=tf.float32
            )
            assert values.shape == self.dist.logits.shape, (
                values.shape,
                self.dist.logits.shape,
            )

        # [0]'s implementation (see line below) seems to be an approximation
        # to the actual Gumbel Softmax density.
        return -tf.reduce_sum(
            -x * tf.nn.log_softmax(self.dist.logits, axis=-1), axis=-1
        )

    @override(TFActionDistribution)
    def _build_sample_op(self) -> TensorType:
        return self.dist.sample()

    @staticmethod
    @override(ActionDistribution)
    def required_model_output_shape(
        action_space: gym.Space, model_config: ModelConfigDict
    ) -> Union[int, np.ndarray]:
        return action_space.n


@DeveloperAPI
class DiagGaussian(TFActionDistribution):
    """Action distribution where each vector element is a gaussian.

    The first half of the input vector defines the gaussian means, and the
    second half the gaussian standard deviations.
    """

    def __init__(
        self,
        inputs: List[TensorType],
        model: ModelV2,
        *,
        action_space: Optional[gym.spaces.Space] = None
    ):
        mean, log_std = tf.split(inputs, 2, axis=1)
        self.mean = mean
        self.log_std = log_std
        self.std = tf.exp(log_std)
        # Remember to squeeze action samples in case action space is Box(shape)
        self.zero_action_dim = action_space and action_space.shape == ()
        super().__init__(inputs, model)

    @override(ActionDistribution)
    def deterministic_sample(self) -> TensorType:
        return self.mean

    @override(ActionDistribution)
    def logp(self, x: TensorType) -> TensorType:
        # Cover case where action space is Box(shape=()).
        if int(tf.shape(x).shape[0]) == 1:
            x = tf.expand_dims(x, axis=1)
        return (
            -0.5
            * tf.reduce_sum(
                tf.math.square((tf.cast(x, tf.float32) - self.mean) / self.std), axis=1
            )
            - 0.5 * np.log(2.0 * np.pi) * tf.cast(tf.shape(x)[1], tf.float32)
            - tf.reduce_sum(self.log_std, axis=1)
        )

    @override(ActionDistribution)
    def kl(self, other: ActionDistribution) -> TensorType:
        assert isinstance(other, DiagGaussian)
        return tf.reduce_sum(
            other.log_std
            - self.log_std
            + (tf.math.square(self.std) + tf.math.square(self.mean - other.mean))
            / (2.0 * tf.math.square(other.std))
            - 0.5,
            axis=1,
        )

    @override(ActionDistribution)
    def entropy(self) -> TensorType:
        return tf.reduce_sum(self.log_std + 0.5 * np.log(2.0 * np.pi * np.e), axis=1)

    @override(TFActionDistribution)
    def _build_sample_op(self) -> TensorType:
        sample = self.mean + self.std * tf.random.normal(tf.shape(self.mean))
        if self.zero_action_dim:
            return tf.squeeze(sample, axis=-1)
        return sample

    @staticmethod
    @override(ActionDistribution)
    def required_model_output_shape(
        action_space: gym.Space, model_config: ModelConfigDict
    ) -> Union[int, np.ndarray]:
        return np.prod(action_space.shape, dtype=np.int32) * 2


@DeveloperAPI
class SquashedGaussian(TFActionDistribution):
    """A tanh-squashed Gaussian distribution defined by: mean, std, low, high.

    The distribution will never return low or high exactly, but
    `low`+SMALL_NUMBER or `high`-SMALL_NUMBER respectively.
    """

    def __init__(
        self,
        inputs: List[TensorType],
        model: ModelV2,
        low: float = -1.0,
        high: float = 1.0,
    ):
        """Parameterizes the distribution via `inputs`.

        Args:
            low (float): The lowest possible sampling value
                (excluding this value).
            high (float): The highest possible sampling value
                (excluding this value).
        """
        assert tfp is not None
        mean, log_std = tf.split(inputs, 2, axis=-1)
        # Clip `scale` values (coming from NN) to reasonable values.
        log_std = tf.clip_by_value(log_std, MIN_LOG_NN_OUTPUT, MAX_LOG_NN_OUTPUT)
        std = tf.exp(log_std)
        self.distr = tfp.distributions.Normal(loc=mean, scale=std)
        assert np.all(np.less(low, high))
        self.low = low
        self.high = high
        super().__init__(inputs, model)

    @override(ActionDistribution)
    def deterministic_sample(self) -> TensorType:
        mean = self.distr.mean()
        return self._squash(mean)

    @override(TFActionDistribution)
    def _build_sample_op(self) -> TensorType:
        return self._squash(self.distr.sample())

    @override(ActionDistribution)
    def logp(self, x: TensorType) -> TensorType:
        # Unsquash values (from [low,high] to ]-inf,inf[)
        unsquashed_values = tf.cast(self._unsquash(x), self.inputs.dtype)
        # Get log prob of unsquashed values from our Normal.
        log_prob_gaussian = self.distr.log_prob(unsquashed_values)
        # For safety reasons, clamp somehow, only then sum up.
        log_prob_gaussian = tf.clip_by_value(log_prob_gaussian, -100, 100)
        log_prob_gaussian = tf.reduce_sum(log_prob_gaussian, axis=-1)
        # Get log-prob for squashed Gaussian.
        unsquashed_values_tanhd = tf.math.tanh(unsquashed_values)
        log_prob = log_prob_gaussian - tf.reduce_sum(
            tf.math.log(1 - unsquashed_values_tanhd ** 2 + SMALL_NUMBER), axis=-1
        )
        return log_prob

    def sample_logp(self):
        z = self.distr.sample()
        actions = self._squash(z)
        return actions, tf.reduce_sum(
            self.distr.log_prob(z) - tf.math.log(1 - actions * actions + SMALL_NUMBER),
            axis=-1,
        )

    @override(ActionDistribution)
    def entropy(self) -> TensorType:
        raise ValueError("Entropy not defined for SquashedGaussian!")

    @override(ActionDistribution)
    def kl(self, other: ActionDistribution) -> TensorType:
        raise ValueError("KL not defined for SquashedGaussian!")

    def _squash(self, raw_values: TensorType) -> TensorType:
        # Returned values are within [low, high] (including `low` and `high`).
        squashed = ((tf.math.tanh(raw_values) + 1.0) / 2.0) * (
            self.high - self.low
        ) + self.low
        return tf.clip_by_value(squashed, self.low, self.high)

    def _unsquash(self, values: TensorType) -> TensorType:
        normed_values = (values - self.low) / (self.high - self.low) * 2.0 - 1.0
        # Stabilize input to atanh.
        save_normed_values = tf.clip_by_value(
            normed_values, -1.0 + SMALL_NUMBER, 1.0 - SMALL_NUMBER
        )
        unsquashed = tf.math.atanh(save_normed_values)
        return unsquashed

    @staticmethod
    @override(ActionDistribution)
    def required_model_output_shape(
        action_space: gym.Space, model_config: ModelConfigDict
    ) -> Union[int, np.ndarray]:
        return np.prod(action_space.shape, dtype=np.int32) * 2


@DeveloperAPI
class Beta(TFActionDistribution):
    """
    A Beta distribution is defined on the interval [0, 1] and parameterized by
    shape parameters alpha and beta (also called concentration parameters).

    PDF(x; alpha, beta) = x**(alpha - 1) (1 - x)**(beta - 1) / Z
        with Z = Gamma(alpha) Gamma(beta) / Gamma(alpha + beta)
        and Gamma(n) = (n - 1)!
    """

    def __init__(
        self,
        inputs: List[TensorType],
        model: ModelV2,
        low: float = 0.0,
        high: float = 1.0,
    ):
        # Stabilize input parameters (possibly coming from a linear layer).
        inputs = tf.clip_by_value(inputs, log(SMALL_NUMBER), -log(SMALL_NUMBER))
        inputs = tf.math.log(tf.math.exp(inputs) + 1.0) + 1.0
        self.low = low
        self.high = high
        alpha, beta = tf.split(inputs, 2, axis=-1)
        # Note: concentration0==beta, concentration1=alpha (!)
        self.dist = tfp.distributions.Beta(concentration1=alpha, concentration0=beta)
        super().__init__(inputs, model)

    @override(ActionDistribution)
    def deterministic_sample(self) -> TensorType:
        mean = self.dist.mean()
        return self._squash(mean)

    @override(TFActionDistribution)
    def _build_sample_op(self) -> TensorType:
        return self._squash(self.dist.sample())

    @override(ActionDistribution)
    def logp(self, x: TensorType) -> TensorType:
        unsquashed_values = self._unsquash(x)
        return tf.math.reduce_sum(self.dist.log_prob(unsquashed_values), axis=-1)

    def _squash(self, raw_values: TensorType) -> TensorType:
        return raw_values * (self.high - self.low) + self.low

    def _unsquash(self, values: TensorType) -> TensorType:
        return (values - self.low) / (self.high - self.low)

    @staticmethod
    @override(ActionDistribution)
    def required_model_output_shape(
        action_space: gym.Space, model_config: ModelConfigDict
    ) -> Union[int, np.ndarray]:
        return np.prod(action_space.shape, dtype=np.int32) * 2


@DeveloperAPI
class Deterministic(TFActionDistribution):
    """Action distribution that returns the input values directly.

    This is similar to DiagGaussian with standard deviation zero (thus only
    requiring the "mean" values as NN output).
    """

    @override(ActionDistribution)
    def deterministic_sample(self) -> TensorType:
        return self.inputs

    @override(TFActionDistribution)
    def logp(self, x: TensorType) -> TensorType:
        return tf.zeros_like(self.inputs)

    @override(TFActionDistribution)
    def _build_sample_op(self) -> TensorType:
        return self.inputs

    @staticmethod
    @override(ActionDistribution)
    def required_model_output_shape(
        action_space: gym.Space, model_config: ModelConfigDict
    ) -> Union[int, np.ndarray]:
        return np.prod(action_space.shape, dtype=np.int32)


@DeveloperAPI
class MultiActionDistribution(TFActionDistribution):
    """Action distribution that operates on a set of actions.

    Args:
        inputs (Tensor list): A list of tensors from which to compute samples.
    """

    def __init__(
        self, inputs, model, *, child_distributions, input_lens, action_space, **kwargs
    ):
        ActionDistribution.__init__(self, inputs, model)

        self.action_space_struct = get_base_struct_from_space(action_space)

        self.input_lens = np.array(input_lens, dtype=np.int32)
        split_inputs = tf.split(inputs, self.input_lens, axis=1)
        self.flat_child_distributions = tree.map_structure(
            lambda dist, input_: dist(input_, model, **kwargs),
            child_distributions,
            split_inputs,
        )

    @override(ActionDistribution)
    def logp(self, x):
        # Single tensor input (all merged).
        if isinstance(x, (tf.Tensor, np.ndarray)):
            split_indices = []
            for dist in self.flat_child_distributions:
                if isinstance(dist, Categorical):
                    split_indices.append(1)
                elif (
                    isinstance(dist, MultiCategorical) and dist.action_space is not None
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
            if isinstance(dist, Categorical):
                val = tf.cast(
                    tf.squeeze(val, axis=-1) if len(val.shape) > 1 else val, tf.int32
                )
            return dist.logp(val)

        # Remove extra categorical dimension and take the logp of each
        # component.
        flat_logps = tree.map_structure(map_, split_x, self.flat_child_distributions)

        return functools.reduce(lambda a, b: a + b, flat_logps)

    @override(ActionDistribution)
    def kl(self, other):
        kl_list = [
            d.kl(o)
            for d, o in zip(
                self.flat_child_distributions, other.flat_child_distributions
            )
        ]
        return functools.reduce(lambda a, b: a + b, kl_list)

    @override(ActionDistribution)
    def entropy(self):
        entropy_list = [d.entropy() for d in self.flat_child_distributions]
        return functools.reduce(lambda a, b: a + b, entropy_list)

    @override(ActionDistribution)
    def sample(self):
        child_distributions = tree.unflatten_as(
            self.action_space_struct, self.flat_child_distributions
        )
        return tree.map_structure(lambda s: s.sample(), child_distributions)

    @override(ActionDistribution)
    def deterministic_sample(self):
        child_distributions = tree.unflatten_as(
            self.action_space_struct, self.flat_child_distributions
        )
        return tree.map_structure(
            lambda s: s.deterministic_sample(), child_distributions
        )

    @override(TFActionDistribution)
    def sampled_action_logp(self):
        p = self.flat_child_distributions[0].sampled_action_logp()
        for c in self.flat_child_distributions[1:]:
            p += c.sampled_action_logp()
        return p

    @override(ActionDistribution)
    def required_model_output_shape(self, action_space, model_config):
        return np.sum(self.input_lens, dtype=np.int32)


@DeveloperAPI
class Dirichlet(TFActionDistribution):
    """Dirichlet distribution for continuous actions that are between
    [0,1] and sum to 1.

    e.g. actions that represent resource allocation."""

    def __init__(self, inputs: List[TensorType], model: ModelV2):
        """Input is a tensor of logits. The exponential of logits is used to
        parametrize the Dirichlet distribution as all parameters need to be
        positive. An arbitrary small epsilon is added to the concentration
        parameters to be zero due to numerical error.

        See issue #4440 for more details.
        """
        self.epsilon = 1e-7
        concentration = tf.exp(inputs) + self.epsilon
        self.dist = tf1.distributions.Dirichlet(
            concentration=concentration,
            validate_args=True,
            allow_nan_stats=False,
        )
        super().__init__(concentration, model)

    @override(ActionDistribution)
    def deterministic_sample(self) -> TensorType:
        return tf.nn.softmax(self.dist.concentration)

    @override(ActionDistribution)
    def logp(self, x: TensorType) -> TensorType:
        # Support of Dirichlet are positive real numbers. x is already
        # an array of positive numbers, but we clip to avoid zeros due to
        # numerical errors.
        x = tf.maximum(x, self.epsilon)
        x = x / tf.reduce_sum(x, axis=-1, keepdims=True)
        return self.dist.log_prob(x)

    @override(ActionDistribution)
    def entropy(self) -> TensorType:
        return self.dist.entropy()

    @override(ActionDistribution)
    def kl(self, other: ActionDistribution) -> TensorType:
        return self.dist.kl_divergence(other.dist)

    @override(TFActionDistribution)
    def _build_sample_op(self) -> TensorType:
        return self.dist.sample()

    @staticmethod
    @override(ActionDistribution)
    def required_model_output_shape(
        action_space: gym.Space, model_config: ModelConfigDict
    ) -> Union[int, np.ndarray]:
        return np.prod(action_space.shape, dtype=np.int32)
