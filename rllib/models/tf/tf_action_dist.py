import numpy as np
import functools

from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.utils.annotations import override, DeveloperAPI
from ray.rllib.utils import try_import_tf, try_import_tfp, SMALL_NUMBER, \
    MIN_LOG_NN_OUTPUT, MAX_LOG_NN_OUTPUT
from ray.rllib.utils.tuple_actions import TupleActions

tf = try_import_tf()
tfp = try_import_tfp()


@DeveloperAPI
class TFActionDistribution(ActionDistribution):
    """TF-specific extensions for building action distributions."""

    @DeveloperAPI
    def __init__(self, inputs, model):
        super().__init__(inputs, model)
        self.sample_op = self._build_sample_op()

    @DeveloperAPI
    def _build_sample_op(self):
        """Implement this instead of sample(), to enable op reuse.

        This is needed since the sample op is non-deterministic and is shared
        between sample() and sampled_action_logp().
        """
        raise NotImplementedError

    @override(ActionDistribution)
    def sample(self):
        """Draw a sample from the action distribution."""
        return self.sample_op

    @override(ActionDistribution)
    def sampled_action_logp(self):
        """Returns the log probability of the sampled action."""
        return self.logp(self.sample_op)


class Categorical(TFActionDistribution):
    """Categorical distribution for discrete action spaces."""

    @DeveloperAPI
    def __init__(self, inputs, model=None, temperature=1.0):
        temperature = max(0.0001, temperature)  # clamp for stability reasons
        # Allow softmax formula w/ temperature != 1.0:
        # Divide inputs by temperature.
        super().__init__(inputs / temperature, model)

    @override(ActionDistribution)
    def deterministic_sample(self):
        return tf.math.argmax(self.inputs, axis=1)

    @override(ActionDistribution)
    def logp(self, x):
        return -tf.nn.sparse_softmax_cross_entropy_with_logits(
            logits=self.inputs, labels=tf.cast(x, tf.int32))

    @override(ActionDistribution)
    def entropy(self):
        a0 = self.inputs - tf.reduce_max(self.inputs, axis=1, keep_dims=True)
        ea0 = tf.exp(a0)
        z0 = tf.reduce_sum(ea0, axis=1, keep_dims=True)
        p0 = ea0 / z0
        return tf.reduce_sum(p0 * (tf.log(z0) - a0), axis=1)

    @override(ActionDistribution)
    def kl(self, other):
        a0 = self.inputs - tf.reduce_max(self.inputs, axis=1, keep_dims=True)
        a1 = other.inputs - tf.reduce_max(other.inputs, axis=1, keep_dims=True)
        ea0 = tf.exp(a0)
        ea1 = tf.exp(a1)
        z0 = tf.reduce_sum(ea0, axis=1, keep_dims=True)
        z1 = tf.reduce_sum(ea1, axis=1, keep_dims=True)
        p0 = ea0 / z0
        return tf.reduce_sum(p0 * (a0 - tf.log(z0) - a1 + tf.log(z1)), axis=1)

    @override(TFActionDistribution)
    def _build_sample_op(self):
        return tf.squeeze(tf.multinomial(self.inputs, 1), axis=1)

    @staticmethod
    @override(ActionDistribution)
    def required_model_output_shape(action_space, model_config):
        return action_space.n


class MultiCategorical(TFActionDistribution):
    """MultiCategorical distribution for MultiDiscrete action spaces."""

    def __init__(self, inputs, model, input_lens):
        # skip TFActionDistribution init
        ActionDistribution.__init__(self, inputs, model)
        self.cats = [
            Categorical(input_, model)
            for input_ in tf.split(inputs, input_lens, axis=1)
        ]
        self.sample_op = self._build_sample_op()

    @override(ActionDistribution)
    def deterministic_sample(self):
        return tf.math.argmax(self.inputs, axis=-1)

    @override(ActionDistribution)
    def logp(self, actions):
        # If tensor is provided, unstack it into list
        if isinstance(actions, tf.Tensor):
            actions = tf.unstack(tf.cast(actions, tf.int32), axis=1)
        logps = tf.stack(
            [cat.logp(act) for cat, act in zip(self.cats, actions)])
        return tf.reduce_sum(logps, axis=0)

    @override(ActionDistribution)
    def multi_entropy(self):
        return tf.stack([cat.entropy() for cat in self.cats], axis=1)

    @override(ActionDistribution)
    def entropy(self):
        return tf.reduce_sum(self.multi_entropy(), axis=1)

    @override(ActionDistribution)
    def multi_kl(self, other):
        return tf.stack(
            [cat.kl(oth_cat) for cat, oth_cat in zip(self.cats, other.cats)],
            axis=1)

    @override(ActionDistribution)
    def kl(self, other):
        return tf.reduce_sum(self.multi_kl(other), axis=1)

    @override(TFActionDistribution)
    def _build_sample_op(self):
        return tf.stack([cat.sample() for cat in self.cats], axis=1)

    @staticmethod
    @override(ActionDistribution)
    def required_model_output_shape(action_space, model_config):
        return np.sum(action_space.nvec)


class DiagGaussian(TFActionDistribution):
    """Action distribution where each vector element is a gaussian.

    The first half of the input vector defines the gaussian means, and the
    second half the gaussian standard deviations.
    """

    def __init__(self, inputs, model):
        mean, log_std = tf.split(inputs, 2, axis=1)
        self.mean = mean
        self.log_std = log_std
        self.std = tf.exp(log_std)
        super().__init__(inputs, model)

    @override(ActionDistribution)
    def deterministic_sample(self):
        return self.mean

    @override(ActionDistribution)
    def logp(self, x):
        return -0.5 * tf.reduce_sum(
            tf.square((x - self.mean) / self.std), axis=1) - \
               0.5 * np.log(2.0 * np.pi) * tf.to_float(tf.shape(x)[1]) - \
               tf.reduce_sum(self.log_std, axis=1)

    @override(ActionDistribution)
    def kl(self, other):
        assert isinstance(other, DiagGaussian)
        return tf.reduce_sum(
            other.log_std - self.log_std +
            (tf.square(self.std) + tf.square(self.mean - other.mean)) /
            (2.0 * tf.square(other.std)) - 0.5,
            axis=1)

    @override(ActionDistribution)
    def entropy(self):
        return tf.reduce_sum(
            self.log_std + .5 * np.log(2.0 * np.pi * np.e), axis=1)

    @override(TFActionDistribution)
    def _build_sample_op(self):
        return self.mean + self.std * tf.random_normal(tf.shape(self.mean))

    @staticmethod
    @override(ActionDistribution)
    def required_model_output_shape(action_space, model_config):
        return np.prod(action_space.shape) * 2


class _SquashedGaussianBase(TFActionDistribution):
    """A univariate gaussian distribution, squashed into bounded support."""

    def __init__(self, inputs, model, low=-1.0, high=1.0):
        """Parameterizes the distribution via `inputs`.

        Args:
            low (float): The lowest possible sampling value
                (excluding this value).
            high (float): The highest possible sampling value
                (excluding this value).
        """
        assert tfp is not None
        loc, log_scale = inputs[:, 0], inputs[:, 1]
        # Clip `scale` values (coming from NN) to reasonable values.
        self.log_std = tf.clip_by_value(log_scale, MIN_LOG_NN_OUTPUT,
                                        MAX_LOG_NN_OUTPUT)
        scale = tf.exp(self.log_std)
        self.distr = tfp.distributions.Normal(loc=loc, scale=scale)
        assert len(self.distr.loc.shape) == 1
        assert len(self.distr.scale.shape) == 1
        assert np.all(np.less(low, high))
        self.low = low
        self.high = high
        super().__init__(inputs, model)

    @override(ActionDistribution)
    def deterministic_sample(self):
        mean = self.distr.mean()
        assert len(mean.shape) == 1, "Shape should be batch dim only"
        s = self._squash(mean)
        assert len(s.shape) == 1
        return s[:, None]

    @override(ActionDistribution)
    def logp(self, x):
        assert len(x.shape) >= 2, "First dim batch, second dim variable"
        unsquashed_values = self._unsquash(x[:, 0])
        log_prob = self.distr.log_prob(value=unsquashed_values)
        return log_prob - self._log_squash_grad(unsquashed_values)

    @override(TFActionDistribution)
    def _build_sample_op(self):
        s = self._squash(self.distr.sample())
        assert len(s.shape) == 1
        return s[:, None]

    def _squash(self, unsquashed_values):
        """Squash an array element-wise into the (high, low) range
        
        Arguments:
            unsquashed_values: values to be squashed

        Returns:
            The squashed values.  The output shape is `unsquashed_values.shape`

        """
        raise NotImplementedError

    def _unsquash(self, values):
        """Unsquash an array element-wise from the (high, low) range
        
        Arguments:
            squashed_values: values to be unsquashed

        Returns:
            The unsquashed values.  The output shape is `squashed_values.shape`

        """
        raise NotImplementedError

    def _log_squash_grad(self, unsquashed_values):
        """Log gradient of _squash with respect to its argument.

        Arguments:
            squashed_values:  Point at which to measure the gradient.

        Returns:
            The gradient at the given point.  The output shape is
            `squashed_values.shape`.

        """
        raise NotImplementedError


class SquashedGaussian(_SquashedGaussianBase):
    """A tanh-squashed Gaussian distribution defined by: mean, std, low, high.

    The distribution will never return low or high exactly, but
    `low`+SMALL_NUMBER or `high`-SMALL_NUMBER respectively.
    """

    @override(TFActionDistribution)
    def sampled_action_logp(self):
        unsquashed_values = self._unsquash(self.sample_op)
        log_prob = tf.reduce_sum(
            self.distr.log_prob(unsquashed_values), axis=-1)
        unsquashed_values_tanhd = tf.math.tanh(unsquashed_values)
        log_prob -= tf.math.reduce_sum(
            tf.math.log(1 - unsquashed_values_tanhd**2 + SMALL_NUMBER),
            axis=-1)
        return log_prob

    def _log_squash_grad(self, unsquashed_values):
        unsquashed_values_tanhd = tf.math.tanh(unsquashed_values)
        return tf.math.log(1 - unsquashed_values_tanhd**2 + SMALL_NUMBER)

    def _squash(self, raw_values):
        # Make sure raw_values are not too high/low (such that tanh would
        # return exactly 1.0/-1.0, which would lead to +/-inf log-probs).
        return (tf.clip_by_value(
            tf.math.tanh(raw_values),
            -1.0 + SMALL_NUMBER,
            1.0 - SMALL_NUMBER) + 1.0) / 2.0 * (self.high - self.low) + \
               self.low

    def _unsquash(self, values):
        return tf.math.atanh((values - self.low) /
                             (self.high - self.low) * 2.0 - 1.0)


class GaussianSquashedGaussian(_SquashedGaussianBase):
    """A gaussian CDF-squashed Gaussian distribution.

    The distribution will never return low or high exactly, but
    `low`+SMALL_NUMBER or `high`-SMALL_NUMBER respectively.
    """
    # Chosen to match the standard logistic variance, so that:
    #   Var(N(0, 2 * _SCALE)) = Var(Logistic(0, 1))
    _SCALE = 0.5 * 1.8137

    @override(ActionDistribution)
    def kl(self, other):
        # KL(self || other) is just the KL of the two unsquashed distributions.
        assert isinstance(other, GaussianSquashedGaussian)

        mean = self.distr.loc
        std = self.distr.scale

        other_mean = other.distr.loc
        other_std = other.distr.scale

        return (other.log_std - self.log_std +
                (tf.square(std) + tf.square(mean - other_mean)) /
                (2.0 * tf.square(other_std)) - 0.5)

    def entropy(self):
        # Entropy is:
        #   -KL(self.distr || N(0, _SCALE)) + log(high - low)
        # where the latter distribution's CDF is used to do the squashing.

        mean = self.distr.loc
        std = self.distr.scale

        return (tf.log(self.high - self.low) -
                (tf.log(self._SCALE) - self.log_std +
                 (tf.square(std) + tf.square(mean)) /
                 (2.0 * tf.square(self._SCALE)) - 0.5))

    def _log_squash_grad(self, unsquashed_values):
        squash_dist = tfp.distributions.Normal(loc=0, scale=self._SCALE)
        log_grad = squash_dist.log_prob(value=unsquashed_values)
        log_grad += tf.log(self.high - self.low)
        return log_grad

    def _squash(self, raw_values):
        # Make sure raw_values are not too high/low (such that tanh would
        # return exactly 1.0/-1.0, which would lead to +/-inf log-probs).

        values = tfp.bijectors.NormalCDF().forward(raw_values / self._SCALE)
        return (tf.clip_by_value(values, SMALL_NUMBER, 1.0 - SMALL_NUMBER) *
                (self.high - self.low) + self.low)

    def _unsquash(self, values):
        return self._SCALE * tfp.bijectors.NormalCDF().inverse(
            (values - self.low) / (self.high - self.low))


class Deterministic(TFActionDistribution):
    """Action distribution that returns the input values directly.

    This is similar to DiagGaussian with standard deviation zero.
    """

    @override(ActionDistribution)
    def deterministic_sample(self):
        return self.inputs

    @override(TFActionDistribution)
    def sampled_action_logp(self):
        return 0.0

    @override(TFActionDistribution)
    def _build_sample_op(self):
        return self.inputs

    @staticmethod
    @override(ActionDistribution)
    def required_model_output_shape(action_space, model_config):
        return np.prod(action_space.shape)


class MultiActionDistribution(TFActionDistribution):
    """Action distribution that operates for list of actions.

    Args:
        inputs (Tensor list): A list of tensors from which to compute samples.
    """

    def __init__(self, inputs, model, action_space, child_distributions,
                 input_lens):
        # skip TFActionDistribution init
        ActionDistribution.__init__(self, inputs, model)
        self.input_lens = input_lens
        split_inputs = tf.split(inputs, self.input_lens, axis=1)
        child_list = []
        for i, distribution in enumerate(child_distributions):
            child_list.append(distribution(split_inputs[i], model))
        self.child_distributions = child_list

    @override(ActionDistribution)
    def logp(self, x):
        split_indices = []
        for dist in self.child_distributions:
            if isinstance(dist, Categorical):
                split_indices.append(1)
            else:
                split_indices.append(tf.shape(dist.sample())[1])
        split_list = tf.split(x, split_indices, axis=1)
        for i, distribution in enumerate(self.child_distributions):
            # Remove extra categorical dimension
            if isinstance(distribution, Categorical):
                split_list[i] = tf.cast(
                    tf.squeeze(split_list[i], axis=-1), tf.int32)
        log_list = [
            distribution.logp(split_x) for distribution, split_x in zip(
                self.child_distributions, split_list)
        ]
        return functools.reduce(lambda a, b: a + b, log_list)

    @override(ActionDistribution)
    def kl(self, other):
        kl_list = [
            distribution.kl(other_distribution)
            for distribution, other_distribution in zip(
                self.child_distributions, other.child_distributions)
        ]
        return functools.reduce(lambda a, b: a + b, kl_list)

    @override(ActionDistribution)
    def entropy(self):
        entropy_list = [s.entropy() for s in self.child_distributions]
        return functools.reduce(lambda a, b: a + b, entropy_list)

    @override(ActionDistribution)
    def sample(self):
        return TupleActions([s.sample() for s in self.child_distributions])

    @override(ActionDistribution)
    def deterministic_sample(self):
        return TupleActions(
            [s.deterministic_sample() for s in self.child_distributions])

    @override(TFActionDistribution)
    def sampled_action_logp(self):
        p = self.child_distributions[0].sampled_action_logp()
        for c in self.child_distributions[1:]:
            p += c.sampled_action_logp()
        return p


class Dirichlet(TFActionDistribution):
    """Dirichlet distribution for continuous actions that are between
    [0,1] and sum to 1.

    e.g. actions that represent resource allocation."""

    def __init__(self, inputs, model):
        """Input is a tensor of logits. The exponential of logits is used to
        parametrize the Dirichlet distribution as all parameters need to be
        positive. An arbitrary small epsilon is added to the concentration
        parameters to be zero due to numerical error.

        See issue #4440 for more details.
        """
        self.epsilon = 1e-7
        concentration = tf.exp(inputs) + self.epsilon
        self.dist = tf.distributions.Dirichlet(
            concentration=concentration,
            validate_args=True,
            allow_nan_stats=False,
        )
        super().__init__(concentration, model)

    @override(ActionDistribution)
    def logp(self, x):
        # Support of Dirichlet are positive real numbers. x is already
        # an array of positive numbers, but we clip to avoid zeros due to
        # numerical errors.
        x = tf.maximum(x, self.epsilon)
        x = x / tf.reduce_sum(x, axis=-1, keepdims=True)
        return self.dist.log_prob(x)

    @override(ActionDistribution)
    def entropy(self):
        return self.dist.entropy()

    @override(ActionDistribution)
    def kl(self, other):
        return self.dist.kl_divergence(other.dist)

    @override(TFActionDistribution)
    def _build_sample_op(self):
        return self.dist.sample()

    @staticmethod
    @override(ActionDistribution)
    def required_model_output_shape(action_space, model_config):
        return np.prod(action_space.shape)
