import gym
import tensorflow_probability as tfp

from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf

tf1, tf, tfv = try_import_tf()


class OnlineLinearRegression(tf.Module):
    def __init__(self, feature_dim, alpha=1, lambda_=1):
        super(OnlineLinearRegression, self).__init__()

        self.d = feature_dim
        self.delta_f = tf.zeros(self.d)
        self.delta_b = tf.zeros((self.d, self.d))
        self.update_schedule = 1
        self.time = 0
        self.alpha = alpha
        self.precision = tf.Variable(
            initial_value=lambda_ * tf.eye(self.d), name="precision"
        )
        self.f = tf.Variable(initial_value=tf.zeros(self.d), name="f")
        self.covariance = tf.Variable(
            initial_value=tf.linalg.inv(self.precision), name="covariance"
        )
        self.theta = tf.Variable(
            initial_value=tf.linalg.matvec(self.covariance, self.f), name="theta"
        )

        self._init_params()

    def _init_params(self):
        self.covariance.assign(self.covariance * self.alpha)
        self.dist = tfp.distributions.MultivariateNormalTriL(
            self.theta, scale_tril=tf.linalg.cholesky(self.covariance)
        )

    def partial_fit(self, x, y):
        x, y = self._check_inputs(x, y)
        x = tf.squeeze(x, axis=0)
        y = y[0]
        self.time += 1
        self.delta_f += y * x
        self.delta_b += tf.tensordot(x, x, axes=0)
        # Can follow an update schedule if not doing sherman morison updates
        if self.time % self.update_schedule == 0:
            self.precision.assign_add(self.delta_b)
            self.f.assign_add(self.delta_f)
            self.delta_f = tf.zeros(self.d)
            self.delta_b = tf.zeros((self.d, self.d))
            self.covariance.assign(tf.linalg.inv(self.precision))
            self.theta.assign(tf.linalg.matvec(self.covariance, self.f))
            self.covariance.assign(self.covariance * self.alpha)

    def sample_theta(self):
        theta = self.dist.sample()
        return theta

    def get_ucbs(self, x: tf.Tensor):
        """Calculate upper confidence bounds using covariance matrix according
        to algorithm 1: LinUCB
        (http://proceedings.mlr.press/v15/chu11a/chu11a.pdf).

        Args:
            x: Input feature tensor of shape
                (batch_size, [num_items]?, feature_dim)
        """
        x = tf.cast(x, dtype=tf.float32)
        # Fold batch and num-items dimensions into one dim.
        if len(x.shape) == 3:
            B, C, F = x.shape
            x_folded_batch = tf.reshape(x, [-1, F])
        # Only batch and feature dims.
        else:
            x_folded_batch = x

        projections = tf.linalg.matmul(
            a=self.covariance, b=x_folded_batch, transpose_b=True
        )
        batch_dots = tf.math.reduce_sum(
            x_folded_batch * tf.transpose(projections), axis=-1
        )
        batch_dots = tf.math.sqrt(batch_dots)

        # Restore original B and C dimensions.
        if len(x.shape) == 3:
            batch_dots = tf.reshape(batch_dots, [B, C])
        return batch_dots

    def __call__(self, x: tf.Tensor, sample_theta=False):
        """Predict scores on input batch using the underlying linear model.

        Args:
            x: Input feature tensor of shape
                (batch_size, feature_dim)
            sample_theta: Whether to sample the weights from its
                posterior distribution to perform Thompson Sampling as per
                http://proceedings.mlr.press/v28/agrawal13.pdf .
        """
        x = tf.cast(x, dtype=tf.float32)
        x = self._check_inputs(x)
        theta = self.sample_theta() if sample_theta else self.theta
        scores = tf.linalg.matvec(x, theta)
        return scores

    def _check_inputs(self, x, y=None):
        assert len(x.shape) in [2, 3], (
            "Input context tensor must be 2 (no batch) or 3 dimensional (where the"
            " first dimension is the batch size)."
        )
        assert x.shape[-1] == self.d, (
            "Feature dimensions of weights ({}) and context ({}) do not "
            "match!".format(self.d, x.shape[-1])
        )
        if y is not None:
            assert tf.is_tensor(y), f"ERROR: Target should be a tensor, but is {y}!"
        return x if y is None else (x, y)


class DiscreteLinearModel(TFModelV2):
    def __init__(self, obs_space, action_space, num_outputs, model_config, name):
        TFModelV2.__init__(
            self, obs_space, action_space, num_outputs, model_config, name
        )

        alpha = model_config.get("alpha", 1)
        lambda_ = model_config.get("lambda_", 1)
        self.feature_dim = obs_space.sample().size
        self.arms = [
            OnlineLinearRegression(
                feature_dim=self.feature_dim, alpha=alpha, lambda_=lambda_
            )
            for i in range(self.num_outputs)
        ]
        self._cur_value = None
        self._cur_ctx = None

    @override(ModelV2)
    def forward(self, input_dict, state, seq_lens):
        x = input_dict["obs"]
        scores = self.predict(x)
        return scores, state

    def predict(self, x, sample_theta=False, use_ucb=False):
        self._cur_ctx = x
        scores = tf.stack(
            [self.arms[i](x, sample_theta) for i in range(self.num_outputs)], axis=-1
        )
        if use_ucb:
            ucbs = tf.stack(
                [self.arms[i].get_ucbs(x) for i in range(self.num_outputs)], axis=-1
            )
            scores += scores + ucbs
        self._cur_value = scores
        return scores

    def partial_fit(self, x, y, arms):
        for i, arm in enumerate(arms):
            assert (
                0 <= arm < len(self.arms)
            ), "Invalid arm: {}. It should be 0 <= arm < {}".format(arm, len(self.arms))
            xi = tf.expand_dims(x[i], axis=0)
            yi = tf.expand_dims(y[i], axis=0)
            self.arms[arm].partial_fit(xi, yi)

    @override(ModelV2)
    def value_function(self):
        assert self._cur_value is not None, "must call forward() first"
        return self._cur_value

    def current_obs(self):
        assert self._cur_ctx is not None, "must call forward() first"
        return self._cur_ctx


class DiscreteLinearModelUCB(DiscreteLinearModel):
    def forward(self, input_dict, state, seq_lens):
        x = input_dict["obs"]
        scores = super(DiscreteLinearModelUCB, self).predict(
            x, sample_theta=False, use_ucb=True
        )
        return scores, state


class DiscreteLinearModelThompsonSampling(DiscreteLinearModel):
    def forward(self, input_dict, state, seq_lens):
        x = input_dict["obs"]
        scores = super(DiscreteLinearModelThompsonSampling, self).predict(
            x, sample_theta=True, use_ucb=False
        )
        return scores, state


class ParametricLinearModel(TFModelV2):
    def __init__(self, obs_space, action_space, num_outputs, model_config, name):
        TFModelV2.__init__(
            self, obs_space, action_space, num_outputs, model_config, name
        )

        alpha = model_config.get("alpha", 1)
        lambda_ = model_config.get("lambda_", 0.1)

        # RLlib preprocessors will flatten the observation space and unflatten
        # it later. Accessing the original space here.
        original_space = obs_space.original_space
        assert (
            isinstance(original_space, gym.spaces.Dict)
            and "item" in original_space.spaces
        ), "This model only supports gym.spaces.Dict observation spaces."
        self.feature_dim = original_space["item"].shape[-1]
        self.arm = OnlineLinearRegression(
            feature_dim=self.feature_dim, alpha=alpha, lambda_=lambda_
        )
        self._cur_value = None
        self._cur_ctx = None

    def _check_inputs(self, x):
        assert (
            len(x.shape) == 3
        ), f"ERROR: Inputs ({x}) must have 3 dimensions (B x num-items x features)."
        return x

    @override(ModelV2)
    def forward(self, input_dict, state, seq_lens):
        x = input_dict["obs"]["item"]
        x = self._check_inputs(x)
        scores = self.predict(x)
        return scores, state

    def predict(self, x, sample_theta=False, use_ucb=False):
        self._cur_ctx = x
        scores = self.arm(x, sample_theta)
        if use_ucb:
            scores += 0.3 * self.arm.get_ucbs(x)
        self._cur_value = scores
        return scores

    def partial_fit(self, x, y, arms):
        x = x["item"]
        for i, arm in enumerate(arms):
            xi = tf.expand_dims(x[i, arm], axis=0)
            yi = tf.expand_dims(y[i], axis=0)
            self.arm.partial_fit(xi, yi)

    @override(ModelV2)
    def value_function(self):
        assert self._cur_value is not None, "Must call `forward()` first."
        return self._cur_value

    def current_obs(self):
        assert self._cur_ctx is not None, "Must call `forward()` first."
        return self._cur_ctx


class ParametricLinearModelUCB(ParametricLinearModel):
    def forward(self, input_dict, state, seq_lens):
        x = input_dict["obs"]["item"]
        x = self._check_inputs(x)
        scores = super().predict(x, sample_theta=False, use_ucb=True)
        return scores, state


class ParametricLinearModelThompsonSampling(ParametricLinearModel):
    def forward(self, input_dict, state, seq_lens):
        x = input_dict["obs"]["item"]
        x = self._check_inputs(x)
        scores = super().predict(x, sample_theta=True, use_ucb=False)
        return scores, state
