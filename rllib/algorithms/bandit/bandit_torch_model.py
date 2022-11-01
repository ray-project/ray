import gym

from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import TensorType

torch, nn = try_import_torch()


class OnlineLinearRegression(nn.Module):
    def __init__(self, feature_dim, alpha=1, lambda_=1):
        super(OnlineLinearRegression, self).__init__()
        self.d = feature_dim
        self.alpha = alpha
        # Diagonal matrix of size d (feature_dim).
        # If lambda=1.0, this will be an identity matrix.
        self.precision = nn.Parameter(
            data=lambda_ * torch.eye(self.d), requires_grad=False
        )
        # Inverse of the above diagnoal. If lambda=1.0, this is also an
        # identity matrix.
        self.covariance = nn.Parameter(
            data=torch.inverse(self.precision), requires_grad=False
        )
        # All-0s vector of size d (feature_dim).
        self.f = nn.Parameter(
            data=torch.zeros(
                self.d,
            ),
            requires_grad=False,
        )
        # Dot product between f and covariance matrix
        # (batch dim stays intact; reduce dim 1).
        self.theta = nn.Parameter(
            data=self.covariance.matmul(self.f), requires_grad=False
        )
        self._init_params()

    def _init_params(self):
        self.update_schedule = 1
        self.delta_f = 0
        self.delta_b = 0
        self.time = 0
        self.covariance.mul_(self.alpha)
        self.dist = torch.distributions.multivariate_normal.MultivariateNormal(
            self.theta, self.covariance
        )

    def partial_fit(self, x, y):
        x, y = self._check_inputs(x, y)
        x = x.squeeze(0)
        y = y.item()
        self.time += 1
        self.delta_f += y * x
        self.delta_b += torch.ger(x, x)
        # Can follow an update schedule if not doing sherman morison updates
        if self.time % self.update_schedule == 0:
            self.precision += self.delta_b
            self.f += self.delta_f
            self.delta_b = 0
            self.delta_f = 0
            torch.inverse(self.precision, out=self.covariance)
            torch.matmul(self.covariance, self.f, out=self.theta)
            self.covariance.mul_(self.alpha)

    def sample_theta(self):
        theta = self.dist.sample()
        return theta

    def get_ucbs(self, x: TensorType):
        """Calculate upper confidence bounds using covariance matrix according
        to algorithm 1: LinUCB
        (http://proceedings.mlr.press/v15/chu11a/chu11a.pdf).

        Args:
            x: Input feature tensor of shape
                (batch_size, [num_items]?, feature_dim)
        """
        # Fold batch and num-items dimensions into one dim.
        if len(x.shape) == 3:
            B, C, F = x.shape
            x_folded_batch = x.reshape([-1, F])
        # Only batch and feature dims.
        else:
            x_folded_batch = x

        projections = self.covariance @ x_folded_batch.T
        batch_dots = (x_folded_batch * projections.T).sum(dim=-1)
        batch_dots = batch_dots.sqrt()

        # Restore original B and C dimensions.
        if len(x.shape) == 3:
            batch_dots = batch_dots.reshape([B, C])
        return batch_dots

    def forward(self, x: TensorType, sample_theta: bool = False):
        """Predict scores on input batch using the underlying linear model.

        Args:
            x: Input feature tensor of shape (batch_size, feature_dim)
            sample_theta: Whether to sample the weights from its
                posterior distribution to perform Thompson Sampling as per
                http://proceedings.mlr.press/v28/agrawal13.pdf .
        """
        x = self._check_inputs(x)
        theta = self.sample_theta() if sample_theta else self.theta
        scores = x @ theta
        return scores

    def _check_inputs(self, x, y=None):
        assert x.ndim in [2, 3], (
            "Input context tensor must be 2 (no batch) or 3 dimensional (where the"
            " first dimension is the batch size)."
        )
        assert x.shape[-1] == self.d, (
            "Feature dimensions of weights ({}) and context ({}) do not "
            "match!".format(self.d, x.shape[-1])
        )
        if y is not None:
            assert torch.is_tensor(y), f"ERROR: Target should be a tensor, but is {y}!"
        return x if y is None else (x, y)


class DiscreteLinearModel(TorchModelV2, nn.Module):
    def __init__(self, obs_space, action_space, num_outputs, model_config, name):
        TorchModelV2.__init__(
            self, obs_space, action_space, num_outputs, model_config, name
        )
        nn.Module.__init__(self)

        alpha = model_config.get("alpha", 1)
        lambda_ = model_config.get("lambda_", 1)
        self.feature_dim = obs_space.sample().size
        self.arms = nn.ModuleList(
            [
                OnlineLinearRegression(
                    feature_dim=self.feature_dim, alpha=alpha, lambda_=lambda_
                )
                for i in range(self.num_outputs)
            ]
        )
        self._cur_value = None
        self._cur_ctx = None

    @override(ModelV2)
    def forward(self, input_dict, state, seq_lens):
        x = input_dict["obs"]
        scores = self.predict(x)
        return scores, state

    def predict(self, x, sample_theta=False, use_ucb=False):
        self._cur_ctx = x
        scores = torch.stack(
            [self.arms[i](x, sample_theta) for i in range(self.num_outputs)], dim=-1
        )
        if use_ucb:
            ucbs = torch.stack(
                [self.arms[i].get_ucbs(x) for i in range(self.num_outputs)], dim=-1
            )
            scores += ucbs
        self._cur_value = scores
        return scores

    def partial_fit(self, x, y, arms):
        for i, arm in enumerate(arms):
            assert (
                0 <= arm.item() < len(self.arms)
            ), "Invalid arm: {}. It should be 0 <= arm < {}".format(
                arm.item(), len(self.arms)
            )
            self.arms[arm].partial_fit(x[[i]], y[[i]])

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


class ParametricLinearModel(TorchModelV2, nn.Module):
    def __init__(self, obs_space, action_space, num_outputs, model_config, name):
        TorchModelV2.__init__(
            self, obs_space, action_space, num_outputs, model_config, name
        )
        nn.Module.__init__(self)

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
            x.ndim == 3
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
            ucbs = self.arm.get_ucbs(x)
            scores += 0.3 * ucbs
        self._cur_value = scores
        return scores

    def partial_fit(self, x, y, arms):
        x = x["item"]
        for i, arm in enumerate(arms):
            action_id = arm.item()
            self.arm.partial_fit(x[[i], action_id], y[[i]])

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
