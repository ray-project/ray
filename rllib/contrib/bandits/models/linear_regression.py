import gym

from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.utils import try_import_torch
from ray.rllib.utils.annotations import override

torch, nn = try_import_torch()


class OnlineLinearRegression(nn.Module):
    def __init__(self, feature_dim, alpha=1, lambda_=1):
        super(OnlineLinearRegression, self).__init__()
        self.d = feature_dim
        self.alpha = alpha
        self.precision = nn.Parameter(
            data=lambda_ * torch.eye(self.d), requires_grad=False)
        self.f = nn.Parameter(data=torch.zeros(self.d, ), requires_grad=False)
        self.covariance = nn.Parameter(
            data=torch.inverse(self.precision), requires_grad=False)
        self.theta = nn.Parameter(
            data=self.covariance.matmul(self.f), requires_grad=False)
        self._init_params()

    def _init_params(self):
        self.update_schedule = 1
        self.delta_f = 0
        self.delta_b = 0
        self.time = 0
        self.covariance.mul_(self.alpha)
        self.dist = torch.distributions.multivariate_normal\
            .MultivariateNormal(self.theta, self.covariance)

    def partial_fit(self, x, y):
        # TODO: Handle batch of data rather than individual points
        self._check_inputs(x, y)
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

    def get_ucbs(self, x):
        """ Calculate upper confidence bounds using covariance matrix according
        to algorithm 1: LinUCB
        (http://proceedings.mlr.press/v15/chu11a/chu11a.pdf).

        Args:
            x (torch.Tensor): Input feature tensor of shape
                (batch_size, feature_dim)
        """

        projections = self.covariance @ x.T
        batch_dots = (x * projections.T).sum(dim=1)
        return batch_dots.sqrt()

    def forward(self, x, sample_theta=False):
        """ Predict scores on input batch using the underlying linear model.

        Args:
            x (torch.Tensor): Input feature tensor of shape
                (batch_size, feature_dim)
            sample_theta (bool): Whether to sample the weights from its
                posterior distribution to perform Thompson Sampling as per
                http://proceedings.mlr.press/v28/agrawal13.pdf .
        """
        self._check_inputs(x)
        theta = self.sample_theta() if sample_theta else self.theta
        scores = x @ theta
        return scores

    def _check_inputs(self, x, y=None):
        assert x.ndim in [2, 3], \
            "Input context tensor must be 2 or 3 dimensional, where the" \
            " first dimension is batch size"
        assert x.shape[1] == self.d, \
            "Feature dimensions of weights ({}) and context ({}) do not " \
            "match!".format(self.d, x.shape[1])
        if y:
            assert torch.is_tensor(y) and y.numel() == 1,\
                "Target should be a tensor;" \
                "Only online learning with a batch size of 1 is " \
                "supported for now!"


class DiscreteLinearModel(TorchModelV2, nn.Module):
    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        TorchModelV2.__init__(self, obs_space, action_space, num_outputs,
                              model_config, name)
        nn.Module.__init__(self)

        alpha = model_config.get("alpha", 1)
        lambda_ = model_config.get("lambda_", 1)
        self.feature_dim = obs_space.sample().size
        self.arms = nn.ModuleList([
            OnlineLinearRegression(
                feature_dim=self.feature_dim, alpha=alpha, lambda_=lambda_)
            for i in range(self.num_outputs)
        ])
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
            [self.arms[i](x, sample_theta) for i in range(self.num_outputs)],
            dim=-1)
        self._cur_value = scores
        if use_ucb:
            ucbs = torch.stack(
                [self.arms[i].get_ucbs(x) for i in range(self.num_outputs)],
                dim=-1)
            return scores + ucbs
        else:
            return scores

    def partial_fit(self, x, y, arm):
        assert 0 <= arm.item() < len(self.arms), \
            "Invalid arm: {}. It should be 0 <= arm < {}".format(
                arm.item(), len(self.arms))
        self.arms[arm].partial_fit(x, y)

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
            x, sample_theta=False, use_ucb=True)
        return scores, state


class DiscreteLinearModelThompsonSampling(DiscreteLinearModel):
    def forward(self, input_dict, state, seq_lens):
        x = input_dict["obs"]
        scores = super(DiscreteLinearModelThompsonSampling, self).predict(
            x, sample_theta=True, use_ucb=False)
        return scores, state


class ParametricLinearModel(TorchModelV2, nn.Module):
    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        TorchModelV2.__init__(self, obs_space, action_space, num_outputs,
                              model_config, name)
        nn.Module.__init__(self)

        alpha = model_config.get("alpha", 1)
        lambda_ = model_config.get("lambda_", 0.1)

        # RLlib preprocessors will flatten the observation space and unflatten
        # it later. Accessing the original space here.
        original_space = obs_space.original_space
        assert isinstance(original_space, gym.spaces.Dict) and \
            "item" in original_space.spaces, \
            "This model only supports gym.spaces.Dict observation spaces."
        self.feature_dim = original_space["item"].shape[-1]
        self.arm = OnlineLinearRegression(
            feature_dim=self.feature_dim, alpha=alpha, lambda_=lambda_)
        self._cur_value = None
        self._cur_ctx = None

    def _check_inputs(self, x):
        if x.ndim == 3:
            assert x.size()[
                0] == 1, "Only batch size of 1 is supported for now."

    @override(ModelV2)
    def forward(self, input_dict, state, seq_lens):
        x = input_dict["obs"]["item"]
        self._check_inputs(x)
        x.squeeze_(dim=0)  # Remove the batch dimension
        scores = self.predict(x)
        scores.unsqueeze_(dim=0)  # Add the batch dimension
        return scores, state

    def predict(self, x, sample_theta=False, use_ucb=False):
        self._cur_ctx = x
        scores = self.arm(x, sample_theta)
        self._cur_value = scores
        if use_ucb:
            ucbs = self.arm.get_ucbs(x)
            return scores + 0.3 * ucbs
        else:
            return scores

    def partial_fit(self, x, y, arm):
        x = x["item"]
        action_id = arm.item()
        self.arm.partial_fit(x[:, action_id], y)

    @override(ModelV2)
    def value_function(self):
        assert self._cur_value is not None, "must call forward() first"
        return self._cur_value

    def current_obs(self):
        assert self._cur_ctx is not None, "must call forward() first"
        return self._cur_ctx


class ParametricLinearModelUCB(ParametricLinearModel):
    def forward(self, input_dict, state, seq_lens):
        x = input_dict["obs"]["item"]
        self._check_inputs(x)
        x.squeeze_(dim=0)  # Remove the batch dimension
        scores = super(ParametricLinearModelUCB, self).predict(
            x, sample_theta=False, use_ucb=True)
        scores.unsqueeze_(dim=0)  # Add the batch dimension
        return scores, state


class ParametricLinearModelThompsonSampling(ParametricLinearModel):
    def forward(self, input_dict, state, seq_lens):
        x = input_dict["obs"]["item"]
        self._check_inputs(x)
        x.squeeze_(dim=0)  # Remove the batch dimension
        scores = super(ParametricLinearModelThompsonSampling, self).predict(
            x, sample_theta=True, use_ucb=False)
        scores.unsqueeze_(dim=0)  # Add the batch dimension
        return scores, state
