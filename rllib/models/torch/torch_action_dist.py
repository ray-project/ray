from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

try:
    import torch
except ImportError:
    pass  # soft dep

import numpy as np

from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.utils.annotations import override


class TorchDistributionWrapper(ActionDistribution):
    """Wrapper class for torch.distributions."""

    @override(ActionDistribution)
    def logp(self, actions):
        return self.dist.log_prob(actions)

    @override(ActionDistribution)
    def entropy(self):
        return self.dist.entropy()

    @override(ActionDistribution)
    def kl(self, other):
        return torch.distributions.kl.kl_divergence(self.dist, other)

    @override(ActionDistribution)
    def sample(self):
        return self.dist.sample()


class TorchCategorical(TorchDistributionWrapper):
    """Wrapper class for PyTorch Categorical distribution."""

    @override(ActionDistribution)
    def __init__(self, inputs, model_config):
        self.dist = torch.distributions.categorical.Categorical(logits=inputs)
        self.model_config = model_config

    @staticmethod
    @override(ActionDistribution)
    def required_model_output_shape(action_space, model_config):
        return action_space.n


class TorchDiagGaussian(TorchDistributionWrapper):
    """Wrapper class for PyTorch Normal distribution."""

    @override(ActionDistribution)
    def __init__(self, inputs, model_config):
        mean, log_std = torch.chunk(inputs, 2, dim=1)
        self.dist = torch.distributions.normal.Normal(mean, torch.exp(log_std))
        self.model_config = model_config

    @override(TorchDistributionWrapper)
    def logp(self, actions):
        return TorchDistributionWrapper.logp(self, actions).sum(-1)

    @staticmethod
    @override(ActionDistribution)
    def required_model_output_shape(action_space, model_config):
        return np.prod(action_space.shape) * 2
