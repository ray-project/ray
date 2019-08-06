from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.utils.annotations import DeveloperAPI


@DeveloperAPI
class ActionDistribution(object):
    """The policy action distribution of an agent.

    Args:
      inputs (Tensor): The input vector to compute samples from.
    """

    @DeveloperAPI
    def __init__(self, inputs):
        self.inputs = inputs

    @DeveloperAPI
    def sample(self):
        """Draw a sample from the action distribution."""
        raise NotImplementedError

    @DeveloperAPI
    def logp(self, x):
        """The log-likelihood of the action distribution."""
        raise NotImplementedError

    @DeveloperAPI
    def kl(self, other):
        """The KL-divergence between two action distributions."""
        raise NotImplementedError

    @DeveloperAPI
    def entropy(self):
        """The entropy of the action distribution."""
        raise NotImplementedError

    def multi_kl(self, other):
        """The KL-divergence between two action distributions.

        This differs from kl() in that it can return an array for
        MultiDiscrete. TODO(ekl) consider removing this.
        """
        return self.kl(other)

    def multi_entropy(self):
        """The entropy of the action distribution.

        This differs from entropy() in that it can return an array for
        MultiDiscrete. TODO(ekl) consider removing this.
        """
        return self.entropy()
