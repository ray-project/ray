from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.utils.annotations import DeveloperAPI


@DeveloperAPI
class ActionDistribution(object):
    """The policy action distribution of an agent.

    Args:
      inputs (Tensor): The input vector to compute samples from.
      model_config (dict): Optional model config dict
          (as defined in catalog.py)
    """

    @DeveloperAPI
    def __init__(self, inputs, model_config):
        self.inputs = inputs
        self.model_config = model_config

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

    @DeveloperAPI
    @staticmethod
    def required_model_output_shape(action_space, model_config):
        """Returns the required shape of an input parameter tensor for a
        particular action space and an optional dict of distribution-specific
        options.

        Args:
            action_space (gym.Space): The action space this distribution will
                be used for, whose shape attributes will be used to determine
                the required shape of the input parameter tensor.
            model_config (dict): Model's config dict (as defined in catalog.py)

        Returns:
            model_output_shape (int or np.ndarray of ints): size of the
                required input vector (minus leading batch dimension).
        """
        raise NotImplementedError
