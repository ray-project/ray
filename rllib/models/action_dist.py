import numpy as np
import gym

from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.utils.typing import TensorType, List, Union, ModelConfigDict


@DeveloperAPI
class ActionDistribution:
    """The policy action distribution of an agent.

    Attributes:
        inputs: input vector to compute samples from.
        model (ModelV2): reference to model producing the inputs.
    """

    @DeveloperAPI
    def __init__(self, inputs: List[TensorType], model: ModelV2):
        """Initializes an ActionDist object.

        Args:
            inputs: input vector to compute samples from.
            model (ModelV2): reference to model producing the inputs. This
                is mainly useful if you want to use model variables to compute
                action outputs (i.e., for auto-regressive action distributions,
                see examples/autoregressive_action_dist.py).
        """
        self.inputs = inputs
        self.model = model

    @DeveloperAPI
    def sample(self) -> TensorType:
        """Draw a sample from the action distribution."""
        raise NotImplementedError

    @DeveloperAPI
    def deterministic_sample(self) -> TensorType:
        """
        Get the deterministic "sampling" output from the distribution.
        This is usually the max likelihood output, i.e. mean for Normal, argmax
        for Categorical, etc..
        """
        raise NotImplementedError

    @DeveloperAPI
    def sampled_action_logp(self) -> TensorType:
        """Returns the log probability of the last sampled action."""
        raise NotImplementedError

    @DeveloperAPI
    def logp(self, x: TensorType) -> TensorType:
        """The log-likelihood of the action distribution."""
        raise NotImplementedError

    @DeveloperAPI
    def kl(self, other: "ActionDistribution") -> TensorType:
        """The KL-divergence between two action distributions."""
        raise NotImplementedError

    @DeveloperAPI
    def entropy(self) -> TensorType:
        """The entropy of the action distribution."""
        raise NotImplementedError

    def multi_kl(self, other: "ActionDistribution") -> TensorType:
        """The KL-divergence between two action distributions.

        This differs from kl() in that it can return an array for
        MultiDiscrete. TODO(ekl) consider removing this.
        """
        return self.kl(other)

    def multi_entropy(self) -> TensorType:
        """The entropy of the action distribution.

        This differs from entropy() in that it can return an array for
        MultiDiscrete. TODO(ekl) consider removing this.
        """
        return self.entropy()

    @staticmethod
    @DeveloperAPI
    def required_model_output_shape(
        action_space: gym.Space, model_config: ModelConfigDict
    ) -> Union[int, np.ndarray]:
        """Returns the required shape of an input parameter tensor for a
        particular action space and an optional dict of distribution-specific
        options.

        Args:
            action_space (gym.Space): The action space this distribution will
                be used for, whose shape attributes will be used to determine
                the required shape of the input parameter tensor.
            model_config: Model's config dict (as defined in catalog.py)

        Returns:
            model_output_shape (int or np.ndarray of ints): size of the
                required input vector (minus leading batch dimension).
        """
        raise NotImplementedError
