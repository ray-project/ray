from typing import Tuple
import gym
import abc

from ray.rllib.utils.annotations import ExperimentalAPI
from ray.rllib.utils.typing import TensorType, Union, ModelConfigDict


@ExperimentalAPI
class ActionDistributionV2(abc.ABC):
    """The policy action distribution of an agent.

    Args:
        inputs: input vector to define the distribution over.

    Examples:
        >>> model = ... # a model that outputs a vector of logits
        >>> action_logits = model.forward(obs)
        >>> action_dist = ActionDistribution(action_logits)
        >>> action = action_dist.sample()
        >>> logp = action_dist.logp(action)
        >>> kl = action_dist.kl(action_dist2)
        >>> entropy = action_dist.entropy()

    """

    @abc.abstractmethod
    def sample(
        self,
        *,
        sample_shape: Tuple[int, ...] = None,
        return_logp: bool = False,
        **kwargs
    ) -> Union[TensorType, Tuple[TensorType, TensorType]]:
        """Draw a sample from the action distribution.

        Args:
            sample_shape: The shape of the sample to draw.
            return_logp: Whether to return the logp of the sampled action.
            **kwargs: Forward compatibility placeholder.

        Returns:
            The sampled action. If return_logp is True, returns a tuple of the
            sampled action and its logp.
        """

    @abc.abstractmethod
    def logp(self, action: TensorType, **kwargs) -> TensorType:
        """The log-likelihood of the action distribution.

        Args:
            action: The action to compute the log-likelihood for.
            **kwargs: Forward compatibility placeholder.

        Returns:
            The log-likelihood of the action.
        """

    @abc.abstractmethod
    def kl(self, other: "ActionDistributionV2", **kwargs) -> TensorType:
        """The KL-divergence between two action distributions.

        Args:
            other: The other action distribution.
            **kwargs: Forward compatibility placeholder.

        Returns:
            The KL-divergence between the two action distributions.
        """

    @abc.abstractmethod
    def entropy(self, **kwargs) -> TensorType:
        """The entropy of the action distribution.

        Args:
            **kwargs: Forward compatibility placeholder.

        Returns:
            The entropy of the action distribution.
        """

    @staticmethod
    @abc.abstractmethod
    def required_model_output_shape(
        action_space: gym.Space, model_config: ModelConfigDict
    ) -> Tuple[int, ...]:
        """Returns the required shape of an input parameter tensor for a
        particular action space and an optional dict of distribution-specific
        options.

        Args:
            action_space: The action space this distribution will be used for,
                whose shape attributes will be used to determine the required shape of
                the input parameter tensor.
            model_config: Model's config dict (as defined in catalog.py)

        Returns:
            size of the required input vector (minus leading batch dimension).
        """
