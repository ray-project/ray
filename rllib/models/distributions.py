"""This is the next version of action distribution base class."""
from typing import Tuple
import gym
import abc

from ray.rllib.utils.annotations import ExperimentalAPI
from ray.rllib.utils.typing import TensorType, Union, ModelConfigDict


@ExperimentalAPI
class Distribution(abc.ABC):
    """The base class for distribution over a random variable.

    Examples:
        >>> model = ... # a model that outputs a vector of logits
        >>> action_logits = model.forward(obs)
        >>> action_dist = Distribution(action_logits)
        >>> action = action_dist.sample()
        >>> logp = action_dist.logp(action)
        >>> kl = action_dist.kl(action_dist2)
        >>> entropy = action_dist.entropy()

    """

    def __init__(self, *args, **kwargs) -> None:
        # save the args and kwargs in a state dict for later use
        self._state_dict = {"args": args, "kwargs": kwargs}

    @abc.abstractmethod
    def sample(
        self,
        *,
        sample_shape: Tuple[int, ...] = None,
        return_logp: bool = False,
        **kwargs
    ) -> Union[TensorType, Tuple[TensorType, TensorType]]:
        """Draw a sample from the distribution.

        Args:
            sample_shape: The shape of the sample to draw.
            return_logp: Whether to return the logp of the sampled values.
            **kwargs: Forward compatibility placeholder.

        Returns:
            The sampled values. If return_logp is True, returns a tuple of the
            sampled values and its logp.
        """

    @abc.abstractmethod
    def rsample(
        self,
        *,
        sample_shape: Tuple[int, ...] = None,
        return_logp: bool = False,
        **kwargs
    ) -> Union[TensorType, Tuple[TensorType, TensorType]]:
        """Draw a re-parameterized sample from the action distribution.

        If this method is implemented, we can take gradients of samples w.r.t. the
        distribution parameters.

        Args:
            sample_shape: The shape of the sample to draw.
            return_logp: Whether to return the logp of the sampled values.
            **kwargs: Forward compatibility placeholder.

        Returns:
            The sampled values. If return_logp is True, returns a tuple of the
            sampled values and its logp.
        """

    @abc.abstractmethod
    def logp(self, value: TensorType, **kwargs) -> TensorType:
        """The log-likelihood of the distribution computed at `value`

        Args:
            value: The value to compute the log-likelihood at.
            **kwargs: Forward compatibility placeholder.

        Returns:
            The log-likelihood of the value.
        """

    @abc.abstractmethod
    def kl(self, other: "Distribution", **kwargs) -> TensorType:
        """The KL-divergence between two distributions.

        Args:
            other: The other distribution.
            **kwargs: Forward compatibility placeholder.

        Returns:
            The KL-divergence between the two distributions.
        """

    @abc.abstractmethod
    def entropy(self, **kwargs) -> TensorType:
        """The entropy of the distribution.

        Args:
            **kwargs: Forward compatibility placeholder.

        Returns:
            The entropy of the distribution.
        """

    @staticmethod
    @abc.abstractmethod
    def required_model_output_shape(
        space: gym.Space, model_config: ModelConfigDict
    ) -> Tuple[int, ...]:
        """Returns the required shape of an input parameter tensor for a
        particular space and an optional dict of distribution-specific
        options.

        Let's have this method here just as a reminder to the next developer that this
        was part of the old distribution classes that we may or may not keep depending
        on how the catalog gets written.

        Args:
            space: The space this distribution will be used for,
                whose shape attributes will be used to determine the required shape of
                the input parameter tensor.
            model_config: Model's config dict (as defined in catalog.py)

        Returns:
            size of the required input vector (minus leading batch dimension).
        """

    @abc.abstractmethod
    def get_state(self) -> dict:
        """Returns the state of the distribution.

        The output of this method is used in set_state.

        Returns:
            The state of the distribution.
        """

    @abc.abstractmethod
    def set_state(self, state: dict) -> None:
        """Sets the state of the distribution.

        Args:
            state: The state of the distribution. This is the output of get_state.
        """

    def to_dict(self) -> dict:
        """Returns a dict representation of the distribution.

        Returns:
            A dict representation of the distribution.
        """
        return self._state_dict

    @classmethod
    def from_dict(cls, data: dict) -> "Distribution":
        """Creates a distribution from a dict representation.

        Args:
            data: The dict representation of the distribution.

        Returns:
            A distribution.
        """
        return cls(*data["args"], **data["kwargs"])
