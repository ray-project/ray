from typing import Mapping, Any
import abc
from ray.rllib.models.specs.specs_base import TensorSpecs
from ray.rllib.utils.annotations import (
    ExperimentalAPI,
    OverrideToImplementCustomLogic_CallToSuperRecommended,
)
from ray.rllib.core.base_module import Module

from ray.rllib.models.specs.specs_dict import ModelSpecDict, check_specs
from ray.rllib.models.action_dist_v2 import ActionDistributionV2


@ExperimentalAPI
class RLModule(abc.ABC):
    """Base class for RLlib modules.

    Here is the pseudo code for how the forward methods are called:

    # During Training (acting in env from each rollout worker)
    ----------------------------------------------------------
    .. code-block:: python

        module: RLModule = ...
        obs = env.reset()
        while not done:
            fwd_outputs = module.forward_exploration({"obs": obs})
            # this can be deterministic or stochastic exploration
            action = fwd_outputs["action_dist"].sample()
            next_obs, reward, done, info = env.step(action)
            buffer.add(obs, action, next_obs, reward, done, info)
            next_obs = obs

    # During Training (learning the policy)
    ----------------------------------------------------------
    .. code-block:: python
        module: RLModule = ...
        fwd_ins = buffer.sample()
        fwd_outputs = module.forward_train(fwd_ins)
        loss = compute_loss(fwd_outputs, fwd_ins)
        update_params(module, loss)

    # During Inference (acting in env during evaluation)
    ----------------------------------------------------------
    .. code-block:: python
        module: RLModule = ...
        obs = env.reset()
        while not done:
            fwd_outputs = module.forward_inference({"obs": obs})
            # this can be deterministic or stochastic evaluation
            action = fwd_outputs["action_dist"].sample()
            next_obs, reward, done, info = env.step(action)
            next_obs = obs

    Args:
        config: The config object for the module.
        **kwargs: Foward compatibility kwargs.

    Abstract Methods:
        forward_train: Forward pass during training.
        forward_exploration: Forward pass during training for exploration.
        forward_inference: Forward pass during inference.
    """

    def __init__(self, config: Mapping[str, Any], **kwargs) -> None:
        self.config = config

    @property
    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def output_specs_inference(self) -> ModelSpecDict:
        """Returns the output specs of the forward_inference method.

        Override this method to customize the output specs of the inference call.
        The default implementation requires the forward_inference to reutn a dict that
        has `action_dist` key and its value is an instance of `ActionDistributionV2`. This assumption must always hold.
        """
        return ModelSpecDict({"action_dist": ActionDistributionV2})

    @property
    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def output_specs_exploration(self) -> ModelSpecDict:
        """Returns the output specs of the forward_exploration method.

        Override this method to customize the output specs of the inference call.
        The default implementation requires the forward_exploration to reutn a dict
        that has `action_dist` key and its value is an instance of
        `ActionDistributionV2`. This assumption must always hold.
        """
        return ModelSpecDict({"action_dist": ActionDistributionV2})

    @property
    @abc.abstractmethod
    def output_specs_train(self) -> ModelSpecDict:
        """Returns the output specs of the forward_train method."""

    @property
    @abc.abstractmethod
    def input_specs_inference(self) -> ModelSpecDict:
        """Returns the input specs of the forward_inference method."""

    @property
    @abc.abstractmethod
    def input_specs_exploration(self) -> ModelSpecDict:
        """Returns the input specs of the forward_exploration method."""

    @property
    @abc.abstractmethod
    def input_specs_train(self) -> ModelSpecDict:
        """Returns the input specs of the forward_train method."""

    @check_specs(
        input_spec="input_specs_inference", output_spec="output_specs_inference"
    )
    def forward_inference(
        self, batch: Mapping[str, Any], **kwargs
    ) -> Mapping[str, Any]:
        """Forward-pass during evaluation, called from the sampler. This method should
        not be overriden. Instead, override the _forward_inference method."""
        return self._forward_inference(batch, **kwargs)

    @abc.abstractmethod
    def _forward_inference(
        self, batch: Mapping[str, Any], **kwargs
    ) -> Mapping[str, Any]:
        """Forward-pass during evaluation"""

    @check_specs(
        input_spec="input_specs_exploration", output_spec="output_specs_exploration"
    )
    def forward_exploration(
        self, batch: Mapping[str, Any], **kwargs
    ) -> Mapping[str, Any]:
        """Forward-pass during exploration, called from the sampler. This method should
        not be overriden. Instead, override the _forward_exploration method."""
        return self._forward_exploration(batch, **kwargs)

    @abc.abstractmethod
    def _forward_exploration(
        self, batch: Mapping[str, Any], **kwargs
    ) -> Mapping[str, Any]:
        """Forward-pass during exploration"""

    @check_specs(input_spec="input_specs_train", output_spec="output_specs_train")
    def forward_train(self, batch: Mapping[str, Any], **kwargs) -> Mapping[str, Any]:
        """Forward-pass during training called from the trainer. This method should
        not be overriden. Instead, override the _forward_train method."""
        return self._forward_train(batch, **kwargs)

    @abc.abstractmethod
    def _forward_train(self, batch: Mapping[str, Any], **kwargs) -> Mapping[str, Any]:
        """Forward-pass during training"""

    @abc.abstractmethod
    def get_state(self) -> Mapping[str, Any]:
        """Returns the state dict of the module."""

    @abc.abstractmethod
    def set_state(self, state_dict: Mapping[str, Any]) -> None:
        """Sets the state dict of the module."""


class MultiAgentRLModule(RLModule):
    pass
