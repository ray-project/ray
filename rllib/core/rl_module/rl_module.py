from typing import Mapping, Any
import abc
from ray.rllib.models.specs.specs_base import TensorSpecs
from ray.rllib.utils.annotations import ExperimentalAPI
from ray.rllib.core.base_module import Module

from ray.rllib.models.specs.specs_dict import apply_specs, ModelSpecDict


@ExperimentalAPI
class RLModule(Module):
    """Base class for RLlib modules.

    Here is the pseudo code for how the forward methods are called:

    # During Training (acting in env from each rollout worker)
    ----------------------------------------------------------
    .. code-block:: python

        module: RLModule = ...
        obs = env.reset()
        while not done:
            fwd_outputs = module.forward_exploration({"obs": obs})
            if "action" in fwd_outputs:
                action = fwd_outputs["action"]
            else:
                action = fwd_outputs["action_dist"].sample()
            next_obs, reward, done, info = env.step(action)
            buffer.add(obs, action, reward, done, info, next_obs)
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
            action = fwd_outputs["action"]
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

    def __init__(self, config, **kwargs) -> None:
        self.config = config

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()

        if not hasattr(cls.forward_exploration, "__apply_specs__"):
            setattr(
                cls,
                "forward_exploration",
                apply_specs(
                    cls.forward_exploration, output_specs="output_specs_exploration"
                ),
            )

        if not hasattr(cls.forward_inference, "__apply_specs__"):
            setattr(
                cls,
                "forward_inference",
                apply_specs(
                    cls.forward_inference, output_specs="output_specs_inference"
                ),
            )

    @property
    @abc.abstractmethod
    def output_specs_inference(self) -> ModelSpecDict:
        """Returns the inference output specs of this module."""

    @property
    @abc.abstractmethod
    def output_specs_exploration(self) -> ModelSpecDict:
        """Returns the exploration output specs of this module."""

    @apply_specs(output_specs="output_specs_inference")
    @abc.abstractmethod
    def forward_inference(
        self, batch: Mapping[str, Any], **kwargs
    ) -> Mapping[str, Any]:
        """Forward-pass during evaluation"""

    @apply_specs(output_specs="output_specs_exploration")
    @abc.abstractmethod
    def forward_exploration(
        self, batch: Mapping[str, Any], **kwargs
    ) -> Mapping[str, Any]:
        """Forward-pass during exploration"""

    @abc.abstractmethod
    def forward_train(self, batch: Mapping[str, Any], **kwargs) -> Mapping[str, Any]:
        """Forward-pass during training"""


class MultiAgentRLModule(RLModule):
    def forward_train(self, batch: Mapping[str, Any], **kwargs) -> Mapping[str, Any]:
        return batch
