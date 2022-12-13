import abc
from typing import Mapping, Any, Type, TYPE_CHECKING

if TYPE_CHECKING:
    from ray.rllib.core.rl_module.marl_module import MultiAgentRLModule

from ray.rllib.utils.annotations import (
    ExperimentalAPI,
    OverrideToImplementCustomLogic,
    OverrideToImplementCustomLogic_CallToSuperRecommended,
)

from ray.rllib.models.specs.specs_dict import ModelSpec, check_specs
from ray.rllib.models.distributions import Distribution
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.utils.typing import SampleBatchType


ModuleID = str


@ExperimentalAPI
class RLModule(abc.ABC):
    """Base class for RLlib modules.

    Here is the pseudocode for how the forward methods are called:

    # During Training (acting in env from each rollout worker)
    ----------------------------------------------------------
    .. code-block:: python

        module: RLModule = ...
        obs, info = env.reset()
        while not terminated and not truncated:
            fwd_outputs = module.forward_exploration({"obs": obs})
            # this can be deterministic or stochastic exploration
            action = fwd_outputs["action_dist"].sample()
            next_obs, reward, terminated, truncated, info = env.step(action)
            buffer.add(obs, action, next_obs, reward, terminated, truncated, info)
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
        obs, info = env.reset()
        while not terminated and not truncated:
            fwd_outputs = module.forward_inference({"obs": obs})
            # this can be deterministic or stochastic evaluation
            action = fwd_outputs["action_dist"].sample()
            next_obs, reward, terminated, truncated, info = env.step(action)
            next_obs = obs

    Args:
        config: The config object for the module.

    Abstract Methods:
        forward_train: Forward pass during training.
        forward_exploration: Forward pass during training for exploration.
        forward_inference: Forward pass during inference.

    Note: There is a reason that the specs are not written as abstract properties.
        The reason is that torch overrides `__getattr__` and `__setattr__`. This means
        that if we define the specs as properties, then any error in the property will
        be interpreted as a failure to retrieve the attribute and will invoke
        `__getattr__` which will give a confusing error about the attribute not found.
        More details here: https://github.com/pytorch/pytorch/issues/49726.
    """

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def __init__(self, config: Mapping[str, Any] = None) -> None:
        self.config = config or {}
        self.setup()
        self._input_specs_train = self.input_specs_train()
        self._output_specs_train = self.output_specs_train()
        self._input_specs_exploration = self.input_specs_exploration()
        self._output_specs_exploration = self.output_specs_exploration()
        self._input_specs_inference = self.input_specs_inference()
        self._output_specs_inference = self.output_specs_inference()

    def setup(self) -> None:
        """Called once during initialization.

        Override this method to perform any setup logic.
        """
        pass

    def get_initial_state(self) -> NestedDict:
        """Returns the initial state of the module.

        This is used for recurrent models.
        """
        return {}

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def output_specs_inference(self) -> ModelSpec:
        """Returns the output specs of the forward_inference method.

        Override this method to customize the output specs of the inference call.
        The default implementation requires the forward_inference to reutn a dict that
        has `action_dist` key and its value is an instance of `Distribution`.
        This assumption must always hold.
        """
        return ModelSpec({"action_dist": Distribution})

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def output_specs_exploration(self) -> ModelSpec:
        """Returns the output specs of the forward_exploration method.

        Override this method to customize the output specs of the inference call.
        The default implementation requires the forward_exploration to reutn a dict
        that has `action_dist` key and its value is an instance of
        `Distribution`. This assumption must always hold.
        """
        return ModelSpec({"action_dist": Distribution})

    def output_specs_train(self) -> ModelSpec:
        """Returns the output specs of the forward_train method."""
        return ModelSpec()

    def input_specs_inference(self) -> ModelSpec:
        """Returns the input specs of the forward_inference method."""
        return ModelSpec()

    def input_specs_exploration(self) -> ModelSpec:
        """Returns the input specs of the forward_exploration method."""
        return ModelSpec()

    def input_specs_train(self) -> ModelSpec:
        """Returns the input specs of the forward_train method."""
        return ModelSpec()

    @check_specs(
        input_spec="_input_specs_inference", output_spec="_output_specs_inference"
    )
    def forward_inference(self, batch: SampleBatchType, **kwargs) -> Mapping[str, Any]:
        """Forward-pass during evaluation, called from the sampler. This method should
        not be overriden. Instead, override the _forward_inference method.

        Args:
            batch: The input batch. This input batch should comply with
                input_specs_inference().
            **kwargs: Additional keyword arguments.

        Returns:
            The output of the forward pass. This output should comply with the
            ouptut_specs_inference().
        """
        return self._forward_inference(batch, **kwargs)

    @abc.abstractmethod
    def _forward_inference(self, batch: NestedDict, **kwargs) -> Mapping[str, Any]:
        """Forward-pass during evaluation. See forward_inference for details."""

    @check_specs(
        input_spec="_input_specs_exploration", output_spec="_output_specs_exploration"
    )
    def forward_exploration(
        self, batch: SampleBatchType, **kwargs
    ) -> Mapping[str, Any]:
        """Forward-pass during exploration, called from the sampler. This method should
        not be overriden. Instead, override the _forward_exploration method.

        Args:
            batch: The input batch. This input batch should comply with
                input_specs_exploration().
            **kwargs: Additional keyword arguments.

        Returns:
            The output of the forward pass. This output should comply with the
            ouptut_specs_exploration().
        """
        return self._forward_exploration(batch, **kwargs)

    @abc.abstractmethod
    def _forward_exploration(self, batch: NestedDict, **kwargs) -> Mapping[str, Any]:
        """Forward-pass during exploration. See forward_exploration for details."""

    @check_specs(input_spec="_input_specs_train", output_spec="_output_specs_train")
    def forward_train(
        self,
        batch: SampleBatchType,
    ) -> Mapping[str, Any]:
        """Forward-pass during training called from the trainer. This method should
        not be overriden. Instead, override the _forward_train method.

        Args:
            batch: The input batch. This input batch should comply with
                input_specs_train().
            **kwargs: Additional keyword arguments.

        Returns:
            The output of the forward pass. This output should comply with the
            ouptut_specs_train().
        """
        return self._forward_train(batch)

    @abc.abstractmethod
    def _forward_train(self, batch: NestedDict, **kwargs) -> Mapping[str, Any]:
        """Forward-pass during training. See forward_train for details."""

    @abc.abstractmethod
    def get_state(self) -> Mapping[str, Any]:
        """Returns the state dict of the module."""

    @abc.abstractmethod
    def set_state(self, state_dict: Mapping[str, Any]) -> None:
        """Sets the state dict of the module."""

    @abc.abstractmethod
    def make_distributed(self, dist_config: Mapping[str, Any] = None) -> None:
        """Reserved API, Makes the module distributed."""

    @abc.abstractmethod
    def is_distributed(self) -> bool:
        """Reserved API, Returns True if the module is distributed."""

    def as_multi_agent(self) -> "MultiAgentRLModule":
        """Returns a multi-agent wrapper around this module."""
        return self.get_multi_agent_class()({DEFAULT_POLICY_ID: self})

    @classmethod
    @OverrideToImplementCustomLogic
    def get_multi_agent_class(cls) -> Type["MultiAgentRLModule"]:
        """Returns the multi-agent wrapper class for this module."""
        from ray.rllib.core.rl_module.marl_module import MultiAgentRLModule

        return MultiAgentRLModule
