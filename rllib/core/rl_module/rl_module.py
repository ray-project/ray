import abc
from dataclasses import dataclass
import gymnasium as gym
from typing import Mapping, Any, TYPE_CHECKING, Optional, Type

if TYPE_CHECKING:
    from ray.rllib.core.rl_module.marl_module import MultiAgentRLModule
    from ray.rllib.core.models.catalog import Catalog

from ray.rllib.utils.annotations import (
    ExperimentalAPI,
    OverrideToImplementCustomLogic_CallToSuperRecommended,
)

from ray.rllib.models.specs.typing import SpecType
from ray.rllib.models.specs.checker import (
    check_input_specs,
    check_output_specs,
    convert_to_canonical_format,
)
from ray.rllib.models.distributions import Distribution
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID, SampleBatch
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.utils.typing import SampleBatchType
from ray.rllib.utils.serialization import gym_space_from_dict, gym_space_to_dict


ModuleID = str


@ExperimentalAPI
@dataclass
class SingleAgentRLModuleSpec:
    """A utility spec class to make it constructing RLModules (in single-agent case) easier.

    Args:
        module_class: The RLModule class to use.
        observation_space: The observation space of the RLModule.
        action_space: The action space of the RLModule.
        model_config_dict: The model config dict to use.
        catalog_class: The Catalog class to use.
    """

    module_class: Optional[Type["RLModule"]] = None
    observation_space: Optional[gym.Space] = None
    action_space: Optional[gym.Space] = None
    model_config_dict: Optional[Mapping[str, Any]] = None
    catalog_class: Optional[Type["Catalog"]] = None

    def get_rl_module_config(self) -> "RLModuleConfig":
        """Returns the RLModule config for this spec."""
        return RLModuleConfig(
            observation_space=self.observation_space,
            action_space=self.action_space,
            model_config_dict=self.model_config_dict,
            catalog_class=self.catalog_class,
        )

    def build(self) -> "RLModule":
        if self.module_class is None:
            raise ValueError("RLModule class is not set.")
        if self.observation_space is None:
            raise ValueError("Observation space is not set.")
        if self.action_space is None:
            raise ValueError("Action space is not set.")
        if self.model_config_dict is None:
            raise ValueError("Model config is not set.")

        module_config = self.get_rl_module_config()
        return self.module_class(module_config)

    @classmethod
    def from_module(cls, module: "RLModule") -> "SingleAgentRLModuleSpec":
        from ray.rllib.core.rl_module.marl_module import MultiAgentRLModule

        if isinstance(module, MultiAgentRLModule):
            raise ValueError(
                "MultiAgentRLModule cannot be converted to SingleAgentRLModuleSpec."
            )

        return SingleAgentRLModuleSpec(
            module_class=type(module),
            observation_space=module.config.observation_space,
            action_space=module.config.action_space,
            model_config_dict=module.config.model_config_dict,
            catalog_class=module.config.catalog_class,
        )

    def to_dict(self):
        """Returns a serialized representation of the spec."""

        return {
            "module_class": self.module_class,
            "module_config": self.get_rl_module_config().to_dict(),
        }

    @classmethod
    def from_dict(cls, d):
        """Returns a single agent RLModule spec from a serialized representation."""
        module_class = d["module_class"]

        module_config = RLModuleConfig.from_dict(d["module_config"])
        observation_space = module_config.observation_space
        action_space = module_config.action_space
        model_config_dict = module_config.model_config_dict
        catalog_class = module_config.catalog_class

        return SingleAgentRLModuleSpec(
            module_class=module_class,
            observation_space=observation_space,
            action_space=action_space,
            model_config_dict=model_config_dict,
            catalog_class=catalog_class,
        )

    def update(self, other) -> None:
        """Updates this spec with the given other spec. Works like dict.update()."""
        if not isinstance(other, SingleAgentRLModuleSpec):
            raise ValueError("Can only update with another SingleAgentRLModuleSpec.")

        # If the field is None in the other, keep the current field, otherwise update
        # with the new value.
        self.module_class = other.module_class or self.module_class
        self.observation_space = other.observation_space or self.observation_space
        self.action_space = other.action_space or self.action_space
        self.model_config_dict = other.model_config_dict or self.model_config_dict
        self.catalog_class = other.catalog_class or self.catalog_class


@ExperimentalAPI
@dataclass
class RLModuleConfig:

    observation_space: gym.Space = None
    action_space: gym.Space = None
    model_config_dict: Mapping[str, Any] = None
    catalog_class: Type["Catalog"] = None

    def get_catalog(self) -> "Catalog":
        """Returns the catalog for this config."""
        return self.catalog_class(
            observation_space=self.observation_space,
            action_space=self.action_space,
            model_config_dict=self.model_config_dict,
        )

    def to_dict(self):
        """Returns a serialized representation of the config."""
        return {
            "observation_space": gym_space_to_dict(self.observation_space),
            "action_space": gym_space_to_dict(self.action_space),
            "model_config_dict": self.model_config_dict,
            "catalog_class": self.catalog_class,
        }

    @classmethod
    def from_dict(cls, d):
        """Creates a config from a serialized representation."""
        return cls(
            observation_space=gym_space_from_dict(d["observation_space"]),
            action_space=gym_space_from_dict(d["action_space"]),
            model_config_dict=d["model_config_dict"],
            catalog_class=d["catalog_class"],
        )


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
        *args: Arguments for constructing the RLModule.
        **kwargs: Keyword args for constructing the RLModule.

    Abstract Methods:
        forward_train: Forward pass during training.
        forward_exploration: Forward pass during training for exploration.
        forward_inference: Forward pass during inference.

    Error:
        The args and kwargs that are passed to the constructor are saved for
        serialization and deserialization purposes. The RLModule checks if they
        are serializable/deserializable using ray and if they are not, a
        ValueError is thrown.

    Note: There is a reason that the specs are not written as abstract properties.
        The reason is that torch overrides `__getattr__` and `__setattr__`. This means
        that if we define the specs as properties, then any error in the property will
        be interpreted as a failure to retrieve the attribute and will invoke
        `__getattr__` which will give a confusing error about the attribute not found.
        More details here: https://github.com/pytorch/pytorch/issues/49726.
    """

    def __init__(self, config: RLModuleConfig):
        self.config = config

    def __init_subclass__(cls, **kwargs):
        # Automatically add a __post_init__ method to all subclasses of RLModule.
        # This method is called after the __init__ method of the subclass.
        def init_decorator(previous_init):
            def new_init(self, *args, **kwargs):
                previous_init(self, *args, **kwargs)
                if type(self) == cls:
                    self.__post_init__()

            return new_init

        cls.__init__ = init_decorator(cls.__init__)

    def __post_init__(self):
        """Called automatically after the __init__ method of the subclass.

        The module first calls the __init__ method of the subclass, With in the
        __init__ you should call the super().__init__ method. Then after the __init__
        method of the subclass is called, the __post_init__ method is called.

        This is a good place to do any initialization that requires access to the
        subclass's attributes.
        """
        self._input_specs_train = convert_to_canonical_format(self.input_specs_train())
        self._output_specs_train = convert_to_canonical_format(
            self.output_specs_train()
        )
        self._input_specs_exploration = convert_to_canonical_format(
            self.input_specs_exploration()
        )
        self._output_specs_exploration = convert_to_canonical_format(
            self.output_specs_exploration()
        )
        self._input_specs_inference = convert_to_canonical_format(
            self.input_specs_inference()
        )
        self._output_specs_inference = convert_to_canonical_format(
            self.output_specs_inference()
        )

    def get_initial_state(self) -> NestedDict:
        """Returns the initial state of the module.

        This is used for recurrent models.
        """
        return {}

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def output_specs_inference(self) -> SpecType:
        """Returns the output specs of the forward_inference method.

        Override this method to customize the output specs of the inference call.
        The default implementation requires the forward_inference to reutn a dict that
        has `action_dist` key and its value is an instance of `Distribution`.
        This assumption must always hold.
        """
        return {"action_dist": Distribution}

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def output_specs_exploration(self) -> SpecType:
        """Returns the output specs of the forward_exploration method.

        Override this method to customize the output specs of the inference call.
        The default implementation requires the forward_exploration to reutn a dict
        that has `action_dist` key and its value is an instance of
        `Distribution`. This assumption must always hold.
        """
        return {"action_dist": Distribution}

    def output_specs_train(self) -> SpecType:
        """Returns the output specs of the forward_train method."""
        return {}

    def input_specs_inference(self) -> SpecType:
        """Returns the input specs of the forward_inference method."""
        return self._default_input_specs()

    def input_specs_exploration(self) -> SpecType:
        """Returns the input specs of the forward_exploration method."""
        return self._default_input_specs()

    def input_specs_train(self) -> SpecType:
        """Returns the input specs of the forward_train method."""
        return self._default_input_specs()

    def _default_input_specs(self) -> SpecType:
        """Returns the default input specs."""
        return [SampleBatch.OBS]

    @check_input_specs("_input_specs_inference")
    @check_output_specs("_output_specs_inference")
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

    @check_input_specs("_input_specs_exploration")
    @check_output_specs("_output_specs_exploration")
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

    @check_input_specs("_input_specs_train")
    @check_output_specs("_output_specs_train")
    def forward_train(self, batch: SampleBatchType, **kwargs) -> Mapping[str, Any]:
        """Forward-pass during training called from the learner. This method should
        not be overriden. Instead, override the _forward_train method.

        Args:
            batch: The input batch. This input batch should comply with
                input_specs_train().
            **kwargs: Additional keyword arguments.

        Returns:
            The output of the forward pass. This output should comply with the
            ouptut_specs_train().
        """
        return self._forward_train(batch, **kwargs)

    @abc.abstractmethod
    def _forward_train(self, batch: NestedDict, **kwargs) -> Mapping[str, Any]:
        """Forward-pass during training. See forward_train for details."""

    @abc.abstractmethod
    def get_state(self) -> Mapping[str, Any]:
        """Returns the state dict of the module."""

    @abc.abstractmethod
    def set_state(self, state_dict: Mapping[str, Any]) -> None:
        """Sets the state dict of the module."""

    def serialize(self) -> Mapping[str, Any]:
        """Return the serialized state of the module."""
        return {
            "class": self.__class__,
            "config": self.config.to_dict(),
            "state": self.get_state(),
        }

    @classmethod
    def deserialize(cls, state: Mapping[str, Any]) -> "RLModule":
        """Construct a module from a serialized state.

        Args:
            state: The serialized state of the module.

        NOTE: this state is typically obtained from `serialize()`.

        NOTE: This method needs to be implemented in order to support
            checkpointing and fault tolerance.

        Returns:
            A deserialized RLModule.
        """
        module_class = state["class"]
        config = RLModuleConfig.from_dict(state["config"])
        module = module_class(config)
        module.set_state(state["state"])
        return module

    @abc.abstractmethod
    def make_distributed(self, dist_config: Mapping[str, Any] = None) -> None:
        """Reserved API, Makes the module distributed."""

    @abc.abstractmethod
    def is_distributed(self) -> bool:
        """Reserved API, Returns True if the module is distributed."""

    def as_multi_agent(self) -> "MultiAgentRLModule":
        """Returns a multi-agent wrapper around this module."""
        from ray.rllib.core.rl_module.marl_module import MultiAgentRLModule

        marl_module = MultiAgentRLModule()
        marl_module.add_module(DEFAULT_POLICY_ID, self)
        return marl_module
