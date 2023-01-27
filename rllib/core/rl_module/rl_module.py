import abc
from dataclasses import dataclass
import gymnasium as gym
from typing import Mapping, Any, TYPE_CHECKING, Union, Optional, Type, Dict

if TYPE_CHECKING:
    from ray.rllib.core.rl_module.marl_module import MultiAgentRLModule

from ray.rllib.utils.annotations import (
    ExperimentalAPI,
    OverrideToImplementCustomLogic_CallToSuperRecommended,
)
from ray.rllib.utils.serialization import check_if_args_kwargs_serializable

from ray.rllib.models.specs.typing import SpecType
from ray.rllib.models.specs.checker import check_input_specs, check_output_specs
from ray.rllib.models.distributions import Distribution
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.nested_dict import NestedDict

from ray.rllib.utils.typing import SampleBatchType


ModuleID = str


@ExperimentalAPI
@dataclass
class SingleAgentRLModuleSpec:
    """A utility spec class to make it constructing RLModules (in single-agent case) easier.

    Args:
        module_class: ...
        observation_space: ...
        action_space: ...
        model_config: ...
    """

    module_class: Optional[Type["RLModule"]] = None
    observation_space: Optional["gym.Space"] = None
    action_space: Optional["gym.Space"] = None
    model_config: Optional[Dict[str, Any]] = None

    def build(self) -> "RLModule":
        return self.module_class.from_model_config(
            observation_space=self.observation_space,
            action_space=self.action_space,
            model_config=self.model_config,
        )


@ExperimentalAPI
@dataclass
class RLModuleConfig:
    """Configuration for the PPO module.
    # TODO (Kourosh): Whether we need this or not really depends on how the catalog
    # design end up being.
    Attributes:
        observation_space: The observation space of the environment.
        action_space: The action space of the environment.
        max_seq_len: Max seq len for training an RNN model.
        (TODO (Kourosh) having max_seq_len here seems a bit unnatural, can we rethink
        this design?)
    """

    observation_space: gym.Space = None
    action_space: gym.Space = None
    max_seq_len: int = None


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

    def __init__(self, *args, **kwargs):
        check_if_args_kwargs_serializable(args, kwargs)
        self._args_and_kwargs = {"args": args, "kwargs": kwargs}

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
        self._input_specs_train = self.input_specs_train()
        self._output_specs_train = self.output_specs_train()
        self._input_specs_exploration = self.input_specs_exploration()
        self._output_specs_exploration = self.output_specs_exploration()
        self._input_specs_inference = self.input_specs_inference()
        self._output_specs_inference = self.output_specs_inference()

    @classmethod
    def from_model_config(
        cls,
        observation_space: gym.Space,
        action_space: gym.Space,
        model_config: Mapping[str, Any],
    ) -> Union["RLModule", Mapping[str, Any]]:
        """Creates a RLModule instance from a model config dict and spaces.

        The model config dict is the same as the one passed to the AlgorithmConfig
        object that contains global model configurations parameters.

        This method can also be used to create a config dict for the module constructor
        so it can be re-used to create multiple instances of the module.

        Example:

        .. code-block:: python

            class MyModule(RLModule):
                def __init__(self, input_dim, output_dim):
                    self.input_dim, self.output_dim = input_dim, output_dim

                @classmethod
                def from_model_config(
                    cls,
                    observation_space: gym.Space,
                    action_space: gym.Space,
                    model_config: Mapping[str, Any],
                    return_config: bool = False,
                ):
                    return cls(
                        input_dim=observation_space.shape[0],
                        output_dim=action_space.n
                    )

            module = MyModule.from_model_config(
                observation_space=gym.spaces.Box(low=0, high=1, shape=(4,)),
                action_space=gym.spaces.Discrete(2),
                model_config={},
            )


        Args:
            observation_space: The observation space of the env.
            action_space: The action space of the env.
            model_config: The model config dict.
        """
        raise NotImplementedError

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
        return {}

    def input_specs_exploration(self) -> SpecType:
        """Returns the input specs of the forward_exploration method."""
        return {}

    def input_specs_train(self) -> SpecType:
        """Returns the input specs of the forward_train method."""
        return {}

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
            "args": self._args_and_kwargs["args"],
            "kwargs": self._args_and_kwargs["kwargs"],
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
        for key in ["class", "args", "kwargs", "state"]:
            if key not in state:
                raise ValueError(
                    "By default, the serialized state must contain the following "
                    f"keys: 'class', 'args', 'args', and 'kwargs'. Got: {state.keys()}"
                )
        constructor = state["class"]
        module = constructor(*state["args"], **state["kwargs"])
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

        return MultiAgentRLModule({DEFAULT_POLICY_ID: self})
