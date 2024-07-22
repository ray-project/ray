import abc
from dataclasses import dataclass, field
from typing import Any, Collection, Dict, Optional, Type, TYPE_CHECKING, Union

import gymnasium as gym
import tree  # pip install dm_tree

if TYPE_CHECKING:
    from ray.rllib.core.rl_module.marl_module import (
        MultiAgentRLModule,
        MultiAgentRLModuleSpec,
    )
    from ray.rllib.core.models.catalog import Catalog

from ray.rllib.core import DEFAULT_MODULE_ID
from ray.rllib.core.columns import Columns
from ray.rllib.core.models.specs.typing import SpecType
from ray.rllib.core.models.specs.checker import (
    check_input_specs,
    check_output_specs,
    convert_to_canonical_format,
)
from ray.rllib.models.distributions import Distribution
from ray.rllib.policy.policy import get_gym_space_from_struct_of_tensors
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.view_requirement import ViewRequirement
from ray.rllib.utils.annotations import (
    ExperimentalAPI,
    override,
    OverrideToImplementCustomLogic,
    OverrideToImplementCustomLogic_CallToSuperRecommended,
)
from ray.rllib.utils.checkpoints import Checkpointable
from ray.rllib.utils.deprecation import Deprecated
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.serialization import (
    gym_space_from_dict,
    gym_space_to_dict,
    serialize_type,
    deserialize_type,
)
from ray.rllib.utils.typing import SampleBatchType, StateDict, ViewRequirementsDict
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
@dataclass
class SingleAgentRLModuleSpec:
    """Utility spec class to make constructing RLModules (in single-agent case) easier.

    Args:
        module_class: The RLModule class to use.
        observation_space: The observation space of the RLModule. This may differ
            from the observation space of the environment. For example, a discrete
            observation space of an environment, would usually correspond to a
            one-hot encoded observation space of the RLModule because of preprocessing.
        action_space: The action space of the RLModule.
        inference_only: Whether the RLModule should be configured in its inference-only
            state, in which any components not needed for pure action computing (such as
            a value function or a target network) might be missing.
        model_config_dict: The model config dict to use.
        catalog_class: The Catalog class to use.
        load_state_path: The path to the module state to load from. NOTE: This must be
            an absolute path.
    """

    module_class: Optional[Type["RLModule"]] = None
    observation_space: Optional[gym.Space] = None
    action_space: Optional[gym.Space] = None
    inference_only: bool = False
    model_config_dict: Optional[Dict[str, Any]] = None
    catalog_class: Optional[Type["Catalog"]] = None
    load_state_path: Optional[str] = None

    def get_rl_module_config(self) -> "RLModuleConfig":
        """Returns the RLModule config for this spec."""
        return RLModuleConfig(
            observation_space=self.observation_space,
            action_space=self.action_space,
            inference_only=self.inference_only,
            model_config_dict=self.model_config_dict or {},
            catalog_class=self.catalog_class,
        )

    def build(self) -> "RLModule":
        """Builds the RLModule from this spec."""
        if self.module_class is None:
            raise ValueError("RLModule class is not set.")
        if self.observation_space is None:
            raise ValueError("Observation space is not set.")
        if self.action_space is None:
            raise ValueError("Action space is not set.")

        module_config = self.get_rl_module_config()
        module = self.module_class(module_config)
        return module

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
            inference_only=module.config.inference_only,
            model_config_dict=module.config.model_config_dict,
            catalog_class=module.config.catalog_class,
        )

    def to_dict(self):
        """Returns a serialized representation of the spec."""

        return {
            "module_class": serialize_type(self.module_class),
            "module_config": self.get_rl_module_config().to_dict(),
        }

    @classmethod
    def from_dict(cls, d):
        """Returns a single agent RLModule spec from a serialized representation."""
        module_class = deserialize_type(d["module_class"])
        module_config = RLModuleConfig.from_dict(d["module_config"])

        spec = SingleAgentRLModuleSpec(
            module_class=module_class,
            observation_space=module_config.observation_space,
            action_space=module_config.action_space,
            inference_only=module_config.inference_only,
            model_config_dict=module_config.model_config_dict,
            catalog_class=module_config.catalog_class,
        )
        return spec

    def update(self, other, override: bool = True) -> None:
        """Updates this spec with the given other spec. Works like dict.update().

        Args:
            other: The other SingleAgentRLModule spec to update this one from.
            override: Whether to update all properties in `self` with those of `other.
                If False, only update those properties in `self` that are not None.
        """
        if not isinstance(other, SingleAgentRLModuleSpec):
            raise ValueError("Can only update with another SingleAgentRLModuleSpec.")

        # If the field is None in the other, keep the current field, otherwise update
        # with the new value.
        if override:
            self.module_class = other.module_class or self.module_class
            self.observation_space = other.observation_space or self.observation_space
            self.action_space = other.action_space or self.action_space
            self.inference_only = other.inference_only or self.inference_only
            self.model_config_dict = other.model_config_dict or self.model_config_dict
            self.catalog_class = other.catalog_class or self.catalog_class
            self.load_state_path = other.load_state_path or self.load_state_path
        # Only override, if the field is None in `self`.
        else:
            self.module_class = self.module_class or other.module_class
            self.observation_space = self.observation_space or other.observation_space
            self.action_space = self.action_space or other.action_space
            self.model_config_dict = self.model_config_dict or other.model_config_dict
            self.catalog_class = self.catalog_class or other.catalog_class
            self.load_state_path = self.load_state_path or other.load_state_path

    def as_multi_agent(self) -> "MultiAgentRLModuleSpec":
        """Returns a MultiAgentRLModuleSpec (`self` under DEFAULT_MODULE_ID key)."""
        from ray.rllib.core.rl_module.marl_module import MultiAgentRLModuleSpec

        return MultiAgentRLModuleSpec(
            module_specs={DEFAULT_MODULE_ID: self},
            load_state_path=self.load_state_path,
        )


@ExperimentalAPI
@dataclass
class RLModuleConfig:
    """A utility config class to make it constructing RLModules easier.

    Args:
        observation_space: The observation space of the RLModule. This may differ
            from the observation space of the environment. For example, a discrete
            observation space of an environment, would usually correspond to a
            one-hot encoded observation space of the RLModule because of preprocessing.
        action_space: The action space of the RLModule.
        inference_only: Whether the RLModule should be configured in its inference-only
            state, in which any components not needed for pure action computing (such as
            a value function or a target network) might be missing.
        model_config_dict: The model config dict to use.
        catalog_class: The Catalog class to use.
    """

    observation_space: gym.Space = None
    action_space: gym.Space = None
    inference_only: bool = False
    model_config_dict: Dict[str, Any] = field(default_factory=dict)
    catalog_class: Type["Catalog"] = None

    def get_catalog(self) -> "Catalog":
        """Returns the catalog for this config."""
        return self.catalog_class(
            observation_space=self.observation_space,
            action_space=self.action_space,
            model_config_dict=self.model_config_dict,
        )

    def to_dict(self):
        """Returns a serialized representation of the config.

        NOTE: This should be JSON-able. Users can test this by calling
            json.dumps(config.to_dict()).

        """
        catalog_class_path = (
            serialize_type(self.catalog_class) if self.catalog_class else ""
        )
        return {
            "observation_space": gym_space_to_dict(self.observation_space),
            "action_space": gym_space_to_dict(self.action_space),
            "inference_only": self.inference_only,
            "model_config_dict": self.model_config_dict,
            "catalog_class_path": catalog_class_path,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]):
        """Creates a config from a serialized representation."""
        catalog_class = (
            None
            if d["catalog_class_path"] == ""
            else deserialize_type(d["catalog_class_path"])
        )
        return cls(
            observation_space=gym_space_from_dict(d["observation_space"]),
            action_space=gym_space_from_dict(d["action_space"]),
            inference_only=d["inference_only"],
            model_config_dict=d["model_config_dict"],
            catalog_class=catalog_class,
        )


@PublicAPI(stability="alpha")
class RLModule(Checkpointable, abc.ABC):
    """Base class for RLlib modules.

    Subclasses should call super().__init__(config) in their __init__ method.
    Here is the pseudocode for how the forward methods are called:

    Example for creating a sampling loop:

    .. testcode::

        from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import (
            PPOTorchRLModule
        )
        from ray.rllib.algorithms.ppo.ppo_catalog import PPOCatalog
        import gymnasium as gym
        import torch

        env = gym.make("CartPole-v1")

        # Create a single agent RL module spec.
        module_spec = SingleAgentRLModuleSpec(
            module_class=PPOTorchRLModule,
            observation_space=env.observation_space,
            action_space=env.action_space,
            model_config_dict = {"hidden": [128, 128]},
            catalog_class = PPOCatalog,
        )
        module = module_spec.build()
        action_dist_class = module.get_inference_action_dist_cls()
        obs, info = env.reset()
        terminated = False

        while not terminated:
            fwd_ins = {"obs": torch.Tensor([obs])}
            fwd_outputs = module.forward_exploration(fwd_ins)
            # this can be either deterministic or stochastic distribution
            action_dist = action_dist_class.from_logits(
                fwd_outputs["action_dist_inputs"]
            )
            action = action_dist.sample()[0].numpy()
            obs, reward, terminated, truncated, info = env.step(action)


    Example for training:

    .. testcode::

        from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import (
            PPOTorchRLModule
        )
        from ray.rllib.algorithms.ppo.ppo_catalog import PPOCatalog
        import gymnasium as gym
        import torch

        env = gym.make("CartPole-v1")

        # Create a single agent RL module spec.
        module_spec = SingleAgentRLModuleSpec(
            module_class=PPOTorchRLModule,
            observation_space=env.observation_space,
            action_space=env.action_space,
            model_config_dict = {"hidden": [128, 128]},
            catalog_class = PPOCatalog,
        )
        module = module_spec.build()

        fwd_ins = {"obs": torch.Tensor([obs])}
        fwd_outputs = module.forward_train(fwd_ins)
        # loss = compute_loss(fwd_outputs, fwd_ins)
        # update_params(module, loss)

    Example for inference:

    .. testcode::

        from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import (
            PPOTorchRLModule
        )
        from ray.rllib.algorithms.ppo.ppo_catalog import PPOCatalog
        import gymnasium as gym
        import torch

        env = gym.make("CartPole-v1")

        # Create a single agent RL module spec.
        module_spec = SingleAgentRLModuleSpec(
            module_class=PPOTorchRLModule,
            observation_space=env.observation_space,
            action_space=env.action_space,
            model_config_dict = {"hidden": [128, 128]},
            catalog_class = PPOCatalog,
        )
        module = module_spec.build()

        while not terminated:
            fwd_ins = {"obs": torch.Tensor([obs])}
            fwd_outputs = module.forward_inference(fwd_ins)
            # this can be either deterministic or stochastic distribution
            action_dist = action_dist_class.from_logits(
                fwd_outputs["action_dist_inputs"]
            )
            action = action_dist.sample()[0].numpy()
            obs, reward, terminated, truncated, info = env.step(action)


    Args:
        config: The config for the RLModule.

    Abstract Methods:
        ``~_forward_train``: Forward pass during training.

        ``~_forward_exploration``: Forward pass during training for exploration.

        ``~_forward_inference``: Forward pass during inference.


    Note:
        There is a reason that the specs are not written as abstract properties.
        The reason is that torch overrides `__getattr__` and `__setattr__`. This means
        that if we define the specs as properties, then any error in the property will
        be interpreted as a failure to retrieve the attribute and will invoke
        `__getattr__` which will give a confusing error about the attribute not found.
        More details here: https://github.com/pytorch/pytorch/issues/49726.
    """

    framework: str = None

    STATE_FILE_NAME = "module_state.pkl"

    def __init__(self, config: RLModuleConfig):
        self.config = config

        # Make sure, `setup()` is only called once, no matter what. In some cases
        # of multiple inheritance (and with our __post_init__ functionality in place,
        # this might get called twice.
        if hasattr(self, "_is_setup") and self._is_setup:
            raise RuntimeError(
                "`RLModule.setup()` called twice within your RLModule implementation "
                f"{self}! Make sure you are using the proper inheritance order "
                "(TorchRLModule before [Algo]RLModule) or (TfRLModule before "
                "[Algo]RLModule) and that you are using `super().__init__(...)` in "
                "your custom constructor."
            )
        self.setup()
        self._is_setup = True

    def __init_subclass__(cls, **kwargs):
        # Automatically add a __post_init__ method to all subclasses of RLModule.
        # This method is called after the __init__ method of the subclass.
        def init_decorator(previous_init):
            def new_init(self, *args, **kwargs):
                previous_init(self, *args, **kwargs)
                if type(self) is cls:
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

    @OverrideToImplementCustomLogic
    def setup(self):
        """Sets up the components of the module.

        This is called automatically during the __init__ method of this class,
        therefore, the subclass should call super.__init__() in its constructor. This
        abstraction can be used to create any components (e.g. NN layers) that your
        RLModule needs.
        """
        return None

    @OverrideToImplementCustomLogic
    def get_train_action_dist_cls(self) -> Type[Distribution]:
        """Returns the action distribution class for this RLModule used for training.

        This class is used to get the correct action distribution class to be used by
        the training components. In case that no action distribution class is needed,
        this method can return None.

        Note that RLlib's distribution classes all implement the `Distribution`
        interface. This requires two special methods: `Distribution.from_logits()` and
        `Distribution.to_deterministic()`. See the documentation of the
        :py:class:`~ray.rllib.models.distributions.Distribution` class for more details.
        """
        raise NotImplementedError

    @OverrideToImplementCustomLogic
    def get_exploration_action_dist_cls(self) -> Type[Distribution]:
        """Returns the action distribution class for this RLModule used for exploration.

        This class is used to create action distributions from outputs of the
        forward_exploration method. If the case that no action distribution class is
        needed, this method can return None.

        Note that RLlib's distribution classes all implement the `Distribution`
        interface. This requires two special methods: `Distribution.from_logits()` and
        `Distribution.to_deterministic()`. See the documentation of the
        :py:class:`~ray.rllib.models.distributions.Distribution` class for more details.
        """
        raise NotImplementedError

    @OverrideToImplementCustomLogic
    def get_inference_action_dist_cls(self) -> Type[Distribution]:
        """Returns the action distribution class for this RLModule used for inference.

        This class is used to create action distributions from outputs of the forward
        inference method. If the case that no action distribution class is needed,
        this method can return None.

        Note that RLlib's distribution classes all implement the `Distribution`
        interface. This requires two special methods: `Distribution.from_logits()` and
        `Distribution.to_deterministic()`. See the documentation of the
        :py:class:`~ray.rllib.models.distributions.Distribution` class for more details.
        """
        raise NotImplementedError

    @OverrideToImplementCustomLogic
    def get_initial_state(self) -> Any:
        """Returns the initial state of the RLModule.

        This can be used for recurrent models.
        """
        return {}

    @OverrideToImplementCustomLogic
    def is_stateful(self) -> bool:
        """Returns False if the initial state is an empty dict (or None).

        By default, RLlib assumes that the module is non-recurrent if the initial
        state is an empty dict and recurrent otherwise.
        This behavior can be overridden by implementing this method.
        """
        initial_state = self.get_initial_state()
        assert isinstance(initial_state, dict), (
            "The initial state of an RLModule must be a dict, but is "
            f"{type(initial_state)} instead."
        )
        return bool(initial_state)

    @OverrideToImplementCustomLogic
    def update_default_view_requirements(
        self, defaults: ViewRequirementsDict
    ) -> Dict[str, ViewRequirement]:
        """Updates default view requirements with the view requirements of this module.

        This method should be called with view requirements that already contain
        information such as the given observation space, action space, etc.
        This method may then add additional shifts or state columns to the view
        requirements, or apply other changes.

        Args:
            defaults: The default view requirements to update.

        Returns:
            The updated view requirements.
        """
        if self.is_stateful():
            # get the initial state in numpy format, infer the state from it, and create
            # appropriate view requirements.
            init_state = convert_to_numpy(self.get_initial_state())
            init_state = tree.map_structure(lambda x: x[None], init_state)
            space = get_gym_space_from_struct_of_tensors(init_state, batched_input=True)
            max_seq_len = self.config.model_config_dict["max_seq_len"]
            assert max_seq_len is not None
            defaults[Columns.STATE_IN] = ViewRequirement(
                data_col=Columns.STATE_OUT,
                shift=-1,
                used_for_compute_actions=True,
                used_for_training=True,
                batch_repeat_value=max_seq_len,
                space=space,
            )

            if self.config.model_config_dict["lstm_use_prev_action"]:
                defaults[SampleBatch.PREV_ACTIONS] = ViewRequirement(
                    data_col=Columns.ACTIONS,
                    shift=-1,
                    used_for_compute_actions=True,
                    used_for_training=True,
                )

            if self.config.model_config_dict["lstm_use_prev_reward"]:
                defaults[SampleBatch.PREV_REWARDS] = ViewRequirement(
                    data_col=Columns.REWARDS,
                    shift=-1,
                    used_for_compute_actions=True,
                    used_for_training=True,
                )

            defaults[Columns.STATE_OUT] = ViewRequirement(
                data_col=Columns.STATE_OUT,
                used_for_compute_actions=False,
                used_for_training=True,
                space=space,
            )

        return defaults

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def output_specs_inference(self) -> SpecType:
        """Returns the output specs of the `forward_inference()` method.

        Override this method to customize the output specs of the inference call.
        The default implementation requires the `forward_inference()` method to return
        a dict that has `action_dist` key and its value is an instance of
        `Distribution`.
        """
        return [Columns.ACTION_DIST_INPUTS]

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def output_specs_exploration(self) -> SpecType:
        """Returns the output specs of the `forward_exploration()` method.

        Override this method to customize the output specs of the exploration call.
        The default implementation requires the `forward_exploration()` method to return
        a dict that has `action_dist` key and its value is an instance of
        `Distribution`.
        """
        return [Columns.ACTION_DIST_INPUTS]

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
        return [Columns.OBS]

    @check_input_specs("_input_specs_inference")
    @check_output_specs("_output_specs_inference")
    def forward_inference(self, batch: SampleBatchType, **kwargs) -> Dict[str, Any]:
        """Forward-pass during evaluation, called from the sampler.

        This method should not be overriden to implement a custom forward inference
        method. Instead, override the _forward_inference method.

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
    def _forward_inference(self, batch: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        """Forward-pass during evaluation. See forward_inference for details."""

    @check_input_specs("_input_specs_exploration")
    @check_output_specs("_output_specs_exploration")
    def forward_exploration(self, batch: SampleBatchType, **kwargs) -> Dict[str, Any]:
        """Forward-pass during exploration, called from the sampler.

        This method should not be overriden to implement a custom forward exploration
        method. Instead, override the _forward_exploration method.

        Args:
            batch: The input batch. This input batch should comply with
                input_specs_exploration().
            **kwargs: Additional keyword arguments.

        Returns:
            The output of the forward pass. This output should comply with the
            output_specs_exploration().
        """
        return self._forward_exploration(batch, **kwargs)

    @abc.abstractmethod
    def _forward_exploration(self, batch: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        """Forward-pass during exploration. See forward_exploration for details."""

    @check_input_specs("_input_specs_train")
    @check_output_specs("_output_specs_train")
    def forward_train(self, batch: SampleBatchType, **kwargs) -> Dict[str, Any]:
        """Forward-pass during training called from the learner. This method should
        not be overriden. Instead, override the _forward_train method.

        Args:
            batch: The input batch. This input batch should comply with
                input_specs_train().
            **kwargs: Additional keyword arguments.

        Returns:
            The output of the forward pass. This output should comply with the
            output_specs_train().
        """
        if self.config.inference_only:
            raise RuntimeError(
                "Calling `forward_train` on an inference_only module is not allowed! "
                "Set the `inference_only=False` flag in the RLModule's config when "
                "building the module."
            )
        return self._forward_train(batch, **kwargs)

    @abc.abstractmethod
    def _forward_train(self, batch: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        """Forward-pass during training. See forward_train for details."""

    @OverrideToImplementCustomLogic
    @override(Checkpointable)
    def get_state(
        self,
        components: Optional[Union[str, Collection[str]]] = None,
        *,
        not_components: Optional[Union[str, Collection[str]]] = None,
        inference_only: bool = False,
        **kwargs,
    ) -> StateDict:
        """Returns the state dict of the module.

        Args:
            inference_only: Whether the returned state should be an inference-only
                state (w/o those model components that are not needed for action
                computations, such as a value function or a target network).
                Note that setting this to `False` might raise an error if
                `self.config.inference_only` is True.

        Returns:
            This RLModule's state dict.
        """
        if components is not None or not_components is not None:
            raise ValueError(
                "`component` arg and `not_component` arg not supported in "
                "`RLModule.get_state()` base implementation! Override this method in "
                "your custom RLModule subclass."
            )
        return {}

    @OverrideToImplementCustomLogic
    @override(Checkpointable)
    def set_state(self, state: StateDict) -> None:
        pass

    @override(Checkpointable)
    def get_ctor_args_and_kwargs(self):
        return (
            (self.config,),  # *args
            {},  # **kwargs
        )

    def as_multi_agent(self) -> "MultiAgentRLModule":
        """Returns a multi-agent wrapper around this module."""
        from ray.rllib.core.rl_module.marl_module import MultiAgentRLModule

        marl_module = MultiAgentRLModule()
        marl_module.add_module(DEFAULT_MODULE_ID, self)
        return marl_module

    def unwrapped(self) -> "RLModule":
        """Returns the underlying module if this module is a wrapper.

        An example of a wrapped is the TorchDDPRLModule class, which wraps
        a TorchRLModule.

        Returns:
            The underlying module.
        """
        return self

    @Deprecated(new="RLModule.save_to_path(...)", error=True)
    def save_state(self, *args, **kwargs):
        pass

    @Deprecated(new="RLModule.restore_from_path(...)", error=True)
    def load_state(self, *args, **kwargs):
        pass

    @Deprecated(new="RLModule.save_to_path(...)", error=True)
    def save_to_checkpoint(self, *args, **kwargs):
        pass
