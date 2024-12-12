import abc
import dataclasses
from dataclasses import dataclass, field
import logging
from typing import Any, Collection, Dict, Optional, Type, TYPE_CHECKING, Union

import gymnasium as gym

from ray.rllib.core import DEFAULT_MODULE_ID
from ray.rllib.core.columns import Columns
from ray.rllib.core.models.specs.typing import SpecType
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.models.distributions import Distribution
from ray.rllib.utils.annotations import (
    override,
    OverrideToImplementCustomLogic,
)
from ray.rllib.utils.checkpoints import Checkpointable
from ray.rllib.utils.deprecation import (
    Deprecated,
    DEPRECATED_VALUE,
    deprecation_warning,
)
from ray.rllib.utils.serialization import (
    gym_space_from_dict,
    gym_space_to_dict,
    serialize_type,
    deserialize_type,
)
from ray.rllib.utils.typing import StateDict
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.rllib.core.rl_module.multi_rl_module import (
        MultiRLModule,
        MultiRLModuleSpec,
    )
    from ray.rllib.core.models.catalog import Catalog

logger = logging.getLogger("ray.rllib")


@PublicAPI(stability="alpha")
@dataclass
class RLModuleSpec:
    """Utility spec class to make constructing RLModules (in single-agent case) easier.

    Args:
        module_class: The RLModule class to use.
        observation_space: The observation space of the RLModule. This may differ
            from the observation space of the environment. For example, a discrete
            observation space of an environment, would usually correspond to a
            one-hot encoded observation space of the RLModule because of preprocessing.
        action_space: The action space of the RLModule.
        inference_only: Whether the RLModule should be configured in its inference-only
            state, in which those components not needed for action computing (for
            example a value function or a target network) might be missing.
            Note that `inference_only=True` AND `learner_only=True` is not allowed.
        learner_only: Whether this RLModule should only be built on Learner workers, but
            NOT on EnvRunners. Useful for RLModules inside a MultiRLModule that are only
            used for training, for example a shared value function in a multi-agent
            setup or a world model in a curiosity-learning setup.
            Note that `inference_only=True` AND `learner_only=True` is not allowed.
        model_config: The model config dict or default RLlib dataclass to use.
        catalog_class: The Catalog class to use.
        load_state_path: The path to the module state to load from. NOTE: This must be
            an absolute path.
    """

    module_class: Optional[Type["RLModule"]] = None
    observation_space: Optional[gym.Space] = None
    action_space: Optional[gym.Space] = None
    inference_only: bool = False
    learner_only: bool = False
    model_config: Optional[Union[Dict[str, Any], DefaultModelConfig]] = None
    catalog_class: Optional[Type["Catalog"]] = None
    load_state_path: Optional[str] = None

    # Deprecated field.
    model_config_dict: Optional[Union[dict, int]] = None

    def __post_init__(self):
        if self.model_config_dict is not None:
            deprecation_warning(
                old="RLModuleSpec(model_config_dict=..)",
                new="RLModuleSpec(model_config=..)",
                error=True,
            )

    def build(self) -> "RLModule":
        """Builds the RLModule from this spec."""
        if self.module_class is None:
            raise ValueError("RLModule class is not set.")
        if self.observation_space is None:
            raise ValueError("Observation space is not set.")
        if self.action_space is None:
            raise ValueError("Action space is not set.")

        try:
            module = self.module_class(
                observation_space=self.observation_space,
                action_space=self.action_space,
                inference_only=self.inference_only,
                model_config=self._get_model_config(),
                catalog_class=self.catalog_class,
            )
        # Older custom model might still require the old `RLModuleConfig` under
        # the `config` arg.
        except AttributeError:
            module_config = self.get_rl_module_config()
            module = self.module_class(module_config)
        return module

    @classmethod
    def from_module(cls, module: "RLModule") -> "RLModuleSpec":
        from ray.rllib.core.rl_module.multi_rl_module import MultiRLModule

        if isinstance(module, MultiRLModule):
            raise ValueError("MultiRLModule cannot be converted to RLModuleSpec.")

        # Try instantiating a new RLModule from the spec using the new c'tor args.
        try:
            rl_module_spec = RLModuleSpec(
                module_class=type(module),
                observation_space=module.observation_space,
                action_space=module.action_space,
                inference_only=module.inference_only,
                learner_only=module.learner_only,
                model_config=module.model_config,
                catalog_class=(
                    type(module.catalog) if module.catalog is not None else None
                ),
            )

        # Old path through deprecated `RLModuleConfig` class. Used only if `module`
        # still has a valid `config` attribute.
        except AttributeError:
            rl_module_spec = RLModuleSpec(
                module_class=type(module),
                observation_space=module.config.observation_space,
                action_space=module.config.action_space,
                inference_only=module.config.inference_only,
                learner_only=module.config.learner_only,
                model_config=module.config.model_config_dict,
                catalog_class=module.config.catalog_class,
            )
        return rl_module_spec

    def to_dict(self):
        """Returns a serialized representation of the spec."""
        return {
            "module_class": serialize_type(self.module_class),
            "observation_space": gym_space_to_dict(self.observation_space),
            "action_space": gym_space_to_dict(self.action_space),
            "inference_only": self.inference_only,
            "learner_only": self.learner_only,
            "model_config": self._get_model_config(),
            "catalog_class": serialize_type(self.catalog_class)
            if self.catalog_class is not None
            else None,
        }

    @classmethod
    def from_dict(cls, d):
        """Returns a single agent RLModule spec from a serialized representation."""
        module_class = deserialize_type(d["module_class"])
        try:
            spec = RLModuleSpec(
                module_class=module_class,
                observation_space=gym_space_from_dict(d["observation_space"]),
                action_space=gym_space_from_dict(d["action_space"]),
                inference_only=d["inference_only"],
                learner_only=d["learner_only"],
                model_config=d["model_config"],
                catalog_class=deserialize_type(d["catalog_class"])
                if d["catalog_class"] is not None
                else None,
            )

        # Old path through deprecated `RLModuleConfig` class.
        except KeyError:
            module_config = RLModuleConfig.from_dict(d["module_config"])
            spec = RLModuleSpec(
                module_class=module_class,
                observation_space=module_config.observation_space,
                action_space=module_config.action_space,
                inference_only=module_config.inference_only,
                learner_only=module_config.learner_only,
                model_config=module_config.model_config_dict,
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
        if not isinstance(other, RLModuleSpec):
            raise ValueError("Can only update with another RLModuleSpec.")

        # If the field is None in the other, keep the current field, otherwise update
        # with the new value.
        if override:
            self.module_class = other.module_class or self.module_class
            self.observation_space = other.observation_space or self.observation_space
            self.action_space = other.action_space or self.action_space
            self.inference_only = other.inference_only or self.inference_only
            self.learner_only = other.learner_only and self.learner_only
            self.model_config = other.model_config or self.model_config
            self.catalog_class = other.catalog_class or self.catalog_class
            self.load_state_path = other.load_state_path or self.load_state_path
        # Only override, if the field is None in `self`.
        # Do NOT override the boolean settings: `inference_only` and `learner_only`.
        else:
            self.module_class = self.module_class or other.module_class
            self.observation_space = self.observation_space or other.observation_space
            self.action_space = self.action_space or other.action_space
            self.model_config = self.model_config or other.model_config
            self.catalog_class = self.catalog_class or other.catalog_class
            self.load_state_path = self.load_state_path or other.load_state_path

    def as_multi_rl_module_spec(self) -> "MultiRLModuleSpec":
        """Returns a MultiRLModuleSpec (`self` under DEFAULT_MODULE_ID key)."""
        from ray.rllib.core.rl_module.multi_rl_module import MultiRLModuleSpec

        return MultiRLModuleSpec(
            rl_module_specs={DEFAULT_MODULE_ID: self},
            load_state_path=self.load_state_path,
        )

    def _get_model_config(self):
        return (
            dataclasses.asdict(self.model_config)
            if dataclasses.is_dataclass(self.model_config)
            else (self.model_config or {})
        )

    @Deprecated(
        new="RLModule(*, observation_space=.., action_space=.., ....)",
        error=False,
    )
    def get_rl_module_config(self):
        return RLModuleConfig(
            observation_space=self.observation_space,
            action_space=self.action_space,
            inference_only=self.inference_only,
            learner_only=self.learner_only,
            model_config_dict=self._get_model_config(),
            catalog_class=self.catalog_class,
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
        module_spec = RLModuleSpec(
            module_class=PPOTorchRLModule,
            observation_space=env.observation_space,
            action_space=env.action_space,
            model_config=DefaultModelConfig(fcnet_hiddens=[128, 128]),
            catalog_class=PPOCatalog,
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
        module_spec = RLModuleSpec(
            module_class=PPOTorchRLModule,
            observation_space=env.observation_space,
            action_space=env.action_space,
            model_config=DefaultModelConfig(fcnet_hiddens=[128, 128]),
            catalog_class=PPOCatalog,
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
        module_spec = RLModuleSpec(
            module_class=PPOTorchRLModule,
            observation_space=env.observation_space,
            action_space=env.action_space,
            model_config=DefaultModelConfig(fcnet_hiddens=[128, 128]),
            catalog_class=PPOCatalog,
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

    def __init__(
        self,
        config=DEPRECATED_VALUE,
        *,
        observation_space: Optional[gym.Space] = None,
        action_space: Optional[gym.Space] = None,
        inference_only: Optional[bool] = None,
        learner_only: bool = False,
        model_config: Optional[Union[dict, DefaultModelConfig]] = None,
        catalog_class=None,
    ):
        # TODO (sven): Deprecate Catalog and replace with utility functions to create
        #  primitive components based on obs- and action spaces.
        self.catalog = None
        self._catalog_ctor_error = None

        # Deprecated
        self.config = config
        if self.config != DEPRECATED_VALUE:
            deprecation_warning(
                old="RLModule(config=[RLModuleConfig])",
                new="RLModule(observation_space=.., action_space=.., inference_only=..,"
                " learner_only=.., model_config=..)",
                help="See https://github.com/ray-project/ray/blob/master/rllib/examples/rl_modules/custom_cnn_rl_module.py "  # noqa
                "for how to write a custom RLModule.",
                error=True,
            )
        else:
            self.observation_space = observation_space
            self.action_space = action_space
            self.inference_only = inference_only
            self.learner_only = learner_only
            self.model_config = model_config
            try:
                self.catalog = catalog_class(
                    observation_space=self.observation_space,
                    action_space=self.action_space,
                    model_config_dict=self.model_config,
                )
            except Exception as e:
                logger.warning(
                    "Could not create a Catalog object for your RLModule! If you are "
                    "not using the new API stack yet, make sure to switch it off in "
                    "your config: `config.api_stack(enable_rl_module_and_learner=False"
                    ", enable_env_runner_and_connector_v2=False)`. Some algos already "
                    "use the new stack by default. Ignore this message, if your "
                    "RLModule does not use a Catalog to build its sub-components."
                )
                self._catalog_ctor_error = e

        # TODO (sven): Deprecate this. We keep it here for now in case users
        #  still have custom models (or subclasses of RLlib default models)
        #  into which they pass in a `config` argument.
        self.config = RLModuleConfig(
            observation_space=self.observation_space,
            action_space=self.action_space,
            inference_only=self.inference_only,
            learner_only=self.learner_only,
            model_config_dict=self.model_config,
            catalog_class=catalog_class,
        )

        self.action_dist_cls = None
        if self.catalog is not None:
            self.action_dist_cls = self.catalog.get_action_dist_cls(
                framework=self.framework
            )

        # Make sure, `setup()` is only called once, no matter what.
        if hasattr(self, "_is_setup") and self._is_setup:
            raise RuntimeError(
                "`RLModule.setup()` called twice within your RLModule implementation "
                f"{self}! Make sure you are using the proper inheritance order "
                "(TorchRLModule before [Algo]RLModule) or (TfRLModule before "
                "[Algo]RLModule) and that you are NOT overriding the constructor, but "
                "only the `setup()` method of your subclass."
            )
        self.setup()
        self._is_setup = True

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
    def _forward(self, batch: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        """Generic forward pass method, used in all phases of training and evaluation.

        If you need a more nuanced distinction between forward passes in the different
        phases of training and evaluation, override the following methods instead:
        For distinct action computation logic w/o exploration, override the
        `self._forward_inference()` method.
        For distinct action computation logic with exploration, override the
        `self._forward_exploration()` method.
        For distinct forward pass logic before loss computation, override the
        `self._forward_train()` method.

        Args:
            batch: The input batch.
            **kwargs: Additional keyword arguments.

        Returns:
            The output of the forward pass.
        """
        return {}

    def forward_inference(self, batch: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        """DO NOT OVERRIDE! Forward-pass during evaluation, called from the sampler.

        This method should not be overridden. Override the `self._forward_inference()`
        method instead.

        Args:
            batch: The input batch. This input batch should comply with
                input_specs_inference().
            **kwargs: Additional keyword arguments.

        Returns:
            The output of the forward pass. This output should comply with the
            ouptut_specs_inference().
        """
        return self._forward_inference(batch, **kwargs)

    @OverrideToImplementCustomLogic
    def _forward_inference(self, batch: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        """Forward-pass used for action computation without exploration behavior.

        Override this method only, if you need specific behavior for non-exploratory
        action computation behavior. If you have only one generic behavior for all
        phases of training and evaluation, override `self._forward()` instead.

        By default, this calls the generic `self._forward()` method.
        """
        return self._forward(batch, **kwargs)

    def forward_exploration(self, batch: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        """DO NOT OVERRIDE! Forward-pass during exploration, called from the sampler.

        This method should not be overridden. Override the `self._forward_exploration()`
        method instead.

        Args:
            batch: The input batch. This input batch should comply with
                input_specs_exploration().
            **kwargs: Additional keyword arguments.

        Returns:
            The output of the forward pass. This output should comply with the
            output_specs_exploration().
        """
        return self._forward_exploration(batch, **kwargs)

    @OverrideToImplementCustomLogic
    def _forward_exploration(self, batch: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        """Forward-pass used for action computation with exploration behavior.

        Override this method only, if you need specific behavior for exploratory
        action computation behavior. If you have only one generic behavior for all
        phases of training and evaluation, override `self._forward()` instead.

        By default, this calls the generic `self._forward()` method.
        """
        return self._forward(batch, **kwargs)

    def forward_train(self, batch: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        """DO NOT OVERRIDE! Forward-pass during training called from the learner.

        This method should not be overridden. Override the `self._forward_train()`
        method instead.

        Args:
            batch: The input batch. This input batch should comply with
                input_specs_train().
            **kwargs: Additional keyword arguments.

        Returns:
            The output of the forward pass. This output should comply with the
            output_specs_train().
        """
        if self.inference_only:
            raise RuntimeError(
                "Calling `forward_train` on an inference_only module is not allowed! "
                "Set the `inference_only=False` flag in the RLModule's config when "
                "building the module."
            )
        return self._forward_train(batch, **kwargs)

    @OverrideToImplementCustomLogic
    def _forward_train(self, batch: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        """Forward-pass used before the loss computation (training).

        Override this method only, if you need specific behavior and outputs for your
        loss computations. If you have only one generic behavior for all
        phases of training and evaluation, override `self._forward()` instead.

        By default, this calls the generic `self._forward()` method.
        """
        return self._forward(batch, **kwargs)

    @OverrideToImplementCustomLogic
    def get_initial_state(self) -> Any:
        """Returns the initial state of the RLModule, in case this is a stateful module.

        Returns:
            A tensor or any nested struct of tensors, representing an initial state for
            this (stateful) RLModule.
        """
        return {}

    @OverrideToImplementCustomLogic
    def is_stateful(self) -> bool:
        """By default, returns False if the initial state is an empty dict (or None).

        By default, RLlib assumes that the module is non-recurrent, if the initial
        state is an empty dict and recurrent otherwise.
        This behavior can be customized by overriding this method.
        """
        initial_state = self.get_initial_state()
        assert isinstance(initial_state, dict), (
            "The initial state of an RLModule must be a dict, but is "
            f"{type(initial_state)} instead."
        )
        return bool(initial_state)

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
                `self.inference_only` is True.

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
            (),  # *args
            {
                "observation_space": self.observation_space,
                "action_space": self.action_space,
                "inference_only": self.inference_only,
                "learner_only": self.learner_only,
                "model_config": self.model_config,
                "catalog_class": (
                    type(self.catalog) if self.catalog is not None else None
                ),
            },  # **kwargs
        )

    def as_multi_rl_module(self) -> "MultiRLModule":
        """Returns a multi-agent wrapper around this module."""
        from ray.rllib.core.rl_module.multi_rl_module import MultiRLModule

        multi_rl_module = MultiRLModule(
            rl_module_specs={DEFAULT_MODULE_ID: RLModuleSpec.from_module(self)}
        )
        return multi_rl_module

    def unwrapped(self) -> "RLModule":
        """Returns the underlying module if this module is a wrapper.

        An example of a wrapped is the TorchDDPRLModule class, which wraps
        a TorchRLModule.

        Returns:
            The underlying module.
        """
        return self

    @Deprecated(new="RLModule.as_multi_rl_module()", error=True)
    def as_multi_agent(self, *args, **kwargs):
        pass

    @Deprecated(new="RLModule.save_to_path(...)", error=True)
    def save_state(self, *args, **kwargs):
        pass

    @Deprecated(new="RLModule.restore_from_path(...)", error=True)
    def load_state(self, *args, **kwargs):
        pass

    @Deprecated(new="RLModule.save_to_path(...)", error=True)
    def save_to_checkpoint(self, *args, **kwargs):
        pass

    def output_specs_inference(self) -> SpecType:
        return [Columns.ACTION_DIST_INPUTS]

    def output_specs_exploration(self) -> SpecType:
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


@Deprecated(
    old="RLModule(config=[RLModuleConfig object])",
    new="RLModule(observation_space=.., action_space=.., inference_only=.., "
    "model_config=.., catalog_class=..)",
    error=False,
)
@dataclass
class RLModuleConfig:
    observation_space: gym.Space = None
    action_space: gym.Space = None
    inference_only: bool = False
    learner_only: bool = False
    model_config_dict: Dict[str, Any] = field(default_factory=dict)
    catalog_class: Type["Catalog"] = None

    def get_catalog(self) -> Optional["Catalog"]:
        if self.catalog_class is not None:
            return self.catalog_class(
                observation_space=self.observation_space,
                action_space=self.action_space,
                model_config_dict=self.model_config_dict,
            )
        return None

    def to_dict(self):
        catalog_class_path = (
            serialize_type(self.catalog_class) if self.catalog_class else ""
        )
        return {
            "observation_space": gym_space_to_dict(self.observation_space),
            "action_space": gym_space_to_dict(self.action_space),
            "inference_only": self.inference_only,
            "learner_only": self.learner_only,
            "model_config_dict": self.model_config_dict,
            "catalog_class_path": catalog_class_path,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]):
        catalog_class = (
            None
            if d["catalog_class_path"] == ""
            else deserialize_type(d["catalog_class_path"])
        )
        return cls(
            observation_space=gym_space_from_dict(d["observation_space"]),
            action_space=gym_space_from_dict(d["action_space"]),
            inference_only=d["inference_only"],
            learner_only=d["learner_only"],
            model_config_dict=d["model_config_dict"],
            catalog_class=catalog_class,
        )
