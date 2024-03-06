import abc
from dataclasses import dataclass
import datetime
import logging
import json
import os
import pathlib
import tempfile
from typing import Any, Dict, List, Optional, Type, TYPE_CHECKING, Union

import gymnasium as gym
import tree

import ray
from ray import cloudpickle as pickle
from ray.rllib.core.columns import Columns
from ray.rllib.policy.policy import get_gym_space_from_struct_of_tensors
from ray.rllib.policy.view_requirement import ViewRequirement

from ray.rllib.core.models.specs.typing import SpecType
from ray.rllib.core.models.specs.checker import (
    check_input_specs,
    check_output_specs,
    convert_to_canonical_format,
)
from ray.rllib.models.distributions import Distribution
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID, SampleBatch
from ray.rllib.utils.annotations import (
    ExperimentalAPI,
    OldAPIStack,
    OverrideToImplementCustomLogic,
)
from ray.rllib.utils.deprecation import Deprecated
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.serialization import (
    gym_space_from_dict,
    gym_space_to_dict,
    serialize_type,
    deserialize_type,
)
from ray.rllib.utils.typing import SampleBatchType, ViewRequirementsDict
from ray.train import Checkpoint

if TYPE_CHECKING:
    from ray.rllib.core.rl_module.marl_module import (
        MultiAgentRLModule,
        MultiAgentRLModuleSpec,
    )
    from ray.rllib.core.models.catalog import Catalog


logger = logging.getLogger("ray.rllib")

RLMODULE_METADATA_FILE_NAME = "rl_module_metadata.json"
RLMODULE_METADATA_RAY_VERSION_KEY = "ray_version"
RLMODULE_METADATA_RAY_COMMIT_HASH_KEY = "ray_commit_hash"
RLMODULE_METADATA_CHECKPOINT_DATE_TIME_KEY = "checkpoint_date_time"
RLMODULE_SPEC_FILE_NAME = "rl_module_spec.pkl"
RLMODULE_STATE_FILE_OR_DIR_NAME = "rl_module_state"


@ExperimentalAPI
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
        model_config_dict: The model config dict to use.
        catalog_class: The Catalog class to use.
        load_state_path: The path to the module state to load from. NOTE: This must be
            an absolute path.
    """

    module_class: Optional[Type["RLModule"]] = None
    observation_space: Optional[gym.Space] = None
    action_space: Optional[gym.Space] = None
    model_config_dict: Optional[Dict[str, Any]] = None
    catalog_class: Optional[Type["Catalog"]] = None
    load_state_path: Optional[str] = None

    def get_rl_module_config(self) -> "RLModuleConfig":
        """Returns the RLModule config for this spec."""
        return RLModuleConfig(
            observation_space=self.observation_space,
            action_space=self.action_space,
            model_config_dict=self.model_config_dict,
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
        if self.model_config_dict is None:
            raise ValueError("Model config is not set.")

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
        observation_space = module_config.observation_space
        action_space = module_config.action_space
        model_config_dict = module_config.model_config_dict
        catalog_class = module_config.catalog_class

        spec = SingleAgentRLModuleSpec(
            module_class=module_class,
            observation_space=observation_space,
            action_space=action_space,
            model_config_dict=model_config_dict,
            catalog_class=catalog_class,
        )
        return spec

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
        self.load_state_path = other.load_state_path or self.load_state_path

    def as_multi_agent(self) -> "MultiAgentRLModuleSpec":
        """Returns a MultiAgentRLModuleSpec (`self` under DEFAULT_POLICY_ID key)."""
        from ray.rllib.core.rl_module.marl_module import MultiAgentRLModuleSpec

        return MultiAgentRLModuleSpec(
            module_specs={DEFAULT_POLICY_ID: self},
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
        model_config_dict: The model config dict to use.
        catalog_class: The Catalog class to use.
    """

    observation_space: gym.Space = None
    action_space: gym.Space = None
    model_config_dict: Dict[str, Any] = None
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
            model_config_dict=d["model_config_dict"],
            catalog_class=catalog_class,
        )


@ExperimentalAPI
class RLModule(abc.ABC):
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
        :py:meth:`~_forward_train`: Forward pass during training.

        :py:meth:`~_forward_exploration`: Forward pass during training for exploration.

        :py:meth:`~_forward_inference`: Forward pass during inference.


    Note:
        There is a reason that the specs are not written as abstract properties.
        The reason is that torch overrides `__getattr__` and `__setattr__`. This means
        that if we define the specs as properties, then any error in the property will
        be interpreted as a failure to retrieve the attribute and will invoke
        `__getattr__` which will give a confusing error about the attribute not found.
        More details here: https://github.com/pytorch/pytorch/issues/49726.
    """

    framework: str = None

    @classmethod
    def from_checkpoint(
        cls,
        checkpoint: Union[str, pathlib.Path, Checkpoint],
        rl_module_spec: Optional[
            Union["MultiAgentRLModuleSpec", "SingleAgentRLModuleSpec"]
        ] = None,
    ) -> "RLModule":
        """Loads the module from a checkpoint directory.

        Args:
            checkpoint_dir_path: The directory to load the checkpoint from.
            rl_module_spec: An optional RLModuleSpec (single- or multi-agent) to use
                instead of the saved/pickled one in `rl_module_spec`.
        """
        # `checkpoint` is a Checkpoint instance: Translate to directory and continue.
        if isinstance(checkpoint, Checkpoint):
            checkpoint: str = checkpoint.to_directory()

        path = pathlib.Path(checkpoint)
        if not path.exists():
            raise ValueError(
                "While running `from_checkpoint()` there was no directory found at "
                f"{path}!"
            )
        if not path.is_dir():
            raise ValueError(
                f"While running `from_checkpoint()` the provided path ({path}) was not "
                "a directory!"
            )
        # Load and log metadata.
        metadata_path = path / RLMODULE_METADATA_FILE_NAME
        with open(metadata_path, "r") as f:
            metadata = json.load(f)
            logger.info(f"Loading checkpoint with metadata: {metadata}")

        # If not provided, try to load the spec (class, spaces, config).
        if rl_module_spec is None:
            rl_module_spec_file = path / RLMODULE_SPEC_FILE_NAME
            with open(rl_module_spec_file, "rb") as f:
                rl_module_spec = pickle.load(f)

        # Build a new RLModule that matches the saved one.
        module: RLModule = rl_module_spec.build()
        # Overwrite the new Module's state with the checkpointed one.
        module.restore(checkpoint_path=path)

        # Return the new Module.
        return module

    def save(self, path: Optional[Union[str, pathlib.Path]] = None) -> Checkpoint:
        """Saves this RLModule's entire state to the given `path` dir as a Checkpoint.

        With the created checkpoint dir, a new RLModule with the exact same state can
        be recreated, even without the original config/spec present.

        .. testcode::



        Args:
            path: The directory to save all the information of this RLModule into and
                return a Checkpoint object for.
        """
        if path is not None:
            os.makedirs(path, exist_ok=True)
        else:
            path = tempfile.mkdtemp()
        path = pathlib.Path(path)

        # Write the spec to file.
        rl_module_spec = SingleAgentRLModuleSpec.from_module(self)
        with open(path / RLMODULE_SPEC_FILE_NAME, "wb") as f:
            pickle.dump(rl_module_spec, f)

        # Write the metadata to file.
        metadata = {
            RLMODULE_METADATA_RAY_VERSION_KEY: ray.__version__,
            RLMODULE_METADATA_RAY_COMMIT_HASH_KEY: ray.__commit__,
            RLMODULE_METADATA_CHECKPOINT_DATE_TIME_KEY: (
                datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S GMT")
            )
        }
        metadata_file = path / RLMODULE_METADATA_FILE_NAME
        with open(metadata_file, "w") as f:
            json.dump(metadata, f)

        # Write the RLModule state to file.
        self._save_state(path / (RLMODULE_STATE_FILE_OR_DIR_NAME + ".pt"))
        return Checkpoint(path)

    def restore(self, checkpoint_path: Union[str, pathlib.Path, Checkpoint]) -> None:
        """Loads the state/weights of an RLModule from the directory dir.

        Args:
            checkpoint_path: The directory to load the checkpoint from.
        """
        path = self._checkpoint_to_path(checkpoint_path)
        module_state_file = path / (RLMODULE_STATE_FILE_OR_DIR_NAME + ".pt")
        self._load_state(module_state_file)

    @staticmethod
    def _checkpoint_to_path(
        checkpoint_or_path: Union[Checkpoint, str, pathlib.Path],
    ) -> pathlib.Path:
        # `checkpoint` is a Checkpoint instance: Translate to directory and continue.
        if isinstance(checkpoint_or_path, Checkpoint):
            checkpoint_or_path: str = checkpoint_or_path.to_directory()

        path = pathlib.Path(checkpoint_or_path)
        if not path.exists():
            raise ValueError("No directory found at {path}!")
        if not path.is_dir():
            raise ValueError(f"Provided path ({path}) is not a directory!")
        return path

    #END common mixin class methods

    @OverrideToImplementCustomLogic
    def _save_state(self, state_file) -> None:
        """Default implementation for saving this RLModule's state to disk."""
        with open(state_file, "wb") as f:
            pickle.dump(self.get_state(), f)

    @OverrideToImplementCustomLogic
    def _load_state(self, state_file) -> None:
        """Default implementation for loading this RLModule's state from disk."""
        with open(state_file, "rb") as f:
            state = pickle.load(f)
        self.set_state(state)

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

    @OverrideToImplementCustomLogic
    def setup(self):
        """Sets up the components of the module.

        This is called automatically during the __init__ method of this class,
        therefore, the subclass should call super.__init__() in its constructor. This
        abstraction can be used to create any component that your RLModule needs.
        """
        pass

    @OverrideToImplementCustomLogic
    def get_inference_action_dist_cls(self) -> Type[Distribution]:
        """Returns the action distribution class for this RLModule used for inference.

        This class is used to create action distributions from outputs of the forward
        inference method. If the case that no action distribution class is needed,
        this method can return None.

        Note that RLlib's distribution classes all implement the `Distribution`
        interface. This requires two special methods: `Distribution.from_logits()` and
        `Distribution.to_deterministic()`. See the documentation for `Distribution`
        for more detail.
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
        `Distribution.to_deterministic()`. See the documentation for `Distribution`
        for more detail.
        """
        raise NotImplementedError

    @OverrideToImplementCustomLogic
    def get_train_action_dist_cls(self) -> Type[Distribution]:
        """Returns the action distribution class for this RLModule used for training.

        This class is used to create action distributions from outputs of the
        forward_train method. If the case that no action distribution class is needed,
        this method can return None.

        Note that RLlib's distribution classes all implement the `Distribution`
        interface. This requires two special methods: `Distribution.from_logits()` and
        `Distribution.to_deterministic()`. See the documentation for `Distribution`
        for more detail.
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
    def input_specs_inference(self) -> SpecType:
        """Returns the input specs of the forward_inference method."""
        return self._default_input_specs()

    @OverrideToImplementCustomLogic
    def input_specs_exploration(self) -> SpecType:
        """Returns the input specs of the forward_exploration method."""
        return self._default_input_specs()

    @OverrideToImplementCustomLogic
    def input_specs_train(self) -> SpecType:
        """Returns the input specs of the forward_train method."""
        return self._default_input_specs()

    @OverrideToImplementCustomLogic
    def output_specs_inference(self) -> SpecType:
        """Returns the output specs of the `forward_inference()` method.

        Override this method to customize the output specs of the inference call.
        The default implementation requires the `forward_inference()` method to return
        a dict that has `action_dist` key and its value is an instance of
        `Distribution`.
        """
        # TODO (sven): We should probably change this to [ACTION_DIST_INPUTS], b/c this
        #  is what most algos will do.
        return {"action_dist": Distribution}

    @OverrideToImplementCustomLogic
    def output_specs_exploration(self) -> SpecType:
        """Returns the output specs of the `forward_exploration()` method.

        Override this method to customize the output specs of the exploration call.
        The default implementation requires the `forward_exploration()` method to return
        a dict that has `action_dist` key and its value is an instance of
        `Distribution`.
        """
        # TODO (sven): We should probably change this to [ACTION_DIST_INPUTS], b/c this
        #  is what most algos will do.
        return {"action_dist": Distribution}

    @OverrideToImplementCustomLogic
    def output_specs_train(self) -> SpecType:
        """Returns the output specs of the forward_train method."""
        return {}

    @OverrideToImplementCustomLogic
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
    def _forward_inference(self, batch: NestedDict, **kwargs) -> Dict[str, Any]:
        """Forward-pass during evaluation. See forward_inference for details."""

    @check_input_specs("_input_specs_exploration")
    @check_output_specs("_output_specs_exploration")
    def forward_exploration(
        self, batch: SampleBatchType, **kwargs
    ) -> Dict[str, Any]:
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
    def _forward_exploration(self, batch: NestedDict, **kwargs) -> Dict[str, Any]:
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
        return self._forward_train(batch, **kwargs)

    @abc.abstractmethod
    def _forward_train(self, batch: NestedDict, **kwargs) -> Dict[str, Any]:
        """Forward-pass during training. See forward_train for details."""

    @OverrideToImplementCustomLogic
    def get_state(
        self,
        components: Optional[Union[str, List[str]]] = None,
    ) -> Dict[str, Any]:
        """Returns the state dict of this RLModule.

        Args:
            components: An optional filter for the resulting dict to only contain those
                keys/components specified.

        Returns:
            The (possibly `components` filtered) state dict.
        """
        return {}

    @OverrideToImplementCustomLogic
    def set_state(self, state: Dict[str, Any]) -> None:
        """Sets the state dict of this RLModule.

        Note that if the given `state` does not contain certain keys, the RLModule
        may tolerate this and only set those components of its state specified in
        `state`.
        """
        pass

    def as_multi_agent(self) -> "MultiAgentRLModule":
        """Returns a multi-agent wrapper around this module."""
        from ray.rllib.core.rl_module.marl_module import MultiAgentRLModule

        marl_module = MultiAgentRLModule()
        marl_module.add_module(DEFAULT_POLICY_ID, self)
        return marl_module

    def unwrapped(self) -> "RLModule":
        """Returns the underlying module if this module is a wrapper.

        An example of a wrapped is the TorchDDPRLModule class, which wraps
        a TorchRLModule.

        Returns:
            The underlying module.
        """
        return self

    @OldAPIStack
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

    @Deprecated(new="save", error=True)
    def save_state(self, *args, **kwargs):
        pass

    @Deprecated(new="load", error=True)
    def load_state(self, *args, **kwargs):
        pass

    @Deprecated(new="save", error=True)
    def save_to_checkpoint(self, *args, **kwargs):
        pass
