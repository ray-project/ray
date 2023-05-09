import abc
from dataclasses import dataclass
import datetime
import gymnasium as gym
import json
import pathlib
from typing import Any, Dict, Mapping, Optional, Type, TYPE_CHECKING, Union

if TYPE_CHECKING:
    from ray.rllib.core.rl_module.marl_module import (
        MultiAgentRLModule,
        MultiAgentRLModuleSpec,
    )
    from ray.rllib.core.models.catalog import Catalog

import ray
from ray.rllib.utils.annotations import (
    ExperimentalAPI,
    OverrideToImplementCustomLogic_CallToSuperRecommended,
)

from ray.rllib.core.models.specs.typing import SpecType
from ray.rllib.core.models.specs.checker import (
    check_input_specs,
    check_output_specs,
    convert_to_canonical_format,
)
from ray.rllib.models.distributions import Distribution
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID, SampleBatch
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.utils.typing import SampleBatchType
from ray.rllib.utils.serialization import (
    gym_space_from_dict,
    gym_space_to_dict,
    serialize_type,
    deserialize_type,
)


ModuleID = str
RLMODULE_METADATA_FILE_NAME = "rl_module_metadata.json"
RLMODULE_METADATA_SPEC_CLASS_KEY = "module_spec_class"
RLMODULE_METADATA_SPEC_KEY = "module_spec_dict"
RLMODULE_STATE_DIR_NAME = "module_state_path"
RLMODULE_METADATA_RAY_VERSION_KEY = "ray_version"
RLMODULE_METADATA_RAY_COMMIT_HASH_KEY = "ray_commit_hash"
RLMODULE_METADATA_CHECKPOINT_DATE_TIME_KEY = "checkpoint_date_time"


@ExperimentalAPI
@dataclass
class SingleAgentRLModuleSpec:
    """A utility spec class to make it constructing RLModules (in single-agent case) easier.

    Args:
        module_class: The RLModule class to use.
        observation_space: The observation space of the RLModule. This may differ
            from the observation space of the environment. For example, a discrete
            observation space of an environment, would usually correspond to a
            one-hot encoded observation space of the RLModule because of preprocessing.
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

    During Training (acting in env from each rollout worker):

    .. code-block:: python

        module = RLModule(...)
        obs, info = env.reset()

        while not env.terminated:
            fwd_outputs = module.forward_exploration({"obs": obs})
            # this can be either deterministic or stochastic distribution
            action = fwd_outputs["action_dist"].sample()
            obs, reward, terminated, truncated, info = env.step(action)



    During Training (learning the policy)

    .. code-block:: python

        module = RLModule(...)
        fwd_ins = {"obs": obs, "action": action, "reward": reward, "next_obs": next_obs}
        fwd_outputs = module.forward_train(fwd_ins)
        loss = compute_loss(fwd_outputs, fwd_ins)
        update_params(module, loss)

    During Inference (acting in env during evaluation)

    .. code-block:: python

        module = RLModule(...)
        obs, info = env.reset()

        while not env.terminated:
            fwd_outputs = module.forward_inference({"obs": obs})
            action = fwd_outputs["action_dist"].sample()
            obs, reward, terminated, truncated, info = env.step(action)

    Args:
        config: The config for the RLModule.

    Abstract Methods:
        :py:meth:`~forward_train`: Forward pass during training.

        :py:meth:`~forward_exploration`: Forward pass during training for exploration.

        :py:meth:`~forward_inference`: Forward pass during inference.


    Note:
        There is a reason that the specs are not written as abstract properties.
        The reason is that torch overrides `__getattr__` and `__setattr__`. This means
        that if we define the specs as properties, then any error in the property will
        be interpreted as a failure to retrieve the attribute and will invoke
        `__getattr__` which will give a confusing error about the attribute not found.
        More details here: https://github.com/pytorch/pytorch/issues/49726.
    """

    framework: str = None

    def __init__(self, config: RLModuleConfig):
        self.config = config
        self.setup()

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

    def setup(self):
        """Sets up the components of the module.

        This is called automatically during the __init__ method of this class,
        therefore, the subclass should call super.__init__() in its constructor. This
        abstraction can be used to create any component that your RLModule needs.
        """

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

    def save_state(self, path: Union[str, pathlib.Path]) -> None:
        """Saves the weights of this RLModule to path.

        Args:
            path: The file path to save the checkpoint to.

        """
        raise NotImplementedError

    def load_state(self, path: Union[str, pathlib.Path]) -> None:
        """Loads the weights of an RLModule from path.

        Args:
            path: The directory to load the checkpoint from.
        """
        raise NotImplementedError

    def _module_metadata(
        self,
        module_spec_class: Union[
            Type[SingleAgentRLModuleSpec], Type["MultiAgentRLModuleSpec"]
        ],
        additional_metadata: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        """Returns the metadata of the module.

        This method is used to save the metadata of the module to the checkpoint.

        Includes:
            - module spec class (e.g SingleAgentRLModuleSpec or MultiAgentRLModuleSpec)
            - module spec serialized to a dict
            - module state path (if provided)
            - the ray version used
            - the ray commit hash used
            - the date and time of the checkpoint was created

        Args:
            module_spec_class: The module spec class that can be used to construct this
                module.
            additional_metadata: Any additional metadata to be added to metadata.

        Returns:
            A dict of json serializable the metadata.
        """
        metadata = {}
        gmt_time = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S GMT")

        # TODO (Avnishn): Find a way to incorporate the tune registry here.
        metadata[RLMODULE_METADATA_SPEC_CLASS_KEY] = serialize_type(module_spec_class)
        metadata[RLMODULE_METADATA_SPEC_KEY] = module_spec_class.from_module(
            self
        ).to_dict()
        metadata[RLMODULE_METADATA_RAY_VERSION_KEY] = ray.__version__
        metadata[RLMODULE_METADATA_RAY_COMMIT_HASH_KEY] = ray.__commit__
        metadata[RLMODULE_METADATA_CHECKPOINT_DATE_TIME_KEY] = gmt_time
        if not additional_metadata:
            additional_metadata = {}
        metadata.update(**additional_metadata)
        return metadata

    def _save_module_metadata(
        self,
        checkpoint_dir: Union[str, pathlib.Path],
        module_spec_class: Union[
            Type[SingleAgentRLModuleSpec], Type["MultiAgentRLModuleSpec"]
        ],
        additional_metadata: Mapping[str, Any] = None,
    ):
        """Saves the metadata of the module to checkpoint_dir.

        Args:
            checkpoint_dir: The directory to save the metadata to.
            additional_metadata: Additional metadata to save.

        """
        if not additional_metadata:
            additional_metadata = {}
        checkpoint_dir = pathlib.Path(checkpoint_dir)
        metadata = self._module_metadata(module_spec_class, additional_metadata)
        metadata_path = checkpoint_dir / RLMODULE_METADATA_FILE_NAME
        with open(metadata_path, "w") as f:
            json.dump(metadata, f)

    @classmethod
    def _from_metadata_file(cls, metadata_path: Union[str, pathlib.Path]) -> "RLModule":
        """Constructs a module from the metadata.

        Args:
            metadata_path: The path to the metadata json file for a module.

        Returns:
            The module.
        """
        metadata_path = pathlib.Path(metadata_path)
        if not metadata_path.exists():
            raise ValueError(
                "While constructing the module from the metadata, the "
                f"metadata file was not found at {str(metadata_path)}"
            )
        with open(metadata_path, "r") as f:
            metadata = json.load(f)
        module_spec_class = deserialize_type(metadata[RLMODULE_METADATA_SPEC_CLASS_KEY])
        module_spec = module_spec_class.from_dict(metadata[RLMODULE_METADATA_SPEC_KEY])
        module = module_spec.build()
        return module

    def _module_state_file_name(self) -> pathlib.Path:
        """The name of the file to save the module state to while checkpointing."""
        raise NotImplementedError

    def save_to_checkpoint(self, checkpoint_dir_path: Union[str, pathlib.Path]) -> None:
        """Saves the module to a checkpoint directory.

        Args:
            checkpoint_dir_path: The directory to save the checkpoint to.

        Raises:
            ValueError: If dir_path is not an absolute path.
        """
        path = pathlib.Path(checkpoint_dir_path)
        path.mkdir(parents=True, exist_ok=True)
        module_state_dir = path / RLMODULE_STATE_DIR_NAME
        module_state_dir.mkdir(parents=True, exist_ok=True)
        self.save_state(module_state_dir / self._module_state_file_name())
        self._save_module_metadata(path, SingleAgentRLModuleSpec)

    @classmethod
    def from_checkpoint(cls, checkpoint_dir_path: Union[str, pathlib.Path]) -> None:
        """Loads the module from a checkpoint directory.

        Args:
            checkpoint_dir_path: The directory to load the checkpoint from.
        """
        path = pathlib.Path(checkpoint_dir_path)
        if not path.exists():
            raise ValueError(
                "While loading from checkpoint there was no directory"
                " found at {}".format(checkpoint_dir_path)
            )
        if not path.is_dir():
            raise ValueError(
                "While loading from checkpoint the checkpoint_dir_path "
                "provided was not a directory."
            )
        metadata_path = path / RLMODULE_METADATA_FILE_NAME
        module = cls._from_metadata_file(metadata_path)
        module_state_dir = path / RLMODULE_STATE_DIR_NAME
        state_path = module_state_dir / module._module_state_file_name()
        module.load_state(state_path)
        return module

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
