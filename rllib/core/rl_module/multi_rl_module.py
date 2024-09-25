from dataclasses import dataclass, field
import logging
import pprint
from typing import (
    Any,
    Callable,
    Collection,
    Dict,
    ItemsView,
    KeysView,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
    ValuesView,
)

from ray.rllib.core.models.specs.typing import SpecType
from ray.rllib.core.rl_module.rl_module import RLModule, RLModuleSpec

from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils import force_list
from ray.rllib.utils.annotations import (
    ExperimentalAPI,
    override,
    OverrideToImplementCustomLogic,
)
from ray.rllib.utils.checkpoints import Checkpointable
from ray.rllib.utils.deprecation import Deprecated
from ray.rllib.utils.serialization import serialize_type, deserialize_type
from ray.rllib.utils.typing import ModuleID, StateDict, T
from ray.util.annotations import PublicAPI

logger = logging.getLogger("ray.rllib")


@PublicAPI(stability="alpha")
class MultiRLModule(RLModule):
    """Base class for an RLModule that contains n sub-RLModules.

    This class holds a mapping from ModuleID to underlying RLModules. It provides
    a convenient way of accessing each individual module, as well as accessing all of
    them with only one API call. Whether a given module is trainable is
    determined by the caller of this class (not the instance of this class itself).

    The extension of this class can include any arbitrary neural networks as part of
    the MultiRLModule. For example, a MultiRLModule can include a shared encoder network
    that is used by all the individual (single-agent) RLModules. It is up to the user
    to decide how to implement this class.

    The default implementation assumes the data communicated as input and output of
    the APIs in this class are `MultiAgentBatch` types. The `MultiRLModule` simply
    loops through each `module_id`, and runs the forward pass of the corresponding
    `RLModule` object with the associated `SampleBatch` within the `MultiAgentBatch`.
    It also assumes that the underlying RLModules do not share any parameters or
    communication with one another. The behavior of modules with such advanced
    communication would be undefined by default. To share parameters or communication
    between the underlying RLModules, you should implement your own
    `MultiRLModule` subclass.
    """

    def __init__(self, config: Optional["MultiRLModuleConfig"] = None) -> None:
        """Initializes a MultiRLModule instance.

        Args:
            config: An optional MultiRLModuleConfig to use. If None, will use
                `MultiRLModuleConfig()` as default config.
        """
        super().__init__(config or MultiRLModuleConfig())

    @override(RLModule)
    def setup(self):
        """Sets up the underlying RLModules."""
        self._rl_modules = {}
        self.__check_module_configs(self.config.modules)
        # Make sure all individual RLModules have the same framework OR framework=None.
        framework = None
        for module_id, module_spec in self.config.modules.items():
            self._rl_modules[module_id] = module_spec.build()
            if framework is None:
                framework = self._rl_modules[module_id].framework
            else:
                assert self._rl_modules[module_id].framework in [None, framework]
        self.framework = framework

    @OverrideToImplementCustomLogic
    @override(RLModule)
    def get_initial_state(self) -> Any:
        # TODO (sven): Replace by call to `self.foreach_module`, but only if this method
        #  supports returning dicts.
        ret = {}
        for module_id, module in self._rl_modules.items():
            ret[module_id] = module.get_initial_state()
        return ret

    @OverrideToImplementCustomLogic
    @override(RLModule)
    def is_stateful(self) -> bool:
        initial_state = self.get_initial_state()
        assert isinstance(initial_state, dict), (
            "The initial state of an RLModule must be a dict, but is "
            f"{type(initial_state)} instead."
        )
        return bool(any(sa_init_state for sa_init_state in initial_state.values()))

    @classmethod
    def __check_module_configs(cls, module_configs: Dict[ModuleID, Any]):
        """Checks the module configs for validity.

        The module_configs be a mapping from module_ids to RLModuleSpec
        objects.

        Args:
            module_configs: The module configs to check.

        Raises:
            ValueError: If the module configs are invalid.
        """
        for module_id, module_spec in module_configs.items():
            if not isinstance(module_spec, RLModuleSpec):
                raise ValueError(f"Module {module_id} is not a RLModuleSpec object.")

    def items(self) -> ItemsView[ModuleID, RLModule]:
        """Returns a keys view over the module IDs in this MultiRLModule."""
        return self._rl_modules.items()

    def keys(self) -> KeysView[ModuleID]:
        """Returns a keys view over the module IDs in this MultiRLModule."""
        return self._rl_modules.keys()

    def values(self) -> ValuesView[ModuleID]:
        """Returns a keys view over the module IDs in this MultiRLModule."""
        return self._rl_modules.values()

    def __len__(self) -> int:
        """Returns the number of RLModules within this MultiRLModule."""
        return len(self._rl_modules)

    @override(RLModule)
    def as_multi_rl_module(self) -> "MultiRLModule":
        """Returns self in order to match `RLModule.as_multi_rl_module()` behavior.

        This method is overridden to avoid double wrapping.

        Returns:
            The instance itself.
        """
        return self

    def add_module(
        self,
        module_id: ModuleID,
        module: RLModule,
        *,
        override: bool = False,
    ) -> None:
        """Adds a module at run time to the multi-agent module.

        Args:
            module_id: The module ID to add. If the module ID already exists and
                override is False, an error is raised. If override is True, the module
                is replaced.
            module: The module to add.
            override: Whether to override the module if it already exists.

        Raises:
            ValueError: If the module ID already exists and override is False.
                Warnings are raised if the module id is not valid according to the
                logic of ``validate_module_id()``.
        """
        from ray.rllib.core.rl_module import validate_module_id

        validate_module_id(module_id)

        if module_id in self._rl_modules and not override:
            raise ValueError(
                f"Module ID {module_id} already exists. If your intention is to "
                "override, set override=True."
            )
        # Set our own inference_only flag to False as soon as any added Module
        # has `inference_only=False`.
        if not module.config.inference_only:
            self.config.inference_only = False
        self._rl_modules[module_id] = module
        # Update our `MultiRLModuleConfig`, such that - if written to disk -
        # it'll allow for proper restoring this instance through `.from_checkpoint()`.
        self.config.modules[module_id] = RLModuleSpec.from_module(module)

    def remove_module(
        self, module_id: ModuleID, *, raise_err_if_not_found: bool = True
    ) -> None:
        """Removes a module at run time from the multi-agent module.

        Args:
            module_id: The module ID to remove.
            raise_err_if_not_found: Whether to raise an error if the module ID is not
                found.
        Raises:
            ValueError: If the module ID does not exist and raise_err_if_not_found is
                True.
        """
        if raise_err_if_not_found:
            self._check_module_exists(module_id)
        del self._rl_modules[module_id]
        del self.config.modules[module_id]

    def foreach_module(
        self,
        func: Callable[[ModuleID, RLModule, Optional[Any]], T],
        *,
        return_dict: bool = False,
        **kwargs,
    ) -> Union[List[T], Dict[ModuleID, T]]:
        """Calls the given function with each (module_id, module).

        Args:
            func: The function to call with each (module_id, module) tuple.
            return_dict: Whether to return a dict mapping ModuleID to the individual
                module's return values of calling `func`. If False (default), return
                a list.

        Returns:
            The list of return values of all calls to
            `func([module_id, module, **kwargs])` or a dict (if `return_dict=True`)
            mapping ModuleIDs to the respective models' return values.
        """
        ret_dict = {
            module_id: func(module_id, module.unwrapped(), **kwargs)
            for module_id, module in self._rl_modules.items()
        }
        if return_dict:
            return ret_dict
        return list(ret_dict.values())

    def __contains__(self, item) -> bool:
        """Returns whether the given `item` (ModuleID) is present in self."""
        return item in self._rl_modules

    def __getitem__(self, module_id: ModuleID) -> RLModule:
        """Returns the RLModule with the given module ID.

        Args:
            module_id: The module ID to get.

        Returns:
            The RLModule with the given module ID.

        Raises:
            KeyError: If `module_id` cannot be found in self.
        """
        self._check_module_exists(module_id)
        return self._rl_modules[module_id]

    def get(
        self,
        module_id: ModuleID,
        default: Optional[RLModule] = None,
    ) -> Optional[RLModule]:
        """Returns the module with the given module ID or default if not found in self.

        Args:
            module_id: The module ID to get.

        Returns:
            The RLModule with the given module ID or `default` if `module_id` not found
            in `self`.
        """
        if module_id not in self._rl_modules:
            return default
        return self._rl_modules[module_id]

    @override(RLModule)
    def output_specs_train(self) -> SpecType:
        return []

    @override(RLModule)
    def output_specs_inference(self) -> SpecType:
        return []

    @override(RLModule)
    def output_specs_exploration(self) -> SpecType:
        return []

    @override(RLModule)
    def _default_input_specs(self) -> SpecType:
        """MultiRLModule should not check the input specs.

        The underlying single-agent RLModules will check the input specs.
        """
        return []

    @override(RLModule)
    def _forward_train(
        self, batch: MultiAgentBatch, **kwargs
    ) -> Union[Dict[str, Any], Dict[ModuleID, Dict[str, Any]]]:
        """Runs the forward_train pass.

        TODO(avnishn, kourosh): Review type hints for forward methods.

        Args:
            batch: The batch of multi-agent data (i.e. mapping from module ids to
                SampleBaches).

        Returns:
            The output of the forward_train pass the specified modules.
        """
        return self._run_forward_pass("forward_train", batch, **kwargs)

    @override(RLModule)
    def _forward_inference(
        self, batch: MultiAgentBatch, **kwargs
    ) -> Union[Dict[str, Any], Dict[ModuleID, Dict[str, Any]]]:
        """Runs the forward_inference pass.

        TODO(avnishn, kourosh): Review type hints for forward methods.

        Args:
            batch: The batch of multi-agent data (i.e. mapping from module ids to
                SampleBaches).

        Returns:
            The output of the forward_inference pass the specified modules.
        """
        return self._run_forward_pass("forward_inference", batch, **kwargs)

    @override(RLModule)
    def _forward_exploration(
        self, batch: MultiAgentBatch, **kwargs
    ) -> Union[Dict[str, Any], Dict[ModuleID, Dict[str, Any]]]:
        """Runs the forward_exploration pass.

        TODO(avnishn, kourosh): Review type hints for forward methods.

        Args:
            batch: The batch of multi-agent data (i.e. mapping from module ids to
                SampleBaches).

        Returns:
            The output of the forward_exploration pass the specified modules.
        """
        return self._run_forward_pass("forward_exploration", batch, **kwargs)

    @override(RLModule)
    def get_state(
        self,
        components: Optional[Union[str, Collection[str]]] = None,
        *,
        not_components: Optional[Union[str, Collection[str]]] = None,
        inference_only: bool = False,
        **kwargs,
    ) -> StateDict:
        state = {}

        for module_id, rl_module in self.get_checkpointable_components():
            if self._check_component(module_id, components, not_components):
                state[module_id] = rl_module.get_state(
                    components=self._get_subcomponents(module_id, components),
                    not_components=self._get_subcomponents(module_id, not_components),
                    inference_only=inference_only,
                )
        return state

    @override(RLModule)
    def set_state(self, state: StateDict) -> None:
        """Sets the state of the multi-agent module.

        It is assumed that the state_dict is a mapping from module IDs to the
        corresponding module's state. This method sets the state of each module by
        calling their set_state method. If you want to set the state of some of the
        RLModules within this MultiRLModule your state_dict can only include the
        state of those RLModules. Override this method to customize the state_dict for
        custom more advanced multi-agent use cases.

        Args:
            state: The state dict to set.
        """
        for module_id, module_state in state.items():
            if module_id in self:
                self._rl_modules[module_id].set_state(module_state)

    @override(Checkpointable)
    def get_checkpointable_components(self) -> List[Tuple[str, Checkpointable]]:
        return list(self._rl_modules.items())

    def __repr__(self) -> str:
        return f"MARL({pprint.pformat(self._rl_modules)})"

    def _run_forward_pass(
        self,
        forward_fn_name: str,
        batch: Dict[ModuleID, Any],
        **kwargs,
    ) -> Dict[ModuleID, Dict[ModuleID, Any]]:
        """This is a helper method that runs the forward pass for the given module.

        It uses forward_fn_name to get the forward pass method from the RLModule
        (e.g. forward_train vs. forward_exploration) and runs it on the given batch.

        Args:
            forward_fn_name: The name of the forward pass method to run.
            batch: The batch of multi-agent data (i.e. mapping from module ids to
                SampleBaches).
            **kwargs: Additional keyword arguments to pass to the forward function.

        Returns:
            The output of the forward pass the specified modules. The output is a
            mapping from module ID to the output of the forward pass.
        """

        outputs = {}
        for module_id in batch.keys():
            self._check_module_exists(module_id)
            rl_module = self._rl_modules[module_id]
            forward_fn = getattr(rl_module, forward_fn_name)
            outputs[module_id] = forward_fn(batch[module_id], **kwargs)

        return outputs

    def _check_module_exists(self, module_id: ModuleID) -> None:
        if module_id not in self._rl_modules:
            raise KeyError(
                f"Module with module_id {module_id} not found. "
                f"Available modules: {set(self.keys())}"
            )


@PublicAPI(stability="alpha")
@dataclass
class MultiRLModuleSpec:
    """A utility spec class to make it constructing MultiRLModules easier.

    Users can extend this class to modify the behavior of base class. For example to
    share neural networks across the modules, the build method can be overriden to
    create the shared module first and then pass it to custom module classes that would
    then use it as a shared module.

    Args:
        multi_rl_module_class: The class of the MultiRLModule to construct. By
            default it is set to MultiRLModule class. This class simply loops
            throught each module and calls their foward methods.
        module_specs: The module specs for each individual module. It can be either a
            RLModuleSpec used for all module_ids or a dictionary mapping
            from module IDs to RLModuleSpecs for each individual module.
        load_state_path: The path to the module state to load from. NOTE: This must be
            an absolute path. NOTE: If the load_state_path of this spec is set, and
            the load_state_path of one of the RLModuleSpecs' is also set,
            the weights of that RL Module will be loaded from the path specified in
            the RLModuleSpec. This is useful if you want to load the weights
            of a MultiRLModule and also manually load the weights of some of the RL
            modules within that MultiRLModule from other checkpoints.
        modules_to_load: A set of module ids to load from the checkpoint. This is
            only used if load_state_path is set. If this is None, all modules are
            loaded.
    """

    multi_rl_module_class: Type[MultiRLModule] = MultiRLModule
    inference_only: bool = False
    # TODO (sven): Once we support MultiRLModules inside other MultiRLModules, we would
    #  need this flag in here as well, but for now, we'll leave it out for simplicity.
    # learner_only: bool = False
    module_specs: Union[RLModuleSpec, Dict[ModuleID, RLModuleSpec]] = None
    load_state_path: Optional[str] = None
    modules_to_load: Optional[Set[ModuleID]] = None

    # To be deprecated (same as `multi_rl_module_class`).
    marl_module_class: Type[MultiRLModule] = MultiRLModule

    def __post_init__(self):
        if self.module_specs is None:
            raise ValueError(
                "Module_specs cannot be None. It should be either a "
                "RLModuleSpec or a dictionary mapping from module IDs to "
                "RLModuleSpecs for each individual module."
            )

    def get_multi_rl_module_config(self) -> "MultiRLModuleConfig":
        """Returns the MultiRLModuleConfig for this spec."""
        return MultiRLModuleConfig(
            # Only set `inference_only=True` if all single-agent specs are
            # `inference_only`.
            inference_only=all(
                spec.inference_only for spec in self.module_specs.values()
            ),
            modules=self.module_specs,
        )

    @OverrideToImplementCustomLogic
    def build(self, module_id: Optional[ModuleID] = None) -> RLModule:
        """Builds either the multi-agent module or the single-agent module.

        If module_id is None, it builds the multi-agent module. Otherwise, it builds
        the single-agent module with the given module_id.

        Note: If when build is called the module_specs is not a dictionary, it will
        raise an error, since it should have been updated by the caller to inform us
        about the module_ids.

        Args:
            module_id: The module_id of the single-agent module to build. If None, it
                builds the multi-agent module.

        Returns:
            The built module. If module_id is None, it returns the multi-agent module.
        """
        self._check_before_build()

        # ModuleID provided, return single-agent RLModule.
        if module_id:
            return self.module_specs[module_id].build()

        # Return MultiRLModule.
        module_config = self.get_multi_rl_module_config()
        module = self.multi_rl_module_class(module_config)
        return module

    def add_modules(
        self,
        module_specs: Dict[ModuleID, RLModuleSpec],
        override: bool = True,
    ) -> None:
        """Add new module specs to the spec or updates existing ones.

        Args:
            module_specs: The mapping for the module_id to the single-agent module
                specs to be added to this multi-agent module spec.
            override: Whether to override the existing module specs if they already
                exist. If False, they are only updated.
        """
        if self.module_specs is None:
            self.module_specs = {}
        for module_id, module_spec in module_specs.items():
            if override or module_id not in self.module_specs:
                # Disable our `inference_only` as soon as any single-agent module has
                # `inference_only=False`.
                if not module_spec.inference_only:
                    self.inference_only = False
                self.module_specs[module_id] = module_spec
            else:
                self.module_specs[module_id].update(module_spec)

    def remove_modules(self, module_ids: Union[ModuleID, Collection[ModuleID]]) -> None:
        """Removes the provided ModuleIDs from this MultiRLModuleSpec.

        Args:
            module_ids: Collection of the ModuleIDs to remove from this spec.
        """
        for module_id in force_list(module_ids):
            self.module_specs.pop(module_id, None)

    @classmethod
    def from_module(self, module: MultiRLModule) -> "MultiRLModuleSpec":
        """Creates a MultiRLModuleSpec from a MultiRLModule.

        Args:
            module: The MultiRLModule to create the spec from.

        Returns:
            The MultiRLModuleSpec.
        """
        # we want to get the spec of the underlying unwrapped module that way we can
        # easily reconstruct it. The only wrappers that we expect to support today are
        # wrappers that allow us to do distributed training. Those will be added back
        # by the learner if necessary.
        module_specs = {
            module_id: RLModuleSpec.from_module(rl_module.unwrapped())
            for module_id, rl_module in module._rl_modules.items()
        }
        multi_rl_module_class = module.__class__
        return MultiRLModuleSpec(
            multi_rl_module_class=multi_rl_module_class,
            inference_only=module.config.inference_only,
            module_specs=module_specs,
        )

    def _check_before_build(self):
        if not isinstance(self.module_specs, dict):
            raise ValueError(
                f"When build() is called on {self.__class__}, the module_specs "
                "should be a dictionary mapping from module IDs to "
                "RLModuleSpecs for each individual module."
            )

    def to_dict(self) -> Dict[str, Any]:
        """Converts the MultiRLModuleSpec to a dictionary."""
        return {
            "multi_rl_module_class": serialize_type(self.multi_rl_module_class),
            "inference_only": self.inference_only,
            "module_specs": {
                module_id: module_spec.to_dict()
                for module_id, module_spec in self.module_specs.items()
            },
        }

    @classmethod
    def from_dict(cls, d) -> "MultiRLModuleSpec":
        """Creates a MultiRLModuleSpec from a dictionary."""
        return MultiRLModuleSpec(
            multi_rl_module_class=deserialize_type(d["multi_rl_module_class"]),
            inference_only=d["inference_only"],
            module_specs={
                module_id: RLModuleSpec.from_dict(module_spec)
                for module_id, module_spec in d["module_specs"].items()
            },
        )

    def update(
        self,
        other: Union["MultiRLModuleSpec", RLModuleSpec],
        override: bool = False,
    ) -> None:
        """Updates this spec with the other spec.

        Traverses this MultiRLModuleSpec's module_specs and updates them with
        the module specs from the other MultiRLModuleSpec.

        Args:
            other: The other spec to update this spec with.
            override: Whether to override the existing module specs if they already
                exist. If False, they are only updated.
        """
        if isinstance(other, RLModuleSpec):
            # Disable our `inference_only` as soon as any single-agent module has
            # `inference_only=False`.
            if not other.inference_only:
                self.inference_only = False
            for mid, spec in self.module_specs.items():
                self.module_specs[mid].update(other, override=False)
        elif isinstance(other.module_specs, dict):
            self.add_modules(other.module_specs, override=override)
        else:
            assert isinstance(other, MultiRLModuleSpec)
            if not self.module_specs:
                self.inference_only = other.inference_only
                self.module_specs = other.module_specs
            else:
                if not other.inference_only:
                    self.inference_only = False
                self.module_specs.update(other.module_specs)

    def as_multi_rl_module_spec(self) -> "MultiRLModuleSpec":
        """Returns self in order to match `RLModuleSpec.as_multi_rl_module_spec()`."""
        return self

    def __contains__(self, item) -> bool:
        """Returns whether the given `item` (ModuleID) is present in self."""
        return item in self.module_specs

    def __getitem__(self, item) -> RLModuleSpec:
        """Returns the RLModuleSpec under the ModuleID."""
        return self.module_specs[item]

    @Deprecated(new="MultiRLModuleSpec.as_multi_rl_module_spec()", error=True)
    def as_multi_agent(self):
        pass

    @Deprecated(new="MultiRLModuleSpec.get_multi_rl_module_config", error=True)
    def get_marl_config(self, *args, **kwargs):
        pass


# TODO (sven): Shouldn't we simply use this class inside MultiRLModuleSpec instead
#  of duplicating all data records (e.g. `inference_only`) in `MultiRLModuleSpec`?
#  Same for RLModuleSpec, which should use RLModuleConfig instead of
#  duplicating all settings, e.g. `observation_space`, `inference_only`, ...
@ExperimentalAPI
@dataclass
class MultiRLModuleConfig:
    inference_only: bool = False
    modules: Dict[ModuleID, RLModuleSpec] = field(default_factory=dict)

    def to_dict(self):
        return {
            "inference_only": self.inference_only,
            "modules": {
                module_id: module_spec.to_dict()
                for module_id, module_spec in self.modules.items()
            },
        }

    @classmethod
    def from_dict(cls, d) -> "MultiRLModuleConfig":
        return cls(
            inference_only=d["inference_only"],
            modules={
                module_id: RLModuleSpec.from_dict(module_spec)
                for module_id, module_spec in d["modules"].items()
            },
        )

    def get_catalog(self) -> None:
        return None
