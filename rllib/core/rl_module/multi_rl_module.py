import copy
import dataclasses
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

import gymnasium as gym

from ray.rllib.core.rl_module.rl_module import RLModule, RLModuleSpec
from ray.rllib.utils import force_list
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
    the APIs in this class are `Dict[ModuleID, Dict[str, Any]]` types. The
    `MultiRLModule` by default loops through each `module_id`, and runs the forward pass
    of the corresponding `RLModule` object with the associated `batch` within the
    input.
    It also assumes that the underlying RLModules do not share any parameters or
    communication with one another. The behavior of modules with such advanced
    communication would be undefined by default. To share parameters or communication
    between the underlying RLModules, you should implement your own
    `MultiRLModule` subclass.
    """

    def __init__(
        self,
        config=DEPRECATED_VALUE,
        *,
        observation_space: Optional[gym.Space] = None,
        action_space: Optional[gym.Space] = None,
        inference_only: Optional[bool] = None,
        # TODO (sven): Ignore learner_only setting for now on MultiRLModule.
        learner_only: Optional[bool] = None,
        model_config: Optional[dict] = None,
        rl_module_specs: Optional[Dict[ModuleID, RLModuleSpec]] = None,
        **kwargs,
    ) -> None:
        """Initializes a MultiRLModule instance.

        Args:
            observation_space: The MultiRLModule's observation space.
            action_space: The MultiRLModule's action space.
            inference_only: The MultiRLModule's `inference_only` setting. If True, force
                sets all inference_only flags inside `rl_module_specs` also to True.
                If None, infers the value for `inference_only` by setting it to True,
                iff all `inference_only` flags inside `rl_module_specs`, otherwise to
                False.
            model_config: The MultiRLModule's `model_config` dict.
            rl_module_specs: A dict mapping ModuleIDs to `RLModuleSpec` instances used
                to create the submodules.
        """
        if config != DEPRECATED_VALUE and isinstance(config, MultiRLModuleConfig):
            deprecation_warning(
                old="MultiRLModule(config=..)",
                new="MultiRLModule(*, observation_space=.., action_space=.., "
                "inference_only=.., model_config=.., rl_module_specs=..)",
                error=True,
            )

        # Make sure we don't alter incoming module specs in this c'tor.
        rl_module_specs = copy.deepcopy(rl_module_specs or {})
        # Figure out global inference_only setting.
        # If not provided (None), only if all submodules are
        # inference_only, this MultiRLModule will be inference_only.
        inference_only = (
            inference_only
            if inference_only is not None
            else all(spec.inference_only for spec in rl_module_specs.values())
        )
        # If given inference_only=True, make all submodules also inference_only (before
        # creating them).
        if inference_only is True:
            for rl_module_spec in rl_module_specs.values():
                rl_module_spec.inference_only = True
        self._check_module_specs(rl_module_specs)
        self.rl_module_specs = rl_module_specs

        super().__init__(
            observation_space=observation_space,
            action_space=action_space,
            inference_only=inference_only,
            learner_only=None,
            catalog_class=None,
            model_config=model_config,
            **kwargs,
        )

    @OverrideToImplementCustomLogic
    @override(RLModule)
    def setup(self):
        """Sets up the underlying, individual RLModules."""
        self._rl_modules = {}
        # Make sure all individual RLModules have the same framework OR framework=None.
        framework = None
        for module_id, rl_module_spec in self.rl_module_specs.items():
            self._rl_modules[module_id] = rl_module_spec.build()
            if framework is None:
                framework = self._rl_modules[module_id].framework
            else:
                assert self._rl_modules[module_id].framework in [None, framework]
        self.framework = framework

    @override(RLModule)
    def _forward(
        self,
        batch: Dict[ModuleID, Any],
        **kwargs,
    ) -> Dict[ModuleID, Dict[str, Any]]:
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
            batch: The input batch, a dict mapping from ModuleID to individual modules'
                batches.
            **kwargs: Additional keyword arguments.

        Returns:
            The output of the forward pass.
        """
        return {
            mid: self._rl_modules[mid]._forward(batch[mid], **kwargs)
            for mid in batch.keys()
            if mid in self
        }

    @override(RLModule)
    def _forward_inference(
        self, batch: Dict[str, Any], **kwargs
    ) -> Union[Dict[str, Any], Dict[ModuleID, Dict[str, Any]]]:
        """Forward-pass used for action computation without exploration behavior.

        Override this method only, if you need specific behavior for non-exploratory
        action computation behavior. If you have only one generic behavior for all
        phases of training and evaluation, override `self._forward()` instead.

        By default, this calls the generic `self._forward()` method.
        """
        return {
            mid: self._rl_modules[mid]._forward_inference(batch[mid], **kwargs)
            for mid in batch.keys()
            if mid in self
        }

    @override(RLModule)
    def _forward_exploration(
        self, batch: Dict[str, Any], **kwargs
    ) -> Union[Dict[str, Any], Dict[ModuleID, Dict[str, Any]]]:
        """Forward-pass used for action computation with exploration behavior.

        Override this method only, if you need specific behavior for exploratory
        action computation behavior. If you have only one generic behavior for all
        phases of training and evaluation, override `self._forward()` instead.

        By default, this calls the generic `self._forward()` method.
        """
        return {
            mid: self._rl_modules[mid]._forward_exploration(batch[mid], **kwargs)
            for mid in batch.keys()
            if mid in self
        }

    @override(RLModule)
    def _forward_train(
        self, batch: Dict[str, Any], **kwargs
    ) -> Union[Dict[str, Any], Dict[ModuleID, Dict[str, Any]]]:
        """Forward-pass used before the loss computation (training).

        Override this method only, if you need specific behavior and outputs for your
        loss computations. If you have only one generic behavior for all
        phases of training and evaluation, override `self._forward()` instead.

        By default, this calls the generic `self._forward()` method.
        """
        return {
            mid: self._rl_modules[mid]._forward_train(batch[mid], **kwargs)
            for mid in batch.keys()
            if mid in self
        }

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
        if not module.inference_only:
            self.inference_only = False

        # Check framework of incoming RLModule against `self.framework`.
        if module.framework is not None:
            if self.framework is None:
                self.framework = module.framework
            elif module.framework != self.framework:
                raise ValueError(
                    f"Framework ({module.framework}) of incoming RLModule does NOT "
                    f"match framework ({self.framework}) of MultiRLModule! If the "
                    f"added module should not be trained, try setting its framework "
                    f"to None."
                )

        self._rl_modules[module_id] = module
        # Update our RLModuleSpecs dict, such that - if written to disk -
        # it'll allow for proper restoring this instance through `.from_checkpoint()`.
        self.rl_module_specs[module_id] = RLModuleSpec.from_module(module)

    def remove_module(
        self, module_id: ModuleID, *, raise_err_if_not_found: bool = True
    ) -> None:
        """Removes a module at runtime from the multi-agent module.

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
        del self.rl_module_specs[module_id]

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

    def items(self) -> ItemsView[ModuleID, RLModule]:
        """Returns an ItemsView over the module IDs in this MultiRLModule."""
        return self._rl_modules.items()

    def keys(self) -> KeysView[ModuleID]:
        """Returns a KeysView over the module IDs in this MultiRLModule."""
        return self._rl_modules.keys()

    def values(self) -> ValuesView[ModuleID]:
        """Returns a ValuesView over the module IDs in this MultiRLModule."""
        return self._rl_modules.values()

    def __len__(self) -> int:
        """Returns the number of RLModules within this MultiRLModule."""
        return len(self._rl_modules)

    def __repr__(self) -> str:
        return f"MARL({pprint.pformat(self._rl_modules)})"

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
        # Now, set the individual states
        for module_id, module_state in state.items():
            if module_id in self:
                self._rl_modules[module_id].set_state(module_state)

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
                "rl_module_specs": self.rl_module_specs,
            },  # **kwargs
        )

    @override(Checkpointable)
    def get_checkpointable_components(self) -> List[Tuple[str, Checkpointable]]:
        return list(self._rl_modules.items())

    @override(RLModule)
    def as_multi_rl_module(self) -> "MultiRLModule":
        """Returns self in order to match `RLModule.as_multi_rl_module()` behavior.

        This method is overridden to avoid double wrapping.

        Returns:
            The instance itself.
        """
        return self

    @classmethod
    def _check_module_specs(cls, rl_module_specs: Dict[ModuleID, RLModuleSpec]):
        """Checks the individual RLModuleSpecs for validity.

        Args:
            rl_module_specs: Dict mapping ModuleIDs to the respective RLModuleSpec.

        Raises:
            ValueError: If any RLModuleSpec is invalid.
        """
        for module_id, rl_module_spec in rl_module_specs.items():
            if not isinstance(rl_module_spec, RLModuleSpec):
                raise ValueError(f"Module {module_id} is not a RLModuleSpec object.")

    def _check_module_exists(self, module_id: ModuleID) -> None:
        if module_id not in self._rl_modules:
            raise KeyError(
                f"Module with module_id {module_id} not found. "
                f"Available modules: {set(self.keys())}"
            )

    @Deprecated(error=False)
    def output_specs_train(self):
        pass

    @Deprecated(error=False)
    def output_specs_inference(self):
        pass

    @Deprecated(error=False)
    def output_specs_exploration(self):
        pass

    @Deprecated(error=False)
    def _default_input_specs(self):
        pass


@PublicAPI(stability="alpha")
@dataclasses.dataclass
class MultiRLModuleSpec:
    """A utility spec class to make it constructing MultiRLModules easier.

    Users can extend this class to modify the behavior of base class. For example to
    share neural networks across the modules, the build method can be overridden to
    create the shared module first and then pass it to custom module classes that would
    then use it as a shared module.
    """

    #: The class of the MultiRLModule to construct. By default,
    #: this is the base `MultiRLModule` class.
    multi_rl_module_class: Type[MultiRLModule] = MultiRLModule
    #: Optional global observation space for the MultiRLModule.
    #: Useful for shared network components that live only inside the MultiRLModule
    #: and don't have their own ModuleID and own RLModule within
    #: `self._rl_modules`.
    observation_space: Optional[gym.Space] = None
    #: Optional global action space for the MultiRLModule. Useful for
    #: shared network components that live only inside the MultiRLModule and don't
    #: have their own ModuleID and own RLModule within `self._rl_modules`.
    action_space: Optional[gym.Space] = None
    #: An optional global inference_only flag. If not set (None by
    #: default), considers the MultiRLModule to be inference_only=True, only if all
    #: submodules also have their own inference_only flags set to True.
    inference_only: Optional[bool] = None
    # TODO (sven): Once we support MultiRLModules inside other MultiRLModules, we would
    #  need this flag in here as well, but for now, we'll leave it out for simplicity.
    # learner_only: bool = False
    #: An optional global model_config dict. Useful to configure shared
    #: network components that only live inside the MultiRLModule and don't have
    #: their own ModuleID and own RLModule within `self._rl_modules`.
    model_config: Optional[dict] = None
    #: The module specs for each individual module. It can be either
    #: an RLModuleSpec used for all module_ids or a dictionary mapping from module
    #: IDs to RLModuleSpecs for each individual module.
    rl_module_specs: Union[RLModuleSpec, Dict[ModuleID, RLModuleSpec]] = None

    # TODO (sven): Deprecate these in favor of using the pure Checkpointable APIs for
    #  loading and saving state.
    load_state_path: Optional[str] = None
    modules_to_load: Optional[Set[ModuleID]] = None

    # Deprecated: Do not use anymore.
    module_specs: Optional[Union[RLModuleSpec, Dict[ModuleID, RLModuleSpec]]] = None

    def __post_init__(self):
        if self.module_specs is not None:
            deprecation_warning(
                old="MultiRLModuleSpec(module_specs=..)",
                new="MultiRLModuleSpec(rl_module_specs=..)",
                error=True,
            )
        if self.rl_module_specs is None:
            raise ValueError(
                "Module_specs cannot be None. It should be either a "
                "RLModuleSpec or a dictionary mapping from module IDs to "
                "RLModuleSpecs for each individual module."
            )
        self.module_specs = self.rl_module_specs
        # Figure out global inference_only setting.
        # If not provided (None), only if all submodules are
        # inference_only, this MultiRLModule will be inference_only.
        self.inference_only = (
            self.inference_only
            if self.inference_only is not None
            else all(spec.inference_only for spec in self.rl_module_specs.values())
        )

    @OverrideToImplementCustomLogic
    def build(self, module_id: Optional[ModuleID] = None) -> RLModule:
        """Builds either the MultiRLModule or a (single) sub-RLModule under `module_id`.

        Args:
            module_id: Optional ModuleID of a single RLModule to be built. If None
                (default), builds the MultiRLModule.

        Returns:
            The built RLModule if `module_id` is provided, otherwise the built
            MultiRLModule.
        """
        self._check_before_build()

        # ModuleID provided, return single-agent RLModule.
        if module_id:
            return self.rl_module_specs[module_id].build()

        # Return MultiRLModule.
        try:
            module = self.multi_rl_module_class(
                observation_space=self.observation_space,
                action_space=self.action_space,
                inference_only=self.inference_only,
                model_config=(
                    dataclasses.asdict(self.model_config)
                    if dataclasses.is_dataclass(self.model_config)
                    else self.model_config
                ),
                rl_module_specs=self.rl_module_specs,
            )
        # Older custom model might still require the old `MultiRLModuleConfig` under
        # the `config` arg.
        except AttributeError as e:
            if self.multi_rl_module_class is not MultiRLModule:
                multi_rl_module_config = self.get_rl_module_config()
                module = self.multi_rl_module_class(multi_rl_module_config)
            else:
                raise e

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
        if self.rl_module_specs is None:
            self.rl_module_specs = {}
        for module_id, module_spec in module_specs.items():
            if override or module_id not in self.rl_module_specs:
                # Disable our `inference_only` as soon as any single-agent module has
                # `inference_only=False`.
                if not module_spec.inference_only:
                    self.inference_only = False
                self.rl_module_specs[module_id] = module_spec
            else:
                self.rl_module_specs[module_id].update(module_spec)

    def remove_modules(self, module_ids: Union[ModuleID, Collection[ModuleID]]) -> None:
        """Removes the provided ModuleIDs from this MultiRLModuleSpec.

        Args:
            module_ids: Collection of the ModuleIDs to remove from this spec.
        """
        for module_id in force_list(module_ids):
            self.rl_module_specs.pop(module_id, None)

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
        rl_module_specs = {
            module_id: RLModuleSpec.from_module(rl_module.unwrapped())
            for module_id, rl_module in module._rl_modules.items()
        }
        multi_rl_module_class = module.__class__
        return MultiRLModuleSpec(
            multi_rl_module_class=multi_rl_module_class,
            observation_space=module.observation_space,
            action_space=module.action_space,
            inference_only=module.inference_only,
            model_config=module.model_config,
            rl_module_specs=rl_module_specs,
        )

    def _check_before_build(self):
        if not isinstance(self.rl_module_specs, dict):
            raise ValueError(
                f"When build() is called on {self.__class__}, the `rl_module_specs` "
                "attribute should be a dictionary mapping ModuleIDs to "
                "RLModuleSpecs for each individual RLModule."
            )

    def to_dict(self) -> Dict[str, Any]:
        """Converts the MultiRLModuleSpec to a dictionary."""
        return {
            "multi_rl_module_class": serialize_type(self.multi_rl_module_class),
            "observation_space": gym_space_to_dict(self.observation_space),
            "action_space": gym_space_to_dict(self.action_space),
            "inference_only": self.inference_only,
            "model_config": self.model_config,
            "rl_module_specs": {
                module_id: rl_module_spec.to_dict()
                for module_id, rl_module_spec in self.rl_module_specs.items()
            },
        }

    @classmethod
    def from_dict(cls, d) -> "MultiRLModuleSpec":
        """Creates a MultiRLModuleSpec from a dictionary."""
        return MultiRLModuleSpec(
            multi_rl_module_class=deserialize_type(d["multi_rl_module_class"]),
            observation_space=gym_space_from_dict(d.get("observation_space")),
            action_space=gym_space_from_dict(d.get("action_space")),
            model_config=d.get("model_config"),
            inference_only=d["inference_only"],
            rl_module_specs={
                module_id: RLModuleSpec.from_dict(rl_module_spec)
                for module_id, rl_module_spec in (
                    d.get("rl_module_specs", d.get("module_specs")).items()
                )
            },
        )

    def update(
        self,
        other: Union["MultiRLModuleSpec", RLModuleSpec],
        override: bool = False,
    ) -> None:
        """Updates this spec with the other spec.

        Traverses this MultiRLModuleSpec's module_specs and updates them with
        the module specs from the `other` (Multi)RLModuleSpec.

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
            for mid, spec in self.rl_module_specs.items():
                self.rl_module_specs[mid].update(other, override=False)
        elif isinstance(other.module_specs, dict):
            self.add_modules(other.rl_module_specs, override=override)
        else:
            assert isinstance(other, MultiRLModuleSpec)
            if not self.rl_module_specs:
                self.inference_only = other.inference_only
                self.rl_module_specs = other.rl_module_specs
            else:
                if not other.inference_only:
                    self.inference_only = False
                self.rl_module_specs.update(other.rl_module_specs)

    def as_multi_rl_module_spec(self) -> "MultiRLModuleSpec":
        """Returns self in order to match `RLModuleSpec.as_multi_rl_module_spec()`."""
        return self

    def __contains__(self, item) -> bool:
        """Returns whether the given `item` (ModuleID) is present in self."""
        return item in self.rl_module_specs

    def __getitem__(self, item) -> RLModuleSpec:
        """Returns the RLModuleSpec under the ModuleID."""
        return self.rl_module_specs[item]

    @Deprecated(
        new="MultiRLModule(*, module_specs={module1: [RLModuleSpec], "
        "module2: [RLModuleSpec], ..}, inference_only=..)",
        error=True,
    )
    def get_multi_rl_module_config(self):
        pass

    @Deprecated(new="MultiRLModuleSpec.as_multi_rl_module_spec()", error=True)
    def as_multi_agent(self):
        pass

    @Deprecated(new="MultiRLModuleSpec.get_multi_rl_module_config", error=True)
    def get_marl_config(self, *args, **kwargs):
        pass

    @Deprecated(
        new="MultiRLModule(*, observation_space=.., action_space=.., ....)",
        error=False,
    )
    def get_rl_module_config(self):
        return MultiRLModuleConfig(
            inference_only=self.inference_only,
            modules=self.rl_module_specs,
        )


@Deprecated(
    new="MultiRLModule(*, rl_module_specs={module1: [RLModuleSpec], "
    "module2: [RLModuleSpec], ..}, inference_only=..)",
    error=False,
)
@dataclasses.dataclass
class MultiRLModuleConfig:
    inference_only: bool = False
    modules: Dict[ModuleID, RLModuleSpec] = dataclasses.field(default_factory=dict)

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
