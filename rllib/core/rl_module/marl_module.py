from typing import Iterator, Mapping, Any, Union, Dict, Set
import pprint


from ray.rllib.utils.annotations import (
    OverrideToImplementCustomLogic,
    override,
)

from ray.rllib.models.specs.specs_dict import ModelSpec
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.core.rl_module import RLModule

# TODO (Kourosh): change this to module_id later to enforce consistency
from ray.rllib.utils.policy import validate_policy_id

ModuleID = str


class MultiAgentRLModule(RLModule):
    """Base class for multi-agent RLModules.

    This class holds a mapping from module_ids to the underlying RLModules. It provides
    a convenient way of accessing each individual module, as well as accessing all of
    them with only one api call.

    The default implementation assumes the data communicated as input and output of
    the APIs in this class are `MultiAgentBatch` types. The `MultiAgentRLModule` simply
    loops throught each `module_id`, and runs the forward pass of the corresponding
    `RLModule` object with the associated `SampleBatch` within the `MultiAgentBatch`.
    It also assumes that the underlying RLModules do not share any parameters or
    communication with one another. The behavior of modules with such advanced
    communication would be undefined by default. To share parameters or communication
    between the underlying RLModules, you should implement your own MultiAgentRLModule.

    # TODO (Kourosh): Link to example of custom MultiAgentRLModule once it exists.

    Input config keys:
        `modules`: Mapping from module_id to each RLModule config.
        `trainable_modules`: Set of module_ids that are trainable. If not specified,
            all modules are trainable. For those modules that are specified after the
            construction of the class, whether they are trainable or not is determined
            by the `is_trainable` argument of `add_module`.

    Each RLModule config values can be of the following forms:
        1. The RLModule instance itself.
        2. A dict with the following keys:
            `module_class`: The RLModule class Type or full path separate by `.`.
                (e.g. `ray.rllib.algoirhms.dqn.torch.DQNTorchRLModule`).
            `module_config`: The config object for the RLModule.


    Args:
        config: The config dict for the multi-agent RLModule (see above).
    """

    def __init__(self, config: Mapping[str, Any], **kwargs) -> None:
        super().__init__(config, **kwargs)

        # TODO (Kourosh): Also make it possible that trainable_modules in config can
        # accept a Callable[[ModuleID, MultiAgentBatch], bool]
        self._trainable_rl_modules: Set[ModuleID] = set()
        self._rl_modules: Mapping[ModuleID, RLModule] = {}
        self._make_modules()

    def keys(self) -> Iterator[ModuleID]:
        """Returns an iteratable of module ids."""
        return self._rl_modules.keys()

    def get_trainable_module_ids(self) -> Set[ModuleID]:
        """Returns the set of ids of the trainable modules."""
        return self._trainable_rl_modules

    @override(RLModule)
    def make_distributed(self, dist_config: Mapping[str, Any] = None) -> None:
        """Makes the module distributed.

        It loops through all the modules and calls their make_distributed method.

        Args:
            dist_config: The optional distributed configuration to use for all make
                distributed calls.
        """
        for module in self._rl_modules.values():
            module.make_distributed(dist_config)

    @override(RLModule)
    def is_distributed(self) -> bool:
        """Returns True if all sub-modules are distributed."""
        return all(module.is_distributed() for module in self._rl_modules.values())

    @override(RLModule)
    def get_state(self) -> Mapping[str, Any]:
        """Returns the state of the multi-agent module.

        The default implementation loops all modules and calls their get_state method.
        Override this method if you want to change the get_state behavior.

        Returns:
            A nested state dict with the first layer being the module ID and the second
            is the state of the module.
        """
        return {
            module_id: module.get_state()
            for module_id, module in self._rl_modules.items()
        }

    @override(RLModule)
    def set_state(self, state_dict: Mapping[str, Any]) -> None:
        """Sets the state dict of the multi-agent module.

        The default implementation is a mapping from independent module IDs to their
        corresponding RLModule state_dicts. Override this method to customize the
        state_dict for custom more advanced multi-agent use cases.

        Args:
            state_dict: The state dict to set.
        """
        for module_id, module in self._rl_modules.items():
            module.set_state(state_dict[module_id])

    @override(RLModule)
    def as_multi_agent(self) -> "MultiAgentRLModule":
        """Returns a multi-agent wrapper around this module.

        This method is overridden to avoid double wrapping.

        Returns:
            The instance itself.
        """
        return self

    def add_module(
        self,
        module_id: ModuleID,
        module_spec: Union[RLModule, Mapping[str, Any]],
        *,
        dist_config=None,
        override: bool = False,
        is_trainable: bool = True,
    ) -> None:
        """Adds a module at run time to the multi-agent module.

        Args:
            module_id: The module ID to add. If the module ID already exists and
                override is False, an error is raised. If override is True, the module
                is replaced.
            module: The module to add.
            dist_config: The distributed configuration to use if module needs to be
                distributed.
            override: Whether to override the module if it already exists.
            is_trainable: Whether the module is trainable.

        Raises:
            ValueError: If the module ID already exists and override is False.
        """
        validate_policy_id(module_id)
        if module_id in self._rl_modules and not override:
            raise ValueError(
                f"Module ID {module_id} already exists. If your intention is to "
                "override, set override=True."
            )

        module = self._build_module_from_spec(module_spec)

        if self.is_distributed():
            module.make_distributed(dist_config)

        self._rl_modules[module_id] = module
        if is_trainable:
            self._trainable_rl_modules.add(module_id)

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
        self._trainable_rl_modules.discard(module_id)

    def __getitem__(self, module_id: ModuleID) -> RLModule:
        """Returns the module with the given module ID.

        Args:
            module_id: The module ID to get.

        Returns:
            The module with the given module ID.
        """
        self._check_module_exists(module_id)
        return self._rl_modules[module_id]

    @OverrideToImplementCustomLogic
    def _make_modules(self) -> None:
        modules_info_dict = self.config["modules"]
        trainables = self.config.get("trainable_modules", set())
        for module_id, module_config in modules_info_dict.items():
            # should be trainable if trainables is not specified or if module_id exists
            # in the trainables
            is_trainable = not trainables or module_id in trainables
            self.add_module(module_id, module_config, is_trainable=is_trainable)

    @override(RLModule)
    def output_specs_train(self) -> ModelSpec:
        return self._get_specs_for_modules("output_specs_train")

    @override(RLModule)
    def output_specs_inference(self) -> ModelSpec:
        return self._get_specs_for_modules("output_specs_inference")

    @override(RLModule)
    def output_specs_exploration(self) -> ModelSpec:
        return self._get_specs_for_modules("output_specs_exploration")

    @override(RLModule)
    def input_specs_train(self) -> ModelSpec:
        return self._get_specs_for_modules("input_specs_train")

    @override(RLModule)
    def input_specs_inference(self) -> ModelSpec:
        return self._get_specs_for_modules("input_specs_inference")

    @override(RLModule)
    def input_specs_exploration(self) -> ModelSpec:
        return self._get_specs_for_modules("input_specs_exploration")

    def _get_specs_for_modules(self, method_name: str) -> ModelSpec:
        """Returns the specs for the given property name."""
        return ModelSpec(
            {
                module_id: getattr(module, method_name)()
                for module_id, module in self._rl_modules.items()
            }
        )

    @override(RLModule)
    def _forward_train(
        self, batch: MultiAgentBatch, module_id: ModuleID = "", **kwargs
    ) -> Union[Mapping[str, Any], Dict[ModuleID, Mapping[str, Any]]]:
        return self._run_forward_pass("forward_train", batch, module_id, **kwargs)

    @override(RLModule)
    def _forward_inference(
        self, batch: MultiAgentBatch, module_id: ModuleID = "", **kwargs
    ) -> Union[Mapping[str, Any], Dict[ModuleID, Mapping[str, Any]]]:
        return self._run_forward_pass("forward_inference", batch, module_id, **kwargs)

    @override(RLModule)
    def _forward_exploration(
        self, batch: MultiAgentBatch, module_id: ModuleID = "", **kwargs
    ) -> Union[Mapping[str, Any], Dict[ModuleID, Mapping[str, Any]]]:
        return self._run_forward_pass("forward_exploration", batch, module_id, **kwargs)

    def _run_forward_pass(
        self,
        forward_fn_name: str,
        batch: MultiAgentBatch,
        module_id: ModuleID = "",
        **kwargs,
    ) -> Union[Mapping[str, Any], Dict[ModuleID, Mapping[str, Any]]]:
        if module_id:
            self._check_module_exists(module_id)
            module_ids = [module_id]
        else:
            module_ids = self.keys()

        outputs = {}
        for module_id in module_ids:
            rl_module = self._rl_modules[module_id]
            forward_fn = getattr(rl_module, forward_fn_name)
            outputs[module_id] = forward_fn(batch[module_id], **kwargs)
        return outputs

    def _check_module_exists(self, module_id: ModuleID) -> None:
        if module_id not in self._rl_modules:
            raise ValueError(
                f"Module with module_id {module_id} not found. "
                f"Available modules: {set(self.keys())}"
            )

    def _build_module_from_spec(
        self, module_spec: Union[RLModule, Mapping[str, Any]]
    ) -> RLModule:
        """Builds a module from the given module spec.

        Args:
            module_spec: The module spec to build the module from.
                module_spec can be one of the following:
                    - An RLModule instance.
                    - A dict with keys "module_class" and "module_config".
        Returns:
            The built module.
        Raises:
            ValueError: If the module spec is invalid.
        """
        if isinstance(module_spec, RLModule):
            module = module_spec
        elif isinstance(module_spec, Mapping):
            mod_class = module_spec.get("module_class")
            if mod_class is None:
                raise ValueError(
                    "key `module_class` is missing in the module "
                    "specfication of module"
                )
            mod_config = module_spec.get("module_config", {})
            module = mod_class(config=mod_config)
        else:
            raise ValueError(f"Invalid module spec for module: {module_spec}")

        return module

    def __repr__(self) -> str:
        return f"MARL({pprint.pformat(self._rl_modules)})"
