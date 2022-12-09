import abc
import pprint
from typing import Iterator, Mapping, Any, Union, Dict

from ray.util.annotations import PublicAPI
from ray.rllib.utils.annotations import override

from ray.rllib.models.specs.specs_dict import ModelSpec
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.core.rl_module import RLModule

# TODO (Kourosh): change this to module_id later to enforce consistency
from ray.rllib.utils.policy import validate_policy_id

ModuleID = str


@PublicAPI(stability="alpha")
class MultiAgentRLModule(RLModule):
    """Base class for multi-agent RLModules.

    This class holds a mapping from module_ids to the underlying RLModules. It provides
    a convenient way of accessing each individual module, as well as accessing all of
    them with only one api call. Whether or not a given module is trainable is
    determined by the caller of this class (not the instance of this class itself).

    The extension of this class can include any arbitrary neural networks as part of
    the multi-agent module. For example, a multi-agent module can include a shared
    encoder network that is used by all the individual RLModules. It is up to the user
    to decide how to implement this class.
    """

    def __init__(self, rl_modules: Mapping[ModuleID, RLModule] = None) -> None:
        self._rl_modules: Mapping[ModuleID, RLModule] = rl_modules or {}
        super().__init__()

    @abc.abstractclassmethod
    def from_multi_agent_config(
        self, config: Mapping[str, Any]
    ) -> "MultiAgentRLModule":
        """Creates a MultiAgentRLModule from a multi-agent config.

        No assumption on the config format is made in this base class. The user is
        responsible for implementing this method.
        """

    def keys(self) -> Iterator[ModuleID]:
        """Returns an iteratable of module ids."""
        return self._rl_modules.keys()

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
            Warnings are raised if the module id is not valid according to the logic of
            validate_policy_id().
        """
        validate_policy_id(module_id)
        if module_id in self._rl_modules and not override:
            raise ValueError(
                f"Module ID {module_id} already exists. If your intention is to "
                "override, set override=True."
            )
        self._rl_modules[module_id] = module

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

    @override(RLModule)
    def make_distributed(self, dist_config: Mapping[str, Any] = None) -> None:
        # TODO (Avnish) Implement this.
        pass

    @override(RLModule)
    def is_distributed(self) -> bool:
        # TODO (Avnish) Implement this.
        return False

    def __getitem__(self, module_id: ModuleID) -> RLModule:
        """Returns the module with the given module ID.

        Args:
            module_id: The module ID to get.

        Returns:
            The module with the given module ID.
        """
        self._check_module_exists(module_id)
        return self._rl_modules[module_id]

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
        """Returns a ModelSpec from the given method_name for all modules."""
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
        """Runs the forward_train pass.

        Args:
            batch: The batch of multi-agent data (i.e. mapping from module ids to
                SampleBaches).
            module_id: The module ID to run the forward pass for. If not specified, all
                modules are run.

        Returns:
            The output of the forward_train pass the specified modules.
        """
        return self._run_forward_pass("forward_train", batch, module_id, **kwargs)

    @override(RLModule)
    def _forward_inference(
        self, batch: MultiAgentBatch, module_id: ModuleID = "", **kwargs
    ) -> Union[Mapping[str, Any], Dict[ModuleID, Mapping[str, Any]]]:
        """Runs the forward_inference pass.

        Args:
            batch: The batch of multi-agent data (i.e. mapping from module ids to
                SampleBaches).
            module_id: The module ID to run the forward pass for. If not specified, all
                modules are run.

        Returns:
            The output of the forward_inference pass the specified modules.
        """
        return self._run_forward_pass("forward_inference", batch, module_id, **kwargs)

    @override(RLModule)
    def _forward_exploration(
        self, batch: MultiAgentBatch, module_id: ModuleID = "", **kwargs
    ) -> Union[Mapping[str, Any], Dict[ModuleID, Mapping[str, Any]]]:
        """Runs the forward_exploration pass.

        Args:
            batch: The batch of multi-agent data (i.e. mapping from module ids to
                SampleBaches).
            module_id: The module ID to run the forward pass for. If not specified, all
                modules are run.

        Returns:
            The output of the forward_exploration pass the specified modules.
        """
        return self._run_forward_pass("forward_exploration", batch, module_id, **kwargs)

    def _run_forward_pass(
        self,
        forward_fn_name: str,
        batch: MultiAgentBatch,
        module_id: ModuleID = "",
        **kwargs,
    ) -> Dict[ModuleID, Mapping[str, Any]]:
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


@PublicAPI(stability="alpha")
class DefaultMultiAgentRLModule(MultiAgentRLModule):
    """The default implementation for multi-agent RLModules.

    The default implementation assumes the data communicated as input and output of
    the APIs in this class are `MultiAgentBatch` types. The `MultiAgentRLModule` simply
    loops through each `module_id`, and runs the forward pass of the corresponding
    `RLModule` object with the associated `SampleBatch` within the `MultiAgentBatch`.
    It also assumes that the underlying RLModules do not share any parameters or
    communication with one another. The behavior of modules with such advanced
    communication would be undefined by default. To share parameters or communication
    between the underlying RLModules, you should implement your own
    `MultiAgentRLModule`.
    """

    def __init__(self, rl_modules: Mapping[ModuleID, RLModule] = None) -> None:
        super().__init__(rl_modules)

    @classmethod
    @override(MultiAgentRLModule)
    def from_multi_agent_config(cls, config: Mapping[str, Any]) -> "MultiAgentRLModule":
        """Creates a MultiAgentRLModule from a multi-agent config.

        # TODO (Kourosh): Link to example of custom MultiAgentRLModule once it exists.

        Args:
            config: A Mapping from module_id to each RLModule config. Each RLModule
                config value should be a tuple in the following order:
                `module_class`: The RLModule class Type or full path separate by `.`.
                    (e.g. `ray.rllib.algoirhms.dqn.torch.DQNTorchRLModule`).
                `module_config`: The config object for the RLModule.

            Returns:
                The MultiAgentRLModule.
        """
        ma_module = cls()

        for module_id, module_spec in config.items():
            module = cls._build_module_from_spec(module_spec)
            ma_module.add_module(module_id, module)

        return ma_module

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

    @classmethod
    def _build_module_from_spec(
        cls, module_spec: Union[RLModule, Mapping[str, Any]]
    ) -> RLModule:
        """Builds a module from the given module spec.

        Args:
            module_spec: a tuple "module_class" and "module_config".
        Returns:
            The built module.
        Raises:
            ValueError: If the module spec is invalid.
        """
        mod_class, mod_config = module_spec
        if mod_class is None:
            raise ValueError(
                "`module_class` is missing in the module spec of the module"
            )
        module = mod_class(config=mod_config)
        return module

    def __repr__(self) -> str:
        return f"MARL({pprint.pformat(self._rl_modules)})"
