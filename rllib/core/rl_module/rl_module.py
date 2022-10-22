import abc
from collections import defaultdict
from typing import Iterator, Mapping, Any, Union, Dict, Type, Set
import pprint


from ray.rllib.utils.annotations import (
    ExperimentalAPI,
    OverrideToImplementCustomLogic,
    OverrideToImplementCustomLogic_CallToSuperRecommended,
    override,
)

from ray.rllib.models.specs.specs_dict import ModelSpec, check_specs
from ray.rllib.models.action_dist_v2 import ActionDistributionV2
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID, MultiAgentBatch
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.utils.typing import SampleBatchType

ModuleID = str


@ExperimentalAPI
class RLModule(abc.ABC):
    """Base class for RLlib modules.

    Here is the pseudo code for how the forward methods are called:

    # During Training (acting in env from each rollout worker)
    ----------------------------------------------------------
    .. code-block:: python

        module: RLModule = ...
        obs = env.reset()
        while not done:
            fwd_outputs = module.forward_exploration({"obs": obs})
            # this can be deterministic or stochastic exploration
            action = fwd_outputs["action_dist"].sample()
            next_obs, reward, done, info = env.step(action)
            buffer.add(obs, action, next_obs, reward, done, info)
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
        obs = env.reset()
        while not done:
            fwd_outputs = module.forward_inference({"obs": obs})
            # this can be deterministic or stochastic evaluation
            action = fwd_outputs["action_dist"].sample()
            next_obs, reward, done, info = env.step(action)
            next_obs = obs

    Args:
        config: The config object for the module.

    Abstract Methods:
        forward_train: Forward pass during training.
        forward_exploration: Forward pass during training for exploration.
        forward_inference: Forward pass during inference.

    Note: There is a reason that the specs are not written as abstract properties.
        The reason is that torch overrides `__getattr__` and `__setattr__`. This means
        that if we define the specs as properties, then any error in the property will
        be interpreted as a failure to retrieve the attribute and will invoke
        `__getattr__` which will give a confusing error about the attribute not found.
        More details here: https://github.com/pytorch/pytorch/issues/49726.
    """

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def __init__(self, config: Mapping[str, Any]) -> None:
        self.config = config

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def output_specs_inference(self) -> ModelSpec:
        """Returns the output specs of the forward_inference method.

        Override this method to customize the output specs of the inference call.
        The default implementation requires the forward_inference to reutn a dict that
        has `action_dist` key and its value is an instance of `ActionDistributionV2`.
        This assumption must always hold.
        """
        return ModelSpec({"action_dist": ActionDistributionV2})

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def output_specs_exploration(self) -> ModelSpec:
        """Returns the output specs of the forward_exploration method.

        Override this method to customize the output specs of the inference call.
        The default implementation requires the forward_exploration to reutn a dict
        that has `action_dist` key and its value is an instance of
        `ActionDistributionV2`. This assumption must always hold.
        """
        return ModelSpec({"action_dist": ActionDistributionV2})

    def output_specs_train(self) -> ModelSpec:
        """Returns the output specs of the forward_train method."""
        return ModelSpec()

    def input_specs_inference(self) -> ModelSpec:
        """Returns the input specs of the forward_inference method."""
        return ModelSpec()

    def input_specs_exploration(self) -> ModelSpec:
        """Returns the input specs of the forward_exploration method."""
        return ModelSpec()

    def input_specs_train(self) -> ModelSpec:
        """Returns the input specs of the forward_train method."""
        return ModelSpec()

    @check_specs(
        input_spec="input_specs_inference", output_spec="output_specs_inference"
    )
    def forward_inference(self, batch: SampleBatchType, **kwargs) -> Mapping[str, Any]:
        """Forward-pass during evaluation, called from the sampler. This method should
        not be overriden. Instead, override the _forward_inference method."""
        return self._forward_inference(batch, **kwargs)

    @abc.abstractmethod
    def _forward_inference(self, batch: NestedDict, **kwargs) -> Mapping[str, Any]:
        """Forward-pass during evaluation"""

    @check_specs(
        input_spec="input_specs_exploration", output_spec="output_specs_exploration"
    )
    def forward_exploration(
        self, batch: SampleBatchType, **kwargs
    ) -> Mapping[str, Any]:
        """Forward-pass during exploration, called from the sampler. This method should
        not be overriden. Instead, override the _forward_exploration method."""
        return self._forward_exploration(batch, **kwargs)

    @abc.abstractmethod
    def _forward_exploration(self, batch: NestedDict, **kwargs) -> Mapping[str, Any]:
        """Forward-pass during exploration"""

    @check_specs(input_spec="input_specs_train", output_spec="output_specs_train")
    def forward_train(self, batch: SampleBatchType, **kwargs) -> Mapping[str, Any]:
        """Forward-pass during training called from the trainer. This method should
        not be overriden. Instead, override the _forward_train method."""
        return self._forward_train(batch, **kwargs)

    @abc.abstractmethod
    def _forward_train(self, batch: NestedDict, **kwargs) -> Mapping[str, Any]:
        """Forward-pass during training"""

    @abc.abstractmethod
    def get_state(self) -> Mapping[str, Any]:
        """Returns the state dict of the module."""

    @abc.abstractmethod
    def set_state(self, state_dict: Mapping[str, Any]) -> None:
        """Sets the state dict of the module."""

    @abc.abstractmethod
    def make_distributed(self, dist_config: Mapping[str, Any] = None) -> None:
        """Makes the module distributed."""

    @abc.abstractmethod
    def is_distributed(self) -> bool:
        """Returns True if the module is distributed."""

    def as_multi_agent(self) -> "MultiAgentRLModule":
        """Returns a multi-agent wrapper around this module."""
        return self.get_multi_agent_class()({"modules": {DEFAULT_POLICY_ID: self}})

    @classmethod
    @OverrideToImplementCustomLogic
    def get_multi_agent_class(cls) -> Type["MultiAgentRLModule"]:
        """Returns the multi-agent wrapper class for this module."""
        return MultiAgentRLModule


class MultiAgentRLModule(RLModule):
    def __init__(self, config: Mapping[str, Any], **kwargs) -> None:
        super().__init__(config, **kwargs)

        self._rl_modules: Mapping[ModuleID, RLModule] = self._make_modules()
        self._trainable_rl_modules = self.config.get(
            "trainable_modules", set(self._rl_modules.keys())
        )

    def keys(self) -> Iterator[ModuleID]:
        """Returns an iteratable of module ids."""
        return self._rl_modules.keys()

    def get_trainable_module_ids(self) -> Set[ModuleID]:
        """Returns the ids of the trainable modules."""
        return self._trainable_rl_modules

    @override(RLModule)
    def make_distributed(self, dist_config: Mapping[str, Any] = None) -> None:
        for module in self._rl_modules.values():
            module.make_distributed(dist_config)

    @override(RLModule)
    def is_distributed(self) -> bool:
        return all(module.is_distributed() for module in self._rl_modules.values())

    @override(RLModule)
    def get_state(self) -> Mapping[str, Any]:
        return {
            module_id: module.get_state()
            for module_id, module in self._rl_modules.items()
        }

    @override(RLModule)
    def set_state(self, state_dict: Mapping[str, Any]) -> None:
        """Sets the state dict of the multi-agent module.

        The default implementation is a mapping from independent module IDs to their
        individual state_dicts. Override this method to customize the state_dict for
        custom more advanced multi-agent use cases.

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
        module: RLModule,
        *,
        dist_config=None,
        override: bool = False,
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

        Raises:
            ValueError: If the module ID already exists and override is False.
        """
        if module_id in self._rl_modules and not override:
            raise ValueError(
                f"Module ID {module_id} already exists. If your intention is to "
                "override, set override=True."
            )
        if self.is_distributed():
            module.make_distributed(dist_config)
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
            ValueError: If the module ID does not exist.
        """
        if raise_err_if_not_found:
            self._check_module_exists(module_id)
        del self._rl_modules[module_id]

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
    def _make_modules(self) -> Mapping[ModuleID, RLModule]:
        modules_info_dict = self.config["modules"]
        modules = {}
        for module_id, module_info in modules_info_dict.items():

            if isinstance(module_info, RLModule):
                modules[module_id] = module_info
            elif isinstance(module_info, dict):
                mod_class = module_info.get("module_class")
                if mod_class is None:
                    raise KeyError(
                        f"key `module_class` is missing in the module "
                        f"specfication of module {module_id}"
                    )
                mod_config = module_info.get("module_config", {})
                modules[module_id] = mod_class(config=mod_config)
            elif isinstance(module_id, tuple):
                if len(module_id) != 2:
                    raise ValueError(
                        f"module {module_id} should has 2 elements: "
                        f"(module_class, module_config)"
                    )
                mod_class, mod_config = module_info
                modules[module_id] = mod_class(config=mod_config)
            else:
                raise ValueError(
                    f"Invalid module info for module {module_id}: {module_info}"
                )

        return modules

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

    def _get_specs_for_modules(self, property_name: str) -> ModelSpec:
        return ModelSpec(
            {
                module_id: getattr(module, property_name)()
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
            outputs[module_id] = forward_fn(batch.get(module_id), **kwargs)
        return outputs

    def _check_module_exists(self, module_id: ModuleID) -> None:
        if module_id not in self._rl_modules:
            raise ValueError(
                f"Module with module_id {module_id} not found. "
                f"Available modules: {set(self.keys())}"
            )

    def __repr__(self) -> str:
        return f"MARL({pprint.pformat(self._rl_modules)})"


# TODO (Kourosh): I don't know if this will be used at all in the future, but let's
# keep it for now
class MultiAgentRLModuleWithSimpleSharedSubmodules(MultiAgentRLModule):
    def __init__(self, config: Mapping[str, Any], **kwargs) -> None:
        self._shared_module_infos = self._make_shared_module_infos()
        super().__init__(config, **kwargs)

    def _make_shared_module_infos(self):
        config = self.configs
        shared_config = config["shared_submodules"]

        shared_mod_infos = defaultdict({})  # mapping from module_id to kwarg and value
        for mod_info in shared_config.values():
            mod_class = mod_info["class"]
            mod_config = mod_info["config"]
            mod_obj = mod_class(mod_config)

            for module_id, kw in mod_info["shared_between"].items():
                shared_mod_infos[module_id][kw] = mod_obj

        """
        shared_mod_infos = 'policy_kwargs'{
            'A': {'encoder': encoder, 'dynamics': dyna},
            'B': {'encoder': encoder, 'dynamics': dyna},
            '__all__': {'mixer': mixer}
        }
        """
        return shared_mod_infos

    @override(MultiAgentRLModule)
    def _make_modules(self):
        shared_mod_info = self.shared_module_infos
        policies = self.config["multi_agent"]["modules"]
        modules = {}
        for pid, pid_info in policies.items():
            # prepare the module parameters and class type
            rl_mod_class = pid_info["module_class"]
            rl_mod_config = pid_info["module_config"]
            kwargs = shared_mod_info[pid]
            rl_mod_config.update(**kwargs)

            # create the module instance
            rl_mod_obj = rl_mod_class(config=rl_mod_config)
            modules[pid] = rl_mod_obj

        return modules
