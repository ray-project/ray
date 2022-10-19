import abc
from collections import defaultdict
from typing import Iterator, List, Mapping, Any, Union, Dict


from ray.rllib.utils.annotations import (
    ExperimentalAPI,
    OverrideToImplementCustomLogic,
    OverrideToImplementCustomLogic_CallToSuperRecommended,
    override,
)
from ray.rllib.core.base_module import Module

from ray.rllib.models.specs.specs_dict import ModelSpecDict, check_specs
from ray.rllib.models.action_dist_v2 import ActionDistributionV2
from ray.rllib.policy.sample_batch import SampleBatch, MultiAgentBatch
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
        **kwargs: Foward compatibility kwargs.

    Abstract Methods:
        forward_train: Forward pass during training.
        forward_exploration: Forward pass during training for exploration.
        forward_inference: Forward pass during inference.
    """

    def __init__(self, config: Mapping[str, Any], **kwargs) -> None:
        self.config = config

    @property
    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def output_specs_inference(self) -> ModelSpecDict:
        """Returns the output specs of the forward_inference method.

        Override this method to customize the output specs of the inference call.
        The default implementation requires the forward_inference to reutn a dict that
        has `action_dist` key and its value is an instance of `ActionDistributionV2`. This assumption must always hold.
        """
        return ModelSpecDict({"action_dist": ActionDistributionV2})

    @property
    @OverrideToImplementCustomLogic_CallToSuperRecommended
    def output_specs_exploration(self) -> ModelSpecDict:
        """Returns the output specs of the forward_exploration method.

        Override this method to customize the output specs of the inference call.
        The default implementation requires the forward_exploration to reutn a dict
        that has `action_dist` key and its value is an instance of
        `ActionDistributionV2`. This assumption must always hold.
        """
        return ModelSpecDict({"action_dist": ActionDistributionV2})

    @property
    @abc.abstractmethod
    def output_specs_train(self) -> ModelSpecDict:
        """Returns the output specs of the forward_train method."""

    @property
    def input_specs_inference(self) -> ModelSpecDict:
        """Returns the input specs of the forward_inference method."""
        return ModelSpecDict()

    @property
    def input_specs_exploration(self) -> ModelSpecDict:
        """Returns the input specs of the forward_exploration method."""
        return ModelSpecDict()

    @property
    def input_specs_train(self) -> ModelSpecDict:
        """Returns the input specs of the forward_train method."""
        return ModelSpecDict()

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


class MultiAgentRLModule(RLModule):
    def __init__(self, config: Mapping[str, Any], **kwargs) -> None:
        super().__init__(config, **kwargs)

        self._shared_module_infos = self._make_shared_module_infos()
        self._modules: Mapping[ModuleID, RLModule] = self._make_modules()

    @override(RLModule)
    def output_specs_train(self) -> ModelSpecDict:
        return self._get_specs_for_modules(self._modules, "output_specs_train")

    @override(RLModule)
    def output_specs_inference(self) -> ModelSpecDict:
        return self._get_specs_for_modules(self._modules, "output_specs_inference")

    @override(RLModule)
    def output_specs_exploration(self) -> ModelSpecDict:
        return self._get_specs_for_modules("output_specs_exploration")

    @override(RLModule)
    def input_specs_train(self) -> ModelSpecDict:
        return self._get_specs_for_modules(self._modules, "input_specs_train")

    @override(RLModule)
    def input_specs_inference(self) -> ModelSpecDict:
        return self._get_specs_for_modules(self._modules, "input_specs_inference")

    @override(RLModule)
    def input_specs_exploration(self) -> ModelSpecDict:
        return self._get_specs_for_modules(self._modules, "input_specs_exploration")

    def _get_specs_for_modules(self, property_name: str) -> ModelSpecDict:
        return ModelSpecDict(
            {
                module_id: getattr(module, property_name)
                for module_id, module in self._modules.items()
            }
        )

    @override(RLModule)
    def _forward_train(
        self, batch: MultiAgentBatch, module_id: ModuleID = "", **kwargs
    ) -> Union[Mapping[str, Any], Dict[ModuleID, Mapping[str, Any]]]:
        self._run_forward_pass("forward_train", batch, module_id, **kwargs)

    @override(RLModule)
    def _forward_inference(
        self, batch: MultiAgentBatch, module_id: ModuleID = "", **kwargs
    ) -> Union[Mapping[str, Any], Dict[ModuleID, Mapping[str, Any]]]:
        self._run_forward_pass("forward_inference", batch, module_id, **kwargs)
    
    @override(RLModule)
    def _forward_exploration(
        self, batch: MultiAgentBatch, module_id: ModuleID = "", **kwargs
    ) -> Union[Mapping[str, Any], Dict[ModuleID, Mapping[str, Any]]]:
        self._run_forward_pass("forward_exploration", batch, module_id, **kwargs)
    
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

        return {
            module_id: getattr(self._modules[module_id], forward_fn_name)(
                batch[module_id], **kwargs
            )
            for module_id in module_ids
        }

    def keys(self) -> Iterator[ModuleID]:
        """Returns the list of agent ids."""
        return self._modules.keys()

    def __getitem__(self, module_id: ModuleID) -> RLModule:
        self._check_module_exists(module_id)
        return self._modules[module_id]

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

    def _check_module_exists(self, module_id: ModuleID) -> None:
        if module_id not in self._modules:
            raise ValueError(
                f"Module with module_id {module_id} not found in ModuleDict"
            )
