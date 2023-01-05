import torch.nn as nn
from typing import Any, Mapping, Dict, Union

from ray.rllib.core.rl_module.marl_module import MultiAgentRLModule
from ray.rllib.core.rl_module.rl_module import ModuleID, RLModule
from ray.rllib.models.specs.specs_torch import TorchTensorSpec
from ray.rllib.models.specs.specs_dict import SpecDict
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils.annotations import override

from ray.rllib.utils.nested_dict import NestedDict


class MaModuleWObsEncoder(MultiAgentRLModule):
    def __init__(
        self, input_dim_encoder, hidden_dim_encoder, output_dim_encoder, modules
    ):
        super().__init__(modules)
        self.encoder = nn.Sequential(
            nn.Linear(input_dim_encoder, hidden_dim_encoder),
            nn.ReLU(),
            nn.Linear(hidden_dim_encoder, output_dim_encoder),
        )
        self.input_encoder_dim = input_dim_encoder

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
        return self.__run_forward_pass("forward_train", batch, module_id, **kwargs)

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
        return self.__run_forward_pass("forward_inference", batch, module_id, **kwargs)

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
        return self.__run_forward_pass(
            "forward_exploration", batch, module_id, **kwargs
        )

    def __run_forward_pass(
        self,
        forward_fn_name: str,
        batch: NestedDict,
        module_id: ModuleID = "",
        **kwargs,
    ) -> Dict[ModuleID, Mapping[str, Any]]:
        for _module_id in batch.shallow_keys():
            agent_batch = batch[_module_id]
            agent_batch["obs"] = self.encoder(agent_batch["obs"])

        module_ids = self.keys() if module_id == "" else [module_id]

        outputs = {}
        for module_id in module_ids:
            rl_module = self._rl_modules[module_id]
            forward_fn = getattr(rl_module, forward_fn_name)
            outputs[module_id] = forward_fn(batch[module_id], **kwargs)

        return outputs

    @override(MultiAgentRLModule)
    def input_specs_exploration(self) -> SpecDict:
        sd = super().input_specs_exploration()
        for module_id in sd.keys():
            if "obs" in module_id:
                sd[module_id] = TorchTensorSpec("b, do", do=self.input_encoder_dim)
        return sd

    @override(MultiAgentRLModule)
    def input_specs_inference(self) -> SpecDict:
        sd = super().input_specs_inference()
        for module_id in sd.keys():
            if "obs" in module_id:
                sd[module_id] = TorchTensorSpec("b, do", do=self.input_encoder_dim)
        return sd

    @override(MultiAgentRLModule)
    def input_specs_train(self) -> SpecDict:
        sd = super().input_specs_train()
        for module_id in sd.keys():
            if "obs" in module_id:
                sd[module_id] = TorchTensorSpec("b, do", do=self.input_encoder_dim)
        return sd

    @classmethod
    def from_multi_agent_config(
        cls, config: Mapping[str, Any]
    ) -> "MaModuleWObsEncoder":
        encoder_config = config.pop("encoder")
        modules_config = config.pop("modules")

        modules_dict = {}

        for module_id, module_spec in modules_config.items():
            module_cls = module_spec.pop("module_class")
            module = module_cls(**module_spec)
            modules_dict[module_id] = module

        return cls(**encoder_config, modules=modules_dict)
