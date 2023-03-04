import gymnasium as gym
from typing import Any, Mapping, Optional

from ray.rllib.core.rl_module import RLModule
from ray.rllib.core.rl_module.marl_module import MultiAgentRLModuleSpec, ModuleID
from ray.rllib.core.rl_module.torch.torch_rl_module import TorchRLModule
from ray.rllib.models.specs.typing import SpecType
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.nested_dict import NestedDict

torch, nn = try_import_torch()


class DiscreteBCTorchModule(TorchRLModule):
    def __init__(
        self,
        input_dim: int,
        hidden_dim: int,
        output_dim: int,
    ) -> None:
        super().__init__(
            input_dim=input_dim, hidden_dim=hidden_dim, output_dim=output_dim
        )
        self.policy = nn.Sequential(
            nn.Linear(input_dim, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, output_dim),
        )

        self.input_dim = input_dim

    @override(RLModule)
    def output_specs_exploration(self) -> SpecType:
        return ["action_dist"]

    @override(RLModule)
    def output_specs_inference(self) -> SpecType:
        return ["action_dist"]

    @override(RLModule)
    def output_specs_train(self) -> SpecType:
        return ["action_dist"]

    @override(RLModule)
    def _forward_inference(self, batch: NestedDict) -> Mapping[str, Any]:
        with torch.no_grad():
            return self._forward_train(batch)

    @override(RLModule)
    def _forward_exploration(self, batch: NestedDict) -> Mapping[str, Any]:
        with torch.no_grad():
            return self._forward_train(batch)

    @override(RLModule)
    def _forward_train(self, batch: NestedDict) -> Mapping[str, Any]:
        action_logits = self.policy(batch["obs"])
        return {"action_dist": torch.distributions.Categorical(logits=action_logits)}

    @classmethod
    @override(RLModule)
    def from_model_config(
        cls,
        observation_space: "gym.Space",
        action_space: "gym.Space",
        *,
        model_config_dict: Mapping[str, Any],
    ) -> "DiscreteBCTorchModule":

        config = {
            "input_dim": observation_space.shape[0],
            "hidden_dim": model_config_dict["fcnet_hiddens"][0],
            "output_dim": action_space.n,
        }

        return cls(**config)


class BCTorchRLModuleWithSharedGlobalEncoder(TorchRLModule):
    """An example of an RLModule that uses an encoder shared with other things.

    For example, we could consider a multi-agent case where for inference each agent
    needs to know the global state of the environment, as well as the local state of
    itself. For better representation learning we would like to share the encoder
    across all the modules. So this module simply accepts the encoder object as its
    input argument and uses it to encode the global state. The local state is passed
    through as is. The policy head is then a simple MLP that takes the concatenation of
    the global and local state as input and outputs the action logits.

    """

    def __init__(
        self, encoder: nn.Module, local_dim: int, hidden_dim: int, action_dim: int
    ) -> None:
        super().__init__()

        self.encoder = encoder
        self.policy_head = nn.Sequential(
            nn.Linear(hidden_dim + local_dim, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, action_dim),
        )

    @override(RLModule)
    def _default_input_specs(self):
        return [("obs", "global"), ("obs", "local")]

    @override(RLModule)
    def _forward_inference(self, batch):
        with torch.no_grad():
            return self._common_forward(batch)

    @override(RLModule)
    def _forward_exploration(self, batch):
        with torch.no_grad():
            return self._common_forward(batch)

    @override(RLModule)
    def _forward_train(self, batch):
        return self._common_forward(batch)

    def _common_forward(self, batch):
        obs = batch["obs"]
        global_enc = self.encoder(obs["global"])
        policy_in = torch.cat([global_enc, obs["local"]], dim=-1)
        action_logits = self.policy_head(policy_in)

        return {"action_dist": torch.distributions.Categorical(logits=action_logits)}


class BCTorchMultiAgentSpec(MultiAgentRLModuleSpec):

    # TODO: make sure the default class is MultiAgentRLModule

    def build(self, module_id: Optional[ModuleID] = None):

        self._check_before_build()
        # constructing the global encoder based on the observation_space of the first
        # module
        module_spec = next(iter(self.module_specs.values()))
        global_dim = module_spec.observation_space["global"].shape[0]
        hidden_dim = module_spec.model_config["fcnet_hiddens"][0]
        shared_encoder = nn.Sequential(
            nn.Linear(global_dim, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, hidden_dim),
        )

        if module_id:
            return module_spec.module_class(
                encoder=shared_encoder,
                local_dim=module_spec.observation_space["local"].shape[0],
                hidden_dim=hidden_dim,
                action_dim=module_spec.action_space.n,
            )

        rl_modules = {}
        for module_id, module_spec in self.module_specs.items():
            rl_modules[module_id] = module_spec.module_class(
                encoder=shared_encoder,
                local_dim=module_spec.observation_space["local"].shape[0],
                hidden_dim=hidden_dim,
                action_dim=module_spec.action_space.n,
            )

        return self.marl_module_class(rl_modules)
