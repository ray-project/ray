import abc
from typing import List

from ray.rllib.core.models.configs import RecurrentEncoderConfig
from ray.rllib.core.rl_module.apis import InferenceOnlyAPI
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.annotations import (
    OverrideToImplementCustomLogic_CallToSuperRecommended,
    override,
)
from ray.util.annotations import DeveloperAPI


def _setup_mappo_encoder(module: RLModule) -> None:
    """Shared encoder setup logic for MAPPO actor and critic modules.

    Handles stateful / inference-only configuration and builds the encoder
    from the module's catalog.
    """
    is_stateful = isinstance(
        module.catalog.encoder_config,
        RecurrentEncoderConfig,
    )
    if is_stateful:
        module.inference_only = False
    if module.inference_only and module.framework == "torch":
        module.catalog.encoder_config.inference_only = True
    module.encoder = module.catalog.build_encoder(framework=module.framework)


@DeveloperAPI
class DefaultMAPPORLModule(RLModule, InferenceOnlyAPI, abc.ABC):
    """Default actor RLModule for MAPPO (no value head -- critic is shared)."""

    @override(RLModule)
    def setup(self):
        _setup_mappo_encoder(self)
        self.pi = self.catalog.build_pi_head(framework=self.framework)

    @override(RLModule)
    def get_initial_state(self) -> dict:
        if hasattr(self.encoder, "get_initial_state"):
            return self.encoder.get_initial_state()
        return {}

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    @override(InferenceOnlyAPI)
    def get_non_inference_attributes(self) -> List[str]:
        return []
