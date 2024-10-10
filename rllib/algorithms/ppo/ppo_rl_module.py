"""
This file holds framework-agnostic components for PPO's RLModules.
"""

import abc
import dataclasses
from typing import List

from ray.rllib.core.models.configs import RecurrentEncoderConfig
from ray.rllib.core.rl_module.apis import InferenceOnlyAPI, ValueFunctionAPI
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.annotations import (
    override,
    OverrideToImplementCustomLogic_CallToSuperRecommended,
)
from ray.util.annotations import DeveloperAPI


@DeveloperAPI(stability="alpha")
class PPORLModule(RLModule, InferenceOnlyAPI, ValueFunctionAPI, abc.ABC):
    @override(RLModule)
    def setup(self):
        # TODO (sven): Temporary fix (until we have figured out the final config
        #  architecture for default models). If self.model_config is a dict (which
        #  it should always be, make sure to merge it with the default config).
        self.model_config = dataclasses.asdict(DefaultModelConfig()) | self.model_config

        # __sphinx_doc_begin__
        # If we have a stateful model, states for the critic need to be collected
        # during sampling and `inference-only` needs to be `False`. Note, at this
        # point the encoder is not built, yet and therefore `is_stateful()` does
        # not work.
        is_stateful = isinstance(
            self.catalog.actor_critic_encoder_config.base_encoder_config,
            RecurrentEncoderConfig,
        )
        if is_stateful:
            self.inference_only = False
        # If this is an `inference_only` Module, we'll have to pass this information
        # to the encoder config as well.
        if self.inference_only and self.framework == "torch":
            self.catalog.actor_critic_encoder_config.inference_only = True

        # Build models from catalog.
        self.encoder = self.catalog.build_actor_critic_encoder(framework=self.framework)
        self.pi = self.catalog.build_pi_head(framework=self.framework)
        self.vf = self.catalog.build_vf_head(framework=self.framework)
        # __sphinx_doc_end__

    @override(RLModule)
    def get_initial_state(self) -> dict:
        if hasattr(self.encoder, "get_initial_state"):
            return self.encoder.get_initial_state()
        else:
            return {}

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    @override(InferenceOnlyAPI)
    def get_non_inference_attributes(self) -> List[str]:
        """Return attributes, which are NOT inference-only (only used for training)."""
        return ["vf"] + (
            []
            if self.model_config.get("vf_share_layers")
            else ["encoder.critic_encoder"]
        )
