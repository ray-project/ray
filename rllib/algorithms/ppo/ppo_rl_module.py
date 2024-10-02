"""
This file holds framework-agnostic components for PPO's RLModules.
"""

import abc
from typing import List

from ray.rllib.core.columns import Columns
from ray.rllib.core.models.configs import RecurrentEncoderConfig
from ray.rllib.core.models.specs.specs_dict import SpecDict
from ray.rllib.core.rl_module.apis import InferenceOnlyAPI, ValueFunctionAPI
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
            self.config.inference_only = False
        # If this is an `inference_only` Module, we'll have to pass this information
        # to the encoder config as well.
        if self.config.inference_only and self.framework == "torch":
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

    @override(RLModule)
    def input_specs_inference(self) -> SpecDict:
        return [Columns.OBS]

    @override(RLModule)
    def output_specs_inference(self) -> SpecDict:
        return [Columns.ACTION_DIST_INPUTS]

    @override(RLModule)
    def input_specs_exploration(self):
        return self.input_specs_inference()

    @override(RLModule)
    def output_specs_exploration(self) -> SpecDict:
        return self.output_specs_inference()

    @override(RLModule)
    def input_specs_train(self) -> SpecDict:
        return self.input_specs_exploration()

    @override(RLModule)
    def output_specs_train(self) -> SpecDict:
        return [
            Columns.VF_PREDS,
            Columns.ACTION_DIST_INPUTS,
        ]

    @OverrideToImplementCustomLogic_CallToSuperRecommended
    @override(InferenceOnlyAPI)
    def get_non_inference_attributes(self) -> List[str]:
        """Return attributes, which are NOT inference-only (only used for training)."""
        return ["vf"] + (
            []
            if self.config.model_config_dict.get("vf_share_layers")
            else ["encoder.critic_encoder"]
        )
