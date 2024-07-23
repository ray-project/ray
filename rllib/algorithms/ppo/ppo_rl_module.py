"""
This file holds framework-agnostic components for PPO's RLModules.
"""

import abc
from typing import Type

from ray.rllib.core.columns import Columns
from ray.rllib.core.models.configs import RecurrentEncoderConfig
from ray.rllib.core.models.specs.specs_dict import SpecDict
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.models.distributions import Distribution
from ray.rllib.utils.annotations import ExperimentalAPI
from ray.rllib.utils.annotations import override

# TODO (simon): Write a light-weight version of this class for the `TFRLModule`


@ExperimentalAPI
class PPORLModule(RLModule, abc.ABC):
    def setup(self):
        # __sphinx_doc_begin__
        catalog = self.config.get_catalog()
        # If we have a stateful model, states for the critic need to be collected
        # during sampling and `inference-only` needs to be `False`. Note, at this
        # point the encoder is not built, yet and therefore `is_stateful()` does
        # not work.
        is_stateful = isinstance(
            catalog.actor_critic_encoder_config.base_encoder_config,
            RecurrentEncoderConfig,
        )
        if is_stateful:
            self.config.inference_only = False
        # If this is an `inference_only` Module, we'll have to pass this information
        # to the encoder config as well.
        if self.config.inference_only and self.framework == "torch":
            catalog.actor_critic_encoder_config.inference_only = True

        # Build models from catalog
        self.encoder = catalog.build_actor_critic_encoder(framework=self.framework)
        self.pi = catalog.build_pi_head(framework=self.framework)

        # Only build the critic network when this is a learner module.
        if not self.config.inference_only or self.framework != "torch":
            self.vf = catalog.build_vf_head(framework=self.framework)
            # Holds the parameter names to be removed or renamed when synching
            # from the learner to the inference module.
            self._inference_only_state_dict_keys = {}

        self.action_dist_cls = catalog.get_action_dist_cls(framework=self.framework)
        # __sphinx_doc_end__

    @override(RLModule)
    def get_train_action_dist_cls(self) -> Type[Distribution]:
        return self.action_dist_cls

    @override(RLModule)
    def get_exploration_action_dist_cls(self) -> Type[Distribution]:
        return self.action_dist_cls

    @override(RLModule)
    def get_inference_action_dist_cls(self) -> Type[Distribution]:
        return self.action_dist_cls

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
