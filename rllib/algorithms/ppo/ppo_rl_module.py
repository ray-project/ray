"""
This file holds framework-agnostic components for PPO's RLModules.
"""

import abc
from typing import Any, Type

from ray.rllib.core.columns import Columns
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
        # If this is not a learner module, we use only a single value network. This
        # network is then either the share encoder network from the learner module
        # or the actor encoder network from the learner module (if the value network
        # is not shared with the actor network).
        if self.inference_only and self.framework == "torch":
            # catalog._model_config_dict["vf_share_layers"] = True
            # We need to set the shared flag in the encoder config
            # b/c the catalog has already been built at this point.
            catalog.actor_critic_encoder_config.shared = True

        # Build models from catalog
        self.encoder = catalog.build_actor_critic_encoder(framework=self.framework)
        self.pi = catalog.build_pi_head(framework=self.framework)
        # Only build the critic network when this is a learner module.
        if not self.inference_only or self.framework != "torch":
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

    @abc.abstractmethod
    def _compute_values(self, batch) -> Any:
        """Computes values using the vf-specific network(s) and given a batch of data.

        Args:
            batch: The input batch to pass through this RLModule (value function
                encoder and vf-head).

        Returns:
            A dict mapping ModuleIDs to batches of value function outputs (already
            squeezed on the last dimension (which should have shape (1,) b/c of the
            single value output node). However, for complex multi-agent settings with
            shareed value networks, the output might look differently (e.g. a single
            return batch without the ModuleID-based mapping).
        """
