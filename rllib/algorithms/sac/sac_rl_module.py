from typing import Type
from ray.rllib.algorithms.sac.sac_catalog import SACCatalog

# from ray.rllib.algorithms.sac.sac_learner import QF_PREDS
# from ray.rllib.core.models.base import ACTOR, CRITIC
from ray.rllib.core.models.specs.typing import SpecType
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.core.rl_module.rl_module_with_target_networks_interface import (
    RLModuleWithTargetNetworksInterface,
)
from ray.rllib.models.distributions import Distribution
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import ExperimentalAPI, override

CRITIC_TARGET = "critic_target"
QF_PREDS = "qf_preds"


@ExperimentalAPI
class SACRLModule(RLModule, RLModuleWithTargetNetworksInterface):
    def setup(self):
        # __sphinx_doc_begin__
        # super().setup()
        catalog: SACCatalog = self.config.get_catalog()

        # Build the encoder for the policy.
        self.pi_encoder = catalog.build_encoder(framework=self.framework)

        # SAC needs next to the pi network also a Q network with encoder.
        # Note, as the Q network uses next to the observations, actions as
        # input we need an additional encoder for this.
        self.qf_encoder = catalog.build_qf_encoder(framework=self.framework)

        # Build the target Q encoder as an exact copy of the Q encoder.
        # TODO (simon): Maybe merging encoders together for target and qf
        # and keep only the heads differently?
        self.qf_target_encoder = catalog.build_qf_encoder(framework=self.framework)

        # Build heads.
        self.pi = catalog.build_pi_head(framework=self.framework)
        self.qf = catalog.build_qf_head(framework=self.framework)
        # The Q target network head is an identical copy of the Q network head.
        self.qf_target = catalog.build_qf_head(framework=self.framework)

        # We do not want to train the target network.
        self.qf_target_encoder.trainable = False
        self.qf_target.trainable = False

        self.action_dist_cls = catalog.get_action_dist_cls(framework=self.framework)

        # Define the temperature.
        self.alpha = self.config.model_config_dict["initial_alpha"]
        # __sphinx_doc_end__

    def get_exploration_action_dist_cls(self) -> Type[Distribution]:
        return self.action_dist_cls

    def get_inference_action_dist_cls(self) -> Type[Distribution]:
        return self.action_dist_cls

    def get_train_action_dist_cls(self) -> Type[Distribution]:
        return self.action_dist_cls

    @override(RLModule)
    def get_initial_state(self) -> dict:
        # if hasattr(self.pi_encoder, "get_initial_state"):
        #     return {
        #         ACTOR: self.pi_encoder.get_initial_state(),
        #         CRITIC: self.qf_encoder.get_initial_state(),
        #         CRITIC_TARGET: self.qf_target_encoder.get_initial_state(),
        #     }
        # else:
        #     return {}
        return {}

    @override(RLModule)
    def input_specs_exploration(self) -> SpecType:
        return [SampleBatch.OBS]

    @override(RLModule)
    def input_specs_inference(self) -> SpecType:
        return [SampleBatch.OBS]

    @override(RLModule)
    def input_specs_train(self) -> SpecType:
        return [
            SampleBatch.OBS,
            SampleBatch.ACTIONS,
            SampleBatch.NEXT_OBS,
        ]

    @override(RLModule)
    def output_specs_exploration(self) -> SpecType:
        return [SampleBatch.ACTION_DIST_INPUTS]

    @override(RLModule)
    def output_specs_inference(self) -> SpecType:
        return [SampleBatch.ACTION_DIST_INPUTS]

    @override(RLModule)
    def output_specs_train(self) -> SpecType:
        return [
            QF_PREDS,
            SampleBatch.ACTION_DIST_INPUTS,
            "action_dist_inputs_next",
        ]
