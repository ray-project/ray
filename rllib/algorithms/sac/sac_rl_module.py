from typing import Type
from ray.rllib.algorithms.sac.sac_catalog import SACCatalog
from ray.rllib.core.models.specs.typing import SpecType
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.models.distributions import Distribution
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import ExperimentalAPI, override


@ExperimentalAPI
class SACRLModule(RLModule):
    def setup(self):
        # __sphinx_doc_begin__
        super().setup()
        catalog: SACCatalog = self.config.get_catalog()

        # SAC needs next to the Q function estimator also a
        # value function estimator and the pi network.
        self.pi_and_vf_encoder = catalog.build_actor_critic_encoder(
            framework=self.framework
        )
        self.qf_encoder = catalog.build_encoder(framework=self.framework)
        self.pi = catalog.build_pi_head(framework=self.framework)
        self.vf = catalog.build_vf_head(framework=self.framework)
        self.qf = catalog.build_qf_head(framework=self.framework)

        self.action_dist_cls = catalog.get_action_dist_cls(framework=self.framework)
        # __sphinx_doc_end__

    def get_exploration_action_dist_cls(self) -> Type[Distribution]:
        return self.action_dist_cls

    def get_inference_action_dist_cls(self) -> Type[Distribution]:
        return self.action_dist_cls

    def get_train_action_dist_cls(self) -> Type[Distribution]:
        return self.action_dist_cls

    @override(RLModule)
    def get_initial_state(self) -> dict:
        if hasattr(self.pi_and_vf_encoder, "get_initial_state"):
            return self.pi_and_vf_encoder.get_initial_state()
        else:
            return {}

    @override(RLModule)
    def input_specs_exploration(self) -> SpecType:
        return [SampleBatch.OBS]

    @override(RLModule)
    def input_specs_inference(self) -> SpecType:
        return [SampleBatch.OBS]

    @override(RLModule)
    def input_specs_train(self) -> SpecType:
        return [SampleBatch.OBS, SampleBatch.NEXT_OBS]

    @override(RLModule)
    def output_specs_train(self) -> SpecType:
        return [SampleBatch.AC]
