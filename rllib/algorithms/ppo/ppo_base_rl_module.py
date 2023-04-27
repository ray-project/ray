"""
This file holds framework-agnostic components for PPO's RLModules.
"""

import abc

from ray.rllib.core.models.base import ActorCriticEncoder
from ray.rllib.core.models.specs.specs_base import TensorSpec
from ray.rllib.core.models.specs.specs_dict import SpecDict
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.core.rl_module.rl_module import RLModuleConfig
from ray.rllib.models.distributions import Distribution
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import ExperimentalAPI
from ray.rllib.utils.annotations import override


@ExperimentalAPI
class PPORLModuleBase(RLModule, abc.ABC):
    def __init__(self, config: RLModuleConfig):
        super().__init__(config)

    def setup(self):
        # __sphinx_doc_begin__
        catalog = self.config.get_catalog()

        # Build models from catalog
        self.encoder = catalog.build_actor_critic_encoder(framework=self.framework)
        self.pi = catalog.build_pi_head(framework=self.framework)
        self.vf = catalog.build_vf_head(framework=self.framework)

        self.action_dist_cls = catalog.get_action_dist_cls(framework=self.framework)
        # __sphinx_doc_end__

        assert isinstance(self.encoder, ActorCriticEncoder)

    @override(RLModule)
    def input_specs_inference(self) -> SpecDict:
        return self.input_specs_exploration()

    @override(RLModule)
    def output_specs_inference(self) -> SpecDict:
        return SpecDict({SampleBatch.ACTION_DIST: Distribution})

    @override(RLModule)
    def input_specs_exploration(self):
        return []

    @override(RLModule)
    def output_specs_exploration(self) -> SpecDict:
        return [
            SampleBatch.VF_PREDS,
            SampleBatch.ACTION_DIST,
            SampleBatch.ACTION_DIST_INPUTS,
        ]

    @override(RLModule)
    def input_specs_train(self) -> SpecDict:
        specs = self.input_specs_exploration()
        specs.append(SampleBatch.ACTIONS)
        if SampleBatch.OBS in specs:
            specs.append(SampleBatch.NEXT_OBS)
        return specs

    @override(RLModule)
    def output_specs_train(self) -> SpecDict:
        spec = SpecDict(
            {
                SampleBatch.ACTION_DIST: Distribution,
                SampleBatch.ACTION_LOGP: TensorSpec("b", framework=self.framework),
                SampleBatch.VF_PREDS: TensorSpec("b", framework=self.framework),
                "entropy": TensorSpec("b", framework=self.framework),
            }
        )
        return spec
