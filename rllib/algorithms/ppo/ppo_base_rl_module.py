"""
This file holds framework-agnostic components for PPO's RLModules.
"""

import abc
from typing import List

from ray.rllib.core.models.base import ActorCriticEncoder
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.models.distributions import Distribution
from ray.rllib.models.specs.specs_dict import SpecDict
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import ExperimentalAPI
from ray.rllib.utils.annotations import OverrideToImplementCustomLogic
from ray.rllib.utils.annotations import override


@ExperimentalAPI
class PPORLModuleBase(RLModule, abc.ABC):
    framework = None

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
    def input_specs_train(self) -> List[str]:
        return [SampleBatch.OBS, SampleBatch.ACTIONS, SampleBatch.ACTION_LOGP]

    @override(RLModule)
    def output_specs_train(self) -> List[str]:
        return [
            SampleBatch.ACTION_DIST_INPUTS,
            SampleBatch.ACTION_DIST,
            SampleBatch.ACTION_LOGP,
            SampleBatch.VF_PREDS,
            "entropy",
        ]

    @OverrideToImplementCustomLogic
    @override(RLModule)
    def initialize_subcomponents(self):
        catalog = self.config.get_catalog()

        # Build models from catalog
        self.encoder = catalog.build_actor_critic_encoder(framework=self.framework)
        self.pi = catalog.build_pi_head(framework=self.framework)
        self.vf = catalog.build_vf_head(framework=self.framework)

        self.action_dist_cls = catalog.get_action_dist_cls(framework=self.framework)

        assert isinstance(self.encoder, ActorCriticEncoder)
