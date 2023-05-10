"""
This file holds framework-agnostic components for DreamerV3's RLModule.
"""

import abc

from ray.rllib.core.models.specs.specs_base import TensorSpec
from ray.rllib.core.models.specs.specs_dict import SpecDict
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.models.distributions import Distribution
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import ExperimentalAPI
from ray.rllib.utils.annotations import override


@ExperimentalAPI
class DreamerV3RLModule(RLModule, abc.ABC):
    # def __init__(self, config: RLModuleConfig):
    #    super().__init__(config)

    def setup(self):
        catalog = self.config.get_catalog()
        # Build model from catalog.
        self.dreamer_model = catalog.build_dreamer_model(framework=self.framework)
        self.action_dist_cls = catalog.get_action_dist_cls(framework=self.framework)

    @override(RLModule)
    def input_specs_inference(self) -> SpecDict:
        # TODO
        return self.input_specs_exploration()

    @override(RLModule)
    def output_specs_inference(self) -> SpecDict:
        # TODO
        return SpecDict({SampleBatch.ACTION_DIST: Distribution})

    @override(RLModule)
    def input_specs_exploration(self):
        # TODO
        return []

    @override(RLModule)
    def output_specs_exploration(self) -> SpecDict:
        # TODO
        return [
            SampleBatch.VF_PREDS,
            SampleBatch.ACTION_DIST,
            SampleBatch.ACTION_DIST_INPUTS,
        ]

    @override(RLModule)
    def input_specs_train(self) -> SpecDict:
        # TODO
        specs = self.input_specs_exploration()
        specs.append(SampleBatch.ACTIONS)
        if SampleBatch.OBS in specs:
            specs.append(SampleBatch.NEXT_OBS)
        return specs

    @override(RLModule)
    def output_specs_train(self) -> SpecDict:
        # TODO
        spec = SpecDict(
            {
                SampleBatch.ACTION_DIST: Distribution,
                SampleBatch.ACTION_LOGP: TensorSpec("b", framework=self.framework),
                SampleBatch.VF_PREDS: TensorSpec("b", framework=self.framework),
                "entropy": TensorSpec("b", framework=self.framework),
            }
        )
        return spec
