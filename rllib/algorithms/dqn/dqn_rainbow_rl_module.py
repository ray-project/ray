from typing import Any
from ray.rllib.core.models.specs.typing import SpecType
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.core.rl_module.rl_module_with_target_networks_interface import (
    RLModuleWithTargetNetworksInterface,
)
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import ExperimentalAPI, override


@ExperimentalAPI
class DQNRainbowRLModule(RLModule, RLModuleWithTargetNetworksInterface):
    @override(RLModule)
    def setup(self):
        # Get the DQN Rainbow catalog.
        catalog = self.config.get_catalog()

        # Build the encoder for the advantage and value streams. Note,
        # the same encoder is used, if no dueling setting is used.
        self.base_encoder = catalog.build_encoder()
        # Build the same encoder for the target network(s).
        self.base_target_encoder = catalog.build_encoder()

        # Build heads.
        # TODO (simon): Implement noisy linear layers.
        # TODO (simon): Implement dueling=False.
        self.af = catalog.build_af_head(framework=self.framework)
        self.vf = catalog.build_vf_head(framework=self.framework)
        # Implement the same heads for the target network(s).
        self.af_target = catalog.build_af_head(framework=self.framework)
        self.vf_target = catalog.build_vf_head(framework=self.framework)

        # TODO (simon): Implement double q.

        # We do not want to train the target networks.
        self.base_target_encoder.trainable = False
        self.af_target.trainable = False
        self.vf_target.trainable = False

    # TODO (simon): DQN Rainbow does not support RNNs, yet.
    @override(RLModule)
    def get_initial_state(self) -> Any:
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
        return
