import abc
from typing import List, Tuple, Type

from ray.rllib.algorithms.dqn.dqn_learner import QF_PREDS, QF_TARGET_PREDS
from ray.rllib.core.models.catalog import Catalog
from ray.rllib.core.models.specs.typing import SpecType
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.core.rl_module.rl_module_with_target_networks_interface import (
    RLModuleWithTargetNetworksInterface,
)
from ray.rllib.models.distributions import Distribution
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import ExperimentalAPI, override
from ray.rllib.utils.typing import NetworkType


@ExperimentalAPI
class DQNRLModule(RLModule, RLModuleWithTargetNetworksInterface, abc.ABC):
    def setup(self):
        catalog: Catalog = self.config.get_catalog()

        # Build q network.
        self.q_encoder = catalog.build_encoder(framework=self.framework)
        self.q = catalog.build_q_head(framework=self.framework)

        # Build target network as a copy of the q network.
        self.target_encoder = catalog.build_encoder(framework=self.framework)
        self.target = catalog.build_q_head(framework=self.framework)
        # We do not want to train this network.
        self.target_encoder.trainable = False
        self.target.trainable = False

        # Get action distribution class.
        self.action_dist_cls = catalog.get_action_dist_cls(framework=self.framework)

    def get_train_action_dist_cls(self) -> Type[Distribution]:
        return self.action_dist_cls

    def get_exploration_action_dist_cls(self) -> Type[Distribution]:
        return self.action_dist_cls

    def get_inference_action_dist_cls(self) -> Type[Distribution]:
        return self.action_dist_cls

    @override(RLModule)
    def get_initial_state(self) -> dict:
        if hasattr(self.encoder, "get_initial_state"):
            return self.encoder.get_initial_state()
        else:
            return {}

    @override(RLModule)
    def input_specs_inference(self) -> SpecType:
        # TODO (simon): For RNN it needs the state.
        return self.input_specs_exploration()

    @override(RLModule)
    def output_specs_inference(self) -> SpecType:
        return self.output_specs_exploration()

    @override(RLModule)
    def input_specs_exploration(self) -> SpecType:
        return [SampleBatch.OBS]

    @override(RLModule)
    def output_specs_exploration(self) -> SpecType:
        return [SampleBatch.ACTIONS]

    @override(RLModule)
    def input_specs_train(self) -> SpecType:
        return [
            SampleBatch.OBS,
            SampleBatch.ACTIONS,
            SampleBatch.NEXT_OBS,
        ]

    @override(RLModule)
    def output_specs_train(self) -> SpecType:
        return [QF_PREDS, SampleBatch.ACTIONS, QF_TARGET_PREDS]

    @override(RLModuleWithTargetNetworksInterface)
    def get_target_network_pairs(self) -> List[Tuple[NetworkType, NetworkType]]:
        return [(self.target, self.pi), (self.target_encoder, self.q_encoder)]
