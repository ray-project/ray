import abc
from typing import Any, Type

from ray.rllib.core.models.catalog import Catalog
from ray.rllib.core.models.specs.typing import SpecType
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.models.distributions import Distribution
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import ExperimentalAPI, override

@ExperimentalAPI
class DQNRLModule(RLModule, abc.ABC):
    def setup(self):
        catalog: Catalog = self.config.get_catalog()

        # Build models from catalog.
        self.encoder = catalog.build_q_and_target_encoder(framework=self.framework)
        self.q = catalog.build_q_head(framework=self.framework)
        self.target = catalog.build_target_head(framework=self.framework)

        self.action_dist_cls = catalog.get_action_dist_cls(framework=self.framework)

    def get_train_action_cls(self) -> Type[Distribution]:
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
        return self.input_specs_exploration()
    
    @override(RLModule)
    def output_specs_inference(self) -> SpecType:
        return [SampleBatch.ACTIONS]
    
    @override(RLModule)
    def input_specs_exploration(self) -> SpecType:
        return [SampleBatch.OBS]
    
    @override(RLModule)
    def output_specs_exploration(self) -> SpecType:
        pass