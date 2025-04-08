from ray.rllib.core.columns import Columns
from ray.rllib.core.models.specs.typing import SpecType
from ray.rllib.core.rl_module import RLModule
from ray.rllib.utils.annotations import override


class DefaultBCIRLRewardRLModule(RLModule):
    @override(RLModule)
    def setup(self):
        # Configure the reward-function encoder.
        self.rf_encoder = self.catalog.build_rf_encoder(framework=self.framework)
        # Configure the reward-function head.
        self.rf = self.catalog.build_rf_head(framework=self.framework)

    @override(RLModule)
    def get_initial_state(self) -> dict:
        """Defines the initial state for stateful RLModules."""
        return {}

    @override(RLModule)
    def input_specs_train(self) -> SpecType:
        """Defines the input specs for the train forward pass."""
        # Note, the reward function inputs actual state, action and, next state.
        return [
            Columns.OBS,
            Columns.ACTIONS,
            Columns.NEXT_OBS,
        ]

    @override(RLModule)
    def output_specs_train(self) -> SpecType:
        """Defines the output specs for the train forward pass."""
        return [
            Columns.REWARDS,
        ]
