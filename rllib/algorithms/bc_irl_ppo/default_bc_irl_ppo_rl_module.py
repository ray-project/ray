from enum import Enum

from ray.rllib.core.columns import Columns
from ray.rllib.core.models.specs.typing import SpecType
from ray.rllib.core.rl_module import RLModule
from ray.rllib.utils.annotations import override


class DefaultBCIRLRewardModelType(str, Enum):
    """Defines the default reward model types.

    ACTION: Action, current state, and next state input.
    NEXT_STATE: Next state input.
    CURR_NEXT_STATE: Current state, and next state input.
    """

    ACTION = "action"
    NEXT_STATE = "next_state"
    CURR_NEXT_STATE = "curr_next_state"


class DefaultBCIRLRewardRLModule(RLModule):
    @override(RLModule)
    def setup(self):
        # If an action reward model should be used or a state-only one.
        try:
            self.reward_type = DefaultBCIRLRewardModelType(
                self.model_config["reward_type"]
            )
        except ValueError:
            raise ValueError(
                f"Invalid reward model type: {self.reward_type.value}. "
                f"Allowed types: {[t.value for t in DefaultBCIRLRewardModelType]}"
            )
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
        if self.reward_type == "action":
            return [
                Columns.OBS,
                Columns.ACTIONS,
                Columns.NEXT_OBS,
            ]
        elif self.reward_type == "curr_next_obs":
            return [Columns.OBS, Columns.NEXT_OBS]
        elif self.reward_type == "next_obs":
            return [Columns.NEXT_OBS]

    @override(RLModule)
    def output_specs_train(self) -> SpecType:
        """Defines the output specs for the train forward pass."""
        return [
            Columns.REWARDS,
        ]
