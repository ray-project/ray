from abc import abstractmethod
from typing import Any, Dict, Type, Union
from ray.rllib.algorithms.dqn.dqn_rainbow_catalog import DQNRainbowCatalog
from ray.rllib.core.models.base import Encoder, Model
from ray.rllib.core.models.specs.typing import SpecType
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.core.rl_module.rl_module_with_target_networks_interface import (
    RLModuleWithTargetNetworksInterface,
)
from ray.rllib.models.distributions import Distribution
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import (
    ExperimentalAPI,
    override,
    OverrideToImplementCustomLogic,
)

ATOMS = "atoms"
QF_LOGITS = "qf_logits"
QF_PROBS = "qf_probs"
QF_TARGET_NEXT_PREDS = "qf_target_next_preds"
QF_TARGET_NEXT_PROBS = "qf_target_next_probs"


@ExperimentalAPI
class DQNRainbowRLModule(RLModule, RLModuleWithTargetNetworksInterface):
    @override(RLModule)
    def setup(self):
        # Get the DQN Rainbow catalog.
        catalog: DQNRainbowCatalog = self.config.get_catalog()

        # If a dueling architecture is used.
        self.is_dueling: bool = self.config.model_config_dict.get("dueling")
        # If we use noisy layers.
        self.uses_noisy: bool = self.config.model_config_dict.get("noisy")
        # The number of atoms for a distribution support.
        self.num_atoms: int = self.config.model_config_dict.get("num_atoms")
        # If distributional learning is requested configure the support.
        if self.num_atoms > 1:
            self.v_min: float = self.config.model_config_dict.get("v_min")
            self.v_max: float = self.config.model_config_dict.get("v_max")

        # Build the encoder for the advantage and value streams. Note,
        # the same encoder is used.
        # Note further, by using the base encoder the correct encoder
        # is chosen for the observation space used.
        self.encoder = catalog.build_encoder(framework=self.framework)
        # Build the same encoder for the target network(s).
        self.target_encoder = catalog.build_encoder(framework=self.framework)

        # Build heads.
        self.af = catalog.build_af_head(framework=self.framework)
        if self.is_dueling:
            self.vf = catalog.build_vf_head(framework=self.framework)
        # Implement the same heads for the target network(s).
        self.af_target = catalog.build_af_head(framework=self.framework)
        if self.is_dueling:
            self.vf_target = catalog.build_vf_head(framework=self.framework)

        # We do not want to train the target networks.
        self.target_encoder.trainable = False
        self.af_target.trainable = False
        if self.is_dueling:
            self.vf_target.trainable = False

        # Define the action distribution for sampling the eexploit action
        # during exploration.
        self.action_dist_cls = catalog.get_action_dist_cls(framework=self.framework)

    @override(RLModule)
    def get_exploration_action_dist_cls(self) -> Type[Distribution]:
        """Returns the action distribution class for exploration.

        Note, this class is used to sample the exploit action during
        exploration.
        """
        return self.action_dist_cls

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

    @abstractmethod
    @OverrideToImplementCustomLogic
    def qf(self, batch: Dict[str, Any]) -> Dict[str, Any]:
        """Computes Q-values.

        Note, these can be accompanied with logits and pobabilities
        in case of distributional Q-learning, i.e. `self.num_atoms > 1`.
        """

    @abstractmethod
    @OverrideToImplementCustomLogic
    def qf_target(self, batch: Dict[str, Any]) -> Dict[str, Any]:
        """Computes Q-values from the target network.

        Note, these can be accompanied with logits and pobabilities
        in case of distributional Q-learning, i.e. `self.num_atoms > 1`.
        """

    @abstractmethod
    @OverrideToImplementCustomLogic
    def af_dist(self, batch: Dict[str, Any]) -> Dict[str, Any]:
        """Compute the advantage distribution."""

    @abstractmethod
    @OverrideToImplementCustomLogic
    def _qf_forward_helper(
        self, batch: Dict, encoder: Encoder, head: Union[Model, Dict[str, Model]]
    ) -> Dict:
        """Executes the forward pass for Q-networks."""
