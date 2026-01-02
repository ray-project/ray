"""
Default TQC RLModule.

TQC uses distributional critics with quantile regression.
"""

from abc import abstractmethod
from typing import Any, Dict, List, Tuple

from ray.rllib.core.learner.utils import make_target_network
from ray.rllib.core.models.base import Encoder, Model
from ray.rllib.core.rl_module.apis import InferenceOnlyAPI, QNetAPI, TargetNetworkAPI
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.annotations import (
    OverrideToImplementCustomLogic,
    override,
)
from ray.rllib.utils.typing import NetworkType
from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class DefaultTQCRLModule(RLModule, InferenceOnlyAPI, TargetNetworkAPI, QNetAPI):
    """RLModule for the TQC (Truncated Quantile Critics) algorithm.

    TQC extends SAC by using distributional critics with quantile regression.
    Each critic outputs n_quantiles values instead of a single Q-value.

    Architecture:
    - Policy (Actor): Same as SAC
      [obs] -> [pi_encoder] -> [pi_head] -> [action_dist_inputs]

    - Quantile Critics: Multiple critics, each outputting n_quantiles
      [obs, action] -> [qf_encoder_i] -> [qf_head_i] -> [n_quantiles values]

    - Target Quantile Critics: Target networks for each critic
      [obs, action] -> [target_qf_encoder_i] -> [target_qf_head_i] -> [n_quantiles]
    """

    @override(RLModule)
    def setup(self):
        # TQC-specific parameters from model_config
        self.n_quantiles = self.model_config.get("n_quantiles", 25)
        self.n_critics = self.model_config.get("n_critics", 2)
        self.top_quantiles_to_drop_per_net = self.model_config.get(
            "top_quantiles_to_drop_per_net", 2
        )

        # Total quantiles across all critics
        self.quantiles_total = self.n_quantiles * self.n_critics

        # Build the encoder for the policy (same as SAC)
        self.pi_encoder = self.catalog.build_encoder(framework=self.framework)

        if not self.inference_only or self.framework != "torch":
            # Build multiple Q-function encoders and heads
            self.qf_encoders = []
            self.qf_heads = []

            for i in range(self.n_critics):
                qf_encoder = self.catalog.build_qf_encoder(framework=self.framework)
                qf_head = self.catalog.build_qf_head(framework=self.framework)
                self.qf_encoders.append(qf_encoder)
                self.qf_heads.append(qf_head)

        # Build the policy head (same as SAC)
        self.pi = self.catalog.build_pi_head(framework=self.framework)

    @override(TargetNetworkAPI)
    def make_target_networks(self):
        """Creates target networks for all quantile critics."""
        self.target_qf_encoders = []
        self.target_qf_heads = []

        for i in range(self.n_critics):
            target_encoder = make_target_network(self.qf_encoders[i])
            target_head = make_target_network(self.qf_heads[i])
            self.target_qf_encoders.append(target_encoder)
            self.target_qf_heads.append(target_head)

    @override(InferenceOnlyAPI)
    def get_non_inference_attributes(self) -> List[str]:
        """Returns attributes not needed for inference."""
        attrs = []
        for i in range(self.n_critics):
            attrs.extend([
                f"qf_encoders",
                f"qf_heads",
                f"target_qf_encoders",
                f"target_qf_heads",
            ])
        return list(set(attrs))

    @override(TargetNetworkAPI)
    def get_target_network_pairs(self) -> List[Tuple[NetworkType, NetworkType]]:
        """Returns pairs of (network, target_network) for updating targets."""
        pairs = []
        for i in range(self.n_critics):
            pairs.append((self.qf_encoders[i], self.target_qf_encoders[i]))
            pairs.append((self.qf_heads[i], self.target_qf_heads[i]))
        return pairs

    @override(RLModule)
    def get_initial_state(self) -> dict:
        """TQC does not support RNNs yet."""
        return {}

    @abstractmethod
    @OverrideToImplementCustomLogic
    def _qf_forward_helper(
        self,
        batch: Dict[str, Any],
        encoder: Encoder,
        head: Model,
    ) -> Dict[str, Any]:
        """Executes forward pass for a single Q-network.

        Args:
            batch: Dict containing concatenated observations and actions.
            encoder: The Q-function encoder.
            head: The Q-function head.

        Returns:
            The quantile estimates from this critic.
        """

    @abstractmethod
    @OverrideToImplementCustomLogic
    def _qf_forward_all_critics(
        self,
        batch: Dict[str, Any],
        use_target: bool = False,
    ) -> Dict[str, Any]:
        """Executes forward pass for all critics.

        Args:
            batch: Dict containing observations and actions.
            use_target: Whether to use target networks.

        Returns:
            Stacked quantile estimates from all critics.
            Shape: (batch_size, n_critics, n_quantiles)
        """
