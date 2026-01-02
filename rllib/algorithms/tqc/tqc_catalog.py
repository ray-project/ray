"""
TQC Catalog for building TQC-specific models.

TQC uses multiple quantile critics, each outputting n_quantiles values.
"""

import gymnasium as gym

from ray.rllib.algorithms.sac.sac_catalog import SACCatalog
from ray.rllib.core.models.base import Encoder, Model
from ray.rllib.core.models.configs import MLPHeadConfig
from ray.rllib.utils.annotations import OverrideToImplementCustomLogic


class TQCCatalog(SACCatalog):
    """Catalog class for building TQC models.

    TQC extends SAC by using distributional critics with quantile regression.
    Each critic outputs `n_quantiles` values instead of a single Q-value.

    The catalog builds:
    - Pi Encoder: Same as SAC (encodes observations for the actor)
    - Pi Head: Same as SAC (outputs mean and log_std for Squashed Gaussian)
    - QF Encoders: Multiple encoders for quantile critics
    - QF Heads: Multiple heads, each outputting n_quantiles values
    """

    def __init__(
        self,
        observation_space: gym.Space,
        action_space: gym.Space,
        model_config_dict: dict,
        view_requirements: dict = None,
    ):
        """Initializes the TQCCatalog.

        Args:
            observation_space: The observation space of the environment.
            action_space: The action space of the environment.
            model_config_dict: The model config dictionary containing
                TQC-specific parameters like n_quantiles and n_critics.
            view_requirements: Not used, kept for API compatibility.
        """
        # Extract TQC-specific parameters before calling super().__init__
        self.n_quantiles = model_config_dict.get("n_quantiles", 25)
        self.n_critics = model_config_dict.get("n_critics", 2)

        super().__init__(
            observation_space=observation_space,
            action_space=action_space,
            model_config_dict=model_config_dict,
            view_requirements=view_requirements,
        )

        # Override the QF head config to output n_quantiles instead of 1
        # For TQC, we always output n_quantiles (continuous action space)
        self.qf_head_config = MLPHeadConfig(
            input_dims=self.latent_dims,
            hidden_layer_dims=self.pi_and_qf_head_hiddens,
            hidden_layer_activation=self.pi_and_qf_head_activation,
            output_layer_activation="linear",
            output_layer_dim=self.n_quantiles,
        )

    @OverrideToImplementCustomLogic
    def build_qf_encoder(self, framework: str) -> Encoder:
        """Builds a Q-function encoder for TQC.

        Same as SAC - encodes state-action pairs.

        Args:
            framework: The framework to use ("torch").

        Returns:
            The encoder for the Q-network.
        """
        return super().build_qf_encoder(framework=framework)

    @OverrideToImplementCustomLogic
    def build_qf_head(self, framework: str) -> Model:
        """Builds a Q-function head that outputs n_quantiles values.

        Args:
            framework: The framework to use ("torch").

        Returns:
            The Q-function head outputting n_quantiles values.
        """
        return self.qf_head_config.build(framework=framework)
