"""
PyTorch implementation of the TQC RLModule.
"""

from typing import Any, Dict

from ray.rllib.algorithms.sac.sac_learner import QF_PREDS, QF_TARGET_NEXT
from ray.rllib.algorithms.tqc.default_tqc_rl_module import DefaultTQCRLModule
from ray.rllib.algorithms.tqc.tqc_catalog import TQCCatalog
from ray.rllib.core.columns import Columns
from ray.rllib.core.learner.utils import make_target_network
from ray.rllib.core.models.base import ENCODER_OUT
from ray.rllib.core.rl_module.torch.torch_rl_module import TorchRLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()


class DefaultTQCTorchRLModule(TorchRLModule, DefaultTQCRLModule):
    """PyTorch implementation of the TQC RLModule.

    TQC uses multiple quantile critics, each outputting n_quantiles values.
    """

    framework: str = "torch"

    def __init__(self, *args, **kwargs):
        catalog_class = kwargs.pop("catalog_class", None)
        if catalog_class is None:
            catalog_class = TQCCatalog
        super().__init__(*args, **kwargs, catalog_class=catalog_class)

    @override(DefaultTQCRLModule)
    def setup(self):
        # Call parent setup to initialize TQC-specific parameters and build networks
        super().setup()

        # Convert lists to nn.ModuleList for proper PyTorch parameter tracking
        if not self.inference_only or self.framework != "torch":
            self.qf_encoders = nn.ModuleList(self.qf_encoders)
            self.qf_heads = nn.ModuleList(self.qf_heads)

    @override(DefaultTQCRLModule)
    def make_target_networks(self):
        """Creates target networks for all quantile critics."""
        self.target_qf_encoders = nn.ModuleList()
        self.target_qf_heads = nn.ModuleList()

        for i in range(self.n_critics):
            target_encoder = make_target_network(self.qf_encoders[i])
            target_head = make_target_network(self.qf_heads[i])
            self.target_qf_encoders.append(target_encoder)
            self.target_qf_heads.append(target_head)

    @override(TorchRLModule)
    def _forward_inference(self, batch: Dict[str, Any]) -> Dict[str, Any]:
        """Forward pass for inference (action selection).

        Same as SAC - samples actions from the policy.
        """
        output = {}

        # Extract features from observations
        pi_encoder_out = self.pi_encoder(batch)
        pi_out = self.pi(pi_encoder_out[ENCODER_OUT])

        output[Columns.ACTION_DIST_INPUTS] = pi_out

        return output

    @override(TorchRLModule)
    def _forward_exploration(self, batch: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        """Forward pass for exploration.

        Same as inference for TQC (stochastic policy).
        """
        return self._forward_inference(batch)

    @override(TorchRLModule)
    def _forward_train(self, batch: Dict[str, Any]) -> Dict[str, Any]:
        """Forward pass for training.

        Computes:
        - Action distribution inputs from current observations
        - Q-values (quantiles) for current state-action pairs
        - Q-values (quantiles) for next states with resampled actions
        """
        output = {}

        # Get action distribution inputs for current observations
        pi_encoder_out = self.pi_encoder(batch)
        pi_out = self.pi(pi_encoder_out[ENCODER_OUT])
        output[Columns.ACTION_DIST_INPUTS] = pi_out

        # Sample actions from current policy for current observations
        action_dist_class = self.catalog.get_action_dist_cls(framework=self.framework)
        action_dist_curr = action_dist_class.from_logits(pi_out)
        actions_curr = action_dist_curr.rsample()
        logp_curr = action_dist_curr.logp(actions_curr)

        output["actions_curr"] = actions_curr
        output["logp_curr"] = logp_curr

        # Compute Q-values for actions from replay buffer
        qf_out = self._qf_forward_all_critics(
            batch[Columns.OBS],
            batch[Columns.ACTIONS],
            use_target=False,
        )
        output[QF_PREDS] = qf_out  # (batch, n_critics, n_quantiles)

        # Compute Q-values for resampled actions (for actor loss)
        qf_curr = self._qf_forward_all_critics(
            batch[Columns.OBS],
            actions_curr,
            use_target=False,
        )
        output["qf_curr"] = qf_curr

        # For next state Q-values (target computation)
        if Columns.NEXT_OBS in batch:
            # Get action distribution for next observations
            pi_encoder_out_next = self.pi_encoder(
                {Columns.OBS: batch[Columns.NEXT_OBS]}
            )
            pi_out_next = self.pi(pi_encoder_out_next[ENCODER_OUT])

            # Sample actions for next state
            action_dist_next = action_dist_class.from_logits(pi_out_next)
            actions_next = action_dist_next.rsample()
            logp_next = action_dist_next.logp(actions_next)

            output["actions_next"] = actions_next
            output["logp_next"] = logp_next

            # Compute target Q-values for next state
            qf_target_next = self._qf_forward_all_critics(
                batch[Columns.NEXT_OBS],
                actions_next,
                use_target=True,
            )
            output[QF_TARGET_NEXT] = qf_target_next

        return output

    def _qf_forward_all_critics(
        self,
        obs: torch.Tensor,
        actions: torch.Tensor,
        use_target: bool = False,
    ) -> torch.Tensor:
        """Forward pass through all critic networks.

        Args:
            obs: Observations tensor.
            actions: Actions tensor.
            use_target: Whether to use target networks.

        Returns:
            Stacked quantile values from all critics.
            Shape: (batch_size, n_critics, n_quantiles)
        """
        # Note: obs should already be a flat tensor at this point.
        # Dict observations are handled by connectors (e.g., FlattenObservations)
        # before reaching this method.

        # Concatenate observations and actions
        qf_input = torch.cat([obs, actions], dim=-1)
        batch_dict = {Columns.OBS: qf_input}

        encoders = self.target_qf_encoders if use_target else self.qf_encoders
        heads = self.target_qf_heads if use_target else self.qf_heads

        quantiles_list = []
        for encoder, head in zip(encoders, heads):
            encoder_out = encoder(batch_dict)
            quantiles = head(encoder_out[ENCODER_OUT])  # (batch, n_quantiles)
            quantiles_list.append(quantiles)

        # Stack: (batch, n_critics, n_quantiles)
        return torch.stack(quantiles_list, dim=1)

    @override(DefaultTQCRLModule)
    def compute_q_values(self, batch: Dict[str, Any]) -> Dict[str, Any]:
        """Computes Q-values (mean of quantiles) for the given batch.

        Args:
            batch: Dict containing observations and actions.

        Returns:
            Mean Q-value across all quantiles and critics.
        """
        obs = batch[Columns.OBS]
        actions = batch[Columns.ACTIONS]

        # Get all quantiles from all critics
        quantiles = self._qf_forward_all_critics(obs, actions, use_target=False)

        # Return mean across all quantiles and critics
        return quantiles.mean(dim=(1, 2))

    @override(DefaultTQCRLModule)
    def forward_target(self, batch: Dict[str, Any]) -> Dict[str, Any]:
        """Forward pass through target networks.

        Args:
            batch: Dict containing observations and actions.

        Returns:
            Target Q-values (mean of truncated quantiles).
        """
        obs = batch[Columns.OBS]
        actions = batch[Columns.ACTIONS]

        # Get all quantiles from target critics
        quantiles = self._qf_forward_all_critics(obs, actions, use_target=True)

        # Flatten, sort, and truncate top quantiles
        batch_size = quantiles.shape[0]
        quantiles_flat = quantiles.reshape(batch_size, -1)
        quantiles_sorted, _ = torch.sort(quantiles_flat, dim=1)

        # Calculate number of quantiles to keep
        n_target_quantiles = (
            self.quantiles_total - self.top_quantiles_to_drop_per_net * self.n_critics
        )
        quantiles_truncated = quantiles_sorted[:, :n_target_quantiles]

        # Return mean of truncated quantiles
        return quantiles_truncated.mean(dim=1)

    @staticmethod
    def _get_catalog_class():
        return TQCCatalog
