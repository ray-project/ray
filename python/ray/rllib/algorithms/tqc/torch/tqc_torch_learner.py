"""
PyTorch implementation of the TQC Learner.

Implements the TQC loss computation with quantile Huber loss.
"""

from typing import Any, Dict

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.sac.sac_learner import (
    LOGPS_KEY,
    QF_PREDS,
    QF_TARGET_NEXT,
)
from ray.rllib.algorithms.sac.torch.sac_torch_learner import SACTorchLearner
from ray.rllib.algorithms.tqc.tqc import TQCConfig
from ray.rllib.algorithms.tqc.tqc_learner import (
    QF_LOSS_KEY,
    QF_MAX_KEY,
    QF_MEAN_KEY,
    QF_MIN_KEY,
    TD_ERROR_MEAN_KEY,
    TQCLearner,
)
from ray.rllib.core.columns import Columns
from ray.rllib.core.learner.learner import POLICY_LOSS_KEY
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.metrics import TD_ERROR_KEY
from ray.rllib.utils.typing import ModuleID, TensorType

torch, nn = try_import_torch()


def quantile_huber_loss_per_sample(
    quantiles: torch.Tensor,
    target_quantiles: torch.Tensor,
    kappa: float = 1.0,
) -> torch.Tensor:
    """Computes the quantile Huber loss per sample (for importance sampling).

    Args:
        quantiles: Current quantile estimates. Shape: (batch, n_quantiles)
        target_quantiles: Target quantile values. Shape: (batch, n_target_quantiles)
        kappa: Huber loss threshold parameter.

    Returns:
        Per-sample quantile Huber loss. Shape: (batch,)
    """
    n_quantiles = quantiles.shape[1]

    # Compute cumulative probabilities for quantiles (tau values)
    tau = (
        torch.arange(n_quantiles, device=quantiles.device, dtype=quantiles.dtype) + 0.5
    ) / n_quantiles

    # Expand dimensions for broadcasting
    quantiles_expanded = quantiles.unsqueeze(2)
    target_expanded = target_quantiles.unsqueeze(1)

    # Compute pairwise TD errors: (batch, n_quantiles, n_target_quantiles)
    td_error = target_expanded - quantiles_expanded

    # Compute Huber loss element-wise using nn.HuberLoss
    huber_loss_fn = nn.HuberLoss(reduction="none", delta=kappa)
    huber_loss = huber_loss_fn(quantiles_expanded, target_expanded)

    # Compute quantile weights
    tau_expanded = tau.view(1, n_quantiles, 1)
    quantile_weight = torch.abs(tau_expanded - (td_error < 0).float())

    # Weighted Huber loss
    quantile_huber = quantile_weight * huber_loss

    # Sum over quantile dimensions, keep batch dimension
    return quantile_huber.sum(dim=(1, 2))


class TQCTorchLearner(SACTorchLearner, TQCLearner):
    """PyTorch Learner for TQC algorithm.

    Implements the TQC loss computation:
    - Critic loss: Quantile Huber loss with truncated targets
    - Actor loss: Maximize mean Q-value (from truncated quantiles)
    - Alpha loss: Same as SAC (entropy regularization)
    """

    @override(SACTorchLearner)
    def build(self) -> None:
        super().build()
        self._temp_losses = {}

    @override(SACTorchLearner)
    def configure_optimizers_for_module(
        self, module_id: ModuleID, config: AlgorithmConfig = None
    ) -> None:
        """Configures optimizers for TQC.

        TQC has separate optimizers for:
        - All critic networks (shared optimizer)
        - Actor network
        - Temperature (alpha) parameter
        """
        module = self._module[module_id]

        # Collect all critic parameters
        critic_params = []
        for encoder in module.qf_encoders:
            critic_params.extend(self.get_parameters(encoder))
        for head in module.qf_heads:
            critic_params.extend(self.get_parameters(head))

        optim_critic = torch.optim.Adam(critic_params, eps=1e-7)
        self.register_optimizer(
            module_id=module_id,
            optimizer_name="qf",
            optimizer=optim_critic,
            params=critic_params,
            lr_or_lr_schedule=config.critic_lr,
        )

        # Actor optimizer
        params_actor = self.get_parameters(module.pi_encoder) + self.get_parameters(
            module.pi
        )
        optim_actor = torch.optim.Adam(params_actor, eps=1e-7)
        self.register_optimizer(
            module_id=module_id,
            optimizer_name="policy",
            optimizer=optim_actor,
            params=params_actor,
            lr_or_lr_schedule=config.actor_lr,
        )

        # Temperature optimizer
        temperature = self.curr_log_alpha[module_id]
        optim_temperature = torch.optim.Adam([temperature], eps=1e-7)
        self.register_optimizer(
            module_id=module_id,
            optimizer_name="alpha",
            optimizer=optim_temperature,
            params=[temperature],
            lr_or_lr_schedule=config.alpha_lr,
        )

    @override(SACTorchLearner)
    def compute_loss_for_module(
        self,
        *,
        module_id: ModuleID,
        config: TQCConfig,
        batch: Dict[str, Any],
        fwd_out: Dict[str, TensorType],
    ) -> TensorType:
        """Computes the TQC loss.

        Args:
            module_id: The module ID.
            config: The TQC configuration.
            batch: The training batch.
            fwd_out: Forward pass outputs.

        Returns:
            Total loss (sum of critic, actor, and alpha losses).
        """
        # Get current alpha (temperature parameter)
        alpha = torch.exp(self.curr_log_alpha[module_id])

        # Get TQC parameters
        n_critics = config.n_critics
        n_target_quantiles = self._get_n_target_quantiles(module_id)

        batch_size = batch[Columns.OBS].shape[0]

        # === Critic Loss ===
        # Get current Q-value predictions (quantiles)
        # Shape: (batch, n_critics, n_quantiles)
        qf_preds = fwd_out[QF_PREDS]

        # Get target Q-values for next state
        # Shape: (batch, n_critics, n_quantiles)
        qf_target_next = fwd_out[QF_TARGET_NEXT]
        logp_next = fwd_out["logp_next"]

        # Flatten and sort quantiles across all critics
        # Shape: (batch, n_critics * n_quantiles)
        qf_target_next_flat = qf_target_next.reshape(batch_size, -1)

        # Sort and truncate top quantiles to control overestimation
        qf_target_next_sorted, _ = torch.sort(qf_target_next_flat, dim=1)
        qf_target_next_truncated = qf_target_next_sorted[:, :n_target_quantiles]

        # Compute target with entropy bonus
        # Shape: (batch, n_target_quantiles)
        target_quantiles = (
            qf_target_next_truncated - alpha.detach() * logp_next.unsqueeze(1)
        )

        # Compute TD targets
        rewards = batch[Columns.REWARDS].unsqueeze(1)
        terminateds = batch[Columns.TERMINATEDS].float().unsqueeze(1)
        gamma = config.gamma
        n_step = batch.get("n_step", torch.ones_like(batch[Columns.REWARDS]))
        if isinstance(n_step, (int, float)):
            n_step = torch.full_like(batch[Columns.REWARDS], n_step)

        target_quantiles = (
            rewards
            + (1.0 - terminateds) * (gamma ** n_step.unsqueeze(1)) * target_quantiles
        ).detach()

        # Get importance sampling weights for prioritized replay
        weights = batch.get("weights", torch.ones_like(batch[Columns.REWARDS]))

        # Compute critic loss for each critic
        critic_loss = torch.tensor(0.0, device=qf_preds.device)
        for i in range(n_critics):
            # Get quantiles for this critic: (batch, n_quantiles)
            critic_quantiles = qf_preds[:, i, :]
            # Compute per-sample quantile huber loss
            critic_loss_per_sample = quantile_huber_loss_per_sample(
                critic_quantiles,
                target_quantiles,
            )
            # Apply importance sampling weights
            critic_loss += torch.mean(weights * critic_loss_per_sample)

        # === Actor Loss ===
        # Get Q-values for resampled actions
        qf_curr = fwd_out["qf_curr"]  # (batch, n_critics, n_quantiles)
        logp_curr = fwd_out["logp_curr"]

        # Mean over all quantiles and critics
        qf_curr_mean = qf_curr.mean(dim=(1, 2))

        # Actor loss: maximize Q-value while maintaining entropy
        actor_loss = (alpha.detach() * logp_curr - qf_curr_mean).mean()

        # === Alpha Loss ===
        alpha_loss = -torch.mean(
            self.curr_log_alpha[module_id]
            * (logp_curr.detach() + self.target_entropy[module_id])
        )

        # Total loss
        total_loss = critic_loss + actor_loss + alpha_loss

        # Compute TD error for prioritized replay
        # Use mean across critics and quantiles
        qf_preds_mean = qf_preds.mean(dim=(1, 2))
        target_mean = target_quantiles.mean(dim=1)
        td_error = torch.abs(qf_preds_mean - target_mean)

        # Log metrics
        self.metrics.log_value(
            key=(module_id, TD_ERROR_KEY),
            value=td_error,
            reduce="item_series",
        )

        self.metrics.log_dict(
            {
                POLICY_LOSS_KEY: actor_loss,
                QF_LOSS_KEY: critic_loss,
                "alpha_loss": alpha_loss,
                "alpha_value": alpha[0],
                "log_alpha_value": torch.log(alpha)[0],
                "target_entropy": self.target_entropy[module_id],
                LOGPS_KEY: torch.mean(logp_curr),
                QF_MEAN_KEY: torch.mean(qf_preds),
                QF_MAX_KEY: torch.max(qf_preds),
                QF_MIN_KEY: torch.min(qf_preds),
                TD_ERROR_MEAN_KEY: torch.mean(td_error),
            },
            key=module_id,
            window=1,
        )

        # Store losses for gradient computation
        self._temp_losses[(module_id, POLICY_LOSS_KEY)] = actor_loss
        self._temp_losses[(module_id, QF_LOSS_KEY)] = critic_loss
        self._temp_losses[(module_id, "alpha_loss")] = alpha_loss

        return total_loss

    # Note: compute_gradients is inherited from SACTorchLearner
