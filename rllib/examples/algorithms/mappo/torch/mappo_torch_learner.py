import logging
from typing import Any, Callable, Dict

from ray.rllib.algorithms.ppo.ppo import (
    LEARNER_RESULTS_KL_KEY,
    LEARNER_RESULTS_VF_EXPLAINED_VAR_KEY,
    LEARNER_RESULTS_VF_LOSS_UNCLIPPED_KEY,
    PPOConfig,
)
from ray.rllib.algorithms.ppo.torch.ppo_torch_learner import PPOTorchLearner
from ray.rllib.core.columns import Columns
from ray.rllib.core.learner.learner import (
    ENTROPY_KEY,
    POLICY_LOSS_KEY,
    VF_LOSS_KEY,
    Learner,
)
from ray.rllib.core.rl_module.apis import SelfSupervisedLossAPI
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.examples.algorithms.mappo.connectors.general_advantage_estimation import (
    SHARED_CRITIC_ID,
)
from ray.rllib.examples.algorithms.mappo.mappo_learner import MAPPOLearner
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.torch_utils import explained_variance
from ray.rllib.utils.typing import ModuleID, TensorType

torch, nn = try_import_torch()

logger = logging.getLogger(__name__)


class MAPPOTorchLearner(MAPPOLearner, PPOTorchLearner):
    """Torch MAPPO learner: shared critic + decentralized actor losses.

    Inherits:
        - MAPPOLearner: shared-critic GAE connector, skips critic in
          after_gradient_based_update.
        - PPOTorchLearner: _update_module_kl_coeff, torch _update loop.
    """

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _make_masked_mean_fn(batch: Dict[str, Any]) -> Callable:
        """Return a mean function that respects LOSS_MASK when present."""
        if Columns.LOSS_MASK in batch:
            mask = batch[Columns.LOSS_MASK]
            num_valid = torch.sum(mask)

            def masked_mean(data_):
                return torch.sum(data_[mask]) / num_valid

            return masked_mean
        return torch.mean

    # ------------------------------------------------------------------
    # Shared-critic loss
    # ------------------------------------------------------------------

    def _compute_critic_loss(self, batch: Dict[str, Any]) -> TensorType:
        """Compute the value-function loss for the shared critic."""
        masked_mean = self._make_masked_mean_fn(batch)
        module = self.module[SHARED_CRITIC_ID].unwrapped()
        vf_preds = module.compute_values(batch)
        vf_targets = batch[Postprocessing.VALUE_TARGETS]

        vf_loss = torch.pow(vf_preds - vf_targets, 2.0)
        # Clip per-element BEFORE reducing across agents so the clamp
        # threshold has consistent semantics with PPO's vf_clip_param.
        vf_loss_clipped = torch.clamp(vf_loss, 0, self.config.vf_clip_param)
        # Now reduce the agent dimension.
        vf_loss_clipped = vf_loss_clipped.mean(dim=-1)
        vf_loss_unreduced = vf_loss.mean(dim=-1)

        mean_vf_loss = masked_mean(vf_loss_clipped)
        mean_vf_unclipped_loss = masked_mean(vf_loss_unreduced)

        self.metrics.log_dict(
            {
                VF_LOSS_KEY: mean_vf_loss,
                LEARNER_RESULTS_VF_LOSS_UNCLIPPED_KEY: mean_vf_unclipped_loss,
                LEARNER_RESULTS_VF_EXPLAINED_VAR_KEY: explained_variance(
                    vf_targets.reshape(-1), vf_preds.reshape(-1)
                ),
            },
            key=SHARED_CRITIC_ID,
            window=1,
        )
        return mean_vf_loss

    # ------------------------------------------------------------------
    # Overrides
    # ------------------------------------------------------------------

    @override(Learner)
    def compute_losses(
        self, *, fwd_out: Dict[str, Any], batch: Dict[str, Any]
    ) -> Dict[str, Any]:
        loss_per_module = {}

        # Shared critic loss.
        loss_per_module[SHARED_CRITIC_ID] = self._compute_critic_loss(
            batch[SHARED_CRITIC_ID]
        )

        # Per-agent policy losses.
        for module_id in fwd_out:
            if module_id == SHARED_CRITIC_ID:
                continue

            module_batch = batch[module_id]
            module_fwd_out = fwd_out[module_id]
            module = self.module[module_id].unwrapped()

            if isinstance(module, SelfSupervisedLossAPI):
                loss = module.compute_self_supervised_loss(
                    learner=self,
                    module_id=module_id,
                    config=self.config.get_config_for_module(module_id),
                    batch=module_batch,
                    fwd_out=module_fwd_out,
                )
            else:
                loss = self._compute_actor_loss(
                    module_id=module_id,
                    config=self.config.get_config_for_module(module_id),
                    batch=module_batch,
                    fwd_out=module_fwd_out,
                )
            loss_per_module[module_id] = loss

        return loss_per_module

    def _compute_actor_loss(
        self,
        *,
        module_id: ModuleID,
        config: PPOConfig,
        batch: Dict[str, Any],
        fwd_out: Dict[str, TensorType],
    ) -> TensorType:
        """PPO surrogate loss for an individual actor (no VF term)."""
        module = self.module[module_id].unwrapped()
        masked_mean = self._make_masked_mean_fn(batch)

        action_dist_class_train = module.get_train_action_dist_cls()
        action_dist_class_exploration = module.get_exploration_action_dist_cls()

        curr_action_dist = action_dist_class_train.from_logits(
            fwd_out[Columns.ACTION_DIST_INPUTS]
        )
        prev_action_dist = action_dist_class_exploration.from_logits(
            batch[Columns.ACTION_DIST_INPUTS]
        )

        logp_ratio = torch.exp(
            curr_action_dist.logp(batch[Columns.ACTIONS]) - batch[Columns.ACTION_LOGP]
        )

        if config.use_kl_loss:
            action_kl = prev_action_dist.kl(curr_action_dist)
            mean_kl_loss = masked_mean(action_kl)
        else:
            mean_kl_loss = torch.tensor(0.0, device=logp_ratio.device)

        curr_entropy = curr_action_dist.entropy()
        mean_entropy = masked_mean(curr_entropy)

        surrogate_loss = torch.min(
            batch[Postprocessing.ADVANTAGES] * logp_ratio,
            batch[Postprocessing.ADVANTAGES]
            * torch.clamp(logp_ratio, 1 - config.clip_param, 1 + config.clip_param),
        )

        # Actor-only total loss (no VF term -- handled by shared critic).
        total_loss = masked_mean(
            -surrogate_loss
            - (
                self.entropy_coeff_schedulers_per_module[module_id].get_current_value()
                * curr_entropy
            )
        )

        if config.use_kl_loss:
            total_loss += self.curr_kl_coeffs_per_module[module_id] * mean_kl_loss

        self.metrics.log_dict(
            {
                POLICY_LOSS_KEY: -masked_mean(surrogate_loss),
                ENTROPY_KEY: mean_entropy,
                LEARNER_RESULTS_KL_KEY: mean_kl_loss,
            },
            key=module_id,
            window=1,
        )
        return total_loss

    # _update_module_kl_coeff is inherited from PPOTorchLearner -- no copy needed.
