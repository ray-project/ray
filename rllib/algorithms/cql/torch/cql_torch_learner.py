"""CQL Torch Learner — new-stack implementation.

Changes vs. upstream (ray-project/ray master):
- Implements the Lagrangian dual form of CQL (Kumar et al. 2020, §3.2).
  When `config.lagrangian=True`, `min_q_weight` (α') is no longer a fixed
  hyperparameter but a learned multiplier that automatically satisfies the
  constraint:   E_μ[Q] - E_data[Q]  ≤  lagrangian_thresh

  The update rule is:
      log_α' ← log_α' - lr_lagrangian · ∇_{log_α'} L_lagrangian
  where
      L_lagrangian = α' · (E_μ[Q] - E_data[Q] - lagrangian_thresh)

  α' is clamped ≥ 0 (via exp(log_α')).
  This removes the need to hand-tune `min_q_weight` across environments.

- Adds `lagrangian_lr` to CQLConfig (default 1e-4).
- Stores per-module `log_cql_alpha` parameters and a dedicated optimizer.
- Logs `cql_alpha_value` and `lagrangian_loss` to metrics.

Relation to BCLM (Braille Compliance Law Manifold):
  The Lagrangian form here is the single-constraint specialisation of the
  multi-constraint penalty manifold p(r)^T W p(r) formalised in BCLM.
  The multi-constraint extension (per-region or per-objective CQL) is left
  as a follow-on PR (see TODO below).

References:
  Kumar et al. (2020). Conservative Q-Learning for Offline Reinforcement
  Learning. NeurIPS 2020. https://arxiv.org/abs/2006.04779
"""

from typing import Dict

from ray.rllib.algorithms.cql.cql import CQLConfig
from ray.rllib.algorithms.sac.sac_learner import (
    LOGPS_KEY,
    QF_LOSS_KEY,
    QF_MAX_KEY,
    QF_MEAN_KEY,
    QF_MIN_KEY,
    QF_PREDS,
    QF_TWIN_LOSS_KEY,
    QF_TWIN_PREDS,
    TD_ERROR_MEAN_KEY,
)
from ray.rllib.algorithms.sac.torch.sac_torch_learner import SACTorchLearner
from ray.rllib.core.columns import Columns
from ray.rllib.core.learner.learner import POLICY_LOSS_KEY
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.metrics import ALL_MODULES
from ray.rllib.utils.typing import ModuleID, ParamDict, TensorType
from ray.tune.result import TRAINING_ITERATION

torch, nn = try_import_torch()

# Metric keys for Lagrangian-specific values.
CQL_ALPHA_KEY = "cql_alpha_value"
LAGRANGIAN_LOSS_KEY = "lagrangian_loss"
CQL_CONSTRAINT_KEY = "cql_constraint"   # E_μ[Q] - E_data[Q]


class CQLTorchLearner(SACTorchLearner):
    """CQL Learner (new API stack) with optional Lagrangian α' scheduling.

    When ``config.lagrangian=True`` the conservative weight α' (called
    ``min_q_weight`` in the fixed case) is treated as a learnable parameter
    whose gradient update pushes the Q-gap toward ``config.lagrangian_thresh``.

    When ``config.lagrangian=False`` the behaviour is identical to the
    upstream implementation with a fixed ``config.min_q_weight``.
    """

    # ── Lifecycle ────────────────────────────────────────────────────────────

    @override(SACTorchLearner)
    def build(self) -> None:
        super().build()
        # Per-module learnable log(α') for the Lagrangian form.
        # We store in log-space so that α' = exp(log_α') ≥ 0 always.
        self._log_cql_alpha: Dict[ModuleID, torch.nn.Parameter] = {}
        self._cql_alpha_optims: Dict[ModuleID, torch.optim.Optimizer] = {}
        self._pending_lagrangian_losses: Dict[ModuleID, TensorType] = {}

    def get_state(
        self,
        components=None,
        *,
        inference_only: bool = False,
    ) -> Dict:
        state = super().get_state(components, inference_only=inference_only)
        state["log_cql_alpha"] = {
            mid: param.data.cpu().item()
            for mid, param in self._log_cql_alpha.items()
        }
        state["cql_alpha_optim_states"] = {
            mid: optim.state_dict()
            for mid, optim in self._cql_alpha_optims.items()
        }
        return state

    def set_state(self, state: Dict) -> None:
        super().set_state(state)
        for mid, val in state.get("log_cql_alpha", {}).items():
            param = self._log_cql_alpha.get(mid)
            if param is not None:
                param.data.fill_(val)
        for mid, osd in state.get("cql_alpha_optim_states", {}).items():
            optim = self._cql_alpha_optims.get(mid)
            if optim is not None:
                optim.load_state_dict(osd)

    def _get_log_cql_alpha(
        self, module_id: ModuleID, config: CQLConfig
    ) -> torch.nn.Parameter:
        """Lazily create and register log(α') for a module."""
        if module_id not in self._log_cql_alpha:
            # Initialise at log(lagrangian_thresh) so α' starts near the target.
            # Place on the same device as the learner's parameters.
            device = next(self.module[module_id].parameters()).device
            init_val = torch.log(
                torch.tensor(
                    float(config.lagrangian_thresh),
                    dtype=torch.float32,
                    device=device,
                )
            )
            param = torch.nn.Parameter(init_val)
            self._log_cql_alpha[module_id] = param
            lr = config.lagrangian_lr
            self._cql_alpha_optims[module_id] = torch.optim.Adam([param], lr=lr)
        return self._log_cql_alpha[module_id]

    # ── Loss ────────────────────────────────────────────────────────────────

    @override(SACTorchLearner)
    def compute_loss_for_module(
        self,
        *,
        module_id: ModuleID,
        config: CQLConfig,
        batch: Dict,
        fwd_out: Dict[str, TensorType],
    ) -> TensorType:

        # TODO (simon, sven): Add upstream information pieces into this
        #  timesteps call arg to Learner.update_...().
        self.metrics.log_value(
            (ALL_MODULES, TRAINING_ITERATION),
            1,
            reduce="sum",
        )

        # ── Action distribution ──────────────────────────────────────────
        action_dist_class = self.module[module_id].get_train_action_dist_cls()
        action_dist_curr = action_dist_class.from_logits(
            fwd_out[Columns.ACTION_DIST_INPUTS]
        )

        # ── Alpha (entropy temperature) loss ────────────────────────────
        # TODO (simon): Check, why log(alpha) is used, prob. just better
        # to optimize a monotonic function. Original equation uses alpha.
        alpha_loss = -torch.mean(
            self.curr_log_alpha[module_id]
            * (fwd_out["logp_resampled"].detach() + self.target_entropy[module_id])
        )
        alpha = torch.exp(self.curr_log_alpha[module_id])

        # ── Actor loss ──────────────────────────────────────────────────
        # Behavior cloning warmup for the first `bc_iters` iterations.
        if (
            self.metrics.peek((ALL_MODULES, TRAINING_ITERATION), default=0)
            >= config.bc_iters
        ):
            actor_loss = torch.mean(
                alpha.detach() * fwd_out["logp_resampled"] - fwd_out["q_curr"]
            )
        else:
            bc_logps_curr = action_dist_curr.logp(batch[Columns.ACTIONS])
            actor_loss = torch.mean(
                alpha.detach() * fwd_out["logp_resampled"] - bc_logps_curr
            )

        # ── Bellman targets ─────────────────────────────────────────────
        q_selected = fwd_out[QF_PREDS]
        if config.twin_q:
            q_twin_selected = fwd_out[QF_TWIN_PREDS]

        if not config.deterministic_backup:
            q_next = (
                fwd_out["q_target_next"]
                - alpha.detach() * fwd_out["logp_next_resampled"]
            )
        else:
            q_next = fwd_out["q_target_next"]

        q_next_masked = (1.0 - batch[Columns.TERMINATEDS].float()) * q_next

        # TODO (simon, sven): Kumar et al. (2020) use here also a reward scaler.
        # TODO (simon): Add an `n_step` option to the `AddNextObsToBatch` connector.
        # TODO (simon): Implement n_step.
        q_selected_target = (
            batch[Columns.REWARDS] + config.gamma * q_next_masked
        ).detach()

        # ── TD errors ────────────────────────────────────────────────────
        td_error = torch.abs(q_selected - q_selected_target)
        if config.twin_q:
            td_error += torch.abs(q_twin_selected - q_selected_target)
            td_error *= 0.5

        # ── SAC critic (MSBE) loss ───────────────────────────────────────
        # TODO (simon): Add the Huber loss as an alternative (SAC uses it).
        sac_critic_loss = torch.nn.MSELoss(reduction="mean")(
            q_selected, q_selected_target
        )
        if config.twin_q:
            sac_critic_twin_loss = torch.nn.MSELoss(reduction="mean")(
                q_twin_selected, q_selected_target
            )

        # ── CQL entropy loss ─────────────────────────────────────────────
        # We use the entropy version of CQL (performs best empirically).
        #
        # random_density: log-probability under the uniform action distribution μ.
        # TODO (simon): This is the density for a discrete uniform distribution.
        # For a continuous uniform over [low, high] this should be
        # log(1 / (high - low)) instead of log(0.5).
        random_density = torch.log(
            torch.pow(
                0.5,
                torch.tensor(
                    fwd_out["actions_curr_repeat"].shape[-1],
                    device=fwd_out["actions_curr_repeat"].device,
                ),
            )
        )

        # Stack Q-values from three action sources and subtract log-probs
        # (entropy version). logsumexp approximates the soft-max over OOD Q.
        q_repeat = torch.cat(
            [
                fwd_out["q_rand_repeat"] - random_density,
                fwd_out["q_next_repeat"] - fwd_out["logps_next_repeat"].detach(),
                fwd_out["q_curr_repeat"] - fwd_out["logps_curr_repeat"].detach(),
            ],
            dim=1,
        )

        # ── Lagrangian adaptive α' vs. fixed min_q_weight ───────────────
        #
        # Fixed form (upstream, config.lagrangian=False):
        #   cql_loss = logsumexp(Q_ood) * min_q_weight - Q_data * min_q_weight
        #
        # Lagrangian form (config.lagrangian=True, Kumar et al. §3.2):
        #   The constraint is:  cql_constraint ≤ lagrangian_thresh
        #   where  cql_constraint = E_μ[Q] - E_data[Q]  (the Q-gap).
        #
        #   α' (min_q_weight) becomes a learnable parameter updated by:
        #     L_lagrangian = α' * (cql_constraint - lagrangian_thresh)
        #   Gradient ascent on α' pushes it up when the constraint is
        #   violated and down when it is satisfied. Gradient descent on
        #   the critic uses α' * cql_constraint as before.
        #
        # This is the single-constraint specialisation of the BCLM
        # p(r)^T W p(r) penalty manifold — one regime, one multiplier.
        #
        # TODO (future PR): Extend to multi-constraint CQL via the full
        # BCLM RegimeCorrelationMatrix: one α'_j per constraint type
        # (e.g. per-region conservatism, per-objective Q), with pairwise
        # overlap discounts W_ij = -η·ρ_ij·√(α'_i·α'_j) to avoid
        # double-penalising co-firing constraints.

        logsumexp_q = (
            torch.logsumexp(q_repeat / config.temperature, dim=1).mean()
            * config.temperature
        )
        cql_constraint = logsumexp_q - q_selected.mean()

        if config.lagrangian:
            log_cql_alpha = self._get_log_cql_alpha(module_id, config)
            cql_alpha = torch.clamp(torch.exp(log_cql_alpha), min=0.0)

            # Critic CQL term: α' is detached here so the critic gradient
            # update does not propagate back to the Lagrangian parameter.
            cql_loss = cql_alpha.detach() * cql_constraint
        else:
            cql_alpha = torch.tensor(config.min_q_weight)
            cql_loss = config.min_q_weight * cql_constraint

        critic_loss = sac_critic_loss + cql_loss

        # Twin Q CQL loss (same structure).
        if config.twin_q:
            q_twin_repeat = torch.cat(
                [
                    fwd_out["q_twin_rand_repeat"] - random_density,
                    fwd_out["q_twin_next_repeat"]
                    - fwd_out["logps_next_repeat"].detach(),
                    fwd_out["q_twin_curr_repeat"]
                    - fwd_out["logps_curr_repeat"].detach(),
                ],
                dim=1,
            )
            logsumexp_q_twin = (
                torch.logsumexp(q_twin_repeat / config.temperature, dim=1).mean()
                * config.temperature
            )
            cql_twin_constraint = logsumexp_q_twin - q_twin_selected.mean()

            if config.lagrangian:
                cql_twin_loss = cql_alpha.detach() * cql_twin_constraint
            else:
                cql_twin_loss = config.min_q_weight * cql_twin_constraint

            critic_twin_loss = sac_critic_twin_loss + cql_twin_loss

        # Lagrangian loss for α' (maximised w.r.t. α'):
        #   L_lagrangian = -α' * (mean_constraint - threshold)
        # When twin_q=True, average both Q constraints — matching the
        # old-stack reference: 0.5 * (-min_qf1_loss - min_qf2_loss).
        # cql_alpha must NOT be detached — gradient must reach log_cql_alpha.
        # Constraints ARE detached — Lagrangian update must not affect Q-nets.
        if config.lagrangian:
            if config.twin_q:
                mean_constraint = 0.5 * (
                    cql_constraint.detach() + cql_twin_constraint.detach()
                )
            else:
                mean_constraint = cql_constraint.detach()
            lagrangian_loss = -cql_alpha * (
                mean_constraint - config.lagrangian_thresh
            )
        else:
            lagrangian_loss = torch.tensor(0.0)

        # ── Total loss ───────────────────────────────────────────────────
        total_loss = actor_loss + critic_loss + alpha_loss
        if config.twin_q:
            total_loss += 0.5 * critic_twin_loss - 0.5 * critic_loss

        # ── Metrics ──────────────────────────────────────────────────────
        self.metrics.log_dict(
            {
                POLICY_LOSS_KEY: actor_loss,
                QF_LOSS_KEY: critic_loss,
                "cql_loss": cql_loss,
                "alpha_loss": alpha_loss,
                "alpha_value": alpha.squeeze(),
                "log_alpha_value": torch.log(alpha).squeeze(),
                "target_entropy": self.target_entropy[module_id],
                LOGPS_KEY: torch.mean(fwd_out["logp_resampled"]),
                QF_MEAN_KEY: torch.mean(fwd_out["q_curr_repeat"]),
                QF_MAX_KEY: torch.max(fwd_out["q_curr_repeat"]),
                QF_MIN_KEY: torch.min(fwd_out["q_curr_repeat"]),
                TD_ERROR_MEAN_KEY: torch.mean(td_error),
                CQL_ALPHA_KEY: cql_alpha.detach(),
                CQL_CONSTRAINT_KEY: cql_constraint.detach(),
                LAGRANGIAN_LOSS_KEY: lagrangian_loss.detach(),
            },
            key=module_id,
            window=1,
        )
        self._temp_losses[(module_id, POLICY_LOSS_KEY)] = actor_loss
        self._temp_losses[(module_id, QF_LOSS_KEY)] = critic_loss
        self._temp_losses[(module_id, "alpha_loss")] = alpha_loss
        if config.lagrangian:
            self._pending_lagrangian_losses[module_id] = lagrangian_loss

        if config.twin_q:
            self.metrics.log_value(
                key=(module_id, QF_TWIN_LOSS_KEY),
                value=critic_twin_loss,
                window=1,
            )
            self._temp_losses[(module_id, QF_TWIN_LOSS_KEY)] = critic_twin_loss

        return total_loss

    # ── Gradient step ────────────────────────────────────────────────────────

    @override(SACTorchLearner)
    def compute_gradients(
        self, loss_per_module: Dict[ModuleID, TensorType], **kwargs
    ) -> ParamDict:
        grads = {}
        for module_id in set(loss_per_module.keys()) - {ALL_MODULES}:
            for optim_name, optim in self.get_optimizers_for_module(module_id):
                optim.zero_grad(set_to_none=True)
                loss_tensor = self._temp_losses.pop((module_id, optim_name + "_loss"))
                # lagrangian_loss only flows through log_cql_alpha, which is
                # independent of the main forward graph. retain_graph is only
                # needed for critic/twin-critic (shared activations), not for
                # policy or alpha (last users of their sub-graphs).
                retain = optim_name not in ["policy", "alpha"]
                loss_tensor.backward(retain_graph=retain)
                # TODO (simon): Check another time the graph for overlapping
                # gradients.
                grads.update(
                    {
                        pid: grads[pid] + p.grad.clone()
                        if pid in grads
                        else p.grad.clone()
                        for pid, p in self.filter_param_dict_for_optimizer(
                            self._params, optim
                        ).items()
                    }
                )

        assert not self._temp_losses
        return grads

    @override(SACTorchLearner)
    def apply_gradients(self, gradients_dict: ParamDict) -> None:
        """Apply gradients, then step the Lagrangian α' optimizers."""
        super().apply_gradients(gradients_dict)

        # Step the per-module Lagrangian α' optimizer independently.
        # This mirrors how `curr_log_alpha` (entropy temp) is updated
        # separately from policy/critic.
        for module_id, lag_loss in self._pending_lagrangian_losses.items():
            optim = self._cql_alpha_optims[module_id]
            optim.zero_grad(set_to_none=True)
            lag_loss.backward()
            optim.step()
        self._pending_lagrangian_losses.clear()
