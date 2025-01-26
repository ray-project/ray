import math
import logging
import torch
import torch.nn as nn
from typing import Any, Dict

from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.algorithms.ppo.torch.ppo_torch_learner import PPOTorchLearner
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import ModuleID, TensorType
from ray.rllib.utils.torch_utils import explained_variance

from ray.rllib.algorithms.ppo.ppo import (
    LEARNER_RESULTS_KL_KEY,
    LEARNER_RESULTS_VF_EXPLAINED_VAR_KEY,
    LEARNER_RESULTS_VF_LOSS_UNCLIPPED_KEY,
)
from ray.rllib.core.columns import Columns
from ray.rllib.core.learner.learner import POLICY_LOSS_KEY, VF_LOSS_KEY, ENTROPY_KEY
from ray.rllib.evaluation.postprocessing import Postprocessing

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

torch, _ = try_import_torch()


class PPOTorchLearnerWithMOGLoss(PPOTorchLearner):
    """
    A custom PPOTorchLearner with added log-likelihood loss for mixture of Gaussian (MoG) model.

    This additional loss component optimizes the policy by incorporating a MoG model
    in the critic network. The MoG increases expressiveness in modeling multimodal
    value functions, which can lead to better value function estimation and representation learning.

    As investigated in [Shahriari et al., 2022] from DeepMind, this expressiveness can improve
    policy effectiveness through two mechanisms: adaptive Mahalanobis reweighting and improved
    feature learning. Their findings concluded that both mechanisms contribute to the observed
    performance gains.

    Paper: https://arxiv.org/pdf/2204.10256

    Key difference: While the paper uses a cross-entropy loss, this implementation focuses on
    a form of the negative log-likelihood loss.
    """

    def compute_log_likelihood(
        self,
        td_targets: torch.Tensor,
        mu_current: torch.Tensor,
        sigma_current: torch.Tensor,
        alpha_current: torch.Tensor,
    ) -> torch.Tensor:
        
        """
        Computes a custom negative log-likelihood loss for a mixture of Gaussian (MoG)

        This loss function incorporates a small adjustment: passing alpha_current through
        a log_softmax instead of a regular softmax to prevent very small values that could 
        lead to numerical instability

        The purpose of this loss is to measure how well the critic's predicted value distribution 
        aligns with the target value distribution, as derived from the Bellman operator.
        Intuitively, it can be thought of as comparing two "mounds of dirt" and quantifying the 
        difference between them.

        Args:
            td_targets : torch.Tensor
                The target values for the value function, derived from the next timestep.
            mu_current : torch.Tensor
                The predicted means of the MoG for the current timestep.
            sigma_current : torch.Tensor
                The predicted standard deviations of the MoG for the current timestep
            alpha_current : torch.Tensor
                The predicted mixture weights of the MoG for the current timestep.

        Returns:
            summing_log : torch.Tensor
                The computed negative log-likelihood loss for the given inputs.
        """     

        td_targets_expanded = td_targets.unsqueeze(1)
        sigma_clamped = sigma_current
        log_2_pi = torch.log(2 * torch.tensor(math.pi))
        factor = -torch.log(sigma_clamped) - 0.5 * log_2_pi
        mus = td_targets_expanded - mu_current

        logp = torch.clamp(
            factor - torch.square(mus) / (2 * torch.square(sigma_clamped)), -1e10, 10
        )
        # Little trick of using log_softmax on the current alphas to prevent nans
        loga = torch.clamp(nn.functional.log_softmax(alpha_current, dim=-1), 1e-6, None)

        summing_log = -torch.logsumexp(logp + loga, dim=-1)
        return summing_log

    @override(PPOTorchLearner)
    def compute_loss_for_module(
        self,
        *,
        module_id: ModuleID,
        config: PPOConfig,
        batch: Dict[str, Any],
        fwd_out: Dict[str, TensorType],
    ) -> TensorType:

        if Columns.LOSS_MASK in batch:
            mask = batch[Columns.LOSS_MASK]
            num_valid = torch.sum(mask)

            def possibly_masked_mean(data_):
                return torch.sum(data_[mask]) / num_valid

        else:
            possibly_masked_mean = torch.mean

        module = self.module[module_id].unwrapped()

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

        # Only calculate kl loss if necessary (kl-coeff > 0.0).
        if config.use_kl_loss:
            action_kl = prev_action_dist.kl(curr_action_dist)
            mean_kl_loss = possibly_masked_mean(action_kl)
        else:
            mean_kl_loss = torch.tensor(0.0, device=logp_ratio.device)

        curr_entropy = curr_action_dist.entropy()
        mean_entropy = possibly_masked_mean(curr_entropy)

        surrogate_loss = torch.min(
            batch[Postprocessing.ADVANTAGES] * logp_ratio,
            batch[Postprocessing.ADVANTAGES]
            * torch.clamp(logp_ratio, 1 - config.clip_param, 1 + config.clip_param),
        )

        # Compute the MOG value loss using the negative log-likelihood
        if config.use_critic:
            rewards = batch["rewards"]
            dones = batch["dones"]
            gamma = config["gamma"]

            mog_components = batch[Columns.MOG_COMPONENTS]
            mu_current = mog_components["means"]
            sigmas_current = mog_components["sigmas"]
            alpha_current = mog_components["alphas"]

            """TODO: fix this for multi-learner setup"""
            mog_output_next = batch[Columns.NEXT_MOG_COMPONENTS]
            mu_next = mog_output_next["means"]
            alpha_next = mog_output_next["alphas"]
            alpha_next = torch.clamp(
                nn.functional.softmax(alpha_next, dim=-1), 1e-6, None
            )

            # Detach target
            next_state_values = torch.sum(mu_next * alpha_next, dim=1).clone().detach()
            td_targets = rewards + gamma * next_state_values * (1 - dones.float())
            # This alpha current should not be passed through a softmax yet
            log_likelihood = self.compute_log_likelihood(
                td_targets, mu_current, sigmas_current, alpha_current
            )
            log_likelihood_clipped = torch.clamp(log_likelihood, -10, 80)
            nll_loss = torch.mean(log_likelihood_clipped)
            nll_loss_unclipped = torch.mean(log_likelihood)

            # For logging purposes
            value_fn_out = torch.sum(
                mu_current
                * torch.clamp(nn.functional.softmax(alpha_current, dim=-1), 1e-6, None)
            )
        # Ignore the value function -> Set all to 0.0.
        else:
            z = torch.tensor(0.0, device=surrogate_loss.device)
            value_fn_out = mean_vf_unclipped_loss = vf_loss_clipped = mean_vf_loss = z

        total_loss = possibly_masked_mean(
            -surrogate_loss
            + config.vf_loss_coeff * nll_loss
            - (
                self.entropy_coeff_schedulers_per_module[module_id].get_current_value()
                * curr_entropy
            )
        )

        # Add mean_kl_loss (already processed through `possibly_masked_mean`),
        # if necessary
        if config.use_kl_loss:
            total_loss += self.curr_kl_coeffs_per_module[module_id] * mean_kl_loss

        # Log important loss stats.
        self.metrics.log_dict(
            {
                POLICY_LOSS_KEY: -possibly_masked_mean(surrogate_loss),
                VF_LOSS_KEY: nll_loss,
                LEARNER_RESULTS_VF_LOSS_UNCLIPPED_KEY: nll_loss_unclipped,
                LEARNER_RESULTS_VF_EXPLAINED_VAR_KEY: explained_variance(
                    batch[Postprocessing.VALUE_TARGETS], value_fn_out
                ),
                ENTROPY_KEY: mean_entropy,
                LEARNER_RESULTS_KL_KEY: mean_kl_loss,
            },
            key=module_id,
            window=1,  # <- Single items (should not be mean/ema-reduced over time).
        )
        return total_loss
