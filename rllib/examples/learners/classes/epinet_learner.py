import logging
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

CRITIC_OUTPUTS = "critic_outputs"
NEXT_CRITIC_OUTPUTS = "next_critic_outputs"
CRITIC_OUTPUT_BASE = "critic_output_base"
CRITIC_OUTPUT_ENN = "critic_output_enn"


class PPOTorchLearnerWithEpinetLoss(PPOTorchLearner):
    """
    A custom `PPOTorchLearner` with added epinet to learn epistemic uncertainty. This also
    demonstrates how to put custom components into `batch` and retrieve them.

    The addition of the epinet is very powerful and aids in joint dependencies between states.
    This is done by looking over a z_dim number of priors and testing the base network on each one.
    The more certain it is the more concentrated the value output will be. If the network is
    uncertain, these output values will be sporadic and therefore uncertain.

    The papers and further information on the epinet can be seen in the epinet.py file.
    """

    @override(PPOTorchLearner)
    def compute_loss_for_module(
        self,
        *,
        module_id: ModuleID,
        config: PPOConfig,
        batch: Dict[str, Any],
        fwd_out: Dict[str, TensorType],
    ) -> TensorType:

        """
        Computes a custom loss for the critic network. This is done by having a basic loss function
        for the base critic and adding to it the `enn_loss` component in which the epinet learns
        the uncertainty of the environment. These losses are separate computation graphs. This is
        done by using the TD loss for both the base critic network and the epinet network.
        """

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

        # Compute critic and epinet loss.
        if config.use_critic:
            rewards = batch[Columns.REWARDS]
            dones = batch[Columns.TERMINATEDS]
            gamma = config.gamma
            # Get critic/ENN output to calculate TD loss.
            critic_output = batch[CRITIC_OUTPUTS]
            base_critic_out = critic_output["critic_output_base"]
            enn_out = critic_output["critic_output_enn"]
            # Current value of base network and epinet - detach base so we can do the epinet loss.
            ENN_total_output = base_critic_out.clone().detach() + enn_out
            # Next state's value.
            next_critic_output = batch[NEXT_CRITIC_OUTPUTS]
            next_critic_out = next_critic_output["critic_output_base"]
            next_enn_out = next_critic_output["critic_output_enn"]
            next_value = next_critic_out + next_enn_out
            ENN_target = rewards + gamma * (next_value.clone().detach()) * (
                1 - dones.float()
            )
            # Calculate base MSE loss.
            enn_loss = nn.functional.mse_loss(ENN_total_output, ENN_target)
            # Base critic loss since we detached the gradient map between ENN/base critic.
            base_critic_target = rewards + gamma * (
                next_critic_out.clone().detach()
            ) * (1 - dones.float())
            base_critic_loss = nn.functional.mse_loss(
                base_critic_out, base_critic_target
            )
            # Add both losses together to get final value function loss.
            vf_loss = enn_loss + base_critic_loss
            vf_loss_clipped = torch.clamp(vf_loss, 0, config.vf_clip_param)
            value_fn_out = ENN_total_output
        # Ignore the value function -> Set all to 0.0.
        else:
            z = torch.tensor(0.0, device=surrogate_loss.device)
            value_fn_out = z

        total_loss = possibly_masked_mean(
            -surrogate_loss
            + config.vf_loss_coeff * vf_loss
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
                VF_LOSS_KEY: vf_loss,
                LEARNER_RESULTS_VF_LOSS_UNCLIPPED_KEY: vf_loss_clipped,
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
