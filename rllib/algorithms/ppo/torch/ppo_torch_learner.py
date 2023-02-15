import logging
from typing import Mapping, Any

from ray.rllib.core.learner.torch.torch_learner import TorchLearner
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.torch_utils import (
    explained_variance,
)
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import TensorType

torch, nn = try_import_torch()

logger = logging.getLogger(__name__)


class PPOTorchLearner(TorchLearner):
    """Implements PPO loss / update logic on top of TorchLearner.

    This class implements the ppo loss under `_compute_loss_per_module()` and the
    additional non-gradient based updates such as KL-coeff and learning rate updates
    under `_additional_update_per_module()`.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # TODO (Kourosh): Move these failures to config.validate() or support them.
        self.entropy_coeff_scheduler = None
        if self.hps.entropy_coeff_schedule:
            raise ValueError("entropy_coeff_schedule is not supported in Learner yet")

        # TODO (Kourosh): Create a way on the base class for users to define arbitrary
        # schedulers for learning rates.
        self.lr_scheduler = None
        if self.hps.lr_schedule:
            raise ValueError("lr_schedule is not supported in Learner yet")

        # TODO (Kourosh): We can still use mix-ins in the new design. Do we want that?
        # Most likely not. I rather be specific about everything. kl_coeff is a
        # none-gradient based update which we can define here and add as update with
        # additional_update() method.
        self.kl_coeff = self.hps.kl_coeff
        self.kl_target = self.hps.kl_target

    @override(TorchLearner)
    def compute_loss_per_module(
        self, module_id: str, batch: SampleBatch, fwd_out: Mapping[str, TensorType]
    ) -> TensorType:
        # TODO (Kourosh): batch type is NestedDict.
        # TODO (Kourosh): We may or may not user module_id. For example if we have an
        # agent based learning rate scheduler, we may want to use module_id to get the
        # learning rate for that agent.
        # TODO (Kourosh): come back to RNNs later

        curr_action_dist = fwd_out[SampleBatch.ACTION_DIST]
        action_dist_class = type(fwd_out[SampleBatch.ACTION_DIST])
        prev_action_dist = action_dist_class(
            **batch[SampleBatch.ACTION_DIST_INPUTS].asdict()
        )

        logp_ratio = torch.exp(
            fwd_out[SampleBatch.ACTION_LOGP] - batch[SampleBatch.ACTION_LOGP]
        )

        # Only calculate kl loss if necessary (kl-coeff > 0.0).
        if self.hps.kl_coeff > 0.0:
            action_kl = prev_action_dist.kl(curr_action_dist)
            mean_kl_loss = torch.mean(action_kl)
            if mean_kl_loss.isinf():
                logger.warning(
                    "KL divergence is non-finite, this will likely destabilize "
                    "your model and the training process. Action(s) in a "
                    "specific state have near-zero probability. "
                    "This can happen naturally in deterministic "
                    "environments where the optimal policy has zero mass "
                    "for a specific action. To fix this issue, consider "
                    "setting the coefficient for the KL loss term to "
                    "zero or increasing policy entropy."
                )
        else:
            mean_kl_loss = torch.tensor(0.0, device=logp_ratio.device)

        curr_entropy = fwd_out["entropy"]
        mean_entropy = torch.mean(curr_entropy)

        surrogate_loss = torch.min(
            batch[Postprocessing.ADVANTAGES] * logp_ratio,
            batch[Postprocessing.ADVANTAGES]
            * torch.clamp(logp_ratio, 1 - self.hps.clip_param, 1 + self.hps.clip_param),
        )

        # Compute a value function loss.
        if self.hps.use_critic:
            value_fn_out = fwd_out[SampleBatch.VF_PREDS]
            vf_loss = torch.pow(value_fn_out - batch[Postprocessing.VALUE_TARGETS], 2.0)
            vf_loss_clipped = torch.clamp(vf_loss, 0, self.hps.vf_clip_param)
            mean_vf_loss = torch.mean(vf_loss_clipped)
        # Ignore the value function.
        else:
            value_fn_out = torch.tensor(0.0).to(surrogate_loss.device)
            vf_loss_clipped = mean_vf_loss = torch.tensor(0.0).to(surrogate_loss.device)

        total_loss = torch.mean(
            -surrogate_loss
            + self.hps.vf_loss_coeff * vf_loss_clipped
            - self.hps.entropy_coeff * curr_entropy
        )

        # Add mean_kl_loss (already processed through `reduce_mean_valid`),
        # if necessary.
        if self.hps.kl_coeff > 0.0:
            total_loss += self.hps.kl_coeff * mean_kl_loss

        return {
            self.TOTAL_LOSS_KEY: total_loss,
            "mean_policy_loss": -torch.mean(surrogate_loss),
            "mean_vf_loss": mean_vf_loss,
            "vf_explained_var": explained_variance(
                batch[Postprocessing.VALUE_TARGETS], value_fn_out
            ),
            "mean_entropy": mean_entropy,
            "mean_kl_loss": mean_kl_loss,
        }

    @override(TorchLearner)
    def additional_update_per_module(
        self, module_id: str, sampled_kl_values: dict, timestep: int
    ) -> Mapping[str, Any]:

        sampled_kl = sampled_kl_values[module_id]
        if sampled_kl > 2.0 * self.kl_target:
            # TODO (Kourosh) why not 2?
            self.kl_coeff *= 1.5
        elif sampled_kl < 0.5 * self.kl_target:
            self.kl_coeff *= 0.5

        results = {"kl_coeff": self.kl_coeff}

        # TODO (Kourosh): We may want to index into the schedulers to get the right one
        # for this module
        if self.entropy_coeff_scheduler is not None:
            self.entropy_coeff_scheduler.update(timestep)

        if self.lr_scheduler is not None:
            self.lr_scheduler.update(timestep)

        return results
