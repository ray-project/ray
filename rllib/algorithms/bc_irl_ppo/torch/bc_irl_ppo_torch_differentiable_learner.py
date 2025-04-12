import logging
import numpy as np

from typing import Any, Dict, TYPE_CHECKING

from ray.rllib.algorithms.bc_irl_ppo.bc_irl_ppo import BCIRLPPOConfig
from ray.rllib.algorithms.bc_irl_ppo.bc_irl_ppo_differentiable_learner import (
    BCIRLPPODifferentiableLearner,
)
from ray.rllib.algorithms.bc_irl_ppo.torch.bc_irl_ppo_torch_meta_learner import (
    REWARD_MODULE,
)
from ray.rllib.algorithms.ppo.ppo import (
    LEARNER_RESULTS_KL_KEY,
    LEARNER_RESULTS_CURR_KL_COEFF_KEY,
    LEARNER_RESULTS_VF_EXPLAINED_VAR_KEY,
    LEARNER_RESULTS_VF_LOSS_UNCLIPPED_KEY,
)
from ray.rllib.core.columns import Columns
from ray.rllib.core.learner.learner import POLICY_LOSS_KEY, VF_LOSS_KEY, ENTROPY_KEY
from ray.rllib.core.learner.torch.torch_differentiable_learner import (
    TorchDifferentiableLearner,
)
from ray.rllib.core.models.base import CRITIC, ENCODER_OUT
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils.annotations import override, OverrideToImplementCustomLogic
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.torch_utils import explained_variance
from ray.rllib.utils.typing import ModuleID, NamedParamDict, TensorType

if TYPE_CHECKING:
    from ray.rllib.algorithms.algorithm_config import AlgorithmConfig

logger = logging.getLogger(__name__)
torch, nn = try_import_torch()


class BCIRLPPOTorchDifferentiableLearner(
    TorchDifferentiableLearner, BCIRLPPODifferentiableLearner
):
    @OverrideToImplementCustomLogic
    def compute_loss_for_module(
        self,
        *,
        module_id: ModuleID,
        config: "AlgorithmConfig",
        batch: Dict[str, Any],
        fwd_out: Dict[str, TensorType],
    ) -> TensorType:

        if module_id != REWARD_MODULE:

            # Get the policy module.
            module = self.module[module_id].unwrapped()

            # Possibly apply masking to some sub loss terms and to the total loss term
            # at the end. Masking could be used for RNN-based model (zero padded `batch`)
            # and for PPO's batched value function (and bootstrap value) computations,
            # for which we add an (artificial) timestep to each episode to
            # simplify the actual computation.
            if Columns.LOSS_MASK in batch:
                mask = batch[Columns.LOSS_MASK]
                num_valid = torch.sum(mask)

                def possibly_masked_mean(data_):
                    return torch.sum(data_[mask]) / num_valid

            else:
                possibly_masked_mean = torch.mean

            # Compute advantages with the reward model.
            batch[Columns.ADVANTAGES] = self._compute_general_advantage(
                batch, fwd_out=fwd_out, module=module, config=config
            )

            action_dist_class_train = module.get_train_action_dist_cls()
            action_dist_class_exploration = module.get_exploration_action_dist_cls()

            curr_action_dist = action_dist_class_train.from_logits(
                fwd_out[Columns.ACTION_DIST_INPUTS]
            )
            # TODO (sven): We should ideally do this in the LearnerConnector (separation of
            #  concerns: Only do things on the EnvRunners that are required for computing
            #  actions, do NOT do anything on the EnvRunners that's only required for a
            #   training update).
            prev_action_dist = action_dist_class_exploration.from_logits(
                batch[Columns.ACTION_DIST_INPUTS]
            )

            logp_ratio = torch.exp(
                curr_action_dist.logp(batch[Columns.ACTIONS])
                - batch[Columns.ACTION_LOGP]
            )

            # Only calculate kl loss if necessary (kl-coeff > 0.0).
            if config.ppo_use_kl_loss:
                action_kl = prev_action_dist.kl(curr_action_dist)
                mean_kl_loss = possibly_masked_mean(action_kl)
            else:
                mean_kl_loss = torch.tensor(0.0, device=logp_ratio.device)

            curr_entropy = curr_action_dist.entropy()
            mean_entropy = possibly_masked_mean(curr_entropy)

            surrogate_loss = torch.min(
                batch[Columns.ADVANTAGES] * logp_ratio,
                batch[Columns.ADVANTAGES]
                * torch.clamp(
                    logp_ratio, 1 - config.ppo_clip_param, 1 + config.ppo_clip_param
                ),
            )

            # Compute a value function loss.
            if config.ppo_use_critic:
                value_fn_out = fwd_out[
                    Columns.VF_PREDS
                ]  # self._compute_values_functional_call(params, batch, fwd_out, module_id)
                vf_loss = torch.pow(
                    value_fn_out - batch[Columns.VALUE_TARGETS].detach(), 2.0
                )
                vf_loss_clipped = torch.clamp(vf_loss, 0, config.ppo_vf_clip_param)
                mean_vf_loss = possibly_masked_mean(vf_loss_clipped)
                mean_vf_unclipped_loss = possibly_masked_mean(vf_loss)
            # Ignore the value function -> Set all to 0.0.
            else:
                z = torch.tensor(0.0, device=surrogate_loss.device)
                value_fn_out = (
                    mean_vf_unclipped_loss
                ) = vf_loss_clipped = mean_vf_loss = z

            total_loss = possibly_masked_mean(
                -surrogate_loss
                + config.ppo_vf_loss_coeff * vf_loss_clipped
                - (
                    self.entropy_coeff_schedulers_per_module[
                        module_id
                    ].get_current_value()
                    * curr_entropy
                )
            )

            # Add mean_kl_loss (already processed through `possibly_masked_mean`),
            # if necessary.
            if config.ppo_use_kl_loss:
                total_loss += self.curr_kl_coeffs_per_module[module_id] * mean_kl_loss

            # Log important loss stats.
            self.metrics.log_dict(
                {
                    POLICY_LOSS_KEY: -possibly_masked_mean(surrogate_loss),
                    VF_LOSS_KEY: mean_vf_loss,
                    LEARNER_RESULTS_VF_LOSS_UNCLIPPED_KEY: mean_vf_unclipped_loss,
                    LEARNER_RESULTS_VF_EXPLAINED_VAR_KEY: explained_variance(
                        batch[Columns.VALUE_TARGETS], value_fn_out
                    ),
                    ENTROPY_KEY: mean_entropy,
                    LEARNER_RESULTS_KL_KEY: mean_kl_loss,
                },
                key=module_id,
                window=1,  # <- single items (should not be mean/ema-reduced over time).
            )
            # Return the total loss.
            return total_loss

    def _make_functional_call(
        self, params: Dict[ModuleID, NamedParamDict], batch: MultiAgentBatch
    ) -> Dict[ModuleID, NamedParamDict]:
        """Makes a functional call for each module in the `MultiRLModule`."""

        functional_policy_calls = {}
        for mid in batch:
            functional_policy_calls[mid] = torch.func.functional_call(
                self._module[mid], params[mid], batch[mid]
            )
            embeddings = functional_policy_calls[mid].get(Columns.EMBEDDINGS)
            if embeddings is None:
                # TODO (simon): All of this would not work, because parameters need to be adjusted to the
                # submodules for a functional call.
                # Separate vf-encoder.
                if hasattr(self.encoder, "critic_encoder"):
                    batch_ = batch
                    if self.is_stateful():
                        # The recurrent encoders expect a `(state_in, h)`  key in the
                        # input dict while the key returned is `(state_in, critic, h)`.
                        batch_ = batch.copy()
                        batch_[Columns.STATE_IN] = batch[Columns.STATE_IN][CRITIC]
                    embeddings = torch.func.functional_call(
                        self._module[mid].encoder.critic_encoder, params, batch
                    )[ENCODER_OUT]
                # Shared encoder.
                else:
                    embeddings = torch.func.functional_call(
                        self._module[mid].encoder, params, batch
                    )[ENCODER_OUT][CRITIC]

            # Value head.
            vf_params = {
                k[len("vf.") :]: v
                for k, v in params[mid].items()
                if k.startswith("vf.")
            }
            vf_out = torch.func.functional_call(
                self._module[mid].vf, vf_params, embeddings
            )
            # Squeeze out last dimension (single node value head).
            functional_policy_calls[mid][Columns.VF_PREDS] = vf_out.squeeze(-1)
            functional_policy_calls[mid].update(
                torch.func.functional_call(
                    self._module[REWARD_MODULE], params[REWARD_MODULE], batch[mid]
                )
            )

        return functional_policy_calls

    # def _compute_values_functional_call(self, params, batch, fwd_out, module_id):

    #     embeddings = fwd_out.get(Columns.EMBEDDINGS)
    #     if embeddings is None:
    #         # Separate vf-encoder.
    #         if hasattr(self.encoder, "critic_encoder"):
    #             batch_ = batch
    #             if self.is_stateful():
    #                 # The recurrent encoders expect a `(state_in, h)`  key in the
    #                 # input dict while the key returned is `(state_in, critic, h)`.
    #                 batch_ = batch.copy()
    #                 batch_[Columns.STATE_IN] = batch[Columns.STATE_IN][CRITIC]
    #             embeddings = torch.func.functional_call(
    #                 self._module[module_id].encoder.critic_encoder, params, batch
    #             )[ENCODER_OUT]
    #         # Shared encoder.
    #         else:
    #             embeddings = torch.func.functional_call(
    #                 self._module[module_id].encoder, params, batch
    #             )[ENCODER_OUT][CRITIC]

    #     # Value head.
    #     vf_out = torch.func.functional_call(
    #         self._module[module_id].vf, params, embeddings
    #     )
    #     # Squeeze out last dimension (single node value head).
    #     return vf_out.squeeze(-1)

    @override(BCIRLPPODifferentiableLearner)
    def _update_module_kl_coeff(
        self,
        *,
        module_id: ModuleID,
        config: BCIRLPPOConfig,
        kl_loss: float,
    ) -> None:
        if np.isnan(kl_loss):
            logger.warning(
                f"KL divergence for Module {module_id} is non-finite, this "
                "will likely destabilize your model and the training "
                "process. Action(s) in a specific state have near-zero "
                "probability. This can happen naturally in deterministic "
                "environments where the optimal policy has zero mass for a "
                "specific action. To fix this issue, consider setting "
                "`kl_coeff` to 0.0 or increasing `entropy_coeff` in your "
                "config."
            )

        # Update the KL coefficient.
        curr_var = self.curr_kl_coeffs_per_module[module_id]
        if kl_loss > 2.0 * config.ppo_kl_target:
            # TODO (Kourosh) why not 2?
            curr_var.data *= 1.5
        elif kl_loss < 0.5 * config.ppo_kl_target:
            curr_var.data *= 0.5

        # Log the updated KL-coeff value.
        self.metrics.log_value(
            (module_id, LEARNER_RESULTS_CURR_KL_COEFF_KEY),
            curr_var.item(),
            window=1,
        )

    def _compute_general_advantage(
        self,
        batch,
        fwd_out,
        module,
        config,
    ):
        with torch.no_grad():
            batch[Columns.VF_PREDS] = module.compute_values(batch)
        episode_lens = torch.unique(batch[Columns.EPS_LENS])
        batch[Columns.VALUE_TARGETS] = self._compute_value_targets(
            values=self._unpad_data_if_necessary(episode_lens, batch[Columns.VF_PREDS]),
            rewards=self._unpad_data_if_necessary(
                episode_lens, fwd_out[Columns.REWARDS]
            ),
            terminateds=self._unpad_data_if_necessary(
                episode_lens, batch[Columns.TERMINATEDS].float()
            ),
            truncateds=self._unpad_data_if_necessary(
                episode_lens, batch[Columns.TRUNCATEDS].float()
            ),
            gamma=config.ppo_gamma,
            lambda_=config.ppo_lambda_,
        )
        # assert module_value_targets.shape[0] == episode_lens.sum()

        module_advantages = batch[Columns.VALUE_TARGETS] - batch[Columns.VF_PREDS]

        module_advantages = (module_advantages - module_advantages.mean()) / max(
            1e-4, module_advantages.std()
        )

        return module_advantages

    def _compute_value_targets(
        self,
        values,
        rewards,
        terminateds,
        truncateds,
        gamma: float,
        lambda_: float,
    ):
        """Computes value function (vf) targets given vf predictions and rewards.

        Note that advantages can then easily be computed via the formula:
        advantages = targets - vf_predictions
        """
        # Convert to torch tensors.
        lambda_ = torch.Tensor([lambda_])
        gamma = torch.Tensor([gamma])

        # Force-set all values at terminals (not at truncations!) to 0.0.
        orig_values = flat_values = values * (1.0 - terminateds)

        flat_values = torch.concatenate((flat_values, torch.zeros((1,))), dim=-1)
        intermediates = rewards + gamma * (1.0 - lambda_) * flat_values[1:]
        continues = 1.0 - terminateds

        Rs = []
        last = flat_values[-1]
        for t in reversed(range(intermediates.shape[0])):
            last = intermediates[t] + continues[t] * gamma * lambda_ * last
            Rs.append(last)
            if truncateds[t]:
                last = orig_values[t]

        # Reverse back to correct (time) direction.
        value_targets = torch.concatenate(list(reversed(Rs)))

        return value_targets

    @staticmethod
    def _unpad_data_if_necessary(episode_lens, data):
        """
        Removes right-side zero-padding from data based on `episode_lens`.

        Args:
            episode_lens (list[int]): A list of actual episode lengths.
            data (torch.Tensor): A 2D tensor with right-side zero-padded rows.

        Returns:
            torch.Tensor: A 1D tensor resulting from concatenation of the un-padded
                        input data along the 0-axis.
        """
        # If data does NOT have a time dimension, return it immediately.
        if data.dim() == 1:
            return data

        # Assert that data only has batch (B) and time (T) dimensions.
        assert data.dim() == 2, "data must be a 2D tensor (B, T)"

        new_data = []
        row_idx = 0
        T = data.shape[1]

        for length in episode_lens:
            # Determine how many full rows and remaining elements
            num_rows, col_idx = divmod(length, T)

            # Append all full rows for this episode.
            for _ in range(num_rows):
                new_data.append(data[row_idx])
                row_idx += 1

            # If there's a partial row, append the valid part.
            if col_idx > 0:
                new_data.append(data[row_idx, :col_idx])
                row_idx += 1

        # Concatenate the list of tensors into one 1D tensor.
        return torch.cat(new_data, dim=0)
