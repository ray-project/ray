"""
[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf

[2] Mastering Atari with Discrete World Models - 2021
D. Hafner, T. Lillicrap, M. Norouzi, J. Ba
https://arxiv.org/pdf/2010.02193.pdf
"""
from typing import Any, Dict, Mapping, Tuple

import gymnasium as gym

from ray.rllib.algorithms.dreamerv3.dreamerv3_learner import (
    DreamerV3Learner,
    DreamerV3LearnerHyperparameters,
)
from ray.rllib.core.rl_module.marl_module import ModuleID
from ray.rllib.core.learner.learner import ParamDict
from ray.rllib.core.learner.torch.torch_learner import TorchLearner
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID, SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.torch_utils import symlog, two_hot, clip_gradients
from ray.rllib.utils.typing import TensorType

torch, nn = try_import_torch()


class DreamerV3TfLearner(DreamerV3Learner, TorchLearner):
    """Implements DreamerV3 losses and gradient-based update logic in TensorFlow.

    The critic EMA-copy update step can be found in the `DreamerV3Learner` base class,
    as it is framework independent.

    We define 3 local TensorFlow optimizers for the sub components "world_model",
    "actor", and "critic". Each of these optimizers might use a different learning rate,
    epsilon parameter, and gradient clipping thresholds and procedures.
    """

    @override(TorchLearner)
    def configure_optimizers_for_module(
        self, module_id: ModuleID, hps: DreamerV3LearnerHyperparameters
    ):
        """Create the 3 optimizers for Dreamer learning: world_model, actor, critic.

        The learning rates used are described in [1] and the epsilon values used here
        - albeit probably not that important - are used by the author's own
        implementation.
        """

        dreamerv3_module = self._module[module_id]

        # World Model optimizer.
        optim_world_model = torch.optim.Adam(
            dreamerv3_module.world_model.parameters(),
            lr=hps.world_model_lr,
            eps=1e-8
        )
        self.register_optimizer(
            module_id=module_id,
            optimizer_name="world_model",
            optimizer=optim_world_model,
            params=dreamerv3_module.world_model.parameters(),
        )

        # Actor optimizer.
        optim_actor = torch.optim.Adam(
            dreamerv3_module.actor.parameters(),
            lr=hps.actor_lr,
            eps=1e-5
        )
        self.register_optimizer(
            module_id=module_id,
            optimizer_name="actor",
            optimizer=optim_actor,
            params=dreamerv3_module.actor.parameters(),
        )

        # Critic optimizer.
        optim_critic = torch.optim.Adam(
            dreamerv3_module.critic.parameters(),
            lr=hps.critic_lr,
            eps=1e-5
        )
        self.register_optimizer(
            module_id=module_id,
            optimizer_name="critic",
            optimizer=optim_critic,
            params=dreamerv3_module.critic.parameters(),
        )

    @override(TorchLearner)
    def postprocess_gradients_for_module(
        self,
        *,
        module_id: ModuleID,
        hps: DreamerV3LearnerHyperparameters,
        module_gradients_dict: Dict[str, Any],
    ) -> ParamDict:
        """Performs gradient clipping on the 3 module components' computed grads.

        Note that different grad global-norm clip values are used for the 3
        module components: world model, actor, and critic.
        """
        for optimizer_name, optimizer in self.get_optimizers_for_module(
            module_id=module_id
        ):
            grads_sub_dict = self.filter_param_dict_for_optimizer(
                module_gradients_dict, optimizer
            )
            # Figure out which grad clip setting to use.
            grad_clip = (
                hps.world_model_grad_clip_by_global_norm
                if optimizer_name == "world_model"
                else hps.actor_grad_clip_by_global_norm
                if optimizer_name == "actor"
                else hps.critic_grad_clip_by_global_norm
            )
            global_norm = clip_gradients(
                grads_sub_dict, grad_clip=grad_clip, grad_clip_by="global_norm"
            )
            module_gradients_dict.update(grads_sub_dict)

            # DreamerV3 stats have the format: [WORLD_MODEL|ACTOR|CRITIC]_[stats name].
            self.register_metric(
                module_id,
                optimizer_name.upper() + "_gradients_global_norm",
                global_norm.item(),
            )
            self.register_metric(
                module_id,
                optimizer_name.upper() + "_gradients_maxabs_after_clipping",
                torch.max(torch.abs(
                    torch.cat([g.flatten() for g in grads_sub_dict.values()]))).item(),
            )

        return module_gradients_dict

    @override(TorchLearner)
    def compute_gradients(
        self,
        loss_per_module,
        gradient_tape,
        **kwargs,
    ):
        # Override of the default gradient computation method.
        # For DreamerV3, we need to compute gradients over the individual loss terms
        # as otherwise, the world model's parameters would have their gradients also
        # be influenced by the actor- and critic loss terms/gradient computations.
        grads = {}
        for component in ["world_model", "actor", "critic"]:
            grads.update(
                gradient_tape.gradient(
                    # Take individual loss term from the registered metrics for
                    # the main module.
                    self._metrics[DEFAULT_POLICY_ID][component.upper() + "_L_total"],
                    self.filter_param_dict_for_optimizer(
                        self._params, self.get_optimizer(optimizer_name=component)
                    ),
                )
            )
        del gradient_tape
        return grads

    @override(TorchLearner)
    def compute_loss_for_module(
        self,
        module_id: ModuleID,
        hps: DreamerV3LearnerHyperparameters,
        batch: SampleBatch,
        fwd_out: Mapping[str, TensorType],
    ) -> TensorType:

        # World model losses.
        prediction_losses = self._compute_world_model_prediction_losses(
            hps=hps,
            rewards_B_T=batch[SampleBatch.REWARDS],
            continues_B_T=(1.0 - batch["is_terminated"]),
            fwd_out=fwd_out,
        )

        (
            L_dyn_B_T,
            L_rep_B_T,
        ) = self._compute_world_model_dynamics_and_representation_loss(
            hps=hps, fwd_out=fwd_out
        )
        L_dyn = torch.mean(L_dyn_B_T)
        L_rep = torch.mean(L_rep_B_T)
        # Make sure values for L_rep and L_dyn are the same (they only differ in their
        # gradients).
        assert torch.allclose(L_dyn, L_rep)

        # Compute the actual total loss using fixed weights described in [1] eq. 4.
        L_world_model_total_B_T = (
            1.0 * prediction_losses["L_prediction_B_T"]
            + 0.5 * L_dyn_B_T
            + 0.1 * L_rep_B_T
        )

        # In the paper, it says to sum up timesteps, and average over
        # batch (see eq. 4 in [1]). But Danijar's implementation only does
        # averaging (over B and T), so we'll do this here as well. This is generally
        # true for all other loss terms as well (we'll always just average, no summing
        # over T axis!).
        L_world_model_total = torch.mean(L_world_model_total_B_T)

        # Register world model loss stats.
        self.register_metrics(
            module_id=module_id,
            metrics_dict={
                "WORLD_MODEL_learned_initial_h": self.module[
                    module_id].world_model.initial_h,
                # Prediction losses.
                # Decoder (obs) loss.
                "WORLD_MODEL_L_decoder": prediction_losses["L_decoder"],
                # Reward loss.
                "WORLD_MODEL_L_reward": prediction_losses["L_reward"],
                # Continue loss.
                "WORLD_MODEL_L_continue": prediction_losses["L_continue"],
                # Total.
                "WORLD_MODEL_L_prediction": prediction_losses["L_prediction"],
                # Dynamics loss.
                "WORLD_MODEL_L_dynamics": L_dyn,
                # Representation loss.
                "WORLD_MODEL_L_representation": L_rep,
                # Total loss.
                "WORLD_MODEL_L_total": L_world_model_total,
            },
        )
        if hps.report_individual_batch_item_stats:
            self.register_metrics(
                module_id=module_id,
                metrics_dict={
                    "WORLD_MODEL_L_decoder_B_T": prediction_losses["L_decoder_B_T"],
                    "WORLD_MODEL_L_reward_B_T": prediction_losses["L_reward_B_T"],
                    "WORLD_MODEL_L_continue_B_T": prediction_losses["L_continue_B_T"],
                    "WORLD_MODEL_L_prediction_B_T": prediction_losses[
                        "L_prediction_B_T"],
                    "WORLD_MODEL_L_dynamics_B_T": L_dyn_B_T,
                    "WORLD_MODEL_L_representation_B_T": L_rep_B_T,
                    "WORLD_MODEL_L_total_B_T": L_world_model_total_B_T,
                },
            )

        # Dream trajectories starting in all internal states (h + z_posterior) that were
        # computed during world model training.
        # Everything goes in as BxT: We are starting a new dream trajectory at every
        # actually encountered timestep in the batch, so we are creating B*T
        # trajectories of len `horizon_H`.
        dream_data = self.module[module_id].dreamer_model.dream_trajectory(
            start_states={
                "h": fwd_out["h_states_BxT"],
                "z": fwd_out["z_posterior_states_BxT"],
            },
            start_is_terminated=batch["is_terminated"].reshape(-1),  # -> BxT
            timesteps_H=hps.horizon_H,
            gamma=hps.gamma,
        )
        if hps.report_dream_data:
            # To reduce this massive amount of data a little, slice out a T=1 piece
            # from each stats that has the shape (H, BxT), meaning convert e.g.
            # `rewards_dreamed_t0_to_H_BxT` into `rewards_dreamed_t0_to_H_Bx1`.
            # This will reduce the amount of data to be transferred and reported
            # by the factor of `batch_length_T`.
            self.register_metrics(
                module_id,
                {
                    # Replace 'T' with '1'.
                    "DREAM_DATA_" + key[:-1] + "1": value[..., hps.batch_size_B]
                    for key, value in dream_data.items()
                    if key.endswith("H_BxT")
                },
            )

        value_targets_t0_to_Hm1_BxT = self._compute_value_targets(
            hps=hps,
            # Learn critic in symlog'd space.
            rewards_t0_to_H_BxT=dream_data["rewards_dreamed_t0_to_H_BxT"],
            intrinsic_rewards_t1_to_H_BxT=(
                dream_data["rewards_intrinsic_t1_to_H_B"] if hps.use_curiosity else None
            ),
            continues_t0_to_H_BxT=dream_data["continues_dreamed_t0_to_H_BxT"],
            value_predictions_t0_to_H_BxT=dream_data["values_dreamed_t0_to_H_BxT"],
        )
        self.register_metric(
            module_id, "VALUE_TARGETS_H_BxT", value_targets_t0_to_Hm1_BxT
        )

        CRITIC_L_total = self._compute_critic_loss(
            module_id=module_id,
            hps=hps,
            dream_data=dream_data,
            value_targets_t0_to_Hm1_BxT=value_targets_t0_to_Hm1_BxT,
        )
        if hps.train_actor:
            ACTOR_L_total = self._compute_actor_loss(
                module_id=module_id,
                hps=hps,
                dream_data=dream_data,
                value_targets_t0_to_Hm1_BxT=value_targets_t0_to_Hm1_BxT,
            )
        else:
            ACTOR_L_total = 0.0

        # Return the total loss as a sum of all individual losses.
        return L_world_model_total + CRITIC_L_total + ACTOR_L_total
