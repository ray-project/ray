from typing import Any, Dict, Mapping

from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.algorithms.appo.appo_learner import (
    AppoLearner,
    AppoLearnerHyperparameters,
    LEARNER_RESULTS_CURR_KL_COEFF_KEY,
    LEARNER_RESULTS_KL_KEY,
    OLD_ACTION_DIST_LOGITS_KEY,
)
from ray.rllib.algorithms.impala.torch.vtrace_torch_v2 import (
    make_time_major,
    vtrace_torch,
)
from ray.rllib.core.learner.learner import POLICY_LOSS_KEY, VF_LOSS_KEY, ENTROPY_KEY
from ray.rllib.core.learner.torch.torch_learner import TorchLearner
from ray.rllib.core.rl_module.marl_module import ModuleID, MultiAgentRLModule
from ray.rllib.core.rl_module.torch.torch_rl_module import (
    TorchDDPRLModuleWithTargetNetworksInterface,
    TorchRLModule,
)
from ray.rllib.core.rl_module.rl_module_with_target_networks_interface import (
    RLModuleWithTargetNetworksInterface,
)
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.utils.typing import TensorType

torch, nn = try_import_torch()


class APPOTorchLearner(AppoLearner, TorchLearner):
    """Implements APPO loss / update logic on top of ImpalaTorchLearner."""

    @override(TorchLearner)
    def compute_loss_for_module(
        self,
        *,
        module_id: ModuleID,
        hps: AppoLearnerHyperparameters,
        batch: NestedDict,
        fwd_out: Mapping[str, TensorType],
    ) -> TensorType:

        values = fwd_out[SampleBatch.VF_PREDS]
        action_dist_cls_train = (
            self.module[module_id].unwrapped().get_train_action_dist_cls()
        )
        target_policy_dist = action_dist_cls_train.from_logits(
            fwd_out[SampleBatch.ACTION_DIST_INPUTS]
        )
        old_target_policy_dist = action_dist_cls_train.from_logits(
            fwd_out[OLD_ACTION_DIST_LOGITS_KEY]
        )
        old_target_policy_actions_logp = old_target_policy_dist.logp(
            batch[SampleBatch.ACTIONS]
        )
        behaviour_actions_logp = batch[SampleBatch.ACTION_LOGP]
        target_actions_logp = target_policy_dist.logp(batch[SampleBatch.ACTIONS])

        behaviour_actions_logp_time_major = make_time_major(
            behaviour_actions_logp,
            trajectory_len=hps.rollout_frag_or_episode_len,
            recurrent_seq_len=hps.recurrent_seq_len,
        )
        target_actions_logp_time_major = make_time_major(
            target_actions_logp,
            trajectory_len=hps.rollout_frag_or_episode_len,
            recurrent_seq_len=hps.recurrent_seq_len,
        )
        old_actions_logp_time_major = make_time_major(
            old_target_policy_actions_logp,
            trajectory_len=hps.rollout_frag_or_episode_len,
            recurrent_seq_len=hps.recurrent_seq_len,
        )
        rewards_time_major = make_time_major(
            batch[SampleBatch.REWARDS],
            trajectory_len=hps.rollout_frag_or_episode_len,
            recurrent_seq_len=hps.recurrent_seq_len,
        )
        values_time_major = make_time_major(
            values,
            trajectory_len=hps.rollout_frag_or_episode_len,
            recurrent_seq_len=hps.recurrent_seq_len,
        )
        bootstrap_values_time_major = make_time_major(
            batch[SampleBatch.VALUES_BOOTSTRAPPED],
            trajectory_len=hps.rollout_frag_or_episode_len,
            recurrent_seq_len=hps.recurrent_seq_len,
        )
        bootstrap_value = bootstrap_values_time_major[-1]

        # The discount factor that is used should be gamma except for timesteps where
        # the episode is terminated. In that case, the discount factor should be 0.
        discounts_time_major = (
            1.0
            - make_time_major(
                batch[SampleBatch.TERMINATEDS],
                trajectory_len=hps.rollout_frag_or_episode_len,
                recurrent_seq_len=hps.recurrent_seq_len,
            ).float()
        ) * hps.discount_factor

        # Note that vtrace will compute the main loop on the CPU for better performance.
        vtrace_adjusted_target_values, pg_advantages = vtrace_torch(
            target_action_log_probs=old_actions_logp_time_major,
            behaviour_action_log_probs=behaviour_actions_logp_time_major,
            discounts=discounts_time_major,
            rewards=rewards_time_major,
            values=values_time_major,
            bootstrap_value=bootstrap_value,
            clip_pg_rho_threshold=hps.vtrace_clip_pg_rho_threshold,
            clip_rho_threshold=hps.vtrace_clip_rho_threshold,
        )

        # The policy gradients loss.
        is_ratio = torch.clip(
            torch.exp(behaviour_actions_logp_time_major - old_actions_logp_time_major),
            0.0,
            2.0,
        )
        logp_ratio = is_ratio * torch.exp(
            target_actions_logp_time_major - behaviour_actions_logp_time_major
        )

        surrogate_loss = torch.minimum(
            pg_advantages * logp_ratio,
            pg_advantages
            * torch.clip(logp_ratio, 1 - hps.clip_param, 1 + hps.clip_param),
        )

        if hps.use_kl_loss:
            action_kl = old_target_policy_dist.kl(target_policy_dist)
            mean_kl_loss = torch.mean(action_kl)
        else:
            mean_kl_loss = 0.0
        mean_pi_loss = -torch.mean(surrogate_loss)

        # The baseline loss.
        delta = values_time_major - vtrace_adjusted_target_values
        mean_vf_loss = 0.5 * torch.mean(delta**2)

        # The entropy loss.
        mean_entropy_loss = -torch.mean(target_policy_dist.entropy())

        # The summed weighted loss.
        total_loss = (
            mean_pi_loss
            + (mean_vf_loss * hps.vf_loss_coeff)
            + (
                mean_entropy_loss
                * self.entropy_coeff_schedulers_per_module[
                    module_id
                ].get_current_value()
            )
            + (mean_kl_loss * self.curr_kl_coeffs_per_module[module_id])
        )

        # Register important loss stats.
        self.register_metrics(
            module_id,
            {
                POLICY_LOSS_KEY: mean_pi_loss,
                VF_LOSS_KEY: mean_vf_loss,
                ENTROPY_KEY: -mean_entropy_loss,
                LEARNER_RESULTS_KL_KEY: mean_kl_loss,
                LEARNER_RESULTS_CURR_KL_COEFF_KEY: (
                    self.curr_kl_coeffs_per_module[module_id]
                ),
            },
        )
        # Return the total loss.
        return total_loss

    @override(TorchLearner)
    def _make_modules_ddp_if_necessary(self) -> None:
        """Logic for (maybe) making all Modules within self._module DDP.

        This implementation differs from the super's default one in using the special
        TorchDDPRLModuleWithTargetNetworksInterface wrapper, instead of the default
        TorchDDPRLModule one.
        """

        # If the module is a MultiAgentRLModule and nn.Module we can simply assume
        # all the submodules are registered. Otherwise, we need to loop through
        # each submodule and move it to the correct device.
        # TODO (Kourosh): This can result in missing modules if the user does not
        #  register them in the MultiAgentRLModule. We should find a better way to
        #  handle this.
        if self._distributed:
            # Single agent module: Convert to
            # `TorchDDPRLModuleWithTargetNetworksInterface`.
            if isinstance(self._module, RLModuleWithTargetNetworksInterface):
                self._module = TorchDDPRLModuleWithTargetNetworksInterface(self._module)
            # Multi agent module: Convert each submodule to
            # `TorchDDPRLModuleWithTargetNetworksInterface`.
            else:
                assert isinstance(self._module, MultiAgentRLModule)
                for key in self._module.keys():
                    sub_module = self._module[key]
                    if isinstance(sub_module, TorchRLModule):
                        # Wrap and override the module ID key in self._module.
                        self._module.add_module(
                            key,
                            TorchDDPRLModuleWithTargetNetworksInterface(sub_module),
                            override=True,
                        )

    @override(AppoLearner)
    def _update_module_target_networks(
        self, module_id: ModuleID, hps: AppoLearnerHyperparameters
    ) -> None:
        module = self.module[module_id]

        target_current_network_pairs = module.get_target_network_pairs()
        for target_network, current_network in target_current_network_pairs:
            current_state_dict = current_network.state_dict()
            new_state_dict = {
                k: hps.tau * current_state_dict[k] + (1 - hps.tau) * v
                for k, v in target_network.state_dict().items()
            }
            target_network.load_state_dict(new_state_dict)

    @override(AppoLearner)
    def _update_module_kl_coeff(
        self, module_id: ModuleID, hps: AppoLearnerHyperparameters, sampled_kl: float
    ) -> Dict[str, Any]:
        # Update the current KL value based on the recently measured value.
        # Increase.
        kl_coeff_var = self.curr_kl_coeffs_per_module[module_id]

        if sampled_kl > 2.0 * hps.kl_target:
            # TODO (Kourosh) why not *2.0?
            kl_coeff_var.data *= 1.5
        # Decrease.
        elif sampled_kl < 0.5 * hps.kl_target:
            kl_coeff_var.data *= 0.5

        return {LEARNER_RESULTS_CURR_KL_COEFF_KEY: kl_coeff_var.item()}
