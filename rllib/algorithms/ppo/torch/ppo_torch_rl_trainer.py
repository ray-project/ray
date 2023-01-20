
from ray.rllib.core.rl_trainer.torch.torch_rl_trainer import TorchRLTrainer
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.torch_utils import warn_if_infinite_kl_divergence

class PPOTorchRLTrainer(TorchRLTrainer):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # TODO (Kourosh): Move these failures to config.validate() or support them.
        if self.config.entropy_coeff_schedule: 
            raise ValueError("entropy_coeff_schedule is not supported in RLTrainer yet")
        
        if self.config.lr_schedule:
            raise ValueError("lr_schedule is not supported in RLTrainer yet")
        
        # TODO (Kourosh): We can still use mix-ins in the new design. Do we want that? 
        # Most likely not.
        self.kl_coeff = self.config.kl_coeff
        self.kl_target = self.config.kl_target

    def compute_loss(self, *, fwd_out: MultiAgentBatch, batch: MultiAgentBatch) -> Union[TensorType, Mapping[str, Any]]:
        
        # TODO (Kourosh): come back to RNNs later
        # TODO (Kourosh): This is boiler plate code. Can we minimize this? 
        """
        loss_dict = {}
        loss_total = None
        for module_id in fwd_out:

            loss = ...

            
            if loss_total is None:
                loss_total = loss
            else:
                loss_total += loss

        
        loss_dict[self.TOTAL_LOSS_KEY] = loss_total
        """

        loss_dict = {}
        loss_total = None
        for module_id in fwd_out:
            module_batch = batch[module_id]
            module_fwd_out = fwd_out[module_id]

            curr_action_dist = fwd_out[SampleBatch.ACTION_DIST]
            action_dist_class = type(module_fwd_out[SampleBatch.ACTION_DIST])
            prev_action_dist = action_dist_class(
                **module_batch[SampleBatch.ACTION_DIST_INPUTS]
            )

            logp_ratio = torch.exp(
            module_fwd_out[SampleBatch.ACTION_LOGP] - module_batch[SampleBatch.ACTION_LOGP]
            )

            # Only calculate kl loss if necessary (kl-coeff > 0.0).
            if self.config.kl_coeff > 0.0:
                action_kl = prev_action_dist.kl(curr_action_dist)
                mean_kl_loss = torch.mean(action_kl)
                # TODO smorad: should we do anything besides warn? Could discard KL term
                # for this update
                warn_if_infinite_kl_divergence(self, mean_kl_loss)
            else:
                mean_kl_loss = torch.tensor(0.0, device=logp_ratio.device)

            curr_entropy = module_fwd_out["entropy"]
            mean_entropy = torch.mean(curr_entropy)

            surrogate_loss = torch.min(
                module_batch[Postprocessing.ADVANTAGES] * logp_ratio,
                module_batch[Postprocessing.ADVANTAGES]
                * torch.clamp(
                    logp_ratio, 1 - self.config.clip_param, 1 + self.config.clip_param
                ),
            )


            # Compute a value function loss.
            if self.config.use_critic:
                value_fn_out = module_fwd_out[SampleBatch.VF_PREDS]
                vf_loss = torch.pow(
                    value_fn_out - module_batch[Postprocessing.VALUE_TARGETS], 2.0
                )
                vf_loss_clipped = torch.clamp(vf_loss, 0, self.config.vf_clip_param)
                mean_vf_loss = torch.mean(vf_loss_clipped)
            # Ignore the value function.
            else:
                value_fn_out = torch.tensor(0.0).to(surrogate_loss.device)
                vf_loss_clipped = mean_vf_loss = torch.tensor(0.0).to(surrogate_loss.device)


            total_loss = torch.mean(
                -surrogate_loss
                + self.config.vf_loss_coeff * vf_loss_clipped
                - self.config.entropy_coeff * curr_entropy
            )
            

            # Add mean_kl_loss (already processed through `reduce_mean_valid`),
            # if necessary.
            if self.config.kl_coeff > 0.0:
                total_loss += self.config. * mean_kl_loss

            loss = ...

            
            if loss_total is None:
                loss_total = loss
            else:
                loss_total += loss

        
        loss_dict[self.TOTAL_LOSS_KEY] = loss_total

        return loss_dict


    def additional_update(self, *, sampled_kl_values) -> Mapping[str, Any]:
        
        results = {}
        for module_id in sampled_kl_values:
            sampled_kl = sampled_kl_values[module_id]
            if sampled_kl > 2.0 * self.kl_target:
                # TODO (Kourosh) why not 2? 
                self.kl_coeff *= 1.5
            elif sampled_kl < 0.5 * self.kl_target:
                self.kl_coeff *= 0.5
            
            results[module_id] = {"kl_coeff": self.kl_coeff}

        return results
        
