import torch
from typing import Any, Mapping

from ray.rllib.core.rl_trainer.torch.torch_rl_trainer import TorchRLTrainer
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.sample_batch import MultiAgentBatch


class BCTorchRLTrainer(TorchRLTrainer):
    def compute_loss(
        self, fwd_out: MultiAgentBatch, batch: MultiAgentBatch
    ) -> Mapping[str, Any]:

        loss_dict = {}
        loss_total = None
        for module_id in fwd_out:
            action_dist = fwd_out[module_id]["action_dist"]
            loss = -torch.mean(
                action_dist.log_prob(batch[module_id][SampleBatch.ACTIONS])
            )
            loss_dict[module_id] = loss
            if loss_total is None:
                loss_total = loss
            else:
                loss_total += loss

        loss_dict[self.TOTAL_LOSS_KEY] = loss_total

        return loss_dict
