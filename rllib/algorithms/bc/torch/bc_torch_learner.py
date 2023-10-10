import logging
from typing import Mapping

from ray.rllib.core.learner.learner import (
    POLICY_LOSS_KEY,
    LearnerHyperparameters,
)
from ray.rllib.core.learner.torch.torch_learner import TorchLearner
from ray.rllib.core.rl_module.rl_module import ModuleID
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.utils.torch_utils import sequence_mask
from ray.rllib.utils.typing import TensorType

torch, nn = try_import_torch()
logger = logging.getLogger(__file__)


class BCTorchLearner(TorchLearner):
    """Implements torch-specific BC loss logic."""

    @override(TorchLearner)
    def compute_loss_for_module(
        self,
        *,
        module_id: ModuleID,
        hps: LearnerHyperparameters,
        batch: NestedDict,
        fwd_out: Mapping[str, TensorType]
    ) -> TensorType:
        # In the RNN case, we expect incoming tensors to be padded to the maximum
        # sequence length. We infer the max sequence length from the actions
        # tensor.
        # TODO (sven): Unify format requirement and handling thereof.
        # - If an episode ends in the middle of a row, insert an initial state
        #  instead of zero-padding.
        # - New field in the batch dict "is_first" (shape=(B, T)) indicates at
        #  which positions to insert these initial states.
        # This removes special reduction and only needs torch.mean().
        if self.module[module_id].is_stateful():
            maxlen = torch.max(batch[SampleBatch.SEQ_LENS])
            mask = sequence_mask(batch[SampleBatch.SEQ_LENS], maxlen=maxlen)
            num_valid = torch.sum(mask)

            def possibly_masked_mean(t):
                return torch.sum(t[mask]) / num_valid

        # non-RNN case: use simple mean.
        else:
            mask = None
            possibly_masked_mean = torch.mean

        action_dist_class_train = self.module[module_id].get_train_action_dist_cls()
        action_dist = action_dist_class_train.from_logits(
            fwd_out[SampleBatch.ACTION_DIST_INPUTS]
        )
        log_probs = action_dist.logp(batch[SampleBatch.ACTIONS])

        policy_loss = -possibly_masked_mean(log_probs)

        self.register_metrics(module_id, {POLICY_LOSS_KEY: policy_loss})

        # Return the total loss which is for BC simply the policy loss.
        return policy_loss
