import logging
from typing import Dict

from ray.rllib.algorithms.ppo.ppo_torch_policy import validate_config
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.torch_mixins import (
    EntropyCoeffSchedule,
    LearningRateSchedule,
)
from ray.rllib.policy.torch_policy_v2 import TorchPolicyV2
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.torch_utils import (
    explained_variance,
    global_norm,
)
from ray.rllib.utils.typing import TensorType

torch, nn = try_import_torch()

logger = logging.getLogger(__name__)


class ImpalaTorchPolicyWithRLModule(
    LearningRateSchedule,
    EntropyCoeffSchedule,
    TorchPolicyV2,
):
    def __init__(self, observation_space, action_space, config):
        validate_config(config)
        TorchPolicyV2.__init__(self, observation_space, action_space, config)
        # Initialize MixIns.
        LearningRateSchedule.__init__(self, config["lr"], config["lr_schedule"])
        EntropyCoeffSchedule.__init__(
            self, config["entropy_coeff"], config["entropy_coeff_schedule"]
        )

    @override(TorchPolicyV2)
    def stats_fn(self, train_batch: SampleBatch) -> Dict[str, TensorType]:
        return {
            "cur_lr": self.cur_lr.type(torch.float64),
            "policy_loss": self.stats["pi_loss"],
            "entropy": self.stats["entropy_loss"],
            "entropy_coeff": self.entropy_coeff.type(torch.float64),
            "var_gnorm": global_norm(self.model.trainable_variables),
            "vf_loss": self.stats["vf_loss"],
            "vf_explained_var": explained_variance(
                torch.reshape(self.stats["vtrace_adjusted_target_values"], [-1]),
                torch.reshape(self.stats["values"], [-1]),
            ),
        }

    @override(TorchPolicyV2)
    def get_batch_divisibility_req(self) -> int:
        return self.config["rollout_fragment_length"]
