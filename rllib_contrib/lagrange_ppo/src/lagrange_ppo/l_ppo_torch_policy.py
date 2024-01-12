import logging
from typing import Dict, List, Type, Union

import lagrange_ppo
from lagrange_ppo.cost_postprocessing import compute_gae_for_sample_batch
from lagrange_ppo.utils import CostAndValueNetworkMixins, validate_config

from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.torch_mixins import (
    EntropyCoeffSchedule,
    KLCoeffMixin,
    LearningRateSchedule,
)
from ray.rllib.policy.torch_policy_v2 import TorchPolicyV2
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.torch_utils import apply_grad_clipping
from ray.rllib.utils.typing import TensorType

torch, nn = try_import_torch()

logger = logging.getLogger(__name__)


class PPOLagrangeTorchPolicy(
    CostAndValueNetworkMixins,
    LearningRateSchedule,
    EntropyCoeffSchedule,
    KLCoeffMixin,
    TorchPolicyV2,
):
    def __init__(self, observation_space, action_space, config):
        config = dict(lagrange_ppo.PPOLagrangeConfig().to_dict(), **config)
        validate_config(config)

        TorchPolicyV2.__init__(
            self,
            observation_space,
            action_space,
            config,
            max_seq_len=config["model"]["max_seq_len"],
        )

        CostAndValueNetworkMixins.__init__(self, config)
        LearningRateSchedule.__init__(self, config["lr"], config["lr_schedule"])
        EntropyCoeffSchedule.__init__(
            self, config["entropy_coeff"], config["entropy_coeff_schedule"]
        )
        KLCoeffMixin.__init__(self, config)

        self._initialize_loss_from_dummy_batch()

    @override(TorchPolicyV2)
    def loss(
        self,
        model: ModelV2,
        dist_class: Type[ActionDistribution],
        train_batch: SampleBatch,
    ) -> Union[TensorType, List[TensorType]]:
        """Compute loss for Proximal Policy Objective with a constraint.

        Args:
            model: The Model to calculate the loss for.
            dist_class: The action distr. class.
            train_batch: The training data.

        Returns:
            The PPO Lagrangian loss tensor given the input batch.
        """
        raise NotImplementedError

    # TODO: Make this an event-style subscription (e.g.:
    #  "after_gradients_computed").
    @override(TorchPolicyV2)
    def extra_grad_process(self, local_optimizer, loss):
        return apply_grad_clipping(self, local_optimizer, loss)

    @override(TorchPolicyV2)
    def stats_fn(self, train_batch: SampleBatch) -> Dict[str, TensorType]:
        return convert_to_numpy(
            {
                "cur_kl_coeff": self.kl_coeff,
                "cur_lr": self.cur_lr,
                "total_loss": torch.mean(
                    torch.stack(self.get_tower_stats("total_loss"))
                ),
                "policy_loss": torch.mean(
                    torch.stack(self.get_tower_stats("mean_policy_loss"))
                ),
                "vf_loss": torch.mean(
                    torch.stack(self.get_tower_stats("mean_vf_loss"))
                ),
                "vf_explained_var": torch.mean(
                    torch.stack(self.get_tower_stats("vf_explained_var"))
                ),
                "cvf_loss": torch.mean(
                    torch.stack(self.get_tower_stats("mean_cvf_loss"))
                ),
                "cvf_explained_var": torch.mean(
                    torch.stack(self.get_tower_stats("cvf_explained_var"))
                ),
                "kl": torch.mean(torch.stack(self.get_tower_stats("mean_kl_loss"))),
                "entropy": torch.mean(
                    torch.stack(self.get_tower_stats("mean_entropy"))
                ),
                "entropy_coeff": self.entropy_coeff,
            }
        )

    @override(TorchPolicyV2)
    def postprocess_trajectory(
        self, sample_batch, other_agent_batches=None, episode=None
    ):
        # Do all post-processing always with no_grad().
        # Not using this here will introduce a memory leak
        # in torch (issue #6962).
        # TODO: no_grad still necessary?

        with torch.no_grad():
            return compute_gae_for_sample_batch(
                self, sample_batch, other_agent_batches, episode
            )
