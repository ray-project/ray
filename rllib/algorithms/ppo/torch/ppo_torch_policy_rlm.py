import logging
from typing import Dict, List, Type, Union

import ray
from ray.rllib.algorithms.ppo.ppo_tf_policy import validate_config
from ray.rllib.evaluation.postprocessing import (
    Postprocessing,
    compute_gae_for_sample_batch,
)
from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.torch_mixins import (
    EntropyCoeffSchedule,
    KLCoeffMixin,
    LearningRateSchedule,
)
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.torch_policy_v2 import TorchPolicyV2
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.torch_utils import (
    apply_grad_clipping,
    explained_variance,
    sequence_mask,
    warn_if_infinite_kl_divergence,
)
from ray.rllib.utils.typing import TensorType
from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import (
    PPOTorchRLModule,
    PPOModuleConfig,
    FCConfig,
)

torch, nn = try_import_torch()

logger = logging.getLogger(__name__)


class PPOTorchPolicyWithRLModule(
    LearningRateSchedule,
    EntropyCoeffSchedule,
    KLCoeffMixin,
    TorchPolicyV2,
):
    """PyTorch policy class used with PPO.

    This class is copied from PPOTorchPolicyV2 and is modified to support RLModules.
    Some subtle differences:
    - if config._enable_rl_module api is true make_rl_module should be implemented by
    the policy the policy is assumed to be compatible with rl_modules (i.e. self.model
    would be an RLModule)
    - Tower stats no longer belongs to the model (i.e. RLModule) instead it belongs to
    the policy itself.
    - Connectors should be enabled to use this policy
    - So far it only works for vectorized obs and action spaces (Fully connected neural
    networks). we need model catalog to work for other obs and action spaces.

    # TODO: In the future we will deprecate doing all phases of training, exploration,
    # and inference via one policy abstraction. Instead, we will use separate
    # abstractions for each phase. For training (i.e. gradient updates, given the
    # sample that have been collected) we will use RLTrainer which will own one or
    # possibly many RLModules, and RLOptimizer. For exploration, we will use RLSampler
    # which will own RLModule, and RLTrajectoryProcessor. The exploration and inference
    # phase details are TBD but the whole point is to make rllib extremely modular.
    """

    def __init__(self, observation_space, action_space, config):
        config = dict(ray.rllib.algorithms.ppo.ppo.PPOConfig().to_dict(), **config)
        # TODO: Move into Policy API, if needed at all here. Why not move this into
        #  `PPOConfig`?.
        validate_config(config)

        TorchPolicyV2.__init__(
            self,
            observation_space,
            action_space,
            config,
            max_seq_len=config["model"]["max_seq_len"],
        )

        LearningRateSchedule.__init__(self, config["lr"], config["lr_schedule"])
        EntropyCoeffSchedule.__init__(
            self, config["entropy_coeff"], config["entropy_coeff_schedule"]
        )
        KLCoeffMixin.__init__(self, config)

        # TODO: Don't require users to call this manually.
        self._initialize_loss_from_dummy_batch()

    @override(Policy)
    def make_rl_module(self):
        """Returns the RLModule to use for this policy.

        This implementation will be replaced with model catalog calls in the future.
        For now we basically create the barebones of a fully connected network to match
        the behavior of what is used to be in the old policy. This is a temporary
        solution to get RLModules working with PPO.

        """

        activation = self.config["model"]["fcnet_activation"]
        if activation == "tanh":
            activation = "Tanh"
        elif activation == "relu":
            activation = "ReLU"
        elif activation == "linear":
            activation = "linear"
        else:
            raise ValueError(f"Unsupported activation: {activation}")

        fcnet_hiddens = self.config["model"]["fcnet_hiddens"]
        vf_share_layers = self.config["model"]["vf_share_layers"]
        free_log_std = self.config["model"]["free_log_std"]

        if vf_share_layers:
            encoder_config = FCConfig(
                hidden_layers=fcnet_hiddens,
                activation=activation,
            )
            # TODO
            pi_config = FCConfig()
            vf_config = FCConfig()
        else:
            pi_config = FCConfig(
                hidden_layers=fcnet_hiddens,
                activation=activation,
            )
            vf_config = FCConfig(
                hidden_layers=fcnet_hiddens,
                activation=activation,
            )
            encoder_config = None

        config_ = PPOModuleConfig(
            observation_space=self.observation_space,
            action_space=self.action_space,
            encoder_config=encoder_config,
            pi_config=pi_config,
            vf_config=vf_config,
            free_log_std=free_log_std,
        )

        return PPOTorchRLModule(config_)

    @override(TorchPolicyV2)
    def loss(
        self,
        model: ModelV2,
        dist_class: Type[ActionDistribution],
        train_batch: SampleBatch,
    ) -> Union[TensorType, List[TensorType]]:
        """Compute loss for Proximal Policy Objective.

        Args:
            model: The Model to calculate the loss for.
            dist_class: The action distr. class.
            train_batch: The training data.

        Returns:
            The PPO loss tensor given the input batch.
        """

        fwd_out = model.forward_train(train_batch)
        curr_action_dist = fwd_out[SampleBatch.ACTION_DIST]
        state = fwd_out.get("state_out", {})

        # TODO (Kourosh): come back to RNNs later
        # RNN case: Mask away 0-padded chunks at end of time axis.
        if state:
            B = len(train_batch[SampleBatch.SEQ_LENS])
            max_seq_len = train_batch[SampleBatch.OBS].shape[0] // B
            mask = sequence_mask(
                train_batch[SampleBatch.SEQ_LENS],
                max_seq_len,
                time_major=model.is_time_major(),
            )
            mask = torch.reshape(mask, [-1])
            num_valid = torch.sum(mask)

            def reduce_mean_valid(t):
                return torch.sum(t[mask]) / num_valid

        # non-RNN case: No masking.
        else:
            mask = None
            reduce_mean_valid = torch.mean

        action_dist_class = type(fwd_out[SampleBatch.ACTION_DIST])
        prev_action_dist = action_dist_class(
            **train_batch[SampleBatch.ACTION_DIST_INPUTS]
        )

        logp_ratio = torch.exp(
            fwd_out[SampleBatch.ACTION_LOGP] - train_batch[SampleBatch.ACTION_LOGP]
        )

        # Only calculate kl loss if necessary (kl-coeff > 0.0).
        if self.config["kl_coeff"] > 0.0:
            action_kl = prev_action_dist.kl(curr_action_dist)
            mean_kl_loss = reduce_mean_valid(action_kl)
            # TODO smorad: should we do anything besides warn? Could discard KL term
            # for this update
            warn_if_infinite_kl_divergence(self, mean_kl_loss)
        else:
            mean_kl_loss = torch.tensor(0.0, device=logp_ratio.device)

        curr_entropy = fwd_out["entropy"]
        mean_entropy = reduce_mean_valid(curr_entropy)

        surrogate_loss = torch.min(
            train_batch[Postprocessing.ADVANTAGES] * logp_ratio,
            train_batch[Postprocessing.ADVANTAGES]
            * torch.clamp(
                logp_ratio, 1 - self.config["clip_param"], 1 + self.config["clip_param"]
            ),
        )

        # Compute a value function loss.
        if self.config["use_critic"]:
            value_fn_out = fwd_out[SampleBatch.VF_PREDS]
            vf_loss = torch.pow(
                value_fn_out - train_batch[Postprocessing.VALUE_TARGETS], 2.0
            )
            vf_loss_clipped = torch.clamp(vf_loss, 0, self.config["vf_clip_param"])
            mean_vf_loss = reduce_mean_valid(vf_loss_clipped)
        # Ignore the value function.
        else:
            value_fn_out = torch.tensor(0.0).to(surrogate_loss.device)
            vf_loss_clipped = mean_vf_loss = torch.tensor(0.0).to(surrogate_loss.device)

        total_loss = reduce_mean_valid(
            -surrogate_loss
            + self.config["vf_loss_coeff"] * vf_loss_clipped
            - self.entropy_coeff * curr_entropy
        )

        # Add mean_kl_loss (already processed through `reduce_mean_valid`),
        # if necessary.
        if self.config["kl_coeff"] > 0.0:
            total_loss += self.kl_coeff * mean_kl_loss

        # TODO (Kourosh) Where would tower_stats go? How should stats_fn be implemented
        # here?
        # Store values for stats function in model (tower), such that for
        # multi-GPU, we do not override them during the parallel loss phase.
        self.tower_stats[model]["total_loss"] = total_loss
        self.tower_stats[model]["mean_policy_loss"] = reduce_mean_valid(-surrogate_loss)
        self.tower_stats[model]["mean_vf_loss"] = mean_vf_loss
        self.tower_stats[model]["vf_explained_var"] = explained_variance(
            train_batch[Postprocessing.VALUE_TARGETS], value_fn_out
        )
        self.tower_stats[model]["mean_entropy"] = mean_entropy
        self.tower_stats[model]["mean_kl_loss"] = mean_kl_loss

        return total_loss

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
