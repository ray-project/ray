import logging
from typing import Dict, List, Union

import ray
from ray.rllib.algorithms.ppo.ppo_tf_policy import validate_config
from ray.rllib.evaluation.postprocessing import (
    Postprocessing,
    compute_gae_for_sample_batch,
)
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.tf_mixins import (
    EntropyCoeffSchedule,
    KLCoeffMixin,
    LearningRateSchedule,
)
from ray.rllib.policy.eager_tf_policy_v2 import EagerTFPolicyV2
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf

from ray.rllib.utils.tf_utils import (
    explained_variance,
    warn_if_infinite_kl_divergence,
)

from ray.rllib.utils.typing import TensorType

tf1, tf, tfv = try_import_tf()

logger = logging.getLogger(__name__)


class PPOTfPolicyWithRLModule(
    LearningRateSchedule,
    EntropyCoeffSchedule,
    KLCoeffMixin,
    EagerTFPolicyV2,
):
    """PyTorch policy class used with PPO.

    This class is copied from PPOTFPolicy and is modified to support RLModules.
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
        EagerTFPolicyV2.enable_eager_execution_if_necessary()
        EagerTFPolicyV2.__init__(self, observation_space, action_space, config)
        # Initialize MixIns.
        LearningRateSchedule.__init__(self, config["lr"], config["lr_schedule"])
        EntropyCoeffSchedule.__init__(
            self, config["entropy_coeff"], config["entropy_coeff_schedule"]
        )
        KLCoeffMixin.__init__(self, config)

        self.maybe_initialize_optimizer_and_loss()

    @override(EagerTFPolicyV2)
    def loss(
        self,
        model: Union[ModelV2, "tf.keras.Model"],
        dist_class,
        train_batch: SampleBatch,
    ) -> Union[TensorType, List[TensorType]]:
        del dist_class
        fwd_out = model.forward_train(train_batch)

        curr_action_dist = fwd_out[SampleBatch.ACTION_DIST]
        dist_class = curr_action_dist.__class__
        value_fn_out = fwd_out[SampleBatch.VF_PREDS]

        reduce_mean_valid = tf.reduce_mean

        prev_action_dist = dist_class(
            train_batch[SampleBatch.ACTION_DIST_INPUTS], model
        )

        logp_ratio = tf.exp(
            curr_action_dist.logp(train_batch[SampleBatch.ACTIONS])
            - train_batch[SampleBatch.ACTION_LOGP]
        )

        # Only calculate kl loss if necessary (kl-coeff > 0.0).
        if self.config["kl_coeff"] > 0.0:
            action_kl = prev_action_dist.kl(curr_action_dist)
            mean_kl_loss = reduce_mean_valid(action_kl)
            warn_if_infinite_kl_divergence(self, mean_kl_loss)
        else:
            mean_kl_loss = tf.constant(0.0)

        curr_entropy = curr_action_dist.entropy()
        mean_entropy = reduce_mean_valid(curr_entropy)

        surrogate_loss = tf.minimum(
            train_batch[Postprocessing.ADVANTAGES] * logp_ratio,
            train_batch[Postprocessing.ADVANTAGES]
            * tf.clip_by_value(
                logp_ratio,
                1 - self.config["clip_param"],
                1 + self.config["clip_param"],
            ),
        )

        # Compute a value function loss.
        if self.config["use_critic"]:
            vf_loss = tf.math.square(
                value_fn_out - train_batch[Postprocessing.VALUE_TARGETS]
            )
            vf_loss_clipped = tf.clip_by_value(
                vf_loss,
                0,
                self.config["vf_clip_param"],
            )
            mean_vf_loss = reduce_mean_valid(vf_loss_clipped)
            mean_vf_unclipped_loss = reduce_mean_valid(vf_loss)
        # Ignore the value function.
        else:
            vf_loss_clipped = mean_vf_loss = tf.constant(0.0)

        total_loss = reduce_mean_valid(
            -surrogate_loss
            + self.config["vf_loss_coeff"] * vf_loss_clipped
            - self.entropy_coeff * curr_entropy
        )
        # Add mean_kl_loss (already processed through `reduce_mean_valid`),
        # if necessary.
        if self.config["kl_coeff"] > 0.0:
            total_loss += self.kl_coeff * mean_kl_loss

        # Store stats in policy for stats_fn.
        self._total_loss = total_loss
        self._mean_policy_loss = reduce_mean_valid(-surrogate_loss)
        self._mean_vf_loss = mean_vf_loss
        self._unclipped_mean_vf_loss = mean_vf_unclipped_loss
        self._mean_entropy = mean_entropy
        # Backward compatibility: Deprecate self._mean_kl.
        self._mean_kl_loss = self._mean_kl = mean_kl_loss
        self._value_fn_out = value_fn_out
        self._value_mean = reduce_mean_valid(value_fn_out)

        return total_loss

    @override(EagerTFPolicyV2)
    def stats_fn(self, train_batch: SampleBatch) -> Dict[str, TensorType]:
        return {
            "cur_kl_coeff": tf.cast(self.kl_coeff, tf.float64),
            "cur_lr": tf.cast(self.cur_lr, tf.float64),
            "total_loss": self._total_loss,
            "policy_loss": self._mean_policy_loss,
            "vf_loss": self._mean_vf_loss,
            "unclipped_vf_loss": self._unclipped_mean_vf_loss,
            "vf_explained_var": explained_variance(
                train_batch[Postprocessing.VALUE_TARGETS], self._value_fn_out
            ),
            "kl": self._mean_kl_loss,
            "entropy": self._mean_entropy,
            "entropy_coeff": tf.cast(self.entropy_coeff, tf.float64),
            "value_mean": tf.cast(self._value_mean, tf.float64),
        }

    @override(EagerTFPolicyV2)
    def postprocess_trajectory(
        self, sample_batch, other_agent_batches=None, episode=None
    ):
        sample_batch = super().postprocess_trajectory(sample_batch)
        return compute_gae_for_sample_batch(
            self, sample_batch, other_agent_batches, episode
        )
