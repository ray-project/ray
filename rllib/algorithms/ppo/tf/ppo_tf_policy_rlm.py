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
tf1.enable_eager_execution()

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
        model: ModelV2,
        dist_class: Type[ActionDistribution],
        train_batch: SampleBatch,
    ) -> Union[TensorType, List[TensorType]]:
        fwd_out_module = model.forward_train(train_batch)

        curr_probs = fwd_out_module[SampleBatch.ACTION_LOGP]
        old_probs = train_batch[SampleBatch.ACTION_LOGP]
        likelihood_ratio = tf.math.exp(curr_probs - old_probs)
        clipped_likelihood = tf.clip_by_value(
            likelihood_ratio,
            1 - self.config["clip_param"],
            1 + self.config["clip_param"],
        )

        advantages = train_batch[Postprocessing.ADVANTAGES]
        ppo_clipped_objective = tf.math.minimum(
            likelihood_ratio * advantages, clipped_likelihood * advantages
        )

        entropy_objective = tf.math.reduce_mean(
            self.entropy_coeff * fwd_out_module["entropy"]
        )

        vf_loss = tf.math.square(
            fwd_out_module[SampleBatch.VF_PREDS]
            - train_batch[Postprocessing.VALUE_TARGETS]
        )

        prev_action_dist_cls = fwd_out_module[SampleBatch.ACTION_DIST].__class__
        prev_action_dist_inputs = fwd_out_module[SampleBatch.ACTION_DIST_INPUTS]
        prev_action_dist = prev_action_dist_cls(prev_action_dist_inputs, model=None)
        current_prob_dist = fwd_out_module[SampleBatch.ACTION_DIST]
        kl = prev_action_dist.kl(current_prob_dist)
        mean_kl = tf.math.reduce_mean(kl)
        warn_if_infinite_kl_divergence(self, mean_kl)

        loss = -(ppo_clipped_objective + entropy_objective - vf_loss)
        loss = tf.math.reduce_mean(loss)
        self.stats["total_loss"] = loss
        self.stats["mean_policy_loss"] = tf.math.reduce_mean(ppo_clipped_objective)
        self.stats["mean_vf_loss"] = tf.math.reduce_mean(vf_loss)
        self.stats["vf_explained_var"] = explained_variance(
            train_batch[Postprocessing.VALUE_TARGETS],
            fwd_out_module[SampleBatch.VF_PREDS],
        )
        self.stats["mean_entropy"] = tf.math.reduce_mean(fwd_out_module["entropy"])
        self.stats["kl"] = mean_kl

        return loss

    @override(EagerTFPolicyV2)
    def postprocess_trajectory(
        self, sample_batch, other_agent_batches=None, episode=None
    ):
        sample_batch = super().postprocess_trajectory(sample_batch)
        return compute_gae_for_sample_batch(
            self, sample_batch, other_agent_batches, episode
        )

    @override(EagerTFPolicyV2)
    def stats_fn(self, train_batch: SampleBatch) -> Dict[str, TensorType]:
        return {
            "mean_entropy": self.stats["mean_entropy"],
            "vf_explained_var": self.stats["vf_explained_var"],
            "cur_lr": tf.cast(self.cur_lr, tf.float64),
            "entropy_coeff": tf.cast(self.entropy_coeff, tf.float64),
            "total_loss": self.stats["total_loss"],
            "vf_loss": self.stats["mean_vf_loss"],
            "policy_loss": self.stats["mean_policy_loss"],
            "kl": self.stats["kl"],
        }
