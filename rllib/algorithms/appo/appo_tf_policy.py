"""
TensorFlow policy class used for APPO.

Adapted from VTraceTFPolicy to use the PPO surrogate loss.
Keep in sync with changes to VTraceTFPolicy.
"""

import numpy as np
import logging
import gymnasium as gym
from typing import Dict, List, Optional, Type, Union

from ray.rllib.algorithms.appo.utils import make_appo_models
from ray.rllib.algorithms.impala import vtrace_tf as vtrace
from ray.rllib.algorithms.impala.impala_tf_policy import (
    _make_time_major,
    VTraceClipGradients,
    VTraceOptimizer,
)
from ray.rllib.evaluation.episode import Episode
from ray.rllib.evaluation.postprocessing import (
    compute_gae_for_sample_batch,
    Postprocessing,
)
from ray.rllib.models.tf.tf_action_dist import Categorical
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.dynamic_tf_policy_v2 import DynamicTFPolicyV2
from ray.rllib.policy.eager_tf_policy_v2 import EagerTFPolicyV2
from ray.rllib.policy.tf_mixins import (
    EntropyCoeffSchedule,
    LearningRateSchedule,
    KLCoeffMixin,
    ValueNetworkMixin,
    GradStatsMixin,
    TargetNetworkMixin,
)
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.tf.tf_action_dist import TFActionDistribution
from ray.rllib.utils.annotations import (
    override,
)
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.tf_utils import explained_variance
from ray.rllib.utils.typing import TensorType

tf1, tf, tfv = try_import_tf()

logger = logging.getLogger(__name__)


# TODO (sven): Deprecate once APPO and IMPALA fully on RLModules/Learner APIs.
def get_appo_tf_policy(name: str, base: type) -> type:
    """Construct an APPOTFPolicy inheriting either dynamic or eager base policies.

    Args:
        base: Base class for this policy. DynamicTFPolicyV2 or EagerTFPolicyV2.

    Returns:
        A TF Policy to be used with Impala.
    """

    class APPOTFPolicy(
        VTraceClipGradients,
        VTraceOptimizer,
        LearningRateSchedule,
        KLCoeffMixin,
        EntropyCoeffSchedule,
        ValueNetworkMixin,
        TargetNetworkMixin,
        GradStatsMixin,
        base,
    ):
        def __init__(
            self,
            observation_space,
            action_space,
            config,
            existing_model=None,
            existing_inputs=None,
        ):
            # First thing first, enable eager execution if necessary.
            base.enable_eager_execution_if_necessary()

            # Although this is a no-op, we call __init__ here to make it clear
            # that base.__init__ will use the make_model() call.
            VTraceClipGradients.__init__(self)
            VTraceOptimizer.__init__(self)

            # Initialize base class.
            base.__init__(
                self,
                observation_space,
                action_space,
                config,
                existing_inputs=existing_inputs,
                existing_model=existing_model,
            )

            # TF LearningRateSchedule depends on self.framework, so initialize
            # after base.__init__() is called.
            LearningRateSchedule.__init__(self, config["lr"], config["lr_schedule"])
            EntropyCoeffSchedule.__init__(
                self, config["entropy_coeff"], config["entropy_coeff_schedule"]
            )
            ValueNetworkMixin.__init__(self, config)
            KLCoeffMixin.__init__(self, config)
            GradStatsMixin.__init__(self)

            # Note: this is a bit ugly, but loss and optimizer initialization must
            # happen after all the MixIns are initialized.
            self.maybe_initialize_optimizer_and_loss()

            # Initiate TargetNetwork ops after loss initialization.
            TargetNetworkMixin.__init__(self)

        @override(base)
        def make_model(self) -> ModelV2:
            return make_appo_models(self)

        @override(base)
        def loss(
            self,
            model: Union[ModelV2, "tf.keras.Model"],
            dist_class: Type[TFActionDistribution],
            train_batch: SampleBatch,
        ) -> Union[TensorType, List[TensorType]]:
            model_out, _ = model(train_batch)
            action_dist = dist_class(model_out, model)

            if isinstance(self.action_space, gym.spaces.Discrete):
                is_multidiscrete = False
                output_hidden_shape = [self.action_space.n]
            elif isinstance(self.action_space, gym.spaces.multi_discrete.MultiDiscrete):
                is_multidiscrete = True
                output_hidden_shape = self.action_space.nvec.astype(np.int32)
            else:
                is_multidiscrete = False
                output_hidden_shape = 1

            # TODO: (sven) deprecate this when trajectory view API gets activated.
            def make_time_major(*args, **kw):
                return _make_time_major(
                    self, train_batch.get(SampleBatch.SEQ_LENS), *args, **kw
                )

            actions = train_batch[SampleBatch.ACTIONS]
            dones = train_batch[SampleBatch.TERMINATEDS]
            rewards = train_batch[SampleBatch.REWARDS]
            behaviour_logits = train_batch[SampleBatch.ACTION_DIST_INPUTS]

            target_model_out, _ = self.target_model(train_batch)
            prev_action_dist = dist_class(behaviour_logits, self.model)
            values = self.model.value_function()
            values_time_major = make_time_major(values)

            if self.is_recurrent():
                max_seq_len = tf.reduce_max(train_batch[SampleBatch.SEQ_LENS])
                mask = tf.sequence_mask(train_batch[SampleBatch.SEQ_LENS], max_seq_len)
                mask = tf.reshape(mask, [-1])
                mask = make_time_major(mask, drop_last=self.config["vtrace"])

                def reduce_mean_valid(t):
                    return tf.reduce_mean(tf.boolean_mask(t, mask))

            else:
                reduce_mean_valid = tf.reduce_mean

            if self.config["vtrace"]:
                drop_last = self.config["vtrace_drop_last_ts"]
                logger.debug(
                    "Using V-Trace surrogate loss (vtrace=True; "
                    f"drop_last={drop_last})"
                )

                # Prepare actions for loss.
                loss_actions = (
                    actions if is_multidiscrete else tf.expand_dims(actions, axis=1)
                )

                old_policy_behaviour_logits = tf.stop_gradient(target_model_out)
                old_policy_action_dist = dist_class(old_policy_behaviour_logits, model)

                # Prepare KL for Loss
                mean_kl = make_time_major(
                    old_policy_action_dist.multi_kl(action_dist), drop_last=drop_last
                )

                unpacked_behaviour_logits = tf.split(
                    behaviour_logits, output_hidden_shape, axis=1
                )
                unpacked_old_policy_behaviour_logits = tf.split(
                    old_policy_behaviour_logits, output_hidden_shape, axis=1
                )

                # Compute vtrace on the CPU for better perf.
                with tf.device("/cpu:0"):
                    vtrace_returns = vtrace.multi_from_logits(
                        behaviour_policy_logits=make_time_major(
                            unpacked_behaviour_logits, drop_last=drop_last
                        ),
                        target_policy_logits=make_time_major(
                            unpacked_old_policy_behaviour_logits, drop_last=drop_last
                        ),
                        actions=tf.unstack(
                            make_time_major(loss_actions, drop_last=drop_last), axis=2
                        ),
                        discounts=tf.cast(
                            ~make_time_major(
                                tf.cast(dones, tf.bool), drop_last=drop_last
                            ),
                            tf.float32,
                        )
                        * self.config["gamma"],
                        rewards=make_time_major(rewards, drop_last=drop_last),
                        values=values_time_major[:-1]
                        if drop_last
                        else values_time_major,
                        bootstrap_value=values_time_major[-1],
                        dist_class=Categorical if is_multidiscrete else dist_class,
                        model=model,
                        clip_rho_threshold=tf.cast(
                            self.config["vtrace_clip_rho_threshold"], tf.float32
                        ),
                        clip_pg_rho_threshold=tf.cast(
                            self.config["vtrace_clip_pg_rho_threshold"], tf.float32
                        ),
                    )

                actions_logp = make_time_major(
                    action_dist.logp(actions), drop_last=drop_last
                )
                prev_actions_logp = make_time_major(
                    prev_action_dist.logp(actions), drop_last=drop_last
                )
                old_policy_actions_logp = make_time_major(
                    old_policy_action_dist.logp(actions), drop_last=drop_last
                )

                is_ratio = tf.clip_by_value(
                    tf.math.exp(prev_actions_logp - old_policy_actions_logp), 0.0, 2.0
                )
                logp_ratio = is_ratio * tf.exp(actions_logp - prev_actions_logp)
                self._is_ratio = is_ratio

                advantages = vtrace_returns.pg_advantages
                surrogate_loss = tf.minimum(
                    advantages * logp_ratio,
                    advantages
                    * tf.clip_by_value(
                        logp_ratio,
                        1 - self.config["clip_param"],
                        1 + self.config["clip_param"],
                    ),
                )

                action_kl = (
                    tf.reduce_mean(mean_kl, axis=0) if is_multidiscrete else mean_kl
                )
                mean_kl_loss = reduce_mean_valid(action_kl)
                mean_policy_loss = -reduce_mean_valid(surrogate_loss)

                # The value function loss.
                if drop_last:
                    delta = values_time_major[:-1] - vtrace_returns.vs
                else:
                    delta = values_time_major - vtrace_returns.vs
                value_targets = vtrace_returns.vs
                mean_vf_loss = 0.5 * reduce_mean_valid(tf.math.square(delta))

                # The entropy loss.
                actions_entropy = make_time_major(
                    action_dist.multi_entropy(), drop_last=True
                )
                mean_entropy = reduce_mean_valid(actions_entropy)

            else:
                logger.debug("Using PPO surrogate loss (vtrace=False)")

                # Prepare KL for Loss
                mean_kl = make_time_major(prev_action_dist.multi_kl(action_dist))

                logp_ratio = tf.math.exp(
                    make_time_major(action_dist.logp(actions))
                    - make_time_major(prev_action_dist.logp(actions))
                )

                advantages = make_time_major(train_batch[Postprocessing.ADVANTAGES])
                surrogate_loss = tf.minimum(
                    advantages * logp_ratio,
                    advantages
                    * tf.clip_by_value(
                        logp_ratio,
                        1 - self.config["clip_param"],
                        1 + self.config["clip_param"],
                    ),
                )

                action_kl = (
                    tf.reduce_mean(mean_kl, axis=0) if is_multidiscrete else mean_kl
                )
                mean_kl_loss = reduce_mean_valid(action_kl)
                mean_policy_loss = -reduce_mean_valid(surrogate_loss)

                # The value function loss.
                value_targets = make_time_major(
                    train_batch[Postprocessing.VALUE_TARGETS]
                )
                delta = values_time_major - value_targets
                mean_vf_loss = 0.5 * reduce_mean_valid(tf.math.square(delta))

                # The entropy loss.
                mean_entropy = reduce_mean_valid(
                    make_time_major(action_dist.multi_entropy())
                )

            # The summed weighted loss.
            total_loss = mean_policy_loss - mean_entropy * self.entropy_coeff
            # Optional KL loss.
            if self.config["use_kl_loss"]:
                total_loss += self.kl_coeff * mean_kl_loss
            # Optional vf loss (or in a separate term due to separate
            # optimizers/networks).
            loss_wo_vf = total_loss
            if not self.config["_separate_vf_optimizer"]:
                total_loss += mean_vf_loss * self.config["vf_loss_coeff"]

            # Store stats in policy for stats_fn.
            self._total_loss = total_loss
            self._loss_wo_vf = loss_wo_vf
            self._mean_policy_loss = mean_policy_loss
            # Backward compatibility: Deprecate policy._mean_kl.
            self._mean_kl_loss = self._mean_kl = mean_kl_loss
            self._mean_vf_loss = mean_vf_loss
            self._mean_entropy = mean_entropy
            self._value_targets = value_targets

            # Return one total loss or two losses: vf vs rest (policy + kl).
            if self.config["_separate_vf_optimizer"]:
                return loss_wo_vf, mean_vf_loss
            else:
                return total_loss

        @override(base)
        def stats_fn(self, train_batch: SampleBatch) -> Dict[str, TensorType]:
            values_batched = _make_time_major(
                self,
                train_batch.get(SampleBatch.SEQ_LENS),
                self.model.value_function(),
                drop_last=self.config["vtrace"] and self.config["vtrace_drop_last_ts"],
            )

            stats_dict = {
                "cur_lr": tf.cast(self.cur_lr, tf.float64),
                "total_loss": self._total_loss,
                "policy_loss": self._mean_policy_loss,
                "entropy": self._mean_entropy,
                "var_gnorm": tf.linalg.global_norm(self.model.trainable_variables()),
                "vf_loss": self._mean_vf_loss,
                "vf_explained_var": explained_variance(
                    tf.reshape(self._value_targets, [-1]),
                    tf.reshape(values_batched, [-1]),
                ),
                "entropy_coeff": tf.cast(self.entropy_coeff, tf.float64),
            }

            if self.config["vtrace"]:
                is_stat_mean, is_stat_var = tf.nn.moments(self._is_ratio, [0, 1])
                stats_dict["mean_IS"] = is_stat_mean
                stats_dict["var_IS"] = is_stat_var

            if self.config["use_kl_loss"]:
                stats_dict["kl"] = self._mean_kl_loss
                stats_dict["KL_Coeff"] = self.kl_coeff

            return stats_dict

        @override(base)
        def postprocess_trajectory(
            self,
            sample_batch: SampleBatch,
            other_agent_batches: Optional[SampleBatch] = None,
            episode: Optional["Episode"] = None,
        ):
            if not self.config["vtrace"]:
                sample_batch = compute_gae_for_sample_batch(
                    self, sample_batch, other_agent_batches, episode
                )

            return sample_batch

        @override(base)
        def extra_action_out_fn(self) -> Dict[str, TensorType]:
            extra_action_fetches = super().extra_action_out_fn()
            if not self.config["vtrace"]:
                extra_action_fetches[SampleBatch.VF_PREDS] = self.model.value_function()
            return extra_action_fetches

        @override(base)
        def get_batch_divisibility_req(self) -> int:
            return self.config["rollout_fragment_length"]

    APPOTFPolicy.__name__ = name
    APPOTFPolicy.__qualname__ = name

    return APPOTFPolicy


APPOTF1Policy = get_appo_tf_policy("APPOTF1Policy", DynamicTFPolicyV2)
APPOTF2Policy = get_appo_tf_policy("APPOTF2Policy", EagerTFPolicyV2)
