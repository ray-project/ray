"""Note: Keep in sync with changes to VTraceTFPolicy."""
from typing import Dict, List, Optional, Type, Union

import ray
from ray.rllib.evaluation.episode import Episode
from ray.rllib.evaluation.postprocessing import (
    compute_gae_for_sample_batch,
    Postprocessing,
)
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.tf.tf_action_dist import TFActionDistribution
from ray.rllib.policy.dynamic_tf_policy_v2 import DynamicTFPolicyV2
from ray.rllib.policy.eager_tf_policy_v2 import EagerTFPolicyV2
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.tf_mixins import (
    compute_gradients,
    EntropyCoeffSchedule,
    LearningRateSchedule,
    ValueNetworkMixin,
)
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import Deprecated
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.tf_utils import explained_variance
from ray.rllib.utils.typing import (
    AgentID,
    LocalOptimizer,
    ModelGradients,
    TensorType,
    TFPolicyV2Type,
)

tf1, tf, tfv = try_import_tf()


# We need this builder function because we want to share the same
# custom logics between TF1 dynamic and TF2 eager policies.
def get_a3c_tf_policy(base: TFPolicyV2Type) -> TFPolicyV2Type:
    """Construct a A3CTFPolicy inheriting either dynamic or eager base policies.

    Args:
        base: Base class for this policy. DynamicTFPolicyV2 or EagerTFPolicyV2.

    Returns:
        A TF Policy to be used with MAMLTrainer.
    """

    class A3CTFPolicy(
        ValueNetworkMixin, LearningRateSchedule, EntropyCoeffSchedule, base
    ):
        def __init__(
            self,
            obs_space,
            action_space,
            config,
            existing_model=None,
            existing_inputs=None,
        ):
            # First thing first, enable eager execution if necessary.
            base.enable_eager_execution_if_necessary()

            config = dict(ray.rllib.agents.a3c.a3c.A3CConfig().to_dict(), **config)

            # Initialize base class.
            base.__init__(
                self,
                obs_space,
                action_space,
                config,
                existing_inputs=existing_inputs,
                existing_model=existing_model,
            )

            ValueNetworkMixin.__init__(self, self.config)
            LearningRateSchedule.__init__(
                self, self.config["lr"], self.config["lr_schedule"]
            )
            EntropyCoeffSchedule.__init__(
                self, config["entropy_coeff"], config["entropy_coeff_schedule"]
            )

            # Note: this is a bit ugly, but loss and optimizer initialization must
            # happen after all the MixIns are initialized.
            self.maybe_initialize_optimizer_and_loss()

        @override(base)
        def loss(
            self,
            model: Union[ModelV2, "tf.keras.Model"],
            dist_class: Type[TFActionDistribution],
            train_batch: SampleBatch,
        ) -> Union[TensorType, List[TensorType]]:

            model_out, _ = model(train_batch)
            action_dist = dist_class(model_out, model)
            if self.is_recurrent():
                max_seq_len = tf.reduce_max(train_batch[SampleBatch.SEQ_LENS])
                valid_mask = tf.sequence_mask(
                    train_batch[SampleBatch.SEQ_LENS], max_seq_len
                )
                valid_mask = tf.reshape(valid_mask, [-1])
            else:
                valid_mask = tf.ones_like(train_batch[SampleBatch.REWARDS])

            log_prob = action_dist.logp(train_batch[SampleBatch.ACTIONS])
            vf = model.value_function()

            # The "policy gradients" loss
            self.pi_loss = -tf.reduce_sum(
                tf.boolean_mask(
                    log_prob * train_batch[Postprocessing.ADVANTAGES], valid_mask
                )
            )

            delta = tf.boolean_mask(
                vf - train_batch[Postprocessing.VALUE_TARGETS], valid_mask
            )

            # Compute a value function loss.
            if self.config.get("use_critic", True):
                self.vf_loss = 0.5 * tf.reduce_sum(tf.math.square(delta))
            # Ignore the value function.
            else:
                self.vf_loss = tf.constant(0.0)

            self.entropy_loss = tf.reduce_sum(
                tf.boolean_mask(action_dist.entropy(), valid_mask)
            )

            self.total_loss = (
                self.pi_loss
                + self.vf_loss * self.config["vf_loss_coeff"]
                - self.entropy_loss * self.entropy_coeff
            )

            return self.total_loss

        @override(base)
        def stats_fn(self, train_batch: SampleBatch) -> Dict[str, TensorType]:
            return {
                "cur_lr": tf.cast(self.cur_lr, tf.float64),
                "entropy_coeff": tf.cast(self.entropy_coeff, tf.float64),
                "policy_loss": self.pi_loss,
                "policy_entropy": self.entropy_loss,
                "var_gnorm": tf.linalg.global_norm(
                    list(self.model.trainable_variables())
                ),
                "vf_loss": self.vf_loss,
            }

        @override(base)
        def grad_stats_fn(
            self, train_batch: SampleBatch, grads: ModelGradients
        ) -> Dict[str, TensorType]:
            return {
                "grad_gnorm": tf.linalg.global_norm(grads),
                "vf_explained_var": explained_variance(
                    train_batch[Postprocessing.VALUE_TARGETS],
                    self.model.value_function(),
                ),
            }

        @override(base)
        def postprocess_trajectory(
            self,
            sample_batch: SampleBatch,
            other_agent_batches: Optional[Dict[AgentID, SampleBatch]] = None,
            episode: Optional[Episode] = None,
        ):
            sample_batch = super().postprocess_trajectory(sample_batch)
            return compute_gae_for_sample_batch(
                self, sample_batch, other_agent_batches, episode
            )

        @override(base)
        def compute_gradients_fn(
            self, optimizer: LocalOptimizer, loss: TensorType
        ) -> ModelGradients:
            return compute_gradients(self, optimizer, loss)

    return A3CTFPolicy


A3CStaticGraphTFPolicy = get_a3c_tf_policy(DynamicTFPolicyV2)
A3CEagerTFPolicy = get_a3c_tf_policy(EagerTFPolicyV2)


@Deprecated(
    old="rllib.agents.a3c.a3c_tf_policy.postprocess_advantages",
    new="rllib.evaluation.postprocessing.compute_gae_for_sample_batch",
    error=True,
)
def postprocess_advantages(*args, **kwargs):
    pass
