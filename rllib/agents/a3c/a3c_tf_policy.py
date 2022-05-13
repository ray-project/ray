"""Note: Keep in sync with changes to VTraceTFPolicy."""
from typing import Optional, Dict
import gym

import ray
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.evaluation.episode import Episode
from ray.rllib.evaluation.postprocessing import (
    compute_gae_for_sample_batch,
    Postprocessing,
)
from ray.rllib.models.action_dist import ActionDistribution
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.tf_mixins import (
    EntropyCoeffSchedule,
    LearningRateSchedule,
    ValueNetworkMixin,
)
from ray.rllib.policy.tf_policy_template import build_tf_policy
from ray.rllib.utils.deprecation import Deprecated
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.tf_utils import explained_variance
from ray.rllib.utils.typing import (
    TrainerConfigDict,
    TensorType,
    PolicyID,
    LocalOptimizer,
    ModelGradients,
)

tf1, tf, tfv = try_import_tf()


@Deprecated(
    old="rllib.agents.a3c.a3c_tf_policy.postprocess_advantages",
    new="rllib.evaluation.postprocessing.compute_gae_for_sample_batch",
    error=False,
)
def postprocess_advantages(
    policy: Policy,
    sample_batch: SampleBatch,
    other_agent_batches: Optional[Dict[PolicyID, SampleBatch]] = None,
    episode: Optional[Episode] = None,
) -> SampleBatch:

    return compute_gae_for_sample_batch(
        policy, sample_batch, other_agent_batches, episode
    )


class A3CLoss:
    def __init__(
        self,
        action_dist: ActionDistribution,
        actions: TensorType,
        advantages: TensorType,
        v_target: TensorType,
        vf: TensorType,
        valid_mask: TensorType,
        vf_loss_coeff: float = 0.5,
        entropy_coeff: float = 0.01,
        use_critic: bool = True,
    ):
        log_prob = action_dist.logp(actions)

        # The "policy gradients" loss
        self.pi_loss = -tf.reduce_sum(
            tf.boolean_mask(log_prob * advantages, valid_mask)
        )

        delta = tf.boolean_mask(vf - v_target, valid_mask)

        # Compute a value function loss.
        if use_critic:
            self.vf_loss = 0.5 * tf.reduce_sum(tf.math.square(delta))
        # Ignore the value function.
        else:
            self.vf_loss = tf.constant(0.0)

        self.entropy = tf.reduce_sum(tf.boolean_mask(action_dist.entropy(), valid_mask))

        self.total_loss = (
            self.pi_loss + self.vf_loss * vf_loss_coeff - self.entropy * entropy_coeff
        )


def actor_critic_loss(
    policy: Policy,
    model: ModelV2,
    dist_class: ActionDistribution,
    train_batch: SampleBatch,
) -> TensorType:
    model_out, _ = model(train_batch)
    action_dist = dist_class(model_out, model)
    if policy.is_recurrent():
        max_seq_len = tf.reduce_max(train_batch[SampleBatch.SEQ_LENS])
        mask = tf.sequence_mask(train_batch[SampleBatch.SEQ_LENS], max_seq_len)
        mask = tf.reshape(mask, [-1])
    else:
        mask = tf.ones_like(train_batch[SampleBatch.REWARDS])
    policy.loss = A3CLoss(
        action_dist,
        train_batch[SampleBatch.ACTIONS],
        train_batch[Postprocessing.ADVANTAGES],
        train_batch[Postprocessing.VALUE_TARGETS],
        model.value_function(),
        mask,
        policy.config["vf_loss_coeff"],
        policy.entropy_coeff,
        policy.config.get("use_critic", True),
    )
    return policy.loss.total_loss


def add_value_function_fetch(policy: Policy) -> Dict[str, TensorType]:
    return {SampleBatch.VF_PREDS: policy.model.value_function()}


def stats(policy: Policy, train_batch: SampleBatch) -> Dict[str, TensorType]:
    return {
        "cur_lr": tf.cast(policy.cur_lr, tf.float64),
        "entropy_coeff": tf.cast(policy.entropy_coeff, tf.float64),
        "policy_loss": policy.loss.pi_loss,
        "policy_entropy": policy.loss.entropy,
        "var_gnorm": tf.linalg.global_norm(list(policy.model.trainable_variables())),
        "vf_loss": policy.loss.vf_loss,
    }


def grad_stats(
    policy: Policy, train_batch: SampleBatch, grads: ModelGradients
) -> Dict[str, TensorType]:
    return {
        "grad_gnorm": tf.linalg.global_norm(grads),
        "vf_explained_var": explained_variance(
            train_batch[Postprocessing.VALUE_TARGETS], policy.model.value_function()
        ),
    }


def clip_gradients(
    policy: Policy, optimizer: LocalOptimizer, loss: TensorType
) -> ModelGradients:
    grads_and_vars = optimizer.compute_gradients(
        loss, policy.model.trainable_variables()
    )
    grads = [g for (g, v) in grads_and_vars]
    grads, _ = tf.clip_by_global_norm(grads, policy.config["grad_clip"])
    clipped_grads = list(zip(grads, policy.model.trainable_variables()))
    return clipped_grads


def setup_mixins(
    policy: Policy,
    obs_space: gym.spaces.Space,
    action_space: gym.spaces.Space,
    config: TrainerConfigDict,
) -> None:
    ValueNetworkMixin.__init__(policy, config)
    LearningRateSchedule.__init__(policy, config["lr"], config["lr_schedule"])
    EntropyCoeffSchedule.__init__(
        policy, config["entropy_coeff"], config["entropy_coeff_schedule"]
    )


A3CTFPolicy = build_tf_policy(
    name="A3CTFPolicy",
    get_default_config=lambda: ray.rllib.agents.a3c.a3c.DEFAULT_CONFIG,
    loss_fn=actor_critic_loss,
    stats_fn=stats,
    grad_stats_fn=grad_stats,
    compute_gradients_fn=clip_gradients,
    postprocess_fn=compute_gae_for_sample_batch,
    extra_action_out_fn=add_value_function_fetch,
    before_loss_init=setup_mixins,
    mixins=[ValueNetworkMixin, LearningRateSchedule, EntropyCoeffSchedule],
)
