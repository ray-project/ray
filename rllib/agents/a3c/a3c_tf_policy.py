"""Note: Keep in sync with changes to VTraceTFPolicy."""

import ray
from ray.rllib.agents.ppo.ppo_tf_policy import ValueNetworkMixin
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.evaluation.postprocessing import compute_gae_for_sample_batch, \
    Postprocessing
from ray.rllib.policy.tf_policy_template import build_tf_policy
from ray.rllib.policy.tf_policy import LearningRateSchedule
from ray.rllib.utils.deprecation import deprecation_warning
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.tf_ops import explained_variance

tf1, tf, tfv = try_import_tf()


def postprocess_advantages(policy,
                           sample_batch,
                           other_agent_batches=None,
                           episode=None):

    # Stub serving backward compatibility.
    deprecation_warning(
        old="rllib.agents.a3c.a3c_tf_policy.postprocess_advantages",
        new="rllib.evaluation.postprocessing.compute_gae_for_sample_batch",
        error=False)

    return compute_gae_for_sample_batch(policy, sample_batch,
                                        other_agent_batches, episode)


class A3CLoss:
    def __init__(self,
                 action_dist,
                 actions,
                 advantages,
                 v_target,
                 vf,
                 vf_loss_coeff=0.5,
                 entropy_coeff=0.01):
        log_prob = action_dist.logp(actions)

        # The "policy gradients" loss
        self.pi_loss = -tf.reduce_sum(log_prob * advantages)

        delta = vf - v_target
        self.vf_loss = 0.5 * tf.reduce_sum(tf.math.square(delta))
        self.entropy = tf.reduce_sum(action_dist.entropy())
        self.total_loss = (self.pi_loss + self.vf_loss * vf_loss_coeff -
                           self.entropy * entropy_coeff)


def actor_critic_loss(policy, model, dist_class, train_batch):
    model_out, _ = model.from_batch(train_batch)
    action_dist = dist_class(model_out, model)
    policy.loss = A3CLoss(action_dist, train_batch[SampleBatch.ACTIONS],
                          train_batch[Postprocessing.ADVANTAGES],
                          train_batch[Postprocessing.VALUE_TARGETS],
                          model.value_function(),
                          policy.config["vf_loss_coeff"],
                          policy.config["entropy_coeff"])
    return policy.loss.total_loss


def add_value_function_fetch(policy):
    return {SampleBatch.VF_PREDS: policy.model.value_function()}


def stats(policy, train_batch):
    return {
        "cur_lr": tf.cast(policy.cur_lr, tf.float64),
        "policy_loss": policy.loss.pi_loss,
        "policy_entropy": policy.loss.entropy,
        "var_gnorm": tf.linalg.global_norm(
            list(policy.model.trainable_variables())),
        "vf_loss": policy.loss.vf_loss,
    }


def grad_stats(policy, train_batch, grads):
    return {
        "grad_gnorm": tf.linalg.global_norm(grads),
        "vf_explained_var": explained_variance(
            train_batch[Postprocessing.VALUE_TARGETS],
            policy.model.value_function()),
    }


def clip_gradients(policy, optimizer, loss):
    grads_and_vars = optimizer.compute_gradients(
        loss, policy.model.trainable_variables())
    grads = [g for (g, v) in grads_and_vars]
    grads, _ = tf.clip_by_global_norm(grads, policy.config["grad_clip"])
    clipped_grads = list(zip(grads, policy.model.trainable_variables()))
    return clipped_grads


def setup_mixins(policy, obs_space, action_space, config):
    ValueNetworkMixin.__init__(policy, obs_space, action_space, config)
    LearningRateSchedule.__init__(policy, config["lr"], config["lr_schedule"])


A3CTFPolicy = build_tf_policy(
    name="A3CTFPolicy",
    get_default_config=lambda: ray.rllib.agents.a3c.a3c.DEFAULT_CONFIG,
    loss_fn=actor_critic_loss,
    stats_fn=stats,
    grad_stats_fn=grad_stats,
    gradients_fn=clip_gradients,
    postprocess_fn=compute_gae_for_sample_batch,
    extra_action_fetches_fn=add_value_function_fetch,
    before_loss_init=setup_mixins,
    mixins=[ValueNetworkMixin, LearningRateSchedule])
