"""Note: Keep in sync with changes to VTraceTFPolicy."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.explained_variance import explained_variance
from ray.rllib.evaluation.postprocessing import compute_advantages, \
    Postprocessing
from ray.rllib.policy.tf_policy_template import build_tf_policy
from ray.rllib.policy.tf_policy import LearningRateSchedule
from ray.rllib.utils import try_import_tf

tf = try_import_tf()


class A3CLoss(object):
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
        self.vf_loss = 0.5 * tf.reduce_sum(tf.square(delta))
        self.entropy = tf.reduce_sum(action_dist.entropy())
        self.total_loss = (self.pi_loss + self.vf_loss * vf_loss_coeff -
                           self.entropy * entropy_coeff)


def actor_critic_loss(policy, batch_tensors):
    policy.loss = A3CLoss(
        policy.action_dist, batch_tensors[SampleBatch.ACTIONS],
        batch_tensors[Postprocessing.ADVANTAGES],
        batch_tensors[Postprocessing.VALUE_TARGETS], policy.vf,
        policy.config["vf_loss_coeff"], policy.config["entropy_coeff"])
    return policy.loss.total_loss


def postprocess_advantages(policy,
                           sample_batch,
                           other_agent_batches=None,
                           episode=None):
    completed = sample_batch[SampleBatch.DONES][-1]
    if completed:
        last_r = 0.0
    else:
        next_state = []
        for i in range(len(policy.state_in)):
            next_state.append([sample_batch["state_out_{}".format(i)][-1]])
        last_r = policy._value(sample_batch[SampleBatch.NEXT_OBS][-1],
                               sample_batch[SampleBatch.ACTIONS][-1],
                               sample_batch[SampleBatch.REWARDS][-1],
                               *next_state)
    return compute_advantages(sample_batch, last_r, policy.config["gamma"],
                              policy.config["lambda"])


def add_value_function_fetch(policy):
    return {SampleBatch.VF_PREDS: policy.vf}


class ValueNetworkMixin(object):
    def __init__(self):
        self.vf = self.model.value_function()

    def _value(self, ob, prev_action, prev_reward, *args):
        feed_dict = {
            self.get_placeholder(SampleBatch.CUR_OBS): [ob],
            self.get_placeholder(SampleBatch.PREV_ACTIONS): [prev_action],
            self.get_placeholder(SampleBatch.PREV_REWARDS): [prev_reward],
            self.seq_lens: [1]
        }
        assert len(args) == len(self.state_in), \
            (args, self.state_in)
        for k, v in zip(self.state_in, args):
            feed_dict[k] = v
        vf = self.get_session().run(self.vf, feed_dict)
        return vf[0]


def stats(policy, batch_tensors):
    return {
        "cur_lr": tf.cast(policy.cur_lr, tf.float64),
        "policy_loss": policy.loss.pi_loss,
        "policy_entropy": policy.loss.entropy,
        "var_gnorm": tf.global_norm([x for x in policy.var_list]),
        "vf_loss": policy.loss.vf_loss,
    }


def grad_stats(policy, grads):
    return {
        "grad_gnorm": tf.global_norm(grads),
        "vf_explained_var": explained_variance(
            policy.get_placeholder(Postprocessing.VALUE_TARGETS), policy.vf),
    }


def clip_gradients(policy, optimizer, loss):
    grads = tf.gradients(loss, policy.var_list)
    grads, _ = tf.clip_by_global_norm(grads, policy.config["grad_clip"])
    clipped_grads = list(zip(grads, policy.var_list))
    return clipped_grads


def setup_mixins(policy, obs_space, action_space, config):
    ValueNetworkMixin.__init__(policy)
    LearningRateSchedule.__init__(policy, config["lr"], config["lr_schedule"])
    policy.var_list = tf.get_collection(tf.GraphKeys.TRAINABLE_VARIABLES,
                                        tf.get_variable_scope().name)


A3CTFPolicy = build_tf_policy(
    name="A3CTFPolicy",
    get_default_config=lambda: ray.rllib.agents.a3c.a3c.DEFAULT_CONFIG,
    loss_fn=actor_critic_loss,
    stats_fn=stats,
    grad_stats_fn=grad_stats,
    gradients_fn=clip_gradients,
    postprocess_fn=postprocess_advantages,
    extra_action_fetches_fn=add_value_function_fetch,
    before_loss_init=setup_mixins,
    mixins=[ValueNetworkMixin, LearningRateSchedule])
