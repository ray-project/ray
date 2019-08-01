from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging

import ray
from ray.rllib.evaluation.postprocessing import compute_advantages, \
    Postprocessing
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.tf_policy import LearningRateSchedule, \
    EntropyCoeffSchedule
from ray.rllib.policy.tf_policy_template import build_tf_policy
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.utils.explained_variance import explained_variance
from ray.rllib.utils import try_import_tf

tf = try_import_tf()

logger = logging.getLogger(__name__)

# Frozen logits of the policy that computed the action
BEHAVIOUR_LOGITS = "behaviour_logits"


class PPOLoss(object):
    def __init__(self,
                 action_space,
                 value_targets,
                 advantages,
                 actions,
                 logits,
                 vf_preds,
                 curr_action_dist,
                 value_fn,
                 cur_kl_coeff,
                 valid_mask,
                 entropy_coeff=0,
                 clip_param=0.1,
                 vf_clip_param=0.1,
                 vf_loss_coeff=1.0,
                 use_gae=True,
                 model_config=None):
        """Constructs the loss for Proximal Policy Objective.

        Arguments:
            action_space: Environment observation space specification.
            value_targets (Placeholder): Placeholder for target values; used
                for GAE.
            actions (Placeholder): Placeholder for actions taken
                from previous model evaluation.
            advantages (Placeholder): Placeholder for calculated advantages
                from previous model evaluation.
            logits (Placeholder): Placeholder for logits output from
                previous model evaluation.
            vf_preds (Placeholder): Placeholder for value function output
                from previous model evaluation.
            curr_action_dist (ActionDistribution): ActionDistribution
                of the current model.
            value_fn (Tensor): Current value function output Tensor.
            cur_kl_coeff (Variable): Variable holding the current PPO KL
                coefficient.
            valid_mask (Tensor): A bool mask of valid input elements (#2992).
            entropy_coeff (float): Coefficient of the entropy regularizer.
            clip_param (float): Clip parameter
            vf_clip_param (float): Clip parameter for the value function
            vf_loss_coeff (float): Coefficient of the value function loss
            use_gae (bool): If true, use the Generalized Advantage Estimator.
            model_config (dict): (Optional) model config for use in specifying
                action distributions.
        """

        def reduce_mean_valid(t):
            return tf.reduce_mean(tf.boolean_mask(t, valid_mask))

        dist_cls, _ = ModelCatalog.get_action_dist(action_space, model_config)
        prev_dist = dist_cls(logits, model_config=model_config)
        # Make loss functions.
        logp_ratio = tf.exp(
            curr_action_dist.logp(actions) - prev_dist.logp(actions))
        action_kl = prev_dist.kl(curr_action_dist)
        self.mean_kl = reduce_mean_valid(action_kl)

        curr_entropy = curr_action_dist.entropy()
        self.mean_entropy = reduce_mean_valid(curr_entropy)

        surrogate_loss = tf.minimum(
            advantages * logp_ratio,
            advantages * tf.clip_by_value(logp_ratio, 1 - clip_param,
                                          1 + clip_param))
        self.mean_policy_loss = reduce_mean_valid(-surrogate_loss)

        if use_gae:
            vf_loss1 = tf.square(value_fn - value_targets)
            vf_clipped = vf_preds + tf.clip_by_value(
                value_fn - vf_preds, -vf_clip_param, vf_clip_param)
            vf_loss2 = tf.square(vf_clipped - value_targets)
            vf_loss = tf.maximum(vf_loss1, vf_loss2)
            self.mean_vf_loss = reduce_mean_valid(vf_loss)
            loss = reduce_mean_valid(
                -surrogate_loss + cur_kl_coeff * action_kl +
                vf_loss_coeff * vf_loss - entropy_coeff * curr_entropy)
        else:
            self.mean_vf_loss = tf.constant(0.0)
            loss = reduce_mean_valid(-surrogate_loss +
                                     cur_kl_coeff * action_kl -
                                     entropy_coeff * curr_entropy)
        self.loss = loss


def ppo_surrogate_loss(policy, batch_tensors):
    if policy.state_in:
        max_seq_len = tf.reduce_max(policy.seq_lens)
        mask = tf.sequence_mask(policy.seq_lens, max_seq_len)
        mask = tf.reshape(mask, [-1])
    else:
        mask = tf.ones_like(
            batch_tensors[Postprocessing.ADVANTAGES], dtype=tf.bool)

    policy.loss_obj = PPOLoss(
        policy.action_space,
        batch_tensors[Postprocessing.VALUE_TARGETS],
        batch_tensors[Postprocessing.ADVANTAGES],
        batch_tensors[SampleBatch.ACTIONS],
        batch_tensors[BEHAVIOUR_LOGITS],
        batch_tensors[SampleBatch.VF_PREDS],
        policy.action_dist,
        policy.value_function,
        policy.kl_coeff,
        mask,
        entropy_coeff=policy.entropy_coeff,
        clip_param=policy.config["clip_param"],
        vf_clip_param=policy.config["vf_clip_param"],
        vf_loss_coeff=policy.config["vf_loss_coeff"],
        use_gae=policy.config["use_gae"],
        model_config=policy.config["model"])

    return policy.loss_obj.loss


def kl_and_loss_stats(policy, batch_tensors):
    return {
        "cur_kl_coeff": tf.cast(policy.kl_coeff, tf.float64),
        "cur_lr": tf.cast(policy.cur_lr, tf.float64),
        "total_loss": policy.loss_obj.loss,
        "policy_loss": policy.loss_obj.mean_policy_loss,
        "vf_loss": policy.loss_obj.mean_vf_loss,
        "vf_explained_var": explained_variance(
            batch_tensors[Postprocessing.VALUE_TARGETS],
            policy.value_function),
        "kl": policy.loss_obj.mean_kl,
        "entropy": policy.loss_obj.mean_entropy,
        "entropy_coeff": tf.cast(policy.entropy_coeff, tf.float64),
    }


def vf_preds_and_logits_fetches(policy):
    """Adds value function and logits outputs to experience batches."""
    return {
        SampleBatch.VF_PREDS: policy.value_function,
        BEHAVIOUR_LOGITS: policy.model_out,
    }


def postprocess_ppo_gae(policy,
                        sample_batch,
                        other_agent_batches=None,
                        episode=None):
    """Adds the policy logits, VF preds, and advantages to the trajectory."""

    completed = sample_batch["dones"][-1]
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
    batch = compute_advantages(
        sample_batch,
        last_r,
        policy.config["gamma"],
        policy.config["lambda"],
        use_gae=policy.config["use_gae"])
    return batch


def clip_gradients(policy, optimizer, loss):
    if policy.config["grad_clip"] is not None:
        policy.var_list = tf.get_collection(tf.GraphKeys.TRAINABLE_VARIABLES,
                                            tf.get_variable_scope().name)
        grads = tf.gradients(loss, policy.var_list)
        policy.grads, _ = tf.clip_by_global_norm(grads,
                                                 policy.config["grad_clip"])
        clipped_grads = list(zip(policy.grads, policy.var_list))
        return clipped_grads
    else:
        return optimizer.compute_gradients(
            loss, colocate_gradients_with_ops=True)


class KLCoeffMixin(object):
    def __init__(self, config):
        # KL Coefficient
        self.kl_coeff_val = config["kl_coeff"]
        self.kl_target = config["kl_target"]
        self.kl_coeff = tf.get_variable(
            initializer=tf.constant_initializer(self.kl_coeff_val),
            name="kl_coeff",
            shape=(),
            trainable=False,
            dtype=tf.float32)

    def update_kl(self, sampled_kl):
        if sampled_kl > 2.0 * self.kl_target:
            self.kl_coeff_val *= 1.5
        elif sampled_kl < 0.5 * self.kl_target:
            self.kl_coeff_val *= 0.5
        self.kl_coeff.load(self.kl_coeff_val, session=self.get_session())
        return self.kl_coeff_val


class ValueNetworkMixin(object):
    def __init__(self, obs_space, action_space, config):
        if config["use_gae"]:
            self.value_function = self.model.value_function()
        else:
            self.value_function = tf.zeros(
                shape=tf.shape(self.get_placeholder(SampleBatch.CUR_OBS))[:1])

    def _value(self, ob, prev_action, prev_reward, *args):
        feed_dict = {
            self.get_placeholder(SampleBatch.CUR_OBS): [ob],
            self.get_placeholder(SampleBatch.PREV_ACTIONS): [prev_action],
            self.get_placeholder(SampleBatch.PREV_REWARDS): [prev_reward],
            self.seq_lens: [1]
        }
        assert len(args) == len(self.state_in), (args, self.state_in)
        for k, v in zip(self.state_in, args):
            feed_dict[k] = v
        vf = self.get_session().run(self.value_function, feed_dict)
        return vf[0]


def setup_config(policy, obs_space, action_space, config):
    # auto set the model option for layer sharing
    config["model"]["vf_share_layers"] = config["vf_share_layers"]


def setup_mixins(policy, obs_space, action_space, config):
    ValueNetworkMixin.__init__(policy, obs_space, action_space, config)
    KLCoeffMixin.__init__(policy, config)
    EntropyCoeffSchedule.__init__(policy, config["entropy_coeff"],
                                  config["entropy_coeff_schedule"])
    LearningRateSchedule.__init__(policy, config["lr"], config["lr_schedule"])


PPOTFPolicy = build_tf_policy(
    name="PPOTFPolicy",
    get_default_config=lambda: ray.rllib.agents.ppo.ppo.DEFAULT_CONFIG,
    loss_fn=ppo_surrogate_loss,
    stats_fn=kl_and_loss_stats,
    extra_action_fetches_fn=vf_preds_and_logits_fetches,
    postprocess_fn=postprocess_ppo_gae,
    gradients_fn=clip_gradients,
    before_init=setup_config,
    before_loss_init=setup_mixins,
    mixins=[
        LearningRateSchedule, EntropyCoeffSchedule, KLCoeffMixin,
        ValueNetworkMixin
    ])
