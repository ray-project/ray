from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf

from ray.rllib.models import ModelCatalog


class ProximalPolicyLoss(object):

    other_output = ["vf_preds", "logprobs"]
    is_recurrent = False

    def __init__(
            self, observation_space, action_space,
            observations, value_targets, advantages, actions,
            prev_logits, prev_vf_preds, logit_dim,
            kl_coeff, distribution_class, config, sess, registry):
        self.prev_dist = distribution_class(prev_logits)
        self.shared_model = (config["model"].get("custom_options", {}).
                             get("multiagent_shared_model", False))
        self.num_agents = len(config["model"].get(
            "custom_options", {}).get("multiagent_obs_shapes", [1]))

        # Saved so that we can compute actions given different observations
        self.observations = observations

        self.curr_logits = ModelCatalog.get_model(
            registry, observations, logit_dim, config["model"]).outputs
        self.curr_dist = distribution_class(self.curr_logits)
        self.sampler = self.curr_dist.sample()

        if config["use_gae"]:
            vf_config = config["model"].copy()
            # Do not split the last layer of the value function into
            # mean parameters and standard deviation parameters and
            # do not make the standard deviations free variables.
            vf_config["free_log_std"] = False
            with tf.variable_scope("value_function"):
                self.value_function = ModelCatalog.get_model(
                    registry, observations, 1, vf_config).outputs
            self.value_function = tf.reshape(self.value_function, [-1])

        curr_logp = self.curr_dist.logp(actions)
        prev_logp = self.prev_dist.logp(actions)
        self.kl = self.prev_dist.kl(self.curr_dist)
        self.entropy = self.curr_dist.entropy()

        # handle everything uniform as if it were the multiagent case
        if not isinstance(curr_logp, list):
            self.kl = [self.kl]
            curr_logp = [curr_logp]
            prev_logp = [prev_logp]
            self.entropy = [self.entropy]

        # Make loss functions.
        self.ratio = [tf.exp(curr - prev)
                      for curr, prev in zip(curr_logp, prev_logp)]
        self.mean_kl = [tf.reduce_mean(kl_i) for kl_i in self.kl]
        self.mean_entropy = tf.reduce_mean(self.entropy)
        self.surr1 = [ratio_i * advantages for ratio_i in self.ratio]
        self.surr2 = [tf.clip_by_value(ratio_i, 1 - config["clip_param"],
                                       1 + config["clip_param"]) * advantages
                      for ratio_i in self.ratio]
        self.surr = [tf.minimum(surr1_i, surr2_i) for surr1_i, surr2_i in
                     zip(self.surr1, self.surr2)]
        self.surr = tf.add_n(self.surr)
        self.mean_policy_loss = tf.reduce_mean(-self.surr)

        self.entropy = tf.add_n(self.entropy)
        entropy_prod = config["entropy_coeff"]*self.entropy

        # there's only one kl value for a shared model
        if self.shared_model:
            kl_prod = tf.add_n([kl_coeff[0] * kl_i for
                                i, kl_i in enumerate(self.kl)])
            # all the above values have been rescaled by num_agents
            self.surr /= self.num_agents
            kl_prod /= self.num_agents
            entropy_prod /= self.num_agents
        else:
            kl_prod = tf.add_n([kl_coeff[i] * kl_i for
                                i, kl_i in enumerate(self.kl)])

        if config["use_gae"]:
            # We use a huber loss here to be more robust against outliers,
            # which seem to occur when the rollouts get longer (the variance
            # scales superlinearly with the length of the rollout)
            self.vf_loss1 = tf.square(self.value_function - value_targets)
            vf_clipped = prev_vf_preds + tf.clip_by_value(
                self.value_function - prev_vf_preds,
                -config["clip_param"], config["clip_param"])
            self.vf_loss2 = tf.square(vf_clipped - value_targets)
            self.vf_loss = tf.minimum(self.vf_loss1, self.vf_loss2)
            self.mean_vf_loss = tf.reduce_mean(self.vf_loss)
            self.loss = tf.reduce_mean(
                -self.surr + kl_prod +
                config["vf_loss_coeff"] * self.vf_loss -
                entropy_prod)
        else:
            self.mean_vf_loss = tf.constant(0.0)
            self.loss = tf.reduce_mean(
                -self.surr +
                kl_prod - entropy_prod)

        self.sess = sess

        if config["use_gae"]:
            self.policy_results = [
                self.sampler, self.curr_logits, self.value_function]
        else:
            self.policy_results = [
                self.sampler, self.curr_logits, tf.constant("NA")]

    def compute(self, observation):
        action, logprobs, vf = self.sess.run(
            self.policy_results,
            feed_dict={self.observations: [observation]})
        return action[0], {"vf_preds": vf[0], "logprobs": logprobs[0]}

    def loss(self):
        return self.loss
