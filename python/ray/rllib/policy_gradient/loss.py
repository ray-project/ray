from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import gym.spaces
import tensorflow as tf

from ray.rllib.models import ModelCatalog


class ProximalPolicyLoss(object):

    def __init__(
            self, observation_space, action_space,
            observations, returns, advantages, actions, prev_logits, logit_dim,
            kl_coeff, distribution_class, config, sess):
        assert (isinstance(action_space, gym.spaces.Discrete) or
                isinstance(action_space, gym.spaces.Box))
        self.prev_dist = distribution_class(prev_logits)

        # Saved so that we can compute actions given different observations
        self.observations = observations

        self.curr_logits = ModelCatalog.get_model(
            observations, logit_dim, config["model"]).outputs
        self.curr_dist = distribution_class(self.curr_logits)
        self.sampler = self.curr_dist.sample()

        self.value_function = ModelCatalog.get_model(
            observations, 1, config["model"], "value_function").outputs

        # Make loss functions.
        self.ratio = tf.exp(self.curr_dist.logp(actions) -
                            self.prev_dist.logp(actions))
        self.kl = self.prev_dist.kl(self.curr_dist)
        self.mean_kl = tf.reduce_mean(self.kl)
        self.entropy = self.curr_dist.entropy()
        self.mean_entropy = tf.reduce_mean(self.entropy)
        self.surr1 = self.ratio * advantages
        self.surr2 = tf.clip_by_value(self.ratio, 1 - config["clip_param"],
                                      1 + config["clip_param"]) * advantages
        self.vfloss1 = tf.square(self.value_function - returns)
        self.vfloss = self.vfloss1
        self.surr = tf.minimum(self.surr1, self.surr2)
        self.loss = tf.reduce_mean(-self.surr + kl_coeff * self.kl + self.vfloss -
                                   config["entropy_coeff"] * self.entropy)
        self.sess = sess

    def compute(self, observations):
        return self.sess.run([self.sampler, self.curr_logits, self.value_function],
                             feed_dict={self.observations: observations})

    def loss(self):
        return self.loss
