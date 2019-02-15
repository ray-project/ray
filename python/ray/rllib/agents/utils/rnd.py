""" Implementation of Random Network Distillation(RND).

Author
------
Vishal Satish
"""
import logging

import numpy as np
import tensorflow as tf

from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.utils.filter import get_filter

logger = logging.getLogger(__name__)

class RND(object):
    def __init__(self, is_training, obs_space, logit_dim, model_cfg, rnd_predictor_update_proportion=1.0):
        self._rnd_predictor_update_proportion = rnd_predictor_update_proportion

        # normalize and clip obs to [-5.0, 5.0]
        obs = tf.clip_by_value(obs, -5.0, 5.0)
 
        # build target and predictor networks
        logger.info("Building RND networks...")
        self._norm_obs = tf.placeholder(tf.float32, [None] + list(obs_space.shape))
        self._is_training = is_training
        self._targets = ModelCatalog.get_model(
            {   
                "obs": self._norm_obs,
                "is_training": is_training,
            },  
            obs_space,
            logit_dim,
            model_cfg).outputs
        self._targets = tf.stop_gradient(self._targets) # freeze target network

        self._preds = ModelCatalog.get_model(
            {   
                "obs": self._norm_obs,
                "is_training": is_training,
            },  
            obs_space,
            logit_dim,
            model_cfg).outputs

        # build normalization filter for intr reward (bonus)
        self._intr_reward_filter = get_filter("MeanStdFilter", (1,))

        # build intr reward (bonus) with normalization
        self._intr_reward = self._build_intr_reward()

        # build loss for random network
        self._loss = self._build_loss()

    @property
    def intr_rew(self):
        return self._intr_reward

    @property
    def loss(self):
        return self._loss

    @property
    def norm_obs(self):
        return self._norm_obs

    def _build_intr_reward(self):
        logger.info('Building intrinisic reward...')
        intr_rew = tf.reduce_mean(tf.square(self._preds - self._targets), axis=-1, keep_dims=True)
        return intr_rew

    def compute_intr_rew(self, obs):
        return np.squeeze(self._intr_reward_filter(self.sess.run(self.intr_rew, feed_dict={self.norm_obs: obs})))

    def _build_loss(self):
        logger.info('Building RND loss...')
        loss = tf.reduce_mean(tf.square(self._preds - self._targets), axis=-1)
        keep_mask = tf.random_uniform(shape=tf.shape(loss), minval=0.0, maxval=1.0, dtype=tf.float32)
        keep_mask = tf.cast(keep_mask < self._rnd_predictor_update_proportion, tf.float32)
        loss = tf.reduce_sum(loss * keep_mask) / tf.maximum(tf.reduce_sum(keep_mask), 1.0)
        return loss

