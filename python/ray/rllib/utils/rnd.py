""" Implementation of Random Network Distillation (RND).
Original Paper: https://arxiv.org/abs/1810.12894

Author
------
Vishal Satish
"""
import logging

import numpy as np
import tensorflow as tf

from ray.rllib.models.catalog import ModelCatalog

logger = logging.getLogger(__name__)

class RND(object):
    def __init__(self, obs_ph, is_training_ph, obs_space, logit_dim, model_cfg, sess, rnd_predictor_update_proportion=1.0):
        # set proportion of bonuses to actually use
        self._rnd_predictor_update_proportion = rnd_predictor_update_proportion

        # get handle to TF session for bonus inference
        self._sess = sess

        # get handles to RND net placeholders for bonus inference
        self._obs_ph = obs_ph
        self._is_training_ph = is_training_ph

        # normalize obs
#        obs_ph = tf.layers.batch_normalization(obs_ph, training=is_training_ph)

        # clip obs to [-5.0, 5.0]
#        obs_ph = tf.clip_by_value(obs_ph, -5.0, 5.0)

        # build target and predictor networks
        logger.info("Building RND networks...")
        with tf.variable_scope("rnd"):
            with tf.variable_scope("target"):
                self._targets = ModelCatalog.get_model(
                    {   
                        "obs": obs_ph,
                        "is_training": is_training_ph,
                    },  
                    obs_space,
                    logit_dim,
                    model_cfg).outputs
            self._targets = tf.stop_gradient(self._targets) # freeze target network

            with tf.variable_scope("predictor"):
                self._preds = ModelCatalog.get_model(
                    {   
                        "obs": obs_ph,
                        "is_training": is_training_ph,
                    },  
                    obs_space,
                    logit_dim,
                    model_cfg).outputs

        # build intr reward (bonus)
        self._intr_rew = self._build_intr_reward()

        # build loss for random network
        self._loss = self._build_loss()

    @property
    def loss(self):
        return self._loss

    def _build_intr_reward(self):
        logger.info('Building RND intrinisic reward...')
        intr_rew = tf.reduce_mean(tf.square(self._preds - self._targets), axis=-1, keep_dims=True)
        # normalize intr reward
#        intr_rew = tf.layers.batch_normalization(intr_rew, training=self._is_training_ph)
        return intr_rew

    def compute_intr_rew(self, obs):
        return np.squeeze(self._sess.run(self._intr_rew, feed_dict={self._obs_ph: obs, self._is_training_ph: False}))

    def _build_loss(self):
        logger.info('Building RND loss...')
        loss = tf.reduce_mean(tf.square(self._preds - self._targets), axis=-1)
        keep_mask = tf.random_uniform(shape=tf.shape(loss), minval=0.0, maxval=1.0, dtype=tf.float32)
        keep_mask = tf.cast(keep_mask < self._rnd_predictor_update_proportion, tf.float32)
        loss = tf.reduce_sum(loss * keep_mask) / tf.maximum(tf.reduce_sum(keep_mask), 1.0)
        return loss

