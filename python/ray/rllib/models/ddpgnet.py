from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf
import tensorflow.contrib.slim as slim

from ray.rllib.models.model import Model


class DDPGActor(Model):
    """Actor network for DDPG."""

    def _init(self, inputs, num_outputs, options):
        w_normal = tf.truncated_normal_initializer()
        w_init = tf.random_uniform_initializer(minval=-0.003, maxval=0.003)
        ac_bound = options["action_bound"]

        net = slim.fully_connected(
             inputs, 400, activation_fn=tf.nn.relu,
             weights_initializer=w_normal)
        net = slim.fully_connected(
             net, 300, activation_fn=tf.nn.relu, weights_initializer=w_normal)
        out = slim.fully_connected(
             net, num_outputs, activation_fn=tf.nn.tanh,
             weights_initializer=w_init)
        scaled_out = tf.multiply(out, ac_bound)
        return scaled_out, net


class DDPGCritic(Model):
    """Critic network for DDPG."""

    def _init(self, inputs, num_outputs, options):
        obs, action = inputs
        w_normal = tf.truncated_normal_initializer()
        w_init = tf.random_uniform_initializer(minval=-0.0003, maxval=0.0003)
        net = slim.fully_connected(
             obs, 400, activation_fn=tf.nn.relu, weights_initializer=w_normal)
        t1 = slim.fully_connected(
            net, 300, activation_fn=None, biases_initializer=None,
            weights_initializer=w_normal)
        t2 = slim.fully_connected(
            action, 300, activation_fn=None, weights_initializer=w_normal)
        net = tf.nn.relu(tf.add(t1, t2))

        out = slim.fully_connected(
             net, 1, activation_fn=None, weights_initializer=w_init)
        return out, net
