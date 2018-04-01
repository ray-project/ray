from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.ddpg.random_process import OrnsteinUhlenbeckProcess

import numpy as np
import tensorflow as tf
import tflearn


class DDPGModel():
    other_output = []
    is_recurrent = False

    def __init__(self, registry, env, config, sess):
        self.env = env
        self.registry = registry
        self.config = config
        self.sess = sess

        obs_space = env.observation_space
        ac_space = env.action_space

        self.obs_size = np.prod(obs_space.shape)
        self.obs = tf.placeholder(tf.float32, [None, self.obs_size])
        self.ac_size = np.prod(ac_space.shape)
        self.act = tf.placeholder(tf.float32, [None, self.ac_size])
        self.action_bound = env.action_space.high
        # TODO: change action_bound to make more general

        # set up actor network
        self._setup_actor_network(obs_space, ac_space)

        # setting up critic
        self._setup_critic_network(obs_space, ac_space)
        self._setup_critic_loss(ac_space)

        with tf.variable_scope("critic"):
            self.critic_var_list = tf.get_collection(
                                     tf.GraphKeys.TRAINABLE_VARIABLES,
                                     tf.get_variable_scope().name
                                   )

        with tf.variable_scope("actor"):
            self.actor_var_list = tf.get_collection(
                                    tf.GraphKeys.TRAINABLE_VARIABLES,
                                    tf.get_variable_scope().name
                                  )

        if (self.config["parameter_noise"]):
            self.random_process = OrnsteinUhlenbeckProcess(size=1,
                                                           theta=0.15,
                                                           mu=0,
                                                           sigma=0.2)
            self.epsilon = 1.0

    def _setup_critic_loss(self, action_space):
        self.target_Q = tf.placeholder(tf.float32, [None, 1], name="target_q")

        # compare critic eval to critic_target (squared loss)
        self.reward = tf.placeholder(tf.float32, [None], name="reward")
        self.critic_target = tf.expand_dims(self.reward, 1) + \
            self.config['gamma'] * self.target_Q
        self.critic_loss = tf.reduce_mean(tf.square(
                                        self.critic_target - self.critic_eval))

    def _setup_critic_network(self, obs_space, ac_space):
        """Sets up Q network."""
        with tf.variable_scope("critic", reuse=tf.AUTO_REUSE):
            self.critic_eval = self._create_critic_network(
                               self.obs, self.act)

        with tf.variable_scope("critic", reuse=True):
            tf.get_variable_scope().reuse_variables()
            self.cn_for_loss = self._create_critic_network(
                               self.obs, self.output_action)

    def _create_critic_network(self, obs, action):
        net = tflearn.fully_connected(obs, 400)
        net = tflearn.layers.normalization.batch_normalization(net)
        net = tflearn.activations.relu(net)

        t1 = tflearn.fully_connected(net, 300)
        t2 = tflearn.fully_connected(action, 300)
        net = tflearn.activation(
            tf.matmul(net, t1.W) + tf.matmul(action, t2.W) + t2.b,
            activation='relu')
        w_init = tflearn.initializations.uniform(minval=-0.0003, maxval=0.0003)
        out = tflearn.fully_connected(net, 1, weights_init=w_init)
        return out

    def _setup_actor_network(self, obs_space, ac_space):
        with tf.variable_scope("actor", reuse=tf.AUTO_REUSE):
            self.output_action = self._create_actor_network(self.obs)

    def _create_actor_network(self, obs):
        net = tflearn.fully_connected(obs, 400)
        net = tflearn.layers.normalization.batch_normalization(net)
        net = tflearn.activations.relu(net)
        net = tflearn.fully_connected(net, 300)
        net = tflearn.layers.normalization.batch_normalization(net)
        net = tflearn.activations.relu(net)
        w_init = tflearn.initializations.uniform(minval=-0.003, maxval=0.003)
        out = tflearn.fully_connected(
            net, self.ac_size, activation='tanh', weights_init=w_init)
        # Scale output to -action_bound to action_bound
        scaled_out = tf.multiply(out, self.action_bound)
        return scaled_out

    def get_weights(self):
        """Returns critic weights, actor weights."""
        #TODO: Update
        return self.critic_vars.get_weights(), self.actor_vars.get_weights()

    def set_weights(self, weights):
        """Sets critic and actor weights."""
        #TODO: Update
        critic_weights, actor_weights = weights
        self.critic_vars.set_weights(critic_weights)
        self.actor_vars.set_weights(actor_weights)

    def compute(self, ob):
        """Returns action, given state."""
        flattened_ob = np.reshape(ob, [-1, np.prod(ob.shape)])
        action = self.sess.run(self.output_action, {self.obs: flattened_ob})
        if (self.config["parameter_noise"]):
            action += self.epsilon * self.random_process.sample()
            if (self.epsilon > 0):
                self.epsilon -= self.config["parameter_epsilon"]
        return action[0], {}

    def value(self, *args):
        return 0
