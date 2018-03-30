from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from copy import deepcopy
import ray
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.fcnet import FullyConnectedNetwork
from ray.rllib.ddpg.random_process import OrnsteinUhlenbeckProcess

import numpy as np
import tensorflow as tf


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

        obs_size = np.prod(obs_space.shape)
        self.obs = tf.placeholder(tf.float32, [None, obs_size])
        ac_size = np.prod(ac_space.shape)
        self.act = tf.placeholder(tf.float32, [None, ac_size])

        # set up actor network
        self._setup_actor_network(obs_space, ac_space)

        # setting up critic
        self._setup_critic_network(obs_space, ac_space)
        self._setup_critic_loss(ac_space)

        # setting up actor loss
        self._setup_actor_loss()

        with tf.variable_scope("critic"):
            self.critic_var_list = tf.get_collection(tf.GraphKeys.TRAINABLE_VARIABLES,
                                                    tf.get_variable_scope().name)

        with tf.variable_scope("actor"):
            self.actor_var_list = tf.get_collection(tf.GraphKeys.TRAINABLE_VARIABLES,
                                                    tf.get_variable_scope().name)

        #TODO: self.critic_vars currently also contains actor_vars; was only using this for getting/setting weights though
        #self.critic_vars = ray.experimental.TensorFlowVariables(self.critic_loss, self.sess)
        #self.actor_vars = ray.experimental.TensorFlowVariables(self.actor_loss, self.sess)

        if (self.config["parameter_noise"]):
            self.random_process = OrnsteinUhlenbeckProcess(size=1, theta=0.15, mu=0, sigma=0.2)
            self.epsilon = 1.0

    def _setup_critic_loss(self, action_space):
        # what the target Q network gives us
        self.target_Q = tf.placeholder(tf.float32, [None, 1], name="target_q")

        # compare critic eval to critic_target (squared loss)
        self.reward = tf.placeholder(tf.float32, [None], name="reward")
        self.critic_target = self.reward + self.config['gamma'] * self.target_Q
        self.critic_loss = tf.reduce_mean(tf.square(self.critic_target - self.critic_eval))

    def _setup_critic_network(self, obs_space, ac_space):
        """Sets up Q network."""

        self.obs_and_action = tf.concat([self.obs, self.act], 1)

        with tf.variable_scope("critic", reuse=tf.AUTO_REUSE):
            self.critic_network = FullyConnectedNetwork(self.obs_and_action,
                                                        1, self.config["critic_model"])
        self.critic_eval = self.critic_network.outputs

        self.obs_and_actor = tf.concat([self.obs, self.output_action], 1) #output_action is output of actor network
        with tf.variable_scope("critic", reuse=True):
            self.cn_for_loss = FullyConnectedNetwork(self.obs_and_actor,
                                                        1, self.config["critic_model"])

    def _setup_actor_network(self, obs_space, ac_space):
        dist_class, self.action_dim = ModelCatalog.get_action_dist(ac_space,
                                     dist_type = 'deterministic')
        with tf.variable_scope("actor", reuse=tf.AUTO_REUSE):
            self.actor_network = ModelCatalog.get_model(
                                    self.registry, self.obs, 1,
                                    options=self.config["actor_model"])
        self.output_action = self.actor_network.outputs

    def _setup_actor_loss(self):
        self.actor_loss = -tf.reduce_mean(self.cn_for_loss.outputs)

    def get_weights(self):
        """Returns critic weights, actor weights."""
        return self.critic_vars.get_weights(), self.actor_vars.get_weights()

    def set_weights(self, weights):
        """Sets critic and actor weights."""
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
