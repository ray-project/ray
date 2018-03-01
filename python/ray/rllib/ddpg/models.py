from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from copy import deepcopy
import ray
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.fcnet import FullyConnectedNetwork

import tensorflow as tf

class DDPGModel():
    other_output = []
    is_recurrent = False

    def __init__(self, registry, env, config):
        # Actor: given a state, makes a deterministic choice for what action
        # we should take.
        # Critic: estimates Q(s,a)
        self.env = env
        self.registry = registry
        self.config = config

        self.critic = DDPGCritic(self.registry, self.env, self.config)
        self.actor = DDPGActor(self.registry, self.env, self.config, self.critic.critic_eval)

        # TODO: create target networks, maybe just copy variables
        #self.target_actor = deepcopy(self.actor)
        #self.target_critic = deepcopy(self.critic)

        #self._setup_target_updates()
        self.initialize()

        self.critic_vars = ray.experimental.TensorFlowVariables(self.critic.critic_loss, self.sess)
        self.actor_vars = ray.experimental.TensorFlowVariables(self.actor.actor_loss, self.sess)
        self.setup_gradients()

    def initialize(self):
        self.sess = tf.Session()
        self.variables = ray.experimental.TensorFlowVariables(
                            tf.group(self.critic.critic_loss, self.actor.actor_loss), self.sess)
        self.sess.run(tf.global_variables_initializer())

    def _setup_target_updates(self):
        """Set up actor and critic updates."""
        a_updates = []
        for var, target_var in zip(self.actor.var_list, self.target_actor.var_list):
            a_updates.append(tf.assign(self.target_actor.var_list,
                    (1. - self.config["tau"]) * self.target_actor.var_list
                    + self.config["tau"] * self.actor.var_list))
        actor_updates = tf.group(*a_updates)

        c_updates = []
        for var, target_var in zip(self.critic.var_list, self.target_critic.var_list):
            c_updates.append(tf.assign(self.target_critic.var_list,
                    (1. - self.config["tau"]) * self.target_critic.var_list
                    + self.config["tau"] * self.critic.var_list))
        critic_updates = tf.group(*c_updates)
        self.target_updates = [actor_updates, critic_updates]

    def update_target(self):
        # update target critic and target actor
        self.sess.run(self.target_updates)

    def setup_gradients(self):
        # setup critic gradients
        self.critic._setup_gradients()
        # setup actor gradients
        self.actor._setup_gradients()

    def compute_gradients(self, samples):
        # critic gradients
        critic_grads = self.critic.compute_gradients(samples, self.sess)
        # actor gradients
        actor_grads = self.actor.compute_gradients(samples, self.sess)
        return critic_grads, actor_grads

    def apply_gradients(self, grads):
        """Applies gradients computed by compute_gradients."""
        critic_grads, actor_grads = grads
        self.critic.apply_gradients(critic_grads, self.sess)
        self.actor.apply_gradients(actor_grads, self.sess)

    def get_weights(self):
        # returns critic weights, actor weights
        return self.critic_vars.get_weights(), self.actor_vars.get_weights()

    def set_weights(self, weights):
        critic_weights, actor_weights = weights
        self.critic_vars.set_weights(critic_weights)
        self.actor_vars.set_weights(actor_weights)

    def compute(self, ob):
        # returns action, given state; this method is needed for sampler
        # calls the method in DDPGActor
        return self.actor.act(ob, self.sess)[0], {}

class DDPGCritic():
    # Critic: tries to estimate Q(s,a)
    def __init__(self, registry, env, config):
        self.config = config
        self.registry = registry
        self.env = env
        self.name = 'DDPGCritic'
        optimizer = tf.train.AdamOptimizer(learning_rate=config["critic_lr"])
        with tf.variable_scope("critic"):
            self._setup_network(env.observation_space, env.action_space)
        self._setup_critic_loss(env.action_space)
        self.var_list = tf.get_collection(tf.GraphKeys.TRAINABLE_VARIABLES,
                                                  tf.get_variable_scope().name)

    def _setup_critic_loss(self, action_space):
        # y_i = r_i + gamma * Q'(si+1, mu'(si+1))
        # shouldn't be running anything here

        # what the target Q network gives us
        self.target_Q = tf.placeholder(tf.float32, [None], name="target_q")

        # compare critic eval to critic_target (squared loss)
        self.reward = tf.placeholder(tf.float32, [None], name="reward")
        self.critic_target = self.reward + self.config['gamma'] * self.target_Q
        self.critic_loss = tf.reduce_mean(tf.square(self.critic_target - self.critic_eval))

    def _setup_network(self, obs_space, ac_space):
        """Sets up Q network."""

        # In DDPG Paper, actions are not
        # utilized until the second hidden layer
        #self.critic_model = ModelCatalog.get_model(
        #                self.registry, self.x, 1,
        #                options=self.config["critic_model"])

        # Fix later; apparently tf.reduce_prod wasn't working
        import numpy as np
        obs_size = np.prod(obs_space.shape)
        ac_size = np.prod(ac_space.shape)

        # placeholder concatenating the obs and action
        self.obs_and_action = tf.placeholder(tf.float32, [None, obs_size + ac_size])
        self.critic_network = FullyConnectedNetwork(self.obs_and_action,
                                                    1, self.config["critic_model"])
        self.critic_eval = self.critic_network.outputs

    def _setup_gradients(self):
        self.grads = tf.gradients(self.critic_loss, self.var_list)
        grads_and_vars = list(zip(self.grads, self.var_list))
        opt = tf.train.AdamOptimizer(self.config["critic_lr"])
        self._apply_gradients = opt.apply_gradients(grads_and_vars)

    def compute_gradients(self, samples):
        # take samples, concatenate state/action here
        print (samples)
        feed_dict = {
            self.obs_and_action: None, # TODO: some processing of samples
            # why not just tf.concat(obs, action)
            # advantages?
        }
        self.grads = [g for g in self.grads if g is not None]
        grad = sess.run(self.grads, feed_dict=feed_dict)
        return grad

    def apply_gradients(self, grads):
        feed_dict = dict(zip(self.grads, grads))
        sess.run(self._apply_gradients, feed_dict=feed_dict)

class DDPGActor():
    # Deterministically chooses an action, given state
    def __init__(self, registry, env, config, critic_eval):
        self.registry = registry
        self.env = env
        self.config = config
        self.name = 'DDPGActor'

        self._setup_network(env.observation_space, env.action_space)
        self._setup_actor_loss(critic_eval)

    # for target actor, feed s_{i+1}'s in
    def act(self, ob, sess): # returns action, given state
        action = sess.run(self.output_action, {self.x: [ob]})
        return action

    # Fix actor network; how do the network and loss interact?
    def _setup_network(self, ob_space, ac_space):
        self.x = tf.placeholder(tf.float32, shape=[None]+list(ob_space.shape))
        dist_class, self.action_dim = ModelCatalog.get_action_dist(ac_space,
                                     dist_type = 'deterministic')
        # 1 means one output
        self.model = ModelCatalog.get_model(
                        self.registry, self.x, self.action_dim,
                        options=self.config["actor_model"])
        self.dist = dist_class(self.model.outputs) # deterministic
        self.output_action = self.dist.sample()
        self.var_list = tf.get_collection(tf.GraphKeys.TRAINABLE_VARIABLES,
                                          tf.get_variable_scope().name)

    def _setup_actor_loss(self, critic_eval):
        # takes in output of the critic
        #self.critic_Q = tf.placeholder(tf.float32, [None], name="critic_Q")
        self.actor_loss = -tf.reduce_mean(critic_eval)

    def _setup_gradients(self):
        self.grads = tf.gradients(self.actor_loss, self.var_list)
        grads_and_vars = list(zip(self.grads, self.var_list))
        opt = tf.train.AdamOptimizer(self.config["actor_lr"])
        self._apply_gradients = opt.apply_gradients(grads_and_vars)

    def compute_gradients(self, samples, sess):
        feed_dict = {
            self.x: samples["observations"],
            self.output_action: samples["actions"]# actions
        }
        self.grads = [g for g in self.grads if g is not None]
        grad = sess.run(self.grads, feed_dict=feed_dict)
        return grad

    def apply_gradients(self, grads, sess):
        feed_dict = dict(zip(self.grads, grads))
        sess.run(self._apply_gradients, feed_dict=feed_dict)
