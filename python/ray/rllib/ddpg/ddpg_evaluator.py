# imports
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
from ray.rllib.ddpg.models import DDPGModel
from ray.rllib.optimizers.replay_buffer import ReplayBuffer
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.optimizers import SampleBatch
from ray.rllib.optimizers import PolicyEvaluator
from ray.rllib.utils.filter import NoFilter, MeanStdFilter
from ray.rllib.utils.process_rollout import process_rollout
from ray.rllib.utils.sampler import SyncSampler
from ray.tune.registry import get_registry


import numpy as np
import tensorflow as tf


class DDPGEvaluator(PolicyEvaluator):


    def __init__(self, registry, env_creator, config):
        self.registry = registry
        self.env = ModelCatalog.get_preprocessor_as_wrapper(
            registry, env_creator(config["env_config"]), config["actor_model"])
        self.config = config

        self.sess = tf.Session()

        with tf.variable_scope("model"):
            self.model = DDPGModel(self.registry, self.env, self.config, self.sess)
        with tf.variable_scope("target_model"):
            self.target_model = DDPGModel(self.registry, self.env, self.config, self.sess)

        self.setup_gradients()
        self._setup_target_updates()

        self.initialize()

        # set initial target weights to match model weights
        a_updates = []
        for var, target_var in zip(self.model.actor_var_list, self.target_model.actor_var_list):
            a_updates.append(tf.assign(target_var, var))
        actor_updates = tf.group(*a_updates)

        c_updates = []
        for var, target_var in zip(self.model.critic_var_list, self.target_model.critic_var_list):
            c_updates.append(tf.assign(target_var, var))
        critic_updates = tf.group(*c_updates)
        self.sess.run([actor_updates, critic_updates])

        self.sampler = SyncSampler(
                        self.env, self.model, NoFilter(),
                        config["num_local_steps"], horizon=config["horizon"])

        self.episode_rewards = [0.0]
        self.episode_lengths = [0.0]

    def initialize(self):
        self.sess.run(tf.global_variables_initializer())

    def sample(self):
        """Returns a batch of samples."""

        rollout = self.sampler.get_data()
        rollout.data["weights"] = np.ones_like(rollout.data["rewards"])

        self.episode_rewards[-1] += rollout.data["rewards"][0]
        self.episode_lengths[-1] += 1
        if rollout.data["dones"][0]:
            self.episode_rewards.append(0.0)
            self.episode_lengths.append(0.0)

        samples = process_rollout(
                    rollout, NoFilter(),
                    gamma=1.0, use_gae=False)

        return samples

    def stats(self):
        n = self.config["smoothing_num_episodes"] + 1
        mean_100ep_reward = round(np.mean(self.episode_rewards[-n:-1]), 5)
        mean_100ep_length = round(np.mean(self.episode_lengths[-n:-1]), 5)
        return {
            "mean_100ep_reward": mean_100ep_reward,
            "mean_100ep_length": mean_100ep_length,
            "num_episodes": len(self.episode_rewards),
        }

    def _setup_target_updates(self):
        """Set up target actor and critic updates."""
        a_updates = []
        for var, target_var in zip(self.model.actor_var_list, self.target_model.actor_var_list):
            a_updates.append(tf.assign(target_var,
                    (1. - self.config["tau"]) * target_var
                    + self.config["tau"] * var))
        actor_updates = tf.group(*a_updates)

        c_updates = []
        for var, target_var in zip(self.model.critic_var_list, self.target_model.critic_var_list):
            c_updates.append(tf.assign(target_var,
                    (1. - self.config["tau"]) * target_var
                    + self.config["tau"] * var))
        critic_updates = tf.group(*c_updates)
        self.target_updates = [actor_updates, critic_updates]

    def update_target(self):
        """Updates target critic and target actor."""
        self.sess.run(self.target_updates)

    def setup_gradients(self):
        """Setups critic and actor gradients."""
        self.critic_grads = tf.gradients(self.model.critic_loss, self.model.critic_var_list)
        c_grads_and_vars = list(zip(self.critic_grads, self.model.critic_var_list))
        c_opt = tf.train.AdamOptimizer(self.config["critic_lr"])
        self._apply_c_gradients = c_opt.apply_gradients(c_grads_and_vars)

        self.actor_grads = tf.gradients(self.model.actor_loss, self.model.actor_var_list)
        a_grads_and_vars = list(zip(self.actor_grads, self.model.actor_var_list))
        a_opt = tf.train.AdamOptimizer(self.config["actor_lr"])
        self._apply_a_gradients = a_opt.apply_gradients(a_grads_and_vars)

    def compute_gradients(self, samples):
        """ Returns gradient w.r.t. samples."""
        # actor gradients
        actor_actions = self.sess.run(self.model.output_action,
                                        feed_dict = {self.model.obs: samples["obs"]})

        actor_feed_dict = {
            self.model.obs: samples["obs"],
            self.model.output_action: actor_actions,
        }
        self.actor_grads = [g for g in self.actor_grads if g is not None]
        actor_grad = self.sess.run(self.actor_grads, feed_dict=actor_feed_dict)

        # feed samples into target actor
        target_Q_act = self.sess.run(self.target_model.output_action,
                                    feed_dict={
                                        self.target_model.obs: samples["new_obs"]
                                    })
        target_Q_dict = {
            self.target_model.obs: samples["new_obs"],
            self.target_model.act: target_Q_act,
        }

        target_Q = self.sess.run(self.target_model.critic_eval, feed_dict = target_Q_dict)

        # critic gradients
        critic_feed_dict = {
            self.model.obs: samples["obs"],
            self.model.act: samples["actions"],
            self.model.reward: samples["rewards"],
            self.model.target_Q: target_Q,
        }
        self.critic_grads = [g for g in self.critic_grads if g is not None]
        critic_grad = self.sess.run(self.critic_grads, feed_dict=critic_feed_dict)
        #import ipdb; ipdb.set_trace()
        return (critic_grad, actor_grad), {}

    def apply_gradients(self, grads):
        """Applies gradients to evaluator weights."""
        c_grads, a_grads = grads
        critic_feed_dict = dict(zip(self.critic_grads, c_grads))
        self.sess.run(self._apply_c_gradients, feed_dict=critic_feed_dict)
        actor_feed_dict = dict(zip(self.actor_grads, a_grads))
        self.sess.run(self._apply_a_gradients, feed_dict=actor_feed_dict)

    def compute_apply(self, samples):
        grads, _ = self.compute_gradients(samples)
        self.apply_gradients(grads)

    def get_weights(self):
        """Returns model weights."""
        return self.model.get_weights()

    def set_weights(self, weights):
        """Sets model weights."""
        self.model.set_weights(weights)

    def get_completed_rollout_metrics(self):
        """Returns metrics on previously completed rollouts.

        Calling this clears the queue of completed rollout metrics.
        """
        return self.sampler.get_metrics()

RemoteDDPGEvaluator = ray.remote(DDPGEvaluator)
