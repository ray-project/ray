# imports
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
from ray.rllib.ddpg.models import DDPGModel
from ray.rllib.ddpg.replay_buffer import ReplayBuffer, PrioritizedReplayBuffer
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.optimizers import SampleBatch
from ray.rllib.optimizers import Evaluator
from ray.rllib.utils.filter import NoFilter
from ray.rllib.utils.process_rollout import process_rollout
from ray.rllib.utils.sampler import SyncSampler
from ray.tune.registry import get_registry

import tensorflow as tf


class DDPGEvaluator(Evaluator):


    def __init__(self, registry, env_creator, config):
        self.registry = registry
        self.env = ModelCatalog.get_preprocessor_as_wrapper(
            registry, env_creator(config["env_config"]), config["actor_model"])
        self.config = config

        self.sess = tf.Session()

        self.model = DDPGModel(self.registry, self.env, self.config, self.sess)
        self.target_model = DDPGModel(self.registry, self.env, self.config, self.sess)

        self.initialize()
        self.setup_gradients()

        self.replay_buffer = ReplayBuffer(config["buffer_size"])
        self.sampler = SyncSampler(
                        self.env, self.model, NoFilter(),
                        config["batch_size"], horizon=config["horizon"])

    def initialize(self):
        self.sess.run(tf.global_variables_initializer())

    #TODO: (not critical) Add batch normalization?
    def sample(self, no_replay = True):
        """Returns a batch of samples."""
        # act in the environment, generate new samples
        rollout = self.sampler.get_data()
        samples = process_rollout(
                    rollout, NoFilter(),
                    gamma=self.config["gamma"], use_gae=False)

        # Add samples to replay buffer.
        for row in samples.rows():
            self.replay_buffer.add(row["observations"],
                                    row["actions"], row["rewards"],
                                    row["new_obs"], row['terminal'])

        #if no_replay:
        #    return SampleBatch.concat_samples(samples)

        # Then return a batch sampled from the buffer; copied from DQN
        obses_t, actions, rewards, obses_tp1, dones = \
            self.replay_buffer.sample(self.config["train_batch_size"])
        batch = SampleBatch({
            "obs": obses_t, "actions": actions, "rewards": rewards,
            "new_obs": obses_tp1, "dones": dones,
            })
        return batch

    #TODO: Update this
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

    #TODO: Update this
    def update_target(self):
        # update target critic and target actor
        self.sess.run(self.target_updates)

    def setup_gradients(self):
        # setup critic gradients
        self.critic_grads = tf.gradients(self.model.critic_loss, self.model.critic_var_list)
        c_grads_and_vars = list(zip(self.critic_grads, self.model.critic_var_list))
        #c_opt = tf.train.AdamOptimizer(self.config["critic_lr"])
        c_opt = tf.train.GradientDescentOptimizer(self.config["critic_lr"])
        self._apply_c_gradients = c_opt.apply_gradients(c_grads_and_vars)

        # setup actor gradients
        self.actor_grads = tf.gradients(self.model.actor_loss, self.model.actor_var_list)
        a_grads_and_vars = list(zip(self.actor_grads, self.model.actor_var_list))
        #a_opt = tf.train.AdamOptimizer(self.config["actor_lr"])
        a_opt = tf.train.GradientDescentOptimizer(self.config["actor_lr"])
        self._apply_a_gradients = a_opt.apply_gradients(a_grads_and_vars)

    def compute_gradients(self, samples):
        """ Returns gradient w.r.t. samples."""
        # actor gradients
        actor_feed_dict = {
            self.model.obs: samples["obs"],
            self.model.output_action: samples["actions"],
        }
        self.actor_grads = [g for g in self.actor_grads if g is not None]
        actor_grad = self.sess.run(self.actor_grads, feed_dict=actor_feed_dict)

        target_Q_dict = {
            self.model.obs: samples["obs"],
            self.model.act: samples["actions"]
        }

        target_Q = self.sess.run(self.model.cn_for_loss.outputs, feed_dict = target_Q_dict)

        # critic gradients
        critic_feed_dict = {
            self.model.obs: samples["obs"],
            self.model.act: samples["actions"],
            self.model.reward: samples["rewards"],
            self.model.target_Q: target_Q,
        }
        self.critic_grads = [g for g in self.critic_grads if g is not None]
        critic_grad = self.sess.run(self.critic_grads, feed_dict=critic_feed_dict)

        return critic_grad, actor_grad

    def apply_gradients(self, grads):
        """Applies gradients to evaluator weights."""
        c_grads, a_grads = grads
        critic_feed_dict = dict(zip(self.critic_grads, c_grads))
        self.sess.run(self._apply_c_gradients, feed_dict=critic_feed_dict)
        actor_feed_dict = dict(zip(self.actor_grads, a_grads))
        self.sess.run(self._apply_a_gradients, feed_dict=actor_feed_dict)

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

if __name__ == '__main__':
    ray.init()
    import gym
    config = ray.rllib.ddpg.DEFAULT_CONFIG.copy()
    DDPGEvaluator(get_registry(), lambda config: gym.make("CartPole-v0"),
        config)
