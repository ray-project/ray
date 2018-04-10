from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import ray
import tensorflow as tf
from gym.spaces import Discrete
from ray.rllib.ddpg import models
from ray.rllib.models import ModelCatalog
from ray.rllib.optimizers import SampleBatch, PolicyEvaluator
from ray.rllib.utils.error import UnsupportedSpaceException
from ray.rllib.ddpg.ou_noise import OUNoise


class DDPGEvaluator(PolicyEvaluator):

    """The base DDPG Evaluator that does not include the replay buffer.

    TODO(rliaw): Support observation/reward filters?"""

    def __init__(self, registry, env_creator, config, worker_index):
        env = ModelCatalog.get_preprocessor_as_wrapper(
            registry, env_creator(config["env_config"]), config["model"])
        self.env = env
        self.config = config

        if isinstance(env.action_space, Discrete):
            raise UnsupportedSpaceException(
                "Action space {} is not supported for DDPG.".format(
                    env.action_space))

        tf_config = tf.ConfigProto(**config["tf_session_args"])
        self.sess = tf.Session(config=tf_config)
        self.ddpg_graph = models.DDPGGraph(registry, env, config)

        # Initialize the parameters and copy them to the target network.
        self.sess.run(tf.global_variables_initializer())
        self.ddpg_graph.copy_target(self.sess)
        self.global_timestep = 0
        self.local_timestep = 0
        nb_actions = env.action_space.shape[-1]
        stddev = config["exploration_noise"]
        self.exploration_noise = OUNoise(mu=np.zeros(nb_actions), sigma=float(stddev) * np.ones(nb_actions))
        self.action_range = (-1., 1.)

        # Note that this encompasses both the Q and target network
        self.variables = ray.experimental.TensorFlowVariables(
            tf.group(self.ddpg_graph.td_error, self.ddpg_graph.action_lost), self.sess)
        self.max_action = env.action_space.high
        self.episode_rewards = [0.0]
        self.episode_lengths = [0.0]
        self.saved_mean_reward = None

        self.obs = self.env.reset()

    def set_global_timestep(self, global_timestep):
        self.global_timestep = global_timestep

    def update_target(self):
        self.ddpg_graph.update_target(self.sess)

    def sample(self):
        obs, actions, rewards, new_obs, dones = [], [], [], [], []
        for _ in range(
                self.config["sample_batch_size"]):
            ob, act, rew, ob1, done = self._step(self.global_timestep)
            obs.append(ob)
            actions.append(act)
            rewards.append(rew)
            new_obs.append(ob1)
            dones.append(done)

        batch = SampleBatch({
            "obs": obs, "actions": actions, "rewards": rewards,
            "new_obs": new_obs, "dones": dones})
        assert batch.count == self.config["sample_batch_size"]
        return batch

    def compute_gradients(self, samples):
        grad = self.ddpg_graph.compute_gradients(
            self.sess, samples["obs"], samples["rewards"],
            samples["new_obs"], samples["dones"])
        return grad, {}

    def apply_gradients(self, grads):
        self.ddpg_graph.apply_gradients(self.sess, grads)

    def get_weights(self):
        return self.variables.get_weights()

    def set_weights(self, weights):
        self.variables.set_weights(weights)

    def tf_loss_inputs(self):
        return self.ddpg_graph.loss_inputs

    def _step(self, global_timestep):
        """Takes a single step, and returns the result of the step."""

        action = self.ddpg_graph.act(
            self.sess, np.array(self.obs)[None],
            global_timestep)[0]
        # add randomness to action selection for exploration
        if self.config["action_noise"]:
            noise = self.exploration_noise.noise()
            assert noise.shape == action.shape
            action = action + noise
            # print("acion: %f   noise: %f   " % (action, noise))
        new_obs, rew, done, _ = self.env.step(action)
        ret = (self.obs, action, rew, new_obs, float(done))
        self.obs = new_obs
        self.episode_rewards[-1] += rew
        self.episode_lengths[-1] += 1
        if done:
            self.obs = self.env.reset()
            self.episode_rewards.append(0.0)
            self.episode_lengths.append(0.0)
            # reinitializing random noise for action exploration
            if self.config["action_noise"]:
                self.exploration_noise.reset()

        self.local_timestep += 1
        return ret

    def stats(self):
        mean_100ep_reward = round(np.mean(self.episode_rewards[-101:-1]), 5)
        mean_100ep_length = round(np.mean(self.episode_lengths[-101:-1]), 5)
        return {
            "mean_100ep_reward": mean_100ep_reward,
            "mean_100ep_length": mean_100ep_length,
            "num_episodes": len(self.episode_rewards),
            "local_timestep": self.local_timestep,
        }

    def save(self):
        return [
            self.episode_rewards,
            self.episode_lengths,
            self.saved_mean_reward,
            self.obs,
            self.global_timestep,
            self.local_timestep]

    def restore(self, data):
        self.episode_rewards = data[1]
        self.episode_lengths = data[2]
        self.saved_mean_reward = data[3]
        self.obs = data[4]
        self.global_timestep = data[5]
        self.local_timestep = data[6]
