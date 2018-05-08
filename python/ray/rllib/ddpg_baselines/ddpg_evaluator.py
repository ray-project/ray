from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import ray
import pickle
import tensorflow as tf
from gym.spaces import Discrete
from ray.rllib.utils.filter import get_filter
from ray.rllib.ddpg_baselines import models
from ray.rllib.models import ModelCatalog
from ray.rllib.optimizers import SampleBatch, PolicyEvaluator
from ray.rllib.utils.error import UnsupportedSpaceException
from ray.rllib.utils.noise import OUNoise


class DDPGEvaluator(PolicyEvaluator):

    def __init__(self, registry, env_creator, config, logdir, worker_index):
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
        self.ddpg_graph = models.DDPGGraph(registry, env, config, logdir)

        # Initialize the parameters and copy them to the target network.
        self.sess.run(tf.global_variables_initializer())
        self.ddpg_graph.copy_target(self.sess)
        self.global_timestep = 0
        self.local_timestep = 0
        nb_actions = env.action_space.shape[-1]
        stddev = config["exploration_noise"]
        self.exploration_noise = OUNoise(
            mu=np.zeros(nb_actions), sigma=float(stddev) * np.ones(nb_actions))
        self.action_range = (-1., 1.)

        # Note that this encompasses both the Q and target network
        self.variables = ray.experimental.TensorFlowVariables(tf.group(
            self.ddpg_graph.critic_loss, self.ddpg_graph.action_loss), self.sess)
        self.max_action = env.action_space.high
        self.episode_rewards = [0.0]
        self.episode_lengths = [0.0]
        self.saved_mean_reward = None

        # Technically not needed when not remote
        self.obs_filter = get_filter(
            config["observation_filter"], env.observation_space.shape)
        self.rew_filter = get_filter(config["reward_filter"], ())
        self.filters = {"obs_filter": self.obs_filter,
                        "rew_filter": self.rew_filter}

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
            "new_obs": new_obs, "dones": dones, "weights": np.ones_like(rewards)})
        assert batch.count == self.config["sample_batch_size"]

        # Prioritize on the worker side
        if self.config["worker_side_prioritization"]:
            td_errors = self.ddpg_graph.compute_td_error(
                self.sess, obs, batch["actions"], batch["rewards"], new_obs,
                batch["dones"], batch["weights"])
            new_priorities = (
                np.abs(td_errors) + self.config["prioritized_replay_eps"])
            batch.data["weights"] = new_priorities
        return batch

    def compute_gradients(self, samples):
        grad = self.ddpg_graph.compute_gradients(
            self.sess, samples["obs"], samples["rewards"],
            samples["new_obs"], samples["dones"], samples["weights"])
        return grad, {}

    def apply_gradients(self, grads):
        self.ddpg_graph.apply_gradients(self.sess, grads)

    def compute_apply(self, samples):
        td_error = self.ddpg_graph.compute_apply(
            self.sess, samples["obs"], samples["actions"], samples["rewards"],
            samples["new_obs"], samples["dones"], samples["weights"])
        return {"td_error": td_error}

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
        filters = self.get_filters(flush_after=True)
        weights = self.get_weights()
        return pickle.dumps({
            "filters": filters,
            "weights": weights})

    def restore(self, objs):
        objs = pickle.loads(objs)
        self.sync_filters(objs["filters"])
        self.set_weights(objs["weights"])

    def sync_filters(self, new_filters):
        """Changes self's filter to given and rebases any accumulated delta.

        Args:
            new_filters (dict): Filters with new state to update local copy.
        """
        assert all(k in new_filters for k in self.filters)
        for k in self.filters:
            self.filters[k].sync(new_filters[k])

    def get_filters(self, flush_after=False):
        """Returns a snapshot of filters.

        Args:
            flush_after (bool): Clears the filter buffer state.

        Returns:
             return_filters (dict): Dict for serializable filters
        """
        return_filters = {}
        for k, f in self.filters.items():
            return_filters[k] = f.as_serializable()
            if flush_after:
                f.clear_buffer()
        return return_filters
