from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import pickle
import os

import ray
from ray.rllib.agent import Agent
from ray.rllib.envs import create_and_wrap
from ray.rllib.a3c.runner import RemoteRunner
from ray.rllib.a3c.common import get_policy_cls
from ray.rllib.utils.filter import get_filter
from ray.tune.result import TrainingResult


DEFAULT_CONFIG = {
    "num_workers": 4,
    "num_batches_per_iteration": 100,

    # Size of rollout batch
    "batch_size": 10,
    "use_lstm": True,
    "use_pytorch": False,
    # Which observation filter to apply to the observation
    "observation_filter": "NoFilter",
    # Which reward filter to apply to the reward
    "reward_filter": "NoFilter",

    "model": {"grayscale": True,
              "zero_mean": False,
              "dim": 42,
              "channel_major": False}
}


class A3CAgent(Agent):
    _agent_name = "A3C"
    _default_config = DEFAULT_CONFIG

    def _init(self):
        self.env = create_and_wrap(self.env_creator, self.config["model"])
        policy_cls = get_policy_cls(self.config)
        self.policy = policy_cls(
            self.env.observation_space.shape, self.env.action_space)
        self.obs_filter = get_filter(
            self.config["observation_filter"],
            self.env.observation_space.shape)
        self.rew_filter = get_filter(self.config["reward_filter"], ())
        self.agents = [
            RemoteRunner.remote(self.env_creator, self.config, self.logdir)
            for i in range(self.config["num_workers"])]
        self.parameters = self.policy.get_weights()

    def _train(self):
        remote_params = ray.put(self.parameters)
        ray.get([agent.set_weights.remote(remote_params)
                 for agent in self.agents])

        gradient_list = {agent.compute_gradient.remote(): agent
                         for agent in self.agents}
        max_batches = self.config["num_batches_per_iteration"]
        batches_so_far = len(gradient_list)
        while gradient_list:
            [done_id], _ = ray.wait(list(gradient_list))
            gradient, info = ray.get(done_id)
            agent = gradient_list[done_id]
            del gradient_list[done_id]
            self.obs_filter.update(info["obs_filter"])
            self.rew_filter.update(info["rew_filter"])
            self.policy.apply_gradients(gradient)
            self.parameters = self.policy.get_weights()

            if batches_so_far < max_batches:
                batches_so_far += 1
                agent.update_filters.remote(
                    obs_filter=self.obs_filter,
                    rew_filter=self.rew_filter)
                agent.set_weights.remote(self.parameters)
                gradient_list[agent.compute_gradient.remote()] = agent
        res = self._fetch_metrics_from_workers()
        return res

    def _fetch_metrics_from_workers(self):
        episode_rewards = []
        episode_lengths = []
        metric_lists = [
            a.get_completed_rollout_metrics.remote() for a in self.agents]
        for metrics in metric_lists:
            for episode in ray.get(metrics):
                episode_lengths.append(episode.episode_length)
                episode_rewards.append(episode.episode_reward)
        avg_reward = (
            np.mean(episode_rewards) if episode_rewards else float('nan'))
        avg_length = (
            np.mean(episode_lengths) if episode_lengths else float('nan'))
        timesteps = np.sum(episode_lengths) if episode_lengths else 0

        result = TrainingResult(
            episode_reward_mean=avg_reward,
            episode_len_mean=avg_length,
            timesteps_this_iter=timesteps,
            info={})

        return result

    def _save(self):
        checkpoint_path = os.path.join(
            self.logdir, "checkpoint-{}".format(self.iteration))
        objects = [self.parameters, self.obs_filter, self.rew_filter]
        pickle.dump(objects, open(checkpoint_path, "wb"))
        return checkpoint_path

    def _restore(self, checkpoint_path):
        objects = pickle.load(open(checkpoint_path, "rb"))
        self.parameters = objects[0]
        self.obs_filter = objects[1]
        self.rew_filter = objects[2]
        self.policy.set_weights(self.parameters)

    def compute_action(self, observation):
        actions = self.policy.compute_action(observation)
        return actions[0]
