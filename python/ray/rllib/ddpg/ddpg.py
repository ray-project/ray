# imports
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
from ray.rllib.optimizers import LocalSyncReplayOptimizer, LocalSyncOptimizer
from ray.rllib.agent import Agent
from ray.rllib.ddpg.ddpg_evaluator import DDPGEvaluator, RemoteDDPGEvaluator
from ray.tune.result import TrainingResult
import numpy as np

DEFAULT_CONFIG = {
    "actor_model": {"fcnet_activation": "relu", "fcnet_hiddens": [64, 64]},
    "critic_model": {"fcnet_activation": "relu", "fcnet_hiddens": [64, 64]},
    "env_config": {},
    "gamma": 0.99,
    "horizon": 500,
    "actor_lr": 0.0001,
    "critic_lr": 0.001,
    "num_local_steps": 1,
    "num_workers": 0,

    "optimizer": {
        "buffer_size": 10000,
        "learning_starts": 500,
        "clip_rewards": False,
        "prioritized_replay": False,
        "train_batch_size": 64,
    },

    "parameter_noise": False,
    "parameter_epsilon": 0.0002, # linear decay of exploration policy
    "smoothing_num_episodes": 100,
    "tau": 0.001,
}

class DDPGAgent(Agent):
    _agent_name = "DDPG"
    _default_config = DEFAULT_CONFIG

    def _init(self):
        self.local_evaluator = DDPGEvaluator(
            self.registry, self.env_creator, self.config)
        self.remote_evaluators = [
            RemoteDDPGEvaluator.remote(
                self.registry, self.env_creator, self.config)
            for _ in range(self.config["num_workers"])]
        self.optimizer = LocalSyncReplayOptimizer(
            self.config["optimizer"], self.local_evaluator,
            self.remote_evaluators)

    def _train(self):
        self.optimizer.step()
        # update target
        if self.optimizer.num_steps_trained > 0:
            self.local_evaluator.update_target()

        # generate training result
        stats = self.local_evaluator.stats()
        if not isinstance(stats, list):
            stats = [stats]

        mean_100ep_reward = 0.0
        mean_100ep_length = 0.0
        num_episodes = 0

        for s in stats:
            mean_100ep_reward += s["mean_100ep_reward"] / len(stats)
            mean_100ep_length += s["mean_100ep_length"] / len(stats)
            num_episodes += s["num_episodes"]

        result = TrainingResult(
            episode_reward_mean=mean_100ep_reward,
            episode_len_mean=mean_100ep_length,
            episodes_total=num_episodes,
            timesteps_this_iter=1,
            info = {}
            )

        return result
