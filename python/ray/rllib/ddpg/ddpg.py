from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.agent import Agent
from ray.rllib.ddpg.ddpg_evaluator import DDPGEvaluator, RemoteDDPGEvaluator
from ray.rllib.optimizers import LocalSyncReplayOptimizer
from ray.tune.result import TrainingResult

DEFAULT_CONFIG = {
    # Actor learning rate
    "actor_lr": 0.0001,
    # Critic learning rate
    "critic_lr": 0.001,
    # Arguments to pass in to env creator
    "env_config": {},
    # MDP Discount factor
    "gamma": 0.99,
    # Number of steps after which the rollout gets cut
    "horizon": 500,

    # Number of local steps taken for each call to sample
    "num_local_steps": 1,
    # Number of workers (excluding master)
    "num_workers": 0,

    "optimizer": {
        # Replay buffer size
        "buffer_size": 10000,
        # Number of steps in warm-up phase before learning starts
        "learning_starts": 500,
        # Whether to clip rewards
        "clip_rewards": False,
        # Whether to use prioritized replay
        "prioritized_replay": False,
        # Size of batch sampled from replay buffer
        "train_batch_size": 64,
    },

    # Whether to include parameter noise
    "add_noise": True,
    # Parameters for noise process
    "noise_parameters": {
        "mu": 0,
        "sigma": 0.2,
        "theta": 0.15,
    },
    # Linear decay of exploration policy
    "parameter_epsilon": 0.0002,

    "smoothing_num_episodes": 10,
    # Controls how fast target networks move
    "tau": 0.001,
    # Number of steps taken per training iteration
    "train_steps": 500,
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
        # TODO(rliaw): multiple steps
        self.optimizer.step()
        # update target
        if self.optimizer.num_steps_trained > 0:
            self.local_evaluator.update_target()

        # generate training result
        stats = self.local_evaluator.stats()
        if not isinstance(stats, list):
            stats = [stats]

        mean_10ep_reward = 0.0
        mean_10ep_length = 0.0
        num_episodes = 0

        for s in stats:
            mean_10ep_reward += s["mean_10ep_reward"] / len(stats)
            mean_10ep_length += s["mean_10ep_length"] / len(stats)
            num_episodes += s["num_episodes"]

        result = TrainingResult(
            episode_reward_mean=mean_10ep_reward,
            episode_len_mean=mean_10ep_length,
            episodes_total=num_episodes,
            timesteps_this_iter=1,
            info={}
            )

        return result
