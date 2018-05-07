from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pickle
import os

import numpy as np

import ray
from ray.rllib.ddpg_baselines.ddpg_evaluator import DDPGEvaluator
from ray.rllib import optimizers
from ray.rllib.agent import Agent
from ray.tune.result import TrainingResult

DEFAULT_CONFIG = dict(
    # === Model ===

    # Config options to pass to the model constructor
    model={},
    # Discount factor for the MDP
    gamma=0.99,
    # Arguments to pass to the env creator
    env_config={},

    # === Exploration ===
    # Max num timesteps for annealing schedules. Exploration is annealed from
    # 1.0 to exploration_fraction over this number of timesteps scaled by
    # exploration_fraction
    schedule_max_timesteps=100000,
    # Number of env steps to optimize for before returning
    timesteps_per_iteration=1000,
    # OUnoise : sigma=float(exploration_noise) * np.ones(nb_actions)
    exploration_noise=0.2,
    # Whether to adapt noise (maybe add param_noise in the future)
    action_noise=True,
    # Which observation filter to apply to the observation
    observation_filter="NoFilter",
    # Which reward filter to apply to the reward
    reward_filter="NoFilter",
    # === Replay buffer ===
    # Size of the replay buffer. Note that if async_updates is set, then
    # each worker will have a replay buffer of this size.
    buffer_size=50000,

    # === Optimization ===
    # soft update
    tau=0.001,
    # Actor learning rate
    actor_lr=0.001,
    # Critic learning rate
    critic_lr=0.001,
    # How many steps of the model to sample before learning starts.
    learning_starts=1000,
    # Update the replay buffer with this many samples at once. Note that
    # this setting applies per-worker if num_workers > 1.
    sample_batch_size=4,
    # Size of a batched sampled from replay buffer for training. Note that
    # if async_updates is set, then each worker returns gradients for a
    # batch of this size.
    train_batch_size=32,
    # Smooth the current average reward over this many previous episodes.
    smoothing_num_episodes=100,

    # === Tensorflow ===
    # Arguments to pass to tensorflow
    tf_session_args={
        "device_count": {"CPU": 2},
        "log_device_placement": False,
        "allow_soft_placement": True,
        "inter_op_parallelism_threads": 1,
        "intra_op_parallelism_threads": 1,
    },

    # === Parallelism ===
    # Number of workers for collecting samples with. This only makes sense
    # to increase if your environment is particularly slow to sample, or if
    # you're using the Ape-X optimizer.
    num_workers=0,
    # Whether to allocate GPUs for workers (if > 0).
    num_gpus_per_worker=0,
    # Optimizer class to use.
    optimizer_class="LocalSyncOptimizer",
    # Config to pass to the optimizer.
    optimizer_config=dict())


class DDPGAgent(Agent):
    _agent_name = "DDPG"
    _allow_unknown_subkeys = [
        "model", "optimizer", "tf_session_args", "env_config"]
    _default_config = DEFAULT_CONFIG

    def _init(self):
        self.local_evaluator = DDPGEvaluator(
            self.registry, self.env_creator, self.config, self.logdir, 0)
        remote_cls = ray.remote(
            num_cpus=1, num_gpus=self.config["num_gpus_per_worker"])(
            DDPGEvaluator)
        self.remote_evaluators = [
            remote_cls.remote(
                self.registry, self.env_creator, self.config, self.logdir,
                i)
            for i in range(self.config["num_workers"])]

        self.optimizer = getattr(optimizers, self.config["optimizer_class"])(
            self.config["optimizer_config"], self.local_evaluator,
            self.remote_evaluators)

        self.last_target_update_ts = 0
        self.num_target_updates = 0

    @property
    def global_timestep(self):
        return self.optimizer.num_steps_sampled

    def _train(self):
        start_timestep = self.global_timestep

        self.optimizer.step()
        self.local_evaluator.update_target()
        self.last_target_update_ts = self.global_timestep
        self.num_target_updates += 1

        self.local_evaluator.set_global_timestep(self.global_timestep)
        for e in self.remote_evaluators:
            e.set_global_timestep.remote(self.global_timestep)

        mean_100ep_reward = 0.0
        mean_100ep_length = 0.0
        num_episodes = 0

        if self.remote_evaluators:
            stats = ray.get([
                e.stats.remote() for e in self.remote_evaluators])
        else:
            stats = self.local_evaluator.stats()
            if not isinstance(stats, list):
                stats = [stats]

        test_stats = stats

        for s in test_stats:
            mean_100ep_reward += s["mean_100ep_reward"] / len(test_stats)
            mean_100ep_length += s["mean_100ep_length"] / len(test_stats)

        for s in stats:
            num_episodes += s["num_episodes"]

        result = TrainingResult(
            episode_reward_mean=mean_100ep_reward,
            episode_len_mean=mean_100ep_length,
            episodes_total=num_episodes,
            timesteps_this_iter=self.global_timestep - start_timestep,
            info={})

        return result

    def _stop(self):
        # workaround for https://github.com/ray-project/ray/issues/1516
        for ev in self.remote_evaluators:
            ev.__ray_terminate__.remote(ev._ray_actor_id.id())

    def _save(self, checkpoint_dir):
        checkpoint_path = os.path.join(
            checkpoint_dir, "checkpoint-{}".format(self.iteration))
        agent_state = ray.get(
            [a.save.remote() for a in self.remote_evaluators])
        extra_data = {
            "remote_state": agent_state,
            "local_state": self.local_evaluator.save()}
        pickle.dump(extra_data, open(checkpoint_path + ".extra_data", "wb"))
        return checkpoint_path

    def _restore(self, checkpoint_path):
        extra_data = pickle.load(open(checkpoint_path + ".extra_data", "rb"))
        ray.get(
            [a.restore.remote(o) for a, o in zip(
                self.remote_evaluators, extra_data["remote_state"])])
        self.local_evaluator.restore(extra_data["local_state"])

    def compute_action(self, observation):
        return self.local_evaluator.ddpg_graph.act(
            self.local_evaluator.sess, np.array(observation)[None], 0.0)[0]
