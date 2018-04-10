from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pickle
import os

import numpy as np
import tensorflow as tf

import ray
from ray.rllib.ddpg.ddpg_evaluator import DDPGEvaluator
from ray.rllib import optimizers
from ray.rllib.utils.actors import split_colocated
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

    exploration_noise=0.2,

    action_noise=True,
    # === Replay buffer ===
    # Size of the replay buffer. Note that if async_updates is set, then
    # each worker will have a replay buffer of this size.
    buffer_size=50000,

    # === Optimization ===
    #
    tau=0.001,
    # Learning rate for adam optimizer
    lr=5e-4,
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
    optimizer_config=dict(),
    # Whether to use a distribution of epsilons across workers for exploration.
    per_worker_exploration=False,
    # Whether to compute priorities on workers.
    worker_side_prioritization=False,
    # Whether to force evaluator actors to be placed on remote machines.
    force_evaluators_remote=False)


class DDPGAgent(Agent):
    _agent_name = "DDPG"
    _allow_unknown_subkeys = [
        "model", "optimizer", "tf_session_args", "env_config"]
    _default_config = DEFAULT_CONFIG

    def _init(self):
        self.local_evaluator = DDPGEvaluator(
            self.registry, self.env_creator, self.config, 0)
        remote_cls = ray.remote(
            num_cpus=1, num_gpus=self.config["num_gpus_per_worker"])(
            DDPGEvaluator)
        self.remote_evaluators = [
            remote_cls.remote(
                self.registry, self.env_creator, self.config, self.logdir,
                i)
            for i in range(self.config["num_workers"])]

        if self.config["force_evaluators_remote"]:
            _, self.remote_evaluators = split_colocated(
                self.remote_evaluators)

        self.optimizer = getattr(optimizers, self.config["optimizer_class"])(
            self.config["optimizer_config"], self.local_evaluator,
            self.remote_evaluators)

        self.saver = tf.train.Saver(max_to_keep=None)
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

    def _populate_replay_buffer(self):
        if self.remote_evaluators:
            for e in self.remote_evaluators:
                e.sample.remote(no_replay=True)
        else:
            self.local_evaluator.sample(no_replay=True)

    def _stop(self):
        # workaround for https://github.com/ray-project/ray/issues/1516
        for ev in self.remote_evaluators:
            ev.__ray_terminate__.remote(ev._ray_actor_id.id())

    def _save(self, checkpoint_dir):
        checkpoint_path = self.saver.save(
            self.local_evaluator.sess,
            os.path.join(checkpoint_dir, "checkpoint"),
            global_step=self.iteration)
        extra_data = [
            self.local_evaluator.save(),
            ray.get([e.save.remote() for e in self.remote_evaluators]),
            self.global_timestep,
            self.num_target_updates,
            self.last_target_update_ts]
        pickle.dump(extra_data, open(checkpoint_path + ".extra_data", "wb"))
        return checkpoint_path

    def _restore(self, checkpoint_path):
        self.saver.restore(self.local_evaluator.sess, checkpoint_path)
        extra_data = pickle.load(open(checkpoint_path + ".extra_data", "rb"))
        self.local_evaluator.restore(extra_data[0])
        ray.get([
            e.restore.remote(d) for (d, e)
            in zip(extra_data[1], self.remote_evaluators)])
        self.optimizer.restore(extra_data[2])
        self.num_target_updates = extra_data[3]
        self.last_target_update_ts = extra_data[4]

    def compute_action(self, observation):
        return self.local_evaluator.ddpg_graph.act(
            self.local_evaluator.sess, np.array(observation)[None], 0.0)[0]
