from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time

import numpy as np
import pickle
import os
import sys
import tensorflow as tf

import ray
from ray.rllib.dqn.base_evaluator import DQNEvaluator
from ray.rllib.dqn.replay_evaluator import DQNReplayEvaluator
from ray.rllib.optimizer import SyncLocalOptimizer
from ray.rllib.agent import Agent
from ray.rllib.ppo.filter import RunningStat
from ray.tune.result import TrainingResult


DEFAULT_CONFIG = dict(
    # === Model ===
    # Whether to use dueling dqn
    dueling=True,
    # Whether to use double dqn
    double_q=True,
    # Hidden layer sizes of the state and action value networks
    hiddens=[256],
    # Config options to pass to the model constructor
    model={},
    # Discount factor for the MDP
    gamma=0.99,

    # === Exploration ===
    # Max num timesteps for annealing schedules. Exploration is annealed from
    # 1.0 to exploration_fraction over this number of timesteps scaled by
    # exploration_fraction
    schedule_max_timesteps=100000,
    # Number of env steps to optimize for before returning
    timesteps_per_iteration=1000,
    # Fraction of entire training period over which the exploration rate is
    # annealed
    exploration_fraction=0.1,
    # Final value of random action probability
    exploration_final_eps=0.02,
    # How many steps of the model to sample before learning starts.
    learning_starts=1000,
    # Update the target network every `target_network_update_freq` steps.
    target_network_update_freq=500,

    # === Replay buffer ===
    # Size of the replay buffer. Note that if async_updates is set, then each
    # worker will have a replay buffer of this size.
    buffer_size=50000,
    # If True prioritized replay buffer will be used.
    prioritized_replay=True,
    # Alpha parameter for prioritized replay buffer
    prioritized_replay_alpha=0.6,
    # Initial value of beta for prioritized replay buffer
    prioritized_replay_beta0=0.4,
    # Number of iterations over which beta will be annealed from initial
    # value to 1.0. If set to None equals to schedule_max_timesteps
    prioritized_replay_beta_iters=None,
    # Epsilon to add to the TD errors when updating priorities.
    prioritized_replay_eps=1e-6,

    # === Optimization ===
    # Learning rate for adam optimizer
    lr=5e-4,
    # Update the replay buffer with this many samples at once. Note that this
    # setting applies per-worker if num_workers > 1.
    sample_batch_size=1,
    # Size of a batched sampled from replay buffer for training. Note that if
    # async_updates is set, then each worker returns gradients for a batch of
    # this size.
    train_batch_size=32,
    # SGD minibatch size. Note that this must be << train_batch_size. This
    # config has no effect if gradients_on_workres is True.
    sgd_batch_size=32,
    # If not None, clip gradients during optimization at this value
    grad_norm_clipping=10,

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
    # Number of workers for collecting samples with. Note that the typical
    # setting is 1 unless your environment is particularly slow to sample.
    num_workers=1,
    # Whether to allocate GPUs for workers (if num_workers > 1).
    use_gpu_for_workers=False,
    # (Experimental) Whether to update the model asynchronously from
    # workers. In this mode, gradients will be computed on workers instead of
    # on the driver, and workers will each have their own replay buffer.
    async_updates=False,
    # (Experimental) Whether to use multiple GPUs for SGD optimization.
    # Note that this only helps performance if the SGD batch size is large.
    multi_gpu_optimize=False,
    # Number of SGD iterations over the data. Only applies in multi-gpu mode.
    num_sgd_iter=1,
    # Devices to use for parallel SGD. Only applies in multi-gpu mode.
    devices=["/gpu:0"])


class DQNAgent(Agent):
    _agent_name = "DQN"
    _default_config = DEFAULT_CONFIG

    def stop(self):
        for w in self.workers:
            w.stop.remote()

    def _init(self):
        self.local_evaluator = DQNEvaluator(
            self.env_creator, self.config, self.logdir)
        self.remote_evaluators = [
            DQNReplayEvaluator(self.env_creator, self.config, self.logdir)]
        self.optimizer = Optimizer(
            self.local_evaluator, self.remote_evaluators)

        self.cur_timestep = 0
        self.num_target_updates = 0
        self.steps_since_update = 0
        self.saver = tf.train.Saver(max_to_keep=None)

    def _train(self):
        iter_init_timesteps = self.cur_timestep

        while (self.cur_timestep - iter_init_timesteps <
               config["timesteps_per_iteration"]):

            if self.cur_timestep < config["learning_starts"]:
                for e in self.remote_evaluators:
                    samples = e.sample()
                    print(len(samples))
                continue
        
        return

        config = self.config
        sample_time, sync_time, learn_time, apply_time = 0, 0, 0, 0
        iter_init_timesteps = self.cur_timestep

        num_loop_iters = 0
        while (self.cur_timestep - iter_init_timesteps <
               config["timesteps_per_iteration"]):
            dt = time.time()
            if self.workers:
                worker_steps = ray.get([
                    w.do_steps.remote(
                        config["sample_batch_size"] // len(self.workers),
                        self.cur_timestep, store=False)
                    for w in self.workers])
                for steps in worker_steps:
                    for obs, action, rew, new_obs, done in steps:
                        self.actor.replay_buffer.add(
                            obs, action, rew, new_obs, done)
            else:
                self.actor.do_steps(
                    config["sample_batch_size"], self.cur_timestep, store=True)
            num_loop_iters += 1
            self.cur_timestep += config["sample_batch_size"]
            self.steps_since_update += config["sample_batch_size"]
            sample_time += time.time() - dt

            if self.cur_timestep > config["learning_starts"]:
                if config["multi_gpu_optimize"]:
                    dt = time.time()
                    times = self.actor.do_multi_gpu_optimize(self.cur_timestep)
                    if num_loop_iters <= 1:
                        print("Multi-GPU times", times)
                    learn_time += (time.time() - dt)
                else:
                    # Minimize the error in Bellman's equation on a batch
                    # sampled from replay buffer.
                    for _ in range(
                            max(1, config["train_batch_size"] //
                                config["sgd_batch_size"])):
                        dt = time.time()
                        gradients = [
                            self.actor.sample_buffer_gradient(
                                self.cur_timestep)]
                        learn_time += (time.time() - dt)
                        dt = time.time()
                        for grad in gradients:
                            self.actor.apply_gradients(grad)
                        apply_time += (time.time() - dt)
                dt = time.time()
                self._update_worker_weights()
                sync_time += (time.time() - dt)

            if (self.cur_timestep > config["learning_starts"] and
                    self.steps_since_update >
                    config["target_network_update_freq"]):
                # Update target network periodically.
                self.actor.dqn_graph.update_target(self.actor.sess)
                self.steps_since_update -= config["target_network_update_freq"]
                self.num_target_updates += 1

        mean_100ep_reward = 0.0
        mean_100ep_length = 0.0
        num_episodes = 0
        buffer_size_sum = 0
        if not self.workers:
            stats = self.actor.stats(self.cur_timestep)
            mean_100ep_reward += stats[0]
            mean_100ep_length += stats[1]
            num_episodes += stats[2]
            exploration = stats[3]
            buffer_size_sum += stats[4]
        for mean_rew, mean_len, episodes, exploration, buf_sz in ray.get(
              [w.stats.remote(self.cur_timestep) for w in self.workers]):
            mean_100ep_reward += mean_rew
            mean_100ep_length += mean_len
            num_episodes += episodes
            buffer_size_sum += buf_sz
        mean_100ep_reward /= config["num_workers"]
        mean_100ep_length /= config["num_workers"]

        info = [
            ("mean_100ep_reward", mean_100ep_reward),
            ("exploration_frac", exploration),
            ("steps", self.cur_timestep),
            ("episodes", num_episodes),
            ("buffer_sizes_sum", buffer_size_sum),
            ("target_updates", self.num_target_updates),
            ("sample_time", sample_time),
            ("weight_sync_time", sync_time),
            ("apply_time", apply_time),
            ("learn_time", learn_time),
            ("samples_per_s",
                num_loop_iters * np.float64(config["sample_batch_size"]) /
                sample_time),
            ("learn_samples_per_s",
                num_loop_iters * np.float64(config["train_batch_size"]) /
                learn_time),
        ]

        for k, v in info:
            logger.record_tabular(k, v)
        logger.dump_tabular()

        result = TrainingResult(
            episode_reward_mean=mean_100ep_reward,
            episode_len_mean=mean_100ep_length,
            timesteps_this_iter=self.cur_timestep - iter_init_timesteps,
            info=info)

        return result

    def _save(self):
        checkpoint_path = self.saver.save(
            self.actor.sess,
            os.path.join(self.logdir, "checkpoint"),
            global_step=self.num_iterations)
        extra_data = [
            self.actor.save(),
            ray.get([w.save.remote() for w in self.workers]),
            self.cur_timestep,
            self.num_iterations,
            self.num_target_updates,
            self.steps_since_update]
        pickle.dump(extra_data, open(checkpoint_path + ".extra_data", "wb"))
        return checkpoint_path

    def _restore(self, checkpoint_path):
        self.saver.restore(self.actor.sess, checkpoint_path)
        extra_data = pickle.load(open(checkpoint_path + ".extra_data", "rb"))
        self.actor.restore(extra_data[0])
        ray.get([
            w.restore.remote(d) for (d, w)
            in zip(extra_data[1], self.workers)])
        self.cur_timestep = extra_data[2]
        self.num_iterations = extra_data[3]
        self.num_target_updates = extra_data[4]
        self.steps_since_update = extra_data[5]

    def compute_action(self, observation):
        return self.actor.dqn_graph.act(
            self.actor.sess, np.array(observation)[None], 0.0)[0]
