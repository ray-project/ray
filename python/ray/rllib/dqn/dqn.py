from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time

import gym
import numpy as np
import pickle
import os
import tensorflow as tf

from ray.rllib.common import Agent, TrainingResult
from ray.rllib.dqn import logger, models
from ray.rllib.dqn.common.atari_wrappers_deprecated \
    import wrap_dqn, ScaledFloatFrame
from ray.rllib.dqn.common.schedules import LinearSchedule
from ray.rllib.dqn.replay_buffer import ReplayBuffer, PrioritizedReplayBuffer


"""The default configuration dict for the DQN algorithm.

    dueling: bool
        whether to use dueling dqn
    double_q: bool
        whether to use double dqn
    hiddens: array<int>
        hidden layer sizes of the state and action value networks
    model: dict
        config options to pass to the model constructor
    lr: float
        learning rate for adam optimizer
    schedule_max_timesteps: int
        max num timesteps for annealing schedules
    timesteps_per_iteration: int
        number of env steps to optimize for before returning
    buffer_size: int
        size of the replay buffer
    exploration_fraction: float
        fraction of entire training period over which the exploration rate is
        annealed
    exploration_final_eps: float
        final value of random action probability
    train_freq: int
        update the model every `train_freq` steps.
    batch_size: int
        size of a batched sampled from replay buffer for training
    print_freq: int
        how often to print out training progress
        set to None to disable printing
    checkpoint_freq: int
        how often to save the model. This is so that the best version is
        restored at the end of the training. If you do not wish to restore
        the best version at the end of the training set this variable to None.
    learning_starts: int
        how many steps of the model to collect transitions for before learning
        starts
    gamma: float
        discount factor
    grad_norm_clipping: int or None
        if not None, clip gradients during optimization at this value
    target_network_update_freq: int
        update the target network every `target_network_update_freq` steps.
    prioritized_replay: True
        if True prioritized replay buffer will be used.
    prioritized_replay_alpha: float
        alpha parameter for prioritized replay buffer
    prioritized_replay_beta0: float
        initial value of beta for prioritized replay buffer
    prioritized_replay_beta_iters: int
        number of iterations over which beta will be annealed from initial
        value to 1.0. If set to None equals to schedule_max_timesteps
    prioritized_replay_eps: float
        epsilon to add to the TD errors when updating priorities.
    num_cpu: int
        number of cpus to use for training
"""
DEFAULT_CONFIG = dict(
    dueling=True,
    double_q=True,
    hiddens=[256],
    model={},
    lr=5e-4,
    schedule_max_timesteps=100000,
    timesteps_per_iteration=1000,
    buffer_size=50000,
    exploration_fraction=0.1,
    exploration_final_eps=0.02,
    train_freq=1,
    batch_size=32,
    print_freq=1,
    checkpoint_freq=10000,
    learning_starts=1000,
    gamma=1.0,
    grad_norm_clipping=10,
    target_network_update_freq=500,
    prioritized_replay=False,
    prioritized_replay_alpha=0.6,
    prioritized_replay_beta0=0.4,
    prioritized_replay_beta_iters=None,
    prioritized_replay_eps=1e-6,
    num_cpu=16)


class DQNAgent(Agent):
    def __init__(self, env_name, config, upload_dir=None):
        config.update({"alg": "DQN"})

        Agent.__init__(self, env_name, config, upload_dir=upload_dir)

        with tf.Graph().as_default():
            self._init()

    def _init(self):
        config = self.config
        env = gym.make(self.env_name)
        # TODO(ekl): replace this with RLlib preprocessors
        if "NoFrameskip" in self.env_name:
            env = ScaledFloatFrame(wrap_dqn(env))
        self.env = env

        num_cpu = config["num_cpu"]
        tf_config = tf.ConfigProto(
            inter_op_parallelism_threads=num_cpu,
            intra_op_parallelism_threads=num_cpu)
        self.sess = tf.Session(config=tf_config)
        self.dqn_graph = models.DQNGraph(env, config)

        # Create the replay buffer
        if config["prioritized_replay"]:
            self.replay_buffer = PrioritizedReplayBuffer(
                config["buffer_size"],
                alpha=config["prioritized_replay_alpha"])
            prioritized_replay_beta_iters = (
                config["prioritized_replay_beta_iters"])
            if prioritized_replay_beta_iters is None:
                prioritized_replay_beta_iters = (
                    config["schedule_max_timesteps"])
            self.beta_schedule = LinearSchedule(
                prioritized_replay_beta_iters,
                initial_p=config["prioritized_replay_beta0"],
                final_p=1.0)
        else:
            self.replay_buffer = ReplayBuffer(config["buffer_size"])
            self.beta_schedule = None
        # Create the schedule for exploration starting from 1.
        self.exploration = LinearSchedule(
            schedule_timesteps=int(
                config["exploration_fraction"] *
                config["schedule_max_timesteps"]),
            initial_p=1.0,
            final_p=config["exploration_final_eps"])

        # Initialize the parameters and copy them to the target network.
        self.sess.run(tf.global_variables_initializer())
        self.dqn_graph.update_target(self.sess)

        self.episode_rewards = [0.0]
        self.episode_lengths = [0.0]
        self.saved_mean_reward = None
        self.obs = self.env.reset()
        self.num_timesteps = 0
        self.num_iterations = 0
        self.file_writer = tf.summary.FileWriter(self.logdir, self.sess.graph)
        self.saver = tf.train.Saver(max_to_keep=None)

    def _train(self):
        config = self.config
        sample_time, learn_time = 0, 0
        iter_init_timesteps = self.num_timesteps

        for _ in range(config["timesteps_per_iteration"]):
            self.num_timesteps += 1
            dt = time.time()
            # Take action and update exploration to the newest value
            action = self.dqn_graph.act(
                self.sess, np.array(self.obs)[None],
                self.exploration.value(self.num_timesteps))[0]
            new_obs, rew, done, _ = self.env.step(action)
            # Store transition in the replay buffer.
            self.replay_buffer.add(self.obs, action, rew, new_obs, float(done))
            self.obs = new_obs

            self.episode_rewards[-1] += rew
            self.episode_lengths[-1] += 1
            if done:
                self.obs = self.env.reset()
                self.episode_rewards.append(0.0)
                self.episode_lengths.append(0.0)
            sample_time += time.time() - dt

            if self.num_timesteps > config["learning_starts"] and \
                    self.num_timesteps % config["train_freq"] == 0:
                dt = time.time()
                # Minimize the error in Bellman's equation on a batch sampled
                # from replay buffer.
                if config["prioritized_replay"]:
                    experience = self.replay_buffer.sample(
                        config["batch_size"],
                        beta=self.beta_schedule.value(self.num_timesteps))
                    (obses_t, actions, rewards, obses_tp1,
                        dones, _, batch_idxes) = experience
                else:
                    obses_t, actions, rewards, obses_tp1, dones = (
                        self.replay_buffer.sample(config["batch_size"]))
                    batch_idxes = None
                td_errors = self.dqn_graph.train(
                    self.sess, obses_t, actions, rewards, obses_tp1, dones,
                    np.ones_like(rewards))
                if config["prioritized_replay"]:
                    new_priorities = np.abs(td_errors) + (
                        config["prioritized_replay_eps"])
                    self.replay_buffer.update_priorities(
                        batch_idxes, new_priorities)
                learn_time += (time.time() - dt)

            if self.num_timesteps > config["learning_starts"] and (
                    self.num_timesteps %
                    config["target_network_update_freq"] == 0):
                # Update target network periodically.
                self.dqn_graph.update_target(self.sess)

        mean_100ep_reward = round(np.mean(self.episode_rewards[-101:-1]), 1)
        mean_100ep_length = round(np.mean(self.episode_lengths[-101:-1]), 1)
        num_episodes = len(self.episode_rewards)

        info = {
            "sample_time": sample_time,
            "learn_time": learn_time,
            "steps": self.num_timesteps,
            "episodes": num_episodes,
            "exploration": int(
                100 * self.exploration.value(self.num_timesteps))
        }

        logger.record_tabular("sample_time", sample_time)
        logger.record_tabular("learn_time", learn_time)
        logger.record_tabular("steps", self.num_timesteps)
        logger.record_tabular("buffer_size", len(self.replay_buffer))
        logger.record_tabular("episodes", num_episodes)
        logger.record_tabular("mean 100 episode reward", mean_100ep_reward)
        logger.record_tabular(
            "% time spent exploring",
            int(100 * self.exploration.value(self.num_timesteps)))
        logger.dump_tabular()

        result = TrainingResult(
            episode_reward_mean=mean_100ep_reward,
            episode_len_mean=mean_100ep_length,
            timesteps_this_iter=self.num_timesteps - iter_init_timesteps,
            info=info)

        return result

    def _save(self):
        checkpoint_path = self.saver.save(
            self.sess,
            os.path.join(self.logdir, "checkpoint"),
            global_step=self.num_iterations)
        extra_data = [
            self.replay_buffer,
            self.beta_schedule,
            self.exploration,
            self.episode_rewards,
            self.episode_lengths,
            self.saved_mean_reward,
            self.obs,
            self.num_timesteps,
            self.num_iterations]
        pickle.dump(extra_data, open(checkpoint_path + ".extra_data", "wb"))
        return checkpoint_path

    def _restore(self, checkpoint_path):
        self.saver.restore(self.sess, checkpoint_path)
        extra_data = pickle.load(open(checkpoint_path + ".extra_data", "rb"))
        self.replay_buffer = extra_data[0]
        self.beta_schedule = extra_data[1]
        self.exploration = extra_data[2]
        self.episode_rewards = extra_data[3]
        self.episode_lengths = extra_data[4]
        self.saved_mean_reward = extra_data[5]
        self.obs = extra_data[6]
        self.num_timesteps = extra_data[7]
        self.num_iterations = extra_data[8]

    def compute_action(self, observation):
        return self.dqn_graph.act(
            self.sess, np.array(observation)[None], 0.0)[0]
