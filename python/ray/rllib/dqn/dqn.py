from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time

import numpy as np
import pickle
import os
import tensorflow as tf

import ray
from ray.rllib.agent import Agent
from ray.rllib.dqn import logger, models
from ray.rllib.dqn.common.wrappers import wrap_dqn
from ray.rllib.dqn.common.schedules import LinearSchedule
from ray.rllib.dqn.replay_buffer import ReplayBuffer, PrioritizedReplayBuffer
from ray.tune.result import TrainingResult


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
    sample_batch_size: int
        update the replay buffer with this many samples at once
    num_workers: int
        the number of workers to use for parallel batch sample collection
    train_batch_size: int
        size of a batched sampled from replay buffer for training
    print_freq: int
        how often to print out training progress
        set to None to disable printing
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
    gpu_offset=0,
    lr=5e-4,
    schedule_max_timesteps=100000,
    timesteps_per_iteration=1000,
    buffer_size=50000,
    exploration_fraction=0.1,
    exploration_final_eps=0.02,
    sample_batch_size=1,
    num_workers=1,
    train_batch_size=32,
    print_freq=1,
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


class Actor(object):
    def __init__(self, env_creator, config, logdir):
        env = env_creator()
        env = wrap_dqn(env, config["model"])
        self.env = env
        self.config = config

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
            prioritized_replay_beta_iters = \
                config["prioritized_replay_beta_iters"]
            if prioritized_replay_beta_iters is None:
                prioritized_replay_beta_iters = \
                    config["schedule_max_timesteps"]
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
        self.variables = ray.experimental.TensorFlowVariables(
            tf.group(self.dqn_graph.q_tp1, self.dqn_graph.q_t), self.sess)

        self.episode_rewards = [0.0]
        self.episode_lengths = [0.0]
        self.saved_mean_reward = None
        self.obs = self.env.reset()
        self.file_writer = tf.summary.FileWriter(logdir, self.sess.graph)

    def step(self, cur_timestep):
        # Take action and update exploration to the newest value
        action = self.dqn_graph.act(
            self.sess, np.array(self.obs)[None],
            self.exploration.value(cur_timestep))[0]
        new_obs, rew, done, _ = self.env.step(action)
        ret = (self.obs, action, rew, new_obs, float(done))
        self.obs = new_obs
        self.episode_rewards[-1] += rew
        self.episode_lengths[-1] += 1
        if done:
            self.obs = self.env.reset()
            self.episode_rewards.append(0.0)
            self.episode_lengths.append(0.0)
        return ret

    def do_steps(self, num_steps, cur_timestep):
        for _ in range(num_steps):
            obs, action, rew, new_obs, done = self.step(cur_timestep)
            self.replay_buffer.add(obs, action, rew, new_obs, done)

    def get_gradient(self, cur_timestep):
        if self.config["prioritized_replay"]:
            experience = self.replay_buffer.sample(
                self.config["train_batch_size"],
                beta=self.beta_schedule.value(cur_timestep))
            (obses_t, actions, rewards, obses_tp1,
                dones, _, batch_idxes) = experience
        else:
            obses_t, actions, rewards, obses_tp1, dones = \
                self.replay_buffer.sample(self.config["train_batch_size"])
            batch_idxes = None
        td_errors, grad = self.dqn_graph.compute_gradients(
            self.sess, obses_t, actions, rewards, obses_tp1, dones,
            np.ones_like(rewards))
        if self.config["prioritized_replay"]:
            new_priorities = (
                np.abs(td_errors) + self.config["prioritized_replay_eps"])
            self.replay_buffer.update_priorities(
                batch_idxes, new_priorities)
        return grad

    def apply_gradients(self, grad):
        self.dqn_graph.apply_gradients(self.sess, grad)

    def stats(self, num_timesteps):
        mean_100ep_reward = round(np.mean(self.episode_rewards[-101:-1]), 1)
        mean_100ep_length = round(np.mean(self.episode_lengths[-101:-1]), 1)
        exploration = self.exploration.value(num_timesteps)
        return (
            mean_100ep_reward,
            mean_100ep_length,
            len(self.episode_rewards),
            exploration,
            len(self.replay_buffer))

    def get_weights(self):
        return self.variables.get_weights()

    def set_weights(self, weights):
        self.variables.set_weights(weights)

    def save(self):
        return [
            self.beta_schedule,
            self.exploration,
            self.episode_rewards,
            self.episode_lengths,
            self.saved_mean_reward,
            self.obs,
            self.replay_buffer]

    def restore(self, data):
        self.beta_schedule = data[0]
        self.exploration = data[1]
        self.episode_rewards = data[2]
        self.episode_lengths = data[3]
        self.saved_mean_reward = data[4]
        self.obs = data[5]
        self.replay_buffer = data[6]


@ray.remote
class RemoteActor(Actor):
    def __init__(self, env_creator, config, logdir, gpu_mask):
        os.environ["CUDA_VISIBLE_DEVICES"] = gpu_mask
        Actor.__init__(self, env_creator, config, logdir)


class DQNAgent(Agent):
    _agent_name = "DQN"
    _default_config = DEFAULT_CONFIG

    def _init(self):
        self.actor = Actor(self.env_creator, self.config, self.logdir)
        self.workers = [
            RemoteActor.remote(
                self.env_creator, self.config, self.logdir,
                "{}".format(i + self.config["gpu_offset"]))
            for i in range(self.config["num_workers"])]

        self.cur_timestep = 0
        self.num_iterations = 0
        self.num_target_updates = 0
        self.steps_since_update = 0
        self.file_writer = tf.summary.FileWriter(
            self.logdir, self.actor.sess.graph)
        self.saver = tf.train.Saver(max_to_keep=None)

    def _update_worker_weights(self):
        w = self.actor.get_weights()
        weights = ray.put(self.actor.get_weights())
        for w in self.workers:
            w.set_weights.remote(weights)

    def _train(self):
        config = self.config
        sample_time, sync_time, learn_time, apply_time = 0, 0, 0, 0
        iter_init_timesteps = self.cur_timestep

        num_loop_iters = 0
        steps_per_iter = config["sample_batch_size"] * len(self.workers)
        while (self.cur_timestep - iter_init_timesteps <
               config["timesteps_per_iteration"]):
            dt = time.time()
            ray.get([
                w.do_steps.remote(
                    config["sample_batch_size"], self.cur_timestep)
                for w in self.workers])
            num_loop_iters += 1
            self.cur_timestep += steps_per_iter
            self.steps_since_update += steps_per_iter
            sample_time += time.time() - dt

            if self.cur_timestep > config["learning_starts"]:
                dt = time.time()
                # Minimize the error in Bellman's equation on a batch sampled
                # from replay buffer.
                self._update_worker_weights()
                sync_time += (time.time() - dt)
                dt = time.time()
                gradients = ray.get(
                    [w.get_gradient.remote(self.cur_timestep)
                        for w in self.workers])
                learn_time += (time.time() - dt)
                dt = time.time()
                for grad in gradients:
                    self.actor.apply_gradients(grad)
                apply_time += (time.time() - dt)

            if (self.cur_timestep > config["learning_starts"] and
                    self.steps_since_update >
                    config["target_network_update_freq"]):
                self.actor.dqn_graph.update_target(self.actor.sess)
                # Update target network periodically.
                self._update_worker_weights()
                self.steps_since_update -= config["target_network_update_freq"]
                self.num_target_updates += 1

        mean_100ep_reward = 0.0
        mean_100ep_length = 0.0
        num_episodes = 0
        buffer_size_sum = 0
        for mean_rew, mean_len, episodes, exploration, buf_sz in ray.get(
              [w.stats.remote(self.cur_timestep) for w in self.workers]):
            mean_100ep_reward += mean_rew
            mean_100ep_length += mean_len
            num_episodes += episodes
            buffer_size_sum += buf_sz
        mean_100ep_reward /= len(self.workers)
        mean_100ep_length /= len(self.workers)

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
                num_loop_iters * np.float64(steps_per_iter) / sample_time),
            ("learn_samples_per_s",
                num_loop_iters * np.float64(config["train_batch_size"]) *
                np.float64(config["num_workers"]) / learn_time),
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
