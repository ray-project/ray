from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time

import gym
import numpy as np
import tensorflow as tf

import ray
from ray.rllib.common import Algorithm, TrainingResult
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
    model_config: dict
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
    model_config={},
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
    def __init__(self, env_name, config, logdir):
        env = gym.make(env_name)
        # TODO(ekl): replace this with RLlib preprocessors
        if "NoFrameskip" in env_name:
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

    def step(self, num_timesteps):
        # Take action and update exploration to the newest value
        action = self.dqn_graph.act(
            self.sess, np.array(self.obs)[None],
            self.exploration.value(num_timesteps))[0]
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

    def stats(self, num_timesteps):
        mean_100ep_reward = round(np.mean(self.episode_rewards[-101:-1]), 1)
        mean_100ep_length = round(np.mean(self.episode_lengths[-101:-1]), 1)
        exploration = self.exploration.value(num_timesteps)
        return (
            mean_100ep_reward,
            mean_100ep_length,
            len(self.episode_rewards),
            exploration)

    def get_weights(self):
        return self.variables.get_weights()

    def set_weights(self, weights):
        self.variables.set_weights(weights)


@ray.remote
class RemoteActor(Actor):
    pass


class DQN(Algorithm):
    def __init__(self, env_name, config, upload_dir=None):
        assert config["num_workers"] <= config["sample_batch_size"]
        config.update({"alg": "DQN"})

        Algorithm.__init__(self, env_name, config, upload_dir=upload_dir)

        self.actor = Actor(env_name, config, self.logdir)
        self.workers = [
            RemoteActor.remote(env_name, config, self.logdir)
            for _ in range(config["num_workers"])]

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

        self.cur_timestep = 0
        self.num_iterations = 0
        self.num_target_updates = 0
        self.steps_since_update = 0

    def _get_rollouts(self, steps_requested, cur_timestep):
        requests = {}

        def new_request(worker):
            new_request.num_calls += 1
            obj_id = worker.step.remote(cur_timestep)
            requests[obj_id] = worker
        new_request.num_calls = 0

        for w in self.workers:
            new_request(w)

        while requests:
            ready, _ = ray.wait(requests.keys())
            for obj_id in ready:
                yield ray.get(obj_id)
                idle_worker = requests[obj_id]
                del requests[obj_id]
                if new_request.num_calls < steps_requested:
                    new_request(idle_worker)

    def _update_worker_weights(self):
        self.actor.dqn_graph.update_target(self.actor.sess)
        w = self.actor.get_weights()
        weights = ray.put(self.actor.get_weights())
        for w in self.workers:
            w.set_weights.remote(weights)

    def train(self):
        config = self.config
        sample_time, learn_time = 0, 0
        start_timestep = self.cur_timestep

        num_loop_iters = 0
        while (self.cur_timestep - start_timestep <
               config["timesteps_per_iteration"]):
            num_loop_iters += 1
            self.cur_timestep += config["sample_batch_size"]
            self.steps_since_update += config["sample_batch_size"]

            dt = time.time()
            rollouts = self._get_rollouts(
                config["sample_batch_size"], self.cur_timestep)
            for obs, action, rew, new_obs, done in rollouts:
                self.replay_buffer.add(obs, action, rew, new_obs, done)
            sample_time += time.time() - dt

            if self.cur_timestep > config["learning_starts"]:
                dt = time.time()
                # Minimize the error in Bellman's equation on a batch sampled
                # from replay buffer.
                if config["prioritized_replay"]:
                    experience = self.replay_buffer.sample(
                        config["train_batch_size"],
                        beta=self.beta_schedule.value(self.cur_timestep))
                    (obses_t, actions, rewards, obses_tp1,
                        dones, _, batch_idxes) = experience
                else:
                    obses_t, actions, rewards, obses_tp1, dones = \
                        self.replay_buffer.sample(config["train_batch_size"])
                    batch_idxes = None
                # TODO(ekl) parallelize this over gpus
                td_errors = self.actor.dqn_graph.train(
                    self.actor.sess, obses_t, actions, rewards, obses_tp1,
                    dones, np.ones_like(rewards))
                if config["prioritized_replay"]:
                    new_priorities = (
                        np.abs(td_errors) + config["prioritized_replay_eps"])
                    self.replay_buffer.update_priorities(
                        batch_idxes, new_priorities)
                learn_time += (time.time() - dt)

            if (self.cur_timestep > config["learning_starts"] and
                    self.steps_since_update >
                    config["target_network_update_freq"]):
                # Update target network periodically.
                self._update_worker_weights()
                self.steps_since_update -= config["target_network_update_freq"]
                self.num_target_updates += 1

        mean_100ep_reward = 0.0
        mean_100ep_length = 0.0
        num_episodes = 0
        for mean_rew, mean_len, episodes, exploration in ray.get(
              [w.stats.remote(self.cur_timestep) for w in self.workers]):
            mean_100ep_reward += mean_rew
            mean_100ep_length += mean_len
            num_episodes += episodes
        mean_100ep_reward /= len(self.workers)
        mean_100ep_length /= len(self.workers)

        info = [
            ("mean_100ep_reward", mean_100ep_reward),
            ("exploration_frac", exploration),
            ("steps", self.cur_timestep),
            ("episodes", num_episodes),
            ("buffer_size", len(self.replay_buffer)),
            ("target_updates", self.num_target_updates),
            ("sample_time", sample_time),
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

        res = TrainingResult(
            self.experiment_id.hex, self.num_iterations, mean_100ep_reward,
            mean_100ep_length, dict(info))
        self.num_iterations += 1
        return res
