from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time

import gym
import numpy as np
import tensorflow as tf

import ray
from ray.rllib.common import Algorithm, TrainingResult
from ray.rllib.dqn.build_graph import build_train
from ray.rllib.dqn import logger, models
from ray.rllib.dqn.common.atari_wrappers_deprecated \
    import wrap_dqn, ScaledFloatFrame
from ray.rllib.dqn.common import tf_util as U
from ray.rllib.dqn.common.schedules import LinearSchedule
from ray.rllib.dqn.replay_buffer import ReplayBuffer, PrioritizedReplayBuffer


"""The default configuration dict for the DQN algorithm.

  lr: float
    learning rate for adam optimizer
  schedule_max_timesteps: int
    max num timesteps for annealing schedules
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
  batch_size: int
    size of a batched sampled from replay buffer for training
  print_freq: int
    how often to print out training progress
    set to None to disable printing
  checkpoint_freq: int
    how often to save the model. This is so that the best version is restored
    at the end of the training. If you do not wish to restore the best version
    at the end of the training set this variable to None.
  learning_starts: int
    how many steps of the model to collect transitions for before learning
    starts
  gamma: float
    discount factor
  target_network_update_freq: int
    update the target network every `target_network_update_freq` steps.
  prioritized_replay: True
    if True prioritized replay buffer will be used.
  prioritized_replay_alpha: float
    alpha parameter for prioritized replay buffer
  prioritized_replay_beta0: float
    initial value of beta for prioritized replay buffer
  prioritized_replay_beta_iters: int
    number of iterations over which beta will be annealed from initial value
    to 1.0. If set to None equals to schedule_max_timesteps
  prioritized_replay_eps: float
    epsilon to add to the TD errors when updating priorities.
  num_cpu: int
    number of cpus to use for training
"""
DEFAULT_CONFIG = dict(
    lr=5e-4,
    schedule_max_timesteps=100000,
    buffer_size=50000,
    exploration_fraction=0.1,
    exploration_final_eps=0.02,
    sample_batch_size=1,
    num_workers=4,
    batch_size=32,
    print_freq=1,
    checkpoint_freq=10000,
    learning_starts=1000,
    gamma=1.0,
    target_network_update_freq=500,
    prioritized_replay=False,
    prioritized_replay_alpha=0.6,
    prioritized_replay_beta0=0.4,
    prioritized_replay_beta_iters=None,
    prioritized_replay_eps=1e-6,
    num_cpu=16)


class Actor(object):
  def __init__(self, env_name, config):
    env = gym.make(env_name)
    env = ScaledFloatFrame(wrap_dqn(env))
    self.env = env
    self.episode_rewards = [0.0]
    self.episode_lengths = [0.0]
    self.obs = self.env.reset()
    model = models.cnn_to_mlp(
        convs=[(32, 8, 4), (64, 4, 2), (64, 3, 1)],
        hiddens=[256], dueling=True)
    sess = U.make_session(num_cpu=config["num_cpu"])
    sess.__enter__()

    def make_obs_ph(name):
      return U.BatchInput(env.observation_space.shape, name=name)

    self.act, self.optimize, self.update_target, self.debug = build_train(
        make_obs_ph=make_obs_ph,
        q_func=model,
        num_actions=env.action_space.n,
        optimizer=tf.train.AdamOptimizer(learning_rate=config["lr"]),
        gamma=config["gamma"],
        grad_norm_clipping=10)

    # Create the schedule for exploration starting from 1.
    self.exploration = LinearSchedule(
        schedule_timesteps=int(
            config["exploration_fraction"] * config["schedule_max_timesteps"]),
        initial_p=1.0,
        final_p=config["exploration_final_eps"])

    # Initialize the parameters and copy them to the target network.
    U.initialize()
    self.variables = ray.experimental.TensorFlowVariables(
        self.act.outputs_update, sess)
    self.update_target()

  def get_weights(self):
    weights = self.variables.get_weights()
    return weights


@ray.remote
class RemoteActor(Actor):
  def step(self, num_timesteps):
    # Take action and update exploration to the newest value
    action = self.act(
        np.array(self.obs)[None],
        update_eps=self.exploration.value(num_timesteps))[0]
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
    exploration = int(100 * self.exploration.value(num_timesteps))
    return (
        mean_100ep_reward,
        mean_100ep_length,
        len(self.episode_rewards),
        exploration)

  def set_weights(self, weights):
    self.variables.set_weights(weights)



class DQN(Algorithm):
  def __init__(self, env_name, config, upload_dir=None):
    assert config["num_workers"] <= config["sample_batch_size"]
    config.update({"alg": "DQN"})
    Algorithm.__init__(self, env_name, config, upload_dir=upload_dir)

    self.actor = Actor(env_name, config)
    self.workers = [
        RemoteActor.remote(env_name, config)
        for _ in range(config["num_workers"])]

    # Create the replay buffer
    if config["prioritized_replay"]:
      self.replay_buffer = PrioritizedReplayBuffer(
          config["buffer_size"], alpha=config["prioritized_replay_alpha"])
      prioritized_replay_beta_iters = config["prioritized_replay_beta_iters"]
      if prioritized_replay_beta_iters is None:
        prioritized_replay_beta_iters = config["schedule_max_timesteps"]
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
    requests_total = 0
    requests = {}

    def new_request(worker):
      nonlocal requests_total
      requests_total += 1
      obj_id = worker.step.remote(cur_timestep)
      requests[obj_id] = worker

    for w in self.workers:
      new_request(w)

    while requests:
      ready, _ = ray.wait(requests.keys())
      for obj_id in ready:
        yield ray.get(obj_id)
        idle_worker = requests[obj_id]
        del requests[obj_id]
        if requests_total < steps_requested:
          new_request(idle_worker)

  def _update_worker_weights(self):
    self.actor.update_target()
    weights = ray.put(self.actor.get_weights())
    ray.get([w.set_weights.remote(weights) for w in self.workers])

  def train(self):
    config = self.config
    sample_time, learn_time = 0, 0
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
      # Minimize the error in Bellman's equation on a batch sampled from
      # replay buffer.
      if config["prioritized_replay"]:
        experience = self.replay_buffer.sample(
            config["batch_size"],
            beta=self.beta_schedule.value(self.cur_timestep))
        (obses_t, actions, rewards, obses_tp1,
            dones, _, batch_idxes) = experience
      else:
        obses_t, actions, rewards, obses_tp1, dones = \
            self.replay_buffer.sample(config["batch_size"])
        batch_idxes = None
      # TODO(ekl) parallelize this over gpus
      td_errors = self.actor.optimize(
          obses_t, actions, rewards, obses_tp1, dones, np.ones_like(rewards))
      if config["prioritized_replay"]:
        new_priorities = np.abs(td_errors) + config["prioritized_replay_eps"]
        self.replay_buffer.update_priorities(batch_idxes, new_priorities)
      learn_time += (time.time() - dt)

    if (self.cur_timestep > config["learning_starts"] and
            self.steps_since_update > config["target_network_update_freq"]):
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

    info = {
        "sample_time": sample_time,
        "samples_per_second":
            np.float64(config["sample_batch_size"]) / sample_time,
        "learn_time": learn_time,
        "learn_samples_per_second":
            np.float64(config["batch_size"]) / learn_time,
        "steps": self.cur_timestep,
        "target_updates": self.num_target_updates,
        "episodes": num_episodes,
        "exploration": exploration
    }

    for k, v in info.items():
      logger.record_tabular(k, v)
    logger.dump_tabular()

    res = TrainingResult(
        self.experiment_id.hex, self.num_iterations, mean_100ep_reward,
        mean_100ep_length, info)
    self.num_iterations += 1
    return res
