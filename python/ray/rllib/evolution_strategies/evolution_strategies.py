# Code in this file is copied and adapted from
# https://github.com/openai/evolution-strategies-starter.

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import namedtuple
import gym
import numpy as np
import os
import time

import ray
from ray.rllib.common import Algorithm, TrainingResult

from ray.rllib.evolution_strategies import optimizers
from ray.rllib.evolution_strategies import policies
from ray.rllib.evolution_strategies import tabular_logger as tlogger
from ray.rllib.evolution_strategies import tf_util
from ray.rllib.evolution_strategies import utils


Config = namedtuple("Config", [
    "l2coeff", "noise_stdev", "episodes_per_batch", "timesteps_per_batch",
    "calc_obstat_prob", "eval_prob", "snapshot_freq", "return_proc_mode",
    "episode_cutoff_mode", "num_workers", "stepsize"
])

Result = namedtuple("Result", [
    "noise_inds_n", "returns_n2", "sign_returns_n2", "lengths_n2",
    "eval_return", "eval_length", "ob_sum", "ob_sumsq", "ob_count"
])


DEFAULT_CONFIG = Config(
    l2coeff=0.005,
    noise_stdev=0.02,
    episodes_per_batch=10000,
    timesteps_per_batch=100000,
    calc_obstat_prob=0.01,
    eval_prob=0,
    snapshot_freq=0,
    return_proc_mode="centered_rank",
    episode_cutoff_mode="env_default",
    num_workers=10,
    stepsize=.01)


@ray.remote
def create_shared_noise():
  """Create a large array of noise to be shared by all workers."""
  seed = 123
  count = 250000000
  noise = np.random.RandomState(seed).randn(count).astype(np.float32)
  return noise


class SharedNoiseTable(object):
  def __init__(self, noise):
    self.noise = noise
    assert self.noise.dtype == np.float32

  def get(self, i, dim):
    return self.noise[i:i + dim]

  def sample_index(self, stream, dim):
    return stream.randint(0, len(self.noise) - dim + 1)


@ray.remote
class Worker(object):
  def __init__(self, config, policy_params, env_name, noise,
               min_task_runtime=0.2):
    self.min_task_runtime = min_task_runtime
    self.config = config
    self.policy_params = policy_params
    self.noise = SharedNoiseTable(noise)

    self.env = gym.make(env_name)
    self.sess = utils.make_session(single_threaded=True)
    self.policy = policies.MujocoPolicy(self.env.observation_space,
                                        self.env.action_space,
                                        **policy_params)
    tf_util.initialize()

    self.rs = np.random.RandomState()

    assert self.policy.needs_ob_stat == (self.config.calc_obstat_prob != 0)

  def rollout_and_update_ob_stat(self, timestep_limit, task_ob_stat):
    if (self.policy.needs_ob_stat and self.config.calc_obstat_prob != 0 and
            self.rs.rand() < self.config.calc_obstat_prob):
      rollout_rews, rollout_len, obs = self.policy.rollout(
          self.env, timestep_limit=timestep_limit, save_obs=True,
          random_stream=self.rs)
      task_ob_stat.increment(obs.sum(axis=0), np.square(obs).sum(axis=0),
                             len(obs))
    else:
      rollout_rews, rollout_len = self.policy.rollout(
          self.env, timestep_limit=timestep_limit, random_stream=self.rs)
    return rollout_rews, rollout_len

  def do_rollouts(self, params, ob_mean, ob_std, timestep_limit=None):
    # Set the network weights.
    self.policy.set_trainable_flat(params)

    if self.policy.needs_ob_stat:
      self.policy.set_ob_stat(ob_mean, ob_std)

    if self.config.eval_prob != 0:
      raise NotImplementedError("Eval rollouts are not implemented.")

    noise_inds, returns, sign_returns, lengths = [], [], [], []
    # We set eps=0 because we're incrementing only.
    task_ob_stat = utils.RunningStat(self.env.observation_space.shape, eps=0)

    # Perform some rollouts with noise.
    task_tstart = time.time()
    while (len(noise_inds) == 0 or
           time.time() - task_tstart < self.min_task_runtime):
      noise_idx = self.noise.sample_index(self.rs, self.policy.num_params)
      perturbation = self.config.noise_stdev * self.noise.get(
          noise_idx, self.policy.num_params)

      # These two sampling steps could be done in parallel on different actors
      # letting us update twice as frequently.
      self.policy.set_trainable_flat(params + perturbation)
      rews_pos, len_pos = self.rollout_and_update_ob_stat(timestep_limit,
                                                          task_ob_stat)

      self.policy.set_trainable_flat(params - perturbation)
      rews_neg, len_neg = self.rollout_and_update_ob_stat(timestep_limit,
                                                          task_ob_stat)

      noise_inds.append(noise_idx)
      returns.append([rews_pos.sum(), rews_neg.sum()])
      sign_returns.append([np.sign(rews_pos).sum(), np.sign(rews_neg).sum()])
      lengths.append([len_pos, len_neg])

      return Result(
          noise_inds_n=np.array(noise_inds),
          returns_n2=np.array(returns, dtype=np.float32),
          sign_returns_n2=np.array(sign_returns, dtype=np.float32),
          lengths_n2=np.array(lengths, dtype=np.int32),
          eval_return=None,
          eval_length=None,
          ob_sum=(None if task_ob_stat.count == 0 else task_ob_stat.sum),
          ob_sumsq=(None if task_ob_stat.count == 0 else task_ob_stat.sumsq),
          ob_count=task_ob_stat.count)


class EvolutionStrategies(Algorithm):
  def __init__(self, env_name, config):
    Algorithm.__init__(self, env_name, config)

    policy_params = {
        "ac_bins": "continuous:",
        "ac_noise_std": 0.01,
        "nonlin_type": "tanh",
        "hidden_dims": [256, 256],
        "connection_type": "ff"
    }

    # Create the shared noise table.
    print("Creating shared noise table.")
    noise_id = create_shared_noise.remote()
    self.noise = SharedNoiseTable(ray.get(noise_id))

    # Create the actors.
    print("Creating actors.")
    self.workers = [Worker.remote(config, policy_params, env_name, noise_id)
                    for _ in range(config.num_workers)]

    env = gym.make(env_name)
    utils.make_session(single_threaded=False)
    self.policy = policies.MujocoPolicy(
        env.observation_space, env.action_space, **policy_params)
    tf_util.initialize()
    self.optimizer = optimizers.Adam(self.policy, config.stepsize)
    self.ob_stat = utils.RunningStat(env.observation_space.shape, eps=1e-2)

    self.episodes_so_far = 0
    self.timesteps_so_far = 0
    self.tstart = time.time()
    self.iteration = 0

  def train(self):
    config = self.config

    step_tstart = time.time()
    theta = self.policy.get_trainable_flat()
    assert theta.dtype == np.float32

    # Put the current policy weights in the object store.
    theta_id = ray.put(theta)
    # Use the actors to do rollouts, note that we pass in the ID of the policy
    # weights.
    rollout_ids = [worker.do_rollouts.remote(
        theta_id,
        self.ob_stat.mean if self.policy.needs_ob_stat else None,
        self.ob_stat.std if self.policy.needs_ob_stat else None)
        for worker in self.workers]

    # Get the results of the rollouts.
    results = ray.get(rollout_ids)

    curr_task_results = []
    ob_count_this_batch = 0
    # Loop over the results
    for result in results:
      assert result.eval_length is None, "We aren't doing eval rollouts."
      assert result.noise_inds_n.ndim == 1
      assert result.returns_n2.shape == (len(result.noise_inds_n), 2)
      assert result.lengths_n2.shape == (len(result.noise_inds_n), 2)
      assert result.returns_n2.dtype == np.float32

      result_num_eps = result.lengths_n2.size
      result_num_timesteps = result.lengths_n2.sum()
      self.episodes_so_far += result_num_eps
      self.timesteps_so_far += result_num_timesteps

      curr_task_results.append(result)
      # Update ob stats.
      if self.policy.needs_ob_stat and result.ob_count > 0:
        self.ob_stat.increment(result.ob_sum, result.ob_sumsq, result.ob_count)
        ob_count_this_batch += result.ob_count

    # Assemble the results.
    noise_inds_n = np.concatenate([r.noise_inds_n for
                                   r in curr_task_results])
    returns_n2 = np.concatenate([r.returns_n2 for r in curr_task_results])
    lengths_n2 = np.concatenate([r.lengths_n2 for r in curr_task_results])
    assert noise_inds_n.shape[0] == returns_n2.shape[0] == lengths_n2.shape[0]
    # Process the returns.
    if config.return_proc_mode == "centered_rank":
      proc_returns_n2 = utils.compute_centered_ranks(returns_n2)
    else:
      raise NotImplementedError(config.return_proc_mode)

    # Compute and take a step.
    g, count = utils.batched_weighted_sum(
        proc_returns_n2[:, 0] - proc_returns_n2[:, 1],
        (self.noise.get(idx, self.policy.num_params) for idx in noise_inds_n),
        batch_size=500)
    g /= returns_n2.size
    assert (g.shape == (self.policy.num_params,) and g.dtype == np.float32 and
            count == len(noise_inds_n))
    update_ratio = self.optimizer.update(-g + config.l2coeff * theta)

    # Update ob stat (we're never running the policy in the master, but we
    # might be snapshotting the policy).
    if self.policy.needs_ob_stat:
      self.policy.set_ob_stat(self.ob_stat.mean, self.ob_stat.std)

    step_tend = time.time()
    tlogger.record_tabular("EpRewMean", returns_n2.mean())
    tlogger.record_tabular("EpRewStd", returns_n2.std())
    tlogger.record_tabular("EpLenMean", lengths_n2.mean())

    tlogger.record_tabular(
        "Norm", float(np.square(self.policy.get_trainable_flat()).sum()))
    tlogger.record_tabular("GradNorm", float(np.square(g).sum()))
    tlogger.record_tabular("UpdateRatio", float(update_ratio))

    tlogger.record_tabular("EpisodesThisIter", lengths_n2.size)
    tlogger.record_tabular("EpisodesSoFar", self.episodes_so_far)
    tlogger.record_tabular("TimestepsThisIter", lengths_n2.sum())
    tlogger.record_tabular("TimestepsSoFar", self.timesteps_so_far)

    tlogger.record_tabular("ObCount", ob_count_this_batch)

    tlogger.record_tabular("TimeElapsedThisIter", step_tend - step_tstart)
    tlogger.record_tabular("TimeElapsed", step_tend - self.tstart)
    tlogger.dump_tabular()

    if (config.snapshot_freq != 0 and
            self.iteration % config.snapshot_freq == 0):
      filename = os.path.join(
          "/tmp", "snapshot_iter{:05d}.h5".format(self.iteration))
      assert not os.path.exists(filename)
      self.policy.save(filename)
      tlogger.log("Saved snapshot {}".format(filename))

    res = TrainingResult(self.iteration, returns_n2.mean(), lengths_n2.mean())
    self.iteration += 1
    return res
