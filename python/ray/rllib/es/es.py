# Code in this file is copied and adapted from
# https://github.com/openai/evolution-strategies-starter.

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import namedtuple
import numpy as np
import os
import pickle
import time

import ray
from ray.rllib.agent import Agent
from ray.rllib.models import ModelCatalog

from ray.rllib.es import optimizers
from ray.rllib.es import policies
from ray.rllib.es import tabular_logger as tlogger
from ray.rllib.es import tf_util
from ray.rllib.es import utils
from ray.tune.result import TrainingResult


Result = namedtuple("Result", [
    "noise_inds_n", "returns_n2", "sign_returns_n2", "lengths_n2",
    "eval_return", "eval_length"
])


DEFAULT_CONFIG = dict(
    l2coeff=0.005,
    noise_stdev=0.02,
    episodes_per_batch=1000,
    timesteps_per_batch=10000,
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

    def sample_index(self, dim):
        return np.random.randint(0, len(self.noise) - dim + 1)


@ray.remote
class Worker(object):
    def __init__(self, config, policy_params, env_creator, noise,
                 min_task_runtime=0.2):
        self.min_task_runtime = min_task_runtime
        self.config = config
        self.policy_params = policy_params
        self.noise = SharedNoiseTable(noise)

        self.env = env_creator()
        self.preprocessor = ModelCatalog.get_preprocessor(self.env)

        self.sess = utils.make_session(single_threaded=True)
        self.policy = policies.GenericPolicy(
            self.env.observation_space, self.env.action_space,
            self.preprocessor, **policy_params)
        tf_util.initialize()

    def rollout(self, timestep_limit):
        rollout_rews, rollout_len = policies.rollout(self.policy, self.env,
            self.preprocessor, timestep_limit=timestep_limit, add_noise=True)
        return rollout_rews, rollout_len

    def do_rollouts(self, params, timestep_limit=None):
        # Set the network weights.
        self.policy.set_trainable_flat(params)

        if self.config["eval_prob"] != 0:
            raise NotImplementedError("Eval rollouts are not implemented.")

        noise_inds, returns, sign_returns, lengths = [], [], [], []

        # Perform some rollouts with noise.
        task_tstart = time.time()
        while (len(noise_inds) == 0 or
               time.time() - task_tstart < self.min_task_runtime):
            noise_idx = self.noise.sample_index(self.policy.num_params)
            perturbation = self.config["noise_stdev"] * self.noise.get(
                noise_idx, self.policy.num_params)

            # These two sampling steps could be done in parallel on different
            # actors letting us update twice as frequently.
            self.policy.set_trainable_flat(params + perturbation)
            rews_pos, len_pos = self.rollout(timestep_limit)

            self.policy.set_trainable_flat(params - perturbation)
            rews_neg, len_neg = self.rollout(timestep_limit)

            noise_inds.append(noise_idx)
            returns.append([rews_pos.sum(), rews_neg.sum()])
            sign_returns.append(
                [np.sign(rews_pos).sum(), np.sign(rews_neg).sum()])
            lengths.append([len_pos, len_neg])

            return Result(
                noise_inds_n=np.array(noise_inds),
                returns_n2=np.array(returns, dtype=np.float32),
                sign_returns_n2=np.array(sign_returns, dtype=np.float32),
                lengths_n2=np.array(lengths, dtype=np.int32),
                eval_return=None,
                eval_length=None)


class ESAgent(Agent):
    _agent_name = "ES"
    _default_config = DEFAULT_CONFIG

    def _init(self):

        policy_params = {
            "ac_noise_std": 0.01
        }

        env = self.env_creator()
        preprocessor = ModelCatalog.get_preprocessor(env)

        self.sess = utils.make_session(single_threaded=False)
        self.policy = policies.GenericPolicy(
            env.observation_space, env.action_space, preprocessor,
            **policy_params)
        tf_util.initialize()
        self.optimizer = optimizers.Adam(self.policy, self.config["stepsize"])

        # Create the shared noise table.
        print("Creating shared noise table.")
        noise_id = create_shared_noise.remote()
        self.noise = SharedNoiseTable(ray.get(noise_id))

        # Create the actors.
        print("Creating actors.")
        self.workers = [
            Worker.remote(
                self.config, policy_params, self.env_creator, noise_id)
            for _ in range(self.config["num_workers"])]

        self.episodes_so_far = 0
        self.timesteps_so_far = 0
        self.tstart = time.time()

    def _collect_results(self, theta_id, min_eps, min_timesteps):
        num_eps, num_timesteps = 0, 0
        results = []
        while num_eps < min_eps or num_timesteps < min_timesteps:
            print(
                "Collected {} episodes {} timesteps so far this iter".format(
                    num_eps, num_timesteps))
            rollout_ids = [worker.do_rollouts.remote(theta_id)
                for worker in self.workers]
            # Get the results of the rollouts.
            for result in ray.get(rollout_ids):
                results.append(result)
                num_eps += result.lengths_n2.size
                num_timesteps += result.lengths_n2.sum()
        return results

    def _train(self):
        config = self.config

        step_tstart = time.time()
        theta = self.policy.get_trainable_flat()
        assert theta.dtype == np.float32

        # Put the current policy weights in the object store.
        theta_id = ray.put(theta)
        # Use the actors to do rollouts, note that we pass in the ID of the
        # policy weights.
        results = self._collect_results(
            theta_id,
            config["episodes_per_batch"],
            config["timesteps_per_batch"])

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

        # Assemble the results.
        noise_inds_n = np.concatenate(
            [r.noise_inds_n for r in curr_task_results])
        returns_n2 = np.concatenate([r.returns_n2 for r in curr_task_results])
        lengths_n2 = np.concatenate([r.lengths_n2 for r in curr_task_results])
        assert (noise_inds_n.shape[0] == returns_n2.shape[0] ==
                lengths_n2.shape[0])
        # Process the returns.
        if config["return_proc_mode"] == "centered_rank":
            proc_returns_n2 = utils.compute_centered_ranks(returns_n2)
        else:
            raise NotImplementedError(config["return_proc_mode"])

        # Compute and take a step.
        g, count = utils.batched_weighted_sum(
            proc_returns_n2[:, 0] - proc_returns_n2[:, 1],
            (self.noise.get(idx, self.policy.num_params)
             for idx in noise_inds_n),
            batch_size=500)
        g /= returns_n2.size
        assert (
            g.shape == (self.policy.num_params,) and
            g.dtype == np.float32 and
            count == len(noise_inds_n))
        update_ratio = self.optimizer.update(-g + config["l2coeff"] * theta)

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

        info = {
            "weights_norm": np.square(self.policy.get_trainable_flat()).sum(),
            "grad_norm": np.square(g).sum(),
            "update_ratio": update_ratio,
            "episodes_this_iter": lengths_n2.size,
            "episodes_so_far": self.episodes_so_far,
            "timesteps_this_iter": lengths_n2.sum(),
            "timesteps_so_far": self.timesteps_so_far,
            "ob_count": ob_count_this_batch,
            "time_elapsed_this_iter": step_tend - step_tstart,
            "time_elapsed": step_tend - self.tstart
        }

        result = TrainingResult(
            episode_reward_mean=returns_n2.mean(),
            episode_len_mean=lengths_n2.mean(),
            timesteps_this_iter=lengths_n2.sum(),
            info=info)

        return result

    def _save(self):
        checkpoint_path = os.path.join(
            self.logdir, "checkpoint-{}".format(self.iteration))
        weights = self.policy.get_trainable_flat()
        objects = [
            weights,
            self.episodes_so_far,
            self.timesteps_so_far]
        pickle.dump(objects, open(checkpoint_path, "wb"))
        return checkpoint_path

    def _restore(self, checkpoint_path):
        objects = pickle.load(open(checkpoint_path, "rb"))
        self.policy.set_trainable_flat(objects[0])
        self.episodes_so_far = objects[2]
        self.timesteps_so_far = objects[3]

    def compute_action(self, observation):
        return self.policy.act([observation])[0]
