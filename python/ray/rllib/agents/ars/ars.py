# Code in this file is copied and adapted from
# https://github.com/openai/evolution-strategies-starter and from
# https://github.com/modestyachts/ARS

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import namedtuple
import numpy as np
import os
import pickle
import time

import ray
from ray.rllib.agents import Agent, with_common_config
from ray.tune.trial import Resources

from ray.rllib.agents.ars import optimizers
from ray.rllib.agents.ars import policies
from ray.rllib.agents.es import tabular_logger as tlogger
from ray.rllib.agents.ars import utils

Result = namedtuple("Result", [
    "noise_indices", "noisy_returns", "sign_noisy_returns", "noisy_lengths",
    "eval_returns", "eval_lengths"
])

DEFAULT_CONFIG = with_common_config({
    'noise_stdev': 0.02,  # std deviation of parameter noise
    'num_deltas': 4,  # number of perturbations to try
    'deltas_used': 4,  # number of perturbations to keep in gradient estimate
    'num_workers': 2,
    'stepsize': 0.01,  # sgd step-size
    'observation_filter': "MeanStdFilter",
    'noise_size': 250000000,
    'eval_prob': 0.03,  # probability of evaluating the parameter rewards
    'env_config': {},
    'offset': 0,
    'policy_type': "LinearPolicy",  # ["LinearPolicy", "MLPPolicy"]
    "fcnet_hiddens": [32, 32],  # fcnet structure of MLPPolicy
})


@ray.remote
def create_shared_noise(count):
    """Create a large array of noise to be shared by all workers."""
    seed = 123
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

    def get_delta(self, dim):
        idx = self.sample_index(dim)
        return idx, self.get(idx, dim)


@ray.remote
class Worker(object):
    def __init__(self,
                 config,
                 policy_params,
                 env_creator,
                 noise,
                 min_task_runtime=0.2):
        self.min_task_runtime = min_task_runtime
        self.config = config
        self.policy_params = policy_params
        self.noise = SharedNoiseTable(noise)

        self.env = env_creator(config["env_config"])
        from ray.rllib import models
        self.preprocessor = models.ModelCatalog.get_preprocessor(self.env)

        self.sess = utils.make_session(single_threaded=True)
        if config["policy_type"] == "LinearPolicy":
            self.policy = policies.LinearPolicy(
                self.sess, self.env.action_space, self.preprocessor,
                config["observation_filter"], **policy_params)
        else:
            self.policy = policies.MLPPolicy(
                self.sess, self.env.action_space, self.preprocessor,
                config["observation_filter"], config["fcnet_hiddens"],
                **policy_params)

    def rollout(self, timestep_limit, add_noise=False):
        rollout_rewards, rollout_length = policies.rollout(
            self.policy,
            self.env,
            timestep_limit=timestep_limit,
            add_noise=add_noise,
            offset=self.config['offset'])
        return rollout_rewards, rollout_length

    def do_rollouts(self, params, timestep_limit=None):
        # Set the network weights.
        self.policy.set_weights(params)

        noise_indices, returns, sign_returns, lengths = [], [], [], []
        eval_returns, eval_lengths = [], []

        # Perform some rollouts with noise.
        while (len(noise_indices) == 0):
            if np.random.uniform() < self.config["eval_prob"]:
                # Do an evaluation run with no perturbation.
                self.policy.set_weights(params)
                rewards, length = self.rollout(timestep_limit, add_noise=False)
                eval_returns.append(rewards.sum())
                eval_lengths.append(length)
            else:
                # Do a regular run with parameter perturbations.
                noise_index = self.noise.sample_index(self.policy.num_params)

                perturbation = self.config["noise_stdev"] * self.noise.get(
                    noise_index, self.policy.num_params)

                # These two sampling steps could be done in parallel on
                # different actors letting us update twice as frequently.
                self.policy.set_weights(params + perturbation)
                rewards_pos, lengths_pos = self.rollout(timestep_limit)

                self.policy.set_weights(params - perturbation)
                rewards_neg, lengths_neg = self.rollout(timestep_limit)

                noise_indices.append(noise_index)
                returns.append([rewards_pos.sum(), rewards_neg.sum()])
                sign_returns.append(
                    [np.sign(rewards_pos).sum(),
                     np.sign(rewards_neg).sum()])
                lengths.append([lengths_pos, lengths_neg])

        return Result(
            noise_indices=noise_indices,
            noisy_returns=returns,
            sign_noisy_returns=sign_returns,
            noisy_lengths=lengths,
            eval_returns=eval_returns,
            eval_lengths=eval_lengths)


class ARSAgent(Agent):
    """Large-scale implementation of Augmented Random Search in Ray."""

    _agent_name = "ARS"
    _default_config = DEFAULT_CONFIG

    @classmethod
    def default_resource_request(cls, config):
        cf = dict(cls._default_config, **config)
        return Resources(cpu=1, gpu=0, extra_cpu=cf["num_workers"])

    def _init(self):
        policy_params = {"action_noise_std": 0.0}

        # register the linear network
        utils.register_linear_network()

        env = self.env_creator(self.config["env_config"])
        from ray.rllib import models
        preprocessor = models.ModelCatalog.get_preprocessor(env)

        self.sess = utils.make_session(single_threaded=False)
        if self.config["policy_type"] == "LinearPolicy":
            self.policy = policies.LinearPolicy(
                self.sess, env.action_space, preprocessor,
                self.config["observation_filter"], **policy_params)
        else:
            self.policy = policies.MLPPolicy(
                self.sess, env.action_space, preprocessor,
                self.config["observation_filter"],
                self.config["fcnet_hiddens"], **policy_params)
        self.optimizer = optimizers.Adam(self.policy, self.config["stepsize"])

        self.deltas_used = self.config["deltas_used"]
        self.num_deltas = self.config["num_deltas"]

        # Create the shared noise table.
        print("Creating shared noise table.")
        noise_id = create_shared_noise.remote(self.config["noise_size"])
        self.noise = SharedNoiseTable(ray.get(noise_id))

        # Create the actors.
        print("Creating actors.")
        self.workers = [
            Worker.remote(self.config, policy_params, self.env_creator,
                          noise_id) for _ in range(self.config["num_workers"])
        ]

        self.episodes_so_far = 0
        self.timesteps_so_far = 0
        self.tstart = time.time()

    def _collect_results(self, theta_id, min_episodes):
        num_episodes, num_timesteps = 0, 0
        results = []
        while num_episodes < min_episodes:
            print("Collected {} episodes {} timesteps so far this iter".format(
                num_episodes, num_timesteps))
            rollout_ids = [
                worker.do_rollouts.remote(theta_id) for worker in self.workers
            ]
            # Get the results of the rollouts.
            for result in ray.get(rollout_ids):
                results.append(result)
                # Update the number of episodes and the number of timesteps
                # keeping in mind that result.noisy_lengths is a list of lists,
                # where the inner lists have length 2.
                num_episodes += sum(len(pair) for pair in result.noisy_lengths)
                num_timesteps += sum(
                    sum(pair) for pair in result.noisy_lengths)
        return results, num_episodes, num_timesteps

    def _train(self):
        config = self.config

        step_tstart = time.time()
        theta = self.policy.get_weights()
        assert theta.dtype == np.float32

        # Put the current policy weights in the object store.
        theta_id = ray.put(theta)
        # Use the actors to do rollouts, note that we pass in the ID of the
        # policy weights.
        results, num_episodes, num_timesteps = self._collect_results(
            theta_id, config["num_deltas"])

        all_noise_indices = []
        all_training_returns = []
        all_training_lengths = []
        all_eval_returns = []
        all_eval_lengths = []

        # Loop over the results.
        for result in results:
            all_eval_returns += result.eval_returns
            all_eval_lengths += result.eval_lengths

            all_noise_indices += result.noise_indices
            all_training_returns += result.noisy_returns
            all_training_lengths += result.noisy_lengths

        assert len(all_eval_returns) == len(all_eval_lengths)
        assert (len(all_noise_indices) == len(all_training_returns) ==
                len(all_training_lengths))

        self.episodes_so_far += num_episodes
        self.timesteps_so_far += num_timesteps

        # Assemble the results.
        eval_returns = np.array(all_eval_returns)
        eval_lengths = np.array(all_eval_lengths)
        noise_indices = np.array(all_noise_indices)
        noisy_returns = np.array(all_training_returns)
        noisy_lengths = np.array(all_training_lengths)

        # keep only the best returns
        # select top performing directions if deltas_used < num_deltas
        max_rewards = np.max(noisy_returns, axis=1)
        if self.deltas_used > self.num_deltas:
            self.deltas_used = self.num_deltas

        percentile = 100 * (1 - (self.deltas_used / self.num_deltas))
        idx = np.arange(max_rewards.size)[
            max_rewards >= np.percentile(max_rewards, percentile)]
        noise_idx = noise_indices[idx]
        noisy_returns = noisy_returns[idx, :]

        # Compute and take a step.
        g, count = utils.batched_weighted_sum(
            noisy_returns[:, 0] - noisy_returns[:, 1],
            (self.noise.get(index, self.policy.num_params)
             for index in noise_idx),
            batch_size=min(500, noisy_returns[:, 0].size))
        g /= noise_idx.size
        # scale the returns by their standard deviation
        if not np.isclose(np.std(noisy_returns), 0.0):
            g /= np.std(noisy_returns)
        assert (g.shape == (self.policy.num_params, )
                and g.dtype == np.float32)
        print('the number of policy params is, ', self.policy.num_params)
        # Compute the new weights theta.
        theta, update_ratio = self.optimizer.update(-g)
        # Set the new weights in the local copy of the policy.
        self.policy.set_weights(theta)

        step_tend = time.time()
        tlogger.record_tabular("EvalEpRewMean", eval_returns.mean())
        tlogger.record_tabular("EvalEpRewStd", eval_returns.std())
        tlogger.record_tabular("EvalEpLenMean", eval_lengths.mean())

        tlogger.record_tabular("NoisyEpRewMean", noisy_returns.mean())
        tlogger.record_tabular("NoisyEpRewStd", noisy_returns.std())
        tlogger.record_tabular("NoisyEpLenMean", noisy_lengths.mean())

        tlogger.record_tabular("WeightsNorm", float(np.square(theta).sum()))
        tlogger.record_tabular("WeightsStd", float(np.std(theta)))
        tlogger.record_tabular("Grad2Norm", float(np.sqrt(np.square(g).sum())))
        tlogger.record_tabular("UpdateRatio", float(update_ratio))
        tlogger.dump_tabular()

        info = {
            "weights_norm": np.square(theta).sum(),
            "grad_norm": np.square(g).sum(),
            "update_ratio": update_ratio,
            "episodes_this_iter": noisy_lengths.size,
            "episodes_so_far": self.episodes_so_far,
            "timesteps_so_far": self.timesteps_so_far,
            "time_elapsed_this_iter": step_tend - step_tstart,
            "time_elapsed": step_tend - self.tstart
        }

        result = dict(
            episode_reward_mean=eval_returns.mean(),
            episode_len_mean=eval_lengths.mean(),
            timesteps_this_iter=noisy_lengths.sum(),
            info=info)

        return result

    def _stop(self):
        # workaround for https://github.com/ray-project/ray/issues/1516
        for w in self.workers:
            w.__ray_terminate__.remote()

    def _save(self, checkpoint_dir):
        checkpoint_path = os.path.join(checkpoint_dir,
                                       "checkpoint-{}".format(self.iteration))
        weights = self.policy.get_weights()
        objects = [weights, self.episodes_so_far, self.timesteps_so_far]
        pickle.dump(objects, open(checkpoint_path, "wb"))
        return checkpoint_path

    def _restore(self, checkpoint_path):
        objects = pickle.load(open(checkpoint_path, "rb"))
        self.policy.set_weights(objects[0])
        self.episodes_so_far = objects[1]
        self.timesteps_so_far = objects[2]

    def compute_action(self, observation):
        return self.policy.compute(observation, update=True)[0]
