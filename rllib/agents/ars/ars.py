# Code in this file is copied and adapted from
# https://github.com/openai/evolution-strategies-starter and from
# https://github.com/modestyachts/ARS

from collections import namedtuple
import logging
import numpy as np
import random
import time

import ray
from ray.rllib.agents import Trainer, with_common_config
from ray.rllib.agents.ars.ars_tf_policy import ARSTFPolicy
from ray.rllib.agents.es import optimizers, utils
from ray.rllib.agents.es.es_tf_policy import rollout
from ray.rllib.env.env_context import EnvContext
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import Deprecated
from ray.rllib.utils.torch_utils import set_torch_seed
from ray.rllib.utils.typing import TrainerConfigDict
from ray.rllib.utils import FilterManager

logger = logging.getLogger(__name__)

Result = namedtuple("Result", [
    "noise_indices", "noisy_returns", "sign_noisy_returns", "noisy_lengths",
    "eval_returns", "eval_lengths"
])

# yapf: disable
# __sphinx_doc_begin__
DEFAULT_CONFIG = with_common_config({
    "action_noise_std": 0.0,
    "noise_stdev": 0.02,  # std deviation of parameter noise
    "num_rollouts": 32,  # number of perturbs to try
    "rollouts_used": 32,  # number of perturbs to keep in gradient estimate
    "num_workers": 2,
    "sgd_stepsize": 0.01,  # sgd step-size
    "observation_filter": "MeanStdFilter",
    "noise_size": 250000000,
    "eval_prob": 0.03,  # probability of evaluating the parameter rewards
    "report_length": 10,  # how many of the last rewards we average over
    "offset": 0,
    # ARS will use Trainer's evaluation WorkerSet (if evaluation_interval > 0).
    # Therefore, we must be careful not to use more than 1 env per eval worker
    # (would break ARSPolicy's compute_single_action method) and to not do
    # obs-filtering.
    "evaluation_config": {
        "num_envs_per_worker": 1,
        "observation_filter": "NoFilter"
    },
})
# __sphinx_doc_end__
# yapf: enable


@ray.remote
def create_shared_noise(count):
    """Create a large array of noise to be shared by all workers."""
    seed = 123
    noise = np.random.RandomState(seed).randn(count).astype(np.float32)
    return noise


class SharedNoiseTable:
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
class Worker:
    def __init__(self,
                 config,
                 env_creator,
                 noise,
                 worker_index,
                 min_task_runtime=0.2):

        # Set Python random, numpy, env, and torch/tf seeds.
        seed = config.get("seed")
        if seed is not None:
            # Python random module.
            random.seed(seed)
            # Numpy.
            np.random.seed(seed)
            # Torch.
            if config.get("framework") == "torch":
                set_torch_seed(seed)

        self.min_task_runtime = min_task_runtime
        self.config = config
        self.config["single_threaded"] = True
        self.noise = SharedNoiseTable(noise)

        env_context = EnvContext(config["env_config"] or {}, worker_index)
        self.env = env_creator(env_context)
        # Seed the env, if gym.Env.
        if not hasattr(self.env, "seed"):
            logger.info("Env doesn't support env.seed(): {}".format(self.env))
        # Gym.env.
        else:
            self.env.seed(seed)

        from ray.rllib import models
        self.preprocessor = models.ModelCatalog.get_preprocessor(self.env)

        policy_cls = get_policy_class(config)
        self.policy = policy_cls(self.env.observation_space,
                                 self.env.action_space, config)

    @property
    def filters(self):
        return {DEFAULT_POLICY_ID: self.policy.observation_filter}

    def sync_filters(self, new_filters):
        for k in self.filters:
            self.filters[k].sync(new_filters[k])

    def get_filters(self, flush_after=False):
        return_filters = {}
        for k, f in self.filters.items():
            return_filters[k] = f.as_serializable()
            if flush_after:
                f.clear_buffer()
        return return_filters

    def rollout(self, timestep_limit, add_noise=False):
        rollout_rewards, rollout_fragment_length = rollout(
            self.policy,
            self.env,
            timestep_limit=timestep_limit,
            add_noise=add_noise,
            offset=self.config["offset"])
        return rollout_rewards, rollout_fragment_length

    def do_rollouts(self, params, timestep_limit=None):
        # Set the network weights.
        self.policy.set_flat_weights(params)

        noise_indices, returns, sign_returns, lengths = [], [], [], []
        eval_returns, eval_lengths = [], []

        # Perform some rollouts with noise.
        while (len(noise_indices) == 0):
            if np.random.uniform() < self.config["eval_prob"]:
                # Do an evaluation run with no perturbation.
                self.policy.set_flat_weights(params)
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
                self.policy.set_flat_weights(params + perturbation)
                rewards_pos, lengths_pos = self.rollout(timestep_limit)

                self.policy.set_flat_weights(params - perturbation)
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


def get_policy_class(config):
    if config["framework"] == "torch":
        from ray.rllib.agents.ars.ars_torch_policy import ARSTorchPolicy
        policy_cls = ARSTorchPolicy
    else:
        policy_cls = ARSTFPolicy
    return policy_cls


class ARSTrainer(Trainer):
    """Large-scale implementation of Augmented Random Search in Ray."""

    @classmethod
    @override(Trainer)
    def get_default_config(cls) -> TrainerConfigDict:
        return DEFAULT_CONFIG

    @override(Trainer)
    def validate_config(self, config: TrainerConfigDict) -> None:
        # Call super's validation method.
        super().validate_config(config)

        if config["num_gpus"] > 1:
            raise ValueError("`num_gpus` > 1 not yet supported for ARS!")
        if config["num_workers"] <= 0:
            raise ValueError("`num_workers` must be > 0 for ARS!")
        if config["evaluation_config"]["num_envs_per_worker"] != 1:
            raise ValueError(
                "`evaluation_config.num_envs_per_worker` must always be 1 for "
                "ARS! To parallelize evaluation, increase "
                "`evaluation_num_workers` to > 1.")
        if config["evaluation_config"]["observation_filter"] != "NoFilter":
            raise ValueError(
                "`evaluation_config.observation_filter` must always be "
                "`NoFilter` for ARS!")

    @override(Trainer)
    def _init(self, config, env_creator):
        self.validate_config(config)
        env_context = EnvContext(config["env_config"] or {}, worker_index=0)
        env = env_creator(env_context)

        self._policy_class = get_policy_class(config)
        self.policy = self._policy_class(env.observation_space,
                                         env.action_space, config)
        self.optimizer = optimizers.SGD(self.policy, config["sgd_stepsize"])

        self.rollouts_used = config["rollouts_used"]
        self.num_rollouts = config["num_rollouts"]
        self.report_length = config["report_length"]

        # Create the shared noise table.
        logger.info("Creating shared noise table.")
        noise_id = create_shared_noise.remote(config["noise_size"])
        self.noise = SharedNoiseTable(ray.get(noise_id))

        # Create the actors.
        logger.info("Creating actors.")
        self.workers = [
            Worker.remote(config, env_creator, noise_id, idx + 1)
            for idx in range(config["num_workers"])
        ]

        self.episodes_so_far = 0
        self.reward_list = []
        self.tstart = time.time()

    @override(Trainer)
    def get_policy(self, policy=DEFAULT_POLICY_ID):
        if policy != DEFAULT_POLICY_ID:
            raise ValueError("ARS has no policy '{}'! Use {} "
                             "instead.".format(policy, DEFAULT_POLICY_ID))
        return self.policy

    @override(Trainer)
    def step_attempt(self):
        config = self.config

        theta = self.policy.get_flat_weights()
        assert theta.dtype == np.float32
        assert len(theta.shape) == 1

        # Put the current policy weights in the object store.
        theta_id = ray.put(theta)
        # Use the actors to do rollouts, note that we pass in the ID of the
        # policy weights.
        results, num_episodes, num_timesteps = self._collect_results(
            theta_id, config["num_rollouts"])

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

        # Assemble the results.
        eval_returns = np.array(all_eval_returns)
        eval_lengths = np.array(all_eval_lengths)
        noise_indices = np.array(all_noise_indices)
        noisy_returns = np.array(all_training_returns)
        noisy_lengths = np.array(all_training_lengths)

        # keep only the best returns
        # select top performing directions if rollouts_used < num_rollouts
        max_rewards = np.max(noisy_returns, axis=1)
        if self.rollouts_used > self.num_rollouts:
            self.rollouts_used = self.num_rollouts

        percentile = 100 * (1 - (self.rollouts_used / self.num_rollouts))
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
        # Compute the new weights theta.
        theta, update_ratio = self.optimizer.update(-g)
        # Set the new weights in the local copy of the policy.
        self.policy.set_flat_weights(theta)
        # update the reward list
        if len(all_eval_returns) > 0:
            self.reward_list.append(eval_returns.mean())

        # Now sync the filters
        FilterManager.synchronize({
            DEFAULT_POLICY_ID: self.policy.observation_filter
        }, self.workers)

        info = {
            "weights_norm": np.square(theta).sum(),
            "weights_std": np.std(theta),
            "grad_norm": np.square(g).sum(),
            "update_ratio": update_ratio,
            "episodes_this_iter": noisy_lengths.size,
            "episodes_so_far": self.episodes_so_far,
        }
        result = dict(
            episode_reward_mean=np.mean(
                self.reward_list[-self.report_length:]),
            episode_len_mean=eval_lengths.mean(),
            timesteps_this_iter=noisy_lengths.sum(),
            info=info)

        return result

    @override(Trainer)
    def cleanup(self):
        # workaround for https://github.com/ray-project/ray/issues/1516
        for w in self.workers:
            w.__ray_terminate__.remote()

    @override(Trainer)
    def compute_single_action(self, observation, *args, **kwargs):
        action, _, _ = self.policy.compute_actions([observation], update=True)
        if kwargs.get("full_fetch"):
            return action[0], [], {}
        return action[0]

    @Deprecated(new="compute_single_action", error=False)
    def compute_action(self, observation, *args, **kwargs):
        return self.compute_single_action(observation, *args, **kwargs)

    @override(Trainer)
    def _sync_weights_to_workers(self, *, worker_set=None, workers=None):
        # Broadcast the new policy weights to all evaluation workers.
        assert worker_set is not None
        logger.info("Synchronizing weights to evaluation workers.")
        weights = ray.put(self.policy.get_flat_weights())
        worker_set.foreach_policy(
            lambda p, pid: p.set_flat_weights(ray.get(weights)))

    def _collect_results(self, theta_id, min_episodes):
        num_episodes, num_timesteps = 0, 0
        results = []
        while num_episodes < min_episodes:
            logger.debug(
                "Collected {} episodes {} timesteps so far this iter".format(
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

    def __getstate__(self):
        return {
            "weights": self.policy.get_flat_weights(),
            "filter": self.policy.observation_filter,
            "episodes_so_far": self.episodes_so_far,
        }

    def __setstate__(self, state):
        self.episodes_so_far = state["episodes_so_far"]
        self.policy.set_flat_weights(state["weights"])
        self.policy.observation_filter = state["filter"]
        FilterManager.synchronize({
            DEFAULT_POLICY_ID: self.policy.observation_filter
        }, self.workers)
