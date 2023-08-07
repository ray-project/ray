# Code in this file is copied and adapted from
# https://github.com/openai/evolution-strategies-starter and from
# https://github.com/modestyachts/ARS

from collections import namedtuple
import logging
import numpy as np
import random
import time
from typing import Optional

import ray
from ray.rllib.algorithms import Algorithm
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig, NotProvided
from ray.rllib.algorithms.ars.ars_tf_policy import ARSTFPolicy
from ray.rllib.algorithms.es import optimizers, utils
from ray.rllib.algorithms.es.es_tf_policy import rollout
from ray.rllib.env.env_context import EnvContext
from ray.rllib.evaluation.worker_set import WorkerSet
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils import FilterManager
from ray.rllib.utils.actor_manager import FaultAwareApply
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import Deprecated, ALGO_DEPRECATION_WARNING
from ray.rllib.utils.metrics import (
    NUM_AGENT_STEPS_SAMPLED,
    NUM_AGENT_STEPS_TRAINED,
    NUM_ENV_STEPS_SAMPLED,
    NUM_ENV_STEPS_TRAINED,
)
from ray.rllib.utils.torch_utils import set_torch_seed

logger = logging.getLogger(__name__)

Result = namedtuple(
    "Result",
    [
        "noise_indices",
        "noisy_returns",
        "sign_noisy_returns",
        "noisy_lengths",
        "eval_returns",
        "eval_lengths",
    ],
)


class ARSConfig(AlgorithmConfig):
    """Defines a configuration class from which an ARS Algorithm can be built.

    Example:
        >>> from ray.rllib.algorithms.ars import ARSConfig
        >>> config = ARSConfig()  # doctest: +SKIP
        >>> config = config.training(report_length=20) # doctest: +SKIP
        >>> config = config.resources(num_gpus=0)  # doctest: +SKIP
        >>> config = config.rollouts(num_rollout_workers=4)  # doctest: +SKIP
        >>> config = config.environment("CartPole-v1")  # doctest: +SKIP
        >>> print(config.to_dict())  # doctest: +SKIP
        >>> # Build a Algorithm object from the config and run 1 training iteration.
        >>> algo = config.build()  # doctest: +SKIP
        >>> algo.train()  # doctest: +SKIP

    Example:
        >>> from ray.rllib.algorithms.ars import ARSConfig
        >>> from ray import air
        >>> from ray import tune
        >>> config = ARSConfig()
        >>> # Print out some default values.
        >>> print(config.action_noise_std)  # doctest: +SKIP
        >>> # Update the config object.
        >>> config = config.training(  # doctest: +SKIP
        ...     rollouts_used=tune.grid_search([32, 64]), eval_prob=0.5)
        >>> # Set the config object's env.
        >>> config = config.environment(env="CartPole-v1")  # doctest: +SKIP
        >>> # Use to_dict() to get the old-style python config dict
        >>> # when running with tune.
        >>> tune.Tuner(  # doctest: +SKIP
        ...     "ARS",
        ...     run_config=air.RunConfig(stop={"episode_reward_mean": 200}),
        ...     param_space=config.to_dict(),
        ... ).fit()
    """

    def __init__(self):
        """Initializes a ARSConfig instance."""
        super().__init__(algo_class=ARS)

        # fmt: off
        # __sphinx_doc_begin__

        # ARS specific settings:
        self.action_noise_std = 0.0
        self.noise_stdev = 0.02
        self.num_rollouts = 32
        self.rollouts_used = 32
        self.sgd_stepsize = 0.01
        self.noise_size = 250000000
        self.eval_prob = 0.03
        self.report_length = 10
        self.offset = 0
        self.tf_single_threaded = True

        # Override some of AlgorithmConfig's default values with ARS-specific values.
        self.num_rollout_workers = 2
        self.observation_filter = "MeanStdFilter"

        # ARS will use Algorithm's evaluation WorkerSet (if evaluation_interval > 0).
        # Therefore, we must be careful not to use more than 1 env per eval worker
        # (would break ARSPolicy's compute_single_action method) and to not do
        # obs-filtering.
        self.evaluation(
            evaluation_config=AlgorithmConfig.overrides(
                num_envs_per_worker=1,
                observation_filter="NoFilter",
            )
        )
        self.exploration_config = {
            # The Exploration class to use. In the simplest case, this is the name
            # (str) of any class present in the `rllib.utils.exploration` package.
            # You can also provide the python class directly or the full location
            # of your class (e.g. "ray.rllib.utils.exploration.epsilon_greedy.
            # EpsilonGreedy").
            "type": "StochasticSampling",
            # Add constructor kwargs here (if any).
        }
        # __sphinx_doc_end__
        # fmt: on

    @override(AlgorithmConfig)
    def training(
        self,
        *,
        action_noise_std: Optional[float] = NotProvided,
        noise_stdev: Optional[float] = NotProvided,
        num_rollouts: Optional[int] = NotProvided,
        rollouts_used: Optional[int] = NotProvided,
        sgd_stepsize: Optional[float] = NotProvided,
        noise_size: Optional[int] = NotProvided,
        eval_prob: Optional[float] = NotProvided,
        report_length: Optional[int] = NotProvided,
        offset: Optional[int] = NotProvided,
        tf_single_threaded: Optional[bool] = NotProvided,
        **kwargs,
    ) -> "ARSConfig":
        """Sets the training related configuration.

        Args:
            action_noise_std: Std. deviation to be used when adding (standard normal)
                noise to computed actions. Action noise is only added, if
                `compute_actions` is called with the `add_noise` arg set to True.
            noise_stdev: Std. deviation of parameter noise.
            num_rollouts: Number of perturbs to try.
            rollouts_used: Number of perturbs to keep in gradient estimate.
            sgd_stepsize: SGD step-size used for the Adam optimizer.
            noise_size: Number of rows in the noise table (shared across workers).
                Each row contains a gaussian noise value for each model parameter.
            eval_prob: Probability of evaluating the parameter rewards.
            report_length: How many of the last rewards we average over.
            offset: Value to subtract from the reward (e.g. survival bonus
                from humanoid) during rollouts.
            tf_single_threaded: Whether the tf-session should be generated without any
                parallelism options.

        Returns:
            This updated AlgorithmConfig object.
        """
        # Pass kwargs onto super's `training()` method.
        super().training(**kwargs)

        if action_noise_std is not NotProvided:
            self.action_noise_std = action_noise_std
        if noise_stdev is not NotProvided:
            self.noise_stdev = noise_stdev
        if num_rollouts is not NotProvided:
            self.num_rollouts = num_rollouts
        if rollouts_used is not NotProvided:
            self.rollouts_used = rollouts_used
        if sgd_stepsize is not NotProvided:
            self.sgd_stepsize = sgd_stepsize
        if noise_size is not NotProvided:
            self.noise_size = noise_size
        if eval_prob is not NotProvided:
            self.eval_prob = eval_prob
        if report_length is not NotProvided:
            self.report_length = report_length
        if offset is not NotProvided:
            self.offset = offset
        if tf_single_threaded is not NotProvided:
            self.tf_single_threaded = tf_single_threaded

        return self

    @override(AlgorithmConfig)
    def validate(self) -> None:
        # Call super's validation method.
        super().validate()

        if self.num_gpus > 1:
            raise ValueError("`num_gpus` > 1 not yet supported for ARS!")
        if self.num_rollout_workers <= 0:
            raise ValueError("`num_rollout_workers` must be > 0 for ARS!")
        if (
            self.evaluation_config is not None
            and self.evaluation_config.get("num_envs_per_worker") != 1
        ):
            raise ValueError(
                "`evaluation_config.num_envs_per_worker` must always be 1 for "
                "ARS! To parallelize evaluation, increase "
                "`evaluation_num_workers` to > 1."
            )
        if (
            self.evaluation_config is not None
            and self.evaluation_config.get("observation_filter") != "NoFilter"
        ):
            raise ValueError(
                "`evaluation_config.observation_filter` must always be "
                "`NoFilter` for ARS!"
            )


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
        return self.noise[i : i + dim]

    def sample_index(self, dim):
        return np.random.randint(0, len(self.noise) - dim + 1)

    def get_delta(self, dim):
        idx = self.sample_index(dim)
        return idx, self.get(idx, dim)


@ray.remote(max_restarts=-1)
class Worker(FaultAwareApply):
    def __init__(
        self,
        config: AlgorithmConfig,
        env_creator,
        noise,
        worker_index,
        min_task_runtime=0.2,
    ):
        # Set Python random, numpy, env, and torch/tf seeds.
        seed = config.seed
        if seed is not None:
            # Python random module.
            random.seed(seed)
            # Numpy.
            np.random.seed(seed)
            # Torch.
            if config.framework_str == "torch":
                set_torch_seed(seed)

        self.min_task_runtime = min_task_runtime
        self.config = config
        self.noise = SharedNoiseTable(noise)

        env_context = EnvContext(self.config.env_config, worker_index)
        self.env = env_creator(env_context)
        # Seed the env, if gym.Env.
        if not hasattr(self.env, "seed"):
            logger.info("Env doesn't support env.seed(): {}".format(self.env))
        # Gym.env.
        else:
            self.env.seed(seed)

        from ray.rllib import models

        self.preprocessor = models.ModelCatalog.get_preprocessor(self.env)

        policy_cls = get_policy_class(self.config)
        self.policy = policy_cls(
            self.env.observation_space, self.env.action_space, config.to_dict()
        )

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
                f.reset_buffer()
        return return_filters

    def rollout(self, timestep_limit, add_noise=False):
        rollout_rewards, rollout_fragment_length = rollout(
            self.policy,
            self.env,
            timestep_limit=timestep_limit,
            add_noise=add_noise,
            offset=self.config.offset,
        )
        return rollout_rewards, rollout_fragment_length

    def do_rollouts(self, params, timestep_limit=None):
        # Set the network weights.
        self.policy.set_flat_weights(params)

        noise_indices, returns, sign_returns, lengths = [], [], [], []
        eval_returns, eval_lengths = [], []

        # Perform some rollouts with noise.
        while len(noise_indices) == 0:
            if np.random.uniform() < self.config.eval_prob:
                # Do an evaluation run with no perturbation.
                self.policy.set_flat_weights(params)
                rewards, length = self.rollout(timestep_limit, add_noise=False)
                eval_returns.append(rewards.sum())
                eval_lengths.append(length)
            else:
                # Do a regular run with parameter perturbations.
                noise_index = self.noise.sample_index(self.policy.num_params)

                perturbation = self.config.noise_stdev * self.noise.get(
                    noise_index, self.policy.num_params
                )

                # These two sampling steps could be done in parallel on
                # different actors letting us update twice as frequently.
                self.policy.set_flat_weights(params + perturbation)
                rewards_pos, lengths_pos = self.rollout(timestep_limit)

                self.policy.set_flat_weights(params - perturbation)
                rewards_neg, lengths_neg = self.rollout(timestep_limit)

                noise_indices.append(noise_index)
                returns.append([rewards_pos.sum(), rewards_neg.sum()])
                sign_returns.append(
                    [np.sign(rewards_pos).sum(), np.sign(rewards_neg).sum()]
                )
                lengths.append([lengths_pos, lengths_neg])

        return Result(
            noise_indices=noise_indices,
            noisy_returns=returns,
            sign_noisy_returns=sign_returns,
            noisy_lengths=lengths,
            eval_returns=eval_returns,
            eval_lengths=eval_lengths,
        )

    def stop(self):
        """Releases all resources used by this RolloutWorker."""
        pass


def get_policy_class(config: AlgorithmConfig):
    if config.framework_str == "torch":
        from ray.rllib.algorithms.ars.ars_torch_policy import ARSTorchPolicy

        policy_cls = ARSTorchPolicy
    else:
        policy_cls = ARSTFPolicy
    return policy_cls


@Deprecated(
    old="rllib/algorithms/ars/",
    new="rllib_contrib/ars/",
    help=ALGO_DEPRECATION_WARNING,
    error=False,
)
class ARS(Algorithm):
    """Large-scale implementation of Augmented Random Search in Ray."""

    @classmethod
    @override(Algorithm)
    def get_default_config(cls) -> AlgorithmConfig:
        return ARSConfig()

    @override(Algorithm)
    def setup(self, config: AlgorithmConfig):
        # Setup our config: Merge the user-supplied config (which could
        # be a partial config dict with the class' default).
        if isinstance(config, dict):
            self.config = self.get_default_config().update_from_dict(config)

        # Validate our config dict.
        self.config.validate()

        # Generate the local env.
        env_context = EnvContext(self.config.env_config or {}, worker_index=0)
        env = self.env_creator(env_context)

        self.callbacks = self.config.callbacks_class()

        self._policy_class = get_policy_class(self.config)
        self.policy = self._policy_class(
            env.observation_space, env.action_space, self.config
        )
        self.optimizer = optimizers.SGD(self.policy, self.config.sgd_stepsize)

        self.rollouts_used = self.config.rollouts_used
        self.num_rollouts = self.config.num_rollouts
        self.report_length = self.config.report_length

        # Create the shared noise table.
        logger.info("Creating shared noise table.")
        noise_id = create_shared_noise.remote(self.config.noise_size)
        self.noise = SharedNoiseTable(ray.get(noise_id))

        # Create the actors.
        logger.info("Creating actors.")

        remote_workers = [
            Worker.remote(self.config, self.env_creator, noise_id, idx + 1)
            for idx in range(self.config.num_rollout_workers)
        ]
        self.workers = WorkerSet._from_existing(
            local_worker=None,
            remote_workers=remote_workers,
        )

        self.episodes_so_far = 0
        self.reward_list = []
        self.tstart = time.time()

    @override(Algorithm)
    def get_policy(self, policy=DEFAULT_POLICY_ID):
        if policy != DEFAULT_POLICY_ID:
            raise ValueError(
                "ARS has no policy '{}'! Use {} "
                "instead.".format(policy, DEFAULT_POLICY_ID)
            )
        return self.policy

    @override(Algorithm)
    def step(self):
        config = self.config

        theta = self.policy.get_flat_weights()
        assert theta.dtype == np.float32
        assert len(theta.shape) == 1

        # Put the current policy weights in the object store.
        theta_id = ray.put(theta)
        # Use the actors to do rollouts, note that we pass in the ID of the
        # policy weights.
        results, num_episodes, num_timesteps = self._collect_results(
            theta_id, config["num_rollouts"]
        )
        # Update our sample steps counters.
        self._counters[NUM_AGENT_STEPS_SAMPLED] += num_timesteps
        self._counters[NUM_ENV_STEPS_SAMPLED] += num_timesteps

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
        assert (
            len(all_noise_indices)
            == len(all_training_returns)
            == len(all_training_lengths)
        )

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
            max_rewards >= np.percentile(max_rewards, percentile)
        ]
        noise_idx = noise_indices[idx]
        noisy_returns = noisy_returns[idx, :]

        # Compute and take a step.
        g, count = utils.batched_weighted_sum(
            noisy_returns[:, 0] - noisy_returns[:, 1],
            (self.noise.get(index, self.policy.num_params) for index in noise_idx),
            batch_size=min(500, noisy_returns[:, 0].size),
        )
        g /= noise_idx.size
        # scale the returns by their standard deviation
        if not np.isclose(np.std(noisy_returns), 0.0):
            g /= np.std(noisy_returns)
        assert g.shape == (self.policy.num_params,) and g.dtype == np.float32
        # Compute the new weights theta.
        theta, update_ratio = self.optimizer.update(-g)

        # Update our train steps counters.
        self._counters[NUM_AGENT_STEPS_TRAINED] += num_timesteps
        self._counters[NUM_ENV_STEPS_TRAINED] += num_timesteps

        # Set the new weights in the local copy of the policy.
        self.policy.set_flat_weights(theta)
        # update the reward list
        if len(all_eval_returns) > 0:
            self.reward_list.append(eval_returns.mean())

        # Bring restored workers back if necessary.
        self.restore_workers(self.workers)

        # Now sync the filters
        FilterManager.synchronize(
            {DEFAULT_POLICY_ID: self.policy.observation_filter}, self.workers
        )

        info = {
            "weights_norm": np.square(theta).sum(),
            "weights_std": np.std(theta),
            "grad_norm": np.square(g).sum(),
            "update_ratio": update_ratio,
            "episodes_this_iter": noisy_lengths.size,
            "episodes_so_far": self.episodes_so_far,
        }

        reward_mean = np.mean(self.reward_list[-self.report_length :])
        result = {
            "sampler_results": {
                "episode_reward_mean": reward_mean,
                "episode_len_mean": eval_lengths.mean(),
            },
            "timesteps_this_iter": noisy_lengths.sum(),
            "info": info,
        }

        return result

    @override(Algorithm)
    def cleanup(self):
        self.workers.stop()

    @override(Algorithm)
    def restore_workers(self, workers: WorkerSet):
        restored = self.workers.probe_unhealthy_workers()
        if restored:
            self._sync_weights_to_workers(worker_set=self.workers, worker_ids=restored)

    @override(Algorithm)
    def compute_single_action(self, observation, *args, **kwargs):
        action, _, _ = self.policy.compute_actions([observation], update=True)
        if kwargs.get("full_fetch"):
            return action[0], [], {}
        return action[0]

    @override(Algorithm)
    def _sync_weights_to_workers(self, *, worker_set=None, worker_ids=None):
        # Broadcast the new policy weights to all evaluation workers.
        assert worker_set is not None
        logger.info("Synchronizing weights to evaluation workers.")
        weights = ray.put(self.policy.get_flat_weights())
        worker_set.foreach_worker(
            lambda w: w.foreach_policy(
                lambda p, _: p.set_flat_weights(ray.get(weights))
            ),
            local_worker=False,
            remote_worker_ids=worker_ids,
        )

    def _collect_results(self, theta_id, min_episodes):
        num_episodes, num_timesteps = 0, 0
        results = []
        while num_episodes < min_episodes:
            logger.debug(
                "Collected {} episodes {} timesteps so far this iter".format(
                    num_episodes, num_timesteps
                )
            )
            rollout_ids = self.workers.foreach_worker(
                func=lambda w: w.do_rollouts(ray.get(theta_id)),
                local_worker=False,
            )
            # Get the results of the rollouts.
            for result in rollout_ids:
                results.append(result)
                # Update the number of episodes and the number of timesteps
                # keeping in mind that result.noisy_lengths is a list of lists,
                # where the inner lists have length 2.
                num_episodes += sum(len(pair) for pair in result.noisy_lengths)
                num_timesteps += sum(sum(pair) for pair in result.noisy_lengths)

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
        FilterManager.synchronize(
            {DEFAULT_POLICY_ID: self.policy.observation_filter}, self.workers
        )
