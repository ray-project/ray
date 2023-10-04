"""
Decentralized Distributed PPO (DD-PPO)
======================================

Unlike APPO or PPO, learning is no longer done centralized in the Algorithm
process. Instead, gradients are computed remotely on each rollout worker and
all-reduced to sync them at each mini-batch. This allows each worker's GPU
to be used both for sampling and for training.

DD-PPO should be used if you have envs that require GPUs to function, or have
a very large model that cannot be effectively optimized with the GPUs available
on a single machine (DD-PPO allows scaling to arbitrary numbers of GPUs across
multiple nodes, unlike PPO/APPO which is limited to GPUs on a single node).

Paper reference: https://arxiv.org/abs/1911.00357
Note that unlike the paper, we currently do not implement straggler mitigation.
"""

import logging
import time
from typing import Optional

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig, NotProvided
from ray.rllib.algorithms.ppo import PPOConfig, PPO
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.evaluation.rollout_worker import RolloutWorker
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import Deprecated, ALGO_DEPRECATION_WARNING
from ray.rllib.utils.metrics import (
    LEARN_ON_BATCH_TIMER,
    NUM_AGENT_STEPS_SAMPLED,
    NUM_AGENT_STEPS_TRAINED,
    NUM_ENV_STEPS_SAMPLED,
    NUM_ENV_STEPS_TRAINED,
    SAMPLE_TIMER,
)
from ray.rllib.utils.metrics.learner_info import LearnerInfoBuilder
from ray.rllib.utils.sgd import do_minibatch_sgd
from ray.rllib.utils.typing import (
    ResultDict,
)

logger = logging.getLogger(__name__)


class DDPPOConfig(PPOConfig):
    """Defines a configuration class from which a DDPPO Algorithm can be built.

    Note(jungong) : despite best efforts, DDPPO does not use fault tolerant and
    elastic features of WorkerSet, because of the way Torch DDP is set up.

    Example:
        >>> from ray.rllib.algorithms.ddppo import DDPPOConfig
        >>> config = DDPPOConfig().training(lr=0.003, keep_local_weights_in_sync=True)
        >>> config = config.resources(num_gpus=1)
        >>> config = config.rollouts(num_rollout_workers=10)
        >>> print(config.to_dict())   # doctest: +SKIP
        >>> # Build a Algorithm object from the config and run 1 training iteration.
        >>> algo = config.build(env="CartPole-v1")  # doctest: +SKIP
        >>> algo.train()  # doctest: +SKIP

    Example:
        >>> from ray.rllib.algorithms.ddppo import DDPPOConfig
        >>> from ray import air
        >>> from ray import tune
        >>> config = DDPPOConfig()
        >>> # Print out some default values.
        >>> print(config.kl_coeff)   # doctest: +SKIP
        >>> # Update the config object.
        >>> config.training( # doctest: +SKIP
        ...     lr=tune.grid_search([0.001, 0.0001]), num_sgd_iter=15)
        >>> # Set the config object's env.
        >>> config.environment(env="CartPole-v1") # doctest: +SKIP
        >>> # Use to_dict() to get the old-style python config dict
        >>> # when running with tune.
        >>> tune.Tuner( # doctest: +SKIP
        ...     "DDPPO",
        ...     run_config=air.RunConfig(stop={"episode_reward_mean": 200}),
        ...     param_space=config.to_dict(),
        ... ).fit()
    """

    def __init__(self, algo_class=None):
        """Initializes a DDPPOConfig instance."""
        super().__init__(algo_class=algo_class or DDPPO)

        # fmt: off
        # __sphinx_doc_begin__
        # DD-PPO specific settings:
        self.keep_local_weights_in_sync = True
        self.torch_distributed_backend = "gloo"

        # Override some of PPO/Algorithm's default values with DDPPO-specific values.
        self.num_rollout_workers = 2
        # Vectorize the env (should enable by default since each worker has
        # a GPU).
        self.num_envs_per_worker = 5
        # During the SGD phase, workers iterate over minibatches of this size.
        # The effective minibatch size will be:
        # `sgd_minibatch_size * num_workers`.
        self.sgd_minibatch_size = 50
        # Number of SGD epochs per optimization round.
        self.num_sgd_iter = 10

        # *** WARNING: configs below are DDPPO overrides over PPO; you
        #     shouldn't need to adjust them. ***
        # DDPPO requires PyTorch distributed.
        self.framework_str = "torch"
        # Learning is no longer done on the driver process, so
        # giving GPUs to the driver does not make sense!
        self.num_gpus = 0
        # Each rollout worker gets a GPU.
        self.num_gpus_per_worker = 1
        # Note: This is the train_batch_size per worker (updates happen on worker
        # side). `rollout_fragment_length` (if "auto") is computed as:
        # `train_batch_size` // `num_envs_per_worker`.
        self.train_batch_size = 500
        # Kl divergence penalty should be fixed to 0 in DDPPO because in order
        # for it to be used as a penalty, we would have to un-decentralize
        # DDPPO
        self.kl_coeff = 0.0
        self.kl_target = 0.0
        # TODO (Kourosh) RLModule and Learner API is not supported yet
        self._enable_learner_api = False
        self._enable_rl_module_api = False
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

    @override(PPOConfig)
    def training(
        self,
        *,
        keep_local_weights_in_sync: Optional[bool] = NotProvided,
        torch_distributed_backend: Optional[str] = NotProvided,
        **kwargs,
    ) -> "DDPPOConfig":
        """Sets the training related configuration.

        Args:
            keep_local_weights_in_sync: Download weights between each training step.
                This adds a bit of overhead but allows the user to access the weights
                from the Algorithm.
            torch_distributed_backend: The communication backend for PyTorch
                distributed.

        Returns:
            This updated AlgorithmConfig object.
        """
        # Pass kwargs onto super's `training()` method.
        super().training(**kwargs)

        if keep_local_weights_in_sync is not NotProvided:
            self.keep_local_weights_in_sync = keep_local_weights_in_sync
        if torch_distributed_backend is not NotProvided:
            self.torch_distributed_backend = torch_distributed_backend

        return self

    @override(PPOConfig)
    def validate(self) -> None:
        # Call super's validation method.
        super().validate()

        # Must have `num_rollout_workers` >= 1.
        if self.num_rollout_workers < 1:
            raise ValueError(
                "Due to its distributed, decentralized nature, "
                "DD-PPO requires `num_workers` to be >= 1!"
            )

        # Only supported for PyTorch so far.
        if self.framework_str != "torch":
            raise ValueError("Distributed data parallel is only supported for PyTorch")
        if self.torch_distributed_backend not in ("gloo", "mpi", "nccl"):
            raise ValueError(
                "Only gloo, mpi, or nccl is supported for "
                "the backend of PyTorch distributed."
            )
        # `num_gpus` must be 0/None, since all optimization happens on Workers.
        if self.num_gpus:
            raise ValueError(
                "When using distributed data parallel, you should set "
                "num_gpus=0 since all optimization "
                "is happening on workers. Enable GPUs for workers by setting "
                "num_gpus_per_worker=1."
            )
        # `batch_mode` must be "truncate_episodes".
        if not self.in_evaluation and self.batch_mode != "truncate_episodes":
            raise ValueError(
                "Distributed data parallel requires truncate_episodes batch mode."
            )

        # DDPPO doesn't support KL penalties like PPO-1.
        # In order to support KL penalties, DDPPO would need to become
        # undecentralized, which defeats the purpose of the algorithm.
        # Users can still tune the entropy coefficient to control the
        # policy entropy (similar to controlling the KL penalty).
        if self.kl_coeff != 0.0 or self.kl_target != 0.0:
            raise ValueError(
                "Invalid zero-values for `kl_coeff` and/or `kl_target`! "
                "DDPPO doesn't support KL penalties like PPO-1!"
            )

    @override(AlgorithmConfig)
    def get_rollout_fragment_length(self, worker_index: int = 0) -> int:
        if self.rollout_fragment_length == "auto":
            # Example:
            # 2 workers (ignored as learning happens on workers),
            # 2 envs per worker, 100 train batch size:
            # -> 100 / 2 -> 50
            # 4 workers (ignored), 3 envs per worker, 1500 train batch size:
            # -> 1500 / 3 -> 500
            rollout_fragment_length = self.train_batch_size // (
                self.num_envs_per_worker
            )
            return rollout_fragment_length
        else:
            return self.rollout_fragment_length


@Deprecated(
    old="rllib/algorithms/ddppo/",
    new="rllib_contrib/ddppo/",
    help=ALGO_DEPRECATION_WARNING,
    error=False,
)
class DDPPO(PPO):
    @classmethod
    @override(PPO)
    def get_default_config(cls) -> AlgorithmConfig:
        return DDPPOConfig()

    @override(PPO)
    def setup(self, config: AlgorithmConfig):
        super().setup(config)

        # Initialize torch process group for
        self._curr_learner_info = {}

        worker_ids = self.workers.healthy_worker_ids()
        assert worker_ids, "No healthy rollout workers."

        # Find IP and Port of the first remote worker. This is our Rank 0  worker.
        ip = self.workers.foreach_worker(
            func=lambda w: w.get_node_ip(),
            remote_worker_ids=[worker_ids[0]],
            local_worker=False,
        )[0]
        port = self.workers.foreach_worker(
            func=lambda w: w.find_free_port(),
            remote_worker_ids=[worker_ids[0]],
            local_worker=False,
        )[0]
        address = "tcp://{ip}:{port}".format(ip=ip, port=port)
        logger.info("Creating torch process group with leader {}".format(address))

        # Get setup tasks in order to throw errors on failure.
        world_size = self.workers.num_remote_workers()
        backend = self.config.torch_distributed_backend

        def get_setup_fn(world_rank):
            return lambda w: w.setup_torch_data_parallel(
                url=address,
                world_rank=world_rank,
                world_size=world_size,
                backend=backend,
            )

        funcs = [get_setup_fn(i) for i in range(world_size)]
        # Set up torch distributed on all workers. The assumption here is that
        # all workers should be healthy at this point.
        self.workers.foreach_worker(func=funcs, local_worker=False, healthy_only=False)

        logger.info("Torch process group init completed")

    @override(PPO)
    def training_step(self) -> ResultDict:
        self.workers.foreach_worker_async(
            func=self._sample_and_train_torch_distributed,
            healthy_only=False,
        )
        sample_and_update_results = self.workers.fetch_ready_async_reqs(
            timeout_seconds=0.03
        )

        # For all results collected:
        # - Update our counters and timers.
        # - Update the worker's global_vars.
        # - Build info dict using a LearnerInfoBuilder object.
        learner_info_builder = LearnerInfoBuilder(num_devices=1)
        sampled_workers = set()
        for worker_id, result in sample_and_update_results:
            sampled_workers.add(worker_id)

            self._counters[NUM_AGENT_STEPS_SAMPLED] += result["agent_steps"]
            self._counters[NUM_AGENT_STEPS_TRAINED] += result["agent_steps"]
            self._counters[NUM_ENV_STEPS_SAMPLED] += result["env_steps"]
            self._counters[NUM_ENV_STEPS_TRAINED] += result["env_steps"]
            self._timers[LEARN_ON_BATCH_TIMER].push(result["learn_on_batch_time"])
            self._timers[SAMPLE_TIMER].push(result["sample_time"])

            # Add partial learner info to builder object.
            learner_info_builder.add_learn_on_batch_results_multi_agent(result["info"])

        # Broadcast the local set of global vars to this worker.
        global_vars = {"timestep": self._counters[NUM_AGENT_STEPS_SAMPLED]}
        self.workers.foreach_worker(
            func=lambda w: w.set_global_vars(global_vars),
            local_worker=False,
            remote_worker_ids=list(sampled_workers),
            timeout_seconds=0,  # Don't wait for workers to finish.
        )

        # Sync down the weights from 1st remote worker (only if we have received
        # some results from it).
        # As with the sync up, this is not really needed unless the user is
        # reading the local weights.
        worker_ids = self.workers.healthy_worker_ids()
        assert worker_ids, "No healthy rollout workers?"
        if self.config.keep_local_weights_in_sync and worker_ids[0] in sampled_workers:
            weights = self.workers.foreach_worker(
                func=lambda w: w.get_weights(),
                local_worker=False,
                remote_worker_ids=[worker_ids[0]],
            )
            self.workers.local_worker().set_weights(weights[0])
        # Return merged laarner into results.
        new_learner_info = learner_info_builder.finalize()
        if new_learner_info:
            self._curr_learner_info = new_learner_info
        return self._curr_learner_info

    @staticmethod
    def _sample_and_train_torch_distributed(worker: RolloutWorker):
        # This function is applied remotely on each rollout worker.
        config: AlgorithmConfig = worker.config

        # Generate a sample.
        start = time.perf_counter()
        batch = worker.sample()
        sample_time = time.perf_counter() - start
        expected_batch_size = (
            config.get_rollout_fragment_length() * config.num_envs_per_worker
        )
        assert batch.count == expected_batch_size, (
            "Batch size possibly out of sync between workers, expected:",
            expected_batch_size,
            "got:",
            batch.count,
        )

        # Perform n minibatch SGD update(s) on the worker itself.
        start = time.perf_counter()
        info = do_minibatch_sgd(
            batch,
            worker.policy_map,
            worker,
            config.num_sgd_iter,
            config.sgd_minibatch_size,
            [Postprocessing.ADVANTAGES],
        )
        learn_on_batch_time = time.perf_counter() - start
        return {
            "info": info,
            "env_steps": batch.env_steps(),
            "agent_steps": batch.agent_steps(),
            "sample_time": sample_time,
            "learn_on_batch_time": learn_on_batch_time,
        }
