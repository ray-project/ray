"""
Decentralized Distributed PPO (DD-PPO)
======================================

Unlike APPO or PPO, learning is no longer done centralized in the trainer
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
from typing import Callable, Optional, Union

import ray
from ray.rllib.agents.ppo.ppo import DEFAULT_CONFIG as PPO_DEFAULT_CONFIG, PPOTrainer
from ray.rllib.agents.trainer import Trainer
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.evaluation.rollout_worker import RolloutWorker
from ray.rllib.execution.common import (
    STEPS_TRAINED_THIS_ITER_COUNTER,
)
from ray.rllib.execution.parallel_requests import asynchronous_parallel_requests
from ray.rllib.utils.annotations import override
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
    EnvType,
    PartialTrainerConfigDict,
    ResultDict,
    TrainerConfigDict,
)
from ray.tune.logger import Logger

logger = logging.getLogger(__name__)

# fmt: off
# __sphinx_doc_begin__

# Adds the following updates to the `PPOTrainer` config in
# rllib/agents/ppo/ppo.py.
DEFAULT_CONFIG = Trainer.merge_trainer_configs(
    PPO_DEFAULT_CONFIG,
    {
        # During the sampling phase, each rollout worker will collect a batch
        # `rollout_fragment_length * num_envs_per_worker` steps in size.
        "rollout_fragment_length": 100,
        # Vectorize the env (should enable by default since each worker has
        # a GPU).
        "num_envs_per_worker": 5,
        # During the SGD phase, workers iterate over minibatches of this size.
        # The effective minibatch size will be:
        # `sgd_minibatch_size * num_workers`.
        "sgd_minibatch_size": 50,
        # Number of SGD epochs per optimization round.
        "num_sgd_iter": 10,
        # Download weights between each training step. This adds a bit of
        # overhead but allows the user to access the weights from the trainer.
        "keep_local_weights_in_sync": True,

        # *** WARNING: configs below are DDPPO overrides over PPO; you
        #     shouldn't need to adjust them. ***
        # DDPPO requires PyTorch distributed.
        "framework": "torch",
        # The communication backend for PyTorch distributed.
        "torch_distributed_backend": "gloo",
        # Learning is no longer done on the driver process, so
        # giving GPUs to the driver does not make sense!
        "num_gpus": 0,
        # Each rollout worker gets a GPU.
        "num_gpus_per_worker": 1,
        # This is auto set based on sample batch size.
        "train_batch_size": -1,
        # Kl divergence penalty should be fixed to 0 in DDPPO because in order
        # for it to be used as a penalty, we would have to un-decentralize
        # DDPPO
        "kl_coeff": 0.0,
        "kl_target": 0.0,
    },
    _allow_unknown_configs=True,
)

# __sphinx_doc_end__
# fmt: on


class DDPPOTrainer(PPOTrainer):
    def __init__(
        self,
        config: Optional[PartialTrainerConfigDict] = None,
        env: Optional[Union[str, EnvType]] = None,
        logger_creator: Optional[Callable[[], Logger]] = None,
        remote_checkpoint_dir: Optional[str] = None,
        sync_function_tpl: Optional[str] = None,
    ):
        """Initializes a DDPPOTrainer instance.

        Args:
            config: Algorithm-specific configuration dict.
            env: Name of the environment to use (e.g. a gym-registered str),
                a full class path (e.g.
                "ray.rllib.examples.env.random_env.RandomEnv"), or an Env
                class directly. Note that this arg can also be specified via
                the "env" key in `config`.
            logger_creator: Callable that creates a ray.tune.Logger
                object. If unspecified, a default logger is created.
        """
        super().__init__(
            config, env, logger_creator, remote_checkpoint_dir, sync_function_tpl
        )

        if "train_batch_size" in config.keys() and config["train_batch_size"] != -1:
            # Users should not define `train_batch_size` directly (always -1).
            raise ValueError(
                "Set rollout_fragment_length instead of train_batch_size for DDPPO."
            )

        # Auto-train_batch_size: Calculate from rollout len and
        # envs-per-worker.
        config["train_batch_size"] = config.get(
            "rollout_fragment_length", DEFAULT_CONFIG["rollout_fragment_length"]
        ) * config.get("num_envs_per_worker", DEFAULT_CONFIG["num_envs_per_worker"])

    @classmethod
    @override(PPOTrainer)
    def get_default_config(cls) -> TrainerConfigDict:
        return DEFAULT_CONFIG

    @override(PPOTrainer)
    def validate_config(self, config):
        """Validates the Trainer's config dict.

        Args:
            config (TrainerConfigDict): The Trainer's config to check.

        Raises:
            ValueError: In case something is wrong with the config.
        """
        # Call (base) PPO's config validation function first.
        # Note that this will not touch or check on the train_batch_size=-1
        # setting.
        super().validate_config(config)

        # Must have `num_workers` >= 1.
        if config["num_workers"] < 1:
            raise ValueError(
                "Due to its distributed, decentralized nature, "
                "DD-PPO requires `num_workers` to be >= 1!"
            )

        # Only supported for PyTorch so far.
        if config["framework"] != "torch":
            raise ValueError("Distributed data parallel is only supported for PyTorch")
        if config["torch_distributed_backend"] not in ("gloo", "mpi", "nccl"):
            raise ValueError(
                "Only gloo, mpi, or nccl is supported for "
                "the backend of PyTorch distributed."
            )
        # `num_gpus` must be 0/None, since all optimization happens on Workers.
        if config["num_gpus"]:
            raise ValueError(
                "When using distributed data parallel, you should set "
                "num_gpus=0 since all optimization "
                "is happening on workers. Enable GPUs for workers by setting "
                "num_gpus_per_worker=1."
            )
        # `batch_mode` must be "truncate_episodes".
        if config["batch_mode"] != "truncate_episodes":
            raise ValueError(
                "Distributed data parallel requires truncate_episodes batch mode."
            )
        # DDPPO doesn't support KL penalties like PPO-1.
        # In order to support KL penalties, DDPPO would need to become
        # undecentralized, which defeats the purpose of the algorithm.
        # Users can still tune the entropy coefficient to control the
        # policy entropy (similar to controlling the KL penalty).
        if config["kl_coeff"] != 0.0 or config["kl_target"] != 0.0:
            raise ValueError("DDPPO doesn't support KL penalties like PPO-1")

    @override(PPOTrainer)
    def setup(self, config: PartialTrainerConfigDict):
        super().setup(config)

        # Initialize torch process group for
        if self.config["_disable_execution_plan_api"] is True:
            self._curr_learner_info = {}
            ip = ray.get(self.workers.remote_workers()[0].get_node_ip.remote())
            port = ray.get(self.workers.remote_workers()[0].find_free_port.remote())
            address = "tcp://{ip}:{port}".format(ip=ip, port=port)
            logger.info("Creating torch process group with leader {}".format(address))

            # Get setup tasks in order to throw errors on failure.
            ray.get(
                [
                    worker.setup_torch_data_parallel.remote(
                        url=address,
                        world_rank=i,
                        world_size=len(self.workers.remote_workers()),
                        backend=self.config["torch_distributed_backend"],
                    )
                    for i, worker in enumerate(self.workers.remote_workers())
                ]
            )
            logger.info("Torch process group init completed")

    @override(PPOTrainer)
    def training_iteration(self) -> ResultDict:
        # Shortcut.
        first_worker = self.workers.remote_workers()[0]

        # Run sampling and update steps on each worker in asynchronous fashion.
        sample_and_update_results = asynchronous_parallel_requests(
            remote_requests_in_flight=self.remote_requests_in_flight,
            actors=self.workers.remote_workers(),
            ray_wait_timeout_s=0.0,
            max_remote_requests_in_flight_per_actor=1,  # 2
            remote_fn=self._sample_and_train_torch_distributed,
        )

        # For all results collected:
        # - Update our counters and timers.
        # - Update the worker's global_vars.
        # - Build info dict using a LearnerInfoBuilder object.
        learner_info_builder = LearnerInfoBuilder(num_devices=1)
        steps_this_iter = 0
        for worker, results in sample_and_update_results.items():
            for result in results:
                steps_this_iter += result["env_steps"]
                self._counters[NUM_AGENT_STEPS_SAMPLED] += result["agent_steps"]
                self._counters[NUM_AGENT_STEPS_TRAINED] += result["agent_steps"]
                self._counters[NUM_ENV_STEPS_SAMPLED] += result["env_steps"]
                self._counters[NUM_ENV_STEPS_TRAINED] += result["env_steps"]
                self._timers[LEARN_ON_BATCH_TIMER].push(result["learn_on_batch_time"])
                self._timers[SAMPLE_TIMER].push(result["sample_time"])
            # Add partial learner info to builder object.
            learner_info_builder.add_learn_on_batch_results_multi_agent(result["info"])

            # Broadcast the local set of global vars.
            global_vars = {"timestep": self._counters[NUM_AGENT_STEPS_SAMPLED]}
            for worker in self.workers.remote_workers():
                worker.set_global_vars.remote(global_vars)

        self._counters[STEPS_TRAINED_THIS_ITER_COUNTER] = steps_this_iter

        # Sync down the weights from 1st remote worker (only if we have received
        # some results from it).
        # As with the sync up, this is not really needed unless the user is
        # reading the local weights.
        if (
            self.config["keep_local_weights_in_sync"]
            and first_worker in sample_and_update_results
        ):
            self.workers.local_worker().set_weights(
                ray.get(first_worker.get_weights.remote())
            )
        # Return merged laarner into results.
        new_learner_info = learner_info_builder.finalize()
        if new_learner_info:
            self._curr_learner_info = new_learner_info
        return self._curr_learner_info

    @staticmethod
    def _sample_and_train_torch_distributed(worker: RolloutWorker):
        # This function is applied remotely on each rollout worker.
        config = worker.policy_config

        # Generate a sample.
        start = time.perf_counter()
        batch = worker.sample()
        sample_time = time.perf_counter() - start
        expected_batch_size = (
            config["rollout_fragment_length"] * config["num_envs_per_worker"]
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
            config["num_sgd_iter"],
            config["sgd_minibatch_size"],
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
