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
import sys
import time
from typing import Callable, Optional, Union

import ray
from ray.rllib.agents.ppo.ppo import DEFAULT_CONFIG as PPO_DEFAULT_CONFIG, PPOTrainer
from ray.rllib.agents.trainer import Trainer
from ray.rllib.evaluation.worker_set import WorkerSet
from ray.rllib.execution.rollout_ops import ParallelRollouts
from ray.rllib.execution.metric_ops import StandardMetricsReporting
from ray.rllib.execution.common import (
    STEPS_SAMPLED_COUNTER,
    STEPS_TRAINED_COUNTER,
    STEPS_TRAINED_THIS_ITER_COUNTER,
    LEARN_ON_BATCH_TIMER,
    _get_shared_metrics,
    _get_global_vars,
)
from ray.rllib.evaluation.rollout_worker import get_global_worker
from ray.rllib.utils.annotations import override
from ray.rllib.utils.metrics.learner_info import LEARNER_INFO
from ray.rllib.utils.sgd import do_minibatch_sgd
from ray.rllib.utils.typing import EnvType, PartialTrainerConfigDict, TrainerConfigDict
from ray.tune.logger import Logger
from ray.util.iter import LocalIterator

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
        # Require evenly sized batches. Otherwise,
        # collective allreduce could fail.
        "truncate_episodes": True,
        # This is auto set based on sample batch size.
        "train_batch_size": -1,
        # Kl divergence penalty should be fixed to 0 in DDPPO because in order
        # for it to be used as a penalty, we would have to un-decentralize
        # DDPPO
        "kl_coeff": 0.0,
        "kl_target": 0.0
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

        # Error if run on Win.
        if sys.platform in ["win32", "cygwin"]:
            raise ValueError(
                "DD-PPO not supported on Win yet! Due to usage of torch.distributed."
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

    @staticmethod
    @override(PPOTrainer)
    def execution_plan(
        workers: WorkerSet, config: TrainerConfigDict, **kwargs
    ) -> LocalIterator[dict]:
        """Execution plan of the DD-PPO algorithm. Defines the distributed dataflow.

        Args:
            workers (WorkerSet): The WorkerSet for training the Polic(y/ies)
                of the Trainer.
            config (TrainerConfigDict): The trainer's configuration dict.

        Returns:
            LocalIterator[dict]: The Policy class to use with PGTrainer.
                If None, use `default_policy` provided in build_trainer().
        """
        assert (
            len(kwargs) == 0
        ), "DDPPO execution_plan does NOT take any additional parameters"

        rollouts = ParallelRollouts(workers, mode="raw")

        # Setup the distributed processes.
        if not workers.remote_workers():
            raise ValueError("This optimizer requires >0 remote workers.")
        ip = ray.get(workers.remote_workers()[0].get_node_ip.remote())
        port = ray.get(workers.remote_workers()[0].find_free_port.remote())
        address = "tcp://{ip}:{port}".format(ip=ip, port=port)
        logger.info("Creating torch process group with leader {}".format(address))

        # Get setup tasks in order to throw errors on failure.
        ray.get(
            [
                worker.setup_torch_data_parallel.remote(
                    url=address,
                    world_rank=i,
                    world_size=len(workers.remote_workers()),
                    backend=config["torch_distributed_backend"],
                )
                for i, worker in enumerate(workers.remote_workers())
            ]
        )
        logger.info("Torch process group init completed")

        # This function is applied remotely on each rollout worker.
        def train_torch_distributed_allreduce(batch):
            expected_batch_size = (
                config["rollout_fragment_length"] * config["num_envs_per_worker"]
            )
            this_worker = get_global_worker()
            assert batch.count == expected_batch_size, (
                "Batch size possibly out of sync between workers, expected:",
                expected_batch_size,
                "got:",
                batch.count,
            )
            logger.info(
                "Executing distributed minibatch SGD "
                "with epoch size {}, minibatch size {}".format(
                    batch.count, config["sgd_minibatch_size"]
                )
            )
            info = do_minibatch_sgd(
                batch,
                this_worker.policy_map,
                this_worker,
                config["num_sgd_iter"],
                config["sgd_minibatch_size"],
                ["advantages"],
            )
            return info, batch.count

        # Broadcast the local set of global vars.
        def update_worker_global_vars(item):
            global_vars = _get_global_vars()
            for w in workers.remote_workers():
                w.set_global_vars.remote(global_vars)
            return item

        # Have to manually record stats since we are using "raw" rollouts mode.
        class RecordStats:
            def _on_fetch_start(self):
                self.fetch_start_time = time.perf_counter()

            def __call__(self, items):
                for item in items:
                    info, count = item
                    metrics = _get_shared_metrics()
                    metrics.counters[STEPS_TRAINED_THIS_ITER_COUNTER] = count
                    metrics.counters[STEPS_SAMPLED_COUNTER] += count
                    metrics.counters[STEPS_TRAINED_COUNTER] += count
                    metrics.info[LEARNER_INFO] = info
                # Since SGD happens remotely, the time delay between fetch and
                # completion is approximately the SGD step time.
                metrics.timers[LEARN_ON_BATCH_TIMER].push(
                    time.perf_counter() - self.fetch_start_time
                )

        train_op = (
            rollouts.for_each(train_torch_distributed_allreduce)  # allreduce
            .batch_across_shards()  # List[(grad_info, count)]
            .for_each(RecordStats())
        )

        train_op = train_op.for_each(update_worker_global_vars)

        # Sync down the weights. As with the sync up, this is not really
        # needed unless the user is reading the local weights.
        if config["keep_local_weights_in_sync"]:

            def download_weights(item):
                workers.local_worker().set_weights(
                    ray.get(workers.remote_workers()[0].get_weights.remote())
                )
                return item

            train_op = train_op.for_each(download_weights)

        # In debug mode, check the allreduce successfully synced the weights.
        if logger.isEnabledFor(logging.DEBUG):

            def check_sync(item):
                weights = ray.get(
                    [w.get_weights.remote() for w in workers.remote_workers()]
                )
                sums = []
                for w in weights:
                    acc = 0
                    for p in w.values():
                        for k, v in p.items():
                            acc += v.sum()
                    sums.append(float(acc))
                logger.debug("The worker weight sums are {}".format(sums))
                assert len(set(sums)) == 1, sums

            train_op = train_op.for_each(check_sync)

        return StandardMetricsReporting(train_op, workers, config)
