"""Decentralized Distributed PPO implementation.

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

import ray
from ray.rllib.agents.ppo import ppo
from ray.rllib.agents.trainer import with_base_config
from ray.rllib.execution.rollout_ops import ParallelRollouts
from ray.rllib.execution.metric_ops import StandardMetricsReporting
from ray.rllib.execution.common import STEPS_SAMPLED_COUNTER, \
    STEPS_TRAINED_COUNTER, LEARNER_INFO, LEARN_ON_BATCH_TIMER, \
    _get_shared_metrics
from ray.rllib.evaluation.rollout_worker import get_global_worker
from ray.rllib.utils.sgd import do_minibatch_sgd

logger = logging.getLogger(__name__)

# yapf: disable
# __sphinx_doc_begin__
DEFAULT_CONFIG = with_base_config(ppo.DEFAULT_CONFIG, {
    # During the sampling phase, each rollout worker will collect a batch
    # `rollout_fragment_length * num_envs_per_worker` steps in size.
    "rollout_fragment_length": 100,
    # Vectorize the env (should enable by default since each worker has a GPU).
    "num_envs_per_worker": 5,
    # During the SGD phase, workers iterate over minibatches of this size.
    # The effective minibatch size will be `sgd_minibatch_size * num_workers`.
    "sgd_minibatch_size": 50,
    # Number of SGD epochs per optimization round.
    "num_sgd_iter": 10,
    # Download weights between each training step. This adds a bit of overhead
    # but allows the user to access the weights from the trainer.
    "keep_local_weights_in_sync": True,

    # *** WARNING: configs below are DDPPO overrides over PPO; you
    #     shouldn't need to adjust them. ***
    "framework": "torch",  # DDPPO requires PyTorch distributed.
    "num_gpus": 0,  # Learning is no longer done on the driver process, so
                    # giving GPUs to the driver does not make sense!
    "num_gpus_per_worker": 1,  # Each rollout worker gets a GPU.
    "truncate_episodes": True,  # Require evenly sized batches. Otherwise,
                                # collective allreduce could fail.
    "train_batch_size": -1,  # This is auto set based on sample batch size.
})
# __sphinx_doc_end__
# yapf: enable


def validate_config(config):
    if config["train_batch_size"] == -1:
        # Auto set.
        config["train_batch_size"] = (
            config["rollout_fragment_length"] * config["num_envs_per_worker"])
    else:
        raise ValueError(
            "Set rollout_fragment_length instead of train_batch_size "
            "for DDPPO.")
    if config["framework"] != "torch":
        raise ValueError(
            "Distributed data parallel is only supported for PyTorch")
    if config["num_gpus"]:
        raise ValueError(
            "When using distributed data parallel, you should set "
            "num_gpus=0 since all optimization "
            "is happening on workers. Enable GPUs for workers by setting "
            "num_gpus_per_worker=1.")
    if config["batch_mode"] != "truncate_episodes":
        raise ValueError(
            "Distributed data parallel requires truncate_episodes "
            "batch mode.")
    ppo.validate_config(config)


def execution_plan(workers, config):
    rollouts = ParallelRollouts(workers, mode="raw")

    # Setup the distributed processes.
    if not workers.remote_workers():
        raise ValueError("This optimizer requires >0 remote workers.")
    ip = ray.get(workers.remote_workers()[0].get_node_ip.remote())
    port = ray.get(workers.remote_workers()[0].find_free_port.remote())
    address = "tcp://{ip}:{port}".format(ip=ip, port=port)
    logger.info("Creating torch process group with leader {}".format(address))

    # Get setup tasks in order to throw errors on failure.
    ray.get([
        worker.setup_torch_data_parallel.remote(
            address, i, len(workers.remote_workers()), backend="gloo")
        for i, worker in enumerate(workers.remote_workers())
    ])
    logger.info("Torch process group init completed")

    # This function is applied remotely on each rollout worker.
    def train_torch_distributed_allreduce(batch):
        expected_batch_size = (
            config["rollout_fragment_length"] * config["num_envs_per_worker"])
        this_worker = get_global_worker()
        assert batch.count == expected_batch_size, \
            ("Batch size possibly out of sync between workers, expected:",
             expected_batch_size, "got:", batch.count)
        logger.info("Executing distributed minibatch SGD "
                    "with epoch size {}, minibatch size {}".format(
                        batch.count, config["sgd_minibatch_size"]))
        info = do_minibatch_sgd(batch, this_worker.policy_map, this_worker,
                                config["num_sgd_iter"],
                                config["sgd_minibatch_size"], ["advantages"])
        return info, batch.count

    # Have to manually record stats since we are using "raw" rollouts mode.
    class RecordStats:
        def _on_fetch_start(self):
            self.fetch_start_time = time.perf_counter()

        def __call__(self, items):
            for item in items:
                info, count = item
                metrics = _get_shared_metrics()
                metrics.counters[STEPS_SAMPLED_COUNTER] += count
                metrics.counters[STEPS_TRAINED_COUNTER] += count
                metrics.info[LEARNER_INFO] = info
            # Since SGD happens remotely, the time delay between fetch and
            # completion is approximately the SGD step time.
            metrics.timers[LEARN_ON_BATCH_TIMER].push(time.perf_counter() -
                                                      self.fetch_start_time)

    train_op = (
        rollouts.for_each(train_torch_distributed_allreduce)  # allreduce
        .batch_across_shards()  # List[(grad_info, count)]
        .for_each(RecordStats()))

    # Sync down the weights. As with the sync up, this is not really
    # needed unless the user is reading the local weights.
    if config["keep_local_weights_in_sync"]:

        def download_weights(item):
            workers.local_worker().set_weights(
                ray.get(workers.remote_workers()[0].get_weights.remote()))
            return item

        train_op = train_op.for_each(download_weights)

    # In debug mode, check the allreduce successfully synced the weights.
    if logger.isEnabledFor(logging.DEBUG):

        def check_sync(item):
            weights = ray.get(
                [w.get_weights.remote() for w in workers.remote_workers()])
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


DDPPOTrainer = ppo.PPOTrainer.with_updates(
    name="DDPPO",
    default_config=DEFAULT_CONFIG,
    execution_plan=execution_plan,
    validate_config=validate_config)
