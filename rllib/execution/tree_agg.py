import logging
import platform
from typing import List

import ray
from ray.rllib.execution.common import STEPS_SAMPLED_COUNTER, \
    _get_shared_metrics
from ray.rllib.execution.replay_ops import MixInReplay
from ray.rllib.execution.rollout_ops import ParallelRollouts, ConcatBatches
from ray.rllib.utils.actors import create_colocated
from ray.util.iter import ParallelIterator, ParallelIteratorWorker, \
    from_actors
from ray.rllib.utils.types import SampleBatchType

logger = logging.getLogger(__name__)


@ray.remote(num_cpus=0)
class Aggregator(ParallelIteratorWorker):
    """An aggregation worker used by gather_experiences_tree_aggregation().

    Each of these actors is a shard of a parallel iterator that consumes
    batches from RolloutWorker actors, and emits batches of size
    train_batch_size. This allows expensive decompression / concatenation
    work to be offloaded to these actors instead of run in the learner.
    """

    def __init__(self, config: dict,
                 rollout_group: "ParallelIterator[SampleBatchType]"):
        self.weights = None
        self.global_vars = None

        def generator():
            it = rollout_group.gather_async(
                num_async=config["max_sample_requests_in_flight_per_worker"])

            # Update the rollout worker with our latest policy weights.
            def update_worker(item):
                worker, batch = item
                if self.weights:
                    worker.set_weights.remote(self.weights, self.global_vars)
                return batch

            # Augment with replay and concat to desired train batch size.
            it = it.zip_with_source_actor() \
                .for_each(update_worker) \
                .for_each(lambda batch: batch.decompress_if_needed()) \
                .for_each(MixInReplay(
                    num_slots=config["replay_buffer_num_slots"],
                    replay_proportion=config["replay_proportion"])) \
                .flatten() \
                .combine(
                    ConcatBatches(
                        min_batch_size=config["train_batch_size"]))

            for train_batch in it:
                yield train_batch

        super().__init__(generator, repeat=False)

    def get_host(self):
        return platform.node()

    def set_weights(self, weights, global_vars):
        self.weights = weights
        self.global_vars = global_vars


def gather_experiences_tree_aggregation(workers, config):
    """Tree aggregation version of gather_experiences_directly()."""

    rollouts = ParallelRollouts(workers, mode="raw")

    # Divide up the workers between aggregators.
    worker_assignments = [[] for _ in range(config["num_aggregation_workers"])]
    i = 0
    for w in range(len(workers.remote_workers())):
        worker_assignments[i].append(w)
        i += 1
        i %= len(worker_assignments)
    logger.info("Worker assignments: {}".format(worker_assignments))

    # Create parallel iterators that represent each aggregation group.
    rollout_groups: List["ParallelIterator[SampleBatchType]"] = [
        rollouts.select_shards(assigned) for assigned in worker_assignments
    ]

    # This spawns |num_aggregation_workers| intermediate actors that aggregate
    # experiences in parallel. We force colocation on the same node to maximize
    # data bandwidth between them and the driver.
    train_batches = from_actors([
        create_colocated(Aggregator, [config, g], 1)[0] for g in rollout_groups
    ])

    # TODO(ekl) properly account for replay.
    def record_steps_sampled(batch):
        metrics = _get_shared_metrics()
        metrics.counters[STEPS_SAMPLED_COUNTER] += batch.count
        return batch

    return train_batches.gather_async().for_each(record_steps_sampled)
