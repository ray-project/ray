import logging
import platform
from typing import List, Dict, Any

import ray
from ray.rllib.evaluation.worker_set import WorkerSet
from ray.rllib.execution.common import (
    AGENT_STEPS_SAMPLED_COUNTER,
    STEPS_SAMPLED_COUNTER,
    _get_shared_metrics,
)
from ray.rllib.execution.replay_ops import MixInReplay
from ray.rllib.execution.rollout_ops import ParallelRollouts, ConcatBatches
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils.actors import create_colocated_actors
from ray.rllib.utils.typing import SampleBatchType, ModelWeights
from ray.util.iter import (
    ParallelIterator,
    ParallelIteratorWorker,
    from_actors,
    LocalIterator,
)

logger = logging.getLogger(__name__)


@ray.remote(num_cpus=0)
class Aggregator(ParallelIteratorWorker):
    """An aggregation worker used by gather_experiences_tree_aggregation().

    Each of these actors is a shard of a parallel iterator that consumes
    batches from RolloutWorker actors, and emits batches of size
    train_batch_size. This allows expensive decompression / concatenation
    work to be offloaded to these actors instead of run in the learner.
    """

    def __init__(
        self, config: Dict, rollout_group: "ParallelIterator[SampleBatchType]"
    ):
        self.weights = None
        self.global_vars = None

        def generator():
            it = rollout_group.gather_async(
                num_async=config["max_sample_requests_in_flight_per_worker"]
            )

            # Update the rollout worker with our latest policy weights.
            def update_worker(item):
                worker, batch = item
                if self.weights:
                    worker.set_weights.remote(self.weights, self.global_vars)
                return batch

            # Augment with replay and concat to desired train batch size.
            it = (
                it.zip_with_source_actor()
                .for_each(update_worker)
                .for_each(lambda batch: batch.decompress_if_needed())
                .for_each(
                    MixInReplay(
                        num_slots=config["replay_buffer_num_slots"],
                        replay_proportion=config["replay_proportion"],
                    )
                )
                .flatten()
                .combine(
                    ConcatBatches(
                        min_batch_size=config["train_batch_size"],
                        count_steps_by=config["multiagent"]["count_steps_by"],
                    )
                )
            )

            for train_batch in it:
                yield train_batch

        super().__init__(generator, repeat=False)

    def get_host(self) -> str:
        return platform.node()

    def set_weights(self, weights: ModelWeights, global_vars: Dict) -> None:
        self.weights = weights
        self.global_vars = global_vars


def gather_experiences_tree_aggregation(
    workers: WorkerSet, config: Dict
) -> "LocalIterator[Any]":
    """Tree aggregation version of gather_experiences_directly()."""

    rollouts = ParallelRollouts(workers, mode="raw")

    # Divide up the workers between aggregators.
    worker_assignments = [[] for _ in range(config["num_aggregation_workers"])]
    i = 0
    for worker_idx in range(len(workers.remote_workers())):
        worker_assignments[i].append(worker_idx)
        i += 1
        i %= len(worker_assignments)
    logger.info("Worker assignments: {}".format(worker_assignments))

    # Create parallel iterators that represent each aggregation group.
    rollout_groups: List["ParallelIterator[SampleBatchType]"] = [
        rollouts.select_shards(assigned) for assigned in worker_assignments
    ]

    # This spawns |num_aggregation_workers| intermediate actors that aggregate
    # experiences in parallel. We force colocation on the same node (localhost)
    # to maximize data bandwidth between them and the driver.
    localhost = platform.node()
    assert localhost != "", (
        "ERROR: Cannot determine local node name! "
        "`platform.node()` returned empty string."
    )
    all_co_located = create_colocated_actors(
        actor_specs=[
            # (class, args, kwargs={}, count=1)
            (Aggregator, [config, g], {}, 1)
            for g in rollout_groups
        ],
        node=localhost,
    )

    # Use the first ([0]) of each created group (each group only has one
    # actor: count=1).
    train_batches = from_actors([group[0] for group in all_co_located])

    # TODO(ekl) properly account for replay.
    def record_steps_sampled(batch):
        metrics = _get_shared_metrics()
        metrics.counters[STEPS_SAMPLED_COUNTER] += batch.count
        if isinstance(batch, MultiAgentBatch):
            metrics.counters[AGENT_STEPS_SAMPLED_COUNTER] += batch.agent_steps()
        else:
            metrics.counters[AGENT_STEPS_SAMPLED_COUNTER] += batch.count
        return batch

    return train_batches.gather_async().for_each(record_steps_sampled)
