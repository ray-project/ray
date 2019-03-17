"""Helper class for AsyncSamplesOptimizer."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import logging
import time

import ray
from ray.rllib.utils.actors import TaskPool
from ray.rllib.utils.annotations import override
from ray.rllib.optimizers.aso_aggregator import Aggregator, \
    AggregationWorkerBase

logger = logging.getLogger(__name__)


class TreeAggregator(Aggregator):
    """A hierarchical experiences aggregator.

    The given set of remote evaluators is divided into subsets and assigned to
    one of several aggregation workers. These aggregation workers collate
    experiences into batches of size `train_batch_size` and we collect them
    in this class when `iter_train_batches` is called.
    """

    def __init__(self,
                 local_evaluator,
                 remote_evaluators,
                 num_aggregation_workers,
                 max_sample_requests_in_flight_per_worker=2,
                 replay_proportion=0.0,
                 replay_buffer_num_slots=0,
                 train_batch_size=500,
                 sample_batch_size=50,
                 broadcast_interval=5):
        self.local_evaluator = local_evaluator
        self.broadcasted_weights = ray.put(local_evaluator.get_weights())
        self.num_batches_processed = 0
        self.num_broadcasts = 0
        self.broadcast_interval = broadcast_interval
        self.num_sent_since_broadcast = 0

        if len(remote_evaluators) < num_aggregation_workers:
            raise ValueError(
                "The number of aggregation workers should not exceed the "
                "number of total evaluation workers ({} vs {})".format(
                    num_aggregation_workers, len(remote_evaluators)))

        assigned_evaluators = collections.defaultdict(list)
        for i, ev in enumerate(remote_evaluators):
            assigned_evaluators[i % num_aggregation_workers].append(ev)

        self.workers = [
            AggregationWorker.remote(
                self.broadcasted_weights, assigned_evaluators[i],
                max_sample_requests_in_flight_per_worker, replay_proportion,
                replay_buffer_num_slots, train_batch_size, sample_batch_size)
            for i in range(num_aggregation_workers)
        ]

        self.agg_tasks = TaskPool()
        for agg in self.workers:
            agg.set_weights.remote(self.broadcasted_weights)
            for _ in range(max_sample_requests_in_flight_per_worker):
                self.agg_tasks.add(agg, agg.get_train_batches.remote())

    @override(Aggregator)
    def iter_train_batches(self):
        for agg, batches in self.agg_tasks.completed_prefetch():
            self.agg_tasks.add(agg, agg.get_train_batches.remote())
            for b in ray.get(batches):
                self.num_sent_since_broadcast += 1
                yield b
            agg.set_weights.remote(self.broadcasted_weights)
            self.num_batches_processed += 1

    @override(Aggregator)
    def broadcast_new_weights(self):
        self.broadcasted_weights = ray.put(self.local_evaluator.get_weights())
        self.num_sent_since_broadcast = 0
        self.num_broadcasts += 1

    @override(Aggregator)
    def should_broadcast(self):
        return self.num_sent_since_broadcast >= self.broadcast_interval

    @override(Aggregator)
    def stats(self):
        return {
            "num_broadcasts": self.num_broadcasts,
            "num_batches_processed": self.num_batches_processed,
        }


@ray.remote(num_cpus=1)
class AggregationWorker(AggregationWorkerBase):
    def __init__(self, initial_weights_obj_id, remote_evaluators,
                 max_sample_requests_in_flight_per_worker, replay_proportion,
                 replay_buffer_num_slots, train_batch_size, sample_batch_size):
        logger.info("Assigned evaluators {} to aggregation worker {}".format(
            remote_evaluators, self))
        assert remote_evaluators
        AggregationWorkerBase.__init__(
            self, initial_weights_obj_id, remote_evaluators,
            max_sample_requests_in_flight_per_worker, replay_proportion,
            replay_buffer_num_slots, train_batch_size, sample_batch_size)

    def set_weights(self, weights):
        self.broadcasted_weights = weights

    def get_train_batches(self):
        result = []
        for batch in self.iter_train_batches():
            result.append(batch)
        while not result:
            time.sleep(0.01)
            for batch in self.iter_train_batches():
                result.append(batch)
        return result
