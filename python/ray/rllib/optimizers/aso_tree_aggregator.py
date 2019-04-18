"""Helper class for AsyncSamplesOptimizer."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import logging
import os
import time

import ray
from ray.rllib.utils.actors import TaskPool, create_colocated
from ray.rllib.utils.annotations import override
from ray.rllib.optimizers.aso_aggregator import Aggregator, \
    AggregationWorkerBase
from ray.rllib.utils.memory import ray_get_and_free

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
        self.remote_evaluators = remote_evaluators
        self.num_aggregation_workers = num_aggregation_workers
        self.max_sample_requests_in_flight_per_worker = \
            max_sample_requests_in_flight_per_worker
        self.replay_proportion = replay_proportion
        self.replay_buffer_num_slots = replay_buffer_num_slots
        self.sample_batch_size = sample_batch_size
        self.train_batch_size = train_batch_size
        self.broadcast_interval = broadcast_interval
        self.broadcasted_weights = ray.put(local_evaluator.get_weights())
        self.num_batches_processed = 0
        self.num_broadcasts = 0
        self.num_sent_since_broadcast = 0
        self.initialized = False

    def init(self, aggregators):
        """Deferred init so that we can pass in previously created workers."""

        assert len(aggregators) == self.num_aggregation_workers, aggregators
        if len(self.remote_evaluators) < self.num_aggregation_workers:
            raise ValueError(
                "The number of aggregation workers should not exceed the "
                "number of total evaluation workers ({} vs {})".format(
                    self.num_aggregation_workers, len(self.remote_evaluators)))

        assigned_evaluators = collections.defaultdict(list)
        for i, ev in enumerate(self.remote_evaluators):
            assigned_evaluators[i % self.num_aggregation_workers].append(ev)

        self.workers = aggregators
        for i, worker in enumerate(self.workers):
            worker.init.remote(
                self.broadcasted_weights, assigned_evaluators[i],
                self.max_sample_requests_in_flight_per_worker,
                self.replay_proportion, self.replay_buffer_num_slots,
                self.train_batch_size, self.sample_batch_size)

        self.agg_tasks = TaskPool()
        for agg in self.workers:
            agg.set_weights.remote(self.broadcasted_weights)
            self.agg_tasks.add(agg, agg.get_train_batches.remote())

        self.initialized = True

    @override(Aggregator)
    def iter_train_batches(self):
        assert self.initialized, "Must call init() before using this class."
        for agg, batches in self.agg_tasks.completed_prefetch():
            for b in ray_get_and_free(batches):
                self.num_sent_since_broadcast += 1
                yield b
            agg.set_weights.remote(self.broadcasted_weights)
            self.agg_tasks.add(agg, agg.get_train_batches.remote())
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

    @override(Aggregator)
    def reset(self, remote_evaluators):
        raise NotImplementedError("changing number of remote evaluators")

    @staticmethod
    def precreate_aggregators(n):
        return create_colocated(AggregationWorker, [], n)


@ray.remote(num_cpus=1)
class AggregationWorker(AggregationWorkerBase):
    def __init__(self):
        self.initialized = False

    def init(self, initial_weights_obj_id, remote_evaluators,
             max_sample_requests_in_flight_per_worker, replay_proportion,
             replay_buffer_num_slots, train_batch_size, sample_batch_size):
        """Deferred init that assigns sub-workers to this aggregator."""

        logger.info("Assigned evaluators {} to aggregation worker {}".format(
            remote_evaluators, self))
        assert remote_evaluators
        AggregationWorkerBase.__init__(
            self, initial_weights_obj_id, remote_evaluators,
            max_sample_requests_in_flight_per_worker, replay_proportion,
            replay_buffer_num_slots, train_batch_size, sample_batch_size)
        self.initialized = True

    def set_weights(self, weights):
        self.broadcasted_weights = weights

    def get_train_batches(self):
        assert self.initialized, "Must call init() before using this class."
        start = time.time()
        result = []
        for batch in self.iter_train_batches(max_yield=5):
            result.append(batch)
        while not result:
            time.sleep(0.01)
            for batch in self.iter_train_batches(max_yield=5):
                result.append(batch)
        logger.debug("Returning {} train batches, {}s".format(
            len(result),
            time.time() - start))
        return result

    def get_host(self):
        return os.uname()[1]
