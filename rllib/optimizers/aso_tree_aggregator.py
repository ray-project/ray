"""Helper class for AsyncSamplesOptimizer."""

import collections
import logging
import platform
import time

import ray
from ray.rllib.utils.actors import TaskPool, create_colocated
from ray.rllib.utils.annotations import override
from ray.rllib.optimizers.aso_aggregator import Aggregator, \
    AggregationWorkerBase

logger = logging.getLogger(__name__)


class TreeAggregator(Aggregator):
    """A hierarchical experiences aggregator.

    The given set of remote workers is divided into subsets and assigned to
    one of several aggregation workers. These aggregation workers collate
    experiences into batches of size `train_batch_size` and we collect them
    in this class when `iter_train_batches` is called.
    """

    def __init__(self,
                 workers,
                 num_aggregation_workers,
                 max_sample_requests_in_flight_per_worker=2,
                 replay_proportion=0.0,
                 replay_buffer_num_slots=0,
                 train_batch_size=500,
                 rollout_fragment_length=50,
                 broadcast_interval=5):
        """Initialize a tree aggregator.

        Arguments:
            workers (WorkerSet): set of all workers
            num_aggregation_workers (int): number of intermediate actors to
                use for data aggregation
            max_sample_request_in_flight_per_worker (int): max queue size per
                worker
            replay_proportion (float): ratio of replay to sampled outputs
            replay_buffer_num_slots (int): max number of sample batches to
                store in the replay buffer
            train_batch_size (int): size of batches to learn on
            rollout_fragment_length (int): size of batches to sample from
                workers.
            broadcast_interval (int): max number of workers to send the
                same set of weights to
        """
        self.workers = workers
        self.num_aggregation_workers = num_aggregation_workers
        self.max_sample_requests_in_flight_per_worker = \
            max_sample_requests_in_flight_per_worker
        self.replay_proportion = replay_proportion
        self.replay_buffer_num_slots = replay_buffer_num_slots
        self.rollout_fragment_length = rollout_fragment_length
        self.train_batch_size = train_batch_size
        self.broadcast_interval = broadcast_interval
        self.broadcasted_weights = ray.put(
            workers.local_worker().get_weights())
        self.num_batches_processed = 0
        self.num_broadcasts = 0
        self.num_sent_since_broadcast = 0
        self.initialized = False

    def init(self, aggregators):
        """Deferred init so that we can pass in previously created workers."""

        assert len(aggregators) == self.num_aggregation_workers, aggregators
        if len(self.workers.remote_workers()) < self.num_aggregation_workers:
            raise ValueError(
                "The number of aggregation workers should not exceed the "
                "number of total evaluation workers ({} vs {})".format(
                    self.num_aggregation_workers,
                    len(self.workers.remote_workers())))

        assigned_workers = collections.defaultdict(list)
        for i, ev in enumerate(self.workers.remote_workers()):
            assigned_workers[i % self.num_aggregation_workers].append(ev)

        self.aggregators = aggregators
        for i, agg in enumerate(self.aggregators):
            agg.init.remote(
                self.broadcasted_weights, assigned_workers[i],
                self.max_sample_requests_in_flight_per_worker,
                self.replay_proportion, self.replay_buffer_num_slots,
                self.train_batch_size, self.rollout_fragment_length)

        self.agg_tasks = TaskPool()
        for agg in self.aggregators:
            agg.set_weights.remote(self.broadcasted_weights)
            self.agg_tasks.add(agg, agg.get_train_batches.remote())

        self.initialized = True

    @override(Aggregator)
    def iter_train_batches(self):
        assert self.initialized, "Must call init() before using this class."
        for agg, batches in self.agg_tasks.completed_prefetch():
            for b in ray.get(batches):
                self.num_sent_since_broadcast += 1
                yield b
            agg.set_weights.remote(self.broadcasted_weights)
            self.agg_tasks.add(agg, agg.get_train_batches.remote())
            self.num_batches_processed += 1

    @override(Aggregator)
    def broadcast_new_weights(self):
        self.broadcasted_weights = ray.put(
            self.workers.local_worker().get_weights())
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
    def reset(self, remote_workers):
        raise NotImplementedError("changing number of remote workers")

    @staticmethod
    def precreate_aggregators(n):
        return create_colocated(AggregationWorker, [], n)


@ray.remote(num_cpus=1)
class AggregationWorker(AggregationWorkerBase):
    def __init__(self):
        self.initialized = False

    def init(self, initial_weights_obj_id, remote_workers,
             max_sample_requests_in_flight_per_worker, replay_proportion,
             replay_buffer_num_slots, train_batch_size,
             rollout_fragment_length):
        """Deferred init that assigns sub-workers to this aggregator."""

        logger.info("Assigned workers {} to aggregation worker {}".format(
            remote_workers, self))
        assert remote_workers
        AggregationWorkerBase.__init__(
            self, initial_weights_obj_id, remote_workers,
            max_sample_requests_in_flight_per_worker, replay_proportion,
            replay_buffer_num_slots, train_batch_size, rollout_fragment_length)
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
        return platform.node()
