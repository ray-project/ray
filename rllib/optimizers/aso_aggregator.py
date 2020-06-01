"""Helper class for AsyncSamplesOptimizer."""

import numpy as np
import random

import ray
from ray.rllib.utils.actors import TaskPool
from ray.rllib.utils.annotations import override


class Aggregator:
    """An aggregator collects and processes samples from workers.

    This class is used to abstract away the strategy for sample collection.
    For example, you may want to use a tree of actors to collect samples. The
    use of multiple actors can be necessary to offload expensive work such
    as concatenating and decompressing sample batches.

    Attributes:
        local_worker: local RolloutWorker copy
    """

    def iter_train_batches(self):
        """Returns a generator over batches ready to learn on.

        Iterating through this generator will also send out weight updates to
        remote workers as needed.

        This call may block until results are available.
        """
        raise NotImplementedError

    def broadcast_new_weights(self):
        """Broadcast a new set of weights from the local workers."""
        raise NotImplementedError

    def should_broadcast(self):
        """Returns whether broadcast() should be called to update weights."""
        raise NotImplementedError

    def stats(self):
        """Returns runtime statistics for debugging."""
        raise NotImplementedError

    def reset(self, remote_workers):
        """Called to change the set of remote workers being used."""
        raise NotImplementedError


class AggregationWorkerBase:
    """Aggregators should extend from this class."""

    def __init__(self, initial_weights_obj_id, remote_workers,
                 max_sample_requests_in_flight_per_worker, replay_proportion,
                 replay_buffer_num_slots, train_batch_size,
                 rollout_fragment_length):
        """Initialize an aggregator.

        Arguments:
            initial_weights_obj_id (ObjectID): initial worker weights
            remote_workers (list): set of remote workers assigned to this agg
            max_sample_request_in_flight_per_worker (int): max queue size per
                worker
            replay_proportion (float): ratio of replay to sampled outputs
            replay_buffer_num_slots (int): max number of sample batches to
                store in the replay buffer
            train_batch_size (int): size of batches to learn on
            rollout_fragment_length (int): size of batches to sample from
                workers.
        """

        self.broadcasted_weights = initial_weights_obj_id
        self.remote_workers = remote_workers
        self.rollout_fragment_length = rollout_fragment_length
        self.train_batch_size = train_batch_size

        if replay_proportion:
            if (replay_buffer_num_slots * rollout_fragment_length <=
                    train_batch_size):
                raise ValueError(
                    "Replay buffer size is too small to produce train, "
                    "please increase replay_buffer_num_slots.",
                    replay_buffer_num_slots, rollout_fragment_length,
                    train_batch_size)

        # Kick off async background sampling
        self.sample_tasks = TaskPool()
        for ev in self.remote_workers:
            ev.set_weights.remote(self.broadcasted_weights)
            for _ in range(max_sample_requests_in_flight_per_worker):
                self.sample_tasks.add(ev, ev.sample.remote())

        self.batch_buffer = []

        self.replay_proportion = replay_proportion
        self.replay_buffer_num_slots = replay_buffer_num_slots
        self.replay_batches = []
        self.replay_index = 0
        self.num_sent_since_broadcast = 0
        self.num_weight_syncs = 0
        self.num_replayed = 0

    @override(Aggregator)
    def iter_train_batches(self, max_yield=999):
        """Iterate over train batches.

        Arguments:
            max_yield (int): Max number of batches to iterate over in this
                cycle. Setting this avoids iter_train_batches returning too
                much data at once.
        """

        for ev, sample_batch in self._augment_with_replay(
                self.sample_tasks.completed_prefetch(
                    blocking_wait=True, max_yield=max_yield)):
            sample_batch.decompress_if_needed()
            self.batch_buffer.append(sample_batch)
            if sum(b.count
                   for b in self.batch_buffer) >= self.train_batch_size:
                if len(self.batch_buffer) == 1:
                    # make a defensive copy to avoid sharing plasma memory
                    # across multiple threads
                    train_batch = self.batch_buffer[0].copy()
                else:
                    train_batch = self.batch_buffer[0].concat_samples(
                        self.batch_buffer)
                yield train_batch
                self.batch_buffer = []

            # If the batch was replayed, skip the update below.
            if ev is None:
                continue

            # Put in replay buffer if enabled
            if self.replay_buffer_num_slots > 0:
                if len(self.replay_batches) < self.replay_buffer_num_slots:
                    self.replay_batches.append(sample_batch)
                else:
                    self.replay_batches[self.replay_index] = sample_batch
                    self.replay_index += 1
                    self.replay_index %= self.replay_buffer_num_slots

            ev.set_weights.remote(self.broadcasted_weights)
            self.num_weight_syncs += 1
            self.num_sent_since_broadcast += 1

            # Kick off another sample request
            self.sample_tasks.add(ev, ev.sample.remote())

    @override(Aggregator)
    def stats(self):
        return {
            "num_weight_syncs": self.num_weight_syncs,
            "num_steps_replayed": self.num_replayed,
        }

    @override(Aggregator)
    def reset(self, remote_workers):
        self.sample_tasks.reset_workers(remote_workers)

    def _augment_with_replay(self, sample_futures):
        def can_replay():
            num_needed = int(
                np.ceil(self.train_batch_size / self.rollout_fragment_length))
            return len(self.replay_batches) > num_needed

        for ev, sample_batch in sample_futures:
            sample_batch = ray.get(sample_batch)
            yield ev, sample_batch

            if can_replay():
                f = self.replay_proportion
                while random.random() < f:
                    f -= 1
                    replay_batch = random.choice(self.replay_batches)
                    self.num_replayed += replay_batch.count
                    yield None, replay_batch


class SimpleAggregator(AggregationWorkerBase, Aggregator):
    """Simple single-threaded implementation of an Aggregator."""

    def __init__(self,
                 workers,
                 max_sample_requests_in_flight_per_worker=2,
                 replay_proportion=0.0,
                 replay_buffer_num_slots=0,
                 train_batch_size=500,
                 rollout_fragment_length=50,
                 broadcast_interval=5):
        self.workers = workers
        self.local_worker = workers.local_worker()
        self.broadcast_interval = broadcast_interval
        self.broadcast_new_weights()
        AggregationWorkerBase.__init__(
            self, self.broadcasted_weights, self.workers.remote_workers(),
            max_sample_requests_in_flight_per_worker, replay_proportion,
            replay_buffer_num_slots, train_batch_size, rollout_fragment_length)

    @override(Aggregator)
    def broadcast_new_weights(self):
        self.broadcasted_weights = ray.put(self.local_worker.get_weights())
        self.num_sent_since_broadcast = 0

    @override(Aggregator)
    def should_broadcast(self):
        return self.num_sent_since_broadcast >= self.broadcast_interval
